// Producer reads JSON objects from stdin, one per line, and publishes them
// with one Publish stream per shard, so a full shard doesn't hold up acks of
// the others. Each line's shard is hash(key) % shards, computed here, so lines
// with the same key go to the same shard, in order. Lines without a key take
// turns over the shards.
//
// Lines waiting together are sent as one batch, so the broker stores them in
// one fsync. Each batch is saved to a local spool file before it's sent, and
// the file is deleted once all its batches are acked (see spool.go). A failed
// stream is retried from the first batch not acked, with growing delays, for
// as long as the broker is unreachable. Files left by a crash are sent at the next start.
// The broker drops what it already has (see producer_id in event.proto).
// Producer ids are reused between runs, and the shard count is saved, so a
// run can start and save lines with the broker down.
//
// Usage:
//
//	echo '{"temp":21}' | go run ./clients/producer -topic sensors
//	go run ./clients/producer -topic sensors -key-field device < events.jsonl
package main

// TODO: Make an SDK for like python

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sync"
	"time"
	"wisp/partition"
	pb "wisp/proto"
	"wisp/shards"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// defaultAddr is the address the broker listens on (see main.go at the repo root).
const defaultAddr = "localhost:50051"

const (
	shardBuffer   = 1024 // lines read but not yet saved, per shard
	sendBacklog   = 1024 // saved batches waiting to be sent, per shard; reading stdin blocks when full
	maxRetryDelay = 30 * time.Second
)

// retryDelay is the wait after the first failure; it doubles after each
// failure in a row, up to maxRetryDelay. Tests shorten it.
var retryDelay = 200 * time.Millisecond

func main() {
	addr := flag.String("addr", defaultAddr, "broker address")
	topic := flag.String("topic", "", "topic to publish to (required)")
	keyField := flag.String("key-field", "", "JSON field whose value is the partition key (empty: lines take turns over the shards)")
	spool := flag.String("spool", defaultSpool(), "directory for lines not yet acked; each topic has its own folder")
	flag.Parse()
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "producer: -topic is required")
		flag.Usage()
		os.Exit(2)
	}

	// Ctrl+C stops reading and exits cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("could not create client", "addr", *addr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	sent, err := run(ctx, pb.NewEventBrokerClient(conn), *topic, *keyField, *spool, os.Stdin)
	slog.Info("producer done", "topic", *topic, "published", sent)
	if err != nil {
		slog.Error("producer stopped", "err", err)
		os.Exit(1)
	}
}

// defaultSpool is in the user's cache directory, so it doesn't depend on
// where the producer is started.
func defaultSpool() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "wisp-spool"
	}
	return filepath.Join(dir, "wisp", "spool")
}

// run publishes the records left in the spool, then every non-empty line of
// in, and returns how many were stored. The request id is the line number.
// Lines that aren't JSON objects are skipped. An error that retries can't fix
// stops run; lines not acked stay in the spool for the next run.
func run(ctx context.Context, client pb.EventBrokerClient, topic, keyField, spool string, in io.Reader) (int, error) {
	dir := filepath.Join(spool, topic)
	numShards, err := shardCount(ctx, client, topic, dir)
	if err != nil {
		return 0, err
	}

	leases, err := takeLeases(dir, numShards)
	if err != nil {
		return 0, err
	}
	senders := make([]*sender, numShards)
	for i := range senders {
		senders[i] = &sender{
			client: client,
			dir:    dir,
			lease:  leases[0],
			topic:  topic,
			shard:  int32(i),
			lines:  make(chan line, shardBuffer),
			out:    make(chan *spooled, sendBacklog),
		}
	}
	kept, err := loadSpool(dir, leases, senders)
	defer func() { closeLeases(kept) }()
	if err != nil {
		return 0, err
	}

	// The first error cancels every sender and is what run returns.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var wg sync.WaitGroup
	for _, s := range senders {
		wg.Go(func() {
			if err := s.spoolLines(ctx); err != nil {
				cancel(err)
			}
		})
		wg.Go(func() {
			if err := s.run(ctx); err != nil {
				cancel(err)
			}
		})
	}

	if err := sendLines(ctx, senders, leases[0], topic, keyField, in); err != nil {
		cancel(err)
	}
	for _, s := range senders {
		close(s.lines)
	}
	wg.Wait()

	stored := 0
	for _, s := range senders {
		stored += s.stored
		s.closeFiles()
	}
	return stored, context.Cause(ctx)
}

// shardCount returns the topic's shard count. It's saved in the spool the
// first time, and never changes, so later runs can start with the broker down.
func shardCount(ctx context.Context, client pb.EventBrokerClient, topic, dir string) (int, error) {
	if n := loadShardCount(dir); n > 0 {
		return n, nil
	}
	var info *pb.TopicInfo
	err := retry(ctx, func() (bool, error) {
		var err error
		info, err = client.DescribeTopic(ctx, &pb.DescribeTopicRequest{Topic: topic})
		return false, err
	})
	if err != nil {
		return 0, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	n := int(info.GetNumShards())
	if n <= 0 {
		return 0, fmt.Errorf("describe topic %q: broker reported %d shards", topic, n)
	}
	return n, saveShardCount(dir, n)
}

// sendLines gives each valid line to the sender of its shard, continuing the
// lease's sequences. Invalid lines are skipped here so that they don't use up
// a sequence.
func sendLines(ctx context.Context, senders []*sender, l *lease, topic, keyField string, in io.Reader) error {
	nextShard := 0 // for lines without a key

	scanner := bufio.NewScanner(in)
	for number := uint64(1); scanner.Scan(); number++ {
		payload := bytes.Clone(bytes.TrimSpace(scanner.Bytes()))
		if len(payload) == 0 {
			continue
		}
		if err := shards.ValidatePayload(payload); err != nil {
			slog.Warn("invalid event, skipped", "line", number, "err", err)
			continue
		}

		shard := nextShard
		if key := partitionKey(payload, keyField); key != nil {
			shard = partition.Shard(key, len(senders))
		} else {
			nextShard = (nextShard + 1) % len(senders)
		}
		l.sequences[shard]++
		select {
		case senders[shard].lines <- line{number, l.sequences[shard], payload}:
		case <-ctx.Done():
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	return nil
}

// line is one record read from stdin, not yet saved.
type line struct {
	number   uint64
	sequence uint64
	payload  []byte
}

// sender publishes the records of one shard.
type sender struct {
	client pb.EventBrokerClient
	dir    string
	lease  *lease
	topic  string
	shard  int32
	lines  chan line     // closed when there are no more lines
	out    chan *spooled // saved batches to send; closed after lines
	stored int

	mu      sync.Mutex
	current *batch     // file being written; nil if none
	unacked []*spooled // sent or about to be, oldest first
}

// spoolLines groups the waiting lines into requests, saves each to the spool
// with one write, and passes it on. It doesn't wait for more lines, so a lone
// line goes out at once and requests only grow under load. It's separate from
// sending, so lines are saved while the broker is down.
func (s *sender) spoolLines(ctx context.Context) error {
	defer close(s.out)
	for {
		select {
		case first, ok := <-s.lines:
			if !ok {
				return nil
			}
			record, err := s.spool(s.collect(first))
			if err != nil {
				return fmt.Errorf("save line %d: %w", first.number, err)
			}
			select {
			case s.out <- record:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// collect returns a request of first and the lines waiting behind it.
func (s *sender) collect(first line) *pb.PublishRequest {
	req := &pb.PublishRequest{
		RequestId:     first.number,
		Topic:         s.topic,
		Shard:         s.shard,
		Payloads:      [][]byte{first.payload},
		ProducerId:    s.lease.id,
		FirstSequence: first.sequence,
	}
	size := len(first.payload)
	for len(req.Payloads) < maxBatchLines && size < maxBatchBytes {
		select {
		case next, ok := <-s.lines:
			if !ok {
				return req
			}
			req.Payloads = append(req.Payloads, next.payload)
			size += len(next.payload)
		default:
			return req
		}
	}
	return req
}

// spool saves req to the current file, starting a new one if needed.
func (s *sender) spool(req *pb.PublishRequest) (*spooled, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.current.full() {
		b, err := newBatch(s.dir, s.lease, req)
		if err != nil {
			return nil, err
		}
		if s.current != nil && s.current.pending == 0 {
			s.current.remove()
		}
		s.current = b
	}
	if err := s.current.write(req); err != nil {
		return nil, err
	}
	s.current.pending++
	return &spooled{req: req, batch: s.current}, nil
}

// acked drops the oldest request, and deletes its file if it was the last one
// and the file is full. The file being written stays, so that a slow stream of
// lines doesn't cost one fsync each (see batch.remove). If it's left by a
// crash, the broker drops its acked records as duplicates.
func (s *sender) acked() {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.unacked[0].batch
	s.unacked = s.unacked[1:]
	b.pending--
	if b.pending == 0 && b != s.current {
		b.remove()
	}
}

// closeFiles deletes the current file if it's fully acked, and releases the
// files of requests that aren't. They stay for the next run. out must be
// closed.
func (s *sender) closeFiles() {
	for record := range s.out {
		s.unacked = append(s.unacked, record)
	}
	if s.current != nil && s.current.pending == 0 {
		s.current.remove()
	}
	for _, record := range s.unacked {
		record.batch.file.Close()
	}
}

// run opens streams until out is closed and everything is acked.
func (s *sender) run(ctx context.Context) error {
	return retry(ctx, func() (bool, error) {
		before := s.stored
		err := s.publish(ctx)
		return s.stored > before, err
	})
}

// retry calls attempt until it succeeds, it fails with an error retries can't
// fix, or ctx ends. attempt reports whether it made progress, which resets
// the delay. There is no limit on attempts: the records are safe in the spool.
func retry(ctx context.Context, attempt func() (progress bool, err error)) error {
	delay := retryDelay
	for {
		progress, err := attempt()
		if err == nil || !retryable(err) || ctx.Err() != nil {
			return err
		}
		if progress {
			delay = retryDelay
		}
		slog.Warn("broker call failed, retrying", "delay", delay, "err", err)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		delay = nextDelay(delay)
	}
}

func nextDelay(delay time.Duration) time.Duration {
	return min(delay*2, maxRetryDelay)
}

// retryable says if sending the records again can help. FailedPrecondition
// follows an earlier failure: the resend fills the gap. ResourceExhausted
// means the broker is out of disk space, which someone can fix.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.Internal, codes.FailedPrecondition, codes.ResourceExhausted:
		return true
	}
	return false
}

// publish runs one stream. It returns nil once out is closed and every
// request is acked.
func (s *sender) publish(ctx context.Context) error {
	// Cancelling streamCtx unblocks send if receiving acks fails.
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.client.Publish(streamCtx)
	if err != nil {
		return fmt.Errorf("open publish stream: %w", err)
	}

	sent := make(chan error, 1)
	go func() { sent <- s.send(streamCtx, stream) }()
	ackErr := s.receiveAcks(stream)
	cancel()
	sendErr := <-sent

	s.mu.Lock()
	defer s.mu.Unlock()
	if ackErr != nil {
		return ackErr
	}
	if sendErr != nil || len(s.unacked) > 0 {
		return status.Error(codes.Unavailable, "publish stream ended early")
	}
	return nil
}

// send resends what isn't acked, then sends new requests until out is closed.
// A request joins unacked before it's sent, so a failed send is retried.
func (s *sender) send(ctx context.Context, stream pb.EventBroker_PublishClient) error {
	s.mu.Lock()
	resend := slices.Clone(s.unacked)
	s.mu.Unlock()
	for _, record := range resend {
		if err := stream.Send(record.req); err != nil {
			return err
		}
	}
	for {
		select {
		case record, ok := <-s.out:
			if !ok {
				return stream.CloseSend()
			}
			s.mu.Lock()
			s.unacked = append(s.unacked, record)
			s.mu.Unlock()
			if err := stream.Send(record.req); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// receiveAcks removes each stored request from unacked until the stream ends.
// Acks come in request order, so each one is for the oldest request.
func (s *sender) receiveAcks(stream pb.EventBroker_PublishClient) error {
	for {
		ack, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		code := codes.Code(ack.GetErrorCode())
		if code != codes.OK && code != codes.AlreadyExists { // AlreadyExists: stored by an earlier attempt
			return fmt.Errorf("publish line %d: %w", ack.GetRequestId(), status.Error(code, ack.GetErrorMessage()))
		}
		s.mu.Lock()
		s.stored += len(s.unacked[0].req.GetPayloads())
		s.mu.Unlock()
		s.acked()
	}
}

// partitionKey returns the value of field keyField (strings without quotes,
// other values as JSON text), or nil if there is none.
func partitionKey(payload []byte, keyField string) []byte {
	if keyField == "" {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil
	}
	raw, ok := fields[keyField]
	if !ok {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []byte(s)
	}
	return raw
}

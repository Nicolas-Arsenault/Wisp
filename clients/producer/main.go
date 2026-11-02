// Producer reads JSON objects from stdin, one per line, and publishes them
// with one Publish stream per shard, so a full shard doesn't hold up acks of
// the others. Each line's shard is hash(key) % shards, computed here, so lines
// with the same key go to the same shard, in order. Lines without a key take
// turns over the shards.
//
// A failed stream is retried from the first record not yet acked. The broker
// drops the ones it already has (see producer_id in event.proto).
//
// Usage:
//
//	echo '{"temp":21}' | go run ./clients/producer -topic sensors
//	go run ./clients/producer -topic sensors -key-field device < events.jsonl
package main

// TODO: Make an SDK for like python

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
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
	shardBuffer = 1024 // lines waiting per shard; reading stdin blocks when full
	maxRetries  = 5    // failed attempts in a row before giving up on a shard
)

// retryDelay is the wait between attempts. Tests shorten it.
var retryDelay = 200 * time.Millisecond

func main() {
	addr := flag.String("addr", defaultAddr, "broker address")
	topic := flag.String("topic", "", "topic to publish to (required)")
	keyField := flag.String("key-field", "", "JSON field whose value is the partition key (empty: lines take turns over the shards)")
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

	sent, err := run(ctx, pb.NewEventBrokerClient(conn), *topic, *keyField, os.Stdin)
	slog.Info("producer done", "topic", *topic, "published", sent)
	if err != nil {
		slog.Error("producer stopped", "err", err)
		os.Exit(1)
	}
}

// run publishes every non-empty line of in and returns how many were stored.
// The request id is the line number. Lines that aren't JSON objects are
// skipped. An error that retries can't fix stops run.
func run(ctx context.Context, client pb.EventBrokerClient, topic, keyField string, in io.Reader) (int, error) {
	info, err := client.DescribeTopic(ctx, &pb.DescribeTopicRequest{Topic: topic})
	if err != nil {
		return 0, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	numShards := int(info.GetNumShards())
	if numShards <= 0 {
		return 0, fmt.Errorf("describe topic %q: broker reported %d shards", topic, numShards)
	}

	// The first error cancels every sender and is what run returns.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	senders := make([]*sender, numShards)
	var wg sync.WaitGroup
	for i := range senders {
		s := &sender{client: client, in: make(chan *pb.PublishRequest, shardBuffer)}
		senders[i] = s
		wg.Go(func() {
			if err := s.run(ctx); err != nil {
				cancel(err)
			}
		})
	}

	if err := sendLines(ctx, senders, topic, keyField, in); err != nil {
		cancel(err)
	}
	for _, s := range senders {
		close(s.in)
	}
	wg.Wait()

	stored := 0
	for _, s := range senders {
		stored += s.stored
	}
	return stored, context.Cause(ctx)
}

// sendLines gives each valid line to the sender of its shard, numbering each
// shard's records from 1. The id is new on every run, because the numbering
// restarts. Invalid lines are skipped here so that they don't use up a sequence.
func sendLines(ctx context.Context, senders []*sender, topic, keyField string, in io.Reader) error {
	producerID := rand.Uint64() | 1 // never 0, which means no duplicate check
	sequences := make([]uint64, len(senders))
	nextShard := 0 // for lines without a key

	scanner := bufio.NewScanner(in)
	for line := 1; scanner.Scan(); line++ {
		payload := []byte(strings.TrimSpace(scanner.Text()))
		if len(payload) == 0 {
			continue
		}
		if err := shards.ValidatePayload(payload); err != nil {
			slog.Warn("invalid event, skipped", "line", line, "err", err)
			continue
		}

		shard := nextShard
		if key := partitionKey(payload, keyField); key != nil {
			shard = partition.Shard(key, len(senders))
		} else {
			nextShard = (nextShard + 1) % len(senders)
		}
		sequences[shard]++
		req := &pb.PublishRequest{
			RequestId:  uint64(line),
			Topic:      topic,
			Shard:      int32(shard),
			Payload:    payload,
			ProducerId: producerID,
			Sequence:   sequences[shard],
		}
		select {
		case senders[shard].in <- req:
		case <-ctx.Done():
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	return nil
}

// sender publishes the records of one shard.
type sender struct {
	client pb.EventBrokerClient
	in     chan *pb.PublishRequest // closed when there are no more lines
	stored int

	mu      sync.Mutex
	unacked []*pb.PublishRequest // sent or about to be, oldest first
}

// run opens streams until in is closed and everything is acked.
func (s *sender) run(ctx context.Context) error {
	for failures := 1; ; failures++ {
		before := s.stored
		err := s.publish(ctx)
		if err == nil || !retryable(err) || ctx.Err() != nil {
			return err
		}
		if s.stored > before {
			failures = 1
		}
		if failures >= maxRetries {
			return err
		}
		slog.Warn("publish failed, retrying", "attempt", failures, "err", err)
		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// retryable says if sending the records again can help. FailedPrecondition
// follows an earlier failure: the resend fills the gap.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.Internal, codes.FailedPrecondition:
		return true
	}
	return false
}

// publish runs one stream. It returns nil once in is closed and every record
// is acked.
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

// send resends what isn't acked, then sends new records until in is closed.
// A record joins unacked before it's sent, so a failed send is retried.
func (s *sender) send(ctx context.Context, stream pb.EventBroker_PublishClient) error {
	s.mu.Lock()
	resend := append([]*pb.PublishRequest{}, s.unacked...)
	s.mu.Unlock()
	for _, req := range resend {
		if err := stream.Send(req); err != nil {
			return err
		}
	}
	for {
		select {
		case req, ok := <-s.in:
			if !ok {
				return stream.CloseSend()
			}
			s.mu.Lock()
			s.unacked = append(s.unacked, req)
			s.mu.Unlock()
			if err := stream.Send(req); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// receiveAcks removes each stored record from unacked until the stream ends.
// Acks come in request order, so each one is for the oldest record.
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
		slog.Debug("event stored", "line", ack.GetRequestId(), "shard", ack.GetShard(), "event_id", ack.GetEventId())
		s.stored++
		s.mu.Lock()
		s.unacked = s.unacked[1:]
		s.mu.Unlock()
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

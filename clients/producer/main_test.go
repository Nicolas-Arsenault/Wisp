package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"wisp/partition"
	pb "wisp/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const testShards = 5

// record is one record the broker received, whichever request held it.
type record struct {
	stream   int
	topic    string
	shard    int32
	producer uint64
	sequence uint64
	payload  string
}

// hasPayload says if req holds payload.
func hasPayload(req *pb.PublishRequest, payload string) bool {
	return slices.ContainsFunc(req.GetPayloads(), func(p []byte) bool { return string(p) == payload })
}

// fakeBroker records every published record. code decides the ack's error
// code for each one; nil accepts everything.
type fakeBroker struct {
	pb.UnimplementedEventBrokerServer
	code func(req *pb.PublishRequest) codes.Code
	// describeErr fails DescribeTopic.
	describeErr error

	mu        sync.Mutex
	received  []record
	streams   int
	describes int
}

func (f *fakeBroker) newStream() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams++
	return f.streams
}

// failOnce returns a code func that answers code the first time it sees
// payload, then accepts everything.
func failOnce(payload string, code codes.Code) func(*pb.PublishRequest) codes.Code {
	var mu sync.Mutex
	failed := false
	return func(req *pb.PublishRequest) codes.Code {
		mu.Lock()
		defer mu.Unlock()
		if hasPayload(req, payload) && !failed {
			failed = true
			return code
		}
		return codes.OK
	}
}

// fastRetries makes retries immediate for one test.
func fastRetries(t *testing.T) {
	t.Helper()
	old := retryDelay
	retryDelay = time.Millisecond
	t.Cleanup(func() { retryDelay = old })
}

func (f *fakeBroker) DescribeTopic(_ context.Context, req *pb.DescribeTopicRequest) (*pb.TopicInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describes++
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return &pb.TopicInfo{Topic: req.GetTopic(), NumShards: testShards}, nil
}

func (f *fakeBroker) Publish(stream pb.EventBroker_PublishServer) error {
	id := f.newStream()
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		f.mu.Lock()
		for i, payload := range req.GetPayloads() {
			f.received = append(f.received, record{id, req.GetTopic(), req.GetShard(), req.GetProducerId(), req.GetFirstSequence() + uint64(i), string(payload)})
		}
		f.mu.Unlock()

		ack := &pb.PublishAck{RequestId: req.GetRequestId(), Shard: req.GetShard()}
		if f.code != nil {
			ack.ErrorCode = int32(f.code(req))
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (f *fakeBroker) records() []record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.received)
}

func (f *fakeBroker) payloads() string {
	var out []string
	for _, req := range f.records() {
		out = append(out, string(req.payload))
	}
	slices.Sort(out) // shards are sent independently
	return strings.Join(out, ",")
}

// newClient serves broker on an in-memory listener and returns a client for it.
func newClient(t testing.TB, broker pb.EventBrokerServer) pb.EventBrokerClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterEventBrokerServer(srv, broker)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewEventBrokerClient(conn)
}

func TestRunPublishesEachLine(t *testing.T) {
	broker := &fakeBroker{}
	in := "{\"n\":1}\n\n   \n  {\"n\":2}  \n{\"n\":3}" // blank lines, padding, no final newline

	sent, err := run(t.Context(), newClient(t, broker), "sensors", "", t.TempDir(), strings.NewReader(in))
	if err != nil || sent != 3 {
		t.Fatalf("run = %d, %v, want 3, nil", sent, err)
	}
	if got, want := broker.payloads(), `{"n":1},{"n":2},{"n":3}`; got != want {
		t.Errorf("broker got %s, want %s", got, want)
	}
	for _, req := range broker.records() {
		if req.topic != "sensors" {
			t.Errorf("topic = %q, want sensors", req.topic)
		}
	}
}

// Each line goes to the shard of its key.
func TestRunHashesKeyToShard(t *testing.T) {
	broker := &fakeBroker{}
	in := `{"dev":"a","n":1}` + "\n" + `{"dev":42}` + "\n"

	if _, err := run(t.Context(), newClient(t, broker), "t", "dev", t.TempDir(), strings.NewReader(in)); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		`{"dev":"a","n":1}`: partition.Shard([]byte("a"), testShards),  // string: without quotes
		`{"dev":42}`:        partition.Shard([]byte("42"), testShards), // number: as JSON text
	}
	for _, req := range broker.records() {
		if int(req.shard) != want[req.payload] {
			t.Errorf("%s went to shard %d, want %d", req.payload, req.shard, want[req.payload])
		}
	}
}

func TestRunEmptyInput(t *testing.T) {
	broker := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", t.TempDir(), strings.NewReader(""))
	if err != nil || sent != 0 {
		t.Errorf("run = %d, %v, want 0, nil", sent, err)
	}
}

// Lines without a key take turns over the shards, with or without -key-field.
func TestRunRoundRobinWithoutKey(t *testing.T) {
	for _, keyField := range []string{"", "dev"} {
		broker := &fakeBroker{}
		var in strings.Builder
		for i := range testShards * 2 {
			fmt.Fprintf(&in, "{\"n\":%d}\n", i)
		}
		if _, err := run(t.Context(), newClient(t, broker), "t", keyField, t.TempDir(), strings.NewReader(in.String())); err != nil {
			t.Fatal(err)
		}
		perShard := map[int32]int{}
		for _, req := range broker.records() {
			perShard[req.shard]++
			var n int32
			fmt.Sscanf(req.payload, `{"n":%d}`, &n)
			if want := n % testShards; req.shard != want {
				t.Errorf("%s went to shard %d, want %d", req.payload, req.shard, want)
			}
		}
		for shard := range int32(testShards) {
			if perShard[shard] != 2 {
				t.Errorf("key field %q: shard %d got %d lines, want 2", keyField, shard, perShard[shard])
			}
		}
	}
}

// An invalid line is skipped without being sent, and uses no sequence: a gap
// would make the broker refuse every line after it.
func TestRunSkipsInvalidEvents(t *testing.T) {
	broker := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, broker), "t", "key", t.TempDir(), strings.NewReader(
		"{\"key\":\"a\",\"n\":1}\nbad\n{\"key\":\"a\",\"n\":2}\n"))
	if err != nil || sent != 2 {
		t.Fatalf("run = %d, %v, want 2, nil", sent, err)
	}
	var sequences []uint64
	for _, req := range broker.records() {
		sequences = append(sequences, req.sequence)
	}
	if fmt.Sprint(sequences) != "[1 2]" {
		t.Errorf("sequences = %v, want [1 2]", sequences)
	}
}

// Each shard has its own stream and its own sequence, under one producer id.
func TestRunOneStreamPerShard(t *testing.T) {
	broker := &fakeBroker{}
	in := strings.Repeat(`{"n":1}`+"\n", testShards*3)
	if _, err := run(t.Context(), newClient(t, broker), "t", "", t.TempDir(), strings.NewReader(in)); err != nil {
		t.Fatal(err)
	}

	streamShard := map[int]int32{}
	lastSequence := map[int32]uint64{}
	producers := map[uint64]bool{}
	for _, req := range broker.records() {
		stream := req.stream
		if shard, ok := streamShard[stream]; ok && shard != req.shard {
			t.Errorf("stream %d has shards %d and %d", stream, shard, req.shard)
		}
		streamShard[stream] = req.shard
		lastSequence[req.shard]++
		if req.sequence != lastSequence[req.shard] {
			t.Errorf("shard %d: sequence %d, want %d", req.shard, req.sequence, lastSequence[req.shard])
		}
		producers[req.producer] = true
	}
	if len(producers) != 1 || producers[0] {
		t.Errorf("producer ids = %v, want one that isn't 0", producers)
	}
}

// A shard that doesn't answer holds up none of the others.
func TestRunStuckShardDoesNotBlockOthers(t *testing.T) {
	release := make(chan struct{})
	acked := make(chan struct{}, testShards)
	broker := &fakeBroker{code: func(req *pb.PublishRequest) codes.Code {
		if req.GetShard() == 0 {
			<-release
		} else {
			acked <- struct{}{}
		}
		return codes.OK
	}}
	in := strings.Repeat(`{"n":1}`+"\n", testShards)
	done := make(chan error, 1)
	client := newClient(t, broker)
	go func() {
		_, err := run(t.Context(), client, "t", "", t.TempDir(), strings.NewReader(in))
		done <- err
	}()

	for range testShards - 1 {
		select {
		case <-acked:
		case <-time.After(5 * time.Second):
			t.Fatal("the other shards are blocked")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// After a failed write, the record and those behind it are sent again in
// order, each counted once.
func TestRunRetriesFromFirstUnacked(t *testing.T) {
	fastRetries(t)
	broker := &fakeBroker{code: failOnce(`{"key":"a","n":2}`, codes.Internal)}
	in := "{\"key\":\"a\",\"n\":1}\n{\"key\":\"a\",\"n\":2}\n{\"key\":\"a\",\"n\":3}\n"

	sent, err := run(t.Context(), newClient(t, broker), "t", "key", t.TempDir(), strings.NewReader(in))
	if err != nil || sent != 3 {
		t.Fatalf("run = %d, %v, want 3, nil", sent, err)
	}
	for _, req := range broker.records() {
		var n uint64
		fmt.Sscanf(req.payload, `{"key":"a","n":%d}`, &n)
		if req.sequence != n {
			t.Errorf("%s has sequence %d, want it to keep %d", req.payload, req.sequence, n)
		}
	}
	first := broker.records()[0]
	for _, req := range broker.records()[1:] {
		if req.producer != first.producer {
			t.Error("a retry changed the producer id")
		}
	}
	if broker.streams <= testShards {
		t.Errorf("%d streams, want a new one for the retry", broker.streams)
	}
}

// ALREADY_EXISTS means an earlier attempt stored the record.
func TestRunCountsDuplicateAsStored(t *testing.T) {
	broker := &fakeBroker{code: func(*pb.PublishRequest) codes.Code { return codes.AlreadyExists }}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", t.TempDir(), strings.NewReader("{\"n\":1}\n"))
	if err != nil || sent != 1 {
		t.Errorf("run = %d, %v, want 1, nil", sent, err)
	}
}

// A broker that stays down is retried until it comes back.
func TestRunKeepsRetrying(t *testing.T) {
	fastRetries(t)
	var mu sync.Mutex
	failures := 0
	broker := &fakeBroker{code: func(*pb.PublishRequest) codes.Code {
		mu.Lock()
		defer mu.Unlock()
		failures++
		if failures <= 6 {
			return codes.Unavailable
		}
		return codes.OK
	}}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", t.TempDir(), strings.NewReader("{\"n\":1}\n"))
	if err != nil || sent != 1 {
		t.Fatalf("run = %d, %v, want 1, nil", sent, err)
	}
}

func TestDelayDoublesUpToLimit(t *testing.T) {
	if got := nextDelay(time.Second); got != 2*time.Second {
		t.Errorf("nextDelay(1s) = %v, want 2s", got)
	}
	if got := nextDelay(maxRetryDelay); got != maxRetryDelay {
		t.Errorf("nextDelay(limit) = %v, want the limit", got)
	}
}

// Ctrl+C stops the retries.
func TestRunCancelStopsRetries(t *testing.T) {
	fastRetries(t)
	ctx, cancel := context.WithCancel(t.Context())
	broker := &fakeBroker{code: func(*pb.PublishRequest) codes.Code {
		cancel()
		return codes.Unavailable
	}}
	if _, err := run(ctx, newClient(t, broker), "t", "", t.TempDir(), strings.NewReader("{\"n\":1}\n")); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want Canceled", err)
	}
}

func spoolFiles(t *testing.T, spool string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(spool, "t", "*"+spoolExt))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// A line is on disk before the broker sees it, and is gone once acked.
func TestRunSpoolsBeforeSending(t *testing.T) {
	spool := t.TempDir()
	var seen []string
	broker := &fakeBroker{}
	broker.code = func(*pb.PublishRequest) codes.Code {
		seen = spoolFiles(t, spool)
		return codes.OK
	}
	if _, err := run(t.Context(), newClient(t, broker), "t", "", spool, strings.NewReader("{\"n\":1}\n")); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 {
		t.Errorf("%d spool files while sending, want 1", len(seen))
	}
	if left := spoolFiles(t, spool); len(left) != 0 {
		t.Errorf("spool files left after the acks: %v", left)
	}
}

// A long run uses several small files, and deletes them as it goes.
func TestRunSplitsBatches(t *testing.T) {
	spool := t.TempDir()
	maxFiles := 0
	broker := &fakeBroker{}
	broker.code = func(*pb.PublishRequest) codes.Code {
		maxFiles = max(maxFiles, len(spoolFiles(t, spool)))
		return codes.OK
	}
	line := `{"key":"a","pad":"` + strings.Repeat("x", 1000) + `"}` + "\n"
	const lines = 3 * maxFileBytes / 1000
	sent, err := run(t.Context(), newClient(t, broker), "t", "key", spool, strings.NewReader(strings.Repeat(line, lines)))
	if err != nil || sent != lines {
		t.Fatalf("run = %d, %v", sent, err)
	}
	if left := spoolFiles(t, spool); len(left) != 0 {
		t.Errorf("spool files left: %v", left)
	}
}

// What a run couldn't send, the next one sends, with the same producer id
// and sequences so that the broker can drop what it already has.
func TestRunResendsWhatAnEarlierRunLeft(t *testing.T) {
	spool := t.TempDir()
	in := "{\"key\":\"a\",\"n\":1}\n{\"key\":\"a\",\"n\":2}\n"
	const lost = `{"key":"a","n":2}`
	first := &fakeBroker{code: func(req *pb.PublishRequest) codes.Code {
		if hasPayload(req, lost) {
			return codes.PermissionDenied
		}
		return codes.OK
	}}
	if _, err := run(t.Context(), newClient(t, first), "t", "key", spool, strings.NewReader(in)); err == nil {
		t.Fatal("the first run succeeded")
	}
	if len(spoolFiles(t, spool)) != 1 {
		t.Fatalf("spool files = %v, want 1", spoolFiles(t, spool))
	}

	second := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, second), "t", "key", spool, strings.NewReader(`{"key":"a","n":3}`+"\n"))
	if err != nil || sent != 3 {
		t.Fatalf("run = %d, %v, want 3 (the file holds lines 1 and 2)", sent, err)
	}
	var old record
	for _, req := range first.records() {
		if req.payload == lost {
			old = req
		}
	}
	found := false
	for _, req := range second.records() {
		if req.payload == lost {
			found = true
			if req.producer != old.producer || req.sequence != old.sequence || req.shard != old.shard {
				t.Errorf("resent %v, want the identity of %v", req, old)
			}
		}
	}
	if !found {
		t.Error("line 2 wasn't resent")
	}
	if left := spoolFiles(t, spool); len(left) != 0 {
		t.Errorf("spool files left: %v", left)
	}
}

// With the broker down, nothing is lost: stopping leaves the line on disk.
func TestRunKeepsLinesWhenStopped(t *testing.T) {
	fastRetries(t)
	spool := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	broker := &fakeBroker{code: func(*pb.PublishRequest) codes.Code {
		cancel()
		return codes.Unavailable
	}}
	run(ctx, newClient(t, broker), "t", "", spool, strings.NewReader("{\"n\":1}\n"))
	if len(spoolFiles(t, spool)) != 1 {
		t.Errorf("spool files = %v, want the line kept", spoolFiles(t, spool))
	}
}

// A running producer's id is locked: another producer leaves its batches
// alone, and uses an id of its own.
func TestRunSkipsIdInUse(t *testing.T) {
	spool := t.TempDir()
	dir := filepath.Join(spool, "t")
	if err := saveShardCount(dir, testShards); err != nil {
		t.Fatal(err)
	}
	l, err := openLease(filepath.Join(dir, "0000000000000009"+producerExt), testShards)
	if err != nil {
		t.Fatal(err)
	}
	defer l.file.Close()
	req := &pb.PublishRequest{Topic: "t", ProducerId: 9, FirstSequence: 1, Payloads: [][]byte{[]byte(`{"n":1}`)}}
	b, err := newBatch(dir, l, req)
	if err != nil {
		t.Fatal(err)
	}
	defer b.file.Close()
	if err := b.write(req); err != nil {
		t.Fatal(err)
	}

	broker := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", spool, strings.NewReader("{\"n\":2}\n"))
	if err != nil || sent != 1 {
		t.Errorf("run = %d, %v, want 1, nil", sent, err)
	}
	if id := broker.records()[0].producer; id == 9 {
		t.Error("used the id of a running producer")
	}
	if len(spoolFiles(t, spool)) != 1 {
		t.Error("the file in use was touched")
	}
}

// A crash during a write leaves a torn last line, which was never sent.
func TestRunIgnoresTornLine(t *testing.T) {
	spool := t.TempDir()
	dir := filepath.Join(spool, "t")
	if err := saveShardCount(dir, testShards); err != nil {
		t.Fatal(err)
	}
	l, err := openLease(filepath.Join(dir, "0000000000000009"+producerExt), testShards)
	if err != nil {
		t.Fatal(err)
	}
	l.file.Close()
	req := &pb.PublishRequest{Topic: "t", Shard: 2, ProducerId: 9, FirstSequence: 1, Payloads: [][]byte{[]byte(`{"n":1}`)}}
	b, err := newBatch(dir, l, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.write(req); err != nil {
		t.Fatal(err)
	}
	if _, err := b.file.Write([]byte{0, 0, 0, 50, 1, 2, 3}); err != nil { // says 50 bytes, has 3
		t.Fatal(err)
	}
	b.file.Close()

	broker := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", spool, strings.NewReader(""))
	if err != nil || sent != 1 {
		t.Fatalf("run = %d, %v, want 1, nil", sent, err)
	}
	if got := broker.records()[0]; got.shard != 2 || got.producer != 9 || string(got.payload) != `{"n":1}` {
		t.Errorf("sent %v", got)
	}
	if left := spoolFiles(t, spool); len(left) != 0 {
		t.Errorf("spool files left: %v", left)
	}
}

// An error a retry can't fix stops the producer and names the line.
func TestRunStopsOnBrokerError(t *testing.T) {
	broker := &fakeBroker{code: func(req *pb.PublishRequest) codes.Code {
		return codes.PermissionDenied
	}}

	sent, err := run(t.Context(), newClient(t, broker), "t", "key", t.TempDir(), strings.NewReader("{\"key\":\"a\"}\n"))
	if status.Code(errors.Unwrap(err)) != codes.PermissionDenied || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("err = %v, want PermissionDenied error mentioning line 1", err)
	}
	if sent != 0 {
		t.Errorf("sent = %d, want 0", sent)
	}
	if got := len(broker.records()); got != 1 {
		t.Errorf("sent %d times, want no retry", got)
	}
}

func TestRunCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	sent, err := run(ctx, newClient(t, &fakeBroker{}), "t", "", t.TempDir(), strings.NewReader("{\"n\":1}\n"))
	if status.Code(errors.Unwrap(err)) != codes.Canceled && !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want Canceled", err)
	}
	if sent != 0 {
		t.Errorf("sent = %d, want 0", sent)
	}
}

// The longest line stdin accepts, in the worst case for JSON escaping, can
// still be read back from the spool.
func TestRunResendsLongestLine(t *testing.T) {
	spool := t.TempDir()
	line := `{"p":"` + strings.Repeat("<", bufio.MaxScanTokenSize-16) + `"}`
	first := &fakeBroker{code: func(*pb.PublishRequest) codes.Code { return codes.PermissionDenied }}
	if _, err := run(t.Context(), newClient(t, first), "t", "", spool, strings.NewReader(line+"\n")); err == nil {
		t.Fatal("the first run succeeded")
	}

	second := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, second), "t", "", spool, strings.NewReader(""))
	if err != nil || sent != 1 {
		t.Fatalf("run = %d, %v, want 1, nil", sent, err)
	}
	if got := second.records()[0].payload; string(got) != line {
		t.Errorf("resent %d bytes, want %d", len(got), len(line))
	}
}

// Runs share a producer id and carry on counting, so the broker tracks one
// producer however many runs there are, and nothing is dropped as a repeat.
func TestRunsReuseIdAndSequences(t *testing.T) {
	spool := t.TempDir()
	broker := &fakeBroker{}
	client := newClient(t, broker)
	for range 3 {
		if _, err := run(t.Context(), client, "t", "key", spool, strings.NewReader(`{"key":"a"}`+"\n")); err != nil {
			t.Fatal(err)
		}
	}
	reqs := broker.records()
	for i, req := range reqs {
		if req.producer != reqs[0].producer {
			t.Errorf("run %d used another id", i)
		}
		if req.sequence != uint64(i+1) {
			t.Errorf("run %d used sequence %d, want %d", i, req.sequence, i+1)
		}
	}
}

// Producers running at the same time have different ids.
func TestConcurrentRunsUseDifferentIds(t *testing.T) {
	dir := t.TempDir()
	a, err := takeLeases(dir, testShards)
	if err != nil {
		t.Fatal(err)
	}
	defer closeLeases(a)
	b, err := takeLeases(dir, testShards)
	if err != nil {
		t.Fatal(err)
	}
	defer closeLeases(b)
	if a[0].id == b[0].id {
		t.Error("both got the same id")
	}
}

// After a crash, sequences carry on past the leftover records, not just past
// the acked ones.
func TestRunCarriesOnPastLeftovers(t *testing.T) {
	spool := t.TempDir()
	line := `{"key":"a"}` + "\n"
	first := &fakeBroker{code: func(*pb.PublishRequest) codes.Code { return codes.PermissionDenied }}
	run(t.Context(), newClient(t, first), "t", "key", spool, strings.NewReader(line+line))

	second := &fakeBroker{}
	if _, err := run(t.Context(), newClient(t, second), "t", "key", spool, strings.NewReader(line)); err != nil {
		t.Fatal(err)
	}
	var sequences []uint64
	for _, req := range second.records() {
		sequences = append(sequences, req.sequence)
	}
	if fmt.Sprint(sequences) != "[1 2 3]" {
		t.Errorf("sequences = %v, want [1 2 3]", sequences)
	}
}

// Once the topic is known, a run saves its lines with the broker down.
func TestRunStartsWithBrokerDown(t *testing.T) {
	fastRetries(t)
	spool := t.TempDir()
	broker := &fakeBroker{}
	client := newClient(t, broker)
	if _, err := run(t.Context(), client, "t", "", spool, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}

	broker.mu.Lock()
	broker.describeErr = status.Error(codes.Unavailable, "down")
	broker.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	broker.code = func(*pb.PublishRequest) codes.Code {
		cancel()
		return codes.Unavailable
	}
	run(ctx, client, "t", "", spool, strings.NewReader("{\"n\":1}\n"))
	if len(spoolFiles(t, spool)) != 1 {
		t.Errorf("spool files = %v, want the line saved", spoolFiles(t, spool))
	}
	if broker.describes != 1 {
		t.Errorf("DescribeTopic called %d times, want once", broker.describes)
	}
}

// A bad topic name is refused before anything is written.
func TestRunBadTopicLeavesNoFiles(t *testing.T) {
	spool := t.TempDir()
	broker := &fakeBroker{describeErr: status.Error(codes.InvalidArgument, "bad topic")}
	if _, err := run(t.Context(), newClient(t, broker), "t", "", spool, strings.NewReader("")); status.Code(errors.Unwrap(err)) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	if entries, _ := os.ReadDir(spool); len(entries) != 0 {
		t.Errorf("spool has %v", entries)
	}
}

// The broker being out of space is waited out, not fatal.
func TestRunRetriesWhenBrokerFull(t *testing.T) {
	fastRetries(t)
	broker := &fakeBroker{code: failOnce(`{"n":1}`, codes.ResourceExhausted)}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", t.TempDir(), strings.NewReader("{\"n\":1}\n"))
	if err != nil || sent != 1 {
		t.Errorf("run = %d, %v, want 1, nil", sent, err)
	}
}

// Lines waiting together go in one request.
func TestCollectGroupsWaitingLines(t *testing.T) {
	s := &sender{topic: "t", shard: 3, lease: &lease{id: 9}, lines: make(chan line, maxBatchLines+1)}
	for i := range uint64(maxBatchLines + 1) {
		s.lines <- line{i + 1, i + 1, []byte(`{}`)}
	}
	req := s.collect(<-s.lines)
	if len(req.GetPayloads()) != maxBatchLines || req.GetFirstSequence() != 1 || req.GetShard() != 3 || req.GetProducerId() != 9 {
		t.Errorf("request has %d lines, sequence %d, shard %d, id %d", len(req.GetPayloads()), req.GetFirstSequence(), req.GetShard(), req.GetProducerId())
	}
	if len(s.lines) != 0 {
		next := s.collect(<-s.lines)
		if len(next.GetPayloads()) != 1 || next.GetFirstSequence() != maxBatchLines+1 {
			t.Errorf("next request = %v", next)
		}
	}
}

// A request stops growing at the byte limit too.
func TestCollectStopsAtByteLimit(t *testing.T) {
	s := &sender{lease: &lease{}, lines: make(chan line, 3)}
	big := make([]byte, maxBatchBytes)
	s.lines <- line{2, 2, big}
	if got := s.collect(line{1, 1, big}); len(got.GetPayloads()) != 1 {
		t.Errorf("request has %d lines, want 1", len(got.GetPayloads()))
	}
}

// A full spool file is read back as the same requests, in order.
func TestSpoolRoundTrip(t *testing.T) {
	dir := t.TempDir()
	l := &lease{id: 9}
	reqs := []*pb.PublishRequest{
		{RequestId: 1, Topic: "t", Shard: 2, ProducerId: 9, FirstSequence: 1, Payloads: [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`)}},
		{RequestId: 3, Topic: "t", Shard: 2, ProducerId: 9, FirstSequence: 3, Payloads: [][]byte{[]byte(`{"n":3}`)}},
	}
	b, err := newBatch(dir, l, reqs[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range reqs {
		if err := b.write(req); err != nil {
			t.Fatal(err)
		}
	}
	b.file.Close()
	if b.last != 3 {
		t.Errorf("last = %d, want 3", b.last)
	}

	file, err := os.Open(b.path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got := readBatch(file)
	if len(got) != 2 || !proto.Equal(got[0], reqs[0]) || !proto.Equal(got[1], reqs[1]) {
		t.Errorf("read %v", got)
	}
}

// The producer alone, against a broker that acks at once.
func BenchmarkRun(b *testing.B) {
	slog.SetDefault(slog.New(slog.DiscardHandler))
	client := newClient(b, &ackBroker{})
	line := `{"device":"sensor-42","temp":21.5,"humidity":48,"status":"ok","ts":1793137200123}` + "\n"
	in := strings.Repeat(line, b.N)
	b.SetBytes(int64(len(line)))
	b.ResetTimer()
	if _, err := run(b.Context(), client, "t", "", b.TempDir(), strings.NewReader(in)); err != nil {
		b.Fatal(err)
	}
}

// ackBroker acks every request without keeping anything.
type ackBroker struct {
	pb.UnimplementedEventBrokerServer
}

func (ackBroker) DescribeTopic(_ context.Context, req *pb.DescribeTopicRequest) (*pb.TopicInfo, error) {
	return &pb.TopicInfo{Topic: req.GetTopic(), NumShards: testShards}, nil
}

func (ackBroker) Publish(stream pb.EventBroker_PublishServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := stream.Send(&pb.PublishAck{RequestId: req.GetRequestId(), Shard: req.GetShard()}); err != nil {
			return err
		}
	}
}

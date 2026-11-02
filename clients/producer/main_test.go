package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
)

const testShards = 5

// fakeBroker records every published request. code decides the ack's error
// code for each one; nil accepts everything.
type fakeBroker struct {
	pb.UnimplementedEventBrokerServer
	code func(req *pb.PublishRequest) codes.Code

	mu       sync.Mutex
	received []*pb.PublishRequest
	streamOf []int // stream of each received request
	streams  int
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
		if string(req.GetPayload()) == payload && !failed {
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
		f.received = append(f.received, req)
		f.streamOf = append(f.streamOf, id)
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

func (f *fakeBroker) requests() []*pb.PublishRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.PublishRequest{}, f.received...)
}

func (f *fakeBroker) payloads() string {
	var out []string
	for _, req := range f.requests() {
		out = append(out, string(req.GetPayload()))
	}
	slices.Sort(out) // shards are sent independently
	return strings.Join(out, ",")
}

// newClient serves broker on an in-memory listener and returns a client for it.
func newClient(t *testing.T, broker pb.EventBrokerServer) pb.EventBrokerClient {
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

	sent, err := run(t.Context(), newClient(t, broker), "sensors", "", strings.NewReader(in))
	if err != nil || sent != 3 {
		t.Fatalf("run = %d, %v, want 3, nil", sent, err)
	}
	if got, want := broker.payloads(), `{"n":1},{"n":2},{"n":3}`; got != want {
		t.Errorf("broker got %s, want %s", got, want)
	}
	for _, req := range broker.requests() {
		if req.GetTopic() != "sensors" {
			t.Errorf("topic = %q, want sensors", req.GetTopic())
		}
	}
}

// Each line goes to the shard of its key.
func TestRunHashesKeyToShard(t *testing.T) {
	broker := &fakeBroker{}
	in := `{"dev":"a","n":1}` + "\n" + `{"dev":42}` + "\n"

	if _, err := run(t.Context(), newClient(t, broker), "t", "dev", strings.NewReader(in)); err != nil {
		t.Fatal(err)
	}
	want := []int{
		partition.Shard([]byte("a"), testShards),  // string: without quotes
		partition.Shard([]byte("42"), testShards), // number: as JSON text
	}
	for _, req := range broker.requests() {
		if line := req.GetRequestId(); int(req.GetShard()) != want[line-1] {
			t.Errorf("line %d went to shard %d, want %d", line, req.GetShard(), want[line-1])
		}
	}
}

func TestRunEmptyInput(t *testing.T) {
	broker := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", strings.NewReader(""))
	if err != nil || sent != 0 {
		t.Errorf("run = %d, %v, want 0, nil", sent, err)
	}
}

// Lines without a key take turns over the shards, with or without -key-field.
func TestRunRoundRobinWithoutKey(t *testing.T) {
	for _, keyField := range []string{"", "dev"} {
		broker := &fakeBroker{}
		in := strings.Repeat(`{"n":1}`+"\n", testShards*2)
		if _, err := run(t.Context(), newClient(t, broker), "t", keyField, strings.NewReader(in)); err != nil {
			t.Fatal(err)
		}
		perShard := map[int32]int{}
		for _, req := range broker.requests() {
			perShard[req.GetShard()]++
			if want := int32(req.GetRequestId()-1) % testShards; req.GetShard() != want {
				t.Errorf("line %d went to shard %d, want %d", req.GetRequestId(), req.GetShard(), want)
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
	sent, err := run(t.Context(), newClient(t, broker), "t", "key", strings.NewReader(
		"{\"key\":\"a\",\"n\":1}\nbad\n{\"key\":\"a\",\"n\":2}\n"))
	if err != nil || sent != 2 {
		t.Fatalf("run = %d, %v, want 2, nil", sent, err)
	}
	var sequences []uint64
	for _, req := range broker.requests() {
		sequences = append(sequences, req.GetSequence())
	}
	if fmt.Sprint(sequences) != "[1 2]" {
		t.Errorf("sequences = %v, want [1 2]", sequences)
	}
}

// Each shard has its own stream and its own sequence, under one producer id.
func TestRunOneStreamPerShard(t *testing.T) {
	broker := &fakeBroker{}
	in := strings.Repeat(`{"n":1}`+"\n", testShards*3)
	if _, err := run(t.Context(), newClient(t, broker), "t", "", strings.NewReader(in)); err != nil {
		t.Fatal(err)
	}

	streamShard := map[int]int32{}
	lastSequence := map[int32]uint64{}
	producers := map[uint64]bool{}
	for i, req := range broker.requests() {
		stream := broker.streamOf[i]
		if shard, ok := streamShard[stream]; ok && shard != req.GetShard() {
			t.Errorf("stream %d has shards %d and %d", stream, shard, req.GetShard())
		}
		streamShard[stream] = req.GetShard()
		lastSequence[req.GetShard()]++
		if req.GetSequence() != lastSequence[req.GetShard()] {
			t.Errorf("shard %d: sequence %d, want %d", req.GetShard(), req.GetSequence(), lastSequence[req.GetShard()])
		}
		producers[req.GetProducerId()] = true
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
		_, err := run(t.Context(), client, "t", "", strings.NewReader(in))
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

	sent, err := run(t.Context(), newClient(t, broker), "t", "key", strings.NewReader(in))
	if err != nil || sent != 3 {
		t.Fatalf("run = %d, %v, want 3, nil", sent, err)
	}
	var last [4]uint64
	for _, req := range broker.requests() {
		last[req.GetRequestId()] = req.GetSequence()
	}
	if last != [4]uint64{0, 1, 2, 3} {
		t.Errorf("sequences by line = %v, want each line to keep its sequence", last)
	}
	first := broker.requests()[0]
	for _, req := range broker.requests()[1:] {
		if req.GetProducerId() != first.GetProducerId() {
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
	sent, err := run(t.Context(), newClient(t, broker), "t", "", strings.NewReader("{\"n\":1}\n"))
	if err != nil || sent != 1 {
		t.Errorf("run = %d, %v, want 1, nil", sent, err)
	}
}

func TestRunGivesUpAfterRetries(t *testing.T) {
	fastRetries(t)
	broker := &fakeBroker{code: func(*pb.PublishRequest) codes.Code { return codes.Unavailable }}
	sent, err := run(t.Context(), newClient(t, broker), "t", "", strings.NewReader("{\"n\":1}\n"))
	if status.Code(errors.Unwrap(err)) != codes.Unavailable || sent != 0 {
		t.Fatalf("run = %d, %v, want 0, Unavailable", sent, err)
	}
	if got := len(broker.requests()); got != maxRetries {
		t.Errorf("sent %d times, want %d", got, maxRetries)
	}
}

// An error a retry can't fix stops the producer and names the line.
func TestRunStopsOnBrokerError(t *testing.T) {
	broker := &fakeBroker{code: func(req *pb.PublishRequest) codes.Code {
		if req.GetRequestId() == 2 {
			return codes.ResourceExhausted
		}
		return codes.OK
	}}

	sent, err := run(t.Context(), newClient(t, broker), "t", "key", strings.NewReader("{\"key\":\"a\"}\n{\"key\":\"a\"}\n"))
	if status.Code(errors.Unwrap(err)) != codes.ResourceExhausted || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("err = %v, want ResourceExhausted error mentioning line 2", err)
	}
	if sent != 1 {
		t.Errorf("sent = %d, want 1", sent)
	}
	if got := len(broker.requests()); got != 2 {
		t.Errorf("sent %d requests, want no retry", got)
	}
}

func TestRunCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	sent, err := run(ctx, newClient(t, &fakeBroker{}), "t", "", strings.NewReader("{\"n\":1}\n"))
	if status.Code(errors.Unwrap(err)) != codes.Canceled && !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want Canceled", err)
	}
	if sent != 0 {
		t.Errorf("sent = %d, want 0", sent)
	}
}

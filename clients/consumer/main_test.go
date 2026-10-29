package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	pb "wisp/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeBroker sends the assignments put on assign to the consumer that
// joins. Subscribe sends one event of the shard asked for (unless
// subscribeErr is set), then waits until the consumer cancels; closed gets
// the shard number when it does.
type fakeBroker struct {
	pb.UnimplementedEventBrokerServer
	assign       chan *pb.Assignment
	joinErr      error
	subscribeErr error
	closed       chan int32

	mu         sync.Mutex
	join       *pb.JoinRequest
	subscribes []*pb.SubscribeRequest
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{assign: make(chan *pb.Assignment, 10), closed: make(chan int32, 10)}
}

func (f *fakeBroker) Join(req *pb.JoinRequest, stream pb.EventBroker_JoinServer) error {
	f.mu.Lock()
	f.join = req
	f.mu.Unlock()
	if f.joinErr != nil {
		return f.joinErr
	}
	for {
		select {
		case a, ok := <-f.assign:
			if !ok {
				return nil // ends the stream
			}
			if err := stream.Send(a); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (f *fakeBroker) Subscribe(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeServer) error {
	f.mu.Lock()
	f.subscribes = append(f.subscribes, req)
	f.mu.Unlock()
	if f.subscribeErr != nil {
		return f.subscribeErr
	}
	event := &pb.Event{EventId: 0, Timestamp: testTimestamp, Payload: fmt.Appendf(nil, `{"shard":%d}`, req.GetShard())}
	if err := stream.Send(&pb.EventBatch{Topic: req.GetTopic(), Shard: req.GetShard(), Events: []*pb.Event{event}}); err != nil {
		return err
	}
	<-stream.Context().Done()
	f.closed <- req.GetShard()
	return stream.Context().Err()
}

// subscribedShards returns the shard of each Subscribe call, in order.
func (f *fakeBroker) subscribedShards() []int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var shards []int32
	for _, req := range f.subscribes {
		shards = append(shards, req.GetShard())
	}
	return shards
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

// lineWriter sends every line written to it on lines.
type lineWriter struct{ lines chan string }

func (w lineWriter) Write(p []byte) (int, error) {
	w.lines <- strings.TrimSuffix(string(p), "\n")
	return len(p), nil
}

// failingWriter fails every write, like a closed stdout pipe.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

type result struct {
	printed int
	err     error
}

// start runs the consumer in the background.
func start(t *testing.T, ctx context.Context, broker *fakeBroker, opts options, out io.Writer) chan result {
	t.Helper()
	done := make(chan result, 1)
	client := newClient(t, broker)
	go func() {
		printed, err := run(ctx, client, opts, out)
		done <- result{printed, err}
	}()
	return done
}

func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// 2026-10-27T21:40:00.123Z in Unix milliseconds.
const testTimestamp = 1793137200123

var testOpts = options{topic: "t", id: "a"}

func TestFormatEvent(t *testing.T) {
	got := formatEvent(&pb.EventBatch{Topic: "s", Shard: 2}, &pb.Event{EventId: 7, Timestamp: testTimestamp, Payload: []byte(`{"a":1}`)})
	want := "s/2/7\t2026-10-27T21:40:00.123Z\t{\"a\":1}"
	if got != want {
		t.Errorf("formatEvent = %q, want %q", got, want)
	}
}

// The consumer joins, reads the shards it's assigned, stops reading a shard
// that is taken away, and exits cleanly on Ctrl+C.
func TestRunFollowsAssignments(t *testing.T) {
	broker := newFakeBroker()
	out := lineWriter{lines: make(chan string, 10)}
	ctx, cancel := context.WithCancel(t.Context())
	opts := options{topic: "t", id: "a", offset: 5, maxBytes: 100}
	done := start(t, ctx, broker, opts, out)

	broker.assign <- &pb.Assignment{Topic: "t", Generation: 1, Shards: []int32{0, 2}}
	got := map[string]bool{}
	for range 2 {
		got[waitFor(t, out.lines, "event")] = true
	}
	for _, shard := range []int{0, 2} {
		line := fmt.Sprintf("t/%d/0\t2026-10-27T21:40:00.123Z\t{\"shard\":%d}", shard, shard)
		if !got[line] {
			t.Errorf("missing line %q, got %v", line, got)
		}
	}

	broker.mu.Lock()
	if broker.join.GetTopic() != "t" || broker.join.GetConsumerId() != "a" {
		t.Errorf("join request = %v", broker.join)
	}
	for _, req := range broker.subscribes {
		if req.GetStartId() != 5 || req.GetMaxBytes() != 100 || req.GetLatest() {
			t.Errorf("subscribe request = %v, want the options", req)
		}
	}
	broker.mu.Unlock()

	// Shard 0 is taken away: its subscription is cancelled.
	broker.assign <- &pb.Assignment{Topic: "t", Generation: 2, Shards: []int32{2}}
	if shard := waitFor(t, broker.closed, "shard 0 to be closed"); shard != 0 {
		t.Errorf("closed shard %d, want 0", shard)
	}

	cancel()
	r := waitFor(t, done, "run to return")
	if r.err != nil || r.printed != 2 {
		t.Errorf("run = %d, %v, want 2, nil", r.printed, r.err)
	}
}

// A shard that stays assigned isn't subscribed again; a new one is.
func TestRunSubscribesOnlyNewShards(t *testing.T) {
	broker := newFakeBroker()
	out := lineWriter{lines: make(chan string, 10)}
	ctx, cancel := context.WithCancel(t.Context())
	done := start(t, ctx, broker, testOpts, out)

	broker.assign <- &pb.Assignment{Generation: 1, Shards: []int32{0}}
	waitFor(t, out.lines, "event of shard 0")
	broker.assign <- &pb.Assignment{Generation: 2, Shards: []int32{0, 1}}
	waitFor(t, out.lines, "event of shard 1")

	if got := fmt.Sprint(broker.subscribedShards()); got != "[0 1]" {
		t.Errorf("subscribed to %s, want [0 1]", got)
	}
	cancel()
	waitFor(t, done, "run to return")
}

// The broker ending the Join stream is a clean exit.
func TestRunEndsWithJoinStream(t *testing.T) {
	broker := newFakeBroker()
	done := start(t, t.Context(), broker, testOpts, lineWriter{lines: make(chan string, 10)})
	close(broker.assign)

	if r := waitFor(t, done, "run to return"); r.err != nil {
		t.Errorf("run = %v, want nil", r.err)
	}
}

func TestRunReturnsJoinError(t *testing.T) {
	broker := newFakeBroker()
	broker.joinErr = status.Error(codes.AlreadyExists, "consumer already joined")
	r := waitFor(t, start(t, t.Context(), broker, testOpts, failingWriter{}), "run to return")
	if status.Code(errors.Unwrap(r.err)) != codes.AlreadyExists {
		t.Errorf("err = %v, want AlreadyExists", r.err)
	}
}

// A shard that can't be read stops the consumer.
func TestRunReturnsShardError(t *testing.T) {
	broker := newFakeBroker()
	broker.subscribeErr = status.Error(codes.OutOfRange, "start_id is after the end of the shard")
	done := start(t, t.Context(), broker, testOpts, failingWriter{})
	broker.assign <- &pb.Assignment{Generation: 1, Shards: []int32{3}}

	r := waitFor(t, done, "run to return")
	if r.err == nil || !strings.Contains(r.err.Error(), "t/3") || !strings.Contains(r.err.Error(), "OutOfRange") {
		t.Errorf("err = %v, want OutOfRange for t/3", r.err)
	}
}

func TestRunReturnsWriteError(t *testing.T) {
	broker := newFakeBroker()
	done := start(t, t.Context(), broker, testOpts, failingWriter{})
	broker.assign <- &pb.Assignment{Generation: 1, Shards: []int32{0}}

	r := waitFor(t, done, "run to return")
	if r.err == nil || !strings.Contains(r.err.Error(), "broken pipe") {
		t.Errorf("err = %v, want the write error", r.err)
	}
}

package main

import (
	"bytes"
	"context"
	"errors"
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

// fakeBroker answers Subscribe with the subscribe func.
type fakeBroker struct {
	pb.UnimplementedEventBrokerServer
	subscribe func(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeServer) error
}

func (f *fakeBroker) Subscribe(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeServer) error {
	return f.subscribe(req, stream)
}

// batch builds a batch of shard 0 of topic t with one event.
func batch(id uint64, payload string) *pb.EventBatch {
	return &pb.EventBatch{Topic: "t", Shard: 0, Events: []*pb.Event{{EventId: id, Payload: []byte(payload)}}}
}

var testOpts = options{topic: "t", shard: 0}

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

// 2026-10-27T21:40:00.123Z in Unix milliseconds.
const testTimestamp = 1793137200123

func TestFormatEvent(t *testing.T) {
	got := formatEvent(&pb.EventBatch{Topic: "s", Shard: 2}, &pb.Event{EventId: 7, Timestamp: testTimestamp, Payload: []byte(`{"a":1}`)})
	want := "s/2/7\t2026-10-27T21:40:00.123Z\t{\"a\":1}"
	if got != want {
		t.Errorf("formatEvent = %q, want %q", got, want)
	}
}

// The broker sends two batches (3 events), then ends the stream: all are
// printed, the request carries the options, and run returns cleanly.
func TestRunPrintsEvents(t *testing.T) {
	var got *pb.SubscribeRequest
	broker := &fakeBroker{subscribe: func(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeServer) error {
		got = req
		batches := []*pb.EventBatch{
			{Topic: "sensors", Shard: 3, Events: []*pb.Event{
				{EventId: 5, Timestamp: testTimestamp, Payload: []byte(`{"n":1}`)},
				{EventId: 6, Timestamp: testTimestamp, Payload: []byte(`{"n":2}`)},
			}},
			{Topic: "sensors", Shard: 3, Events: []*pb.Event{{EventId: 7, Timestamp: testTimestamp, Payload: []byte(`{"n":3}`)}}},
		}
		for _, b := range batches {
			if err := stream.Send(b); err != nil {
				return err
			}
		}
		return nil // ends the stream: the client sees io.EOF
	}}

	var out bytes.Buffer
	opts := options{topic: "sensors", shard: 3, offset: 5, maxBytes: 100}
	received, err := run(t.Context(), newClient(t, broker), opts, &out)
	if err != nil || received != 3 {
		t.Fatalf("run = %d, %v, want 3, nil", received, err)
	}
	if got.GetTopic() != "sensors" || got.GetShard() != 3 || got.GetStartId() != 5 || got.GetMaxBytes() != 100 || got.GetLatest() {
		t.Errorf("request = %v", got)
	}
	want := "sensors/3/5\t2026-10-27T21:40:00.123Z\t{\"n\":1}\n" +
		"sensors/3/6\t2026-10-27T21:40:00.123Z\t{\"n\":2}\n" +
		"sensors/3/7\t2026-10-27T21:40:00.123Z\t{\"n\":3}\n"
	if out.String() != want {
		t.Errorf("output =\n%s\nwant\n%s", out.String(), want)
	}
}

// signalWriter collects output and signals each write on wrote.
type signalWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	wrote chan struct{}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.wrote <- struct{}{}
	return n, err
}

// Cancelling ctx (Ctrl+C) is a normal shutdown: run returns nil.
func TestRunStopsCleanlyOnCancel(t *testing.T) {
	broker := &fakeBroker{subscribe: func(_ *pb.SubscribeRequest, stream pb.EventBroker_SubscribeServer) error {
		if err := stream.Send(batch(0, `{}`)); err != nil {
			return err
		}
		<-stream.Context().Done() // stay subscribed, like the real broker
		return stream.Context().Err()
	}}
	ctx, cancel := context.WithCancel(t.Context())
	out := &signalWriter{wrote: make(chan struct{}, 1)}

	type result struct {
		received int
		err      error
	}
	done := make(chan result, 1)
	go func() {
		received, err := run(ctx, newClient(t, broker), testOpts, out)
		done <- result{received, err}
	}()

	select {
	case <-out.wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("event was never printed")
	}
	cancel()

	select {
	case r := <-done:
		if r.err != nil || r.received != 1 {
			t.Errorf("run = %d, %v, want 1, nil", r.received, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

// A broker error is returned, not swallowed.
func TestRunReturnsBrokerError(t *testing.T) {
	broker := &fakeBroker{subscribe: func(*pb.SubscribeRequest, pb.EventBroker_SubscribeServer) error {
		return status.Error(codes.Unavailable, "broker shutting down")
	}}

	received, err := run(t.Context(), newClient(t, broker), testOpts, &bytes.Buffer{})
	if status.Code(err) != codes.Unavailable {
		t.Errorf("err = %v, want Unavailable", err)
	}
	if received != 0 {
		t.Errorf("received = %d, want 0", received)
	}
}

// failingWriter fails every write, like a closed stdout pipe.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestRunReturnsWriteError(t *testing.T) {
	broker := &fakeBroker{subscribe: func(_ *pb.SubscribeRequest, stream pb.EventBroker_SubscribeServer) error {
		return stream.Send(batch(0, `{}`))
	}}

	_, err := run(t.Context(), newClient(t, broker), testOpts, failingWriter{})
	if err == nil || !strings.Contains(err.Error(), "broken pipe") {
		t.Errorf("err = %v, want the write error", err)
	}
}

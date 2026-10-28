package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
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

// fakeBroker answers SubscribeTopic with the subscribe func.
type fakeBroker struct {
	pb.UnimplementedEventBrokerServer
	subscribe func(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeTopicServer) error
}

func (f *fakeBroker) SubscribeTopic(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeTopicServer) error {
	return f.subscribe(req, stream)
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

// 2026-10-27T21:40:00.123Z in Unix milliseconds.
const testTimestamp = 1793137200123

func TestFormatEvent(t *testing.T) {
	got := formatEvent(&pb.EventResponse{EventId: "s/0/0", Timestamp: testTimestamp, Payload: []byte(`{"seq_n":0}`)})
	want := "s/0/0\t2026-10-27T21:40:00.123Z\t{\"seq_n\":0}"
	if got != want {
		t.Errorf("formatEvent = %q, want %q", got, want)
	}
}

// The broker sends two events, then ends the stream: both are printed and
// run returns cleanly.
func TestRunPrintsEvents(t *testing.T) {
	var gotTopic string
	broker := &fakeBroker{subscribe: func(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeTopicServer) error {
		gotTopic = req.GetTopic()
		for i, payload := range []string{`{"n":1,"seq_n":0}`, `{"n":2,"seq_n":0}`} {
			id := "sensors/" + strconv.Itoa(i) + "/0"
			if err := stream.Send(&pb.EventResponse{EventId: id, Timestamp: testTimestamp, Payload: []byte(payload)}); err != nil {
				return err
			}
		}
		return nil // ends the stream: the client sees io.EOF
	}}

	var out bytes.Buffer
	received, err := run(t.Context(), newClient(t, broker), "sensors", &out)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if received != 2 {
		t.Errorf("received = %d, want 2", received)
	}
	if gotTopic != "sensors" {
		t.Errorf("subscribed to %q, want sensors", gotTopic)
	}
	want := "sensors/0/0\t2026-10-27T21:40:00.123Z\t{\"n\":1,\"seq_n\":0}\n" +
		"sensors/1/0\t2026-10-27T21:40:00.123Z\t{\"n\":2,\"seq_n\":0}\n"
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
	broker := &fakeBroker{subscribe: func(_ *pb.SubscribeRequest, stream pb.EventBroker_SubscribeTopicServer) error {
		if err := stream.Send(&pb.EventResponse{EventId: "t/0/0", Payload: []byte(`{}`)}); err != nil {
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
		received, err := run(ctx, newClient(t, broker), "t", out)
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
	broker := &fakeBroker{subscribe: func(*pb.SubscribeRequest, pb.EventBroker_SubscribeTopicServer) error {
		return status.Error(codes.Unavailable, "broker shutting down")
	}}

	received, err := run(t.Context(), newClient(t, broker), "t", &bytes.Buffer{})
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
	broker := &fakeBroker{subscribe: func(_ *pb.SubscribeRequest, stream pb.EventBroker_SubscribeTopicServer) error {
		return stream.Send(&pb.EventResponse{EventId: "t/0/0", Payload: []byte(`{}`)})
	}}

	_, err := run(t.Context(), newClient(t, broker), "t", failingWriter{})
	if err == nil || !strings.Contains(err.Error(), "broken pipe") {
		t.Errorf("err = %v, want the write error", err)
	}
}

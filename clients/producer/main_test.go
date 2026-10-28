package main

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	pb "wisp/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeBroker records every published event. reply decides the response to
// each one; nil reply accepts everything.
type fakeBroker struct {
	pb.UnimplementedEventBrokerServer
	reply func(req *pb.EventRequest) error

	mu       sync.Mutex
	received []*pb.EventRequest
}

func (f *fakeBroker) PublishEvent(_ context.Context, req *pb.EventRequest) (*pb.PublishResponse, error) {
	f.mu.Lock()
	f.received = append(f.received, req)
	f.mu.Unlock()
	if f.reply != nil {
		if err := f.reply(req); err != nil {
			return nil, err
		}
	}
	return &pb.PublishResponse{Success: true}, nil
}

// payloads returns the payloads the broker received, in order.
func (f *fakeBroker) payloads() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, req := range f.received {
		out = append(out, string(req.GetPayload()))
	}
	return out
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
	client := newClient(t, broker)
	in := "{\"n\":1}\n\n   \n  {\"n\":2}  \n{\"n\":3}" // blank lines, padding, no final newline

	sent, err := run(t.Context(), client, "sensors", strings.NewReader(in))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if sent != 3 {
		t.Errorf("sent = %d, want 3", sent)
	}
	got, want := broker.payloads(), []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("broker got %v, want %v", got, want)
	}
	for _, req := range broker.received {
		if req.GetTopic() != "sensors" {
			t.Errorf("topic = %q, want sensors", req.GetTopic())
		}
	}
}

func TestRunEmptyInput(t *testing.T) {
	broker := &fakeBroker{}
	sent, err := run(t.Context(), newClient(t, broker), "t", strings.NewReader(""))
	if err != nil || sent != 0 {
		t.Errorf("run = %d, %v, want 0, nil", sent, err)
	}
	if len(broker.payloads()) != 0 {
		t.Errorf("broker got %v, want nothing", broker.payloads())
	}
}

// A rejected event is skipped; the following ones are still published.
func TestRunSkipsInvalidEvents(t *testing.T) {
	broker := &fakeBroker{reply: func(req *pb.EventRequest) error {
		if string(req.GetPayload()) == "bad" {
			return status.Error(codes.InvalidArgument, "payload must be a JSON object")
		}
		return nil
	}}

	sent, err := run(t.Context(), newClient(t, broker), "t", strings.NewReader("{\"n\":1}\nbad\n{\"n\":2}\n"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if sent != 2 {
		t.Errorf("sent = %d, want 2 (bad line not counted)", sent)
	}
	if got := broker.payloads(); len(got) != 3 {
		t.Errorf("broker got %v, want all 3 lines tried", got)
	}
}

// Any other error stops the producer: later lines are not sent.
func TestRunStopsOnBrokerError(t *testing.T) {
	broker := &fakeBroker{reply: func(req *pb.EventRequest) error {
		if string(req.GetPayload()) == `{"n":2}` {
			return status.Error(codes.Internal, "could not write event")
		}
		return nil
	}}

	sent, err := run(t.Context(), newClient(t, broker), "t", strings.NewReader("{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"))
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("err = %v, want Internal error mentioning line 2", err)
	}
	if sent != 1 {
		t.Errorf("sent = %d, want 1", sent)
	}
	if got := broker.payloads(); len(got) != 2 {
		t.Errorf("broker got %v, want line 3 never sent", got)
	}
}

func TestRunCancelledContext(t *testing.T) {
	broker := &fakeBroker{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	sent, err := run(ctx, newClient(t, broker), "t", strings.NewReader("{\"n\":1}\n"))
	if status.Code(err) != codes.Canceled {
		t.Errorf("err = %v, want Canceled", err)
	}
	if sent != 0 {
		t.Errorf("sent = %d, want 0", sent)
	}
}

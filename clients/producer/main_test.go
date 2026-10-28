package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
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
}

func (f *fakeBroker) DescribeTopic(_ context.Context, req *pb.DescribeTopicRequest) (*pb.TopicInfo, error) {
	return &pb.TopicInfo{Topic: req.GetTopic(), NumShards: testShards}, nil
}

func (f *fakeBroker) Publish(stream pb.EventBroker_PublishServer) error {
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
	in := `{"dev":"a","n":1}` + "\n" + `{"dev":42}` + "\n" + `{"other":1}` + "\n"

	if _, err := run(t.Context(), newClient(t, broker), "t", "dev", strings.NewReader(in)); err != nil {
		t.Fatal(err)
	}
	want := []int{
		partition.Shard([]byte("a"), testShards),  // string: without quotes
		partition.Shard([]byte("42"), testShards), // number: as JSON text
		partition.Shard(nil, testShards),          // no field: empty key
	}
	for i, req := range broker.requests() {
		if int(req.GetShard()) != want[i] {
			t.Errorf("line %d went to shard %d, want %d", i+1, req.GetShard(), want[i])
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

// A rejected event is skipped; the following ones are still published.
func TestRunSkipsInvalidEvents(t *testing.T) {
	broker := &fakeBroker{code: func(req *pb.PublishRequest) codes.Code {
		if string(req.GetPayload()) == "bad" {
			return codes.InvalidArgument
		}
		return codes.OK
	}}

	sent, err := run(t.Context(), newClient(t, broker), "t", "", strings.NewReader("{\"n\":1}\nbad\n{\"n\":2}\n"))
	if err != nil || sent != 2 {
		t.Fatalf("run = %d, %v, want 2, nil", sent, err)
	}
	if len(broker.requests()) != 3 {
		t.Errorf("broker got %s, want all 3 lines", broker.payloads())
	}
}

// Any other error ack stops the producer and names the line.
func TestRunStopsOnBrokerError(t *testing.T) {
	broker := &fakeBroker{code: func(req *pb.PublishRequest) codes.Code {
		if string(req.GetPayload()) == `{"n":2}` {
			return codes.Internal
		}
		return codes.OK
	}}

	sent, err := run(t.Context(), newClient(t, broker), "t", "", strings.NewReader("{\"n\":1}\n{\"n\":2}\n"))
	if status.Code(errors.Unwrap(err)) != codes.Internal || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("err = %v, want Internal error mentioning line 2", err)
	}
	if sent != 1 {
		t.Errorf("sent = %d, want 1", sent)
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

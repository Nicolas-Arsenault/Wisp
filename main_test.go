package main

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
	pb "wisp/proto"
	"wisp/stream"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newTestClient starts the broker on an in-memory connection, with its data
// in a temp dir.
func newTestClient(t *testing.T) pb.EventBrokerClient {
	t.Helper()
	topics, err := stream.OpenRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.WaitForHandlers(true))
	pb.RegisterEventBrokerServer(srv, NewBrokerServer(topics))
	go srv.Serve(lis)
	t.Cleanup(func() {
		srv.Stop()
		topics.Close()
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewEventBrokerClient(conn)
}

// publish sends reqs on one Publish stream and returns the acks.
func publish(t *testing.T, client pb.EventBrokerClient, reqs ...*pb.PublishRequest) []*pb.PublishAck {
	t.Helper()
	stream, err := client.Publish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range reqs {
		if err := stream.Send(req); err != nil {
			t.Fatal(err)
		}
	}
	stream.CloseSend()
	var acks []*pb.PublishAck
	for {
		ack, err := stream.Recv()
		if err == io.EOF {
			return acks
		}
		if err != nil {
			t.Fatal(err)
		}
		acks = append(acks, ack)
	}
}

func req(id uint64, shard int32, payload string) *pb.PublishRequest {
	return &pb.PublishRequest{RequestId: id, Topic: "t", Shard: shard, Payload: []byte(payload)}
}

func TestDescribeTopic(t *testing.T) {
	client := newTestClient(t)
	info, err := client.DescribeTopic(t.Context(), &pb.DescribeTopicRequest{Topic: "t"})
	if err != nil || info.GetNumShards() != stream.NumShards {
		t.Errorf("DescribeTopic = %v, %v, want %d shards", info, err, stream.NumShards)
	}
	_, err = client.DescribeTopic(t.Context(), &pb.DescribeTopicRequest{Topic: "../x"})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("bad topic err = %v, want InvalidArgument", err)
	}
}

func TestPublishAcks(t *testing.T) {
	client := newTestClient(t)
	acks := publish(t, client,
		req(1, 0, `{"n":1}`),
		req(2, 0, `not json`),
		req(3, 9, `{"n":3}`),
		req(4, 0, `{"n":4}`),
		req(5, 2, `{"n":5}`),
	)
	if len(acks) != 5 {
		t.Fatalf("got %d acks, want 5", len(acks))
	}
	want := []struct {
		code    codes.Code
		eventID uint64
	}{
		{codes.OK, 0},
		{codes.InvalidArgument, 0},
		{codes.InvalidArgument, 0}, // shard 9 doesn't exist
		{codes.OK, 1},
		{codes.OK, 0}, // first record of shard 2
	}
	for i, ack := range acks {
		if ack.GetRequestId() != uint64(i+1) {
			t.Errorf("ack %d has request id %d", i, ack.GetRequestId())
		}
		if codes.Code(ack.GetErrorCode()) != want[i].code || ack.GetEventId() != want[i].eventID {
			t.Errorf("ack %d = code %d, id %d, want %v, %d", i, ack.GetErrorCode(), ack.GetEventId(), want[i].code, want[i].eventID)
		}
	}
}

// receive reads n events from sub.
func receive(t *testing.T, sub pb.EventBroker_SubscribeClient, n int) []*pb.Event {
	t.Helper()
	var events []*pb.Event
	for len(events) < n {
		batch, err := sub.Recv()
		if err != nil {
			t.Fatalf("Recv after %d events: %v", len(events), err)
		}
		events = append(events, batch.GetEvents()...)
	}
	return events
}

func subscribe(t *testing.T, client pb.EventBrokerClient, req *pb.SubscribeRequest) pb.EventBroker_SubscribeClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	sub, err := client.Subscribe(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

// A consumer from offset 1 gets the stored records, then the new ones, with
// no gap or duplicate.
func TestSubscribeReplaysThenStreams(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{"n":0}`), req(2, 0, `{"n":1}`), req(3, 0, `{"n":2}`))

	sub := subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 0, StartId: 1})
	old := receive(t, sub, 2)

	publish(t, client, req(4, 0, `{"n":3}`))
	live := receive(t, sub, 1)

	var got []string
	for i, e := range append(old, live...) {
		if e.GetEventId() != uint64(i+1) {
			t.Errorf("event %d has id %d", i, e.GetEventId())
		}
		got = append(got, string(e.GetPayload()))
	}
	if want := `{"n":1} {"n":2} {"n":3}`; strings.Join(got, " ") != want {
		t.Errorf("payloads = %v, want %s", got, want)
	}
}

func TestSubscribeLatest(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{"old":true}`))

	sub := subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 0, Latest: true})
	// The broker may read the request after a publish, so keep publishing
	// until the consumer gets something. It must never be the old record.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			stream, err := client.Publish(ctx)
			if err != nil {
				return
			}
			stream.Send(req(2, 0, `{"new":true}`))
			stream.CloseSend()
			stream.Recv()
			time.Sleep(20 * time.Millisecond)
		}
	}()
	e := receive(t, sub, 1)[0]
	if e.GetEventId() == 0 || string(e.GetPayload()) != `{"new":true}` {
		t.Errorf("got event %d %s, want a new one", e.GetEventId(), e.GetPayload())
	}
}

func TestSubscribeMaxBytes(t *testing.T) {
	client := newTestClient(t)
	var reqs []*pb.PublishRequest
	for i := range 10 {
		reqs = append(reqs, req(uint64(i), 0, `{"pad":"0123456789"}`)) // 20 bytes
	}
	publish(t, client, reqs...)

	sub := subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 0, MaxBytes: 50})
	total := 0
	for total < 10 {
		batch, err := sub.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if n := len(batch.GetEvents()); n == 0 || n > 2 {
			t.Errorf("batch has %d events, want 1 or 2 (50 bytes max)", n)
		}
		total += len(batch.GetEvents())
	}
}

func TestSubscribeErrors(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{}`))

	tests := []struct {
		req  *pb.SubscribeRequest
		want codes.Code
	}{
		{&pb.SubscribeRequest{Topic: "nope"}, codes.NotFound},
		{&pb.SubscribeRequest{Topic: "t", Shard: 7}, codes.InvalidArgument},
		{&pb.SubscribeRequest{Topic: "t", StartId: 5}, codes.OutOfRange},
	}
	for _, tt := range tests {
		_, err := subscribe(t, client, tt.req).Recv()
		if status.Code(err) != tt.want {
			t.Errorf("Subscribe(%v) err = %v, want %v", tt.req, err, tt.want)
		}
	}
}

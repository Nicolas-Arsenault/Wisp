package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	pb "wisp/proto"
	"wisp/shards"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// bufSize is the in-memory buffer size of the test gRPC connection.
const bufSize = 1024 * 1024

// useTempDataDir moves the test into a temp working directory. stream writes
// shards to "../data", so they land in <tmp>/data instead of the repo.
// It returns that data directory.
func useTempDataDir(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	work := filepath.Join(root, "work")
	if err := os.Mkdir(work, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(work)
	return filepath.Join(root, "data")
}

// readShard decodes the single record stored in shard id of topic.
func readShard(t *testing.T, dataDir, topic, id string) string {
	t.Helper()
	data, err := shards.ReadFromFile(filepath.Join(dataDir, topic, id))
	if err != nil {
		t.Fatalf("read shard %s/%s: %v", topic, id, err)
	}
	return string(data)
}

// newTestClient starts the broker on an in-memory listener and returns a
// client connected to it, plus the broker itself so tests can inspect it.
// Both are shut down when the test ends.
func newTestClient(t *testing.T) (pb.EventBrokerClient, *Server) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	broker := NewBrokerServer()
	pb.RegisterEventBrokerServer(grpcServer, broker)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

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
	return pb.NewEventBrokerClient(conn), broker
}

// waitForSubscribers waits until topic has n consumers. SubscribeTopic
// registers on the server side after the client call returns, so publishing
// right away could miss a consumer that isn't registered yet.
func waitForSubscribers(t *testing.T, broker *Server, topic string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for broker.subscriberCount(topic) != n {
		if time.Now().After(deadline) {
			t.Fatalf("topic %s has %d consumers, want %d", topic, broker.subscriberCount(topic), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// recvTimeout bounds a subscription in tests, so a missing event fails the
// test with DeadlineExceeded instead of hanging until the test binary times out.
const recvTimeout = 5 * time.Second

// subscribe opens a SubscribeTopic stream for topic and waits until the
// broker registered it as consumer number n. The stream ends with the test,
// or after recvTimeout.
func subscribe(t *testing.T, client pb.EventBrokerClient, broker *Server, topic string, n int) pb.EventBroker_SubscribeTopicClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), recvTimeout)
	t.Cleanup(cancel)
	sub, err := client.SubscribeTopic(ctx, &pb.SubscribeRequest{Topic: topic})
	if err != nil {
		t.Fatalf("SubscribeTopic(%s): %v", topic, err)
	}
	waitForSubscribers(t, broker, topic, n)
	return sub
}

// publish publishes payload on topic and fails the test on error.
func publish(t *testing.T, client pb.EventBrokerClient, topic, payload string) {
	t.Helper()
	if _, err := client.PublishEvent(t.Context(), &pb.EventRequest{Topic: topic, Payload: []byte(payload)}); err != nil {
		t.Fatalf("PublishEvent(%s, %s): %v", topic, payload, err)
	}
}

// recv reads the next event from sub and checks its id and payload.
func recv(t *testing.T, sub pb.EventBroker_SubscribeTopicClient, wantID, wantPayload string) *pb.EventResponse {
	t.Helper()
	event, err := sub.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if event.GetEventId() != wantID || string(event.GetPayload()) != wantPayload {
		t.Errorf("got event %s %s, want %s %s", event.GetEventId(), event.GetPayload(), wantID, wantPayload)
	}
	return event
}

// uniqueTopic returns a topic name no other test or run has used. stream keeps
// its round-robin position per topic for the whole test binary (including
// -count reruns), so a fresh topic is the only way to know it starts at shard 0.
func uniqueTopic() string {
	return "topic-" + strconv.FormatInt(topicCounter.Add(1), 10)
}

// topicCounter backs uniqueTopic; it lives for the whole test binary.
var topicCounter atomic.Int64

func TestPublishEvent(t *testing.T) {
	dataDir := useTempDataDir(t)
	topic := uniqueTopic()
	payload := `{"test":123}`

	resp, err := NewBrokerServer().PublishEvent(context.Background(), &pb.EventRequest{
		Topic:   topic,
		Payload: []byte(payload),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("PublishEvent Success = false, want true")
	}
	// Stored records carry the shard's sequence number (json.Marshal sorts keys).
	want := `{"seq_n":0,"test":123}`
	if got := readShard(t, dataDir, topic, "0"); got != want {
		t.Errorf("shard 0 = %q, want %q", got, want)
	}
}

func TestPublishEventOverGRPC(t *testing.T) {
	dataDir := useTempDataDir(t)
	client, _ := newTestClient(t)
	topic := uniqueTopic()

	// Two events on a fresh topic should go to shards 0 and 1 (round robin).
	payloads := []string{`{"n":1}`, `{"n":2}`}
	for _, p := range payloads {
		resp, err := client.PublishEvent(t.Context(), &pb.EventRequest{
			Topic:   topic,
			Payload: []byte(p),
		})
		if err != nil {
			t.Fatalf("PublishEvent(%s): %v", p, err)
		}
		if !resp.GetSuccess() {
			t.Errorf("PublishEvent(%s) Success = false, want true", p)
		}
	}

	// Each event is the first record of its shard, so both have seq_n 0.
	stored := []string{`{"n":1,"seq_n":0}`, `{"n":2,"seq_n":0}`}
	for i, want := range stored {
		id := strconv.Itoa(i)
		if got := readShard(t, dataDir, topic, id); got != want {
			t.Errorf("shard %s = %q, want %q", id, got, want)
		}
	}
}

func TestPublishEventInvalidPayload(t *testing.T) {
	dataDir := useTempDataDir(t)
	client, _ := newTestClient(t)
	topic := uniqueTopic()

	_, err := client.PublishEvent(t.Context(), &pb.EventRequest{Topic: topic, Payload: []byte("not json")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want code InvalidArgument", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, topic, "0")); !os.IsNotExist(err) {
		t.Errorf("invalid event was written to disk (stat err = %v)", err)
	}
}

func TestPublishEventWriteError(t *testing.T) {
	dataDir := useTempDataDir(t)
	client, _ := newTestClient(t)
	// A file where the data directory should be makes every write fail.
	if err := os.WriteFile(dataDir, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := client.PublishEvent(t.Context(), &pb.EventRequest{Topic: uniqueTopic(), Payload: []byte(`{"a":1}`)})
	if status.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want code Internal", err)
	}
}

func TestSubscribeTopicReceivesEvents(t *testing.T) {
	useTempDataDir(t)
	client, broker := newTestClient(t)
	topic, otherTopic := uniqueTopic(), uniqueTopic()

	sub1 := subscribe(t, client, broker, topic, 1)
	sub2 := subscribe(t, client, broker, topic, 2)
	other := subscribe(t, client, broker, otherTopic, 1)

	before := time.Now().UnixMilli()
	publish(t, client, topic, `{"n":1}`)
	publish(t, client, topic, `{"n":2}`)
	publish(t, client, otherTopic, `{"o":1}`)
	after := time.Now().UnixMilli()

	// Both consumers of topic get both events, in order, as stored on disk:
	// round robin puts them on shards 0 and 1, each as seq_n 0.
	for _, sub := range []pb.EventBroker_SubscribeTopicClient{sub1, sub2} {
		for _, want := range []struct{ id, payload string }{
			{topic + "/0/0", `{"n":1,"seq_n":0}`},
			{topic + "/1/0", `{"n":2,"seq_n":0}`},
		} {
			event := recv(t, sub, want.id, want.payload)
			if ts := event.GetTimestamp(); ts < before || ts > after {
				t.Errorf("timestamp %d not within [%d, %d]", ts, before, after)
			}
		}
	}
	// The other topic's consumer only gets its own topic's event: if it had
	// received topic's events, they would come first.
	recv(t, other, otherTopic+"/0/0", `{"o":1,"seq_n":0}`)
}

func TestSubscribeTopicCleanupOnDisconnect(t *testing.T) {
	useTempDataDir(t)
	client, broker := newTestClient(t)
	topic := uniqueTopic()

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := client.SubscribeTopic(ctx, &pb.SubscribeRequest{Topic: topic}); err != nil {
		t.Fatalf("SubscribeTopic: %v", err)
	}
	waitForSubscribers(t, broker, topic, 1)

	cancel() // the consumer disconnects
	waitForSubscribers(t, broker, topic, 0)

	// Publishing to a topic with no consumers left still works.
	publish(t, client, topic, `{"a":1}`)
}

// A full consumer must not block publishing nor other consumers.
func TestBroadcastDropsForSlowConsumer(t *testing.T) {
	broker := NewBrokerServer()
	slow := broker.addSubscriber("t")
	fast := broker.addSubscriber("t")
	for i := 0; i < subscriberBuffer; i++ {
		slow.events <- &pb.EventResponse{}
	}

	done := make(chan struct{})
	go func() {
		broker.broadcast("t", &pb.EventResponse{EventId: "t/0/0"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked on a full consumer")
	}

	if len(slow.events) != subscriberBuffer {
		t.Errorf("slow consumer has %d events, want %d (event dropped)", len(slow.events), subscriberBuffer)
	}
	select {
	case event := <-fast.events:
		if event.GetEventId() != "t/0/0" {
			t.Errorf("fast consumer got %q, want t/0/0", event.GetEventId())
		}
	default:
		t.Error("fast consumer did not get the event")
	}
}

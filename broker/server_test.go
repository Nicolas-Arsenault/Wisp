package broker

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	pb "wisp/proto"
	"wisp/shards"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
// client connected to it. Both are shut down when the test ends.
func newTestClient(t *testing.T) pb.EventBrokerClient {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterEventBrokerServer(grpcServer, &Server{})
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
	return pb.NewEventBrokerClient(conn)
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

	resp, err := (&Server{}).PublishEvent(context.Background(), &pb.EventRequest{
		Topic:   topic,
		Payload: []byte(payload),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("PublishEvent Success = false, want true")
	}
	if got := readShard(t, dataDir, topic, "0"); got != payload {
		t.Errorf("shard 0 = %q, want %q", got, payload)
	}
}

func TestPublishEventOverGRPC(t *testing.T) {
	dataDir := useTempDataDir(t)
	client := newTestClient(t)
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

	for i, want := range payloads {
		id := strconv.Itoa(i)
		if got := readShard(t, dataDir, topic, id); got != want {
			t.Errorf("shard %s = %q, want %q", id, got, want)
		}
	}
}

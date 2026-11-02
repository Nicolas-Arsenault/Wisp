package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
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
func newTestClient(t testing.TB) pb.EventBrokerClient {
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
	return &pb.PublishRequest{RequestId: id, Topic: "t", Shard: shard, Payloads: [][]byte{[]byte(payload)}}
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

func TestDescribeTopicNumShards(t *testing.T) {
	client := newTestClient(t)

	info, err := client.DescribeTopic(t.Context(), &pb.DescribeTopicRequest{Topic: "run.a", NumShards: 1})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetNumShards() != 1 {
		t.Errorf("num_shards = %d, want 1", info.GetNumShards())
	}
	info, err = client.DescribeTopic(t.Context(), &pb.DescribeTopicRequest{Topic: "run.a", NumShards: 7})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetNumShards() != 1 {
		t.Errorf("num_shards = %d, want the existing 1", info.GetNumShards())
	}
	_, err = client.DescribeTopic(t.Context(), &pb.DescribeTopicRequest{Topic: "run.b", NumShards: -1})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("num_shards -1: got %v, want InvalidArgument", err)
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
		if codes.Code(ack.GetErrorCode()) != want[i].code || ack.GetFirstEventId() != want[i].eventID {
			t.Errorf("ack %d = code %d, id %d, want %v, %d", i, ack.GetErrorCode(), ack.GetFirstEventId(), want[i].code, want[i].eventID)
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

// readToEnd returns the ids a stop_at_end Subscribe sends before it closes.
func readToEnd(t *testing.T, sub pb.EventBroker_SubscribeClient) []uint64 {
	t.Helper()
	var ids []uint64
	for {
		batch, err := sub.Recv()
		if err == io.EOF {
			return ids
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		for _, e := range batch.GetEvents() {
			ids = append(ids, e.GetEventId())
		}
	}
}

func TestSubscribeStopAtEnd(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{"n":0}`), req(2, 0, `{"n":1}`), req(3, 0, `{"n":2}`))

	// One record per batch, so the stream has to go round several times.
	got := readToEnd(t, subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 0, MaxBytes: 1, StopAtEnd: true}))
	if fmt.Sprint(got) != "[0 1 2]" {
		t.Errorf("got ids %v, want [0 1 2]", got)
	}
	got = readToEnd(t, subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 0, StartId: 2, StopAtEnd: true}))
	if fmt.Sprint(got) != "[2]" {
		t.Errorf("from 2: got ids %v, want [2]", got)
	}
}

func TestSubscribeStopAtEndWithNothingToRead(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{"n":0}`))

	if got := readToEnd(t, subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 1, StopAtEnd: true})); len(got) != 0 {
		t.Errorf("empty shard: got ids %v", got)
	}
	if got := readToEnd(t, subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 0, Latest: true, StopAtEnd: true})); len(got) != 0 {
		t.Errorf("latest: got ids %v", got)
	}
}

// Records stored while the stream runs are after the snapshot, so a reader
// racing a writer still gets a contiguous prefix and stops.
func TestSubscribeStopAtEndIgnoresLaterRecords(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{"n":0}`), req(2, 0, `{"n":1}`))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for id := uint64(3); id < 40; id++ {
			stream, err := client.Publish(context.Background())
			if err != nil {
				return
			}
			stream.Send(req(id, 0, `{"n":2}`))
			stream.Recv()
			stream.CloseSend()
		}
	}()
	for range 20 {
		got := readToEnd(t, subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Shard: 0, StopAtEnd: true}))
		if len(got) < 2 {
			t.Fatalf("got ids %v, want every acked record", got)
		}
		for i, id := range got {
			if id != uint64(i) {
				t.Fatalf("got ids %v, want no gap", got)
			}
		}
	}
	<-done
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

// join opens a Join stream for consumer id on topic t. cancel leaves.
func join(t *testing.T, client pb.EventBrokerClient, id string) (pb.EventBroker_JoinClient, context.CancelFunc) {
	t.Helper()
	return joinGroup(t, client, "", id)
}

func joinGroup(t *testing.T, client pb.EventBrokerClient, group, id string) (pb.EventBroker_JoinClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := client.Join(ctx, &pb.JoinRequest{Topic: "t", ConsumerId: id, Group: group})
	if err != nil {
		t.Fatal(err)
	}
	return stream, cancel
}

// expectShards reads the next assignment and checks its shards.
func expectShards(t *testing.T, stream pb.EventBroker_JoinClient, want string) *pb.Assignment {
	t.Helper()
	a, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got := fmt.Sprint(a.GetShards()); got != want {
		t.Errorf("shards = %s, want %s", got, want)
	}
	return a
}

// The flow from the docs: A alone gets every shard; when B joins, both get
// their new split; when B leaves, A gets every shard again.
func TestJoinRebalances(t *testing.T) {
	client := newTestClient(t)

	a, _ := join(t, client, "a")
	first := expectShards(t, a, "[0 1 2 3 4]")
	if first.GetTopic() != "t" {
		t.Errorf("topic = %q, want t", first.GetTopic())
	}

	b, leaveB := join(t, client, "b")
	second := expectShards(t, a, "[0 1 2]")
	expectShards(t, b, "[3 4]")
	if second.GetGeneration() <= first.GetGeneration() {
		t.Errorf("generation %d after %d, want it to increase", second.GetGeneration(), first.GetGeneration())
	}

	leaveB()
	expectShards(t, a, "[0 1 2 3 4]")
}

func TestJoinThreeConsumers(t *testing.T) {
	client := newTestClient(t)
	a, _ := join(t, client, "a")
	expectShards(t, a, "[0 1 2 3 4]")
	b, _ := join(t, client, "b")
	expectShards(t, a, "[0 1 2]")
	expectShards(t, b, "[3 4]")

	// Only shard 2 moves.
	c, _ := join(t, client, "c")
	expectShards(t, a, "[0 1]")
	expectShards(t, b, "[3 4]")
	expectShards(t, c, "[2]")
}

// Every shard has exactly one owner, whatever the number of consumers.
func TestRebalanceCoversEveryShard(t *testing.T) {
	s := NewBrokerServer(nil)
	for n := 1; n <= 7; n++ {
		g := &group{numShards: 5}
		for i := range n {
			g.members = append(g.members, &member{id: fmt.Sprint(i), updates: make(chan *pb.Assignment, 1)})
		}
		s.rebalance("t", g)

		owners := make(map[int32]int)
		for _, m := range g.members {
			for _, shard := range (<-m.updates).GetShards() {
				owners[shard]++
			}
		}
		for shard := range int32(5) {
			if owners[shard] != 1 {
				t.Errorf("%d consumers: shard %d has %d owners, want 1", n, shard, owners[shard])
			}
		}
	}
}

func TestJoinErrors(t *testing.T) {
	client := newTestClient(t)
	a, _ := join(t, client, "a")
	expectShards(t, a, "[0 1 2 3 4]")

	tests := []struct {
		req  *pb.JoinRequest
		want codes.Code
	}{
		{&pb.JoinRequest{Topic: "t", ConsumerId: "a"}, codes.AlreadyExists},
		{&pb.JoinRequest{Topic: "t"}, codes.InvalidArgument},
		{&pb.JoinRequest{Topic: "../x", ConsumerId: "b"}, codes.InvalidArgument},
		{&pb.JoinRequest{Topic: "t", ConsumerId: "b", Group: "../x"}, codes.InvalidArgument},
	}
	for _, tt := range tests {
		stream, err := client.Join(t.Context(), tt.req)
		if err == nil {
			_, err = stream.Recv()
		}
		if status.Code(err) != tt.want {
			t.Errorf("Join(%v) err = %v, want %v", tt.req, err, tt.want)
		}
	}
}

// A leaving consumer only gives up its own shards: the others keep theirs.
func TestRebalanceKeepsShards(t *testing.T) {
	m := func(id string) *member { return &member{id: id, updates: make(chan *pb.Assignment, 1)} }
	a, b, c := m("a"), m("b"), m("c")
	s := NewBrokerServer(nil)
	g := &group{numShards: 5, members: []*member{a, b, c}}
	s.rebalance("t", g)
	before := map[*member]string{a: fmt.Sprint((<-a.updates).GetShards()), b: fmt.Sprint((<-b.updates).GetShards())}

	g.members = []*member{a, b}
	s.rebalance("t", g)
	for _, m := range g.members {
		got := (<-m.updates).GetShards()
		for _, shard := range got[:2] {
			if !strings.Contains(before[m], fmt.Sprint(shard)) {
				t.Errorf("%s: kept shards %v, had %s", m.id, got, before[m])
			}
		}
	}
}

// Each group gets every shard, whatever the other groups do.
func TestGroupsAreIndependent(t *testing.T) {
	client := newTestClient(t)
	a, _ := joinGroup(t, client, "g1", "a")
	expectShards(t, a, "[0 1 2 3 4]")
	same, _ := joinGroup(t, client, "g2", "a") // same id, other group: allowed
	expectShards(t, same, "[0 1 2 3 4]")

	empty, _ := join(t, client, "b")
	expectShards(t, empty, "[0 1 2 3 4]")
	named, _ := joinGroup(t, client, "default", "c") // the empty group is "default"
	expectShards(t, empty, "[0 1 2]")
	expectShards(t, named, "[3 4]")
}

func commitOffset(client pb.EventBrokerClient, t *testing.T, group, id string, generation uint64, shard int32, next uint64) error {
	t.Helper()
	_, err := client.Commit(t.Context(), &pb.CommitRequest{
		Topic: "t", Group: group, ConsumerId: id, Generation: generation, Shard: shard, NextId: next,
	})
	return err
}

// firstID returns the first event a group's Subscribe gets on shard 0.
func firstID(t *testing.T, client pb.EventBrokerClient, group string, req *pb.SubscribeRequest) uint64 {
	t.Helper()
	req.Topic, req.Group = "t", group
	return receive(t, subscribe(t, client, req), 1)[0].GetEventId()
}

// A shard's next owner resumes where the previous owner committed.
func TestCommitMovesWithShard(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{}`), req(2, 0, `{}`), req(3, 0, `{}`), req(4, 0, `{}`))

	a, leaveA := joinGroup(t, client, "g", "a")
	first := expectShards(t, a, "[0 1 2 3 4]")
	if err := commitOffset(client, t, "g", "a", first.GetGeneration(), 0, 2); err != nil {
		t.Fatal(err)
	}

	b, _ := joinGroup(t, client, "g", "b")
	expectShards(t, b, "[3 4]")
	leaveA()
	expectShards(t, b, "[0 1 2 3 4]")

	if id := firstID(t, client, "g", &pb.SubscribeRequest{}); id != 2 {
		t.Errorf("resumed at %d, want 2", id)
	}
	// The request's own position doesn't override a stored offset.
	if id := firstID(t, client, "g", &pb.SubscribeRequest{StartId: 3}); id != 2 {
		t.Errorf("resumed at %d with start_id 3, want 2", id)
	}
	// Other groups and plain Subscribe calls aren't affected.
	if id := firstID(t, client, "other", &pb.SubscribeRequest{}); id != 0 {
		t.Errorf("other group started at %d, want 0", id)
	}
	if id := firstID(t, client, "", &pb.SubscribeRequest{StartId: 1}); id != 1 {
		t.Errorf("plain Subscribe started at %d, want 1", id)
	}
}

// With latest, the start position is stored, so a new owner gets what came
// after it even if nothing was committed.
func TestLatestPositionIsStored(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{"old":true}`))

	// The first event reaches the first reader only after it stored its
	// position, whatever the timing of the publish: id is that position.
	first := subscribe(t, client, &pb.SubscribeRequest{Topic: "t", Group: "g", Latest: true})
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
	id := receive(t, first, 1)[0].GetEventId()
	if id == 0 {
		t.Fatal("latest got the old record")
	}
	if got := firstID(t, client, "g", &pb.SubscribeRequest{Latest: true}); got != id {
		t.Errorf("new owner started at %d, want the stored %d", got, id)
	}
}

// Only the current owner, in the current generation, can commit.
func TestCommitFencing(t *testing.T) {
	client := newTestClient(t)
	publish(t, client, req(1, 0, `{}`), req(2, 0, `{}`), req(3, 3, `{}`))

	a, _ := joinGroup(t, client, "g", "a")
	old := expectShards(t, a, "[0 1 2 3 4]").GetGeneration()
	b, _ := joinGroup(t, client, "g", "b")
	current := expectShards(t, a, "[0 1 2]").GetGeneration()
	expectShards(t, b, "[3 4]")

	tests := []struct {
		name       string
		id         string
		generation uint64
		shard      int32
		next       uint64
		want       codes.Code
	}{
		{"owner", "a", current, 0, 1, codes.OK},
		{"old generation", "a", old, 0, 2, codes.FailedPrecondition},
		{"shard it lost", "a", current, 3, 1, codes.FailedPrecondition},
		{"not a member", "z", current, 0, 2, codes.FailedPrecondition},
		{"past the end", "a", current, 0, 3, codes.OutOfRange},
		{"bad shard", "a", current, 9, 0, codes.InvalidArgument},
	}
	for _, tt := range tests {
		err := commitOffset(client, t, "g", tt.id, tt.generation, tt.shard, tt.next)
		if status.Code(err) != tt.want {
			t.Errorf("%s: err = %v, want %v", tt.name, err, tt.want)
		}
	}
	if id := firstID(t, client, "g", &pb.SubscribeRequest{}); id != 1 {
		t.Errorf("resumed at %d, want 1 (the refused commits changed nothing)", id)
	}
	if err := commitOffset(client, t, "nope", "a", 1, 0, 0); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unknown group: err = %v, want FailedPrecondition", err)
	}
}

// A resent record gets ALREADY_EXISTS and is stored once; a gap is refused.
func TestPublishDropsRetries(t *testing.T) {
	client := newTestClient(t)
	seq := func(id, sequence uint64) *pb.PublishRequest {
		r := req(id, 0, `{}`)
		r.ProducerId, r.FirstSequence = 7, sequence
		return r
	}
	acks := publish(t, client, seq(1, 1), seq(2, 2), seq(3, 2), seq(4, 9), seq(5, 3))
	want := []codes.Code{codes.OK, codes.OK, codes.AlreadyExists, codes.FailedPrecondition, codes.OK}
	for i, ack := range acks {
		if codes.Code(ack.GetErrorCode()) != want[i] {
			t.Errorf("ack %d = %d, want %v", i, ack.GetErrorCode(), want[i])
		}
	}
	if id := acks[4].GetFirstEventId(); id != 2 {
		t.Errorf("last record has id %d, want 2 (one duplicate dropped)", id)
	}
}

// quietLogs stops broker logs from mixing with benchmark results.
func quietLogs(b *testing.B) {
	old := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(old) })
}

const benchEvent = `{"device":"sensor-42","temp":21.5,"humidity":48,"status":"ok","ts":1793137200123}`

// One stream per shard sends batches as fast as acks allow, like the producer.
func BenchmarkPublish(b *testing.B) {
	quietLogs(b)
	client := newTestClient(b)
	b.SetBytes(int64(len(benchEvent)))
	b.ReportAllocs()
	const numShards = 5
	perShard := b.N / numShards
	b.ResetTimer()

	done := make(chan error, numShards)
	for shard := range int32(numShards) {
		go func() {
			stream, err := client.Publish(b.Context())
			if err != nil {
				done <- err
				return
			}
			go func() {
				const batch = 256
				for i := 0; i < perShard; i += batch {
					r := &pb.PublishRequest{RequestId: uint64(i), Topic: "t", Shard: shard}
					for range min(batch, perShard-i) {
						r.Payloads = append(r.Payloads, []byte(benchEvent))
					}
					stream.Send(r)
				}
				stream.CloseSend()
			}()
			for {
				if _, err := stream.Recv(); err != nil {
					if err == io.EOF {
						err = nil
					}
					done <- err
					return
				}
			}
		}()
	}
	for range numShards {
		if err := <-done; err != nil {
			b.Fatal(err)
		}
	}
}

// A consumer reading stored records over gRPC.
func BenchmarkSubscribe(b *testing.B) {
	quietLogs(b)
	client := newTestClient(b)
	const stored = 20_000
	stream, err := client.Publish(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		for i := range stored {
			stream.Send(req(uint64(i), 0, benchEvent))
		}
		stream.CloseSend()
	}()
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}

	b.SetBytes(int64(len(benchEvent)))
	b.ReportAllocs()
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()
	var sub pb.EventBroker_SubscribeClient
	got, left := 0, 0
	for b.Loop() {
		if left == 0 {
			if sub, err = client.Subscribe(ctx, &pb.SubscribeRequest{Topic: "t"}); err != nil {
				b.Fatal(err)
			}
			left = stored
		}
		if got == 0 {
			batch, err := sub.Recv()
			if err != nil {
				b.Fatal(err)
			}
			got = len(batch.GetEvents())
		}
		got--
		left--
	}
}

// A batch of records is stored as one unit, and acked once.
func TestPublishBatch(t *testing.T) {
	client := newTestClient(t)
	batch := func(id uint64, payloads ...string) *pb.PublishRequest {
		r := req(id, 0, "")
		r.Payloads = nil
		for _, p := range payloads {
			r.Payloads = append(r.Payloads, []byte(p))
		}
		return r
	}
	acks := publish(t, client,
		batch(1, `{"n":0}`, `{"n":1}`, `{"n":2}`),
		batch(2, `{"n":3}`, `not json`), // nothing of it is stored
		batch(3),                        // empty
		batch(4, `{"n":3}`),
	)
	want := []struct {
		code  codes.Code
		first uint64
		count uint32
	}{{codes.OK, 0, 3}, {codes.InvalidArgument, 0, 0}, {codes.InvalidArgument, 0, 0}, {codes.OK, 3, 1}}
	for i, ack := range acks {
		w := want[i]
		if codes.Code(ack.GetErrorCode()) != w.code || ack.GetFirstEventId() != w.first || ack.GetCount() != w.count {
			t.Errorf("ack %d = %v, want %+v", i, ack, w)
		}
	}
	sub := subscribe(t, client, &pb.SubscribeRequest{Topic: "t"})
	for i, e := range receive(t, sub, 4) {
		if want := fmt.Sprintf(`{"n":%d}`, i); string(e.GetPayload()) != want {
			t.Errorf("event %d = %s, want %s", i, e.GetPayload(), want)
		}
	}
}

// A resent batch stores only what the first attempt didn't.
func TestPublishPartlyStoredBatch(t *testing.T) {
	client := newTestClient(t)
	seq := func(first uint64, n int) *pb.PublishRequest {
		r := req(first, 0, "")
		r.Payloads, r.ProducerId, r.FirstSequence = nil, 7, first
		for range n {
			r.Payloads = append(r.Payloads, []byte(`{}`))
		}
		return r
	}
	acks := publish(t, client, seq(1, 2), seq(1, 3), seq(1, 3), seq(9, 1))
	want := []struct {
		code  codes.Code
		count uint32
	}{{codes.OK, 2}, {codes.OK, 1}, {codes.AlreadyExists, 0}, {codes.FailedPrecondition, 0}}
	for i, ack := range acks {
		if codes.Code(ack.GetErrorCode()) != want[i].code || ack.GetCount() != want[i].count {
			t.Errorf("ack %d = %v, want %+v", i, ack, want[i])
		}
	}
	if id := acks[1].GetFirstEventId(); id != 2 {
		t.Errorf("first new record has id %d, want 2", id)
	}
}

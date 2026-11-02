package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	pb "wisp/proto"
	"wisp/shards"
	"wisp/stream"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	listenAddr = ":50051"
	dataDir    = "../data"

	publishWindow   = 256            // batches a producer can wait for before we stop reading its stream
	defaultMaxBytes = 1 << 20        // batch size when the consumer sends max_bytes = 0
	maxMaxBytes     = 4<<20 - 64<<10 // gRPC clients refuse messages over 4 MiB
)

var errBadShard = errors.New("shard out of range")

type Server struct {
	pb.UnimplementedEventBrokerServer
	topics *stream.Registry

	mu     sync.Mutex          // guards groups and offset files
	groups map[groupKey]*group // consumers of each group (see groups.go)
}

func NewBrokerServer(topics *stream.Registry) *Server {
	return &Server{topics: topics, groups: make(map[groupKey]*group)}
}

// DescribeTopic returns the topic's shard count, creating the topic if needed.
func (s *Server) DescribeTopic(_ context.Context, req *pb.DescribeTopicRequest) (*pb.TopicInfo, error) {
	topicShards, err := s.topics.TopicWithShards(req.GetTopic(), int(req.GetNumShards()))
	if err != nil {
		return nil, status.Error(codeFor(err), err.Error())
	}
	return &pb.TopicInfo{Topic: req.GetTopic(), NumShards: int32(len(topicShards))}, nil
}

type pendingAck struct {
	ack     *pb.PublishAck
	pending *shards.Pending // nil if the request already failed
}

// Publish serves one producer. Each request, a batch of records, goes on the
// queue of the shard it names, in order, and gets one ack once stored. A bad
// request gets an error ack and the stream stays open.
func (s *Server) Publish(srv pb.EventBroker_PublishServer) error {
	acks := make(chan pendingAck, publishWindow)
	sendDone := make(chan error, 1)
	go func() { sendDone <- sendAcks(srv, acks) }()

	recvErr := s.receive(srv, acks)
	close(acks)
	sendErr := <-sendDone
	if recvErr != nil {
		return recvErr
	}
	return sendErr
}

// receive queues each request until the producer closes its side.
func (s *Server) receive(srv pb.EventBroker_PublishServer, acks chan<- pendingAck) error {
	ctx := srv.Context()
	for {
		req, err := srv.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		ack := &pb.PublishAck{RequestId: req.GetRequestId(), Shard: req.GetShard()}
		pending, err := s.enqueue(ctx, req)
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		if err != nil {
			setAckError(ack, err)
			slog.Warn("rejected event", "topic", req.GetTopic(), "shard", req.GetShard(), "err", err)
		}
		select {
		case acks <- pendingAck{ack, pending}:
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

func (s *Server) enqueue(ctx context.Context, req *pb.PublishRequest) (*shards.Pending, error) {
	topicShards, err := s.topics.Topic(req.GetTopic())
	if err != nil {
		return nil, err
	}
	shard := req.GetShard()
	if shard < 0 || int(shard) >= len(topicShards) {
		return nil, fmt.Errorf("%w: %d (topic has %d)", errBadShard, shard, len(topicShards))
	}
	return topicShards[shard].EnqueueBatch(ctx, req.GetPayloads(), req.GetProducerId(), req.GetFirstSequence())
}

// sendAcks sends acks in request order. After a send error it keeps
// draining so receive never blocks.
func sendAcks(srv pb.EventBroker_PublishServer, acks <-chan pendingAck) error {
	var sendErr error
	for a := range acks {
		if a.pending != nil {
			id, count, err := a.pending.WaitBatch()
			if err != nil {
				setAckError(a.ack, err)
			}
			a.ack.FirstEventId = id
			a.ack.Count = uint32(count)
		}
		if sendErr == nil {
			sendErr = srv.Send(a.ack)
		}
	}
	return sendErr
}

func setAckError(ack *pb.PublishAck, err error) {
	ack.ErrorCode = int32(codeFor(err))
	ack.ErrorMessage = err.Error()
}

func codeFor(err error) codes.Code {
	switch {
	case errors.Is(err, shards.ErrInvalidPayload), errors.Is(err, stream.ErrInvalidTopic), errors.Is(err, stream.ErrInvalidGroup), errors.Is(err, stream.ErrInvalidShards), errors.Is(err, errBadShard):
		return codes.InvalidArgument
	case errors.Is(err, shards.ErrDuplicate):
		return codes.AlreadyExists
	case errors.Is(err, shards.ErrOutOfOrder):
		return codes.FailedPrecondition
	case errors.Is(err, shards.ErrShardFull):
		return codes.ResourceExhausted
	case errors.Is(err, shards.ErrShardClosed):
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

// Subscribe sends one shard from the group's stored offset, or else from
// start_id (or from the end if latest): all stored records first, then new
// ones as they are stored. Records are read from disk, so a slow consumer
// never loses any. With stop_at_end it returns after the last record stored
// when it started. The writer sets the end before it acks, so that includes
// every acked record.
func (s *Server) Subscribe(req *pb.SubscribeRequest, srv pb.EventBroker_SubscribeServer) error {
	topicShards, ok := s.topics.Lookup(req.GetTopic())
	if !ok {
		return status.Errorf(codes.NotFound, "topic %q does not exist", req.GetTopic())
	}
	if req.GetShard() < 0 || int(req.GetShard()) >= len(topicShards) {
		return status.Errorf(codes.InvalidArgument, "shard %d out of range (topic has %d)", req.GetShard(), len(topicShards))
	}
	shard := topicShards[req.GetShard()]
	maxBytes := fetchSize(req.GetMaxBytes())

	start, err := s.startID(req, shard)
	if err != nil {
		return err
	}
	cursor := shards.Cursor{Next: start}
	end := shard.NextID()

	slog.Info("consumer subscribed", "topic", req.GetTopic(), "shard", req.GetShard(), "start_id", start)
	ctx := srv.Context()
	for {
		if req.GetStopAtEnd() && cursor.Next >= end {
			return nil
		}
		records, next, err := shard.Read(cursor, maxBytes)
		if err != nil {
			slog.Error("could not read shard", "topic", req.GetTopic(), "shard", req.GetShard(), "err", err)
			return status.Errorf(codes.Internal, "could not read event %d: %v", cursor.Next, err)
		}
		cursor = next
		if len(records) == 0 {
			if err := shard.Wait(ctx, cursor.Next); err != nil {
				if errors.Is(err, shards.ErrShardClosed) {
					return status.Error(codes.Unavailable, "broker is shutting down")
				}
				return status.FromContextError(err).Err()
			}
			continue
		}

		if req.GetStopAtEnd() && next.Next > end {
			records = records[:end-records[0].ID] // stored after the call started
		}

		batch := &pb.EventBatch{Topic: req.GetTopic(), Shard: req.GetShard(), Events: make([]*pb.Event, len(records))}
		for i, r := range records {
			batch.Events[i] = &pb.Event{EventId: r.ID, Payload: r.Payload, Timestamp: r.Timestamp}
		}
		if err := srv.Send(batch); err != nil {
			return err
		}
	}
}

// startID returns where Subscribe starts. The first time a group reads a
// shard, it stores that position, so with latest a new owner of the shard
// doesn't skip what came after it.
func (s *Server) startID(req *pb.SubscribeRequest, shard *shards.Shard) (uint64, error) {
	if req.GetGroup() == "" {
		return requestedStart(req, shard)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok, err := s.topics.LoadOffset(req.GetTopic(), req.GetGroup(), int(req.GetShard()))
	if err != nil {
		return 0, status.Error(codeFor(err), err.Error())
	}
	if ok {
		return stored, nil
	}
	next, err := requestedStart(req, shard)
	if err != nil {
		return 0, err
	}
	if err := s.topics.SaveOffset(req.GetTopic(), req.GetGroup(), int(req.GetShard()), next); err != nil {
		return 0, status.Error(codeFor(err), err.Error())
	}
	return next, nil
}

func requestedStart(req *pb.SubscribeRequest, shard *shards.Shard) (uint64, error) {
	if req.GetLatest() {
		return shard.NextID(), nil
	}
	if req.GetStartId() > shard.NextID() {
		return 0, status.Errorf(codes.OutOfRange, "start_id %d is after the end of the shard (%d)", req.GetStartId(), shard.NextID())
	}
	return req.GetStartId(), nil
}

func fetchSize(requested uint32) int {
	if requested == 0 {
		return defaultMaxBytes
	}
	return min(int(requested), maxMaxBytes)
}

func main() {
	if err := run(); err != nil {
		slog.Error("broker stopped", "err", err)
		os.Exit(1)
	}
}

// run serves until Ctrl+C or SIGTERM. Then it disconnects every client,
// waits for the handlers to return, and closes the shards (which still
// stores the records already queued).
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	topics, err := stream.OpenRegistry(dataDir)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		topics.Close()
		return err
	}

	// WaitForHandlers: Stop waits until no handler can still read a shard.
	// Keepalive pings idle clients, so a consumer whose connection died
	// without closing leaves its group after about 15 s.
	grpcServer := grpc.NewServer(
		grpc.WaitForHandlers(true),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
	)
	pb.RegisterEventBrokerServer(grpcServer, NewBrokerServer(topics))
	go func() {
		<-ctx.Done()
		slog.Info("shutting down")
		grpcServer.Stop()
	}()

	slog.Info("broker listening", "addr", listenAddr, "data_dir", dataDir)
	serveErr := grpcServer.Serve(lis)
	grpcServer.Stop() // in case Serve failed on its own
	return errors.Join(serveErr, topics.Close())
}

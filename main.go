package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
	pb "wisp/proto"
	"wisp/shards"

	"wisp/stream"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// subscriberBuffer is how many events a consumer can fall behind before
// PublishEvent starts dropping events for it (see Server.broadcast).
const subscriberBuffer = 64

// subscriber is one consumer connected through SubscribeTopic. PublishEvent
// pushes events onto its channel; only the consumer's own SubscribeTopic call
// reads it and calls Send, because gRPC does not allow concurrent Sends on
// one stream.
type subscriber struct {
	events chan *pb.EventResponse
}

type Server struct {
	pb.UnimplementedEventBrokerServer
	mu sync.RWMutex
	// subscribers is the set of consumers of each topic. A set (instead of a
	// slice) makes removal simple and never mutates memory a reader still holds.
	subscribers map[string]map[*subscriber]struct{}
}

func NewBrokerServer() *Server {
	return &Server{
		subscribers: make(map[string]map[*subscriber]struct{}),
	}
}

// gRPC function
// multithreaded from google grpc...
//
// PublishEvent writes the event to the topic's stream, then sends the stored
// event to every current consumer of the topic. Consumers that subscribe
// later don't get it.
//
// Errors: codes.InvalidArgument if the payload is not a JSON object,
// codes.Internal if it could not be written. Nothing is sent to consumers
// in either case.
func (s *Server) PublishEvent(ctx context.Context, req *pb.EventRequest) (*pb.PublishResponse, error) {
	slog.Debug("received event", "topic", req.Topic, "bytes", len(req.Payload))

	//TODO: Change this later like kafkha style
	// Consumer group that gets shards assigned... and auto rebalance
	written, err := stream.WriteToStream(req.Payload, req.Topic)
	if errors.Is(err, shards.ErrInvalidPayload) {
		return nil, status.Errorf(codes.InvalidArgument, "payload must be a JSON object: %v", err)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not write event: %v", err)
	}

	s.broadcast(req.Topic, &pb.EventResponse{
		EventId:   written.EventID(),
		Payload:   written.Payload,
		Timestamp: time.Now().UnixMilli(),
	})
	return &pb.PublishResponse{Success: true}, nil
}

// broadcast hands event to every consumer of topic without ever blocking:
// if a consumer's buffer is full, the event is dropped for that consumer and
// a warning is logged. The event is still on disk.
//
// Events published concurrently on one topic may reach consumers in a
// different order than their seq_n.
func (s *Server) broadcast(topic string, event *pb.EventResponse) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for sub := range s.subscribers[topic] {
		select {
		case sub.events <- event:
		default:
			slog.Warn("consumer too slow, dropped event", "topic", topic, "event_id", event.EventId)
		}
	}
}

// SubscribeTopic registers the consumer for req.Topic and streams it every
// event published on that topic from now on, until the consumer disconnects.
// It returns the context error on disconnect, or the Send error if sending
// fails; either way the consumer is removed.
func (s *Server) SubscribeTopic(req *pb.SubscribeRequest, stream pb.EventBroker_SubscribeTopicServer) error {
	topic := req.Topic
	sub := s.addSubscriber(topic)
	// Ensure cleanup when the consumer eventually disconnects
	defer s.removeSubscriber(topic, sub)

	// Keep the stream alive, sending events, until the client drops the
	// connection or the context is cancelled.
	//TODO: Change this so it uses offset and subscribes to a specific shard
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-sub.events:
			if err := stream.Send(event); err != nil {
				slog.Warn("could not send event to consumer", "topic", topic, "event_id", event.EventId, "err", err)
				return err
			}
		}
	}
}

// addSubscriber registers a new consumer of topic, thread-safely.
func (s *Server) addSubscriber(topic string) *subscriber {
	sub := &subscriber{events: make(chan *pb.EventResponse, subscriberBuffer)}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribers[topic] == nil {
		s.subscribers[topic] = make(map[*subscriber]struct{})
	}
	s.subscribers[topic][sub] = struct{}{}
	slog.Info("consumer subscribed", "topic", topic, "consumers", len(s.subscribers[topic]))
	return sub
}

// removeSubscriber unregisters sub. Its channel is left open on purpose:
// closing it could race with a broadcast still sending to it.
func (s *Server) removeSubscriber(topic string, sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscribers[topic], sub)
	if len(s.subscribers[topic]) == 0 {
		delete(s.subscribers, topic)
	}
	slog.Info("consumer cleaned up", "topic", topic)
}

// subscriberCount returns how many consumers topic has.
func (s *Server) subscriberCount(topic string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers[topic])
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterEventBrokerServer(grpcServer, NewBrokerServer())

	slog.Info("gRPC Broker listening on port 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

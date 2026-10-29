package main

import (
	"log/slog"
	"slices"
	pb "wisp/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// group is the consumers of one topic, in join order.
type group struct {
	numShards  int
	generation uint64
	members    []*member
}

// member is one open Join stream.
type member struct {
	id      string
	updates chan *pb.Assignment // buffer 1: only the newest assignment matters
}

// Join registers the consumer for as long as its stream is open. Each time
// a consumer joins or leaves the topic, every consumer gets its new shards.
func (s *Server) Join(req *pb.JoinRequest, srv pb.EventBroker_JoinServer) error {
	if req.GetConsumerId() == "" {
		return status.Error(codes.InvalidArgument, "consumer_id is required")
	}
	topicShards, err := s.topics.Topic(req.GetTopic())
	if err != nil {
		return status.Error(codeFor(err), err.Error())
	}

	m := &member{id: req.GetConsumerId(), updates: make(chan *pb.Assignment, 1)}
	if err := s.addMember(req.GetTopic(), len(topicShards), m); err != nil {
		return err
	}
	defer s.removeMember(req.GetTopic(), m)

	for {
		select {
		case a := <-m.updates:
			if err := srv.Send(a); err != nil {
				return err
			}
		case <-srv.Context().Done():
			return status.FromContextError(srv.Context().Err()).Err()
		}
	}
}

func (s *Server) addMember(topic string, numShards int, m *member) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.groups[topic]
	if g == nil {
		g = &group{numShards: numShards}
		s.groups[topic] = g
	}
	for _, other := range g.members {
		if other.id == m.id {
			return status.Errorf(codes.AlreadyExists, "consumer %q already joined topic %q", m.id, topic)
		}
	}
	g.members = append(g.members, m)
	slog.Info("consumer joined", "topic", topic, "consumer", m.id, "consumers", len(g.members))
	s.rebalance(topic, g)
	return nil
}

func (s *Server) removeMember(topic string, m *member) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.groups[topic]
	g.members = slices.DeleteFunc(g.members, func(other *member) bool { return other == m })
	slog.Info("consumer left", "topic", topic, "consumer", m.id, "consumers", len(g.members))
	if len(g.members) == 0 {
		delete(s.groups, topic)
		return
	}
	s.rebalance(topic, g)
}

// rebalance gives shard i to member i % n and sends every member its
// shards. Call it with s.mu held. It never blocks: an assignment not yet
// sent is replaced by the new one.
func (s *Server) rebalance(topic string, g *group) {
	g.generation++
	n := len(g.members)
	for k, m := range g.members {
		a := &pb.Assignment{Topic: topic, Generation: g.generation}
		for shard := k; shard < g.numShards; shard += n {
			a.Shards = append(a.Shards, int32(shard))
		}
		select {
		case <-m.updates: // drop the unsent older one
		default:
		}
		m.updates <- a
		slog.Info("assigned shards", "topic", topic, "consumer", m.id, "generation", g.generation, "shards", a.Shards)
	}
}

package main

import (
	"context"
	"log/slog"
	"slices"
	pb "wisp/proto"
	"wisp/stream"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultGroup is the group of a request that doesn't name one.
const defaultGroup = "default"

type groupKey struct{ topic, name string }

// group is the consumers of one group of a topic, in join order.
type group struct {
	numShards  int
	generation uint64
	members    []*member
	owners     map[int]*member // shard -> its consumer
}

// member is one open Join stream.
type member struct {
	id      string
	updates chan *pb.Assignment // buffer 1: only the newest assignment matters
}

func newKey(topic, name string) groupKey {
	if name == "" {
		name = defaultGroup
	}
	return groupKey{topic, name}
}

// Join registers the consumer for as long as its stream is open. Each time
// a consumer joins or leaves the group, every consumer gets its new shards.
func (s *Server) Join(req *pb.JoinRequest, srv pb.EventBroker_JoinServer) error {
	if req.GetConsumerId() == "" {
		return status.Error(codes.InvalidArgument, "consumer_id is required")
	}
	key := newKey(req.GetTopic(), req.GetGroup())
	if err := stream.ValidateGroup(key.name); err != nil {
		return status.Error(codeFor(err), err.Error())
	}
	topicShards, err := s.topics.Topic(req.GetTopic())
	if err != nil {
		return status.Error(codeFor(err), err.Error())
	}

	m := &member{id: req.GetConsumerId(), updates: make(chan *pb.Assignment, 1)}
	if err := s.addMember(key, len(topicShards), m); err != nil {
		return err
	}
	defer s.removeMember(key, m)

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

// Commit stores the group's offset for one shard. It is refused unless the
// caller owns the shard in the group's current generation, so a consumer that
// lost the shard can't overwrite the new owner's offset.
func (s *Server) Commit(_ context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	topicShards, ok := s.topics.Lookup(req.GetTopic())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q does not exist", req.GetTopic())
	}
	shard := int(req.GetShard())
	if shard < 0 || shard >= len(topicShards) {
		return nil, status.Errorf(codes.InvalidArgument, "shard %d out of range (topic has %d)", shard, len(topicShards))
	}
	if end := topicShards[shard].NextID(); req.GetNextId() > end {
		return nil, status.Errorf(codes.OutOfRange, "next_id %d is after the end of the shard (%d)", req.GetNextId(), end)
	}

	key := newKey(req.GetTopic(), req.GetGroup())
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.groups[key]
	if g == nil || req.GetGeneration() != g.generation || g.owners[shard] == nil || g.owners[shard].id != req.GetConsumerId() {
		return nil, status.Errorf(codes.FailedPrecondition, "consumer %q doesn't own shard %d in this generation", req.GetConsumerId(), shard)
	}
	if err := s.topics.SaveOffset(key.topic, key.name, shard, req.GetNextId()); err != nil {
		return nil, status.Error(codeFor(err), err.Error())
	}
	return &pb.CommitResponse{}, nil
}

func (s *Server) addMember(key groupKey, numShards int, m *member) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.groups[key]
	if g == nil {
		g = &group{numShards: numShards}
		s.groups[key] = g
	}
	for _, other := range g.members {
		if other.id == m.id {
			return status.Errorf(codes.AlreadyExists, "consumer %q already joined group %q of topic %q", m.id, key.name, key.topic)
		}
	}
	g.members = append(g.members, m)
	slog.Info("consumer joined", "topic", key.topic, "group", key.name, "consumer", m.id, "consumers", len(g.members))
	s.rebalance(key.topic, g)
	return nil
}

func (s *Server) removeMember(key groupKey, m *member) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.groups[key]
	g.members = slices.DeleteFunc(g.members, func(other *member) bool { return other == m })
	slog.Info("consumer left", "topic", key.topic, "group", key.name, "consumer", m.id, "consumers", len(g.members))
	if len(g.members) == 0 {
		delete(s.groups, key)
		return
	}
	s.rebalance(key.topic, g)
}

// rebalance splits the shards evenly, moving as few as it can: each consumer
// keeps its shards up to its fair share, and only the rest are handed out.
// The extra shards of an uneven split go to the oldest members. It sends
// every member its shards. Call it with s.mu held. It never blocks: an
// assignment not yet sent is replaced by the new one.
func (s *Server) rebalance(topic string, g *group) {
	g.generation++
	n := len(g.members)
	share := make(map[*member]int, n)
	for k, m := range g.members {
		share[m] = g.numShards / n
		if k < g.numShards%n {
			share[m]++
		}
	}

	owners := make(map[int]*member, g.numShards)
	var free []int
	for shard := range g.numShards {
		if old := g.owners[shard]; share[old] > 0 {
			share[old]--
			owners[shard] = old
		} else {
			free = append(free, shard)
		}
	}
	for _, m := range g.members {
		for ; share[m] > 0; share[m]-- {
			owners[free[0]] = m
			free = free[1:]
		}
	}
	g.owners = owners

	for _, m := range g.members {
		a := &pb.Assignment{Topic: topic, Generation: g.generation}
		for shard := range g.numShards {
			if owners[shard] == m {
				a.Shards = append(a.Shards, int32(shard))
			}
		}
		select {
		case <-m.updates: // drop the unsent older one
		default:
		}
		m.updates <- a
		slog.Info("assigned shards", "topic", topic, "consumer", m.id, "generation", g.generation, "shards", a.Shards)
	}
}

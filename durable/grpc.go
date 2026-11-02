package durable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
	pb "wisp/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// journalProducer is the only producer of a run's topic. The shard's
	// duplicate check on it is what makes an append conditional, and a shard
	// can't forget its only producer.
	journalProducer = 1
	// ackTimeout bounds one append, so a broker that hangs counts as an
	// unknown outcome and is retried.
	ackTimeout = 10 * time.Second
)

// GRPC is a Transport over Wisp's broker. It sends one record per Publish
// request, because the records of a request aren't atomic across a crash. It
// isn't safe for concurrent use, and Close must not overlap Append.
type GRPC struct {
	client pb.EventBrokerClient

	stream pb.EventBroker_PublishClient
	cancel context.CancelFunc
	nextID uint64 // request id; the broker only echoes it
}

// NewGRPC returns a Transport using client.
func NewGRPC(client pb.EventBrokerClient) *GRPC {
	return &GRPC{client: client}
}

// Close ends the Publish stream, if there is one.
func (g *GRPC) Close() {
	g.dropStream()
}

func (g *GRPC) dropStream() {
	if g.cancel != nil {
		g.cancel()
	}
	g.stream, g.cancel = nil, nil
}

// Create implements Transport with a one-shard topic.
func (g *GRPC) Create(ctx context.Context, topic string) error {
	_, err := g.client.DescribeTopic(ctx, &pb.DescribeTopicRequest{Topic: topic, NumShards: 1})
	return permanentIf(err, codes.InvalidArgument)
}

// Read implements Transport with a Subscribe that stops at the end of the
// shard.
func (g *GRPC) Read(ctx context.Context, topic string, from uint64) ([]Stored, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sub, err := g.client.Subscribe(ctx, &pb.SubscribeRequest{Topic: topic, StartId: from, StopAtEnd: true})
	if err != nil {
		return nil, readError(err)
	}
	var stored []Stored
	for {
		batch, err := sub.Recv()
		if errors.Is(err, io.EOF) {
			return stored, nil
		}
		if err != nil {
			return nil, readError(err)
		}
		for _, e := range batch.GetEvents() {
			stored = append(stored, Stored{EventID: e.GetEventId(), Payload: e.GetPayload()})
		}
	}
}

func readError(err error) error {
	if status.Code(err) == codes.NotFound {
		return ErrNoJournal
	}
	return permanentIf(err, codes.InvalidArgument, codes.OutOfRange)
}

// permanentIf wraps err in a PermanentError if its code is one of permanent.
func permanentIf(err error, permanent ...codes.Code) error {
	if err == nil {
		return nil
	}
	code := status.Code(err)
	for _, c := range permanent {
		if code == c {
			return &PermanentError{err}
		}
	}
	return err
}

// Append implements Transport. The sequence of the journal's only producer is
// the position, so the shard stores the record only if it follows the last one
// (see checkSequences in shards/shard.go).
//
// Any stream error drops the stream, and the journal retries on a new one. A
// request still queued from the old stream is safe: the sequence lets only one
// copy be stored.
func (g *GRPC) Append(ctx context.Context, topic string, seq uint64, payload []byte) (uint64, error) {
	if g.stream == nil {
		streamCtx, cancel := context.WithCancel(context.Background())
		stream, err := g.client.Publish(streamCtx)
		if err != nil {
			cancel()
			return 0, err
		}
		g.stream, g.cancel = stream, cancel
	}

	// Recv can't take a context, so a late ack or a cancelled ctx ends the
	// stream instead.
	stop := context.AfterFunc(ctx, g.cancel)
	defer stop()
	timer := time.AfterFunc(ackTimeout, g.cancel)
	defer timer.Stop()

	g.nextID++
	err := g.stream.Send(&pb.PublishRequest{
		RequestId: g.nextID, Topic: topic, Payloads: [][]byte{payload},
		ProducerId: journalProducer, FirstSequence: seq,
	})
	var ack *pb.PublishAck
	if err == nil {
		ack, err = g.stream.Recv()
	}
	if err != nil {
		g.dropStream()
		return 0, err
	}
	return ackResult(ack)
}

func ackResult(ack *pb.PublishAck) (uint64, error) {
	code := codes.Code(ack.GetErrorCode())
	switch code {
	case codes.OK:
		if ack.GetCount() != 1 {
			return 0, &PermanentError{fmt.Errorf("durable: ack stored %d records, want 1", ack.GetCount())}
		}
		return ack.GetFirstEventId(), nil
	case codes.AlreadyExists:
		return 0, ErrPositionTaken
	case codes.FailedPrecondition:
		return 0, ErrPositionAhead
	case codes.Unavailable, codes.Internal, codes.ResourceExhausted:
		return 0, status.Error(code, ack.GetErrorMessage())
	default:
		return 0, &PermanentError{status.Error(code, ack.GetErrorMessage())}
	}
}

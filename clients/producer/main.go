// Producer reads JSON objects from stdin, one per line, and publishes them
// over one Publish stream. Each line's shard is hash(key) % shards, computed
// here, so lines with the same key go to the same shard, in order.
//
// Usage:
//
//	echo '{"temp":21}' | go run ./clients/producer -topic sensors
//	go run ./clients/producer -topic sensors -key-field device < events.jsonl
package main

// TODO: Make an SDK for like python

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"wisp/partition"
	pb "wisp/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// defaultAddr is the address the broker listens on (see main.go at the repo root).
const defaultAddr = "localhost:50051"

func main() {
	addr := flag.String("addr", defaultAddr, "broker address")
	topic := flag.String("topic", "", "topic to publish to (required)")
	keyField := flag.String("key-field", "", "JSON field whose value is the partition key (empty: every line has the same key)")
	flag.Parse()
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "producer: -topic is required")
		flag.Usage()
		os.Exit(2)
	}

	// Ctrl+C stops reading and exits cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("could not create client", "addr", *addr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	sent, err := run(ctx, pb.NewEventBrokerClient(conn), *topic, *keyField, os.Stdin)
	slog.Info("producer done", "topic", *topic, "published", sent)
	if err != nil {
		slog.Error("producer stopped", "err", err)
		os.Exit(1)
	}
}

// run publishes every non-empty line of in and returns how many were stored.
// The request id is the line number. Lines rejected as invalid are skipped;
// any other error stops run.
func run(ctx context.Context, client pb.EventBrokerClient, topic, keyField string, in io.Reader) (int, error) {
	info, err := client.DescribeTopic(ctx, &pb.DescribeTopicRequest{Topic: topic})
	if err != nil {
		return 0, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	numShards := int(info.GetNumShards())
	if numShards <= 0 {
		return 0, fmt.Errorf("describe topic %q: broker reported %d shards", topic, numShards)
	}

	// Cancelling streamCtx unblocks Send if receiving acks fails.
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Publish(streamCtx)
	if err != nil {
		return 0, fmt.Errorf("open publish stream: %w", err)
	}

	type ackResult struct {
		stored int
		err    error
	}
	acks := make(chan ackResult, 1)
	go func() {
		stored, err := receiveAcks(stream)
		if err != nil {
			cancel()
		}
		acks <- ackResult{stored, err}
	}()

	sendErr := sendLines(stream, topic, keyField, numShards, in)
	stream.CloseSend() // always nil
	res := <-acks

	switch {
	case ctx.Err() != nil:
		return res.stored, ctx.Err()
	case res.err != nil:
		return res.stored, res.err
	case sendErr != nil && !errors.Is(sendErr, io.EOF):
		return res.stored, sendErr
	}
	return res.stored, nil
}

// sendLines sends each non-empty line. io.EOF means the stream ended early
// (Recv has the reason).
func sendLines(stream pb.EventBroker_PublishClient, topic, keyField string, numShards int, in io.Reader) error {
	scanner := bufio.NewScanner(in)
	for line := 1; scanner.Scan(); line++ {
		payload := []byte(strings.TrimSpace(scanner.Text()))
		if len(payload) == 0 {
			continue
		}
		req := &pb.PublishRequest{
			RequestId: uint64(line),
			Topic:     topic,
			Shard:     int32(partition.Shard(partitionKey(payload, keyField), numShards)),
			Payload:   payload,
		}
		if err := stream.Send(req); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	return nil
}

// receiveAcks counts stored events until the stream ends.
func receiveAcks(stream pb.EventBroker_PublishClient) (int, error) {
	stored := 0
	for {
		ack, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stored, nil
		}
		if err != nil {
			return stored, err
		}
		code := codes.Code(ack.GetErrorCode())
		switch code {
		case codes.OK:
			stored++
			slog.Debug("event stored", "line", ack.GetRequestId(), "shard", ack.GetShard(), "event_id", ack.GetEventId())
		case codes.InvalidArgument:
			slog.Warn("broker rejected event, skipped", "line", ack.GetRequestId(), "err", ack.GetErrorMessage())
		default:
			return stored, fmt.Errorf("publish line %d: %w", ack.GetRequestId(), status.Error(code, ack.GetErrorMessage()))
		}
	}
}

// partitionKey returns the value of field keyField (strings without quotes,
// other values as JSON text), or an empty key if there is none.
func partitionKey(payload []byte, keyField string) []byte {
	if keyField == "" {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil
	}
	raw, ok := fields[keyField]
	if !ok {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return []byte(s)
	}
	return raw
}

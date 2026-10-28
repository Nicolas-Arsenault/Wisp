// Producer is a simple command-line producer for the Wisp broker. It reads
// events from stdin, one JSON object per line, and publishes each one to a
// topic.
//
// Usage:
//
//	echo '{"temp":21}' | go run ./clients/producer -topic sensors
//	go run ./clients/producer -topic sensors -addr localhost:50051 < events.jsonl

// TODO: Make an SDK for like python
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
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

	sent, err := run(ctx, pb.NewEventBrokerClient(conn), *topic, os.Stdin)
	slog.Info("producer done", "topic", *topic, "published", sent)
	if err != nil {
		slog.Error("producer stopped", "err", err)
		os.Exit(1)
	}
}

// run publishes every non-empty line of in to topic, in order, and returns
// how many events the broker accepted.
//
// Edge cases:
//   - empty and whitespace-only lines are skipped.
//   - a line the broker rejects as invalid (not a JSON object) is logged and
//     skipped; the next lines are still published.
//   - any other publish error (broker down, write failure on the broker)
//     stops run and is returned, since the following events would fail too.
//   - a cancelled ctx stops run with ctx's error.
func run(ctx context.Context, client pb.EventBrokerClient, topic string, in io.Reader) (int, error) {
	sent := 0
	scanner := bufio.NewScanner(in)
	for line := 1; scanner.Scan(); line++ {
		payload := strings.TrimSpace(scanner.Text())
		if payload == "" {
			continue
		}
		_, err := client.PublishEvent(ctx, &pb.EventRequest{Topic: topic, Payload: []byte(payload)})
		if status.Code(err) == codes.InvalidArgument {
			slog.Warn("broker rejected event, skipped", "line", line, "err", status.Convert(err).Message())
			continue
		}
		if err != nil {
			return sent, fmt.Errorf("publish line %d: %w", line, err)
		}
		sent++
	}
	if err := scanner.Err(); err != nil {
		return sent, fmt.Errorf("read input: %w", err)
	}
	return sent, nil
}

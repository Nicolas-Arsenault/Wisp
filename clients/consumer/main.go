// Consumer is a simple command-line consumer for the Wisp broker. It
// subscribes to a topic and prints every event published on it from then on,
// one per line, until Ctrl+C.
//
// Output format (tab separated, on stdout; logs go to stderr):
//
//	<event_id>	<timestamp, RFC 3339 UTC>	<payload>
//	sensors/0/0	2026-10-27T21:40:00.123Z	{"seq_n":0,"temp":21}
//
// Usage:
//
//	go run ./clients/consumer -topic sensors
package main

//TODO: Save the last offset saved for the topic if we have already requetsed the topic subscription before

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"time"
	pb "wisp/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// defaultAddr is the address the broker listens on (see main.go at the repo root).
const defaultAddr = "localhost:50051"

// timeLayout is how event timestamps are printed: RFC 3339 with milliseconds.
const timeLayout = "2006-01-02T15:04:05.000Z07:00"

func main() {
	addr := flag.String("addr", defaultAddr, "broker address")
	topic := flag.String("topic", "", "topic to subscribe to (required)")
	flag.Parse()
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "consumer: -topic is required")
		flag.Usage()
		os.Exit(2)
	}

	// Ctrl+C ends the subscription and exits cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("could not create client", "addr", *addr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	slog.Info("consumer subscribing", "addr", *addr, "topic", *topic)
	received, err := run(ctx, pb.NewEventBrokerClient(conn), *topic, os.Stdout)
	slog.Info("consumer done", "topic", *topic, "received", received)
	if err != nil {
		slog.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}

// run subscribes to topic and writes every received event to out (see the
// package comment for the format). It returns how many events it wrote.
//
// It returns nil when ctx is cancelled (normal shutdown) or when the broker
// ends the stream. Any other error (broker unreachable, connection lost,
// failing to write to out) is returned.
func run(ctx context.Context, client pb.EventBrokerClient, topic string, out io.Writer) (int, error) {
	sub, err := client.SubscribeTopic(ctx, &pb.SubscribeRequest{Topic: topic})
	if err != nil {
		return 0, fmt.Errorf("subscribe to %q: %w", topic, err)
	}

	received := 0
	for {
		event, err := sub.Recv()
		if errors.Is(err, io.EOF) {
			return received, nil // the broker closed the stream
		}
		if ctx.Err() != nil && status.Code(err) == codes.Canceled {
			return received, nil // we were asked to stop
		}
		if err != nil {
			return received, fmt.Errorf("receive from %q: %w", topic, err)
		}
		if _, err := fmt.Fprintln(out, formatEvent(event)); err != nil {
			return received, fmt.Errorf("write event: %w", err)
		}
		received++
	}
}

// formatEvent renders event as one output line, without the trailing newline.
func formatEvent(event *pb.EventResponse) string {
	ts := time.UnixMilli(event.GetTimestamp()).UTC().Format(timeLayout)
	return event.GetEventId() + "\t" + ts + "\t" + string(event.GetPayload())
}

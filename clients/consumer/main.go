// Consumer prints one shard of a topic from an event id (offset): first the
// stored events, then new ones as they come, until Ctrl+C.
//
// Output (tab separated, on stdout; logs go to stderr):
//
//	<topic>/<shard>/<event_id>	<timestamp, RFC 3339 UTC>	<payload>
//	sensors/0/0	2026-10-27T21:40:00.123Z	{"temp":21}
//
// Usage:
//
//	go run ./clients/consumer -topic sensors -shard 0              # from the start
//	go run ./clients/consumer -topic sensors -shard 0 -offset 42   # from event 42
//	go run ./clients/consumer -topic sensors -shard 0 -latest      # new events only
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
	"strconv"
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

type options struct {
	topic    string
	shard    int
	offset   uint64 // first event id; ignored if latest
	latest   bool   // only new events
	maxBytes uint   // max payload bytes per batch (0 = broker default)
}

func main() {
	addr := flag.String("addr", defaultAddr, "broker address")
	var opts options
	flag.StringVar(&opts.topic, "topic", "", "topic to read (required)")
	flag.IntVar(&opts.shard, "shard", -1, "shard to read (required)")
	flag.Uint64Var(&opts.offset, "offset", 0, "event id to start at")
	flag.BoolVar(&opts.latest, "latest", false, "start at the end of the shard (only new events); overrides -offset")
	flag.UintVar(&opts.maxBytes, "max-bytes", 0, "max payload bytes per batch received (0 = broker default, 1 MiB)")
	flag.Parse()
	if opts.topic == "" || opts.shard < 0 {
		fmt.Fprintln(os.Stderr, "consumer: -topic and -shard are required")
		flag.Usage()
		os.Exit(2)
	}
	if opts.maxBytes > 1<<32-1 {
		fmt.Fprintln(os.Stderr, "consumer: -max-bytes is too large")
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

	slog.Info("consumer subscribing", "addr", *addr, "topic", opts.topic, "shard", opts.shard, "offset", opts.offset, "latest", opts.latest)
	received, err := run(ctx, pb.NewEventBrokerClient(conn), opts, os.Stdout)
	slog.Info("consumer done", "topic", opts.topic, "shard", opts.shard, "received", received)
	if err != nil {
		slog.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}

// run prints every received event to out and returns how many it printed.
// Ctrl+C (ctx cancelled) and the broker ending the stream return nil.
func run(ctx context.Context, client pb.EventBrokerClient, opts options, out io.Writer) (int, error) {
	sub, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:    opts.topic,
		Shard:    int32(opts.shard),
		StartId:  opts.offset,
		Latest:   opts.latest,
		MaxBytes: uint32(opts.maxBytes),
	})
	if err != nil {
		return 0, fmt.Errorf("subscribe to %s/%d: %w", opts.topic, opts.shard, err)
	}

	received := 0
	for {
		batch, err := sub.Recv()
		if errors.Is(err, io.EOF) {
			return received, nil // the broker closed the stream
		}
		if ctx.Err() != nil && status.Code(err) == codes.Canceled {
			return received, nil // we were asked to stop
		}
		if err != nil {
			return received, fmt.Errorf("receive from %s/%d: %w", opts.topic, opts.shard, err)
		}
		for _, event := range batch.GetEvents() {
			if _, err := fmt.Fprintln(out, formatEvent(batch, event)); err != nil {
				return received, fmt.Errorf("write event: %w", err)
			}
			received++
		}
	}
}

// formatEvent renders one output line, without the newline.
func formatEvent(batch *pb.EventBatch, event *pb.Event) string {
	id := batch.GetTopic() + "/" + strconv.Itoa(int(batch.GetShard())) + "/" + strconv.FormatUint(event.GetEventId(), 10)
	ts := time.UnixMilli(event.GetTimestamp()).UTC().Format(timeLayout)
	return id + "\t" + ts + "\t" + string(event.GetPayload())
}

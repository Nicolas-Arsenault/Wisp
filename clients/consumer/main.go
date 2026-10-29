// Consumer joins a topic and prints the events of the shards the broker
// assigns to it, until Ctrl+C. When other consumers join or leave the topic,
// the broker splits the shards again and this consumer follows its new
// assignment.
//
// Output (tab separated, on stdout; logs go to stderr):
//
//	<topic>/<shard>/<event_id>	<timestamp, RFC 3339 UTC>	<payload>
//	sensors/0/0	2026-10-27T21:40:00.123Z	{"temp":21}
//
// Usage:
//
//	go run ./clients/consumer -topic sensors -id a              # each new shard from the start
//	go run ./clients/consumer -topic sensors -id b -latest      # new events only
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
	"slices"
	"strconv"
	"sync"
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
	id       string // consumer id, unique in the topic
	offset   uint64 // where to start reading an assigned shard; ignored if latest
	latest   bool   // only new events
	maxBytes uint   // max payload bytes per batch (0 = broker default)
}

func main() {
	addr := flag.String("addr", defaultAddr, "broker address")
	hostname, _ := os.Hostname()
	var opts options
	flag.StringVar(&opts.topic, "topic", "", "topic to read (required)")
	flag.StringVar(&opts.id, "id", hostname+"-"+strconv.Itoa(os.Getpid()), "consumer id, unique in the topic")
	flag.Uint64Var(&opts.offset, "offset", 0, "event id to start at in each assigned shard")
	flag.BoolVar(&opts.latest, "latest", false, "start at the end of each assigned shard (only new events); overrides -offset")
	flag.UintVar(&opts.maxBytes, "max-bytes", 0, "max payload bytes per batch received (0 = broker default, 1 MiB)")
	flag.Parse()
	if opts.topic == "" {
		fmt.Fprintln(os.Stderr, "consumer: -topic is required")
		flag.Usage()
		os.Exit(2)
	}
	if opts.maxBytes > 1<<32-1 {
		fmt.Fprintln(os.Stderr, "consumer: -max-bytes is too large")
		os.Exit(2)
	}

	// Ctrl+C leaves the topic and exits cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("could not create client", "addr", *addr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	slog.Info("consumer joining", "addr", *addr, "topic", opts.topic, "id", opts.id)
	received, err := run(ctx, pb.NewEventBrokerClient(conn), opts, os.Stdout)
	slog.Info("consumer done", "topic", opts.topic, "received", received)
	if err != nil {
		slog.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}

// run joins the topic and, for each assignment, reads the assigned shards
// (one Subscribe per shard) and stops reading the others. Every event is
// printed to out by one goroutine, so lines never mix. It returns how many
// events it printed.
//
// Ctrl+C (ctx cancelled) and the broker ending the stream return nil. Any
// other error, from Join, a shard or out, stops run and is returned.
func run(parent context.Context, client pb.EventBrokerClient, opts options, out io.Writer) (int, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	join, err := client.Join(ctx, &pb.JoinRequest{Topic: opts.topic, ConsumerId: opts.id})
	if err != nil {
		return 0, fmt.Errorf("join %q: %w", opts.topic, err)
	}

	lines := make(chan string, 64)
	printed := 0
	var printErr error
	printDone := make(chan struct{})
	go func() {
		defer close(printDone)
		for line := range lines {
			if printErr != nil {
				continue // keep draining so readers never block
			}
			if _, err := fmt.Fprintln(out, line); err != nil {
				printErr = fmt.Errorf("write event: %w", err)
				cancel()
				continue
			}
			printed++
		}
	}()

	var wg sync.WaitGroup
	shardErr := make(chan error, 1)
	readers := make(map[int32]context.CancelFunc) // shard -> stop reading it
	var joinErr error
	for {
		a, err := join.Recv()
		if err != nil {
			joinErr = err
			break
		}
		slog.Info("assigned shards", "topic", opts.topic, "generation", a.GetGeneration(), "shards", a.GetShards())

		for shard, stop := range readers {
			if !slices.Contains(a.GetShards(), shard) {
				stop()
				delete(readers, shard)
			}
		}
		for _, shard := range a.GetShards() {
			if readers[shard] != nil {
				continue
			}
			shardCtx, stop := context.WithCancel(ctx)
			readers[shard] = stop
			wg.Go(func() {
				err := readShard(shardCtx, client, opts, shard, lines)
				if err != nil && shardCtx.Err() == nil {
					select {
					case shardErr <- err:
					default:
					}
					cancel()
				}
			})
		}
	}

	cancel()
	wg.Wait()
	close(lines)
	<-printDone

	if printErr != nil {
		return printed, printErr
	}
	select {
	case err := <-shardErr:
		return printed, err
	default:
	}
	if parent.Err() != nil || errors.Is(joinErr, io.EOF) {
		return printed, nil
	}
	return printed, fmt.Errorf("join %q: %w", opts.topic, joinErr)
}

// readShard sends every event of one shard to lines, from opts.offset (or
// the end if opts.latest), until ctx is cancelled.
func readShard(ctx context.Context, client pb.EventBrokerClient, opts options, shard int32, lines chan<- string) error {
	sub, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:    opts.topic,
		Shard:    shard,
		StartId:  opts.offset,
		Latest:   opts.latest,
		MaxBytes: uint32(opts.maxBytes),
	})
	if err != nil {
		return fmt.Errorf("subscribe to %s/%d: %w", opts.topic, shard, err)
	}
	for {
		batch, err := sub.Recv()
		if errors.Is(err, io.EOF) || (ctx.Err() != nil && status.Code(err) == codes.Canceled) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive from %s/%d: %w", opts.topic, shard, err)
		}
		for _, event := range batch.GetEvents() {
			select {
			case lines <- formatEvent(batch, event):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// formatEvent renders one output line, without the newline.
func formatEvent(batch *pb.EventBatch, event *pb.Event) string {
	id := batch.GetTopic() + "/" + strconv.Itoa(int(batch.GetShard())) + "/" + strconv.FormatUint(event.GetEventId(), 10)
	ts := time.UnixMilli(event.GetTimestamp()).UTC().Format(timeLayout)
	return id + "\t" + ts + "\t" + string(event.GetPayload())
}

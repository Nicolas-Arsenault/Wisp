// Consumer joins a group of a topic and prints the events of the shards the
// broker assigns to it, until Ctrl+C. When other consumers join or leave the
// group, the broker splits the shards again and this consumer follows its new
// assignment.
//
// After printing a batch it commits its offset to the broker, so the group
// resumes there, whoever reads the shard next. A record can be printed twice
// (crash or rebalance before the commit) but is never skipped.
//
// Output (tab separated, on stdout; logs go to stderr):
//
//	<topic>/<shard>/<event_id>	<timestamp, RFC 3339 UTC>	<payload>
//	sensors/0/0	2026-10-27T21:40:00.123Z	{"temp":21}
//
// Usage:
//
//	go run ./clients/consumer -topic sensors -id a                 # group "default"
//	go run ./clients/consumer -topic sensors -id a -group audit    # its own copy of the topic
//	go run ./clients/consumer -topic sensors -id b -latest         # new group: new events only
package main

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
	"sync/atomic"
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

// defaultGroup is the group consumers share unless they name another.
const defaultGroup = "default"

type options struct {
	topic    string
	id       string // consumer id, unique in the group
	group    string // who shares the shards; each group reads the whole topic
	offset   uint64 // where to start a shard the group has no offset for; ignored if latest
	latest   bool   // start at the end of a shard the group has no offset for
	maxBytes uint   // max payload bytes per batch (0 = broker default)
}

// line is one output line. If commit isn't 0, it ends a batch, and commit is
// the shard's offset once it's printed.
type line struct {
	text   string
	shard  int32
	commit uint64
}

func main() {
	addr := flag.String("addr", defaultAddr, "broker address")
	hostname, _ := os.Hostname()
	var opts options
	flag.StringVar(&opts.topic, "topic", "", "topic to read (required)")
	flag.StringVar(&opts.id, "id", hostname+"-"+strconv.Itoa(os.Getpid()), "consumer id, unique in the group")
	flag.StringVar(&opts.group, "group", defaultGroup, "consumer group; each group reads the whole topic")
	flag.Uint64Var(&opts.offset, "offset", 0, "event id to start at in a shard the group has no offset for")
	flag.BoolVar(&opts.latest, "latest", false, "start at the end of a shard the group has no offset for; overrides -offset")
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

	slog.Info("consumer joining", "addr", *addr, "topic", opts.topic, "group", opts.group, "id", opts.id)
	received, err := run(ctx, pb.NewEventBrokerClient(conn), opts, os.Stdout)
	slog.Info("consumer done", "topic", opts.topic, "received", received)
	if err != nil {
		slog.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}

// run joins the group and, for each assignment, reads the assigned shards
// (one Subscribe per shard) and stops reading the others. Every event is
// printed to out by one goroutine, so lines never mix, and that goroutine
// commits each batch once it's printed. It returns how many events it printed.
//
// Ctrl+C (ctx cancelled) and the broker ending the stream return nil. Any
// other error, from Join, a shard or out, stops run and is returned.
func run(parent context.Context, client pb.EventBrokerClient, opts options, out io.Writer) (int, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	join, err := client.Join(ctx, &pb.JoinRequest{Topic: opts.topic, ConsumerId: opts.id, Group: opts.group})
	if err != nil {
		return 0, fmt.Errorf("join %q: %w", opts.topic, err)
	}

	var generation atomic.Uint64 // of the latest assignment
	lines := make(chan line, 64)
	printed := 0
	var printErr error
	printDone := make(chan struct{})
	go func() {
		defer close(printDone)
		for l := range lines {
			if printErr != nil {
				continue // keep draining so readers never block
			}
			if _, err := fmt.Fprintln(out, l.text); err != nil {
				printErr = fmt.Errorf("write event: %w", err)
				cancel()
				continue
			}
			printed++
			if l.commit != 0 {
				commit(ctx, client, opts, generation.Load(), l)
			}
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
		generation.Store(a.GetGeneration())

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

// commit stores the offset of l. A failure isn't fatal: the records after the
// last commit are read again, by this consumer or by the shard's next owner.
func commit(ctx context.Context, client pb.EventBrokerClient, opts options, generation uint64, l line) {
	_, err := client.Commit(ctx, &pb.CommitRequest{
		Topic:      opts.topic,
		Group:      opts.group,
		ConsumerId: opts.id,
		Generation: generation,
		Shard:      l.shard,
		NextId:     l.commit,
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("could not commit offset", "topic", opts.topic, "shard", l.shard, "next_id", l.commit, "err", err)
	}
}

// readShard sends every event of one shard to lines, from the group's stored
// offset, until ctx is cancelled. opts.offset (or the end if opts.latest) is
// used if the group has none.
func readShard(ctx context.Context, client pb.EventBrokerClient, opts options, shard int32, lines chan<- line) error {
	sub, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:    opts.topic,
		Shard:    shard,
		StartId:  opts.offset,
		Latest:   opts.latest,
		MaxBytes: uint32(opts.maxBytes),
		Group:    opts.group,
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
		events := batch.GetEvents()
		for i, event := range events {
			l := line{text: formatEvent(batch, event), shard: shard}
			if i == len(events)-1 {
				l.commit = event.GetEventId() + 1
			}
			select {
			case lines <- l:
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

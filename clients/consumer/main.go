// Consumer joins a group of a topic and prints the events of the shards the
// broker assigns to it, until Ctrl+C. When other consumers join or leave the
// group, the broker splits the shards again and this consumer follows its new
// assignment.
//
// Every second, and when it stops, it commits what it has printed to the
// broker, so the group resumes there, whoever reads the shard next. A record
// can be printed twice (crash or rebalance before a commit, up to a second of
// records) but is never skipped.
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
	"bufio"
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

const (
	outBuffer     = 64 << 10 // bytes of output written at once
	commitTimeout = 2 * time.Second
)

// commitInterval is how often offsets are committed. Tests shorten it.
var commitInterval = time.Second

type options struct {
	topic    string
	id       string // consumer id, unique in the group
	group    string // who shares the shards; each group reads the whole topic
	offset   uint64 // where to start a shard the group has no offset for; ignored if latest
	latest   bool   // start at the end of a shard the group has no offset for
	maxBytes uint   // max payload bytes per batch (0 = broker default)
}

// line is one output line. If commit isn't 0, it ends a batch, and commit is
// the shard's offset once it's written out.
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
// commits offsets. It returns how many events it printed.
//
// Ctrl+C (ctx cancelled) and the broker ending the stream return nil. Any
// other error, from Join, a shard or out, stops run and is returned.
func run(parent context.Context, client pb.EventBrokerClient, opts options, out io.Writer) (int, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Membership outlives ctx: a commit is refused once the consumer has left,
	// so the last one must happen first.
	memberCtx, leave := context.WithCancel(context.WithoutCancel(parent))
	defer leave()
	stopWatch := context.AfterFunc(ctx, leave) // Ctrl+C while joining
	join, err := client.Join(memberCtx, &pb.JoinRequest{Topic: opts.topic, ConsumerId: opts.id, Group: opts.group})
	stopWatch()
	if err != nil {
		return 0, fmt.Errorf("join %q: %w", opts.topic, err)
	}

	var generation atomic.Uint64 // of the latest assignment
	lines := make(chan line, 64)
	p := &printer{
		out:     bufio.NewWriterSize(out, outBuffer),
		offsets: make(map[int32]uint64),
		commit: func(shard int32, next uint64) {
			commit(memberCtx, client, opts, generation.Load(), shard, next)
		},
		stop: cancel,
	}
	printDone := make(chan struct{})
	go func() {
		defer close(printDone)
		p.run(lines)
	}()

	assignments := make(chan *pb.Assignment)
	joinEnded := make(chan error, 1)
	go func() {
		for {
			a, err := join.Recv()
			if err != nil {
				joinEnded <- err
				return
			}
			select {
			case assignments <- a:
			case <-memberCtx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	shardErr := make(chan error, 1)
	readers := make(map[int32]context.CancelFunc) // shard -> stop reading it
	var joinErr error
loop:
	for {
		select {
		case a := <-assignments:
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
		case joinErr = <-joinEnded:
			break loop
		case <-ctx.Done():
			break loop
		}
	}

	cancel()
	wg.Wait()
	close(lines)
	<-printDone
	leave()

	if p.err != nil {
		return p.printed, p.err
	}
	select {
	case err := <-shardErr:
		return p.printed, err
	default:
	}
	if parent.Err() != nil || joinErr == nil || errors.Is(joinErr, io.EOF) {
		return p.printed, nil
	}
	return p.printed, fmt.Errorf("join %q: %w", opts.topic, joinErr)
}

// printer writes lines and commits their offsets. Output is buffered, so an
// offset is only committed after the lines before it are flushed: otherwise a
// crash could skip records that were never written.
type printer struct {
	out      *bufio.Writer
	offsets  map[int32]uint64 // shard -> offset not yet committed
	commit   func(shard int32, next uint64)
	stop     func() // called when out fails
	buffered int    // lines written but not flushed
	printed  int
	err      error
}

// run prints lines until it's closed, then commits a last time.
func (p *printer) run(lines <-chan line) {
	ticker := time.NewTicker(commitInterval)
	defer ticker.Stop()
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				p.commitAll()
				return
			}
			p.print(l)
			if len(lines) == 0 {
				p.flush()
			}
		case <-ticker.C:
			p.commitAll()
		}
	}
}

func (p *printer) print(l line) {
	if p.err != nil {
		return // keep draining so readers never block
	}
	if _, err := fmt.Fprintln(p.out, l.text); err != nil {
		p.fail(err)
		return
	}
	p.buffered++
	if l.commit != 0 {
		p.offsets[l.shard] = l.commit
	}
}

func (p *printer) flush() {
	if p.err != nil {
		return
	}
	if err := p.out.Flush(); err != nil {
		p.fail(err)
		return
	}
	p.printed += p.buffered
	p.buffered = 0
}

func (p *printer) fail(err error) {
	p.err = fmt.Errorf("write event: %w", err)
	p.stop()
}

// commitAll flushes, then commits every shard with something new.
func (p *printer) commitAll() {
	p.flush()
	if p.err != nil {
		return
	}
	for shard, next := range p.offsets {
		p.commit(shard, next)
	}
	clear(p.offsets)
}

// commit stores one offset. A failure isn't fatal: the records after the last
// commit are read again, by this consumer or by the shard's next owner.
func commit(ctx context.Context, client pb.EventBrokerClient, opts options, generation uint64, shard int32, next uint64) {
	ctx, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()
	_, err := client.Commit(ctx, &pb.CommitRequest{
		Topic:      opts.topic,
		Group:      opts.group,
		ConsumerId: opts.id,
		Generation: generation,
		Shard:      shard,
		NextId:     next,
	})
	if err != nil {
		slog.Warn("could not commit offset", "topic", opts.topic, "shard", shard, "next_id", next, "err", err)
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

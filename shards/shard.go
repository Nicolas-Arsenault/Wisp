package shards

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	filePerm = 0o644
	dirPerm  = 0o755

	// A shard's files are <dataDir>/<topic>/<id>.log and <id>.index.
	LogExt   = ".log"
	IndexExt = ".index"

	queueSize     = 1024    // records waiting per shard; Enqueue blocks when full
	maxBatchBytes = 8 << 20 // max bytes written per fsync
)

// Shard is one append-only log. A single writer goroutine appends the
// records it takes from the queue, so no lock is needed. Readers only read
// records below committed, which the writer sets after a record is on disk.
type Shard struct {
	topic string
	id    int
	path  string

	log *os.File
	idx *index

	queue      chan appendRequest
	quit       chan struct{} // closed by Close
	writerDone chan struct{}
	closeOnce  sync.Once
	closeErr   error

	committed atomic.Uint64                 // number of stored records = next event id
	logEnd    atomic.Int64                  // size of the .log holding those records
	indexed   atomic.Uint64                 // number of entries in the .index
	notify    atomic.Pointer[chan struct{}] // closed after each commit to wake Wait

	lastIndexed int64 // .log position of the last index entry (writer only)
}

type appendRequest struct {
	body []byte // compressed payload
	done chan appendResult
}

type appendResult struct {
	id  uint64
	err error
}

// Pending is a queued record. Wait returns its event id once it's on disk.
type Pending struct {
	shard *Shard
	done  chan appendResult
}

// Open opens (or creates) shard id of topic, fixes its files after a crash
// and starts its writer.
func Open(dataDir, topic string, id int) (*Shard, error) {
	dir := filepath.Join(dataDir, topic)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	base := filepath.Join(dir, strconv.Itoa(id))

	logFile, err := os.OpenFile(base+LogExt, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return nil, err
	}
	idx, err := openIndex(base + IndexExt)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	s := &Shard{
		topic:      topic,
		id:         id,
		path:       base + LogExt,
		log:        logFile,
		idx:        idx,
		queue:      make(chan appendRequest, queueSize),
		quit:       make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	ch := make(chan struct{})
	s.notify.Store(&ch)

	if err := s.recover(); err != nil {
		idx.close()
		logFile.Close()
		return nil, err
	}
	go s.run()
	return s, nil
}

// recover makes the .index match the .log, which is the source of truth.
// It reads the .log from the last index entry to the end, adding the
// entries that are missing, and cuts off a torn frame at the end (a write
// that was never acked).
func (s *Shard) recover() error {
	info, err := s.log.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	last := s.idx.lastEntry()
	fromID, fromPos := s.idx.entry(last)
	id, pos, err := s.scan(last, fromID, fromPos, size)
	if s.indexed.Load() == last && last > 0 {
		// The last entry doesn't point at its frame: rebuild the index.
		log().Warn("rebuilding index", "file", s.path)
		id, pos, err = s.scan(0, 0, 0, size)
	}
	if err != nil && !errors.Is(err, ErrCorruptRecord) {
		return err
	}

	if pos < size {
		log().Warn("cutting torn end of log", "file", s.path, "bytes", size-pos)
		if err := s.log.Truncate(pos); err != nil {
			return err
		}
	}
	for i := s.indexed.Load(); i < s.idx.capacity; i++ {
		if eid, epos := s.idx.entry(i); eid != 0 || epos != 0 {
			s.idx.set(i, 0, 0)
		}
	}
	s.logEnd.Store(pos)
	s.committed.Store(id)
	return nil
}

// scan reads frames from pos (the frame of event id, which is index entry
// entry) to size, and re-adds the index entries from there. It returns the
// id and position after the last good frame.
func (s *Shard) scan(entry, id uint64, pos, size int64) (uint64, int64, error) {
	s.indexed.Store(entry)
	r := bufio.NewReader(io.NewSectionReader(s.log, pos, size-pos))
	for pos < size {
		frameID, _, body, err := readFrame(r)
		if err != nil {
			return id, pos, err
		}
		if frameID != id {
			return id, pos, ErrCorruptRecord
		}
		if n := s.indexed.Load(); n == entry || pos-s.lastIndexed >= indexInterval {
			if n >= s.idx.capacity {
				return id, pos, ErrShardFull
			}
			s.idx.set(n, id, pos)
			s.indexed.Store(n + 1)
			s.lastIndexed = pos
		}
		pos += frameSize(len(body))
		id++
	}
	return id, pos, nil
}

// Enqueue validates and compresses payload, then puts it on the queue,
// blocking while the queue is full. Records enqueued in order are stored
// in that order.
func (s *Shard) Enqueue(ctx context.Context, payload []byte) (*Pending, error) {
	if err := ValidatePayload(payload); err != nil {
		return nil, err
	}
	body, err := compress(payload)
	if err != nil {
		return nil, err
	}
	select {
	case <-s.quit:
		return nil, ErrShardClosed
	default:
	}

	req := appendRequest{body: body, done: make(chan appendResult, 1)}
	select {
	case s.queue <- req:
		return &Pending{shard: s, done: req.done}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.quit:
		return nil, ErrShardClosed
	}
}

// Wait returns the record's event id once it's on disk.
func (p *Pending) Wait() (uint64, error) {
	select {
	case r := <-p.done:
		return r.id, r.err
	case <-p.shard.writerDone:
		select {
		case r := <-p.done: // answered just before the writer stopped
			return r.id, r.err
		default:
			return 0, ErrShardClosed
		}
	}
}

// Append is Enqueue + Wait.
func (s *Shard) Append(ctx context.Context, payload []byte) (uint64, error) {
	p, err := s.Enqueue(ctx, payload)
	if err != nil {
		return 0, err
	}
	return p.Wait()
}

// run is the writer goroutine: it takes everything waiting in the queue and
// commits it in one write + fsync. After Close it empties the queue and stops.
func (s *Shard) run() {
	defer close(s.writerDone)
	for {
		select {
		case req := <-s.queue:
			s.commit(s.drain([]appendRequest{req}))
		case <-s.quit:
			for batch := s.drain(nil); len(batch) > 0; batch = s.drain(nil) {
				s.commit(batch)
			}
			return
		}
	}
}

// drain adds what is waiting in the queue to batch, without blocking.
func (s *Shard) drain(batch []appendRequest) []appendRequest {
	size := 0
	for size < maxBatchBytes {
		select {
		case req := <-s.queue:
			batch = append(batch, req)
			size += len(req.body)
		default:
			return batch
		}
	}
	return batch
}

// commit writes batch to the .log, fsyncs, adds the index entries (one
// each time the .log has grown by indexInterval), then answers each request.
func (s *Shard) commit(batch []appendRequest) {
	next := s.committed.Load()
	start := s.logEnd.Load()
	indexed := s.indexed.Load()

	type entry struct {
		id  uint64
		pos int64
	}
	var entries []entry
	lastIndexed := s.lastIndexed
	now := time.Now().UnixMilli()
	var buf []byte
	for i, req := range batch {
		id, pos := next+uint64(i), start+int64(len(buf))
		if indexed+uint64(len(entries)) == 0 || pos-lastIndexed >= indexInterval {
			if indexed+uint64(len(entries)) >= s.idx.capacity {
				log().Error("shard is full", "topic", s.topic, "shard", s.id)
				for _, r := range batch[i:] {
					r.done <- appendResult{err: ErrShardFull}
				}
				batch = batch[:i]
				break
			}
			entries = append(entries, entry{id, pos})
			lastIndexed = pos
		}
		buf = appendFrame(buf, id, now, req.body)
	}
	if len(batch) == 0 {
		return
	}

	_, err := s.log.WriteAt(buf, start)
	if err == nil {
		err = s.log.Sync()
	}
	if err != nil {
		log().Error("write failed", "topic", s.topic, "shard", s.id, "err", err)
		s.log.Truncate(start) // drop the partial write
		for _, req := range batch {
			req.done <- appendResult{err: err}
		}
		return
	}

	for i, e := range entries {
		s.idx.set(indexed+uint64(i), e.id, e.pos)
	}
	s.lastIndexed = lastIndexed
	s.indexed.Store(indexed + uint64(len(entries)))
	s.logEnd.Store(start + int64(len(buf)))
	s.committed.Store(next + uint64(len(batch)))
	ch := make(chan struct{})
	close(*s.notify.Swap(&ch))

	for i, req := range batch {
		req.done <- appendResult{id: next + uint64(i)}
	}
}

// NextID returns the id the next record will get.
func (s *Shard) NextID() uint64 {
	return s.committed.Load()
}

// ReadFrom returns records from event id start, up to maxBytes of payload.
// The first record is always returned, even if it's bigger than maxBytes.
//
// It binary-searches the index for the closest entry at or before start,
// then reads the .log forward from there, skipping records before start.
func (s *Shard) ReadFrom(start uint64, maxBytes int) ([]Record, error) {
	committed := s.committed.Load()
	if start >= committed {
		return nil, nil
	}
	id, pos, ok := s.idx.floor(start, s.indexed.Load())
	if !ok {
		return nil, fmt.Errorf("%w: no index entry for event %d", ErrCorruptRecord, start)
	}
	end := s.logEnd.Load()

	r := bufio.NewReader(io.NewSectionReader(s.log, pos, end-pos))
	var records []Record
	total := 0
	for ; id < committed && (len(records) == 0 || total < maxBytes); id++ {
		frameID, timestamp, body, err := readFrame(r)
		if err != nil {
			return nil, err
		}
		if frameID != id {
			return nil, fmt.Errorf("%w: found event %d, want %d", ErrCorruptRecord, frameID, id)
		}
		if id < start {
			continue // before the offset asked for
		}
		payload, err := decompress(body)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 && total+len(payload) > maxBytes {
			break
		}
		records = append(records, Record{ID: id, Timestamp: timestamp, Payload: payload})
		total += len(payload)
	}
	return records, nil
}

// Wait blocks until event id next exists, ctx ends or the shard closes.
func (s *Shard) Wait(ctx context.Context, next uint64) error {
	for {
		ch := *s.notify.Load() // before the check, so no commit is missed
		if s.committed.Load() > next {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.quit:
			return ErrShardClosed
		}
	}
}

// Close stores what is still queued, then closes the files. No ReadFrom may
// run during or after Close.
func (s *Shard) Close() error {
	s.closeOnce.Do(func() {
		close(s.quit)
		<-s.writerDone
		s.closeErr = errors.Join(s.idx.close(), s.log.Close())
	})
	return s.closeErr
}

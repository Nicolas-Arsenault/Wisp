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
	maxProducers  = 1024    // producers remembered per shard, most recent first
	readBuffer    = 1 << 20 // bytes of .log read at once
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

	buf         []byte                    // frames of the current commit, reused (writer only)
	lastIndexed int64                     // .log position of the last index entry (writer only)
	producers   map[uint64]producerRecord // what is remembered of each producer (writer only)
}

// producerRecord is a producer's last stored record.
type producerRecord struct {
	sequence uint64
	eventID  uint64 // tells how recently it wrote
}

type appendRequest struct {
	bodies   [][]byte // compressed payloads, stored together
	producer uint64   // 0: no duplicate check
	sequence uint64   // of bodies[0]
	done     chan appendResult
}

type appendResult struct {
	id    uint64 // of the first record stored
	count int    // records stored
	err   error
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
		producers:  make(map[uint64]producerRecord),
	}
	ch := make(chan struct{})
	s.notify.Store(&ch)

	err = s.recover()
	if err == nil {
		err = s.loadProducers()
	}
	if err != nil {
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
		f, err := readFrame(r)
		if err != nil {
			return id, pos, err
		}
		if f.id != id {
			return id, pos, ErrCorruptRecord
		}
		if n := s.indexed.Load(); n == entry || pos-s.lastIndexed >= indexInterval {
			if n >= s.idx.capacity {
				if err := s.idx.grow(); err != nil {
					return id, pos, fmt.Errorf("%w: %w", ErrShardFull, err)
				}
			}
			s.idx.set(n, id, pos)
			s.indexed.Store(n + 1)
			s.lastIndexed = pos
		}
		pos += frameSize(len(f.body))
		id++
	}
	return id, pos, nil
}

// loadProducers reads the whole .log to find each producer's last sequence,
// so duplicates are still caught after a restart. A log it can't fully read
// stops Open: accepting records could then store duplicates.
func (s *Shard) loadProducers() error {
	r := bufio.NewReader(io.NewSectionReader(s.log, 0, s.logEnd.Load()))
	for {
		f, err := readFrame(r)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if f.producer != 0 {
			s.remember(f.producer, f.sequence, f.id)
		}
	}
}

// remember updates a producer's last sequence and, past maxProducers, forgets
// the producer that wrote least recently, so memory doesn't grow forever.
func (s *Shard) remember(producer, sequence, eventID uint64) {
	s.producers[producer] = producerRecord{sequence, eventID}
	if len(s.producers) <= maxProducers {
		return
	}
	var oldest uint64
	oldestEvent := eventID
	for id, r := range s.producers {
		if r.eventID < oldestEvent {
			oldest, oldestEvent = id, r.eventID
		}
	}
	delete(s.producers, oldest)
}

// Enqueue is EnqueueBatch of one record, without a duplicate check.
func (s *Shard) Enqueue(ctx context.Context, payload []byte) (*Pending, error) {
	return s.EnqueueBatch(ctx, [][]byte{payload}, 0, 0)
}

// EnqueueBatch validates and compresses payloads, then puts them on the
// queue, blocking while the queue is full. They are stored together, in one
// write and one fsync: all of them or none. Records enqueued in order are
// stored in that order. A payload that isn't valid rejects the whole batch.
//
// If producer isn't 0, sequence is that of payloads[0], and the others follow
// it. It must go on from the producer's last one: WaitBatch returns
// ErrDuplicate if every record is already stored, stores only the new ones if
// some are, and returns ErrOutOfOrder if there is a gap. The first batch of a
// producer the shard doesn't remember is accepted, whatever its sequence:
// it's a new producer, or one forgotten (see remember).
func (s *Shard) EnqueueBatch(ctx context.Context, payloads [][]byte, producer, sequence uint64) (*Pending, error) {
	if len(payloads) == 0 {
		return nil, ErrInvalidPayload
	}
	bodies := make([][]byte, len(payloads))
	for i, payload := range payloads {
		if err := ValidatePayload(payload); err != nil {
			return nil, err
		}
		bodies[i] = compress(payload)
	}
	select {
	case <-s.quit:
		return nil, ErrShardClosed
	default:
	}

	req := appendRequest{bodies: bodies, producer: producer, sequence: sequence, done: make(chan appendResult, 1)}
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
	id, _, err := p.WaitBatch()
	return id, err
}

// WaitBatch returns the event id of the first record added, and how many
// were added, once they're on disk.
func (p *Pending) WaitBatch() (uint64, int, error) {
	select {
	case r := <-p.done:
		return r.id, r.count, r.err
	case <-p.shard.writerDone:
		select {
		case r := <-p.done: // answered just before the writer stopped
			return r.id, r.count, r.err
		default:
			return 0, 0, ErrShardClosed
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
			for _, body := range req.bodies {
				size += len(body)
			}
		default:
			return batch
		}
	}
	return batch
}

// commit writes batch to the .log, fsyncs, adds the index entries (one
// each time the .log has grown by indexInterval), then answers each request.
func (s *Shard) commit(batch []appendRequest) {
	batch = s.checkSequences(batch)
	if len(batch) == 0 {
		return
	}
	next := s.committed.Load()
	start := s.logEnd.Load()
	indexed := s.indexed.Load()

	// Room for one entry per record is checked first, so that nothing fails
	// halfway through a request.
	total := 0
	for _, req := range batch {
		total += len(req.bodies)
	}
	for indexed+uint64(total) > s.idx.capacity {
		if err := s.idx.grow(); err != nil {
			log().Error("could not grow index", "topic", s.topic, "shard", s.id, "err", err)
			for _, req := range batch {
				req.done <- appendResult{err: ErrShardFull}
			}
			return
		}
	}

	type entry struct {
		id  uint64
		pos int64
	}
	var entries []entry
	lastIndexed := s.lastIndexed
	now := time.Now().UnixMilli()
	buf := s.buf[:0]
	id := next
	for _, req := range batch {
		for i, body := range req.bodies {
			pos := start + int64(len(buf))
			if indexed+uint64(len(entries)) == 0 || pos-lastIndexed >= indexInterval {
				entries = append(entries, entry{id, pos})
				lastIndexed = pos
			}
			buf = appendFrame(buf, frame{id: id, timestamp: now, producer: req.producer, sequence: req.sequence + uint64(i), body: body})
			id++
		}
	}
	s.buf = buf

	_, err := s.log.WriteAt(buf, start)
	if err == nil {
		err = s.log.Sync()
	}
	if err != nil {
		log().Error("write failed", "topic", s.topic, "shard", s.id, "err", err)
		s.log.Truncate(start) // drop the partial write
		s.holdSequences(batch, next)
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
	s.committed.Store(id)
	ch := make(chan struct{})
	close(*s.notify.Swap(&ch))

	id = next
	for _, req := range batch {
		count := len(req.bodies)
		if req.producer != 0 {
			s.remember(req.producer, req.sequence+uint64(count)-1, id+uint64(count)-1)
		}
		req.done <- appendResult{id: id, count: count}
		id += uint64(count)
	}
}

// holdSequences is called when batch failed. It makes the shard remember
// producers it didn't know, so their later records count as a gap until the
// failed ones are resent. Otherwise they would be stored, and the resent ones
// dropped as duplicates.
func (s *Shard) holdSequences(batch []appendRequest, next uint64) {
	for _, req := range batch {
		if _, known := s.producers[req.producer]; !known && req.producer != 0 {
			s.remember(req.producer, req.sequence-1, next)
		}
	}
}

// checkSequences answers the requests that must not be stored, drops the
// records already stored from the others, and returns those.
func (s *Shard) checkSequences(batch []appendRequest) []appendRequest {
	kept := batch[:0]
	var last map[uint64]uint64 // sequences in this batch, not yet stored
	for _, req := range batch {
		if req.producer == 0 {
			kept = append(kept, req)
			continue
		}
		prev, known := last[req.producer]
		if !known {
			var r producerRecord
			r, known = s.producers[req.producer]
			prev = r.sequence
		}
		end := req.sequence + uint64(len(req.bodies)) - 1
		switch {
		case known && end <= prev:
			req.done <- appendResult{err: ErrDuplicate}
			continue
		case known && req.sequence > prev+1:
			req.done <- appendResult{err: ErrOutOfOrder}
			continue
		case known && req.sequence <= prev:
			req.bodies = req.bodies[prev+1-req.sequence:]
			req.sequence = prev + 1
		}
		if last == nil {
			last = make(map[uint64]uint64)
		}
		last[req.producer] = end
		kept = append(kept, req)
	}
	return kept
}

// NextID returns the id the next record will get.
func (s *Shard) NextID() uint64 {
	return s.committed.Load()
}

// ReadFrom returns records from event id start, up to maxBytes of payload.
// The first record is always returned, even if it's bigger than maxBytes.
func (s *Shard) ReadFrom(start uint64, maxBytes int) ([]Record, error) {
	records, _, err := s.Read(Cursor{Next: start}, maxBytes)
	return records, err
}

// Cursor is a place to read from. Make one with Next set. Read returns the
// cursor to use next, which knows where in the .log it is.
type Cursor struct {
	Next    uint64 // first event id wanted
	id      uint64 // event id of the frame at pos
	pos     int64
	located bool
}

// readers are reused between reads, because each is a big buffer.
var readers = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, readBuffer) }}

// Read is ReadFrom with a cursor. A new cursor binary-searches the index for
// the closest entry at or before Next, then reads the .log forward from
// there, skipping records before Next. A cursor from an earlier Read starts
// exactly where that one stopped.
func (s *Shard) Read(c Cursor, maxBytes int) ([]Record, Cursor, error) {
	committed := s.committed.Load()
	if c.Next >= committed {
		return nil, c, nil
	}
	id, pos := c.id, c.pos
	if !c.located {
		count := s.indexed.Load() // before the mapping is read, so it covers count
		var ok bool
		if id, pos, ok = s.idx.floor(c.Next, count); !ok {
			return nil, c, fmt.Errorf("%w: no index entry for event %d", ErrCorruptRecord, c.Next)
		}
	}
	end := s.logEnd.Load()

	r := readers.Get().(*bufio.Reader)
	defer readers.Put(r)
	r.Reset(io.NewSectionReader(s.log, pos, end-pos))

	var records []Record
	total := 0
	for ; id < committed && (len(records) == 0 || total < maxBytes); id++ {
		f, err := readFrame(r)
		if err != nil {
			return nil, c, err
		}
		if f.id != id {
			return nil, c, fmt.Errorf("%w: found event %d, want %d", ErrCorruptRecord, f.id, id)
		}
		if id < c.Next {
			pos += frameSize(len(f.body))
			continue // before the offset asked for
		}
		payload, err := decompress(f.body)
		if err != nil {
			return nil, c, err
		}
		if len(records) > 0 && total+len(payload) > maxBytes {
			break
		}
		records = append(records, Record{ID: id, Timestamp: f.timestamp, Payload: payload})
		total += len(payload)
		pos += frameSize(len(f.body))
	}
	return records, Cursor{Next: id, id: id, pos: pos, located: true}, nil
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

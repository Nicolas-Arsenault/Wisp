package shards

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// openTestShard opens shard 0 of topic "t" in dir and closes it when the
// test ends (closing twice is fine).
func openTestShard(t *testing.T, dir string) *Shard {
	t.Helper()
	s, err := Open(dir, "t", 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func appendOK(t *testing.T, s *Shard, payload string) uint64 {
	t.Helper()
	id, err := s.Append(t.Context(), []byte(payload))
	if err != nil {
		t.Fatalf("Append(%s): %v", payload, err)
	}
	return id
}

// readAll returns the payloads of every record from start.
func readAll(t *testing.T, s *Shard, start uint64) []string {
	t.Helper()
	records, err := s.ReadFrom(start, 1<<30)
	if err != nil {
		t.Fatalf("ReadFrom(%d): %v", start, err)
	}
	var out []string
	for i, r := range records {
		if r.ID != start+uint64(i) {
			t.Errorf("record %d has id %d, want %d", i, r.ID, start+uint64(i))
		}
		out = append(out, string(r.Payload))
	}
	return out
}

func equal(a, b []string) bool {
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func TestAppendAndReadFrom(t *testing.T) {
	s := openTestShard(t, t.TempDir())
	payloads := []string{`{"n":0}`, `{"b": "kept as sent"}`, `{"n":2}`}
	for i, p := range payloads {
		if id := appendOK(t, s, p); id != uint64(i) {
			t.Errorf("Append %d got id %d", i, id)
		}
	}
	if s.NextID() != 3 {
		t.Errorf("NextID = %d, want 3", s.NextID())
	}
	if got := readAll(t, s, 0); !equal(got, payloads) {
		t.Errorf("ReadFrom(0) = %v, want %v", got, payloads)
	}
	if got := readAll(t, s, 1); !equal(got, payloads[1:]) {
		t.Errorf("ReadFrom(1) = %v, want %v", got, payloads[1:])
	}
	if got := readAll(t, s, 3); len(got) != 0 {
		t.Errorf("ReadFrom(end) = %v, want nothing", got)
	}

	records, _ := s.ReadFrom(0, 1<<20)
	now := time.Now().UnixMilli()
	if ts := records[0].Timestamp; ts <= 0 || ts > now {
		t.Errorf("timestamp = %d, want a time before now (%d)", ts, now)
	}
}

func TestAppendRejectsInvalidPayload(t *testing.T) {
	s := openTestShard(t, t.TempDir())
	if _, err := s.Append(t.Context(), []byte(`[1]`)); !errors.Is(err, ErrInvalidPayload) {
		t.Errorf("err = %v, want ErrInvalidPayload", err)
	}
	// Nothing was stored, so the next id is still 0.
	if id := appendOK(t, s, `{}`); id != 0 {
		t.Errorf("id = %d, want 0", id)
	}
}

// Records enqueued one after another by one goroutine keep their order.
func TestEnqueueKeepsOrder(t *testing.T) {
	s := openTestShard(t, t.TempDir())
	var pendings []*Pending
	var want []string
	for i := range 200 {
		p := fmt.Sprintf(`{"n":%d}`, i)
		pending, err := s.Enqueue(t.Context(), []byte(p))
		if err != nil {
			t.Fatal(err)
		}
		pendings = append(pendings, pending)
		want = append(want, p)
	}
	for i, p := range pendings {
		if id, err := p.Wait(); err != nil || id != uint64(i) {
			t.Fatalf("Wait %d = %d, %v", i, id, err)
		}
	}
	if got := readAll(t, s, 0); !equal(got, want) {
		t.Error("records are not in enqueue order")
	}
}

// Many concurrent writers get distinct, dense ids, and each id's record is
// the payload of the writer that got it.
func TestConcurrentAppend(t *testing.T) {
	s := openTestShard(t, t.TempDir())
	const writers, perWriter = 20, 50

	var mu sync.Mutex
	byID := make(map[uint64]string)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				p := fmt.Sprintf(`{"w":%d,"i":%d}`, w, i)
				id, err := s.Append(context.Background(), []byte(p))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if _, dup := byID[id]; dup {
					t.Errorf("id %d given twice", id)
				}
				byID[id] = p
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	total := writers * perWriter
	if s.NextID() != uint64(total) {
		t.Fatalf("NextID = %d, want %d", s.NextID(), total)
	}
	for id, payload := range readAll(t, s, 0) {
		if byID[uint64(id)] != payload {
			t.Errorf("record %d = %s, want %s", id, payload, byID[uint64(id)])
		}
	}
}

func TestReadFromRespectsMaxBytes(t *testing.T) {
	s := openTestShard(t, t.TempDir())
	payload := `{"pad":"0123456789"}` // 20 bytes
	for range 10 {
		appendOK(t, s, payload)
	}

	records, err := s.ReadFrom(0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Errorf("ReadFrom(0, 50) gave %d records, want 2 (40 bytes)", len(records))
	}

	// A record larger than maxBytes is still returned, alone.
	records, err = s.ReadFrom(3, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != 3 {
		t.Errorf("ReadFrom(3, 5) = %d records, want just record 3", len(records))
	}

	// Paging with a small maxBytes reads every record once, in order.
	var next uint64
	for next < s.NextID() {
		batch, err := s.ReadFrom(next, 45)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 || batch[0].ID != next {
			t.Fatalf("batch at %d starts wrong: %v", next, batch)
		}
		next = batch[len(batch)-1].ID + 1
	}
}

func TestWaitWakesOnCommit(t *testing.T) {
	s := openTestShard(t, t.TempDir())
	done := make(chan error, 1)
	go func() { done <- s.Wait(t.Context(), 0) }()

	select {
	case err := <-done:
		t.Fatalf("Wait returned before any record: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	appendOK(t, s, `{}`)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Wait = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not wake up after a commit")
	}

	// A record that already exists doesn't block.
	if err := s.Wait(t.Context(), 0); err != nil {
		t.Errorf("Wait for an existing record = %v", err)
	}
}

func TestWaitStopsOnCancelAndClose(t *testing.T) {
	s := openTestShard(t, t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Wait(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait with cancelled ctx = %v, want context.Canceled", err)
	}
	s.Close()
	if err := s.Wait(t.Context(), 0); !errors.Is(err, ErrShardClosed) {
		t.Errorf("Wait after Close = %v, want ErrShardClosed", err)
	}
	if _, err := s.Enqueue(t.Context(), []byte(`{}`)); !errors.Is(err, ErrShardClosed) {
		t.Errorf("Enqueue after Close = %v, want ErrShardClosed", err)
	}
}

// Close stores everything already queued before closing the files.
func TestCloseStoresQueuedRecords(t *testing.T) {
	dir := t.TempDir()
	s := openTestShard(t, dir)
	var pendings []*Pending
	for i := range 100 {
		p, err := s.Enqueue(t.Context(), fmt.Appendf(nil, `{"n":%d}`, i))
		if err != nil {
			t.Fatal(err)
		}
		pendings = append(pendings, p)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for i, p := range pendings {
		if _, err := p.Wait(); err != nil {
			t.Errorf("record %d: %v", i, err)
		}
	}

	r := openTestShard(t, dir)
	if r.NextID() != 100 {
		t.Errorf("after reopen NextID = %d, want 100", r.NextID())
	}
}

func TestReopenContinuesIDs(t *testing.T) {
	dir := t.TempDir()
	s := openTestShard(t, dir)
	appendOK(t, s, `{"n":0}`)
	appendOK(t, s, `{"n":1}`)
	s.Close()

	r := openTestShard(t, dir)
	if id := appendOK(t, r, `{"n":2}`); id != 2 {
		t.Errorf("first id after reopen = %d, want 2", id)
	}
	if got := readAll(t, r, 0); !equal(got, []string{`{"n":0}`, `{"n":1}`, `{"n":2}`}) {
		t.Errorf("records = %v", got)
	}
}

// useIndexInterval sets indexInterval for one test. 1 gives every record
// an index entry.
func useIndexInterval(t *testing.T, n int64) {
	t.Helper()
	old := indexInterval
	indexInterval = n
	t.Cleanup(func() { indexInterval = old })
}

// The index gets one entry per 4 KB of .log, and every offset can still be
// read: from the closest entry before it, then forward through the .log.
func TestSparseIndex(t *testing.T) {
	dir := t.TempDir()
	s := openTestShard(t, dir)
	const n = 500
	for i := range n {
		appendOK(t, s, fmt.Sprintf(`{"n":%d,"pad":"%0100d"}`, i, i))
	}
	logSize := s.logEnd.Load()
	if got, want := s.indexed.Load(), uint64(logSize/indexInterval); got < want || got > want+1 {
		t.Errorf("%d index entries for %d bytes of log, want about %d", got, logSize, want)
	}
	for _, start := range []uint64{0, 1, 77, 250, 499} {
		records, err := s.ReadFrom(start, 1)
		if err != nil || len(records) != 1 || records[0].ID != start {
			t.Fatalf("ReadFrom(%d) = %v, %v", start, records, err)
		}
		if want := fmt.Sprintf(`{"n":%d,`, start); string(records[0].Payload[:len(want)]) != want {
			t.Errorf("ReadFrom(%d) payload = %s", start, records[0].Payload)
		}
	}

	// After a restart the ids continue and the index has the same entries.
	entries := s.indexed.Load()
	s.Close()
	r := openTestShard(t, dir)
	if r.indexed.Load() != entries || r.NextID() != n {
		t.Errorf("after reopen: %d entries, next id %d, want %d, %d", r.indexed.Load(), r.NextID(), entries, n)
	}
}

func TestShardFull(t *testing.T) {
	useIndexCapacity(t, 3)
	useIndexInterval(t, 1)
	s := openTestShard(t, t.TempDir())
	for range 3 {
		appendOK(t, s, `{}`)
	}
	if _, err := s.Append(t.Context(), []byte(`{}`)); !errors.Is(err, ErrShardFull) {
		t.Errorf("err = %v, want ErrShardFull", err)
	}
	if s.NextID() != 3 {
		t.Errorf("NextID = %d, want 3", s.NextID())
	}
}

// shardFiles returns the .log and .index paths of shard 0 of topic "t".
func shardFiles(dir string) (logPath, indexPath string) {
	base := filepath.Join(dir, "t", "0")
	return base + LogExt, base + IndexExt
}

// writeShard creates a shard in dir holding n records and closes it.
func writeShard(t *testing.T, dir string, n int) {
	t.Helper()
	s, err := Open(dir, "t", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if _, err := s.Append(t.Context(), fmt.Appendf(nil, `{"n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// A write cut off by a crash leaves an incomplete frame at the end of the
// log: it is dropped and the stored records stay readable.
func TestRecoverTornLastFrame(t *testing.T) {
	useIndexInterval(t, 1)
	dir := t.TempDir()
	writeShard(t, dir, 3)
	logPath, _ := shardFiles(dir)
	before, _ := os.Stat(logPath)

	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	torn := appendFrame(nil, 3, 0, []byte("cut off"))
	f.Write(torn[:len(torn)-3])
	f.Close()

	s := openTestShard(t, dir)
	if s.NextID() != 3 {
		t.Errorf("NextID = %d, want 3", s.NextID())
	}
	after, _ := os.Stat(logPath)
	if after.Size() != before.Size() {
		t.Errorf("log is %d bytes, want the torn frame dropped (%d)", after.Size(), before.Size())
	}
	if id := appendOK(t, s, `{"n":3}`); id != 3 {
		t.Errorf("next id = %d, want 3", id)
	}
	if got := readAll(t, s, 0); len(got) != 4 {
		t.Errorf("records = %v, want 4", got)
	}
}

// The log has records the index doesn't (a crash between the fsync and the
// index update): the missing entries are rebuilt from the log.
func TestRecoverIndexBehindLog(t *testing.T) {
	for _, keep := range []uint64{0, 1, 3} {
		t.Run(fmt.Sprint("keep ", keep), func(t *testing.T) {
			useIndexInterval(t, 1)
			dir := t.TempDir()
			writeShard(t, dir, 5)
			_, indexPath := shardFiles(dir)

			x, err := openIndex(indexPath)
			if err != nil {
				t.Fatal(err)
			}
			for i := keep; i < 5; i++ {
				x.set(i, 0, 0)
			}
			x.close()

			s := openTestShard(t, dir)
			if s.NextID() != 5 {
				t.Errorf("NextID = %d, want 5", s.NextID())
			}
			if got := readAll(t, s, 2); !equal(got, []string{`{"n":2}`, `{"n":3}`, `{"n":4}`}) {
				t.Errorf("ReadFrom(2) = %v", got)
			}
		})
	}
}

// The index points past the end of the log (the log was cut short): the
// index is rebuilt from the records really in the log.
func TestRecoverIndexAheadOfLog(t *testing.T) {
	useIndexInterval(t, 1)
	dir := t.TempDir()
	writeShard(t, dir, 5)
	logPath, _ := shardFiles(dir)

	// Keep records 0 and 1 and half of record 2.
	keep := 2*frameSize(len(mustCompress(t, `{"n":0}`))) + 10
	if err := os.Truncate(logPath, keep); err != nil {
		t.Fatal(err)
	}

	s := openTestShard(t, dir)
	if s.NextID() != 2 {
		t.Errorf("NextID = %d, want 2", s.NextID())
	}
	if id := appendOK(t, s, `{"n":"new"}`); id != 2 {
		t.Errorf("next id = %d, want 2", id)
	}
	if got := readAll(t, s, 0); !equal(got, []string{`{"n":0}`, `{"n":1}`, `{"n":"new"}`}) {
		t.Errorf("records = %v", got)
	}
}

func mustCompress(t *testing.T, payload string) []byte {
	t.Helper()
	b, err := compress([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

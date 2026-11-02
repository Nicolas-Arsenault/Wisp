package shards

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// benchPayload is a typical small event.
var benchPayload = []byte(`{"device":"sensor-42","temp":21.5,"humidity":48,"status":"ok","ts":1793137200123}`)

func openBenchShard(b *testing.B) *Shard {
	b.Helper()
	s, err := Open(b.TempDir(), "t", 0)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

func BenchmarkCompress(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchPayload)))
	for b.Loop() {
		compress(benchPayload)
	}
}

// One fsync, which limits how many commits a shard can do per second.
func BenchmarkFsync(b *testing.B) {
	f, err := os.Create(filepath.Join(b.TempDir(), "f"))
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	for b.Loop() {
		if _, err := f.Write(benchPayload); err != nil {
			b.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

// Records in flight, as with a producer that doesn't wait for each ack.
func BenchmarkAppendPipelined(b *testing.B) {
	s := openBenchShard(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(benchPayload)))
	const window = 1024
	pending := make([]*Pending, 0, window)
	for b.Loop() {
		p, err := s.Enqueue(b.Context(), benchPayload)
		if err != nil {
			b.Fatal(err)
		}
		pending = append(pending, p)
		if len(pending) == window {
			for _, p := range pending {
				if _, err := p.Wait(); err != nil {
					b.Fatal(err)
				}
			}
			pending = pending[:0]
		}
	}
	for _, p := range pending {
		p.Wait()
	}
}

// A consumer reading a shard from start to end.
func BenchmarkReadFrom(b *testing.B) {
	s := openBenchShard(b)
	var pending []*Pending
	for range 50_000 {
		p, err := s.Enqueue(b.Context(), benchPayload)
		if err != nil {
			b.Fatal(err)
		}
		pending = append(pending, p)
		if len(pending) == 1000 {
			for _, p := range pending {
				p.Wait()
			}
			pending = pending[:0]
		}
	}
	for _, p := range pending {
		p.Wait()
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(benchPayload)))
	next := uint64(0)
	for i := 0; b.Loop(); i++ {
		if next >= s.NextID() {
			next = 0
		}
		records, err := s.ReadFrom(next, 1)
		if err != nil || len(records) == 0 {
			b.Fatal(fmt.Sprint(next, err))
		}
		next++
	}
}

// The same, a batch at a time with a cursor, as Subscribe reads.
func BenchmarkReadCursor(b *testing.B) {
	s := openBenchShard(b)
	payloads := make([][]byte, 1000)
	for i := range payloads {
		payloads[i] = benchPayload
	}
	for range 50 {
		p, err := s.EnqueueBatch(b.Context(), payloads, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
		p.Wait()
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(benchPayload)))
	cursor := Cursor{}
	var records []Record
	for b.Loop() {
		if len(records) == 0 {
			var err error
			if records, cursor, err = s.Read(cursor, 1<<20); err != nil {
				b.Fatal(err)
			}
			if len(records) == 0 {
				cursor = Cursor{}
				continue
			}
		}
		records = records[1:]
	}
}

package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	pb "wisp/proto"

	"google.golang.org/protobuf/proto"
)

// The spool of a topic holds three kinds of file:
//
//	shards                  the topic's shard count
//	<id>.producer           a producer id: 8 bytes per shard, its last acked sequence
//	<id>-<shard>-<seq>.batch  requests not yet acked: [4 B length][protobuf request]...
//
// A run locks a .producer file for as long as it uses that id, so a file that
// isn't locked belongs to no running producer. The lock ends with the process.
const (
	spoolExt      = ".batch"
	producerExt   = ".producer"
	shardsFile    = "shards"
	maxBatchLines = 1024      // lines per request
	maxBatchBytes = 256 << 10 // payload bytes per request
	// maxFileBytes is the size of a spool file. Each file costs one fsync
	// (see lease.saveAcked), so a bigger file is faster, and a crash only
	// resends more records that the broker drops.
	maxFileBytes  = 1 << 20
	spoolDirPerm  = 0o755
	spoolFilePerm = 0o644
)

// lease is a locked producer id.
type lease struct {
	id   uint64
	file *os.File
	// sequences is the last sequence used per shard. Only loadSpool and
	// sendLines use it; acked sequences go straight to the file.
	sequences []uint64
}

// openLease locks the .producer file at path, creating it if needed. It
// returns syscall.EWOULDBLOCK if a running producer has it.
func openLease(path string, numShards int) (*lease, error) {
	id, err := strconv.ParseUint(strings.TrimSuffix(filepath.Base(path), producerExt), 16, 64)
	if err != nil {
		return nil, fmt.Errorf("spool file %s: %w", path, err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, spoolFilePerm)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, err
	}

	data := make([]byte, numShards*8)
	if _, err := file.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
		file.Close()
		return nil, err
	}
	l := &lease{id: id, file: file, sequences: make([]uint64, numShards)}
	for i := range l.sequences {
		l.sequences[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	return l, nil
}

// takeLeases locks every free id of dir, so that their leftover records are
// this run's alone, creating an id if none is free. The first one is the
// run's own. Ids exist per concurrent producer, not per run.
func takeLeases(dir string, numShards int) ([]*lease, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*"+producerExt))
	if err != nil {
		return nil, err
	}
	var leases []*lease
	for _, path := range paths {
		l, err := openLease(path, numShards)
		if errors.Is(err, syscall.EWOULDBLOCK) {
			continue
		}
		if err != nil {
			closeLeases(leases)
			return nil, err
		}
		leases = append(leases, l)
	}
	for len(leases) == 0 {
		id := rand.Uint64() | 1 // never 0, which means no duplicate check
		l, err := openLease(filepath.Join(dir, fmt.Sprintf("%016x%s", id, producerExt)), numShards)
		if errors.Is(err, syscall.EWOULDBLOCK) {
			continue // another producer took it first
		}
		if err != nil {
			return nil, err
		}
		leases = append(leases, l)
	}
	return leases, nil
}

func closeLeases(leases []*lease) {
	for _, l := range leases {
		l.file.Close()
	}
}

// saveAcked stores the last acked sequence of a shard. It always fsyncs: if
// it were lost, the id would reuse sequences and the broker would drop those
// new records as duplicates.
func (l *lease) saveAcked(shard int32, sequence uint64) error {
	data := binary.BigEndian.AppendUint64(nil, sequence)
	if _, err := l.file.WriteAt(data, int64(shard)*8); err != nil {
		return err
	}
	return l.file.Sync()
}

// batch is one spool file. It's deleted once all its requests are acked, so a
// file found at startup holds requests of a run that ended early.
type batch struct {
	lease   *lease
	shard   int32
	file    *os.File
	path    string
	bytes   int
	last    uint64 // sequence of its last record
	pending int    // requests not yet acked
}

// spooled is one request of a batch.
type spooled struct {
	req   *pb.PublishRequest
	batch *batch
}

// newBatch creates a file starting at req. The name sorts the files of a
// producer and shard in sequence order.
func newBatch(dir string, l *lease, req *pb.PublishRequest) (*batch, error) {
	path := filepath.Join(dir, fmt.Sprintf("%016x-%d-%020d%s", req.GetProducerId(), req.GetShard(), req.GetFirstSequence(), spoolExt))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, spoolFilePerm)
	if err != nil {
		return nil, err
	}
	return &batch{lease: l, shard: req.GetShard(), file: file, path: path}, nil
}

func (b *batch) full() bool {
	return b.bytes >= maxFileBytes
}

// write appends req with one write call. There is no fsync, for speed: the
// request survives the process dying, but not a power cut.
func (b *batch) write(req *pb.PublishRequest) error {
	data := make([]byte, 4, 4+proto.Size(req))
	data, err := proto.MarshalOptions{}.MarshalAppend(data, req)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(data, uint32(len(data)-4))
	if _, err := b.file.Write(data); err != nil {
		return err
	}
	b.bytes += len(data)
	b.last = lastSequence(req)
	return nil
}

func lastSequence(req *pb.PublishRequest) uint64 {
	return req.GetFirstSequence() + uint64(len(req.GetPayloads())) - 1
}

// remove saves the batch's last sequence, then deletes the file. In this
// order a crash leaves the sequence known one way or the other. If the save
// fails the file stays, and the next run resends it.
func (b *batch) remove() {
	defer b.file.Close()
	if err := b.lease.saveAcked(b.shard, b.last); err != nil {
		slog.Warn("could not save producer state", "err", err)
		return
	}
	if err := os.Remove(b.path); err != nil {
		slog.Warn("could not delete spool file", "file", b.path, "err", err)
	}
}

// loadSpool gives each sender the records of the leases' batches, oldest
// first, and moves the run's sequences past them. It returns the leases
// still needed; the others are released for other producers to use.
func loadSpool(dir string, leases []*lease, senders []*sender) ([]*lease, error) {
	kept := leases[:1]
	for i, l := range leases {
		paths, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%016x-*%s", l.id, spoolExt)))
		if err != nil {
			return leases, err
		}
		found := false
		for _, path := range paths { // sorted by name
			ok, err := loadBatch(path, l, senders)
			if err != nil {
				return leases, err
			}
			found = found || ok
		}
		switch {
		case i == 0:
		case found:
			kept = append(kept, l)
		default:
			l.file.Close()
		}
	}
	return kept, nil
}

// loadBatch adds the requests of one file to its sender. It returns false if
// the file held none, and deletes it.
func loadBatch(path string, l *lease, senders []*sender) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	reqs := readBatch(file)
	if len(reqs) == 0 {
		file.Close()
		return false, os.Remove(path)
	}
	first, last := reqs[0], reqs[len(reqs)-1]
	if int(first.GetShard()) >= len(senders) {
		file.Close()
		return false, fmt.Errorf("spool file %s: shard %d out of range (topic has %d)", path, first.GetShard(), len(senders))
	}

	b := &batch{lease: l, shard: first.GetShard(), file: file, path: path, last: lastSequence(last), pending: len(reqs)}
	s := senders[first.GetShard()]
	for _, req := range reqs {
		s.unacked = append(s.unacked, &spooled{req: req, batch: b})
	}
	l.sequences[b.shard] = max(l.sequences[b.shard], b.last)
	slog.Info("resending requests of an earlier run", "file", path, "requests", len(reqs))
	return true, nil
}

// readBatch returns the requests of a file. A torn last one was never sent,
// because a request is written before it's sent, so it stops the reading.
func readBatch(file *os.File) []*pb.PublishRequest {
	var reqs []*pb.PublishRequest
	r := bufio.NewReader(file)
	for {
		var size [4]byte
		if _, err := io.ReadFull(r, size[:]); err != nil {
			return reqs
		}
		data := make([]byte, binary.BigEndian.Uint32(size[:]))
		if _, err := io.ReadFull(r, data); err != nil {
			slog.Warn("ignoring torn spool request", "file", file.Name(), "err", err)
			return reqs
		}
		req := &pb.PublishRequest{}
		if err := proto.Unmarshal(data, req); err != nil {
			slog.Warn("ignoring torn spool request", "file", file.Name(), "err", err)
			return reqs
		}
		reqs = append(reqs, req)
	}
}

// loadShardCount returns the shard count saved in dir, or 0 if there is none.
func loadShardCount(dir string) int {
	data, err := os.ReadFile(filepath.Join(dir, shardsFile))
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return n
}

func saveShardCount(dir string, n int) error {
	if err := os.MkdirAll(dir, spoolDirPerm); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, shardsFile), []byte(strconv.Itoa(n)), spoolFilePerm)
}

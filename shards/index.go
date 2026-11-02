package shards

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// Each .index entry is [8 B event id][8 B position of the frame in the .log].
// The index is sparse: a new entry is added each time the .log has grown by
// indexInterval bytes since the last entry (the first record always has one).
const indexEntrySize = 16

// indexCapacity is the number of entries in a new .index (16 MiB, so about
// 4 GiB of .log). The file is created at full size (sparse on disk) and doubles
// when full. indexCapacity and indexInterval are variables so tests can change
// them.
var (
	indexCapacity uint64 = 1 << 20
	indexInterval int64  = 4096
)

// index is a .index file mapped in memory (mmap). Only the shard writer
// writes to it and grows it. Readers take no lock: they use whichever mapping
// they load, and every mapping stays valid until close.
type index struct {
	file     *os.File
	data     atomic.Pointer[[]byte]
	old      [][]byte // earlier mappings, unmapped by close (writer only)
	capacity uint64   // writer only
}

func openIndex(path string) (*index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, filePerm)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		size = int64(indexCapacity * indexEntrySize)
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, err
		}
	}
	if size%indexEntrySize != 0 {
		f.Close()
		return nil, fmt.Errorf("%w: index %q has a bad size", ErrCorruptRecord, path)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("shards: mmap %q: %w", path, err)
	}
	x := &index{file: f, capacity: uint64(size) / indexEntrySize}
	x.data.Store(&data)
	return x, nil
}

// grow doubles the file and maps it again. The old mapping shows the same
// file, so a reader still using it sees every entry it can ask for: the shard
// publishes a new entry count only after grow returned. A failure leaves the
// index usable at its current size.
func (x *index) grow() error {
	capacity := x.capacity * 2
	size := int64(capacity * indexEntrySize)
	if err := x.file.Truncate(size); err != nil {
		return err
	}
	data, err := unix.Mmap(int(x.file.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("shards: mmap %q: %w", x.file.Name(), err)
	}
	x.old = append(x.old, *x.data.Load())
	x.data.Store(&data)
	x.capacity = capacity
	return nil
}

func (x *index) entry(i uint64) (id uint64, pos int64) {
	e := (*x.data.Load())[i*indexEntrySize:]
	return binary.BigEndian.Uint64(e[0:8]), int64(binary.BigEndian.Uint64(e[8:16]))
}

func (x *index) set(i uint64, id uint64, pos int64) {
	e := (*x.data.Load())[i*indexEntrySize:]
	binary.BigEndian.PutUint64(e[0:8], id)
	binary.BigEndian.PutUint64(e[8:16], uint64(pos))
}

// floor binary-searches the first count entries for the last one whose id
// is <= id, and returns that entry. ok is false if there is none.
func (x *index) floor(id uint64, count uint64) (entryID uint64, pos int64, ok bool) {
	i := sort.Search(int(count), func(i int) bool {
		got, _ := x.entry(uint64(i))
		return got > id
	})
	if i == 0 {
		return 0, 0, false
	}
	entryID, pos = x.entry(uint64(i - 1))
	return entryID, pos, true
}

// lastEntry returns the position in the index of the last written entry, or
// 0 if only entry 0 (or nothing) is written. Entry 0 is always (0, 0); every
// later entry points past byte 0, so the first entry with position 0 marks
// the end.
func (x *index) lastEntry() uint64 {
	if x.capacity < 2 {
		return 0
	}
	return uint64(sort.Search(int(x.capacity-1), func(j int) bool {
		_, pos := x.entry(uint64(j + 1))
		return pos == 0
	}))
}

func (x *index) close() error {
	data := *x.data.Load()
	errs := []error{unix.Msync(data, unix.MS_SYNC), unix.Munmap(data)}
	for _, old := range x.old {
		errs = append(errs, unix.Munmap(old))
	}
	x.old = nil
	return errors.Join(append(errs, x.file.Close())...)
}

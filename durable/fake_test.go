package durable

import (
	"context"
	"errors"
	"sync"
)

// fakeLog is a Transport with the rules of a one-shard, one-producer topic,
// and hooks to break it.
type fakeLog struct {
	mu      sync.Mutex
	exists  bool
	records [][]byte
	appends int // calls of Append, whatever they returned

	// before runs at the start of each Append. A non-nil error is returned
	// and nothing is stored.
	before func(seq uint64) error
	// after runs once a record is stored. A non-nil error is returned
	// instead of the ack, as if the ack was lost.
	after func(seq uint64) error
	// readErrs are returned by the next calls of Read.
	readErrs []error
}

func (f *fakeLog) Create(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exists = true
	return nil
}

func (f *fakeLog) Read(_ context.Context, _ string, from uint64) ([]Stored, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.readErrs) > 0 {
		err := f.readErrs[0]
		f.readErrs = f.readErrs[1:]
		return nil, err
	}
	if !f.exists {
		return nil, ErrNoJournal
	}
	var out []Stored
	for id := from; id < uint64(len(f.records)); id++ {
		out = append(out, Stored{EventID: id, Payload: f.records[id]})
	}
	return out, nil
}

func (f *fakeLog) Append(_ context.Context, _ string, seq uint64, payload []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appends++
	if f.before != nil {
		if err := f.before(seq); err != nil {
			return 0, err
		}
	}
	last := uint64(len(f.records))
	switch {
	case seq <= last:
		return 0, ErrPositionTaken
	case seq > last+1:
		return 0, ErrPositionAhead
	}
	f.records = append(f.records, append([]byte(nil), payload...))
	if f.after != nil {
		if err := f.after(seq); err != nil {
			return 0, err
		}
	}
	return seq - 1, nil
}

// failTimes returns a hook that fails the first n calls.
func failTimes(n int) func(uint64) error {
	return func(uint64) error {
		if n > 0 {
			n--
			return errFlaky
		}
		return nil
	}
}

var errFlaky = errors.New("connection lost")

package durable

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MaxRecordBytes caps an encoded record. gRPC refuses messages over 4 MiB by
// default, and a journal holds whole prompts, so a limit has to be explicit.
const MaxRecordBytes = 1 << 20

const (
	// maxAppendTime is how long an append keeps retrying before it gives up
	// and stops the run. An intent that never landed fired nothing, and a
	// result that never landed leaves the step open, which is safe either way.
	maxAppendTime = 30 * time.Second
	firstBackoff  = 50 * time.Millisecond
	maxBackoff    = 2 * time.Second
)

var (
	// ErrFenced means another session wrote to the run. This one must stop
	// without firing anything else.
	ErrFenced = errors.New("durable: another session owns this run")
	// ErrJournalGap means the log is shorter than this session wrote, so
	// acked records were lost.
	ErrJournalGap = errors.New("durable: journal is missing acked records")
	// ErrCorruptJournal means the journal isn't a legal history.
	ErrCorruptJournal = errors.New("durable: corrupt journal")
	// ErrNoJournal means the run has no topic yet.
	ErrNoJournal = errors.New("durable: no journal for this run")
	// ErrRecordTooLarge means a record is over MaxRecordBytes.
	ErrRecordTooLarge = errors.New("durable: record is too large")

	// ErrPositionTaken is returned by Transport.Append when the position
	// holds a record already, which may or may not be this one.
	ErrPositionTaken = errors.New("durable: journal position is taken")
	// ErrPositionAhead is returned by Transport.Append when the log ends
	// before the position.
	ErrPositionAhead = errors.New("durable: journal position is past the end")
)

// PermanentError wraps a Transport error that a retry can't fix.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Transport is the log under a journal. The Wisp implementation is GRPC.
type Transport interface {
	// Create makes sure the journal's topic exists.
	Create(ctx context.Context, topic string) error
	// Read returns every record from event id from to the end of the log, as
	// of the call. Every record acked before the call must be included. It
	// returns ErrNoJournal if the topic doesn't exist.
	Read(ctx context.Context, topic string, from uint64) ([]Stored, error)
	// Append stores payload at position seq and returns its event id, only if
	// the log's last position is seq-1. It returns ErrPositionTaken or
	// ErrPositionAhead if not, and a *PermanentError for an error a retry
	// can't fix. Any other error means the outcome is unknown: the record may
	// or may not be stored.
	Append(ctx context.Context, topic string, seq uint64, payload []byte) (uint64, error)
}

// Journal is the one writer of a run in this process. It isn't safe for
// concurrent use: appends happen one at a time.
type Journal struct {
	transport Transport
	topic     string
	run       string
	session   string
	next      uint64 // position of the next record

	// wait sleeps between retries. Tests replace it.
	wait func(context.Context, time.Duration) error
	// maxTime is how long an append retries.
	maxTime time.Duration
}

func newSessionID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return hex.EncodeToString(b[:])
}

func newJournal(t Transport, topic, run string, next uint64) *Journal {
	return &Journal{
		transport: t, topic: topic, run: run, session: newSessionID(), next: next,
		wait: sleep, maxTime: maxAppendTime,
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// append stores r at the next position, filling in its common fields, and
// returns once it is durable. The bytes are encoded once, so a resend is
// identical and a lost ack can be told apart from another writer.
//
// It returns ErrFenced if another session holds the position, ErrJournalGap if
// the log lost records, ErrRecordTooLarge without sending anything, or the last
// error if the outcome is still unknown after maxTime. After any error the
// journal must not be used to fire another step: the record may be stored.
func (j *Journal) append(ctx context.Context, r *Record) error {
	r.V, r.Run, r.Seq, r.Session = recordVersion, j.run, j.next, j.session
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(payload) > MaxRecordBytes {
		return fmt.Errorf("%w: %d bytes, max %d", ErrRecordTooLarge, len(payload), MaxRecordBytes)
	}

	ctx, cancel := context.WithTimeout(ctx, j.maxTime)
	defer cancel()
	backoff := firstBackoff
	for {
		err := j.tryAppend(ctx, payload)
		if err == nil {
			j.next++
			return nil
		}
		var permanent *PermanentError
		if errors.Is(err, ErrFenced) || errors.Is(err, ErrJournalGap) || errors.Is(err, ErrCorruptJournal) || errors.As(err, &permanent) {
			return err
		}
		if waitErr := j.wait(ctx, backoff); waitErr != nil {
			return fmt.Errorf("durable: outcome of record %d unknown: %w", j.next, errors.Join(err, waitErr))
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// tryAppend makes one attempt. A nil error means payload is stored at j.next.
func (j *Journal) tryAppend(ctx context.Context, payload []byte) error {
	id, err := j.transport.Append(ctx, j.topic, j.next, payload)
	switch {
	case err == nil:
		if id != j.next-1 {
			return fmt.Errorf("%w: seq %d stored as event %d", ErrCorruptJournal, j.next, id)
		}
		return nil
	case errors.Is(err, ErrPositionAhead):
		return fmt.Errorf("%w: at seq %d", ErrJournalGap, j.next)
	case errors.Is(err, ErrPositionTaken):
		return j.checkOwner(ctx, payload)
	default:
		return err
	}
}

// checkOwner reads the taken position. Our own bytes mean an earlier attempt
// was stored and its ack was lost. Anything else means we were fenced.
func (j *Journal) checkOwner(ctx context.Context, payload []byte) error {
	stored, err := j.transport.Read(ctx, j.topic, j.next-1)
	if err != nil {
		return err
	}
	if len(stored) == 0 {
		return fmt.Errorf("%w: seq %d is taken but can't be read", ErrCorruptJournal, j.next)
	}
	if !bytes.Equal(stored[0].Payload, payload) {
		return fmt.Errorf("%w: seq %d is held by another writer", ErrFenced, j.next)
	}
	return nil
}

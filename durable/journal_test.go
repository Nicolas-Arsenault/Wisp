package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	pb "wisp/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func noWait(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func testJournal(log *fakeLog, next uint64) *Journal {
	log.exists = true
	j := newJournal(log, "run.r", "r", next)
	j.wait = noWait
	return j
}

func TestAppendStoresAndAdvances(t *testing.T) {
	log := &fakeLog{}
	j := testJournal(log, 1)
	for i, kind := range []Kind{KindRunStarted, KindSessionOpened} {
		r := &Record{Kind: kind}
		if err := j.append(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		if r.Seq != uint64(i+1) || r.Run != "r" || r.Session != j.session || r.V != recordVersion {
			t.Errorf("record %d: common fields not set: %+v", i, r)
		}
	}
	if len(log.records) != 2 {
		t.Errorf("stored %d records, want 2", len(log.records))
	}
}

// The outcome-unknown row of the design: nothing was stored, so a retry stores.
func TestAppendRetriesWhenNotStored(t *testing.T) {
	log := &fakeLog{before: failTimes(3)}
	j := testJournal(log, 1)
	if err := j.append(t.Context(), &Record{Kind: KindRunStarted}); err != nil {
		t.Fatal(err)
	}
	if log.appends != 4 || len(log.records) != 1 {
		t.Errorf("%d appends, %d records; want 4 and 1", log.appends, len(log.records))
	}
}

// The record was stored and the ack lost. The retry finds the position taken
// by its own bytes, which isn't fencing, and nothing is stored twice.
func TestAppendLostAckIsOurOwnRecord(t *testing.T) {
	log := &fakeLog{after: failTimes(1)}
	j := testJournal(log, 1)
	if err := j.append(t.Context(), &Record{Kind: KindRunStarted}); err != nil {
		t.Fatal(err)
	}
	if len(log.records) != 1 {
		t.Errorf("stored %d records, want 1", len(log.records))
	}
	if j.next != 2 {
		t.Errorf("next = %d, want 2", j.next)
	}
}

func TestAppendFencedByAnotherWriter(t *testing.T) {
	log := &fakeLog{}
	a, b := testJournal(log, 1), testJournal(log, 1)
	if err := a.append(t.Context(), &Record{Kind: KindRunStarted}); err != nil {
		t.Fatal(err)
	}
	err := b.append(t.Context(), &Record{Kind: KindRunStarted})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("second writer: %v, want ErrFenced", err)
	}
	if b.next != 1 {
		t.Errorf("a fenced writer advanced to %d", b.next)
	}
	if len(log.records) != 1 {
		t.Errorf("stored %d records, want 1", len(log.records))
	}
}

func TestAppendLostRecordsIsAGap(t *testing.T) {
	log := &fakeLog{}
	j := testJournal(log, 5)
	if err := j.append(t.Context(), &Record{Kind: KindSessionOpened}); !errors.Is(err, ErrJournalGap) {
		t.Fatalf("got %v, want ErrJournalGap", err)
	}
}

func TestAppendPermanentErrorIsNotRetried(t *testing.T) {
	failure := &PermanentError{errors.New("bad topic")}
	log := &fakeLog{before: func(uint64) error { return failure }}
	j := testJournal(log, 1)
	if err := j.append(t.Context(), &Record{Kind: KindRunStarted}); !errors.Is(err, failure) {
		t.Fatalf("got %v", err)
	}
	if log.appends != 1 {
		t.Errorf("%d appends, want 1", log.appends)
	}
}

func TestAppendGivesUpWithOutcomeUnknown(t *testing.T) {
	log := &fakeLog{before: func(uint64) error { return errFlaky }}
	j := testJournal(log, 1)
	j.maxTime = 20 * time.Millisecond
	j.wait = sleep
	err := j.append(t.Context(), &Record{Kind: KindRunStarted})
	if !errors.Is(err, errFlaky) {
		t.Fatalf("got %v, want the last error", err)
	}
	if j.next != 1 {
		t.Errorf("next = %d, want 1", j.next)
	}
}

func TestAppendRetriesAFailedReadBack(t *testing.T) {
	log := &fakeLog{after: failTimes(1), readErrs: []error{errFlaky}}
	j := testJournal(log, 1)
	if err := j.append(t.Context(), &Record{Kind: KindRunStarted}); err != nil {
		t.Fatal(err)
	}
	if j.next != 2 {
		t.Errorf("next = %d, want 2", j.next)
	}
}

func TestAppendRefusesLargeRecordWithoutSending(t *testing.T) {
	log := &fakeLog{}
	j := testJournal(log, 1)
	large, _ := json.Marshal(strings.Repeat("x", MaxRecordBytes))
	err := j.append(t.Context(), &Record{Kind: KindRunStarted, Input: large})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("got %v, want ErrRecordTooLarge", err)
	}
	if log.appends != 0 {
		t.Errorf("%d appends, want 0", log.appends)
	}
}

func TestAppendWrongEventIDIsCorrupt(t *testing.T) {
	j := testJournal(&fakeLog{}, 1)
	j.transport = wrongIDLog{&fakeLog{}}
	if err := j.append(t.Context(), &Record{Kind: KindRunStarted}); !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("got %v, want ErrCorruptJournal", err)
	}
}

// wrongIDLog stores at an event id that isn't seq-1, as if something else
// wrote to the topic.
type wrongIDLog struct{ *fakeLog }

func (wrongIDLog) Append(context.Context, string, uint64, []byte) (uint64, error) {
	return 7, nil
}

func TestAckResult(t *testing.T) {
	tests := []struct {
		name      string
		ack       *pb.PublishAck
		want      error
		permanent bool
		retry     bool
	}{
		{name: "stored", ack: &pb.PublishAck{Count: 1, FirstEventId: 4}},
		{name: "taken", ack: &pb.PublishAck{ErrorCode: int32(codes.AlreadyExists)}, want: ErrPositionTaken},
		{name: "gap", ack: &pb.PublishAck{ErrorCode: int32(codes.FailedPrecondition)}, want: ErrPositionAhead},
		{name: "unavailable", ack: &pb.PublishAck{ErrorCode: int32(codes.Unavailable)}, retry: true},
		{name: "internal", ack: &pb.PublishAck{ErrorCode: int32(codes.Internal)}, retry: true},
		{name: "disk full", ack: &pb.PublishAck{ErrorCode: int32(codes.ResourceExhausted)}, retry: true},
		{name: "invalid", ack: &pb.PublishAck{ErrorCode: int32(codes.InvalidArgument)}, permanent: true},
		{name: "ok with no record", ack: &pb.PublishAck{}, permanent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := ackResult(tt.ack)
			var permanent *PermanentError
			switch {
			case tt.want != nil:
				if !errors.Is(err, tt.want) {
					t.Errorf("got %v, want %v", err, tt.want)
				}
			case tt.permanent:
				if !errors.As(err, &permanent) {
					t.Errorf("got %v, want a PermanentError", err)
				}
			case tt.retry:
				if err == nil || errors.As(err, &permanent) {
					t.Errorf("got %v, want a retryable error", err)
				}
			default:
				if err != nil || id != 4 {
					t.Errorf("got %d, %v", id, err)
				}
			}
		})
	}
}

func TestReadErrorMapsNotFound(t *testing.T) {
	if err := readError(status.Error(codes.NotFound, "x")); !errors.Is(err, ErrNoJournal) {
		t.Errorf("got %v, want ErrNoJournal", err)
	}
	var permanent *PermanentError
	if err := readError(status.Error(codes.InvalidArgument, "x")); !errors.As(err, &permanent) {
		t.Errorf("got %v, want a PermanentError", err)
	}
	if err := readError(status.Error(codes.Unavailable, "x")); errors.As(err, &permanent) {
		t.Errorf("got %v, want a retryable error", err)
	}
}

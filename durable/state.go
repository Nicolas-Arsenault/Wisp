package durable

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// StepState is what the journal says about one step.
type StepState struct {
	Started  *Record // intent of the latest attempt
	Attempts int
	Outcome  *Record // step_completed or step_failed; nil if the step is open
}

// Open reports whether the step has an intent and no outcome. Nothing in the
// journal says whether its effect happened.
func (s *StepState) Open() bool { return s.Outcome == nil }

// State is a journal folded into what the runner needs. Steps[i] is step i+1.
type State struct {
	Started  *Record // run_started; nil if the journal is empty
	Finished *Record // run_finished; nil if the run hasn't finished
	Steps    []*StepState
	Sessions int
	Session  string // session allowed to write when the journal ended
	NextSeq  uint64 // position of the next record
}

// Stored is a record as it comes out of the log.
type Stored struct {
	EventID uint64
	Payload []byte
}

// Fold validates a whole journal and returns its state. It is the only place
// that decides whether a history is legal, so the runner can trust the State.
// Any record that breaks an invariant returns ErrCorruptJournal: running a
// step on a history we don't understand could fire it again.
func Fold(runID string, stored []Stored) (*State, error) {
	st := &State{NextSeq: 1}
	for i, s := range stored {
		rec, err := decodeRecord(s.Payload)
		if err != nil {
			return nil, corrupt(i, "%v", err)
		}
		if err := st.apply(runID, s.EventID, rec); err != nil {
			return nil, corrupt(i, "%v", err)
		}
	}
	return st, nil
}

func corrupt(i int, format string, args ...any) error {
	return fmt.Errorf("%w: record %d: %s", ErrCorruptJournal, i, fmt.Sprintf(format, args...))
}

func decodeRecord(payload []byte) (*Record, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields() // a field we don't know could change the meaning
	rec := new(Record)
	if err := dec.Decode(rec); err != nil {
		return nil, err
	}
	if rec.V != recordVersion {
		return nil, fmt.Errorf("version %d, want %d", rec.V, recordVersion)
	}
	return rec, nil
}

// apply adds one record, checking that it can follow what came before.
func (st *State) apply(runID string, eventID uint64, r *Record) error {
	switch {
	case r.Run != runID:
		return fmt.Errorf("run %q, want %q", r.Run, runID)
	case r.Seq != st.NextSeq || eventID != r.Seq-1:
		return fmt.Errorf("seq %d at event %d, want seq %d", r.Seq, eventID, st.NextSeq)
	case st.Finished != nil:
		return fmt.Errorf("%s after run_finished", r.Kind)
	case r.Session == "":
		return fmt.Errorf("%s has no session", r.Kind)
	case st.Started == nil && r.Kind != KindRunStarted:
		return fmt.Errorf("%s before run_started", r.Kind)
	case r.Kind != KindRunStarted && r.Kind != KindSessionOpened && r.Session != st.Session:
		return fmt.Errorf("%s from session %q, which was fenced by %q", r.Kind, r.Session, st.Session)
	}

	var err error
	switch r.Kind {
	case KindRunStarted:
		if st.Started != nil {
			return fmt.Errorf("second run_started")
		}
		st.Started, st.Session = r, r.Session
		st.Sessions++
	case KindSessionOpened:
		st.Session = r.Session
		st.Sessions++
	case KindStepStarted:
		err = st.applyStarted(r)
	case KindStepCompleted, KindStepFailed:
		err = st.applyOutcome(r)
	case KindRunFinished:
		if n := len(st.Steps); n > 0 && st.Steps[n-1].Open() {
			return fmt.Errorf("run_finished with step %d open", n)
		}
		st.Finished = r
	default:
		return fmt.Errorf("unknown kind %q", r.Kind)
	}
	if err != nil {
		return err
	}
	st.NextSeq++
	return nil
}

// applyStarted accepts the next step, or a new attempt of the open one.
func (st *State) applyStarted(r *Record) error {
	n := uint64(len(st.Steps))
	if n > 0 && st.Steps[n-1].Open() {
		open := st.Steps[n-1]
		if r.Step != n {
			return fmt.Errorf("step %d started with step %d open", r.Step, n)
		}
		if r.Attempt != open.Attempts+1 {
			return fmt.Errorf("step %d attempt %d, want %d", r.Step, r.Attempt, open.Attempts+1)
		}
		if r.Type != open.Started.Type || r.Name != open.Started.Name || r.InputHash != open.Started.InputHash {
			return fmt.Errorf("attempt %d of step %d isn't the same step", r.Attempt, r.Step)
		}
		open.Started, open.Attempts = r, r.Attempt
		return nil
	}
	if r.Step != n+1 || r.Attempt != 1 {
		return fmt.Errorf("step %d attempt %d, want step %d attempt 1", r.Step, r.Attempt, n+1)
	}
	st.Steps = append(st.Steps, &StepState{Started: r, Attempts: 1})
	return nil
}

// applyOutcome closes the open step, which must be the latest one.
func (st *State) applyOutcome(r *Record) error {
	n := uint64(len(st.Steps))
	if n == 0 || r.Step != n || !st.Steps[n-1].Open() {
		return fmt.Errorf("%s for step %d, which isn't open", r.Kind, r.Step)
	}
	step := st.Steps[n-1]
	if r.Attempt != step.Attempts {
		return fmt.Errorf("%s for attempt %d, want %d", r.Kind, r.Attempt, step.Attempts)
	}
	step.Outcome = r
	return nil
}

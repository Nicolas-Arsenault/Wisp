package durable

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// history builds a journal from records, numbering them the way a Journal
// would. Tests then break one field.
func history(t *testing.T, records ...Record) []Stored {
	t.Helper()
	stored := make([]Stored, len(records))
	for i := range records {
		r := records[i]
		r.V, r.Run, r.Seq = recordVersion, "r", uint64(i+1)
		if r.Session == "" {
			r.Session = "s1"
		}
		payload, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		stored[i] = Stored{EventID: uint64(i), Payload: payload}
	}
	return stored
}

func started(step uint64, attempt int) Record {
	return Record{Kind: KindStepStarted, Step: step, Attempt: attempt, Type: TypeTool, Name: "pay", InputHash: "h"}
}

func completed(step uint64, attempt int) Record {
	return Record{Kind: KindStepCompleted, Step: step, Attempt: attempt, Output: json.RawMessage(`1`)}
}

var runStarted = Record{Kind: KindRunStarted}

func TestFoldEmpty(t *testing.T) {
	st, err := Fold("r", nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Started != nil || st.NextSeq != 1 {
		t.Errorf("state = %+v", st)
	}
}

func TestFoldFullRun(t *testing.T) {
	st, err := Fold("r", history(t,
		runStarted, started(1, 1), completed(1, 1),
		Record{Kind: KindStepStarted, Step: 2, Attempt: 1, Type: TypeLLM, Name: "plan"},
		Record{Kind: KindStepFailed, Step: 2, Attempt: 1, Error: &ErrorInfo{Message: "no"}},
		Record{Kind: KindRunFinished, Status: StatusCompleted},
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Steps) != 2 || st.Finished == nil || st.NextSeq != 7 {
		t.Errorf("state = %+v", st)
	}
	if st.Steps[0].Open() || st.Steps[1].Open() {
		t.Error("a closed step is open")
	}
}

func TestFoldOpenStepAcrossSessions(t *testing.T) {
	st, err := Fold("r", history(t,
		runStarted, started(1, 1),
		Record{Kind: KindSessionOpened, Session: "s2"},
		Record{Kind: KindStepStarted, Session: "s2", Step: 1, Attempt: 2, Type: TypeTool, Name: "pay", InputHash: "h"},
	))
	if err != nil {
		t.Fatal(err)
	}
	if st.Sessions != 2 || st.Session != "s2" {
		t.Errorf("sessions = %d, current %q", st.Sessions, st.Session)
	}
	if step := st.Steps[0]; !step.Open() || step.Attempts != 2 {
		t.Errorf("step = %+v", step)
	}
}

func TestFoldRejectsIllegalHistories(t *testing.T) {
	tests := []struct {
		name    string
		records []Record
		want    string
	}{
		{"no run_started", []Record{{Kind: KindSessionOpened}}, "before run_started"},
		{"second run_started", []Record{runStarted, runStarted}, "second run_started"},
		{"unknown kind", []Record{runStarted, {Kind: "mystery"}}, "unknown kind"},
		{"skipped step", []Record{runStarted, started(2, 1)}, "want step 1"},
		{"two open steps", []Record{runStarted, started(1, 1), started(2, 1)}, "open"},
		{"attempt skipped", []Record{runStarted, started(1, 1), started(1, 3)}, "attempt 3"},
		{"attempt changes the step", []Record{runStarted, started(1, 1),
			{Kind: KindStepStarted, Step: 1, Attempt: 2, Type: TypeTool, Name: "other", InputHash: "h"}}, "isn't the same step"},
		{"outcome with nothing open", []Record{runStarted, completed(1, 1)}, "isn't open"},
		{"second outcome", []Record{runStarted, started(1, 1), completed(1, 1), completed(1, 1)}, "isn't open"},
		{"outcome of an old attempt", []Record{runStarted, started(1, 1), started(1, 2), completed(1, 1)}, "attempt 1"},
		{"finished with a step open", []Record{runStarted, started(1, 1), {Kind: KindRunFinished}}, "open"},
		{"record after finish", []Record{runStarted, {Kind: KindRunFinished}, {Kind: KindSessionOpened}}, "after run_finished"},
		{"write from a fenced session", []Record{runStarted, {Kind: KindSessionOpened, Session: "s2"},
			{Kind: KindStepStarted, Session: "s1", Step: 1, Attempt: 1}}, "fenced"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Fold("r", history(t, tt.records...))
			if !errors.Is(err, ErrCorruptJournal) || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("got %v, want ErrCorruptJournal with %q", err, tt.want)
			}
		})
	}
}

func TestFoldRejectsForeignRecords(t *testing.T) {
	good := history(t, runStarted, started(1, 1))
	tests := []struct {
		name   string
		stored []Stored
	}{
		{"not json", []Stored{{EventID: 0, Payload: []byte(`nope`)}}},
		{"plain event from another producer", []Stored{{EventID: 0, Payload: []byte(`{"temp":21}`)}}},
		{"other version", []Stored{{EventID: 0, Payload: []byte(`{"v":2,"run":"r","seq":1,"session":"s","kind":"run_started"}`)}}},
		{"other run", []Stored{{EventID: 0, Payload: []byte(`{"v":1,"run":"x","seq":1,"session":"s","kind":"run_started"}`)}}},
		{"no session", []Stored{{EventID: 0, Payload: []byte(`{"v":1,"run":"r","seq":1,"kind":"run_started"}`)}}},
		{"missing record", []Stored{good[0], {EventID: 2, Payload: good[1].Payload}}},
		{"seq doesn't match the event", []Stored{good[1]}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Fold("r", tt.stored); !errors.Is(err, ErrCorruptJournal) {
				t.Errorf("got %v, want ErrCorruptJournal", err)
			}
		})
	}
}

func TestCanonicalIgnoresKeyOrder(t *testing.T) {
	_, a, err := Canonical(json.RawMessage(`{"b":1,"a":[2,{"d":3,"c":4}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := Canonical(json.RawMessage(`{"a":[2,{"c":4,"d":3}],"b":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("hashes differ: %s, %s", a, b)
	}
	_, c, _ := Canonical(json.RawMessage(`{"a":1}`))
	_, d, _ := Canonical(json.RawMessage(`{"a":2}`))
	if c == d {
		t.Error("different values have the same hash")
	}
}

// Large integers must not pass through float64, or ids that differ would hash
// the same.
func TestCanonicalKeepsNumbers(t *testing.T) {
	raw, _, err := Canonical(json.RawMessage(`{"id":12345678901234567891}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"id":12345678901234567891}` {
		t.Errorf("got %s", raw)
	}
}

func TestCanonicalRejectsUnencodable(t *testing.T) {
	if _, _, err := Canonical(make(chan int)); err == nil {
		t.Error("no error for a channel")
	}
}

func TestTopicValidatesRunID(t *testing.T) {
	if topic, err := Topic("run-1_A"); err != nil || topic != "run.run-1_A" {
		t.Errorf("got %q, %v", topic, err)
	}
	for _, id := range []string{"", "a/b", "..", "a b", "a.b"} {
		if _, err := Topic(id); err == nil {
			t.Errorf("Topic(%q) was accepted", id)
		}
	}
}

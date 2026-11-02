// Package durable runs LLM agents so that they survive a crash. Each step of a
// run is journaled to a Wisp topic, and the journal decides whether a step is
// executed or its recorded result is returned. See docs/durable.md for the
// guarantees, and for where "exactly-once" is really at-least-once plus
// idempotency.
package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

// recordVersion is the schema version of every record. A reader refuses other
// versions, because guessing at a journal could fire a step again.
const recordVersion = 1

// Kind is what a record says happened.
type Kind string

const (
	KindRunStarted    Kind = "run_started"    // always the first record
	KindSessionOpened Kind = "session_opened" // a process took over the run
	KindStepStarted   Kind = "step_started"   // intent, durable before the effect
	KindStepCompleted Kind = "step_completed" // the commit point of a step
	KindStepFailed    Kind = "step_failed"    // a failure, returned again on replay
	KindRunFinished   Kind = "run_finished"   // nothing may follow it
)

// StepType is what a step does, which decides what happens to it in doubt.
type StepType string

const (
	TypeLLM         StepType = "llm"
	TypeTool        StepType = "tool"
	TypeObservation StepType = "observation"
	TypeDecision    StepType = "decision"
)

// Run statuses of a run_finished record.
const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// ErrorInfo is a failure as stored in the journal.
type ErrorInfo struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
}

// Record is one journal entry, stored as one Wisp record. Only the fields of
// its Kind are set; see docs/durable.md.
type Record struct {
	V       int    `json:"v"`
	Run     string `json:"run"`
	Seq     uint64 `json:"seq"` // position in the journal, from 1
	Session string `json:"session"`
	Kind    Kind   `json:"kind"`

	Agent string          `json:"agent,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`

	Step           uint64   `json:"step,omitempty"` // logical step, from 1
	Type           StepType `json:"type,omitempty"`
	Name           string   `json:"name,omitempty"`
	InputHash      string   `json:"input_hash,omitempty"`
	Attempt        int      `json:"attempt,omitempty"`
	Policy         string   `json:"policy,omitempty"`
	IdempotencyKey string   `json:"idempotency_key,omitempty"`

	Output     json.RawMessage `json:"output,omitempty"`
	Error      *ErrorInfo      `json:"error,omitempty"`
	DurationMS int64           `json:"duration_ms,omitempty"`
	ResolvedBy string          `json:"resolved_by,omitempty"`

	Status string `json:"status,omitempty"`
}

var validRunID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Topic returns the topic of a run's journal. It fails if the id can't be part
// of a topic name.
func Topic(runID string) (string, error) {
	if !validRunID.MatchString(runID) {
		return "", fmt.Errorf("durable: invalid run id %q: must match %s", runID, validRunID)
	}
	return "run." + runID, nil
}

// IdempotencyKey returns the key given to a step's function. It stays the same
// across attempts and sessions, so a tool can recognize a retry.
func IdempotencyKey(runID string, step uint64) string {
	return fmt.Sprintf("%s/%d", runID, step)
}

// Canonical returns v as canonical JSON, with object keys sorted and numbers
// kept as written, and its SHA-256 hash. Equal values give equal bytes, so the
// hash can tell whether a step's input changed between sessions.
//
// It fails if v can't be encoded as JSON.
func Canonical(v any) (json.RawMessage, string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, "", err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return nil, "", err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(canonical)
	return canonical, "sha256:" + hex.EncodeToString(sum[:]), nil
}

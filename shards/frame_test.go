package shards

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain routes package logs to stderr at debug level so `go test -v`
// shows them next to the test that produced them.
func TestMain(m *testing.M) {
	SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	os.Exit(m.Run())
}

func TestValidatePayload(t *testing.T) {
	tests := []struct {
		payload string
		valid   bool
	}{
		{`{}`, true},
		{`  {"a":1}`, true},
		{"\n\t{\"a\":[1,2]}\n", true},
		{`[1,2]`, false},
		{`"text"`, false},
		{`42`, false},
		{`null`, false},
		{``, false},
		{`   `, false},
		{`{"a":`, false},
		{`{"a":1} trailing`, false},
	}
	for _, tt := range tests {
		err := ValidatePayload([]byte(tt.payload))
		if tt.valid && err != nil {
			t.Errorf("ValidatePayload(%q) = %v, want nil", tt.payload, err)
		}
		if !tt.valid && !errors.Is(err, ErrInvalidPayload) {
			t.Errorf("ValidatePayload(%q) = %v, want ErrInvalidPayload", tt.payload, err)
		}
	}
}

func TestCompressRoundTrip(t *testing.T) {
	for _, in := range [][]byte{{}, []byte(`{"a":1}`), bytes.Repeat([]byte("x"), 100_000)} {
		body, err := compress(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := decompress(body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(in, out) {
			t.Errorf("round trip of %d bytes gave %d bytes", len(in), len(out))
		}
	}
}

func TestDecompressRejectsGarbage(t *testing.T) {
	if _, err := decompress([]byte("not zlib")); !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("err = %v, want ErrCorruptRecord", err)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	var buf []byte
	buf = appendFrame(buf, 7, 1234, []byte("first"))
	buf = appendFrame(buf, 8, 5678, []byte{})
	if int64(len(buf)) != frameSize(5)+frameSize(0) {
		t.Fatalf("frames are %d bytes, want %d", len(buf), frameSize(5)+frameSize(0))
	}

	r := bufio.NewReader(bytes.NewReader(buf))
	id, ts, body, err := readFrame(r)
	if err != nil || id != 7 || ts != 1234 || string(body) != "first" {
		t.Errorf("frame 1 = %d, %d, %q, %v", id, ts, body, err)
	}
	id, ts, body, err = readFrame(r)
	if err != nil || id != 8 || ts != 5678 || len(body) != 0 {
		t.Errorf("frame 2 = %d, %d, %q, %v", id, ts, body, err)
	}
	if _, _, _, err := readFrame(r); err != io.EOF {
		t.Errorf("after last frame err = %v, want io.EOF", err)
	}
}

func TestReadFrameDetectsCorruption(t *testing.T) {
	frame := appendFrame(nil, 1, 2, []byte("payload"))

	tests := map[string][]byte{
		"torn header":  frame[:10],
		"torn body":    frame[:len(frame)-2],
		"flipped body": append(append([]byte{}, frame[:len(frame)-1]...), frame[len(frame)-1]^0xff),
		"flipped id":   append(append(append([]byte{}, frame[:5]...), frame[5]^0x01), frame[6:]...),
		"huge length":  append([]byte{0xff, 0xff, 0xff, 0xff}, frame[4:]...),
	}
	for name, data := range tests {
		_, _, _, err := readFrame(bufio.NewReader(bytes.NewReader(data)))
		if !errors.Is(err, ErrCorruptRecord) {
			t.Errorf("%s: err = %v, want ErrCorruptRecord", name, err)
		}
	}
}

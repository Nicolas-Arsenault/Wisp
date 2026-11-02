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
		out, err := decompress(compress(in))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(in, out) {
			t.Errorf("round trip of %d bytes gave %d bytes", len(in), len(out))
		}
	}
}

func TestDecompressRejectsGarbage(t *testing.T) {
	if _, err := decompress([]byte("not s2 data")); !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("err = %v, want ErrCorruptRecord", err)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	var buf []byte
	buf = appendFrame(buf, frame{id: 7, timestamp: 1234, producer: 9, sequence: 3, body: []byte("first")})
	buf = appendFrame(buf, frame{id: 8, timestamp: 5678, body: []byte{}})
	if int64(len(buf)) != frameSize(5)+frameSize(0) {
		t.Fatalf("frames are %d bytes, want %d", len(buf), frameSize(5)+frameSize(0))
	}

	r := bufio.NewReader(bytes.NewReader(buf))
	f, err := readFrame(r)
	if err != nil || f.id != 7 || f.timestamp != 1234 || f.producer != 9 || f.sequence != 3 || string(f.body) != "first" {
		t.Errorf("frame 1 = %+v, %v", f, err)
	}
	f, err = readFrame(r)
	if err != nil || f.id != 8 || f.timestamp != 5678 || f.producer != 0 || len(f.body) != 0 {
		t.Errorf("frame 2 = %+v, %v", f, err)
	}
	if _, err := readFrame(r); err != io.EOF {
		t.Errorf("after last frame err = %v, want io.EOF", err)
	}
}

func TestReadFrameDetectsCorruption(t *testing.T) {
	stored := appendFrame(nil, frame{id: 1, timestamp: 2, body: []byte("payload")})

	tests := map[string][]byte{
		"torn header":  stored[:10],
		"torn body":    stored[:len(stored)-2],
		"flipped body": append(append([]byte{}, stored[:len(stored)-1]...), stored[len(stored)-1]^0xff),
		"flipped id":   append(append(append([]byte{}, stored[:5]...), stored[5]^0x01), stored[6:]...),
		"huge length":  append([]byte{0xff, 0xff, 0xff, 0xff}, stored[4:]...),
	}
	for name, data := range tests {
		_, err := readFrame(bufio.NewReader(bytes.NewReader(data)))
		if !errors.Is(err, ErrCorruptRecord) {
			t.Errorf("%s: err = %v, want ErrCorruptRecord", name, err)
		}
	}
}

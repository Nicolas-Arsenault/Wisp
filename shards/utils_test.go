// Writing records to disk, etc...
package shards

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain routes package logs to stderr at debug level so `go test -v`
// shows them next to the test that produced them.
func TestMain(m *testing.M) {
	SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	os.Exit(m.Run())
}

// resetSeqNum makes sequence-number tests independent of test order.
func resetSeqNum(t *testing.T) {
	t.Helper()
	old := seqNum
	seqNum = 0
	t.Cleanup(func() { seqNum = old })
}

func TestCompressDecompressRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"plain text", []byte("Hey, its me Nick!")},
		{"json object", []byte(`{"test": 123, "nested": {"a": [1, 2, 3]}}`)},
		{"binary", []byte{0x00, 0xff, 0x0a, 0x0d, 0x80}},
		{"large", bytes.Repeat([]byte("abcdefgh"), 10_000)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := CompressRawJSON(tc.input)
			if err != nil {
				t.Fatalf("CompressRawJSON: %v", err)
			}
			if strings.ContainsAny(compressed, "\n\r") {
				t.Errorf("compressed output contains newline characters: %q", compressed)
			}
			got, err := DecompressToRawJSON([]byte(compressed))
			if err != nil {
				t.Fatalf("DecompressToRawJSON: %v", err)
			}
			if !bytes.Equal(got, tc.input) {
				t.Errorf("round trip mismatch: got %d bytes, want %d bytes", len(got), len(tc.input))
			}
		})
	}
}

func TestDecompressToRawJSONErrors(t *testing.T) {
	one, err := CompressRawJSON([]byte("record"))
	if err != nil {
		t.Fatalf("CompressRawJSON: %v", err)
	}
	tests := []struct {
		name  string
		input []byte
	}{
		{"empty input", []byte{}},
		{"invalid base64", []byte("!!!not base64!!!")},
		{"valid base64, not zlib", []byte("aGVsbG8gd29ybGQ=")},
		{"truncated record", []byte(one[:len(one)/2])},
		{"two concatenated records", []byte(one + one)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecompressToRawJSON(tc.input)
			if !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("got err %v, want ErrCorruptRecord", err)
			}
			if got != nil {
				t.Errorf("got data %q on error, want nil", got)
			}
		})
	}
}

func TestAppendToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.bin")
	const want = "Hey, its me Nick!"

	if err := AppendToFile(path, want); err != nil {
		t.Fatalf("AppendToFile: %v", err)
	}
	data, err := ReadFromFile(path)
	if err != nil {
		t.Fatalf("ReadFromFile: %v", err)
	}
	if string(data) != want {
		t.Errorf("ReadFromFile = %q, want %q", data, want)
	}
}

func TestAppendToFileWritesNoNewlines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.bin")
	for range 3 {
		if err := AppendToFile(path, "record"); err != nil {
			t.Fatalf("AppendToFile: %v", err)
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if bytes.ContainsAny(raw, "\n\r") {
		t.Errorf("file contains newline bytes: %q", raw)
	}
}

// Documents current behavior: records have no framing, so a file with more
// than one record cannot be decoded. Update this test when framing is added.
func TestReadFromFileWithMultipleRecordsFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.bin")
	for range 2 {
		if err := AppendToFile(path, "record"); err != nil {
			t.Fatalf("AppendToFile: %v", err)
		}
	}
	if _, err := ReadFromFile(path); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("got err %v, want ErrCorruptRecord", err)
	}
}

func TestAppendToFileMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist", "test.bin")
	if err := AppendToFile(path, "x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("got err %v, want fs.ErrNotExist", err)
	}
}

func TestReadFromFileMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.bin")
	if _, err := ReadFromFile(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("got err %v, want fs.ErrNotExist", err)
	}
}

func TestDecoratePayload(t *testing.T) {
	resetSeqNum(t)

	s, err := DecoratePayload(`{"test": 123}`)
	if err != nil {
		t.Fatalf("DecoratePayload: %v", err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(s), &data); err != nil {
		t.Fatalf("decorated payload is not valid JSON: %v", err)
	}
	// JSON numbers decode as float64.
	if data["seq_n"] != float64(0) {
		t.Errorf("seq_n = %v, want 0", data["seq_n"])
	}
	if data["test"] != float64(123) {
		t.Errorf("original field altered: test = %v, want 123", data["test"])
	}

	if _, err := DecoratePayload(`{"test": 321}`); err != nil {
		t.Fatalf("DecoratePayload: %v", err)
	}
	if seqNum != 2 {
		t.Errorf("seqNum = %d, want 2", seqNum)
	}
}

func TestDecoratePayloadOverwritesExistingSeqN(t *testing.T) {
	resetSeqNum(t)
	seqNum = 7

	s, err := DecoratePayload(`{"seq_n": 999}`)
	if err != nil {
		t.Fatalf("DecoratePayload: %v", err)
	}
	if s != `{"seq_n":7}` {
		t.Errorf("DecoratePayload = %s, want {\"seq_n\":7}", s)
	}
}

func TestDecoratePayloadInvalid(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"empty", ""},
		{"malformed", `{"test": `},
		{"null", "null"},
		{"array", "[1, 2, 3]"},
		{"string", `"hello"`},
		{"number", "42"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSeqNum(t)
			got, err := DecoratePayload(tc.payload)
			if !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("got err %v, want ErrInvalidPayload", err)
			}
			if got != "" {
				t.Errorf("got %q on error, want empty string", got)
			}
			if seqNum != 0 {
				t.Errorf("seqNum advanced to %d on failure, want 0", seqNum)
			}
		})
	}
}

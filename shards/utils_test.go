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
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestMain routes package logs to stderr at debug level so `go test -v`
// shows them next to the test that produced them.
func TestMain(m *testing.M) {
	SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	os.Exit(m.Run())
}

// resetSequences gives the test an empty GlobalShardSequences, so
// sequence-number tests don't depend on test order or -count reruns.
func resetSequences(t *testing.T) {
	t.Helper()
	old := GlobalShardSequences
	GlobalShardSequences = &ShardSequences{next: make(map[string]int)}
	t.Cleanup(func() { GlobalShardSequences = old })
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
	resetSequences(t)
	path := filepath.Join(t.TempDir(), "test.bin")
	// The stored record is the decorated payload; json.Marshal sorts keys.
	const want = `{"msg":"Hey, its me Nick!","seq_n":0}`

	record, err := AppendToFile(path, "t", 0, `{"msg":"Hey, its me Nick!"}`)
	if err != nil {
		t.Fatalf("AppendToFile: %v", err)
	}
	if record.Seq != 0 || record.Payload != want {
		t.Errorf("AppendToFile returned %+v, want Seq 0, Payload %q", record, want)
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
		if _, err := AppendToFile(path, "t", 0, `{"r":1}`); err != nil {
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
		if _, err := AppendToFile(path, "t", 0, `{"r":1}`); err != nil {
			t.Fatalf("AppendToFile: %v", err)
		}
	}
	if _, err := ReadFromFile(path); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("got err %v, want ErrCorruptRecord", err)
	}
}

func TestAppendToFileMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist", "test.bin")
	if _, err := AppendToFile(path, "t", 0, `{"x":1}`); !errors.Is(err, fs.ErrNotExist) {
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
	resetSequences(t)

	s, _, err := DecoratePayload(`{"test": 123}`, "t", 0)
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

	if s, _, _ := DecoratePayload(`{"test": 321}`, "t", 0); s != `{"seq_n":1,"test":321}` {
		t.Errorf("second record on t/0 = %s, want seq_n 1", s)
	}
	// Every shard has its own sequence.
	if s, _, _ := DecoratePayload(`{"test": 1}`, "t", 1); s != `{"seq_n":0,"test":1}` {
		t.Errorf("first record on t/1 = %s, want seq_n 0", s)
	}
	if s, _, _ := DecoratePayload(`{"test": 1}`, "other", 0); s != `{"seq_n":0,"test":1}` {
		t.Errorf("first record on other/0 = %s, want seq_n 0", s)
	}
}

func TestDecoratePayloadOverwritesExistingSeqN(t *testing.T) {
	resetSequences(t)
	GlobalShardSequences.next[generateShardKey("t", 0)] = 7

	s, _, err := DecoratePayload(`{"seq_n": 999}`, "t", 0)
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
			resetSequences(t)
			got, _, err := DecoratePayload(tc.payload, "t", 0)
			if !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("got err %v, want ErrInvalidPayload", err)
			}
			if got != "" {
				t.Errorf("got %q on error, want empty string", got)
			}
			if n := GlobalShardSequences.next[generateShardKey("t", 0)]; n != 0 {
				t.Errorf("sequence advanced to %d on failure, want 0", n)
			}
		})
	}
}

func TestGetlockSamePathSharesMutex(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	l := &FileLock{locks: make(map[string]*sync.Mutex)}

	// Different spellings of <dir>/t/0.
	paths := []string{
		filepath.Join(dir, "t", "0"),
		"t/0",
		"./t/0",
		"t/../t/0",
		"t//0",
	}
	want, err := l.Getlock(paths[0])
	if err != nil {
		t.Fatalf("Getlock(%q): %v", paths[0], err)
	}
	for _, p := range paths[1:] {
		got, err := l.Getlock(p)
		if err != nil {
			t.Fatalf("Getlock(%q): %v", p, err)
		}
		if got != want {
			t.Errorf("Getlock(%q) returned a different mutex than Getlock(%q)", p, paths[0])
		}
	}

	other, err := l.Getlock("t/1")
	if err != nil {
		t.Fatalf("Getlock(t/1): %v", err)
	}
	if other == want {
		t.Error("Getlock(t/1) returned the same mutex as t/0")
	}
	if len(l.locks) != 2 {
		t.Errorf("len(locks) = %d, want 2", len(l.locks))
	}
}

func TestGenerateShardKey(t *testing.T) {
	if got := generateShardKey("orders", 3); got != "orders/3" {
		t.Errorf("generateShardKey(orders, 3) = %q, want orders/3", got)
	}
	if generateShardKey("a1", 1) == generateShardKey("a", 11) {
		t.Error("keys for (a1, 1) and (a, 11) must differ")
	}
}

// Run with -race. Many goroutines on one shard must get every number in
// 0..total-1 exactly once.
func TestShardSequencesConcurrentNext(t *testing.T) {
	const goroutines, perGoroutine = 50, 100
	const total = goroutines * perGoroutine
	s := &ShardSequences{next: make(map[string]int)}

	var mu sync.Mutex
	seen := make(map[int]bool, total)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				n := s.Next("t/0")
				s.Next("t/1") // another shard at the same time
				mu.Lock()
				if seen[n] {
					t.Errorf("sequence %d handed out twice", n)
				}
				seen[n] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	for n := 0; n < total; n++ {
		if !seen[n] {
			t.Fatalf("sequence %d was never handed out", n)
		}
	}
	if got := s.Next("t/1"); got != total {
		t.Errorf("t/1 next = %d, want %d", got, total)
	}
}

// Concurrent appends to one shard must write records in sequence order.
// Files can't be read back record by record yet (no framing), so the test
// compares the raw file with records seq_n 0..n-1 compressed and concatenated.
func TestAppendToFileConcurrentInOrder(t *testing.T) {
	resetSequences(t)
	path := filepath.Join(t.TempDir(), "0")
	const writers = 50

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := AppendToFile(path, "t", 0, `{"r":1}`); err != nil {
				t.Errorf("AppendToFile: %v", err)
			}
		}()
	}
	wg.Wait()

	var want strings.Builder
	for n := 0; n < writers; n++ {
		c, err := CompressRawJSON([]byte(`{"r":1,"seq_n":` + strconv.Itoa(n) + `}`))
		if err != nil {
			t.Fatal(err)
		}
		want.WriteString(c)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want.String() {
		t.Error("file content is not records seq_n 0..49 in order")
	}
}

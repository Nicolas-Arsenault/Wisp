package shards

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// Sentinel errors. Callers should match them with errors.Is; the returned
// errors wrap them with additional context (file names, underlying causes).
var (
	// ErrInvalidPayload is returned when a payload is not a JSON object.
	ErrInvalidPayload = errors.New("records: payload must be a JSON object")
	// ErrCorruptRecord is returned when stored bytes cannot be decoded back
	// into exactly one record (bad base64, bad zlib stream, or trailing data).
	ErrCorruptRecord = errors.New("records: corrupt record data")
)

type FileLock struct {
	mu    sync.Mutex //for the lock itself
	locks map[string]*sync.Mutex
}

var GlobalFileLock = &FileLock{
	locks: make(map[string]*sync.Mutex),
}

// seqNumField is the JSON key DecoratePayload writes the sequence number to.
const seqNumField = "seq_n"

// filePerm is the permission used when AppendToFile creates a new file.
const filePerm = 0o644

// ShardSequences holds the next sequence number of each shard, keyed by
// generateShardKey. Safe for concurrent use.
type ShardSequences struct {
	mu   sync.Mutex //for the map itself
	next map[string]int
}

// GlobalShardSequences is the sequence map DecoratePayload hands numbers out from.
var GlobalShardSequences = &ShardSequences{
	next: make(map[string]int),
}

// Next atomically returns the next sequence number for key and advances it.
// A key seen for the first time starts at 0. Concurrent callers on the same
// key always get distinct, consecutive numbers.
func (s *ShardSequences) Next(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := s.next[key] // missing key: zero value, so it starts at 0
	s.next[key] = n + 1
	return n
}

// logger is the package logger. nil means "use slog.Default() at call time",
// so the package follows whatever the application configured with slog.SetDefault.
var logger *slog.Logger

// SetLogger overrides the logger used by this package. Pass nil to go back to
// slog.Default(). Call it once during startup; it is not safe to call
// concurrently with other functions in this package.
func SetLogger(l *slog.Logger) {
	logger = l
}

func log() *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.Default()
}

// CompressRawJSON zlib-compresses jsonBytes and base64-encodes the result.
// Any byte slice is accepted (including empty); the content is not validated as JSON.
// Errors only come from the underlying writers and are wrapped with context.
func CompressRawJSON(jsonBytes []byte) (string, error) {
	var b bytes.Buffer

	// 1. Set up the Base64 encoder
	b64Encoder := base64.NewEncoder(base64.StdEncoding, &b)

	// 2. Set up the Zlib compressor
	zlibWriter := zlib.NewWriter(b64Encoder)

	// 3. Write the raw JSON bytes straight into the compression stream
	if _, err := zlibWriter.Write(jsonBytes); err != nil {
		return "", fmt.Errorf("records: compress: %w", err)
	}

	// 4. Flush and close streams in reverse order
	if err := zlibWriter.Close(); err != nil {
		return "", fmt.Errorf("records: close zlib writer: %w", err)
	}
	if err := b64Encoder.Close(); err != nil {
		return "", fmt.Errorf("records: close base64 encoder: %w", err)
	}

	log().Debug("compressed payload", "raw_bytes", len(jsonBytes), "encoded_bytes", b.Len())
	return b.String(), nil
}

// DecoratePayload parses payload as a JSON object, adds the next sequence
// number of the shard (topic, shard) under "seq_n", and returns the
// re-serialized object and that sequence number. Each shard has its own
// sequence, starting at 0.
//
// Safe for concurrent use. To keep records in sequence order inside a shard
// file, call it while holding that file's lock (AppendToFile does).
//
// Edge cases:
//   - payload must be a JSON object; arrays, scalars and `null` return ErrInvalidPayload.
//   - an existing "seq_n" field is overwritten (a warning is logged).
//   - an invalid payload does not use up a sequence number.
func DecoratePayload(payload string, topic string, shard int) (string, int, error) {

	//TODO: review flow to limit serialization to a minimum
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return "", 0, fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	// `null` unmarshals without error into a nil map; writing to it would panic.
	if data == nil {
		return "", 0, fmt.Errorf("%w: got null", ErrInvalidPayload)
	}
	if _, exists := data[seqNumField]; exists {
		log().Warn("payload already contains sequence field, overwriting", "field", seqNumField)
	}
	// Take the number only once the payload is known to be valid.
	key := generateShardKey(topic, shard)
	seq := GlobalShardSequences.Next(key)
	data[seqNumField] = seq
	updatedBytes, err := json.Marshal(data)
	if err != nil {
		// Can't happen for data decoded from JSON; if it did, seq is skipped.
		return "", 0, fmt.Errorf("records: marshal decorated payload: %w", err)
	}
	log().Debug("decorated payload", "shard", key, "seq_n", seq)
	return string(updatedBytes), seq, nil
}

// DecompressToRawJSON reverses CompressRawJSON: base64-decodes then
// zlib-decompresses compressedBytes.
//
// The input must hold exactly one record. Invalid base64, an invalid zlib
// stream, or bytes left over after the zlib stream all return an error
// wrapping ErrCorruptRecord. (Previously, extra data after the first record
// was silently ignored.)
func DecompressToRawJSON(compressedBytes []byte) ([]byte, error) {
	// 1. Decode from Base64. Decoding the whole input up front (instead of
	// streaming) guarantees that malformed or concatenated input is reported.
	raw := make([]byte, base64.StdEncoding.DecodedLen(len(compressedBytes)))
	n, err := base64.StdEncoding.Decode(raw, compressedBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: base64 decode: %w", ErrCorruptRecord, err)
	}
	src := bytes.NewReader(raw[:n])

	// 2. Set up Zlib reader
	zlibReader, err := zlib.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("%w: open zlib stream: %w", ErrCorruptRecord, err)
	}
	defer zlibReader.Close()

	// 3. Read everything out into a byte slice
	data, err := io.ReadAll(zlibReader)
	if err != nil {
		return nil, fmt.Errorf("%w: zlib decompress: %w", ErrCorruptRecord, err)
	}
	if src.Len() > 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes after zlib stream", ErrCorruptRecord, src.Len())
	}
	return data, nil
}

// Getlock returns the mutex for filePath, creating it on first use.
//
// The key is the absolute, cleaned path, so different spellings of the same
// file ("../data/t/0", "data/t/../t/0", "/abs/data/t/0") share one mutex.
// Symlinks are not resolved: a file reached through a symlink gets its own key.
//
// Returns an error only if the absolute path cannot be built (the working
// directory cannot be read); no lock is created in that case.
func (l *FileLock) Getlock(filePath string) (*sync.Mutex, error) {
	key, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("records: lock key for %q: %w", filePath, err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	lock, exists := l.locks[key]
	if !exists {
		lock = &sync.Mutex{}
		l.locks[key] = lock
	}
	return lock, nil
}

// generateShardKey returns the key of a shard in GlobalShardSequences:
// "<topic>/<shard>", e.g. "orders/3". The separator keeps keys unique
// ("a1"+1 and "a"+11 would otherwise both be "a11"); the shard is always the
// last segment, so a "/" inside the topic can't cause a clash either.
func generateShardKey(topic string, shard int) string {
	return topic + "/" + strconv.Itoa(shard)
}

// ReadFromFile reads filename and decodes it as a single record.
// Returns an error wrapping fs.ErrNotExist if the file is missing, or
// ErrCorruptRecord if its content is not exactly one encoded record
// (e.g. after AppendToFile was called more than once — see TODO there).
func ReadFromFile(filename string) ([]byte, error) {

	fileLock, err := GlobalFileLock.Getlock(filename)
	if err != nil {
		return nil, err
	}
	fileLock.Lock()
	defer fileLock.Unlock()

	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("records: read %q: %w", filename, err)
	}
	data, err := DecompressToRawJSON(fileBytes)
	if err != nil {
		return nil, fmt.Errorf("records: decode %q: %w", filename, err)
	}
	log().Debug("read record from file", "file", filename, "bytes", len(data))
	return data, nil
}

// StoredRecord describes a record written by AppendToFile.
type StoredRecord struct {
	Seq     int    // the record's seq_n within its shard
	Payload string // the record as stored, before compression (JSON with seq_n)
}

// AppendToFile decorates content with the next sequence number of shard
// (topic, shard), compresses it and appends it to filename, creating the file
// if needed. The parent directory must exist. content must be a JSON object
// (see DecoratePayload). On success it returns the stored record.
//
// The sequence number is taken while holding the file lock, so records are
// written to the file in sequence order even with concurrent callers.
// If opening or writing the file fails after that, the number is skipped
// (a gap in the shard's sequence).
//
// The error from closing the file is returned too, because for writes a
// failed Close can mean the data never reached disk.
//
// TODO: records are appended with no separator or length prefix, so a file
// holding more than one record cannot be read back. Add framing and check
// for contiguous memory.
// TODO: LIMIT JSON SERIALIZATION.....
func AppendToFile(filename string, topic string, shard int, content string) (record StoredRecord, err error) {
	fileLock, err := GlobalFileLock.Getlock(filename)
	if err != nil {
		return StoredRecord{}, err
	}
	fileLock.Lock()
	defer fileLock.Unlock()

	// Decorate and compress before opening, so a bad payload never leaves the
	// file opened/created for nothing.
	newContent, seq, err := DecoratePayload(content, topic, shard)
	if err != nil {
		return StoredRecord{}, err
	}
	compressed, err := CompressRawJSON([]byte(newContent))
	if err != nil {
		return StoredRecord{}, err
	}

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return StoredRecord{}, fmt.Errorf("records: open %q for append: %w", filename, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("records: close %q: %w", filename, closeErr)
			record = StoredRecord{}
		}
	}()

	if _, err = file.WriteString(compressed); err != nil {
		return StoredRecord{}, fmt.Errorf("records: write %q: %w", filename, err)
	}
	log().Debug("appended record to file", "file", filename, "seq_n", seq, "bytes", len(compressed))
	return StoredRecord{Seq: seq, Payload: newContent}, nil
}

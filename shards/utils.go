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

// seqNum is the next sequence number handed out by DecoratePayload.
// NOTE: not safe for concurrent use.
var seqNum = 0

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
// number under "seq_n", and returns the re-serialized object.
//
// Edge cases:
//   - payload must be a JSON object; arrays, scalars and `null` return ErrInvalidPayload.
//   - an existing "seq_n" field is overwritten (a warning is logged).
//   - seqNum is only incremented on success.
func DecoratePayload(payload string) (string, error) {

	//TODO: review flow to limit serialization to a minimum
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidPayload, err)
	}
	// `null` unmarshals without error into a nil map; writing to it would panic.
	if data == nil {
		return "", fmt.Errorf("%w: got null", ErrInvalidPayload)
	}
	if _, exists := data[seqNumField]; exists {
		log().Warn("payload already contains sequence field, overwriting", "field", seqNumField)
	}
	data[seqNumField] = seqNum
	updatedBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("records: marshal decorated payload: %w", err)
	}
	log().Debug("decorated payload", "seq_n", seqNum)
	seqNum += 1
	return string(updatedBytes), nil
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

// AppendToFile compresses content and appends it to filename, creating the
// file if needed. The parent directory must exist.
//
// The error from closing the file is returned too, because for writes a
// failed Close can mean the data never reached disk.
//
// TODO: records are appended with no separator or length prefix, so a file
// holding more than one record cannot be read back. Add framing and check
// for contiguous memory.
func AppendToFile(filename string, content string) (err error) {
	// Compress first so a failure never leaves the file opened/created for nothing.

	fileLock, err := GlobalFileLock.Getlock(filename)
	if err != nil {
		return err
	}
	fileLock.Lock()
	defer fileLock.Unlock()
	compressed, err := CompressRawJSON([]byte(content))
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, filePerm)
	if err != nil {
		return fmt.Errorf("records: open %q for append: %w", filename, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("records: close %q: %w", filename, closeErr)
		}
	}()

	if _, err = file.WriteString(compressed); err != nil {
		return fmt.Errorf("records: write %q: %w", filename, err)
	}
	log().Debug("appended record to file", "file", filename, "bytes", len(compressed))
	return nil
}

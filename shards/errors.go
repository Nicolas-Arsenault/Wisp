// Package shards stores one shard: a .log file of records, a .index file
// that says where each record is in the .log, and one writer goroutine.
package shards

import (
	"errors"
	"log/slog"
)

var (
	ErrInvalidPayload = errors.New("shards: payload must be a JSON object")
	ErrCorruptRecord  = errors.New("shards: corrupt record data")
	ErrShardFull      = errors.New("shards: shard index is full")
	ErrShardClosed    = errors.New("shards: shard is closed")
)

// logger is the package logger; nil means slog.Default().
var logger *slog.Logger

// SetLogger overrides the package logger. Call it once at startup.
func SetLogger(l *slog.Logger) {
	logger = l
}

func log() *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.Default()
}

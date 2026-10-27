package stream

import (
	"log/slog"
	"path/filepath"
	"strconv"
)

// dataDir is the root directory that holds one sub-directory per topic.
// It is a variable so tests can point it at a temporary directory.
var dataDir = "../data"

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

// generateShardFileName returns the path of shard id for topic:
// <dataDir>/<topic>/<id>, e.g. "../data/orders/3".
func generateShardFileName(id int, topic string) string {
	return filepath.Join(dataDir, topic, strconv.Itoa(id))
}

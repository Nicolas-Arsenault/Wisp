package stream

import "log/slog"

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

// Package logx is a minimal structured logger that writes to any io.Writer.
package logx

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Level is a log level.
type Level int

const (
	Debug Level = iota
	Info
	Warn
	Error
)

// ParseLevel parses a level name.
func ParseLevel(s string) Level {
	switch s {
	case "debug":
		return Debug
	case "warn", "warning":
		return Warn
	case "error":
		return Error
	default:
		return Info
	}
}

func (l Level) String() string {
	switch l {
	case Debug:
		return "DEBUG"
	case Warn:
		return "WARN"
	case Error:
		return "ERROR"
	default:
		return "INFO"
	}
}

// Logger is a thread-safe logger.
type Logger struct {
	mu    sync.Mutex
	out   io.Writer
	level atomic.Int32
}

// New creates a logger.
func New(out io.Writer, level Level) *Logger {
	l := &Logger{out: out}
	l.level.Store(int32(level))
	return l
}

// Stderr returns a default logger that writes to stderr.
func Stderr(level Level) *Logger { return New(os.Stderr, level) }

// SetOutput switches the output target.
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	l.out = w
	l.mu.Unlock()
}

// Level returns the current level.
func (l *Logger) Level() Level { return Level(l.level.Load()) }

// SetLevel adjusts the level at runtime (used when the configuration is reloaded).
func (l *Logger) SetLevel(level Level) { l.level.Store(int32(level)) }

func (l *Logger) log(level Level, component, format string, args ...any) {
	if level < Level(l.level.Load()) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	ts := time.Now().Format("2006-01-02 15:04:05.000")
	line := ts + " " + level.String() + " "
	if component != "" {
		line += component + ": "
	}
	line += msg + "\n"
	l.mu.Lock()
	_, _ = io.WriteString(l.out, line)
	l.mu.Unlock()
}

// Debugf logs a debug message.
func (l *Logger) Debugf(component, format string, args ...any) {
	l.log(Debug, component, format, args...)
}

// Infof logs a general message.
func (l *Logger) Infof(component, format string, args ...any) {
	l.log(Info, component, format, args...)
}

// Warnf logs a warning.
func (l *Logger) Warnf(component, format string, args ...any) {
	l.log(Warn, component, format, args...)
}

// Errorf logs an error.
func (l *Logger) Errorf(component, format string, args ...any) {
	l.log(Error, component, format, args...)
}

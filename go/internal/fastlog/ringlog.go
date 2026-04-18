// Package fastlog provides a lock-free ring buffer logger.
//
// The hot-path goroutines write log entries to the ring buffer without
// blocking. A background goroutine drains the buffer to stdout/file.
// This prevents logging from adding latency to the trading path.
//
// Design:
//   - Fixed-size entries (no allocation on write)
//   - Atomic head/tail with power-of-2 masking
//   - Overflows are silently dropped (trading > logging)
package fastlog

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

const (
	// RingSize must be power of 2.
	RingSize = 4096
	ringMask = RingSize - 1

	// MaxMsgLen is the maximum message length per entry.
	MaxMsgLen = 120
)

// Level represents log severity.
type Level uint8

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelTrade // special level for trade execution logs
)

var levelNames = [5]string{"DBG", "INF", "WRN", "ERR", "TRD"}

// Entry is a fixed-size log entry — no heap allocation.
type Entry struct {
	TimestampNs int64
	Level       Level
	MsgLen      uint8
	Msg         [MaxMsgLen]byte
}

// RingLogger is a lock-free ring buffer logger.
type RingLogger struct {
	entries [RingSize]Entry
	head    atomic.Uint64 // write cursor (producers)
	tail    atomic.Uint64 // read cursor (consumer)
	dropped atomic.Uint64 // overflow counter
}

// NewRingLogger creates a new ring buffer logger.
func NewRingLogger() *RingLogger {
	return &RingLogger{}
}

// Log writes a message to the ring buffer without blocking.
// If the buffer is full, the message is silently dropped.
//
//go:nosplit
func (r *RingLogger) Log(level Level, msg string) {
	// Claim a slot atomically.
	head := r.head.Add(1) - 1
	tail := r.tail.Load()

	// Check if buffer is full.
	if head-tail >= RingSize {
		r.dropped.Add(1)
		return
	}

	idx := head & ringMask
	e := &r.entries[idx]
	e.TimestampNs = time.Now().UnixNano()
	e.Level = level

	// Copy message without allocation.
	n := len(msg)
	if n > MaxMsgLen {
		n = MaxMsgLen
	}
	copy(e.Msg[:n], msg)
	e.MsgLen = uint8(n)
}

// Logf formats and logs a message. Allocates — use only off hot-path.
func (r *RingLogger) Logf(level Level, format string, args ...interface{}) {
	r.Log(level, fmt.Sprintf(format, args...))
}

// Info is a convenience method.
func (r *RingLogger) Info(msg string)  { r.Log(LevelInfo, msg) }
func (r *RingLogger) Warn(msg string)  { r.Log(LevelWarn, msg) }
func (r *RingLogger) Error(msg string) { r.Log(LevelError, msg) }
func (r *RingLogger) Trade(msg string) { r.Log(LevelTrade, msg) }

// Dropped returns the number of dropped messages.
func (r *RingLogger) Dropped() uint64 { return r.dropped.Load() }

// DrainLoop runs in a background goroutine, writing entries to a file.
// Exits when done is closed.
func (r *RingLogger) DrainLoop(done <-chan struct{}, file *os.File) {
	if file == nil {
		file = os.Stdout
	}

	buf := make([]byte, 0, 256)

	for {
		select {
		case <-done:
			// Final drain.
			r.drainAll(file, &buf)
			return
		default:
		}

		drained := r.drainAll(file, &buf)
		if drained == 0 {
			time.Sleep(1 * time.Millisecond) // idle backoff
		}
	}
}

func (r *RingLogger) drainAll(file *os.File, buf *[]byte) int {
	drained := 0
	for {
		tail := r.tail.Load()
		head := r.head.Load()
		if tail >= head {
			break
		}

		idx := tail & ringMask
		e := &r.entries[idx]

		// Format: [timestamp] [LEVEL] message\n
		*buf = (*buf)[:0]
		ts := time.Unix(0, e.TimestampNs).Format("15:04:05.000000")
		*buf = append(*buf, '[')
		*buf = append(*buf, ts...)
		*buf = append(*buf, "] ["...)
		*buf = append(*buf, levelNames[e.Level]...)
		*buf = append(*buf, "] "...)
		*buf = append(*buf, e.Msg[:e.MsgLen]...)
		*buf = append(*buf, '\n')

		_, _ = file.Write(*buf)
		r.tail.Add(1)
		drained++
	}
	return drained
}

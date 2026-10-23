// Package logging emits the project's stable key=value event lines.
//
// Contract (v0.3 plan §5): one line per event, field-parseable, ordered as
// the caller passes fields, safe for concurrent use, always to stderr for
// STDIO proxy modes. Events must never carry business payload or identity
// credentials — callers pass only metadata (ids, reasons, byte counts).
package logging

import (
	"io"
	"strconv"
	"sync"
)

type Logger struct {
	w  io.Writer
	mu *sync.Mutex
}

func New(w io.Writer) *Logger { return &Logger{w: w, mu: new(sync.Mutex)} }

// Event writes one line: `event=<event> k1=v1 k2=v2\n`.
// kv is flat key/value pairs; an odd trailing value is ignored. Values that
// would break field parsing (empty, whitespace, '=', quotes) are Go-quoted,
// so `strings.Fields` plus a small unquote step always recovers the fields.
func (l *Logger) Event(event string, kv ...string) {
	if l == nil || l.w == nil {
		return
	}
	b := appendField(nil, "event", event)
	for i := 0; i+1 < len(kv); i += 2 {
		b = appendField(b, kv[i], kv[i+1])
	}
	b = append(b, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(b)
}

func appendField(b []byte, key, val string) []byte {
	if key == "" {
		return b
	}
	if len(b) > 0 {
		b = append(b, ' ')
	}
	b = append(b, key...)
	b = append(b, '=')
	if needsQuote(val) {
		return strconv.AppendQuote(b, val)
	}
	return append(b, val...)
}

func needsQuote(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c <= ' ' || c == '=' || c == '"' || c == '\'' || c == 0x7f {
			return true
		}
	}
	return false
}

// FormatInt is a tiny helper so call sites pass numbers as fields without
// importing strconv everywhere.
func FormatInt(n int64) string { return strconv.FormatInt(n, 10) }

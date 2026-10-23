package logging

import (
	"strings"
	"sync"
	"testing"
)

func TestEventFieldOrderAndQuoting(t *testing.T) {
	var out strings.Builder
	l := New(&out)
	l.Event("closed", "connection_id", "7", "source", "127.0.0.1:9000", "reason", "eof")
	want := "event=closed connection_id=7 source=127.0.0.1:9000 reason=eof\n"
	if out.String() != want {
		t.Fatalf("line = %q, want %q", out.String(), want)
	}

	out.Reset()
	l.Event("x", "detail", "has space", "empty", "", "eq", "a=b", "skipme")
	want = `event=x detail="has space" empty="" eq="a=b"` + "\n"
	if out.String() != want {
		t.Fatalf("line = %q, want %q", out.String(), want)
	}
}

func TestEventIgnoresOddTrailingValue(t *testing.T) {
	var out strings.Builder
	l := New(&out)
	l.Event("e", "a", "1", "dangling")
	want := "event=e a=1\n"
	if out.String() != want {
		t.Fatalf("line = %q, want %q", out.String(), want)
	}
}

func TestEventNilLoggerSafe(t *testing.T) {
	var l *Logger
	l.Event("e", "k", "v") // must not panic
	New(nil).Event("e")    // nil writer must not panic
}

func TestEventConcurrentLines(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	w := writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, string(p))
		return len(p), nil
	})
	l := New(w)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Event("e", "i", FormatInt(int64(i)), "pad", strings.Repeat("p", 100))
		}(i)
	}
	wg.Wait()
	if len(lines) != 16 {
		t.Fatalf("lines = %d, want 16", len(lines))
	}
	for _, line := range lines {
		if c := strings.Count(line, "\n"); c != 1 {
			t.Fatalf("line contains %d newlines: %q", c, line)
		}
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

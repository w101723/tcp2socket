package transport

import (
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

func TestStdioCloseInterruptsRead(t *testing.T) {
	reader, input, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	output, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	defer output.Close()
	stdio := NewStdio(reader, writer)
	if _, ok := any(stdio).(DeadlineSetter); ok {
		t.Fatal("stdio must not advertise deadlines")
	}
	done := make(chan error, 1)
	go func() { _, err := stdio.Read(make([]byte, 1)); done <- err }()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = stdio.Close() }()
	}
	wg.Wait()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("read unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("read not interrupted")
	}
	if _, err := io.ReadAll(output); err != nil {
		t.Fatal(err)
	}
}

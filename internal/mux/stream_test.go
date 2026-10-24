package mux

import (
	"errors"
	"sync"
	"testing"
)

func TestStreamLifecycleHappyPath(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	if err := r.Open(1, 65536); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != StreamOpening {
		t.Fatalf("state = %s, want opening", st)
	}
	if err := r.OpenAck(1); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != StreamOpen {
		t.Fatalf("state = %s, want open", st)
	}
	// full-duplex data, both half closes, closed
	if _, err := r.CheckData(1, 1, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CheckData(1, 2, 100); err != nil {
		t.Fatal(err)
	}
	if err := r.LocalFin(1); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != StreamHalfClosedLocal {
		t.Fatalf("state = %s, want half_closed_local", st)
	}
	if err := r.RemoteFin(1); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.State(1); st != StreamClosed {
		t.Fatalf("state = %s, want closed", st)
	}
}

// TestOpenParity: client streams odd, server even (§6).
func TestOpenParity(t *testing.T) {
	client := NewRegistry(SideClient, 256)
	if err := client.Open(1, 1024); err != nil {
		t.Fatal(err)
	}
	if err := client.Open(2, 1024); !errors.Is(err, ErrStreamIDParity) {
		t.Fatalf("even id on client: %v", err)
	}
	server := NewRegistry(SideServer, 256)
	if err := server.Open(2, 1024); err != nil {
		t.Fatal(err)
	}
	if err := server.Open(1, 1024); !errors.Is(err, ErrStreamIDParity) {
		t.Fatalf("odd id on server: %v", err)
	}
	if err := client.Open(0, 1024); !errors.Is(err, ErrStreamIDZero) {
		t.Fatalf("id 0: %v", err)
	}
}

// TestAcceptOpenMirroredParity: the peer's streams live in the opposite
// parity domain.
func TestAcceptOpenMirroredParity(t *testing.T) {
	client := NewRegistry(SideClient, 256) // accepts server's even streams
	if err := client.AcceptOpen(2, 1024); err != nil {
		t.Fatal(err)
	}
	if err := client.AcceptOpen(3, 1024); !errors.Is(err, ErrStreamIDParity) {
		t.Fatalf("odd id accepted by client: %v", err)
	}
	server := NewRegistry(SideServer, 256) // accepts client's odd streams
	if err := server.AcceptOpen(1, 1024); err != nil {
		t.Fatal(err)
	}
	if err := server.AcceptOpen(4, 1024); !errors.Is(err, ErrStreamIDParity) {
		t.Fatalf("even id accepted by server: %v", err)
	}
}

func TestDuplicateOpen(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	if err := r.Open(1, 1024); err != nil {
		t.Fatal(err)
	}
	if err := r.Open(1, 1024); !errors.Is(err, ErrStreamIDDuplicate) {
		t.Fatalf("duplicate open: %v", err)
	}
}

func TestStreamLimit(t *testing.T) {
	r := NewRegistry(SideClient, 3)
	for id := uint32(1); id <= 5; id += 2 {
		err := r.Open(id, 1024)
		if id <= 5 && err == nil {
			continue
		}
		if err == nil || errors.Is(err, ErrStreamIDDuplicate) {
			t.Fatalf("stream %d accepted over limit", id)
		}
	}
	if r.Count() != 3 {
		t.Fatalf("count = %d, want 3", r.Count())
	}
}

func TestDuplicateFinRejected(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	_ = r.Open(1, 1024)
	_ = r.OpenAck(1)
	if err := r.RemoteFin(1); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoteFin(1); !errors.Is(err, ErrDuplicateFin) {
		t.Fatalf("second remote FIN: %v", err)
	}
	_ = r.LocalFin(1) // closes the stream
	if err := r.LocalFin(1); !errors.Is(err, ErrDuplicateFin) {
		t.Fatalf("FIN on closed stream: %v", err)
	}
}

// TestDataAfterFin: late DATA after the peer's FIN is a protocol error, but
// DATA while we merely half-closed locally is still legal (response drain).
func TestDataAfterFin(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	_ = r.Open(1, 65536)
	_ = r.OpenAck(1)
	// our FIN first, their data still flows back to us? No: DATA arrives
	// FROM the peer; after OUR local FIN receiving is still legal.
	if _, err := r.CheckData(1, 1, 10); err != nil {
		t.Fatalf("data before remote fin: %v", err)
	}
	if err := r.RemoteFin(1); err != nil {
		t.Fatal(err)
	}
	// state now half_closed_remote (we haven't FINed): the peer must not
	// send more data after announcing its FIN.
	if _, err := r.CheckData(1, 2, 10); !errors.Is(err, ErrDataAfterFin) {
		t.Fatalf("data after remote fin: %v", err)
	}
}

func TestGhostFrames(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	for _, check := range []struct {
		name string
		fn   func() error
	}{
		{"data", func() error { _, err := r.CheckData(999, 1, 1); return err }},
		{"fin", func() error { return r.RemoteFin(999) }},
		{"close", func() error { return r.Close(999) }},
		{"state", func() error { _, err := r.State(999); return err }},
		{"consume", func() error { return r.Consume(999, 1) }},
		{"window", func() error { return r.CheckWindowUpdate(999, 1) }},
	} {
		if err := check.fn(); !errors.Is(err, ErrGhostStream) {
			t.Fatalf("%s on ghost stream: %v", check.name, err)
		}
	}
}

func TestSequenceRules(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	_ = r.Open(1, 65536)
	_ = r.OpenAck(1)
	if _, err := r.CheckData(1, 2, 10); !errors.Is(err, ErrSequence) {
		t.Fatalf("skipped sequence: %v", err)
	}
	if _, err := r.CheckData(1, 1, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CheckData(1, 1, 10); !errors.Is(err, ErrSequence) {
		t.Fatalf("duplicate sequence: %v", err)
	}
	if _, err := r.CheckData(1, 2, 10); err != nil {
		t.Fatal(err)
	}
	// WINDOW_UPDATE has its own counter
	if err := r.CheckWindowUpdate(1, 1); err != nil {
		t.Fatal(err)
	}
	if err := r.CheckWindowUpdate(1, 1); !errors.Is(err, ErrSequence) {
		t.Fatalf("duplicate window sequence: %v", err)
	}
}

func TestWindowEnforcement(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	_ = r.Open(1, 100)
	_ = r.OpenAck(1)
	if rem, err := r.CheckData(1, 1, 60); err != nil || rem != 40 {
		t.Fatalf("credit after 60: %d %v", rem, err)
	}
	if rem, err := r.CheckData(1, 2, 50); err != nil || rem != -10 {
		t.Fatalf("overrun must surface negative credit: %d %v", rem, err)
	}
	// consume and the window recovers (caller sends WINDOW_UPDATE)
	if err := r.Consume(1, 110); err != nil {
		t.Fatal(err)
	}
}

func TestCloseAll(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	_ = r.Open(1, 1024)
	_ = r.Open(3, 1024)
	ids := r.CloseAll()
	if len(ids) != 2 {
		t.Fatalf("closed %d streams, want 2", len(ids))
	}
	if r.Count() != 0 {
		t.Fatalf("registry not emptied")
	}
	// further opens rejected
	if err := r.Open(5, 1024); err == nil {
		t.Fatal("open after CloseAll accepted")
	}
}

// TestRegistryConcurrent: parallel open/data/fin/close under -race.
func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry(SideClient, 256)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				id := uint32(w*40+i)*2 + 1
				if err := r.Open(id, 65536); err != nil {
					return // limit or duplicate; fine
				}
				_ = r.OpenAck(id)
				_, _ = r.CheckData(id, 1, 1)
				_ = r.LocalFin(id)
				_ = r.RemoteFin(id)
				r.Remove(id)
			}
		}(w)
	}
	wg.Wait()
	if r.Count() != 0 {
		t.Fatalf("leftover streams: %d", r.Count())
	}
}

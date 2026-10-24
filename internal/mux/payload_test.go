package mux

import (
	"testing"
)

func TestCapabilities(t *testing.T) {
	if _, err := ParseCapabilities([]byte{CapStreamMux}); err != nil {
		t.Fatalf("valid caps: %v", err)
	}
	if _, err := ParseCapabilities(nil); err != nil {
		t.Fatalf("empty caps must parse: %v", err)
	}
	for name, bad := range map[string][]byte{
		"zero":       {0x00},
		"dupe":       {CapStreamMux, CapStreamMux},
		"descending": {CapHeartbeat, CapStreamMux},
	} {
		if _, err := ParseCapabilities(bad); err == nil {
			t.Fatalf("%s caps accepted", name)
		}
	}
	if _, err := EncodeCapabilities([]byte{CapHeartbeat, CapStreamMux}); err == nil {
		t.Fatal("descending encode accepted")
	}
}

func TestTargetDescriptor(t *testing.T) {
	tg := Target{Network: NetworkTCP, Address: "127.0.0.1:8999"}
	p, err := tg.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseTarget(p)
	if err != nil || back != tg {
		t.Fatalf("roundtrip: %+v err=%v", back, err)
	}

	unix := Target{Network: NetworkUnix, Address: "/tmp/app.sock"}
	if p, err = unix.Encode(); err != nil {
		t.Fatal(err)
	}
	if back, err = ParseTarget(p); err != nil || back != unix {
		t.Fatalf("unix roundtrip: %+v err=%v", back, err)
	}

	for name, p := range map[string][]byte{
		"empty":         {},
		"net only":      {NetworkTCP},
		"bad network":   {0x09, 3, 'a', 'b', 'c'},
		"length short":  {NetworkTCP, 5, 'a'},
		"length long":   {NetworkTCP, 5, 'a', 'b', 'c', 'd', 'e', 'f'},
		"address empty": {NetworkTCP, 0},
	} {
		if _, err := ParseTarget(p); err == nil {
			t.Fatalf("%s target accepted", name)
		}
	}
	if _, err := (Target{Network: NetworkTCP, Address: ""}).Encode(); err == nil {
		t.Fatal("empty address accepted")
	}
	if _, err := (Target{Network: 0x07, Address: "x"}).Encode(); err == nil {
		t.Fatal("bad network accepted")
	}
}

func TestErrorPayload(t *testing.T) {
	e := ErrorPayload{Code: ErrCodeFrame, Detail: "bad crc"}
	p, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseErrorPayload(p)
	if err != nil || back.Code != e.Code || back.Detail != e.Detail {
		t.Fatalf("roundtrip: %+v err=%v", back, err)
	}

	if _, err := ParseErrorPayload(nil); err == nil {
		t.Fatal("empty error payload accepted")
	}
	long := ErrorPayload{Code: ErrCodeDial, Detail: string(make([]byte, MaxErrorDetail+10))}
	p, err = long.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 1+MaxErrorDetail {
		t.Fatalf("detail not truncated: %d", len(p)-1)
	}
	if _, err := ParseErrorPayload(append([]byte{byte(ErrCodeDial)}, make([]byte, MaxErrorDetail+1)...)); err == nil {
		t.Fatal("oversized detail accepted")
	}
}

func TestWindowUpdatePayload(t *testing.T) {
	const delta = uint32(0x01020304)
	got, err := ParseWindowUpdate(WindowUpdatePayload(delta))
	if err != nil || got != delta {
		t.Fatalf("delta roundtrip: %d err=%v", got, err)
	}
	if _, err := ParseWindowUpdate(nil); err == nil {
		t.Fatal("empty window update accepted")
	}
	if _, err := ParseWindowUpdate([]byte{1, 2, 3}); err == nil {
		t.Fatal("3-byte window update accepted")
	}
	if _, err := ParseWindowUpdate([]byte{1, 2, 3, 4, 5}); err == nil {
		t.Fatal("5-byte window update accepted")
	}
}

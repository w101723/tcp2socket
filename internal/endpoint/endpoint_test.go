package endpoint

import (
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantNetwork string
		wantAddress string
		wantErr     bool
	}{
		{
			name:        "tcp with scheme",
			input:       "tcp://127.0.0.1:8080",
			wantNetwork: "tcp",
			wantAddress: "127.0.0.1:8080",
		},
		{
			name:        "tcp with empty host",
			input:       "tcp://:8080",
			wantNetwork: "tcp",
			wantAddress: ":8080",
		},
		{
			name:    "tcp with empty address",
			input:   "tcp://",
			wantErr: true,
		},
		{
			name:        "unix with scheme",
			input:       "unix:///tmp/test.sock",
			wantNetwork: "unix",
			wantAddress: "/tmp/test.sock",
		},
		{
			name:    "unix with relative path",
			input:   "unix://relative/test.sock",
			wantErr: true,
		},
		{
			name:    "unix with empty path",
			input:   "unix://",
			wantErr: true,
		},
		{
			name:        "shorthand unix absolute",
			input:       "/tmp/test.sock",
			wantNetwork: "unix",
			wantAddress: "/tmp/test.sock",
		},
		{
			name:        "shorthand tcp with host and port",
			input:       "127.0.0.1:8080",
			wantNetwork: "tcp",
			wantAddress: "127.0.0.1:8080",
		},
		{
			name:        "shorthand tcp port only",
			input:       ":8080",
			wantNetwork: "tcp",
			wantAddress: ":8080",
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			input:   "   \t\n",
			wantErr: true,
		},
		{
			name:    "unsupported format without colon or slash",
			input:   "invalid_endpoint",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep, err := Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr {
				if ep.Network != tt.wantNetwork {
					t.Errorf("ep.Network = %q, want %q", ep.Network, tt.wantNetwork)
				}
				if ep.Address != tt.wantAddress {
					t.Errorf("ep.Address = %q, want %q", ep.Address, tt.wantAddress)
				}
				if ep.Raw != tt.input {
					t.Errorf("ep.Raw = %q, want %q", ep.Raw, tt.input)
				}
			}
		})
	}
}

func FuzzParse(f *testing.F) {
	seeds := []string{
		"tcp://127.0.0.1:8080",
		"tcp://:8080",
		"unix:///tmp/app.sock",
		"/tmp/app.sock",
		"127.0.0.1:8080",
		":8080",
		"",
		"   ",
		"invalid",
		"tcp://",
		"unix://",
		"unix://relative.sock",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		ep, err := Parse(s)
		if err == nil {
			if ep.Network != "tcp" && ep.Network != "unix" {
				t.Fatalf("unexpected network: %s", ep.Network)
			}
			if ep.Address == "" {
				t.Fatalf("address cannot be empty on successful parse")
			}
		}
	})
}

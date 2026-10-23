package endpoint

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Endpoint represents a parsed network endpoint.
type Endpoint struct {
	Network string
	Address string
	Raw     string
}

// Parse parses a string representation into an Endpoint.
// Supported formats:
// - tcp://host:port
// - unix:///abs/path
// - /abs/path or /path (unix)
// - host:port or :port (tcp)
func Parse(s string) (Endpoint, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Endpoint{}, errors.New("empty endpoint")
	}

	switch {
	case strings.HasPrefix(s, "tcp://"):
		addr := strings.TrimPrefix(s, "tcp://")
		if addr == "" {
			return Endpoint{}, fmt.Errorf("invalid tcp endpoint: %q", s)
		}
		return Endpoint{Network: "tcp", Address: addr, Raw: s}, nil

	case strings.HasPrefix(s, "unix://"):
		path := strings.TrimPrefix(s, "unix://")
		if path == "" {
			return Endpoint{}, fmt.Errorf("invalid unix endpoint: %q", s)
		}
		if !filepath.IsAbs(path) {
			return Endpoint{}, fmt.Errorf("unix socket path must be absolute: %q", path)
		}
		return Endpoint{Network: "unix", Address: path, Raw: s}, nil

	default:
		if strings.HasPrefix(s, "/") {
			return Endpoint{Network: "unix", Address: s, Raw: s}, nil
		}
		if strings.Contains(s, ":") {
			return Endpoint{Network: "tcp", Address: s, Raw: s}, nil
		}
		return Endpoint{}, fmt.Errorf(
			"unsupported endpoint %q; use tcp://host:port or unix:///path.sock", s,
		)
	}
}

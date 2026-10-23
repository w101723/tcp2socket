package cli

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

type nopCloser struct {
	io.Reader
	io.Writer
}

func (n nopCloser) Close() error { return nil }

func TestRunExitCodes(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantExit   int
		wantSubstr string
	}{
		{
			name:       "HelpFlag",
			args:       []string{"-h"},
			wantExit:   0,
			wantSubstr: "tcp2socket - TCP / Unix Socket / STDIO bidirectional proxy",
		},
		{
			name:       "HelpLongFlag",
			args:       []string{"--help"},
			wantExit:   0,
			wantSubstr: "tcp2socket - TCP / Unix Socket / STDIO bidirectional proxy",
		},
		{
			name:       "ConnectHelpFlag",
			args:       []string{"connect", "-h"},
			wantExit:   0,
			wantSubstr: "Usage:\n  tcp2socket connect [options] <target>",
		},
		{
			name:       "SocksStdioHelpFlag",
			args:       []string{"socks-stdio", "-h"},
			wantExit:   0,
			wantSubstr: "Usage:\n  tcp2socket socks-stdio [options]",
		},
		{
			name:       "ProxyMissingArgs",
			args:       []string{},
			wantExit:   2,
			wantSubstr: "Options:",
		},
		{
			name:       "ProxyMissingTarget",
			args:       []string{"-l", ":8080"},
			wantExit:   2,
			wantSubstr: "Options:",
		},
		{
			name:       "ProxyMissingListen",
			args:       []string{"-t", ":8080"},
			wantExit:   2,
			wantSubstr: "Options:",
		},
		{
			name:       "ConnectMissingTarget",
			args:       []string{"connect"},
			wantExit:   2,
			wantSubstr: "Usage:\n  tcp2socket connect [options] <target>",
		},
		{
			name:       "SocksStdioExtraArg",
			args:       []string{"socks-stdio", "extra"},
			wantExit:   2,
			wantSubstr: "tcp2socket socks-stdio",
		},
		{
			name:       "InvalidEndpointListen",
			args:       []string{"-l", "invalid_ep", "-t", ":8080"},
			wantExit:   1,
			wantSubstr: "invalid listen endpoint",
		},
		{
			name:       "InvalidEndpointTarget",
			args:       []string{"-l", ":8080", "-t", "invalid_ep"},
			wantExit:   1,
			wantSubstr: "invalid target endpoint",
		},
		{
			name:       "InvalidResourceLimitsMaxConn",
			args:       []string{"-l", ":8080", "-t", ":8081", "-max-connections", "0"},
			wantExit:   1,
			wantSubstr: "invalid resource limits",
		},
		{
			name:       "NegativeTimeout",
			args:       []string{"-l", ":8080", "-t", ":8081", "-dial-timeout", "-1s"},
			wantExit:   1,
			wantSubstr: "invalid resource limits",
		},
		{
			name:       "InvalidUnixModeNonOctal",
			args:       []string{"-l", "/tmp/test.sock", "-t", ":8081", "-unix-mode", "999"},
			wantExit:   1,
			wantSubstr: "invalid --unix-mode",
		},
		{
			name:       "InvalidUnixModeTooLarge",
			args:       []string{"-l", "/tmp/test.sock", "-t", ":8081", "-unix-mode", "1777"},
			wantExit:   1,
			wantSubstr: "invalid --unix-mode",
		},
		{
			name:       "ConnectInvalidTarget",
			args:       []string{"connect", "invalid_ep"},
			wantExit:   1,
			wantSubstr: "invalid target endpoint",
		},
		{
			name:       "ConnectNegativeTimeout",
			args:       []string{"connect", "-dial-timeout", "-1s", ":8080"},
			wantExit:   1,
			wantSubstr: "invalid dial timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			stdin := nopCloser{Reader: strings.NewReader("")}
			stdout := nopCloser{Writer: &bytes.Buffer{}}

			code := Run(tc.args, stdin, stdout, &stderr)
			if code != tc.wantExit {
				t.Fatalf("Run(%v) exit code = %d, want %d. Output: %s", tc.args, code, tc.wantExit, stderr.String())
			}

			if tc.wantSubstr != "" && !strings.Contains(stderr.String(), tc.wantSubstr) {
				t.Fatalf("Run(%v) stderr = %q, want substring %q", tc.args, stderr.String(), tc.wantSubstr)
			}
		})
	}
}

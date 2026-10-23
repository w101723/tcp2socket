package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"runtime"

	"tcp2socket/internal/buildinfo"
)

// runVersionCLI implements `tcp2socket version` (plan §5): print version,
// commit, go and os/arch. Build metadata comes from -ldflags; a plain
// `go build` shows the documented dev/unknown defaults.
func runVersionCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, `Usage:
  tcp2socket version

Prints version, commit, go and os/arch. Build metadata is injected with
-ldflags "-X tcp2socket/internal/buildinfo.Version=... -X tcp2socket/internal/buildinfo.Commit=...";
without it the values are dev/unknown.
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return 2
	}

	fmt.Fprintf(stdout, "version=%s\n", buildinfo.Version)
	fmt.Fprintf(stdout, "commit=%s\n", buildinfo.Commit)
	fmt.Fprintf(stdout, "go=%s\n", runtime.Version())
	fmt.Fprintf(stdout, "os/arch=%s/%s\n", runtime.GOOS, runtime.GOARCH)
	return 0
}

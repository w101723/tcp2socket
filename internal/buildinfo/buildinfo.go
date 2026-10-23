// Package buildinfo carries version metadata injected at build time via
// -ldflags, e.g.:
//
//	go build -ldflags "-X tcp2socket/internal/buildinfo.Version=v0.3.0 \
//	                  -X tcp2socket/internal/buildinfo.Commit=$(git rev-parse --short HEAD)" .
//
// Missing metadata stays at the documented defaults ("dev"/"unknown"); the
// doctor build_info check reports them as a WARN so unreleased binaries are
// never mistaken for a tagged release.
package buildinfo

var (
	Version = "dev"
	Commit  = "unknown"
)

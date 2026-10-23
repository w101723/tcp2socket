//go:build !unix

package doctor

import "context"

func checkFDLimit(ctx context.Context) Report {
	return Report{Status: Unavailable, Detail: "file descriptor limit reporting not available on this platform"}
}

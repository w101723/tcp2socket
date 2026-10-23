//go:build unix

package doctor

import (
	"context"
	"fmt"
	"syscall"
)

// minSoftFDs is the soft nofile floor below which the default proxy shape
// (128 workers x 2 sockets + listener + stdio) gets uncomfortable.
const minSoftFDs = 512

func checkFDLimit(ctx context.Context) Report {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return Report{Status: Unavailable, Detail: fmt.Sprintf("getrlimit(RLIMIT_NOFILE): %v", err)}
	}
	status := Pass
	extra := ""
	if lim.Cur < minSoftFDs {
		status = Warn
		extra = fmt.Sprintf("; soft limit below %d — raise with `ulimit -n` before running high-concurrency proxies", minSoftFDs)
	}
	return Report{Status: status, Detail: fmt.Sprintf("nofile soft=%d hard=%d%s", lim.Cur, lim.Max, extra)}
}

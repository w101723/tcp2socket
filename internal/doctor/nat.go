package doctor

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

// checkNAT is the opt-in, read-only MASQUERADE hint behind --inspect-nat
// (plan §5). It runs `iptables-save -t nat`, never mutates anything, and
// reports rules that could reproduce the dubhe-vpn loopback rewrite: a
// MASQUERADE rule with no destination or outbound-interface condition can
// match loopback traffic and rewrite its source. Findings are suggestions;
// the doctor never executes fixes and never touches nft/iptables state.
func checkNAT(ctx context.Context) Report {
	if runtime.GOOS != "linux" {
		return Report{Status: Unavailable, Detail: "--inspect-nat is only meaningful on Linux"}
	}
	path, err := exec.LookPath("iptables-save")
	if err != nil {
		return Report{Status: Unavailable, Detail: "iptables-save not found in PATH; NAT inspection unavailable (read-only tool, no fallback is attempted)"}
	}
	cmd := exec.CommandContext(ctx, path, "-t", "nat")
	out, err := cmd.Output()
	if err != nil {
		return Report{Status: Unavailable, Detail: fmt.Sprintf("iptables-save -t nat: %v (read-only dump; usually requires root)", err)}
	}
	unconditional, scoped := masqueradeFindings(string(out))
	if len(unconditional) == 0 {
		return Report{Status: Pass, Detail: fmt.Sprintf("no unconditional MASQUERADE detected (%d scoped MASQUERADE rule(s))", scoped)}
	}
	return Report{
		Status: Warn,
		Detail: fmt.Sprintf("%d unconditional MASQUERADE rule(s) (no -d/-o condition) can rewrite loopback sources; consider adding a loopback exception such as an `-o lo` accept before MASQUERADE; suggestion only, nothing was modified: %s",
			len(unconditional), strings.Join(unconditional, " | ")),
	}
}

// masqueradeFindings splits iptables-save output into unconditional and
// scoped MASQUERADE rules. A rule is unconditional when its match section
// (before -j/--jump) contains no source, destination or interface condition.
// This is a hint heuristic, not a policy engine.
func masqueradeFindings(output string) (unconditional []string, scoped int) {
	condTokens := map[string]bool{
		"-s": true, "--source": true, "--src": true,
		"-d": true, "--destination": true, "--dst": true,
		"-i": true, "--in-interface": true,
		"-o": true, "--out-interface": true,
	}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "-A ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		jump := -1
		for i := 2; i < len(fields)-1; i++ {
			if fields[i] == "-j" || fields[i] == "--jump" {
				jump = i
				break
			}
		}
		if jump < 0 || fields[jump+1] != "MASQUERADE" {
			continue
		}
		conditioned := false
		for _, tok := range fields[2:jump] {
			if condTokens[tok] {
				conditioned = true
				break
			}
		}
		if conditioned {
			scoped++
		} else {
			unconditional = append(unconditional, line)
		}
	}
	return unconditional, scoped
}

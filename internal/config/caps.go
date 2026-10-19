package config

import (
	"fmt"
	"strconv"
	"strings"
)

// Common capability names -> numbers (linux/capability.h order).
var capNames = map[string]uintptr{
	"chown":              0,
	"dac_override":       1,
	"dac_read_search":    2,
	"fowner":             3,
	"fsetid":             4,
	"kill":               5,
	"setgid":             6,
	"setuid":             7,
	"setpcap":            8,
	"linux_immutable":    9,
	"net_bind_service":   10,
	"net_broadcast":      11,
	"net_admin":          12,
	"net_raw":            13,
	"ipc_lock":           14,
	"ipc_owner":          15,
	"sys_module":         16,
	"sys_rawio":          17,
	"sys_chroot":         18,
	"sys_ptrace":         19,
	"sys_pacct":          20,
	"sys_admin":          21,
	"sys_boot":           22,
	"sys_nice":           23,
	"sys_resource":       24,
	"sys_time":           25,
	"sys_tty_config":     26,
	"mknod":              27,
	"lease":              28,
	"audit_write":        29,
	"audit_control":      30,
	"setfcap":            31,
	"mac_override":       32,
	"mac_admin":          33,
	"syslog":             34,
	"wake_alarm":         35,
	"block_suspend":      36,
	"audit_read":         37,
	"perfmon":            38,
	"bpf":                39,
	"checkpoint_restore": 40,
}

// CapName returns the canonical name of a capability; unknown numbers return "cap_<n>".
func CapName(c uintptr) string {
	// capNames has no duplicate numbers, so the reverse lookup is deterministic.
	best := ""
	for name, n := range capNames {
		if n == c && (best == "" || name < best) {
			best = name
		}
	}
	if best != "" {
		return "cap_" + best
	}
	return fmt.Sprintf("cap_%d", c)
}

// parseCaps parses a capabilities list, supporting:
//   - names: net_bind_service, CAP_NET_BIND_SERVICE
//   - numbers: 10, cap_10
//   - the "cap_" prefix may be omitted
//   - the special value "all" means every capability supported by the current kernel
func parseCaps(items []string) ([]uintptr, error) {
	if len(items) == 0 {
		return nil, nil
	}
	seen := make(map[uintptr]bool, len(items))
	var out []uintptr
	add := func(c uintptr) error {
		if c > 63 {
			return fmt.Errorf("capability number %d is out of range (must be 0-63)", c)
		}
		if seen[c] {
			return nil
		}
		seen[c] = true
		out = append(out, c)
		return nil
	}
	for _, item := range items {
		name := strings.ToLower(strings.TrimSpace(item))
		name = strings.TrimPrefix(name, "cap_")
		if name == "all" {
			// The upper bound of capabilities the kernel supports depends on its version; this covers up to 40.
			for c := uintptr(0); c <= 40; c++ {
				if err := add(c); err != nil {
					return nil, err
				}
			}
			continue
		}
		if n, ok := capNames[name]; ok {
			if err := add(n); err != nil {
				return nil, err
			}
			continue
		}
		n, err := strconv.Atoi(name)
		if err != nil {
			return nil, fmt.Errorf("unknown capability %q", item)
		}
		if err := add(uintptr(n)); err != nil {
			return nil, fmt.Errorf("capability %q: %w", item, err)
		}
	}
	return out, nil
}

package config

import (
	"fmt"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/ini.v1"
)

// section is an ini section reader that collects errors: it reports all invalid values at once instead of returning on the first one.
type section struct {
	sec    *ini.Section
	known  map[string]bool
	issues []string
}

// newSection creates a reader; known lists the allowed key names, and any other key is treated as a typo.
func newSection(sec *ini.Section, known ...string) *section {
	k := make(map[string]bool, len(known))
	for _, name := range known {
		k[name] = true
	}
	return &section{sec: sec, known: k}
}

func (s *section) raw(key string) (string, bool) {
	k, err := s.sec.GetKey(key)
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(k.String()), true
}

func (s *section) fail(key, format string, args ...any) {
	s.issues = append(s.issues, key+": "+fmt.Sprintf(format, args...))
}

func (s *section) errsOrNil() error {
	if len(s.issues) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(s.issues, "; "))
}

func (s *section) str(key, def string) string {
	if v, ok := s.raw(key); ok {
		return v
	}
	return def
}

func (s *section) boolean(key string, def bool) bool {
	v, ok := s.raw(key)
	if !ok || v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on", "y", "t":
		return true
	case "0", "false", "no", "off", "n", "f":
		return false
	}
	s.fail(key, "expected a boolean (true/false), got %q", v)
	return def
}

func (s *section) integer(key string, def int) int {
	v, ok := s.raw(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		s.fail(key, "expected an integer, got %q", v)
		return def
	}
	return n
}

func (s *section) duration(key string, def time.Duration) time.Duration {
	v, ok := s.raw(key)
	if !ok || v == "" {
		return def
	}
	d, err := ParseDuration(v)
	if err != nil {
		s.fail(key, "%v", err)
		return def
	}
	return d
}

func (s *section) list(key string, def []string) []string {
	v, ok := s.raw(key)
	if !ok || v == "" {
		return def
	}
	return SplitList(v)
}

func (s *section) signal(key string, def syscall.Signal) syscall.Signal {
	v, ok := s.raw(key)
	if !ok || v == "" {
		return def
	}
	sig, err := ParseSignal(v)
	if err != nil {
		s.fail(key, "%v", err)
		return def
	}
	return sig
}

// errs checks for unknown keys and consolidates the collected errors, returning nil or one summary error.
func (s *section) errs() error {
	for _, k := range s.sec.Keys() {
		name := strings.ToLower(k.Name())
		if name == "default" {
			continue
		}
		if !s.known[name] {
			s.fail(k.Name(), "unknown config key")
		}
	}
	return s.errsOrNil()
}

// ParseDuration parses a duration: values without a unit are treated as seconds, and forms like 500ms, 2m and 1h are also accepted.
func ParseDuration(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, fmt.Errorf("duration is empty")
	}
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		return time.Duration(n * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q (examples: 10, 500ms, 2m)", v)
	}
	return d, nil
}

// ParseSignal parses a signal name or number.
func ParseSignal(v string) (syscall.Signal, error) {
	v = strings.TrimSpace(v)
	upper := strings.ToUpper(v)
	if !strings.HasPrefix(upper, "SIG") {
		upper = "SIG" + upper
	}
	if sig, ok := signalNames[upper]; ok {
		return sig, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid signal %q", v)
	}
	if sig, ok := signalNames[fmt.Sprintf("SIG%d", n)]; ok {
		return sig, nil
	}
	if n < 1 || n > 64 {
		return 0, fmt.Errorf("signal number %d is out of range", n)
	}
	return syscall.Signal(n), nil
}

var signalNames = map[string]syscall.Signal{
	"SIGHUP":    syscall.SIGHUP,
	"SIGINT":    syscall.SIGINT,
	"SIGQUIT":   syscall.SIGQUIT,
	"SIGILL":    syscall.SIGILL,
	"SIGTRAP":   syscall.SIGTRAP,
	"SIGABRT":   syscall.SIGABRT,
	"SIGBUS":    syscall.SIGBUS,
	"SIGFPE":    syscall.SIGFPE,
	"SIGKILL":   syscall.SIGKILL,
	"SIGUSR1":   syscall.SIGUSR1,
	"SIGSEGV":   syscall.SIGSEGV,
	"SIGUSR2":   syscall.SIGUSR2,
	"SIGPIPE":   syscall.SIGPIPE,
	"SIGALRM":   syscall.SIGALRM,
	"SIGTERM":   syscall.SIGTERM,
	"SIGSTKFLT": syscall.SIGSTKFLT,
	"SIGCHLD":   syscall.SIGCHLD,
	"SIGCONT":   syscall.SIGCONT,
	"SIGSTOP":   syscall.SIGSTOP,
	"SIGTSTP":   syscall.SIGTSTP,
	"SIGTTIN":   syscall.SIGTTIN,
	"SIGTTOU":   syscall.SIGTTOU,
	"SIGURG":    syscall.SIGURG,
	"SIGXCPU":   syscall.SIGXCPU,
	"SIGXFSZ":   syscall.SIGXFSZ,
	"SIGVTALRM": syscall.SIGVTALRM,
	"SIGPROF":   syscall.SIGPROF,
	"SIGWINCH":  syscall.SIGWINCH,
	"SIGIO":     syscall.SIGIO,
	"SIGPWR":    syscall.SIGPWR,
	"SIGSYS":    syscall.SIGSYS,
}

// SplitList splits a list on commas, allowing double quotes to wrap elements that contain commas.
func SplitList(v string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for _, r := range v {
		switch {
		case r == '"':
			inQuote = !inQuote
		case r == ',' && !inQuote:
			if s := strings.TrimSpace(cur.String()); s != "" {
				out = append(out, s)
			}
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

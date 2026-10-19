// Package config parses gosup's ini configuration file.
//
// The configuration contains one [daemon] section and any number of [program:<id>] sections.
// Validation here is as strict as possible: unknown sections, unknown keys and invalid
// values all fail at startup, so typos cannot silently take effect.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"gopkg.in/ini.v1"
)

// Defaults.
const (
	DefaultListen          = "127.0.0.1:9090"
	DefaultPidFile         = "/run/gosup.pid"
	DefaultLogFile         = "/var/log/gosup.log"
	DefaultStateDir        = "/run/gosup"
	DefaultStopTimeout     = 10 * time.Second
	DefaultShutdownTimeout = 30 * time.Second
	DefaultOutputBuffer    = 256 * 1024
	DefaultStableAfter     = 60 * time.Second
	DefaultRestartDelay    = 1 * time.Second
	DefaultRestartDelayMax = 30 * time.Second
	DefaultLogLevel        = "info"

	// Local use does not need many concurrent connections; limiting them keeps runaway
	// clients from exhausting fds/goroutines. Each log follow is one SSE long-lived
	// connection, so this limit also caps the number of concurrent follows.
	DefaultMaxConnections = 16
)

// Daemon is the configuration for the [daemon] section.
type Daemon struct {
	Listen          string // HTTP listen address (POST /rpc, GET /events)
	TLSCert         string // optional: certificate path (enables https)
	TLSKey          string // optional: private key path
	Password        string // access password (plaintext)
	PasswordSHA256  string // access password (sha256 hex), takes precedence over Password
	PasswordFile    string // read the password from a file
	AllowNoAuth     bool   // allow no password (dangerous)
	PidFile         string // pid file, also used with flock to guarantee a single instance
	LogFile         string // in daemon mode, stdout/stderr are redirected here
	LogLevel        string // debug/info/warn/error
	StateDir        string // directory holding each program instance's pid/starttime records
	StopTimeout     time.Duration
	ShutdownTimeout time.Duration
	OutputBuffer    int // bytes retained for each program's stdout/stderr
	AllowedOrigins  []string

	MaxConnections int // max concurrent SSE long-lived connections (0 means unlimited)
}

// Restart policies.
const (
	RestartAlways    = "always"
	RestartOnFailure = "on-failure"
	RestartNever     = "never"
)

// Program is the configuration for a [program:<id>] section (including the resolved runtime identity and capabilities).
type Program struct {
	ID         string
	Command    string
	Argv       []string // parsed argv
	Shell      bool     // when true, run via /bin/sh -c
	WorkDir    string
	Enabled    bool
	Restart    string
	RestartGap time.Duration // initial restart delay
	RestartMax time.Duration // exponential backoff cap
	RestartNum int           // consecutive failures before giving up; 0 means unlimited
	StableFor  time.Duration // running longer than this counts as stable and resets the failure count

	// Runtime identity
	User     string
	Group    string
	Groups   []string
	SetCred  bool
	UID, GID int
	SuppGIDs []uint32
	NoSetGrp bool

	// capabilities
	Caps []uintptr

	StopSignal  syscall.Signal
	StopTimeout time.Duration

	// Output: ring (ring buffer only), discard, or an absolute path
	Stdout string
	Stderr string

	Env   []string
	Stdin string
}

// Config is the complete configuration.
type Config struct {
	Path     string
	Daemon   Daemon
	Programs []*Program
}

// Load reads and validates the configuration file.
func Load(path string) (*Config, error) {
	opts := ini.LoadOptions{
		// Do not parse inline comments, otherwise a ";" inside command would be truncated.
		IgnoreInlineComment:         true,
		UnescapeValueDoubleQuotes:   true,
		InsensitiveKeys:             true,
		InsensitiveSections:         false,
		AllowBooleanKeys:            false,
		PreserveSurroundedQuote:     false,
		UnescapeValueCommentSymbols: false,
		// Do not merge sections with the same name, making duplicates easy to detect and report.
		AllowNonUniqueSections: true,
	}
	f, err := ini.LoadSources(opts, path)
	if err == nil {
		// The ini library merges sections with the same name; without an explicit check the later one silently overrides the earlier one.
		if dup := duplicateSection(f); dup != "" {
			return nil, fmt.Errorf("%s: section [%s] is duplicated", path, dup)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read config %s: %w", path, err)
	}

	cfg := &Config{Path: path}
	cfg.Daemon = Daemon{
		Listen:          DefaultListen,
		PidFile:         DefaultPidFile,
		LogFile:         DefaultLogFile,
		LogLevel:        DefaultLogLevel,
		StateDir:        DefaultStateDir,
		StopTimeout:     DefaultStopTimeout,
		ShutdownTimeout: DefaultShutdownTimeout,
		OutputBuffer:    DefaultOutputBuffer,
		MaxConnections:  DefaultMaxConnections,
	}

	seenDaemon := false
	for _, sec := range f.Sections() {
		name := sec.Name()
		switch {
		case strings.EqualFold(name, "DEFAULT"):
			if len(sec.Keys()) > 0 {
				return nil, fmt.Errorf("%s: keys must be written inside the [daemon] or [program:<id>] section", path)
			}
		case strings.EqualFold(name, "daemon"):
			if seenDaemon {
				return nil, fmt.Errorf("%s: [daemon] section is duplicated", path)
			}
			seenDaemon = true
			if err := parseDaemon(&cfg.Daemon, sec); err != nil {
				return nil, fmt.Errorf("%s: [daemon]: %w", path, err)
			}
		case strings.HasPrefix(strings.ToLower(name), "program:"):
			id := strings.TrimSpace(name[len("program:"):])
			if id == "" {
				return nil, fmt.Errorf("%s: [program:<id>] id must not be empty", path)
			}
			p, err := parseProgram(id, sec, cfg.Daemon)
			if err != nil {
				return nil, fmt.Errorf("%s: [program:%s]: %w", path, id, err)
			}
			cfg.Programs = append(cfg.Programs, p)
		default:
			return nil, fmt.Errorf("%s: unknown section [%s] (only [daemon] and [program:<id>] are allowed)", path, name)
		}
	}

	if !seenDaemon {
		return nil, fmt.Errorf("%s: missing [daemon] section", path)
	}
	if err := finalizeDaemon(&cfg.Daemon); err != nil {
		return nil, fmt.Errorf("%s: [daemon]: %w", path, err)
	}

	ids := make(map[string]bool, len(cfg.Programs))
	for _, p := range cfg.Programs {
		if ids[p.ID] {
			return nil, fmt.Errorf("%s: program id %q is duplicated", path, p.ID)
		}
		ids[p.ID] = true
	}
	return cfg, nil
}

// duplicateSection returns the first duplicate section name (original case preserved).
func duplicateSection(f *ini.File) string {
	seen := make(map[string]bool)
	for _, sec := range f.Sections() {
		name := strings.ToLower(sec.Name())
		if seen[name] {
			return sec.Name()
		}
		seen[name] = true
	}
	return ""
}

func parseDaemon(d *Daemon, sec *ini.Section) error {
	s := newSection(sec,
		"listen", "tls_cert", "tls_key", "password", "password_sha256", "password_file",
		"allow_no_auth", "pid_file", "log_file", "log_level", "state_dir",
		"stop_timeout", "shutdown_timeout", "output_buffer_bytes", "allowed_origins",
		"max_connections",
	)

	d.Listen = s.str("listen", d.Listen)
	d.TLSCert = s.str("tls_cert", d.TLSCert)
	d.TLSKey = s.str("tls_key", d.TLSKey)
	d.Password = s.str("password", d.Password)
	d.PasswordSHA256 = strings.ToLower(strings.TrimSpace(s.str("password_sha256", d.PasswordSHA256)))
	d.PasswordFile = s.str("password_file", d.PasswordFile)
	d.AllowNoAuth = s.boolean("allow_no_auth", d.AllowNoAuth)
	d.PidFile = s.str("pid_file", d.PidFile)
	d.LogFile = s.str("log_file", d.LogFile)
	d.LogLevel = strings.ToLower(s.str("log_level", d.LogLevel))
	d.StateDir = s.str("state_dir", d.StateDir)
	d.StopTimeout = s.duration("stop_timeout", d.StopTimeout)
	d.ShutdownTimeout = s.duration("shutdown_timeout", d.ShutdownTimeout)
	d.OutputBuffer = s.integer("output_buffer_bytes", d.OutputBuffer)
	d.AllowedOrigins = s.list("allowed_origins", d.AllowedOrigins)
	d.MaxConnections = s.integer("max_connections", d.MaxConnections)

	if err := s.errs(); err != nil {
		return err
	}
	return nil
}

func finalizeDaemon(d *Daemon) error {
	d.Listen = strings.TrimSpace(d.Listen)
	if d.Listen == "" {
		return fmt.Errorf("listen must not be empty")
	}
	if (d.TLSCert == "") != (d.TLSKey == "") {
		return fmt.Errorf("tls_cert and tls_key must be configured together")
	}
	if d.StopTimeout <= 0 {
		return fmt.Errorf("stop_timeout must be greater than 0")
	}
	if d.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown_timeout must be greater than 0")
	}
	if d.OutputBuffer < 4096 {
		return fmt.Errorf("output_buffer_bytes must be at least 4096")
	}
	if d.MaxConnections < 0 {
		return fmt.Errorf("max_connections must not be negative (0 means unlimited)")
	}
	switch d.LogLevel {
	case "debug", "info", "warn", "warning", "error":
		if d.LogLevel == "warning" {
			d.LogLevel = "warn"
		}
	default:
		return fmt.Errorf("log_level must be one of debug/info/warn/error")
	}

	if d.PasswordFile != "" {
		b, err := os.ReadFile(d.PasswordFile)
		if err != nil {
			return fmt.Errorf("failed to read password_file: %w", err)
		}
		if d.Password == "" {
			d.Password = strings.TrimSpace(string(b))
		}
	}
	if d.PasswordSHA256 != "" {
		if _, err := hex.DecodeString(d.PasswordSHA256); err != nil || len(d.PasswordSHA256) != 64 {
			return fmt.Errorf("password_sha256 must be 64 hexadecimal characters")
		}
		d.Password = ""
	}
	if d.Password == "" && d.PasswordSHA256 == "" && !d.AllowNoAuth {
		return fmt.Errorf("password or password_sha256 must be configured (if authentication is really not needed, set allow_no_auth = true)")
	}
	if d.PidFile == "" {
		return fmt.Errorf("pid_file must not be empty")
	}
	if d.StateDir == "" {
		return fmt.Errorf("state_dir must not be empty")
	}
	return nil
}

func parseProgram(id string, sec *ini.Section, d Daemon) (*Program, error) {
	if !validID(id) {
		return nil, fmt.Errorf("id may only contain letters, digits, '_', '-', '.'")
	}
	s := newSection(sec,
		"command", "shell", "workdir", "enabled", "restart", "restart_delay",
		"restart_delay_max", "restart_max", "stable_after", "user", "group",
		"supplementary_groups", "capabilities", "stop_signal", "stop_timeout",
		"stdout", "stderr", "env", "stdin",
	)

	p := &Program{
		ID:          id,
		Enabled:     true,
		Restart:     RestartAlways,
		RestartGap:  DefaultRestartDelay,
		RestartMax:  DefaultRestartDelayMax,
		RestartNum:  0,
		StableFor:   DefaultStableAfter,
		StopSignal:  syscall.SIGTERM,
		StopTimeout: d.StopTimeout,
		Stdout:      "ring",
		Stderr:      "ring",
	}

	p.Command = s.str("command", "")
	p.Shell = s.boolean("shell", false)
	p.WorkDir = s.str("workdir", "")
	p.Enabled = s.boolean("enabled", true)
	p.Restart = strings.ToLower(strings.TrimSpace(s.str("restart", p.Restart)))
	p.RestartGap = s.duration("restart_delay", p.RestartGap)
	p.RestartMax = s.duration("restart_delay_max", p.RestartMax)
	p.RestartNum = s.integer("restart_max", p.RestartNum)
	p.StableFor = s.duration("stable_after", p.StableFor)
	p.User = strings.TrimSpace(s.str("user", ""))
	p.Group = strings.TrimSpace(s.str("group", ""))
	p.Groups = s.list("supplementary_groups", nil)
	p.StopSignal = s.signal("stop_signal", p.StopSignal)
	p.StopTimeout = s.duration("stop_timeout", p.StopTimeout)
	p.Stdout = strings.TrimSpace(s.str("stdout", p.Stdout))
	p.Stderr = strings.TrimSpace(s.str("stderr", p.Stderr))
	p.Env = s.list("env", nil)
	p.Stdin = strings.TrimSpace(s.str("stdin", ""))
	caps := s.list("capabilities", nil)

	if err := s.errs(); err != nil {
		return nil, err
	}

	if p.Command == "" {
		return nil, fmt.Errorf("command must not be empty")
	}
	if p.Restart != RestartAlways && p.Restart != RestartOnFailure && p.Restart != RestartNever {
		return nil, fmt.Errorf("restart must be one of always/on-failure/never")
	}
	if p.RestartGap <= 0 {
		return nil, fmt.Errorf("restart_delay must be greater than 0")
	}
	if p.RestartMax < p.RestartGap {
		return nil, fmt.Errorf("restart_delay_max must not be less than restart_delay")
	}
	if p.RestartNum < 0 {
		return nil, fmt.Errorf("restart_max must not be negative")
	}
	if p.StableFor <= 0 {
		return nil, fmt.Errorf("stable_after must be greater than 0")
	}
	if p.StopTimeout <= 0 {
		return nil, fmt.Errorf("stop_timeout must be greater than 0")
	}
	if p.Group != "" && p.User == "" {
		return nil, fmt.Errorf("user must also be configured when group is configured")
	}
	if p.WorkDir != "" {
		st, err := os.Stat(p.WorkDir)
		if err != nil {
			return nil, fmt.Errorf("workdir %q: %w", p.WorkDir, err)
		}
		if !st.IsDir() {
			return nil, fmt.Errorf("workdir %q is not a directory", p.WorkDir)
		}
	}
	for _, dest := range []string{p.Stdout, p.Stderr} {
		if dest == "ring" || dest == "discard" {
			continue
		}
		if !strings.HasPrefix(dest, "/") {
			return nil, fmt.Errorf("stdout/stderr must be one of ring, discard, or an absolute path, got %q", dest)
		}
	}
	for _, e := range p.Env {
		if !strings.Contains(e, "=") {
			return nil, fmt.Errorf("env item %q is missing '='", e)
		}
	}
	if p.Stdin != "" && p.Stdin != "null" && !strings.HasPrefix(p.Stdin, "/") {
		return nil, fmt.Errorf("stdin must be one of null, /dev/null, or an absolute path, got %q", p.Stdin)
	}

	argv, err := buildArgv(p.Command, p.Shell)
	if err != nil {
		return nil, err
	}
	p.Argv = argv

	capsOut, err := parseCaps(caps)
	if err != nil {
		return nil, err
	}
	p.Caps = capsOut

	if err := resolveIdentity(p); err != nil {
		return nil, err
	}
	return p, nil
}

func buildArgv(command string, shell bool) ([]string, error) {
	if shell {
		return []string{"/bin/sh", "-c", command}, nil
	}
	argv, err := splitCommand(command)
	if err != nil {
		return nil, fmt.Errorf("failed to parse command: %w", err)
	}
	if len(argv) == 0 {
		return nil, fmt.Errorf("command is empty after parsing")
	}
	return argv, nil
}

func resolveIdentity(p *Program) error {
	euid := os.Geteuid()
	want := p.User != "" || p.Group != "" || len(p.Groups) > 0
	if !want {
		if len(p.Caps) > 0 && euid != 0 {
			return fmt.Errorf("configuring capabilities requires running gosup as root")
		}
		return nil
	}

	p.SetCred = true
	r := defaultResolver()
	uid, gid := 0, 0
	explicitGroup := p.Group != ""

	if p.User != "" {
		var supp []uint32
		var err error
		uid, gid, supp, err = r.userID(p.User)
		if err != nil {
			return err
		}
		p.SuppGIDs = append(p.SuppGIDs, supp...)
	}
	if p.Group != "" {
		g, err := r.groupID(p.Group)
		if err != nil {
			return err
		}
		gid = g
	}
	for _, name := range p.Groups {
		g, err := r.groupID(name)
		if err != nil {
			return fmt.Errorf("supplementary group: %w", err)
		}
		p.SuppGIDs = append(p.SuppGIDs, uint32(g))
	}
	// Keeping uid at 0 when only group is configured without user makes no sense;
	// group already requires user, so stay consistent with that.
	_ = explicitGroup
	p.UID, p.GID = uid, gid

	if euid != 0 {
		if uid != euid {
			return fmt.Errorf("when gosup runs as non-root, a program can only switch to the current user (current uid=%d, configured uid=%d)", euid, uid)
		}
		if gid != os.Getegid() {
			return fmt.Errorf("when gosup runs as non-root, only the current group can be used (current gid=%d, configured gid=%d)", os.Getegid(), gid)
		}
		if len(p.Caps) > 0 {
			return fmt.Errorf("configuring capabilities requires running gosup as root")
		}
		// A non-root process cannot call setgroups.
		p.NoSetGrp = true
		p.SuppGIDs = nil
	}
	// Deduplicate while keeping a stable order.
	p.SuppGIDs = dedupUint32(p.SuppGIDs)
	return nil
}

func dedupUint32(in []uint32) []uint32 {
	if len(in) == 0 {
		return in
	}
	seen := make(map[uint32]bool, len(in))
	out := in[:0]
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func validID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.':
		default:
			return false
		}
	}
	return true
}

// SHA256Hex computes the password digest.
func SHA256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

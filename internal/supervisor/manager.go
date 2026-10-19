// Package supervisor starts, monitors, restarts and stops managed programs.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"gosup/internal/config"
	"gosup/internal/logx"
	"gosup/internal/output"
)

// Common errors so the RPC layer can distinguish them (and return suitable error codes).
var (
	ErrAlreadyRunning = errors.New("program is already running")
	ErrNotRunning     = errors.New("program is not running")
	ErrNotFound       = errors.New("program not found")
	ErrStopping       = errors.New("program is stopping")
)

// State is the lifecycle phase of a program.
type State string

const (
	StateStopped  State = "stopped"
	StateStarting State = "starting"
	StateRunning  State = "running"
	StateStopping State = "stopping"
	StateBackoff  State = "backoff" // waiting for the next restart
)

// Status is a snapshot of a program's state.
type Status struct {
	ID      string `json:"id"`
	Enabled bool   `json:"enabled"`
	State   State  `json:"state"`
	Running bool   `json:"running"`
	PID     int    `json:"pid"`
	Pgid    int    `json:"pgid"`
	// StartedAt is the start time of this instance (wall clock). Clients derive uptime
	// by subtracting it from the current time; the server does not provide uptime:
	// CLOCK_MONOTONIC excludes device suspend, which does not match the user's perception.
	StartedAt     *time.Time `json:"started_at,omitempty"`
	Restarts      int        `json:"restarts"`
	ExitCode      *int       `json:"exit_code,omitempty"`
	Signal        string     `json:"signal,omitempty"`
	StoppedAt     *time.Time `json:"stopped_at,omitempty"`
	NextRestartAt *time.Time `json:"next_restart_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	Command       string     `json:"command"`
	Argv          []string   `json:"argv"`
	WorkDir       string     `json:"workdir"`
	User          string     `json:"user,omitempty"`
	Capabilities  []string   `json:"capabilities,omitempty"`
	RestartPolicy string     `json:"restart_policy"`
	PendingReload bool       `json:"pending_reload,omitempty"`
}

// ReloadResult describes the outcome of a configuration reload.
type ReloadResult struct {
	Added           []string `json:"added,omitempty"`
	Removed         []string `json:"removed,omitempty"`
	Updated         []string `json:"updated,omitempty"`
	Restarted       []string `json:"restarted,omitempty"`
	Started         []string `json:"started,omitempty"`
	Stopped         []string `json:"stopped,omitempty"`
	PendingRestart  []string `json:"pending_restart,omitempty"`
	RequiresRestart []string `json:"requires_restart,omitempty"`
	// DaemonUpdated lists the hot-reloadable [daemon] fields that took effect immediately.
	DaemonUpdated []string `json:"daemon_updated,omitempty"`
	Warnings      []string `json:"warnings,omitempty"`
	Unchanged     bool     `json:"unchanged"`
}

// Manager manages all programs.
type Manager struct {
	log *logx.Logger

	ctx  context.Context
	stop context.CancelFunc

	// stateDir and the config file path are immutable at runtime; Reload does not change them.
	stateDir string
	cfgPath  string

	cfg atomic.Pointer[config.Config]

	mu      sync.RWMutex
	progs   map[string]*Program
	order   []string
	started bool
	closed  bool
	wg      sync.WaitGroup

	reloadMu sync.Mutex
	verbose  bool
}

// SetVerbose keeps the debug log level across reloads (foreground --verbose).
func (m *Manager) SetVerbose(v bool) { m.verbose = v }

// New creates a Manager from cfg (without starting any program).
func New(cfg *config.Config, log *logx.Logger) (*Manager, error) {
	if err := os.MkdirAll(cfg.Daemon.StateDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create state_dir %s: %w", cfg.Daemon.StateDir, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		log:      log,
		ctx:      ctx,
		stop:     cancel,
		stateDir: cfg.Daemon.StateDir,
		cfgPath:  cfg.Path,
		progs:    make(map[string]*Program, len(cfg.Programs)),
		order:    make([]string, 0, len(cfg.Programs)),
	}
	m.cfg.Store(cfg)
	for _, pc := range cfg.Programs {
		m.progs[pc.ID] = m.newProgram(pc)
		m.order = append(m.order, pc.ID)
	}
	return m, nil
}

// newProgram creates a controller for a program that is not running yet.
func (m *Manager) newProgram(pc *config.Program) *Program {
	p := &Program{
		id:      pc.ID,
		cfg:     pc,
		manager: m,
		log:     m.log,
		stdout:  output.NewRing(m.cfg.Load().Daemon.OutputBuffer),
		stderr:  output.NewRing(m.cfg.Load().Daemon.OutputBuffer),
		cmdCh:   make(chan command),
		done:    make(chan struct{}),
		waitCh:  make(chan waitResult, 4),
		state:   StateStopped,
	}
	p.stop = stopParams{signal: pc.StopSignal, timeout: pc.StopTimeout}
	p.currentStopTimeout.Store(int64(pc.StopTimeout))
	p.publish()
	return p
}

// Config returns the currently effective configuration.
func (m *Manager) Config() *config.Config { return m.cfg.Load() }

// DaemonConfig returns the currently effective [daemon] config (for RPC to read
// hot-reloadable items such as the password).
func (m *Manager) DaemonConfig() config.Daemon { return m.cfg.Load().Daemon }

// ConfigPath returns the config file path.
func (m *Manager) ConfigPath() string { return m.cfgPath }

// Log returns the logger.
func (m *Manager) Log() *logx.Logger { return m.log }

// IDs returns all program ids (in config order).
func (m *Manager) IDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.order))
	copy(out, m.order)
	return out
}

// Statuses returns the status of all programs.
func (m *Manager) Statuses() []Status {
	m.mu.RLock()
	progs := make([]*Program, 0, len(m.order))
	for _, id := range m.order {
		progs = append(progs, m.progs[id])
	}
	m.mu.RUnlock()

	out := make([]Status, 0, len(progs))
	for _, p := range progs {
		out = append(out, p.Status())
	}
	return out
}

// Status returns the status of a single program.
func (m *Manager) Status(id string) (Status, error) {
	p, err := m.program(id)
	if err != nil {
		return Status{}, err
	}
	return p.Status(), nil
}

func (m *Manager) program(id string) (*Program, error) {
	m.mu.RLock()
	p, ok := m.progs[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return p, nil
}

// Start starts all enabled programs and launches a supervisor goroutine for each.
func (m *Manager) Start() {
	m.mu.Lock()
	if m.closed || m.started {
		m.mu.Unlock()
		return
	}
	m.started = true
	progs := make([]*Program, 0, len(m.order))
	for _, id := range m.order {
		progs = append(progs, m.progs[id])
	}
	m.mu.Unlock()

	for _, p := range progs {
		m.launch(p)
	}
	for _, p := range progs {
		if !p.Status().Enabled {
			m.log.Infof("program:"+p.id, "disabled, skipping start")
			continue
		}
		if err := p.Start(); err != nil {
			m.log.Errorf("program:"+p.id, "failed to start: %v", err)
		}
	}
}

// launch starts the program's supervisor goroutine.
func (m *Manager) launch(p *Program) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		p.run()
	}()
}

// StartProgram starts the program with the given id.
func (m *Manager) StartProgram(id string) error {
	p, err := m.program(id)
	if err != nil {
		return err
	}
	return p.Start()
}

// StopProgram stops the program with the given id.
func (m *Manager) StopProgram(id string) error {
	p, err := m.program(id)
	if err != nil {
		return err
	}
	return p.Stop()
}

// RestartProgram restarts the program with the given id.
func (m *Manager) RestartProgram(id string) error {
	p, err := m.program(id)
	if err != nil {
		return err
	}
	return p.Restart()
}

// Output returns a snapshot of a program's stdout/stderr; when lines > 0 only the
// last lines of each stream are kept.
func (m *Manager) Output(id string, stream string, lines int) (Output, error) {
	p, err := m.program(id)
	if err != nil {
		return Output{}, err
	}
	out := Output{ID: id}
	take := func(ring *output.Ring) (data []byte, trunc bool, total int64) {
		snap := ring.Snapshot()
		tail, cut := output.TailLines(snap.Data, lines)
		return tail, snap.Truncated || cut, snap.Total
	}
	switch stream {
	case "", "stdout":
		out.Stdout, out.StdoutTruncated, out.StdoutTotal = take(p.stdout)
	case "stderr":
		out.Stderr, out.StderrTruncated, out.StderrTotal = take(p.stderr)
	case "both":
		out.Stdout, out.StdoutTruncated, out.StdoutTotal = take(p.stdout)
		out.Stderr, out.StderrTruncated, out.StderrTotal = take(p.stderr)
	default:
		return Output{}, fmt.Errorf("unknown output stream %q (must be one of stdout/stderr/both)", stream)
	}
	return out, nil
}

// ValidStream reports whether the stream argument is valid.
func ValidStream(stream string) bool {
	switch stream {
	case "", "stdout", "stderr", "both":
		return true
	}
	return false
}

// OutputRings returns a program's output ring buffers for incremental reads in follow mode.
func (m *Manager) OutputRings(id string, stream string) (stdout, stderr *output.Ring, err error) {
	p, err := m.program(id)
	if err != nil {
		return nil, nil, err
	}
	switch stream {
	case "", "stdout":
		return p.stdout, nil, nil
	case "stderr":
		return nil, p.stderr, nil
	case "both":
		return p.stdout, p.stderr, nil
	}
	return nil, nil, fmt.Errorf("unknown output stream %q (must be one of stdout/stderr/both)", stream)
}

// ReloadFromDisk re-reads the config file (without applying it) for RPC reload.
func (m *Manager) ReloadFromDisk() (*config.Config, error) {
	return config.Load(m.cfgPath)
}

// Output is a snapshot of output.
type Output struct {
	ID              string
	Stdout          []byte
	Stderr          []byte
	StdoutTruncated bool
	StderrTruncated bool
	StdoutTotal     int64
	StderrTotal     int64
}

// Reload replaces the current configuration with a new one.
//
// Rules:
//   - Added programs: brought under management; started immediately if enabled.
//   - Removed programs: stopped and dropped from management.
//   - Existing programs whose config changed: config updated. With restart true they
//     are restarted/started/stopped as needed right away; otherwise the running process
//     keeps using the old config, and the new config applies on the next start (marked
//     as pending in the result).
//   - [daemon] items that cannot be hot-reloaded (listen, TLS, pid_file, state_dir,
//     log_file) are recorded in requires_restart; the daemon must be restarted.
func (m *Manager) Reload(newCfg *config.Config, restart bool) (ReloadResult, error) {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return ReloadResult{}, errors.New("supervisor is closed")
	}
	m.mu.RUnlock()

	old := m.cfg.Load()
	res := ReloadResult{}

	// Daemon items that cannot be hot-reloaded.
	if old.Daemon.Listen != newCfg.Daemon.Listen {
		res.RequiresRestart = append(res.RequiresRestart, "listen")
	}
	if old.Daemon.TLSCert != newCfg.Daemon.TLSCert || old.Daemon.TLSKey != newCfg.Daemon.TLSKey {
		res.RequiresRestart = append(res.RequiresRestart, "tls")
	}
	if old.Daemon.PidFile != newCfg.Daemon.PidFile {
		res.RequiresRestart = append(res.RequiresRestart, "pid_file")
	}
	if old.Daemon.StateDir != newCfg.Daemon.StateDir {
		res.RequiresRestart = append(res.RequiresRestart, "state_dir")
	}
	if old.Daemon.LogFile != newCfg.Daemon.LogFile {
		res.RequiresRestart = append(res.RequiresRestart, "log_file")
	}
	// Hot-reloadable [daemon] items: listed after applying so users don't think
	// the config did not change.
	hotDaemon := []struct {
		name    string
		changed bool
	}{
		{"password", old.Daemon.Password != newCfg.Daemon.Password},
		{"password_sha256", old.Daemon.PasswordSHA256 != newCfg.Daemon.PasswordSHA256},
		{"allow_no_auth", old.Daemon.AllowNoAuth != newCfg.Daemon.AllowNoAuth},
		{"allowed_origins", !reflect.DeepEqual(old.Daemon.AllowedOrigins, newCfg.Daemon.AllowedOrigins)},
		{"log_level", old.Daemon.LogLevel != newCfg.Daemon.LogLevel},
		{"stop_timeout", old.Daemon.StopTimeout != newCfg.Daemon.StopTimeout},
		{"shutdown_timeout", old.Daemon.ShutdownTimeout != newCfg.Daemon.ShutdownTimeout},
		{"output_buffer_bytes", old.Daemon.OutputBuffer != newCfg.Daemon.OutputBuffer},
		{"max_connections", old.Daemon.MaxConnections != newCfg.Daemon.MaxConnections},
	}
	for _, hd := range hotDaemon {
		if hd.changed {
			res.DaemonUpdated = append(res.DaemonUpdated, hd.name)
		}
	}

	oldProgs := make(map[string]*config.Program, len(old.Programs))
	for _, pc := range old.Programs {
		oldProgs[pc.ID] = pc
	}
	newProgs := make(map[string]*config.Program, len(newCfg.Programs))
	for _, pc := range newCfg.Programs {
		newProgs[pc.ID] = pc
	}

	// Update order first so the new config takes effect immediately in status queries.
	m.mu.Lock()
	oldOrder := m.order
	newOrder := make([]string, 0, len(newCfg.Programs))
	for _, pc := range newCfg.Programs {
		newOrder = append(newOrder, pc.ID)
	}

	// Remove programs that were dropped.
	var removed []*Program
	for _, id := range oldOrder {
		if _, ok := newProgs[id]; ok {
			continue
		}
		removed = append(removed, m.progs[id])
		delete(m.progs, id)
	}
	// Add new programs.
	var added []*Program
	for _, pc := range newCfg.Programs {
		if _, ok := m.progs[pc.ID]; ok {
			continue
		}
		p := m.newProgram(pc)
		m.progs[pc.ID] = p
		added = append(added, p)
	}
	// Collect existing programs together with their new config.
	type existing struct {
		p  *Program
		pc *config.Program
	}
	var existings []existing
	for _, id := range newOrder {
		if _, isNew := newProgs[id]; !isNew {
			continue
		}
		if _, wasOld := oldProgs[id]; !wasOld {
			continue
		}
		existings = append(existings, existing{p: m.progs[id], pc: newProgs[id]})
	}
	m.order = newOrder
	started := m.started
	m.mu.Unlock()

	for _, p := range removed {
		if err := p.remove(); err != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s: failed to stop: %v", p.id, err))
		}
		res.Removed = append(res.Removed, p.id)
	}

	for _, p := range added {
		if started {
			m.launch(p)
		}
		res.Added = append(res.Added, p.id)
		if started && p.cfg.Enabled {
			if err := p.Start(); err != nil {
				res.Warnings = append(res.Warnings, fmt.Sprintf("%s: failed to start: %v", p.id, err))
			} else {
				res.Started = append(res.Started, p.id)
			}
		}
	}

	for _, e := range existings {
		changed := !reflect.DeepEqual(oldProgs[e.pc.ID], e.pc)
		st := e.p.Status()
		// A previous reload left spawn parameters not yet applied: handle them when
		// restart is requested this time even if the config file did not change again.
		if !changed && !(restart && st.PendingReload) {
			continue
		}
		res.Updated = append(res.Updated, e.p.id)
		action := actNone
		if restart {
			switch {
			case !e.pc.Enabled && (st.Running || st.State == StateBackoff):
				action = actStop
				res.Stopped = append(res.Stopped, e.p.id)
			case e.pc.Enabled && (st.Running || st.State == StateBackoff):
				action = actRestart
				res.Restarted = append(res.Restarted, e.p.id)
			case e.pc.Enabled && !st.Running:
				action = actStart
				res.Started = append(res.Started, e.p.id)
			}
		}
		if err := e.p.update(e.pc, action); err != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("%s: failed to apply config: %v", e.p.id, err))
			continue
		}
		// Let Program decide whether the new config has spawn-only fields not yet applied.
		if !restart && e.p.Status().PendingReload {
			res.PendingRestart = append(res.PendingRestart, e.p.id)
		}
		// Buffer size is hot-adjustable; existing content is preserved.
		e.p.stdout.Resize(newCfg.Daemon.OutputBuffer)
		e.p.stderr.Resize(newCfg.Daemon.OutputBuffer)
	}

	// Log level and output buffer size can be hot-reloaded.
	m.cfg.Store(newCfg)
	if verbose := m.verbose; !verbose {
		m.log.SetLevel(logx.ParseLevel(newCfg.Daemon.LogLevel))
	} else {
		m.log.SetLevel(logx.Debug)
	}
	res.Unchanged = len(res.Added) == 0 && len(res.Removed) == 0 &&
		len(res.Updated) == 0 && len(res.DaemonUpdated) == 0
	return res, nil
}

// KillAll immediately SIGKILLs every child process group (for cases where a
// graceful exit is not possible).
func (m *Manager) KillAll() {
	m.mu.RLock()
	progs := make([]*Program, 0, len(m.order))
	for _, id := range m.order {
		progs = append(progs, m.progs[id])
	}
	m.mu.RUnlock()
	for _, p := range progs {
		p.forceKill()
	}
}

// Shutdown gracefully stops all programs and waits for the supervisor goroutines to exit.
func (m *Manager) Shutdown() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	started := m.started
	progs := make([]*Program, 0, len(m.order))
	for _, id := range m.order {
		progs = append(progs, m.progs[id])
	}
	m.mu.Unlock()

	// shutdown_timeout is the overall budget: all programs exit gracefully in
	// parallel, and are force-killed together once it expires.
	shutdownTimeout := m.cfg.Load().Daemon.ShutdownTimeout
	m.log.Infof("supervisor", "stopping all programs (up to %s)", shutdownTimeout)

	var wg sync.WaitGroup
	for _, p := range progs {
		wg.Add(1)
		go func(p *Program) {
			defer wg.Done()
			// If Start was never called there is no supervisor goroutine to receive commands; just clean up.
			if !started {
				p.cleanupStateFile()
				return
			}
			if err := p.shutdown(shutdownTimeout); err != nil {
				m.log.Warnf("program:"+p.id, "failed to stop: %v", err)
			}
		}(p)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		m.log.Warnf("supervisor", "graceful stop did not finish within %s, force-killing the remaining processes", shutdownTimeout)
		m.KillAll()
		<-done
	}

	m.stop()
	m.wg.Wait()
	m.cleanupState()
	m.log.Infof("supervisor", "all programs stopped")
	return nil
}

// Context returns the manager's root context (cancelled by Shutdown).
func (m *Manager) Context() context.Context { return m.ctx }

func (m *Manager) cleanupState() {
	entries, err := os.ReadDir(m.stateDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".json" {
			_ = os.Remove(filepath.Join(m.stateDir, e.Name()))
		}
	}
}

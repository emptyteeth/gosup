package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gosup/internal/config"
	"gosup/internal/logx"
	"gosup/internal/output"
)

// Control commands for a single program.
type commandKind int

const (
	cmdStart commandKind = iota
	cmdStop
	cmdRestart
	cmdShutdown
	cmdUpdate
	cmdRemove
)

type command struct {
	kind   commandKind
	cfg    *config.Program
	action updateAction
	reply  chan error
}

// updateAction is the action to perform when applying a new config.
type updateAction int

const (
	actNone updateAction = iota
	actRestart
	actStart
	actStop
)

// waitResult is a child exit event.
//
// gen is the sequence number of the start this event belongs to: even if an old
// event's pid happens to be reused, it can still be identified as stale.
type waitResult struct {
	gen uint64
	pid int
	err error
	ps  *os.ProcessState
}

// Program is the controller for a managed child process.
//
// All mutable state is accessed only by the supervisor goroutine (run); Status
// reads a published snapshot to avoid data races. External methods interact only
// through the command channel.
type Program struct {
	id      string
	cfg     *config.Program
	manager *Manager
	log     *logx.Logger

	stdout *output.Ring
	stderr *output.Ring

	cmdCh  chan command
	done   chan struct{}
	waitCh chan waitResult

	// currentStopTimeout is the currently configured stop_timeout (nanoseconds),
	// used by callers to bound "wait for the operation to finish". Caller goroutines
	// read it, so it is an atomic value.
	currentStopTimeout atomic.Int64

	// The following fields are accessed only by the supervisor goroutine.
	//
	// stop holds the stop parameters in effect when this instance was started.
	// Stopping "the process started with the current config" must use the config
	// that started it; otherwise reload --restart would stop an old process with
	// the new config: increasing stop_timeout would have no effect (the old process
	// still times out with the old value), and changing it to SIGKILL by mistake
	// would kill it outright. Only these two items are snapshotted — they are the
	// entire config the stop decision reads.
	stop stopParams

	pending       bool // the running process still uses the old config
	state         State
	want          bool // the user wants this program running
	restartOnStop bool // the current stop is part of a restart
	cmd           *exec.Cmd
	pid           int
	pgid          int
	startedAt     time.Time
	stoppedAt     time.Time
	nextRestartAt time.Time
	restarts      int
	failures      int
	gen           uint64 // incremented on every spawn, used to identify stale exit events
	exitCode      *int
	exitSignal    string
	lastError     string

	// published is the snapshot used by Status().
	mu  sync.Mutex
	pub published
}

// stopParams is the stop configuration in effect when the child was started.
type stopParams struct {
	signal  syscall.Signal
	timeout time.Duration
}

type published struct {
	enabled       bool
	pending       bool
	state         State
	running       bool
	pid           int
	pgid          int
	startedAt     time.Time
	stoppedAt     time.Time
	nextRestartAt time.Time
	restarts      int
	exitCode      *int
	exitSignal    string
	lastError     string
}

// Start requests a start.
func (p *Program) Start() error { return p.send(command{kind: cmdStart}) }

// Stop requests a stop.
func (p *Program) Stop() error { return p.send(command{kind: cmdStop}) }

// Restart requests a restart.
func (p *Program) Restart() error { return p.send(command{kind: cmdRestart}) }

// waitTimeout is the upper bound for a caller waiting for an operation to finish;
// it follows the current config (not the process snapshot).
func (p *Program) waitTimeout() time.Duration {
	return time.Duration(p.currentStopTimeout.Load()) + 15*time.Second
}

func (p *Program) send(c command) error {
	c.reply = make(chan error, 1)
	select {
	case p.cmdCh <- c:
	case <-p.done:
		return errors.New("supervisor is closed")
	}
	select {
	case err := <-c.reply:
		return err
	case <-p.done:
		return errors.New("supervisor is closed")
	case <-time.After(p.waitTimeout()):
		return errors.New("operation timed out")
	}
}

// update applies a new config and adjusts the run state according to action.
func (p *Program) update(cfg *config.Program, action updateAction) error {
	return p.send(command{kind: cmdUpdate, cfg: cfg, action: action})
}

// remove stops the program and makes its supervisor goroutine exit (the program
// was deleted from the config).
func (p *Program) remove() error {
	reply := make(chan error, 1)
	select {
	case p.cmdCh <- command{kind: cmdRemove, reply: reply}:
	case <-p.done:
		return nil
	}
	select {
	case err := <-reply:
		return err
	case <-time.After(p.waitTimeout()):
		return errors.New("timed out waiting for stop")
	}
}

// shutdown requests a graceful stop and waits for it to complete.
//
// timeout is the overall budget (from [daemon] shutdown_timeout), covering the
// worst path of "stop_timeout + wait after SIGKILL"; Manager also has an overall
// timeout as a backstop.
func (p *Program) shutdown(timeout time.Duration) error {
	reply := make(chan error, 1)
	select {
	case p.cmdCh <- command{kind: cmdShutdown, reply: reply}:
	case <-p.done:
		return nil
	}
	select {
	case err := <-reply:
		return err
	case <-p.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("graceful stop exceeded %s", timeout)
	}
}

// Status returns a thread-safe snapshot of the state.
func (p *Program) Status() Status {
	p.mu.Lock()
	pub := p.pub
	p.mu.Unlock()

	st := Status{
		ID:            p.id,
		Enabled:       pub.enabled,
		PendingReload: pub.pending,
		State:         pub.state,
		Running:       pub.running,
		PID:           pub.pid,
		Pgid:          pub.pgid,
		Restarts:      pub.restarts,
		ExitCode:      pub.exitCode,
		Signal:        pub.exitSignal,
		LastError:     pub.lastError,
		Command:       p.cfg.Command,
		Argv:          p.cfg.Argv,
		WorkDir:       p.cfg.WorkDir,
		User:          p.cfg.User,
		RestartPolicy: p.cfg.Restart,
	}
	if p.cfg.User == "" && p.cfg.SetCred {
		st.User = fmt.Sprintf("%d:%d", p.cfg.UID, p.cfg.GID)
	}
	for _, c := range p.cfg.Caps {
		st.Capabilities = append(st.Capabilities, config.CapName(c))
	}
	if !pub.startedAt.IsZero() {
		t := pub.startedAt
		st.StartedAt = &t
	}
	if !pub.stoppedAt.IsZero() {
		t := pub.stoppedAt
		st.StoppedAt = &t
	}
	if !pub.nextRestartAt.IsZero() {
		t := pub.nextRestartAt
		st.NextRestartAt = &t
	}
	return st
}

// publish syncs the supervisor goroutine's state into the snapshot.
func (p *Program) publish() {
	p.mu.Lock()
	p.pub = published{
		enabled:       p.cfg.Enabled,
		pending:       p.pending,
		state:         p.state,
		running:       p.pid > 0,
		pid:           p.pid,
		pgid:          p.pgid,
		startedAt:     p.startedAt,
		stoppedAt:     p.stoppedAt,
		nextRestartAt: p.nextRestartAt,
		restarts:      p.restarts,
		exitCode:      p.exitCode,
		exitSignal:    p.exitSignal,
		lastError:     p.lastError,
	}
	p.mu.Unlock()
}

// run is the program's supervision loop.
//
// Backoff restarts are triggered by a timer set precisely to nextRestartAt, not by
// fixed-interval polling: restart_delay may be under 1 second, and checking with a
// 500ms ticker would pin the actual interval to the ticker period.
func (p *Program) run() {
	defer close(p.done)

	// restartTimer is reset to nextRestartAt when a backoff restart is needed;
	// nil means no restart is pending.
	var restartTimer *time.Timer
	var restartC <-chan time.Time
	arm := func() {
		if p.nextRestartAt.IsZero() || !p.want || p.state != StateBackoff {
			return
		}
		d := time.Until(p.nextRestartAt)
		if d < 0 {
			d = 0
		}
		if restartTimer == nil {
			restartTimer = time.NewTimer(d)
			restartC = restartTimer.C
			return
		}
		if !restartTimer.Stop() {
			select {
			case <-restartTimer.C:
			default:
			}
		}
		restartTimer.Reset(d)
		restartC = restartTimer.C
	}
	defer func() {
		if restartTimer != nil {
			restartTimer.Stop()
		}
	}()

	for {
		select {
		case <-p.manager.ctx.Done():
			p.killGroupNow()
			return
		case r := <-p.waitCh:
			p.handleExit(r)
			arm()
		case c := <-p.cmdCh:
			if p.handleCommand(c) {
				return
			}
			arm()
		case <-restartC:
			restartC = nil
			if p.want && p.state == StateBackoff && !p.nextRestartAt.IsZero() && !time.Now().Before(p.nextRestartAt) {
				if err := p.spawn(); err != nil {
					p.log.Errorf("program:"+p.id, "restart failed: %v", err)
				}
			}
			// A failed spawn sets nextRestartAt again, so arm once more.
			arm()
		}
	}
}

func (p *Program) handleCommand(c command) (exit bool) {
	switch c.kind {
	case cmdStart:
		c.reply <- p.doStart()
	case cmdStop:
		c.reply <- p.doStop()
	case cmdRestart:
		c.reply <- p.doRestart()
	case cmdUpdate:
		c.reply <- p.doUpdate(c.cfg, c.action)
	case cmdRemove:
		p.want = false
		p.restartOnStop = false
		stopErr := p.stopAndWait()
		if stopErr != nil {
			p.lastError = stopErr.Error()
		}
		p.reap()
		p.setState(StateStopped)
		p.cleanupStateFile()
		c.reply <- stopErr
		return true
	case cmdShutdown:
		// Uses the same graceful path as a manual stop: send stop_signal first, wait
		// stop_timeout, and only SIGKILL on timeout. Previously this called
		// killGroupNow() directly, which made gosup skip every program's cleanup
		// logic when it exited.
		p.want = false
		p.restartOnStop = false
		err := p.stopCurrent()
		p.reap()
		p.setState(StateStopped)
		c.reply <- err
		return true
	}
	return false
}

// stopAndWait stops the current process and waits for it to exit, returning an
// error (only used for remove/shutdown).
func (p *Program) stopAndWait() error {
	if p.cmd == nil {
		return nil
	}
	p.setState(StateStopping)
	p.publish()
	if err := killGroup(p.pgid, p.stop.signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		p.log.Warnf("program:"+p.id, "failed to send signal: %v", err)
	}
	if r, ok := p.waitExit(p.stop.timeout); ok {
		p.applyExit(r)
		return nil
	}
	_ = killGroup(p.pgid, syscall.SIGKILL)
	if r, ok := p.waitExit(5 * time.Second); ok {
		p.applyExit(r)
		return nil
	}
	p.cmd = nil
	p.pid = 0
	p.pgid = 0
	return errors.New("process did not exit after SIGKILL")
}

// doUpdate applies a new config.
//
// If the program is running and the new config contains spawn-only fields
// (command, user, capabilities, etc.), actNone does not interrupt the current
// process and only marks it pending; the next start uses the new config.
func (p *Program) doUpdate(cfg *config.Program, action updateAction) error {
	if cfg == nil {
		return errors.New("new config is missing")
	}
	running := p.cmd != nil || p.state == StateBackoff
	spawnFieldsChanged := running && spawnFieldsDiffer(p.cfg, cfg)

	// Replace the config only; a running process keeps the stop parameters from
	// when it was started (p.stop).
	p.cfg = cfg
	p.currentStopTimeout.Store(int64(cfg.StopTimeout))
	p.pending = spawnFieldsChanged && action == actNone
	p.publish()

	if !running {
		p.pending = false
		p.publish()
		if action == actStart {
			return p.doStartLocked()
		}
		return nil
	}
	switch action {
	case actStart:
		return ErrAlreadyRunning
	case actStop:
		return p.doStopLocked()
	case actRestart:
		return p.doRestartLocked()
	}
	return nil
}

// doStartLocked is the internal version of doStart (runs on the same goroutine as doUpdate).
func (p *Program) doStartLocked() error {
	p.want = true
	p.failures = 0
	p.nextRestartAt = time.Time{}
	return p.spawn()
}

// doStopLocked is the internal version of doStop.
func (p *Program) doStopLocked() error {
	p.want = false
	p.restartOnStop = false
	p.nextRestartAt = time.Time{}
	if p.state != StateRunning && p.state != StateStarting {
		p.setState(StateStopped)
		p.publish()
		return nil
	}
	p.setState(StateStopping)
	return p.stopCurrent()
}

// doRestartLocked is the internal version of doRestart.
func (p *Program) doRestartLocked() error {
	p.want = true
	p.failures = 0
	p.nextRestartAt = time.Time{}
	if p.state == StateRunning || p.state == StateStarting {
		p.restartOnStop = true
		p.setState(StateStopping)
		return p.stopCurrent()
	}
	return p.spawn()
}

func (p *Program) doStart() error {
	switch p.state {
	case StateRunning, StateStarting:
		return ErrAlreadyRunning
	case StateStopping:
		return ErrStopping
	}
	p.want = true
	p.failures = 0
	p.nextRestartAt = time.Time{}
	return p.spawn()
}

func (p *Program) doStop() error {
	p.want = false
	p.restartOnStop = false
	p.nextRestartAt = time.Time{}
	switch p.state {
	case StateRunning, StateStarting:
		p.setState(StateStopping)
		return p.stopCurrent()
	case StateStopping:
		return ErrStopping
	default:
		p.setState(StateStopped)
		p.publish()
		return nil
	}
}

func (p *Program) doRestart() error {
	p.want = true
	p.failures = 0
	p.nextRestartAt = time.Time{}
	switch p.state {
	case StateRunning, StateStarting:
		p.restartOnStop = true
		p.setState(StateStopping)
		return p.stopCurrent()
	case StateStopping:
		return ErrStopping
	default:
		return p.spawn()
	}
}

// stopCurrent sends the stop signal to the process group and waits for exit, SIGKILLing on timeout.
func (p *Program) stopCurrent() error {
	if p.cmd == nil {
		p.setState(StateStopped)
		p.publish()
		return p.afterStop()
	}
	name := "program:" + p.id
	p.log.Infof(name, "process group %d: sending %s", p.pgid, SignalName(p.stop.signal))
	if err := killGroup(p.pgid, p.stop.signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		p.log.Warnf(name, "failed to send signal: %v", err)
	}

	if r, ok := p.waitExit(p.stop.timeout); ok {
		p.applyExit(r)
		return p.afterStop()
	}

	p.log.Warnf(name, "stop timed out (%s), sending SIGKILL", p.stop.timeout)
	if err := killGroup(p.pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		p.log.Warnf(name, "failed to send SIGKILL: %v", err)
	}
	if r, ok := p.waitExit(5 * time.Second); ok {
		p.applyExit(r)
	} else {
		p.lastError = "process did not exit after SIGKILL"
		p.log.Errorf(name, "%s", p.lastError)
		// Give up on this process: clear the references so we never get stuck in stopping.
		p.cmd = nil
		p.pid = 0
		p.pgid = 0
	}
	return p.afterStop()
}

// waitExit waits up to timeout for a child exit event.
func (p *Program) waitExit(timeout time.Duration) (waitResult, bool) {
	select {
	case r := <-p.waitCh:
		return r, true
	case <-time.After(timeout):
		return waitResult{}, false
	}
}

func (p *Program) afterStop() error {
	restart := p.restartOnStop
	p.restartOnStop = false
	if restart {
		return p.spawn()
	}
	p.want = false
	p.setState(StateStopped)
	p.publish()
	return nil
}

// killGroupNow immediately SIGKILLs the current process group.
func (p *Program) killGroupNow() {
	if p.pid <= 0 {
		return
	}
	_ = killGroup(p.pgid, syscall.SIGKILL)
}

// reap reaps an exited child without blocking, avoiding leftover zombies.
func (p *Program) reap() {
	if p.cmd == nil {
		return
	}
	select {
	case r := <-p.waitCh:
		p.applyExit(r)
	case <-time.After(2 * time.Second):
	}
}

// handleExit handles a child exiting on its own (crash or self-termination).
func (p *Program) handleExit(r waitResult) {
	if p.cmd == nil || (r.gen != 0 && r.gen != p.gen) {
		return // stale event
	}
	p.applyExit(r)

	if !p.want {
		p.setState(StateStopped)
		p.publish()
		return
	}
	// The process group may still contain children (e.g. the main process exited but forked a daemon).
	killGroup(p.pgid, syscall.SIGKILL)

	if p.restartOnStop {
		// Restart explicitly requested by the user: restart unconditionally.
		p.restartOnStop = false
		p.failures = 0
		p.setState(StateBackoff)
		p.nextRestartAt = time.Now()
		p.publish()
		return
	}

	ok := p.exitCode != nil && *p.exitCode == 0
	restart := true
	switch p.cfg.Restart {
	case config.RestartNever:
		restart = false
	case config.RestartOnFailure:
		restart = !ok
	}
	if !restart {
		p.want = false
		p.setState(StateStopped)
		p.publish()
		return
	}

	p.failures++
	if p.cfg.RestartNum > 0 && p.failures > p.cfg.RestartNum {
		p.log.Errorf("program:"+p.id, "failed %d times in a row, reached restart_max, giving up on restarting", p.failures)
		p.want = false
		p.setState(StateStopped)
		p.publish()
		return
	}
	p.pending = false
	delay := backoff(p.cfg.RestartGap, p.cfg.RestartMax, p.failures)
	p.nextRestartAt = time.Now().Add(delay)
	p.setState(StateBackoff)
	p.publish()
	p.log.Infof("program:"+p.id, "restart in %s: attempt %d", delay, p.failures)
}

func backoff(base, max time.Duration, failures int) time.Duration {
	d := base
	for i := 1; i < failures; i++ {
		if d >= max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

// applyExit updates state fields from an exit event.
func (p *Program) applyExit(r waitResult) {
	pid := p.pid
	p.cmd = nil
	p.pid = 0
	p.stoppedAt = time.Now()
	p.nextRestartAt = time.Time{}

	var code int
	var sig syscall.Signal
	if r.ps != nil {
		if ws, ok := r.ps.Sys().(syscall.WaitStatus); ok {
			if ws.Exited() {
				code = ws.ExitStatus()
				p.exitCode = &code
				p.exitSignal = ""
			} else if ws.Signaled() {
				sig = ws.Signal()
				p.exitSignal = SignalName(sig)
				p.exitCode = nil
			}
		}
		_ = r.ps
	}
	p.cleanupStateFile()

	name := "program:" + p.id
	var exitErr *exec.ExitError
	switch {
	case r.err != nil && !errors.As(r.err, &exitErr):
		// A real wait error (e.g. the child was reaped by someone else).
		p.lastError = r.err.Error()
		p.log.Warnf(name, "failed to wait for process %d: %v", pid, r.err)
	case p.exitSignal != "":
		p.log.Infof(name, "process %d terminated by signal %s", pid, p.exitSignal)
	case p.exitCode != nil:
		p.log.Infof(name, "process %d exited with exit code %d", pid, *p.exitCode)
	case r.err != nil:
		p.log.Infof(name, "process %d exited: %v", pid, r.err)
	}

	// Reset the failure count if the process ran long enough.
	if !p.startedAt.IsZero() && time.Since(p.startedAt) >= p.cfg.StableFor {
		p.failures = 0
	}
}

// onSpawnFailed handles a failure to start.
//
// A failed start (command missing, working directory deleted, output file not
// openable, etc.) should be retried just like a crash while running: otherwise
// cases such as mounts not ready yet at boot or a one-off config typo would
// require manual intervention. With a restart policy and the user still wanting
// the program running, it enters backoff; otherwise it stays stopped.
func (p *Program) onSpawnFailed(err error) {
	p.setState(StateStopped)
	if !p.want {
		p.publish()
		return
	}
	restart := p.cfg.Restart != config.RestartNever
	if p.restartOnStop {
		// Restart explicitly requested by the user: retry regardless of policy.
		p.restartOnStop = false
		restart = true
		p.failures = 0
	}
	if !restart {
		p.want = false
		p.publish()
		return
	}
	p.failures++
	if p.cfg.RestartNum > 0 && p.failures > p.cfg.RestartNum {
		p.log.Errorf("program:"+p.id, "failed %d times in a row, reached restart_max, giving up on restarting", p.failures)
		p.want = false
		p.publish()
		return
	}
	delay := backoff(p.cfg.RestartGap, p.cfg.RestartMax, p.failures)
	p.nextRestartAt = time.Now().Add(delay)
	p.setState(StateBackoff)
	p.publish()
	p.log.Infof("program:"+p.id, "failed to start, retrying in %s (attempt %d)", delay, p.failures)
	_ = err
}

// spawn starts the child process.
func (p *Program) spawn() error {
	if p.cmd != nil {
		return ErrAlreadyRunning
	}
	p.setState(StateStarting)
	p.publish()

	name := "program:" + p.id
	stdio, cleanup, err := p.openStdio()
	if err != nil {
		p.lastError = err.Error()
		p.log.Errorf(name, "failed to prepare stdio: %v", err)
		p.onSpawnFailed(err)
		return err
	}

	p.pending = false
	cmd := exec.Command(p.cfg.Argv[0], p.cfg.Argv[1:]...)
	cmd.Dir = p.cfg.WorkDir
	cmd.Stdin = stdio.stdin
	cmd.Stdout = stdio.stdout
	cmd.Stderr = stdio.stderr
	cmd.Env = p.buildEnv()

	attr := &syscall.SysProcAttr{
		Setsid: true, // new session and process group: detach from the controlling terminal for whole-group management
	}
	if p.cfg.SetCred {
		attr.Credential = &syscall.Credential{
			Uid:         uint32(p.cfg.UID),
			Gid:         uint32(p.cfg.GID),
			Groups:      p.cfg.SuppGIDs,
			NoSetGroups: p.cfg.NoSetGrp,
		}
	}
	if len(p.cfg.Caps) > 0 {
		// Before exec, Go's child process sets inheritable/permitted and raises ambient,
		// so programs started by a non-root user can hold these capabilities too.
		attr.AmbientCaps = p.cfg.Caps
	}
	cmd.SysProcAttr = attr

	if err := cmd.Start(); err != nil {
		cleanup()
		p.lastError = err.Error()
		p.log.Errorf(name, "failed to start: %v", err)
		p.onSpawnFailed(err)
		return err
	}
	// The child has inherited the fds: the parent must close the write ends and stdin,
	// otherwise the reader goroutines never see EOF, and every restart leaks descriptors.
	cleanup()

	pid := cmd.Process.Pid
	p.cmd = cmd
	p.pid = pid
	p.pgid = pid // after Setsid the session/process group id equals the child pid
	p.gen++
	gen := p.gen
	p.stop = stopParams{signal: p.cfg.StopSignal, timeout: p.cfg.StopTimeout}
	p.startedAt = time.Now()
	p.stoppedAt = time.Time{}
	p.nextRestartAt = time.Time{}
	p.exitCode = nil
	p.exitSignal = ""
	p.lastError = ""
	p.restarts++
	p.pending = false
	p.setState(StateRunning)
	p.publish()
	p.writeStateFile()

	p.log.Infof(name, "started pid=%d uid=%d gid=%d argv=%q", pid, p.cfg.UID, p.cfg.GID, p.cfg.Argv)

	go func(gen uint64) {
		err := cmd.Wait()
		var ps *os.ProcessState
		if cmd.ProcessState != nil {
			ps = cmd.ProcessState
		}
		select {
		case p.waitCh <- waitResult{gen: gen, pid: pid, err: err, ps: ps}:
		case <-p.done:
		}
	}(gen)
	return nil
}

func (p *Program) setState(s State) {
	p.state = s
}

type stdioFiles struct {
	stdin  *os.File
	stdout *os.File
	stderr *os.File
}

// openStdio prepares the child's standard input and output.
//
// stdout/stderr go through pipes to background goroutines that read them into ring
// buffers (and optional log files), so that even if grandchildren forked by the child
// inherit the pipes, Wait is not blocked. The returned cleanup is called after the
// child is started: it closes the parent's stdin and pipe write ends so the reader
// goroutines see EOF once the child exits; log files are closed by the readers themselves.
func (p *Program) openStdio() (stdio stdioFiles, cleanup func(), err error) {
	var once sync.Once
	var closers []io.Closer
	cleanup = func() {
		once.Do(func() {
			for _, c := range closers {
				_ = c.Close()
			}
		})
	}
	fail := func(e error) (stdioFiles, func(), error) {
		cleanup()
		return stdioFiles{}, func() {}, e
	}

	// stdin is held by the child; the parent closes it after Start.
	switch p.cfg.Stdin {
	case "", "null", "/dev/null":
		f, e := os.Open(os.DevNull)
		if e != nil {
			return fail(fmt.Errorf("failed to open /dev/null: %w", e))
		}
		stdio.stdin = f
		closers = append(closers, f)
	default:
		f, e := os.Open(p.cfg.Stdin)
		if e != nil {
			return fail(fmt.Errorf("failed to open stdin %s: %w", p.cfg.Stdin, e))
		}
		stdio.stdin = f
		closers = append(closers, f)
	}

	p.stdout.Reset()
	p.stderr.Reset()

	out, e := p.pipeFor(p.cfg.Stdout, p.stdout, "stdout", &closers)
	if e != nil {
		return fail(e)
	}
	stdio.stdout = out
	errOut, e := p.pipeFor(p.cfg.Stderr, p.stderr, "stderr", &closers)
	if e != nil {
		return fail(e)
	}
	stdio.stderr = errOut

	return stdio, cleanup, nil
}

// pipeFor creates a pipe or /dev/null for an output destination and starts the reader goroutine.
// The write end is added to closers (closed by the parent after Start); the reader
// goroutine closes its own files when done.
func (p *Program) pipeFor(dest string, ring *output.Ring, label string, closers *[]io.Closer) (*os.File, error) {
	if dest == "discard" {
		f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to open /dev/null: %w", err)
		}
		*closers = append(*closers, f)
		return f, nil
	}

	var sink io.Writer = ring
	var file *os.File
	if dest != "" && dest != "ring" {
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("failed to open %s output file %s: %w", label, dest, err)
		}
		file = f
		// Write to the file and the ring buffer at the same time, so the tail can also be
		// viewed through the API/SSE while the file receives the full history.
		sink = io.MultiWriter(f, ring)
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		if file != nil {
			file.Close()
		}
		return nil, fmt.Errorf("failed to create %s pipe: %w", label, err)
	}
	*closers = append(*closers, pw)
	go func() {
		defer pr.Close()
		if file != nil {
			defer file.Close()
		}
		buf := make([]byte, 32*1024)
		for {
			n, rerr := pr.Read(buf)
			if n > 0 {
				_, _ = sink.Write(buf[:n])
			}
			if rerr != nil {
				return
			}
		}
	}()
	return pw, nil
}

func (p *Program) buildEnv() []string {
	base := os.Environ()
	if len(p.cfg.Env) == 0 {
		return base
	}
	env := make(map[string]string, len(base)+len(p.cfg.Env))
	order := make([]string, 0, len(base)+len(p.cfg.Env))
	set := func(kv string) {
		i := indexByte(kv, '=')
		if i < 0 {
			return
		}
		k := kv[:i]
		if _, ok := env[k]; !ok {
			order = append(order, k)
		}
		env[k] = kv[i+1:]
	}
	for _, kv := range base {
		set(kv)
	}
	for _, kv := range p.cfg.Env {
		set(kv)
	}
	out := make([]string, 0, len(order))
	for _, k := range order {
		out = append(out, k+"="+env[k])
	}
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// State file: records pid and start time to help operators troubleshoot.
type stateFile struct {
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	Command   []string  `json:"command"`
}

func (p *Program) statePath() string {
	return filepath.Join(p.manager.stateDir, p.id+".json")
}

func (p *Program) writeStateFile() {
	sf := stateFile{PID: p.pid, StartedAt: p.startedAt, Command: p.cfg.Argv}
	b, err := json.Marshal(sf)
	if err != nil {
		return
	}
	_ = os.WriteFile(p.statePath(), b, 0o644)
}

func (p *Program) cleanupStateFile() {
	if p.id == "" {
		return
	}
	_ = os.Remove(p.statePath())
}

// forceKill is called by Shutdown's backstop path: send SIGKILL straight to the process group.
// This is best-effort only; state is still maintained by the supervisor goroutine itself.
func (p *Program) forceKill() {
	p.mu.Lock()
	pgid := p.pub.pgid
	p.mu.Unlock()
	_ = killGroup(pgid, syscall.SIGKILL)
}

// spawnFieldsDiffer reports whether spawn-only fields differ between two configs.
// Output destinations, environment variables, restart policy and the like take effect
// directly at runtime, so they are not compared.
func spawnFieldsDiffer(old, new *config.Program) bool {
	return old.Command != new.Command ||
		old.Shell != new.Shell ||
		!reflect.DeepEqual(old.Argv, new.Argv) ||
		old.WorkDir != new.WorkDir ||
		old.User != new.User ||
		old.Group != new.Group ||
		!reflect.DeepEqual(old.Groups, new.Groups) ||
		old.SetCred != new.SetCred ||
		old.UID != new.UID ||
		old.GID != new.GID ||
		!reflect.DeepEqual(old.SuppGIDs, new.SuppGIDs) ||
		old.NoSetGrp != new.NoSetGrp ||
		!reflect.DeepEqual(old.Caps, new.Caps) ||
		!reflect.DeepEqual(old.Env, new.Env) ||
		old.Stdout != new.Stdout ||
		old.Stderr != new.Stderr ||
		old.Stdin != new.Stdin
}

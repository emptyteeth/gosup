package supervisor

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"gosup/internal/config"
	"gosup/internal/logx"
)

// newTestManager builds a Manager from minimal Program configs.
func newTestManager(t *testing.T, programs ...*config.Program) *Manager {
	t.Helper()
	cfg := &config.Config{
		Path: "test",
		Daemon: config.Daemon{
			StateDir:        t.TempDir(),
			OutputBuffer:    64 * 1024,
			StopTimeout:     5 * time.Second,
			ShutdownTimeout: 10 * time.Second,
		},
		Programs: programs,
	}
	log := logx.New(os.Stderr, logx.Error)
	m, err := New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func prog(id, command string) *config.Program {
	return &config.Program{
		ID:          id,
		Command:     command,
		Argv:        []string{"/bin/sh", "-c", command},
		Enabled:     true,
		Restart:     config.RestartNever,
		RestartGap:  time.Second,
		RestartMax:  5 * time.Second,
		StableFor:   time.Minute,
		StopSignal:  syscall.SIGTERM,
		StopTimeout: 2 * time.Second,
		Stdout:      "ring",
		Stderr:      "ring",
	}
}

func TestStartStopAndOutput(t *testing.T) {
	m := newTestManager(t, prog("echoer", "echo hello; echo oops >&2; sleep 30"))
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	// Starting is asynchronous: wait until it reaches running.
	waitFor(t, func() bool {
		st, err := m.Status("echoer")
		return err == nil && st.Running
	})

	st, err := m.Status("echoer")
	if err != nil {
		t.Fatal(err)
	}
	if st.PID <= 0 {
		t.Fatalf("pid = %d", st.PID)
	}
	if st.StartedAt == nil {
		t.Error("started_at should be set while running")
	} else if time.Since(*st.StartedAt) <= 0 {
		t.Error("started_at should be in the past")
	}

	// Output may arrive slightly later.
	waitFor(t, func() bool {
		out, err := m.Output("echoer", "stdout", 0)
		return err == nil && strings.Contains(string(out.Stdout), "hello")
	})
	out, err := m.Output("echoer", "stderr", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out.Stderr), "oops") {
		t.Errorf("stderr = %q", out.Stderr)
	}

	if err := m.StopProgram("echoer"); err != nil {
		t.Fatal(err)
	}
	st, _ = m.Status("echoer")
	if st.Running || st.State != StateStopped {
		t.Errorf("state after stop = %s running=%v", st.State, st.Running)
	}
}

func TestOnlyOneInstance(t *testing.T) {
	m := newTestManager(t, prog("single", "sleep 30"))
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("single")
		return st.Running
	})

	if err := m.StartProgram("single"); err != ErrAlreadyRunning {
		t.Fatalf("duplicate start should return ErrAlreadyRunning, got %v", err)
	}
}

func TestRestartPolicyOnFailure(t *testing.T) {
	p := prog("flaky", "exit 1")
	p.Restart = config.RestartOnFailure
	p.RestartGap = 50 * time.Millisecond
	p.RestartNum = 2
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	// First start + 2 restarts, then give up.
	waitFor(t, func() bool {
		st, _ := m.Status("flaky")
		return st.State == StateStopped && st.Restarts >= 3
	})
	st, _ := m.Status("flaky")
	if st.Restarts != 3 {
		t.Errorf("restarts = %d, want 3", st.Restarts)
	}
	if st.ExitCode == nil || *st.ExitCode != 1 {
		t.Errorf("exit_code = %v", st.ExitCode)
	}
}

func TestRestartAlwaysWithZeroExit(t *testing.T) {
	p := prog("once", "true")
	p.Restart = config.RestartAlways
	p.RestartGap = 50 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	waitFor(t, func() bool {
		st, _ := m.Status("once")
		return st.Restarts >= 2
	})
}

func TestUnknownProgram(t *testing.T) {
	m := newTestManager(t)
	if _, err := m.Status("nope"); err == nil {
		t.Error("an unknown id should error")
	}
	if err := m.StartProgram("nope"); err == nil {
		t.Error("starting an unknown id should error")
	}
}

func TestDisabledNotStarted(t *testing.T) {
	p := prog("off", "sleep 30")
	p.Enabled = false
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	time.Sleep(200 * time.Millisecond)
	st, _ := m.Status("off")
	if st.Running {
		t.Error("a disabled program should not start automatically")
	}
	// A manual start is still possible.
	if err := m.StartProgram("off"); err != nil {
		t.Fatalf("manual start failed: %v", err)
	}
	waitFor(t, func() bool {
		st, _ := m.Status("off")
		return st.Running
	})
}

func TestRestartCommand(t *testing.T) {
	m := newTestManager(t, prog("svc", "sleep 30"))
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.Running
	})
	before, _ := m.Status("svc")

	if err := m.RestartProgram("svc"); err != nil {
		t.Fatal(err)
	}
	after, _ := m.Status("svc")
	if !after.Running {
		t.Fatal("should be running after restart")
	}
	if after.PID == before.PID {
		t.Error("pid should change after restart")
	}
}

func TestStateFileCleanup(t *testing.T) {
	p := prog("file", "sleep 30")
	m := newTestManager(t, p)
	m.Start()
	waitFor(t, func() bool {
		st, _ := m.Status("file")
		return st.Running
	})
	if _, err := os.Stat(filepath.Join(m.stateDir, "file.json")); err != nil {
		t.Errorf("state file should be written while running: %v", err)
	}
	_ = m.Shutdown()
	if _, err := os.Stat(filepath.Join(m.stateDir, "file.json")); !os.IsNotExist(err) {
		t.Errorf("state file should be removed after exit: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the condition")
}

func TestStopKillsWholeProcessGroup(t *testing.T) {
	// The main process forks a background child; stopping must clean up the child too.
	childPidFile := filepath.Join(t.TempDir(), "child.pid")
	p := prog("tree", "sleep 300 & echo $! > "+childPidFile+"; sleep 300")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("tree")
		return st.Running
	})
	time.Sleep(200 * time.Millisecond)

	if err := m.StopProgram("tree"); err != nil {
		t.Fatal(err)
	}
	// Leftover processes in the process group should be SIGKILLed.
	waitFor(t, func() bool {
		out, err := os.ReadFile(childPidFile)
		if err != nil {
			return false
		}
		pid := strings.TrimSpace(string(out))
		if pid == "" {
			return false
		}
		// The process has exited if /proc/<pid> does not exist.
		_, statErr := os.Stat("/proc/" + pid)
		return os.IsNotExist(statErr)
	})
}

func TestShutdownStopsEverything(t *testing.T) {
	m := newTestManager(t,
		prog("a", "sleep 30"),
		prog("b", "sleep 30"),
	)
	m.Start()
	waitFor(t, func() bool {
		a, _ := m.Status("a")
		b, _ := m.Status("b")
		return a.Running && b.Running
	})
	pa, _ := m.Status("a")
	pb, _ := m.Status("b")

	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{pa.PID, pb.PID} {
		if _, err := os.Stat("/proc/" + itoaTest(pid)); !os.IsNotExist(err) {
			t.Errorf("pid %d still exists", pid)
		}
	}
}

func itoaTest(n int) string {
	return strconv.Itoa(n)
}

// programFrom builds a reusable config from a command.
func programFrom(id, command string) *config.Program {
	return prog(id, command)
}

func TestReloadAddRemove(t *testing.T) {
	p1 := prog("keep", "sleep 30")
	m := newTestManager(t, p1)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("keep")
		return st.Running
	})
	keepPid, _ := m.Status("keep")

	newCfg := &config.Config{
		Path:     "test",
		Daemon:   m.Config().Daemon,
		Programs: []*config.Program{p1, prog("added", "sleep 30")},
	}
	res, err := m.Reload(newCfg, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Added) != 1 || res.Added[0] != "added" {
		t.Errorf("added = %v", res.Added)
	}
	waitFor(t, func() bool {
		st, _ := m.Status("added")
		return st.Running
	})
	// A program that did not change must not be interrupted.
	if st, _ := m.Status("keep"); st.PID != keepPid.PID {
		t.Error("an unchanged program should not restart")
	}

	// Remove a program.
	res, err = m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{p1}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 1 || res.Removed[0] != "added" {
		t.Errorf("removed = %v", res.Removed)
	}
	if _, err := m.Status("added"); err == nil {
		t.Error("should not be queryable after removal")
	}
	if st, _ := m.Status("keep"); !st.Running {
		t.Error("keep should still be running")
	}
}

func TestReloadPendingAndRestart(t *testing.T) {
	p := prog("svc", "sleep 30")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.Running
	})
	before, _ := m.Status("svc")

	// Change spawn parameters without restarting: the process stays, status is marked pending.
	p2 := *p
	p2.Command = "sleep 31"
	p2.Argv = []string{"/bin/sh", "-c", "sleep 31"}
	res, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&p2}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Updated) != 1 || len(res.PendingRestart) != 1 {
		t.Fatalf("res = %+v", res)
	}
	st, _ := m.Status("svc")
	if !st.Running || st.PID != before.PID {
		t.Error("the process should not change when no restart is requested")
	}
	if !st.PendingReload {
		t.Error("should mark PendingReload")
	}
	if st.Command != "sleep 31" {
		t.Errorf("command = %q", st.Command)
	}

	// A reload with restart should restart immediately.
	p3 := *p
	p3.Command = "sleep 32"
	p3.Argv = []string{"/bin/sh", "-c", "sleep 32"}
	res, err = m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&p3}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Restarted) != 1 {
		t.Fatalf("res = %+v", res)
	}
	after, _ := m.Status("svc")
	if !after.Running || after.PID == before.PID {
		t.Error("should already be restarted")
	}
	if after.PendingReload {
		t.Error("no pending mark should remain after restart")
	}
}

func TestReloadDisabledStopsWithRestart(t *testing.T) {
	p := prog("toggle", "sleep 30")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("toggle")
		return st.Running
	})
	off := *p
	off.Enabled = false
	res, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&off}}, true)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st, _ := m.Status("toggle")
		return !st.Running
	})
	if len(res.Stopped) != 1 {
		t.Errorf("stopped = %v", res.Stopped)
	}
	if st, _ := m.Status("toggle"); st.Enabled {
		t.Error("enabled should be updated to false")
	}
}

func TestReloadNonSpawnFieldsApplyImmediately(t *testing.T) {
	p := prog("svc", "sleep 30")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.Running
	})
	before, _ := m.Status("svc")

	// Change only the restart policy and stop signal: no restart needed, and it
	// should not be marked pending.
	p2 := *p
	p2.Restart = config.RestartNever
	p2.StopSignal = syscall.SIGINT
	res, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&p2}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.PendingRestart) != 0 {
		t.Errorf("pending = %v", res.PendingRestart)
	}
	st, _ := m.Status("svc")
	if st.PID != before.PID || st.PendingReload {
		t.Error("changing only non-spawn fields should not affect the running process")
	}
	if st.RestartPolicy != config.RestartNever {
		t.Errorf("restart_policy = %q", st.RestartPolicy)
	}
}

func TestReloadUnchanged(t *testing.T) {
	p := prog("svc", "sleep 30")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	res, err := m.Reload(m.Config(), false)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Unchanged {
		t.Errorf("res = %+v", res)
	}
}

func TestOutputTailLines(t *testing.T) {
	m := newTestManager(t, prog("lines", "i=0; while [ $i -lt 5 ]; do i=$((i+1)); echo line$i; done; sleep 30"))
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		out, err := m.Output("lines", "stdout", 0)
		return err == nil && strings.Contains(string(out.Stdout), "line5")
	})
	out, err := m.Output("lines", "stdout", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(out.Stdout)); got != "line4\nline5" {
		t.Errorf("stdout = %q", got)
	}
	if !out.StdoutTruncated {
		t.Error("should be marked truncated after truncation")
	}
}

func TestReloadRestartAppliesPending(t *testing.T) {
	p := prog("svc", "sleep 30")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.Running
	})
	before, _ := m.Status("svc")

	p2 := *p
	p2.Command = "sleep 31"
	p2.Argv = []string{"/bin/sh", "-c", "sleep 31"}
	cfg2 := &config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&p2}}
	if _, err := m.Reload(cfg2, false); err != nil {
		t.Fatal(err)
	}
	if st, _ := m.Status("svc"); !st.PendingReload {
		t.Fatal("should be pending")
	}

	// The config file did not change again, but a second reload with restart
	// should make pending take effect.
	res, err := m.Reload(cfg2, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Restarted) != 1 {
		t.Fatalf("res = %+v", res)
	}
	st, _ := m.Status("svc")
	if st.PID == before.PID || st.PendingReload {
		t.Errorf("should already be restarted and no longer pending: %+v", st)
	}
	if !strings.Contains(strings.Join(st.Argv, " "), "sleep 31") {
		t.Errorf("argv = %v", st.Argv)
	}
}

func TestReloadReportsDaemonHotFields(t *testing.T) {
	m := newTestManager(t, prog("svc", "sleep 30"))
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	cfg := *m.Config()
	cfg.Daemon.MaxConnections = 7
	cfg.Daemon.LogLevel = "warn"
	res, err := m.Reload(&cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Unchanged {
		t.Error("should not report unchanged when a daemon hot-reloadable field changes")
	}
	want := map[string]bool{"max_connections": true, "log_level": true}
	for _, name := range res.DaemonUpdated {
		delete(want, name)
	}
	if len(want) != 0 {
		t.Errorf("missing daemon_updated fields: %v (actual %v)", want, res.DaemonUpdated)
	}
	if got := m.DaemonConfig().MaxConnections; got != 7 {
		t.Errorf("max_connections = %v", got)
	}
}

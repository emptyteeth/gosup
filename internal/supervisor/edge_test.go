package supervisor

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"gosup/internal/config"
)

// Probe 1: stdout points to an unwritable path. A failed spawn must stay stopped,
// and the backoff restarts of restart=always must not "resurrect" the failed main
// program into a zombie state.
func TestSpawnFailureWithAlwaysRestart(t *testing.T) {
	p := prog("badout", "sleep 30")
	p.Stdout = "/nonexistent-dir/out.log"
	p.Restart = config.RestartAlways
	p.RestartGap = 10 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	time.Sleep(200 * time.Millisecond)
	st, _ := m.Status("badout")
	if st.Running {
		t.Fatal("shown as running although the output file cannot be opened")
	}
	if st.LastError == "" {
		t.Error("should record last_error")
	}
}

// Probe 2: after a failed spawn, checkBackoff does not retry forever (backoff restarts
// should only happen once the process has run). Here we assert that the state can still
// accept a manual start rather than being stuck.
func TestSpawnFailureRecoversManually(t *testing.T) {
	p := prog("badout", "sleep 30")
	p.Stdout = "/nonexistent-dir/out.log"
	p.Restart = config.RestartAlways
	p.RestartGap = 10 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	time.Sleep(100 * time.Millisecond)

	// After fixing the output path, a manual start should succeed.
	good := *p
	good.Stdout = "ring"
	if _, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&good}}, true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st, _ := m.Status("badout")
		return st.Running
	})
}

// Probe 3: very fast crashes + restart_max; restart count must not exceed the limit.
func TestFastCrashRespectsRestartMax(t *testing.T) {
	p := prog("fast", "exit 1")
	p.Restart = config.RestartAlways
	p.RestartGap = time.Millisecond
	p.RestartMax = time.Millisecond
	p.RestartNum = 5
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("fast")
		return st.State == StateStopped
	})
	time.Sleep(200 * time.Millisecond)
	st, _ := m.Status("fast")
	// First start + 5 restarts = 6 starts.
	if st.Restarts > 6 {
		t.Errorf("restart_max=5 but started %d times", st.Restarts)
	}
	if st.State != StateStopped {
		t.Errorf("state after giving up = %s", st.State)
	}
}

// Probe 4: stop during backoff must cancel the pending restart.
func TestStopCancelsBackoff(t *testing.T) {
	p := prog("flaky", "exit 1")
	p.Restart = config.RestartAlways
	p.RestartGap = time.Hour
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("flaky")
		return st.State == StateBackoff
	})
	if err := m.StopProgram("flaky"); err != nil {
		t.Fatalf("error stopping during backoff: %v", err)
	}
	st, _ := m.Status("flaky")
	if st.NextRestartAt != nil {
		t.Error("next_restart_at still set after stop")
	}
	if st.State != StateStopped {
		t.Errorf("state after stop = %s", st.State)
	}
}

// Probe 5: concurrent stops must not error or leave a stopping state.
func TestConcurrentStop(t *testing.T) {
	p := prog("slow", "trap '' TERM; sleep 30")
	p.StopTimeout = 300 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("slow")
		return st.Running
	})

	errs := make(chan error, 2)
	go func() { errs <- m.StopProgram("slow") }()
	time.Sleep(50 * time.Millisecond)
	go func() { errs <- m.StopProgram("slow") }()
	<-errs
	<-errs
	waitFor(t, func() bool {
		st, _ := m.Status("slow")
		return !st.Running
	})
}

// Probe 6: removing a program from the config while it is in backoff must not bring it back up.
func TestRemoveWhileBackoff(t *testing.T) {
	p := prog("flaky", "exit 1")
	p.Restart = config.RestartAlways
	p.RestartGap = 200 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("flaky")
		return st.State == StateBackoff
	})
	if _, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon}, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := m.Status("flaky"); err == nil {
		t.Error("program still queryable after removal")
	}
}

// Probe 7: reloading a program that is in backoff must not cause an extra restart storm.
func TestReloadWhileBackoff(t *testing.T) {
	p := prog("flaky", "exit 1")
	p.Restart = config.RestartAlways
	p.RestartGap = 300 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("flaky")
		return st.State == StateBackoff
	})
	cfg := &config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{p}}
	before, _ := m.Status("flaky")
	if _, err := m.Reload(cfg, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)
	after, _ := m.Status("flaky")
	if after.Restarts-before.Restarts > 3 {
		t.Errorf("restarted %d times after one reload", after.Restarts-before.Restarts)
	}
}

// Probe 8: Shutdown must be bounded by shutdown_timeout and force termination.
func TestShutdownBounded(t *testing.T) {
	p := prog("stubborn", "trap '' TERM; sleep 300")
	p.StopTimeout = 10 * time.Second
	m := newTestManager(t, p)
	m.Start()
	waitFor(t, func() bool {
		st, _ := m.Status("stubborn")
		return st.Running
	})
	cfg := *m.Config()
	cfg.Daemon.ShutdownTimeout = 500 * time.Millisecond
	if _, err := m.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_ = m.Shutdown()
	if el := time.Since(start); el > 5*time.Second {
		t.Errorf("shutdown took %v, did not force-kill after the timeout", el)
	}
}

// When Shutdown is called without Start, there is no supervisor goroutine to receive
// commands; it must return immediately instead of blocking.
func TestShutdownBeforeStart(t *testing.T) {
	m := newTestManager(t, prog("svc", "sleep 30"))
	done := make(chan struct{})
	go func() { _ = m.Shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown without Start blocks forever")
	}
}

// A failed start (command temporarily missing) should be retried with backoff under
// restart=always, and started automatically once the condition recovers.
func TestSpawnFailureRetries(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "svc.sh")

	p := prog("svc", "sleep 30")
	p.Argv = []string{script}
	p.Restart = config.RestartAlways
	p.RestartGap = 50 * time.Millisecond
	p.RestartMax = 50 * time.Millisecond

	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.State == StateBackoff
	})

	// Simulate the dependency becoming ready (e.g. a mount completing).
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.Running
	})
}

// With restart=never, a failed start should not be retried.
func TestSpawnFailureNoRetryWhenNever(t *testing.T) {
	p := prog("svc", "sleep 30")
	p.Argv = []string{"/nonexistent/gosup-test-binary"}
	p.Restart = config.RestartNever
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.State == StateStopped
	})
	time.Sleep(150 * time.Millisecond)
	st, _ := m.Status("svc")
	if st.State != StateStopped || st.Running {
		t.Errorf("restart=never should not retry: state=%s", st.State)
	}
	if st.LastError == "" {
		t.Error("should record last_error")
	}
}

// A late old exit event (different generation) must not be applied to the current process.
func TestStaleWaitEventIgnored(t *testing.T) {
	m := newTestManager(t, prog("svc", "sleep 30"))
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.Running
	})
	before, _ := m.Status("svc")

	m.progs["svc"].waitCh <- waitResult{gen: 999, pid: before.PID + 999999}
	time.Sleep(300 * time.Millisecond)

	after, _ := m.Status("svc")
	if !after.Running || after.PID != before.PID {
		t.Errorf("stale event affected the current process: running=%v pid=%d (want %d)", after.Running, after.PID, before.PID)
	}
}

// When gosup exits it must stop gracefully: send stop_signal first so programs get a
// chance to clean up. Previously cmdShutdown sent SIGKILL directly, skipping every
// program's cleanup logic.
func TestShutdownIsGraceful(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "cleaned")
	script := filepath.Join(dir, "svc.sh")
	// Write the marker file on TERM and then exit.
	body := "#!/bin/sh\ntrap 'echo done > " + marker + "; exit 0' TERM\nwhile true; do sleep 0.05; done\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	p := prog("svc", "sleep 30")
	p.Argv = []string{script}
	p.Restart = config.RestartNever
	m := newTestManager(t, p)
	m.Start()
	waitFor(t, func() bool {
		st, _ := m.Status("svc")
		return st.Running
	})
	// The state becomes running as soon as cmd.Start() returns, while the shell has not
	// yet reached the trap statement. Wait until it has actually installed the signal
	// handler, or TERM is handled by the default action (immediate termination, no marker).
	time.Sleep(300 * time.Millisecond)

	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("program did not receive SIGTERM and run its cleanup: %v", err)
	}
}

// shutdown_timeout is the overall budget: programs that have not exited by then are force-killed.
func TestShutdownHonoursTimeout(t *testing.T) {
	p := prog("stubborn", "trap '' TERM; sleep 300")
	p.StopTimeout = 10 * time.Second // far larger than shutdown_timeout
	m := newTestManager(t, p)
	m.Start()
	waitFor(t, func() bool {
		st, _ := m.Status("stubborn")
		return st.Running
	})
	// Wait until the shell reaches trap '' TERM, otherwise the program exits immediately
	// on TERM and the forced-termination path of shutdown_timeout is not exercised.
	time.Sleep(300 * time.Millisecond)

	cfg := *m.Config()
	cfg.Daemon.ShutdownTimeout = 500 * time.Millisecond
	if _, err := m.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	t.Logf("shutdown took %v", elapsed)
	if elapsed > 3*time.Second {
		t.Errorf("did not force-kill per shutdown_timeout, took %v", elapsed)
	}
}

// When several programs stop at once they should be awaited in parallel, not one after another.
func TestShutdownStopsInParallel(t *testing.T) {
	var progs []*config.Program
	for _, id := range []string{"a", "b", "c"} {
		p := prog(id, "trap '' TERM; sleep 300")
		p.StopTimeout = 800 * time.Millisecond
		progs = append(progs, p)
	}
	m := newTestManager(t, progs...)
	m.Start()
	for _, id := range []string{"a", "b", "c"} {
		waitFor(t, func() bool {
			st, _ := m.Status(id)
			return st.Running
		})
	}
	// Wait until trap '' TERM takes effect, otherwise the programs exit immediately and
	// parallel waiting is not exercised.
	time.Sleep(300 * time.Millisecond)

	cfg := *m.Config()
	cfg.Daemon.ShutdownTimeout = 5 * time.Second
	if _, err := m.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	t.Logf("3 programs with 800ms stop_timeout: %v", elapsed)
	// Serial takes about 2.4s, parallel about 0.8s.
	if elapsed > 2*time.Second {
		t.Errorf("stopping looks serial, took %v", elapsed)
	}
}

// When reload --restart restarts with a new config, the old process must exit according
// to the stop_timeout it was started with. Previously doUpdate replaced p.cfg first, so
// "increase stop_timeout then reload" did not take effect the first time.
func TestReloadRestartUsesSnapshotTimeout(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "svc.sh")
	marker := filepath.Join(dir, "cleaned")
	// The marker is only fully written 500ms after TERM arrives.
	body := "#!/bin/sh\ntrap 'sleep 0.5; echo done > " + marker + "; exit 0' TERM\nwhile true; do sleep 0.05; done\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	p := prog("svc", "sleep 30")
	p.Argv = []string{script}
	p.StopTimeout = 5 * time.Second // old config: plenty
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("svc"); return st.Running })
	time.Sleep(300 * time.Millisecond) // wait for the shell to install the trap

	// The new config's stop_timeout is only 100ms. The old process should still
	// exit within its 5s snapshot.
	next := *p
	next.StopTimeout = 100 * time.Millisecond
	next.Command = "sleep 31"
	next.Argv = []string{"/bin/sh", "-c", "sleep 31"}
	res, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&next}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Restarted) != 1 {
		t.Fatalf("res = %+v", res)
	}

	if _, err := os.Stat(marker); err != nil {
		t.Error("the old process did not get the full stop_timeout and was killed early by the new config's timeout")
	}
}

// The reverse: with a small stop_timeout in the old config and a large one in the new,
// the old process still times out according to the old value.
func TestReloadRestartKeepsSmallSnapshotTimeout(t *testing.T) {
	p := prog("slow", "trap '' TERM; sleep 300")
	p.StopTimeout = 200 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("slow"); return st.Running })
	time.Sleep(300 * time.Millisecond)

	next := *p
	next.StopTimeout = 10 * time.Second
	next.Command = "sleep 31"
	next.Argv = []string{"/bin/sh", "-c", "sleep 31"}
	start := time.Now()
	if _, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&next}}, true); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	t.Logf("restart took %v (old snapshot 200ms)", elapsed)
	if elapsed > 2*time.Second {
		t.Errorf("the old process did not time out per the old snapshot, took %v", elapsed)
	}
}

// When reload --restart also changes stop_signal, the old process still uses the old signal.
func TestReloadRestartUsesSnapshotSignal(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "svc.sh")
	marker := filepath.Join(dir, "got_term")
	body := "#!/bin/sh\ntrap 'echo yes > " + marker + "; exit 0' TERM\nwhile true; do sleep 0.05; done\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	p := prog("svc", "sleep 30")
	p.Argv = []string{script}
	p.StopSignal = syscall.SIGTERM
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("svc"); return st.Running })
	time.Sleep(300 * time.Millisecond)

	next := *p
	next.StopSignal = syscall.SIGKILL // new config switches to a hard kill
	next.Command = "sleep 31"
	next.Argv = []string{"/bin/sh", "-c", "sleep 31"}
	if _, err := m.Reload(&config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{&next}}, true); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(marker); err != nil {
		t.Error("the old process received the new config's SIGKILL instead of the SIGTERM it started with")
	}
}

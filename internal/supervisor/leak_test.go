package supervisor

import (
	"os"
	"runtime"
	"testing"
	"time"

	"gosup/internal/config"
)

func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd: %v", err)
	}
	return len(entries)
}

func settle(t *testing.T, baseG, baseFD int) (int, int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		g, fd := runtime.NumGoroutine(), openFDs(t)
		if g <= baseG+2 && fd <= baseFD+2 {
			return g, fd
		}
		time.Sleep(100 * time.Millisecond)
	}
	runtime.GC()
	return runtime.NumGoroutine(), openFDs(t)
}

// Repeated program restarts: pipeFor creates 2 pipes + 2 reader goroutines + possibly
// a log file each time.
func TestLeakRestartCycles(t *testing.T) {
	p := prog("svc", "i=0; while true; do i=$((i+1)); echo line$i; echo err$i >&2; sleep 0.02; done")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("svc"); return st.Running })
	time.Sleep(200 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	for i := 0; i < 100; i++ {
		if err := m.RestartProgram("svc"); err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
	}
	g, fd := settle(t, baseG, baseFD)
	t.Logf("100 restarts: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

// Output to files: each restart opens a log file, which must be closed.
func TestLeakFileOutputRestart(t *testing.T) {
	dir := t.TempDir()
	p := prog("svc", "echo hi; sleep 30")
	p.Stdout = dir + "/out.log"
	p.Stderr = dir + "/err.log"
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("svc"); return st.Running })
	time.Sleep(200 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	for i := 0; i < 100; i++ {
		if err := m.RestartProgram("svc"); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
	g, fd := settle(t, baseG, baseFD)
	t.Logf("100 file-output restarts: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

// Crash loop: the process keeps exiting and retrying; each instance's pipes must be reclaimed.
func TestLeakCrashLoop(t *testing.T) {
	p := prog("crash", "echo boom; exit 1")
	p.Restart = config.RestartAlways
	p.RestartGap = time.Millisecond
	p.RestartMax = time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	time.Sleep(300 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	// Let it crash about 200 times.
	time.Sleep(4 * time.Second)
	st, _ := m.Status("crash")
	t.Logf("restarts after 4s: %d", st.Restarts)

	_ = m.StopProgram("crash")
	g, fd := settle(t, baseG, baseFD)
	t.Logf("crash loop: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

// Repeated reload add/remove: Program structs, ring buffers and goroutines must all be reclaimed.
func TestLeakReloadChurn(t *testing.T) {
	keep := prog("keep", "sleep 300")
	m := newTestManager(t, keep)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("keep"); return st.Running })
	time.Sleep(200 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	for i := 0; i < 100; i++ {
		tmp := prog("tmp", "echo hi; sleep 300")
		cfg := &config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{keep, tmp}}
		if _, err := m.Reload(cfg, false); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		waitFor(t, func() bool { st, _ := m.Status("tmp"); return st.Running })

		cfg2 := &config.Config{Path: "test", Daemon: m.Config().Daemon, Programs: []*config.Program{keep}}
		if _, err := m.Reload(cfg2, false); err != nil {
			t.Fatalf("remove %d: %v", i, err)
		}
	}
	g, fd := settle(t, baseG, baseFD)
	t.Logf("100 add/remove cycles: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

// After a flood of output (filling and overflowing the ring buffer), memory must not keep growing.
func TestLeakOutputFlood(t *testing.T) {
	p := prog("flood", "while true; do head -c 8192 /dev/zero | tr '\\0' 'a'; echo; done")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("flood"); return st.Running })

	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Keep reading snapshots (simulating client polling) and watch whether the heap stays stable.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = m.Output("flood", "both", 100)
		time.Sleep(5 * time.Millisecond)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("flood heap: %d KiB -> %d KiB (delta %d KiB)", before.HeapAlloc>>10, after.HeapAlloc>>10, delta>>10)
	if delta > 8<<20 {
		t.Errorf("heap growth %d KiB", delta>>10)
	}
}

// State files: state_dir must not accumulate files after repeated start/stop cycles.
func TestLeakStateFiles(t *testing.T) {
	p := prog("svc", "sleep 300")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("svc"); return st.Running })

	for i := 0; i < 50; i++ {
		if err := m.StopProgram("svc"); err != nil {
			t.Fatal(err)
		}
		if err := m.StartProgram("svc"); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(m.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("state files after 50 start/stop cycles: %d", len(entries))
	if len(entries) > 1 {
		t.Errorf("state files piling up: %d", len(entries))
	}
}

// Crash loop: once restart_delay_max applies, the interval should stay at the cap
// rather than keep growing.
func TestLeakCrashLoopRate(t *testing.T) {
	p := prog("crash", "exit 1")
	p.Restart = config.RestartAlways
	p.RestartGap = 10 * time.Millisecond
	p.RestartMax = 50 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	time.Sleep(2 * time.Second)
	st1, _ := m.Status("crash")
	time.Sleep(2 * time.Second)
	st2, _ := m.Status("crash")
	rate := float64(st2.Restarts-st1.Restarts) / 2.0
	t.Logf("restarts: %d -> %d, rate %.1f/s (max 50ms => 20/s)", st1.Restarts, st2.Restarts, rate)
	if rate < 10 {
		t.Errorf("restart rate %.1f/s is too low, backoff may not be capped", rate)
	}
}

// Long-running: the ring buffer has a fixed capacity, so memory does not grow after
// repeated fill-ups.
func TestLeakRingStable(t *testing.T) {
	p := prog("flood", "head -c 1000000 /dev/zero | tr '\\0' 'a'; sleep 300")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool {
		out, err := m.Output("flood", "stdout", 0)
		return err == nil && len(out.Stdout) > 0
	})
	time.Sleep(300 * time.Millisecond)

	before := heapNow()
	for i := 0; i < 200; i++ {
		_, _ = m.Output("flood", "both", 0)
	}
	after := heapNow()
	t.Logf("heap %d KiB -> %d KiB", before>>10, after>>10)
	if int64(after)-int64(before) > 8<<20 {
		t.Errorf("heap growth %d KiB", (after-before)>>10)
	}
}

func heapNow() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// A program producing output continuously over a long period should keep the heap stable
// (it must not grow with accumulated output).
func TestLeakSustainedOutputHeap(t *testing.T) {
	p := prog("noisy", "i=0; while true; do i=$((i+1)); echo \"line $i padding padding padding\"; sleep 0.002; done")
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("noisy"); return st.Running })

	time.Sleep(time.Second)
	var samples []uint64
	for i := 0; i < 6; i++ {
		time.Sleep(time.Second)
		_, _ = m.Output("noisy", "both", 200)
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		samples = append(samples, ms.HeapAlloc)
	}
	t.Logf("heap samples (KiB):")
	for i, s := range samples {
		t.Logf("  t=%ds: %d", i+1, s>>10)
	}
	// Compare only the second half to skip the warm-up.
	first, last := samples[2], samples[5]
	if int64(last) > int64(first)+4<<20 {
		t.Errorf("heap keeps growing: %d KiB -> %d KiB", first>>10, last>>10)
	}
}

// Log file + ring buffer dual write: fd and heap should both stay stable over a long run.
func TestLeakFileSinkSustained(t *testing.T) {
	dir := t.TempDir()
	p := prog("noisy", "i=0; while true; do i=$((i+1)); echo line$i; sleep 0.002; done")
	p.Stdout = dir + "/out.log"
	p.Stderr = "ring"
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })
	waitFor(t, func() bool { st, _ := m.Status("noisy"); return st.Running })

	time.Sleep(500 * time.Millisecond)
	baseFD := openFDs(t)
	var base runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&base)

	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	fd := openFDs(t)
	t.Logf("file sink: heap %d KiB -> %d KiB, fd %d -> %d", base.HeapAlloc>>10, after.HeapAlloc>>10, baseFD, fd)
	if int64(after.HeapAlloc) > int64(base.HeapAlloc)+4<<20 {
		t.Errorf("heap growth")
	}
	if fd > baseFD+1 {
		t.Errorf("fd growth %d -> %d", baseFD, fd)
	}
	_ = os.Remove(dir + "/out.log")
}

// Backoff restarts must fire precisely at nextRestartAt, not be slowed down by a
// fixed polling interval.
func TestBackoffTimingAccuracy(t *testing.T) {
	p := prog("crash", "exit 1")
	p.Restart = config.RestartAlways
	p.RestartGap = 10 * time.Millisecond
	p.RestartMax = 50 * time.Millisecond
	m := newTestManager(t, p)
	m.Start()
	t.Cleanup(func() { _ = m.Shutdown() })

	time.Sleep(2 * time.Second)
	st1, _ := m.Status("crash")
	time.Sleep(2 * time.Second)
	st2, _ := m.Status("crash")
	rate := float64(st2.Restarts-st1.Restarts) / 2.0
	// With a 50ms cap the theoretical rate is 20/s; allow for scheduling jitter, but not below 10/s.
	if rate < 10 {
		t.Errorf("restart rate %.1f/s, the backoff timer may not be firing at nextRestartAt", rate)
	}
}

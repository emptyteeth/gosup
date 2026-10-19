package rpc

import (
	"io"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"
)

// openFDs counts the number of file descriptors open in this process.
func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd: %v", err)
	}
	return len(entries)
}

// newClient returns a client with keep-alive disabled, so the test's own connection pool does not skew the counts.
func newClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		Timeout:   10 * time.Second,
	}
}

// settle waits for goroutines and fds to fall back near the baseline.
func settle(t *testing.T, baseG, baseFD int) (int, int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
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

func TestLeakStreamChurn(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("t", "while true; do echo x; sleep 0.02; done"))
	client := newClient()

	// Warm up.
	for i := 0; i < 3; i++ {
		resp, err := client.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.Body.Read(make([]byte, 64))
		resp.Body.Close()
	}
	time.Sleep(300 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	for i := 0; i < 100; i++ {
		resp, err := client.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("iteration %d status code %d", i, resp.StatusCode)
		}
		_, _ = resp.Body.Read(make([]byte, 64))
		resp.Body.Close()
	}
	g, fd := settle(t, baseG, baseFD)
	t.Logf("100 streams: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

func TestLeakRPCChurn(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("t", "sleep 300"))
	client := newClient()
	for i := 0; i < 5; i++ {
		_, _ = client.Post(srv.URL+"/rpc", "application/json", stringsReader(`{"method":"list"}`))
	}
	time.Sleep(200 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	for i := 0; i < 1000; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rpc", stringsReader(`{"method":"list"}`))
		req.Header.Set("Authorization", "Bearer pw")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	g, fd := settle(t, baseG, baseFD)
	runtime.GC()
	runtime.ReadMemStats(&after)
	heap := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("1000 rpc: goroutines %d -> %d, fds %d -> %d, heap delta %d KiB", baseG, g, baseFD, fd, heap>>10)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
	if heap > 4<<20 {
		t.Errorf("heap growth %d KiB", heap>>10)
	}
}

func TestLeakStreamAcrossRestart(t *testing.T) {
	srv, mgr := newServer(t, "pw", testProgram("t", "i=0; while true; do i=$((i+1)); echo x$i; sleep 0.05; done"))
	client := newClient()
	time.Sleep(200 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	for round := 0; round < 20; round++ {
		resp, err := client.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.Body.Read(make([]byte, 64))
		if err := mgr.RestartProgram("t"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		resp.Body.Close()
	}
	g, fd := settle(t, baseG, baseFD)
	t.Logf("20 stream+restart cycles: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

func TestLeakAbruptDisconnect(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("t", "while true; do echo x; sleep 0.01; done"))
	client := newClient()
	time.Sleep(300 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	for i := 0; i < 50; i++ {
		resp, err := client.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = resp.Body.Read(make([]byte, 1))
		resp.Body.Close()
	}
	g, fd := settle(t, baseG, baseFD)
	t.Logf("50 abrupt disconnects: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

func TestLeakRejectedConnections(t *testing.T) {
	srv, mgr := newServer(t, "pw", testProgram("t", "while true; do echo x; sleep 0.05; done"))
	cfg := *mgr.Config()
	cfg.Daemon.MaxConnections = 1
	if _, err := mgr.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}
	client := newClient()
	held, err := client.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Body.Close()
	_, _ = held.Body.Read(make([]byte, 64))
	time.Sleep(200 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	for i := 0; i < 100; i++ {
		resp, err := client.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", resp.StatusCode)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	// One stream is still held here, so a reasonable +1 occupancy is allowed.
	g, fd := settle(t, baseG, baseFD)
	t.Logf("100 rejected: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

func TestLeakFrozenClient(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("t", "while true; do echo spam; done"))
	client := newClient()
	time.Sleep(200 * time.Millisecond)
	baseG, baseFD := runtime.NumGoroutine(), openFDs(t)

	// Never read after connecting; once the server's write timeout fires, it should disconnect and reclaim the stream.
	var resp *http.Response
	for i := 0; i < 5; i++ {
		r, err := client.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
		if err != nil {
			t.Fatal(err)
		}
		resp = r
	}
	_ = resp
	time.Sleep(15 * time.Second)
	g, fd := settle(t, baseG, baseFD)
	t.Logf("5 frozen clients: goroutines %d -> %d, fds %d -> %d", baseG, g, baseFD, fd)
	if g > baseG+2 || fd > baseFD+2 {
		t.Errorf("leak: goroutines %d->%d, fds %d->%d", baseG, g, baseFD, fd)
	}
}

func stringsReader(s string) io.Reader { return &reader{s: s} }

type reader struct {
	s string
	i int
}

func (r *reader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}

// Following one stream for a long time: the server must not accumulate data "waiting to be sent".
func TestLeakLongFollowHeap(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("noisy", "i=0; while true; do i=$((i+1)); echo \"line $i padding padding\"; sleep 0.002; done"))
	r := openSSE(t, srv, "pw", "program=noisy&stream=both")
	defer r.resp.Body.Close()

	// Keep consuming events.
	var samples []uint64
	for i := 0; i < 6; i++ {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			select {
			case <-r.Events:
			case <-r.Errs:
				t.Fatal("stream ended unexpectedly")
			case <-time.After(50 * time.Millisecond):
			}
		}
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		samples = append(samples, ms.HeapAlloc)
	}
	t.Logf("follow heap samples (KiB): %v", kibs(samples))
	if int64(samples[5]) > int64(samples[2])+4<<20 {
		t.Errorf("heap keeps growing: %d KiB -> %d KiB", samples[2]>>10, samples[5]>>10)
	}
}

// Slow consumer: the client reads slowly, and the server must not buffer without bound.
func TestLeakSlowConsumerBounded(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("noisy", "while true; do echo \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"; done"))
	r := openSSE(t, srv, "pw", "program=noisy&stream=stdout")
	defer r.resp.Body.Close()

	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// No reads at all for 5 seconds; the server should end the connection via the write timeout rather than buffer.
	time.Sleep(5 * time.Second)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("slow consumer heap delta: %d KiB", delta>>10)
	if delta > 16<<20 {
		t.Errorf("slow consumer caused heap growth of %d KiB", delta>>10)
	}
}

func kibs(v []uint64) []uint64 {
	out := make([]uint64, len(v))
	for i := range v {
		out[i] = v[i] >> 10
	}
	return out
}

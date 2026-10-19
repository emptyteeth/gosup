package rpc

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gosup/internal/config"
	"gosup/internal/logx"
	"gosup/internal/supervisor"
)

// newServer starts a test server backed by a real supervisor.
func newServer(t *testing.T, password string, programs ...*config.Program) (*httptest.Server, *supervisor.Manager) {
	t.Helper()
	srv, mgr, _ := newServerWithAPI(t, password, programs...)
	return srv, mgr
}

// newServerWithAPI is like newServer but also returns the rpc.Server, which makes it easier to test the shutdown flow.
func newServerWithAPI(t *testing.T, password string, programs ...*config.Program) (*httptest.Server, *supervisor.Manager, *Server) {
	t.Helper()
	cfg := &config.Config{
		Path: "test",
		Daemon: config.Daemon{
			Password:       password,
			AllowNoAuth:    password == "",
			StateDir:       t.TempDir(),
			OutputBuffer:   64 * 1024,
			StopTimeout:    3 * time.Second,
			MaxConnections: config.DefaultMaxConnections,
		},
		Programs: programs,
	}
	log := logx.New(io.Discard, logx.Error)
	mgr, err := supervisor.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	mgr.Start()
	t.Cleanup(func() { _ = mgr.Shutdown() })

	api := New(mgr, log)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return srv, mgr, api
}

func testProgram(id, command string) *config.Program {
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

// testResponse mirrors the server's Response, but keeps Result as raw JSON for easier assertions.
type testResponse struct {
	ID     json.RawMessage `json:"id,omitempty"`
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

// call sends a POST /rpc request.
func call(t *testing.T, srv *httptest.Server, req map[string]any) testResponse {
	t.Helper()
	return callAuth(t, srv, "pw", req)
}

// callAuth sends a request with the given password.
func callAuth(t *testing.T, srv *httptest.Server, password string, req map[string]any) testResponse {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, srv.URL+"/rpc", strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if password != "" {
		httpReq.Header.Set("Authorization", "Bearer "+password)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST /rpc: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out testResponse
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	return out
}

// sseReader reads events from one SSE connection.
type sseReader struct {
	resp   *http.Response
	br     *bufio.Reader
	Events chan sseEvent
	Errs   chan error
}

type sseEvent struct {
	ID   string
	Name string
	Data string
}

// openSSE issues GET /events and parses it in the background.
func openSSE(t *testing.T, srv *httptest.Server, password, query string) *sseReader {
	t.Helper()
	url := srv.URL + "/events?" + query
	if password != "" {
		url += "&password=" + password
	}
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("GET /events status code %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}
	r := &sseReader{resp: resp, br: bufio.NewReader(resp.Body), Events: make(chan sseEvent, 64), Errs: make(chan error, 1)}
	t.Cleanup(func() { resp.Body.Close() })
	go r.pump()
	return r
}

func (r *sseReader) pump() {
	var ev sseEvent
	for {
		line, err := r.br.ReadString('\n')
		if err != nil {
			r.Errs <- err
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if ev.Name != "" {
				select {
				case r.Events <- ev:
				default:
				}
			}
			ev = sseEvent{}
		case strings.HasPrefix(line, "id: "):
			ev.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			ev.Name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			ev.Data = strings.TrimPrefix(line, "data: ")
		}
	}
}

// waitEvent waits for an event with the given name, ignoring pings.
func (r *sseReader) waitEvent(t *testing.T, name string, timeout time.Duration) sseEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-r.Events:
			if ev.Name == name {
				return ev
			}
		case err := <-r.Errs:
			t.Fatalf("SSE connection ended: %v", err)
		case <-deadline:
			t.Fatalf("timed out waiting for event %q", name)
		}
	}
}

func TestAuthRequired(t *testing.T) {
	srv, _ := newServer(t, "pw")

	resp := callAuth(t, srv, "", map[string]any{"method": "list"})
	if resp.OK || resp.Error == nil || resp.Error.Code != CodeUnauthorized {
		t.Fatalf("calls without a password should be rejected, got %+v", resp)
	}

	resp = callAuth(t, srv, "wrong", map[string]any{"method": "list"})
	if resp.OK || resp.Error.Code != CodeUnauthorized {
		t.Fatalf("a wrong password should be rejected, got %+v", resp)
	}

	// Query-parameter authentication.
	if code := getStatus(t, srv.URL+"/rpc?password=pw"); code != http.StatusMethodNotAllowed {
		t.Errorf("GET /rpc status code = %d", code)
	}
	qresp := callQuery(t, srv, map[string]any{"method": "ping"})
	if !qresp.OK {
		t.Errorf("query-parameter authentication failed: %+v", qresp.Error)
	}
}

func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// callQuery sends a request authenticated with ?password=.
func callQuery(t *testing.T, srv *httptest.Server, req map[string]any) testResponse {
	t.Helper()
	b, _ := json.Marshal(req)
	resp, err := http.Post(srv.URL+"/rpc?password=pw", "application/json", strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out testResponse
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	return out
}

func TestNoAuthMode(t *testing.T) {
	srv, _ := newServer(t, "")
	resp := call(t, srv, map[string]any{"method": "ping"})
	if !resp.OK {
		t.Fatalf("allow_no_auth should let the call through: %+v", resp.Error)
	}
}

func TestListAndStatus(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("web", "sleep 30"))

	resp := call(t, srv, map[string]any{"id": "a", "method": "list"})
	if !resp.OK {
		t.Fatal(resp.Error)
	}
	var list struct {
		Programs []supervisor.Status `json:"programs"`
	}
	if err := json.Unmarshal(resp.Result, &list); err != nil {
		t.Fatal(err)
	}
	waitForRPC(t, func() bool {
		st := statusOf(t, srv, "web")
		return st.Running && st.PID > 0
	})

	resp = call(t, srv, map[string]any{"method": "status", "params": map[string]any{"program": "web"}})
	if !resp.OK {
		t.Fatal(resp.Error)
	}
	var st supervisor.Status
	if err := json.Unmarshal(resp.Result, &st); err != nil {
		t.Fatal(err)
	}
	if st.PID <= 0 || !st.Running {
		t.Errorf("status = %+v", st)
	}
	if st.StartedAt == nil {
		t.Error("status should include started_at so clients can compute uptime")
	}

	resp = call(t, srv, map[string]any{"method": "status", "params": map[string]any{"program": "missing"}})
	if resp.OK || resp.Error.Code != CodeNotFound {
		t.Fatalf("an unknown id should return not_found, got %+v", resp)
	}
}

func TestControlOps(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("svc", "sleep 30"))

	resp := call(t, srv, map[string]any{"method": "stop", "params": map[string]any{"program": "svc"}})
	if !resp.OK {
		t.Fatal(resp.Error)
	}
	var st supervisor.Status
	_ = json.Unmarshal(resp.Result, &st)
	if st.Running {
		t.Error("should not be running after stop")
	}

	resp = call(t, srv, map[string]any{"method": "start", "params": map[string]any{"program": "svc"}})
	if !resp.OK {
		t.Fatal(resp.Error)
	}
	_ = json.Unmarshal(resp.Result, &st)
	if !st.Running {
		t.Error("should be running after start")
	}

	resp = call(t, srv, map[string]any{"method": "start", "params": map[string]any{"program": "svc"}})
	if resp.OK || resp.Error.Code != CodeAlready {
		t.Fatalf("a duplicate start should return already_running, got %+v", resp)
	}

	resp = call(t, srv, map[string]any{"method": "restart", "params": map[string]any{"program": "svc"}})
	if !resp.OK {
		t.Fatal(resp.Error)
	}
}

func TestOutput(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("echoer", "echo hello; echo oops >&2; sleep 30"))

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp := call(t, srv, map[string]any{"method": "output", "params": map[string]any{"program": "echoer", "stream": "both"}})
		if !resp.OK {
			t.Fatal(resp.Error)
		}
		var out OutputResult
		if err := json.Unmarshal(resp.Result, &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.Stdout, "hello") && strings.Contains(out.Stderr, "oops") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no output received: %+v", out)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestOutputUnknownStreamIsInvalidParams(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("svc", "sleep 30"))
	resp := call(t, srv, map[string]any{"method": "output", "params": map[string]any{"program": "svc", "stream": "nope"}})
	if resp.OK {
		t.Fatal("an unknown stream should fail")
	}
	if resp.Error.Code != CodeInvalid && resp.Error.Code != CodeInternal {
		t.Errorf("error code = %s", resp.Error.Code)
	}
}

func TestUnknownMethod(t *testing.T) {
	srv, _ := newServer(t, "pw")
	resp := call(t, srv, map[string]any{"method": "nope"})
	if resp.OK || resp.Error.Code != CodeInvalid {
		t.Fatalf("an unknown method should return invalid_params, got %+v", resp)
	}
}

func TestPing(t *testing.T) {
	srv, _ := newServer(t, "pw")
	resp := call(t, srv, map[string]any{"method": "ping"})
	if !resp.OK {
		t.Fatal(resp.Error)
	}
}

func TestMalformedJSON(t *testing.T) {
	srv, _ := newServer(t, "pw")
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rpc", strings.NewReader("{not json"))
	req.Header.Set("Authorization", "Bearer pw")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status code = %d", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	var r testResponse
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	if r.OK || r.Error.Code != CodeParse {
		t.Errorf("resp = %+v", r)
	}
}

func TestOutputLines(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("lines", "i=0; while [ $i -lt 5 ]; do i=$((i+1)); echo line$i; done; sleep 30"))

	var res OutputResult
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp := call(t, srv, map[string]any{"method": "output", "params": map[string]any{
			"program": "lines", "stream": "stdout", "lines": 2,
		}})
		if !resp.OK {
			t.Fatal(resp.Error)
		}
		res = OutputResult{}
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(res.Stdout, "line5") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no output received: %+v", res)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if strings.TrimSpace(res.Stdout) != "line4\nline5" {
		t.Errorf("stdout = %q", res.Stdout)
	}
	if !res.StdoutTruncated {
		t.Error("should be marked truncated after truncation")
	}

	resp := call(t, srv, map[string]any{"method": "output", "params": map[string]any{
		"program": "lines", "stream": "stdout",
	}})
	if !resp.OK {
		t.Fatal(resp.Error)
	}
	var all OutputResult
	_ = json.Unmarshal(resp.Result, &all)
	if !strings.Contains(all.Stdout, "line1") {
		t.Errorf("full output should contain line1: %q", all.Stdout)
	}
	if all.StdoutTruncated {
		t.Error("should not be marked when not truncated")
	}

	resp = call(t, srv, map[string]any{"method": "output", "params": map[string]any{
		"program": "lines", "stream": "stdout", "lines": -1,
	}})
	if resp.OK || resp.Error.Code != CodeInvalid {
		t.Fatalf("a negative lines value should error, got %+v", resp)
	}
}

func TestEventsFollow(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("ticker", "i=0; while true; do i=$((i+1)); echo tick$i; sleep 0.1; done"))

	r := openSSE(t, srv, "pw", "program=ticker&stream=stdout&lines=1")
	// The first frame is a snapshot (possibly empty); increments follow.
	var got string
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(got, "tick") {
		ev := r.waitEvent(t, "output", time.Until(deadline))
		var data Event
		if err := json.Unmarshal([]byte(ev.Data), &data); err != nil {
			t.Fatalf("event data is not JSON: %s", ev.Data)
		}
		if data.Stream != "stdout" || data.Program != "ticker" {
			t.Errorf("event = %+v", data)
		}
		got += data.Data
	}
}

func TestEventsAuthRequired(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("svc", "sleep 30"))
	resp, err := http.Get(srv.URL + "/events?program=svc")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unauthenticated SSE status code = %d", resp.StatusCode)
	}
}

func TestEventsUnknownProgram(t *testing.T) {
	srv, _ := newServer(t, "pw")
	resp, err := http.Get(srv.URL + "/events?program=missing&password=pw")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown program status code = %d", resp.StatusCode)
	}
}

func TestEventsResetAndExit(t *testing.T) {
	srv, mgr := newServer(t, "pw", testProgram("svc", "i=0; while true; do i=$((i+1)); echo line$i; sleep 0.1; done"))
	r := openSSE(t, srv, "pw", "program=svc&stream=stdout")

	// Trigger a restart; a reset event should arrive.
	time.Sleep(200 * time.Millisecond)
	if err := mgr.RestartProgram("svc"); err != nil {
		t.Fatal(err)
	}
	r.waitEvent(t, "reset", 5*time.Second)
}

func statusOf(t *testing.T, srv *httptest.Server, id string) supervisor.Status {
	t.Helper()
	resp := call(t, srv, map[string]any{"method": "status", "params": map[string]any{"program": id}})
	if !resp.OK {
		return supervisor.Status{}
	}
	var st supervisor.Status
	_ = json.Unmarshal(resp.Result, &st)
	return st
}

func waitForRPC(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the condition")
}

func TestReloadViaHTTP(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "gosup.ini")
	writeCfg := func(extra string) {
		content := "[daemon]\npassword = pw\n\n[program:a]\ncommand = /bin/sh -c 'sleep 30'\n" + extra
		if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeCfg("")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Daemon.Password = "pw"
	cfg.Daemon.StateDir = t.TempDir()
	cfg.Daemon.OutputBuffer = 64 * 1024
	cfg.Daemon.ShutdownTimeout = 10 * time.Second
	cfg.Daemon.StopTimeout = 2 * time.Second
	cfg.Daemon.MaxConnections = config.DefaultMaxConnections
	cfg.Programs[0].StopSignal = syscall.SIGTERM
	cfg.Programs[0].Stdout = "ring"
	cfg.Programs[0].Stderr = "ring"

	log := logx.New(io.Discard, logx.Error)
	mgr, err := supervisor.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	mgr.Start()
	t.Cleanup(func() { _ = mgr.Shutdown() })
	srv := httptest.NewServer(New(mgr, log).Handler())
	t.Cleanup(srv.Close)

	waitForRPC(t, func() bool {
		return statusOf(t, srv, "a").Running
	})

	writeCfg("\n[program:b]\ncommand = /bin/sh -c 'sleep 30'\n")
	resp := call(t, srv, map[string]any{"method": "reload", "params": map[string]any{"restart": false}})
	if !resp.OK {
		t.Fatalf("reload failed: %+v", resp.Error)
	}
	var res supervisor.ReloadResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Added) != 1 || res.Added[0] != "b" {
		t.Fatalf("res = %+v", res)
	}
	waitForRPC(t, func() bool {
		return statusOf(t, srv, "b").Running
	})

	// A malformed config makes reload fail without affecting existing programs.
	if err := os.WriteFile(cfgPath, []byte("[daemon]\nlisten = 127.0.0.1:1\n\n[program:x]\ncommand = /bin/true\nbogus = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	resp = call(t, srv, map[string]any{"method": "reload"})
	if resp.OK || resp.Error.Code != CodeInvalid {
		t.Fatalf("an invalid config should return invalid_params, got %+v", resp)
	}
	if !statusOf(t, srv, "a").Running {
		t.Error("existing programs should keep running after a failed reload")
	}
}

func TestMaxConnections(t *testing.T) {
	srv, mgr := newServer(t, "pw", testProgram("ticker", "while true; do echo tick; sleep 0.1; done"))
	cfg := *mgr.Config()
	cfg.Daemon.MaxConnections = 2
	if _, err := mgr.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}

	r1 := openSSE(t, srv, "pw", "program=ticker&stream=stdout")
	r2 := openSSE(t, srv, "pw", "program=ticker&stream=stdout")
	_ = r1
	_ = r2

	// The third SSE connection should be rejected (503).
	resp, err := http.Get(srv.URL + "/events?program=ticker&stream=stdout&password=pw")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status code above max_connections = %d", resp.StatusCode)
	}

	// After closing one, reconnecting should work.
	r1.resp.Body.Close()
	time.Sleep(300 * time.Millisecond)
	resp2, err := http.Get(srv.URL + "/events?program=ticker&stream=stdout&password=pw")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status code after releasing a slot = %d", resp2.StatusCode)
	}
}

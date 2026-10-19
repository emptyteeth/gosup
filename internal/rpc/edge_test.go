package rpc

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// http.Server.Shutdown does not interrupt active handlers; SSE long-lived connections must be canceled
// by CloseStreams first, otherwise shutdown waits until the Shutdown context times out.
func TestShutdownWithActiveStreamsIsFast(t *testing.T) {
	srv, _, api := newServerWithAPI(t, "pw", testProgram("t", "while true; do echo x; sleep 0.05; done"))
	for i := 0; i < 3; i++ {
		r := openSSE(t, srv, "pw", "program=t&stream=stdout")
		r.waitEvent(t, "output", 3*time.Second)
	}

	// Cancel the streams first, then Shutdown: this is exactly the order used by Daemon.shutdown.
	start := time.Now()
	api.CloseStreams()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Config.Shutdown(ctx)
	elapsed := time.Since(start)
	t.Logf("CloseStreams + Shutdown took %v", elapsed)
	if elapsed > time.Second {
		t.Errorf("close took %v, meaning SSE was not canceled in time", elapsed)
	}
}

// Without calling CloseStreams, Shutdown waits until it times out — this is the behavior before the fix,
// shown here to demonstrate why the streams must be canceled before Shutdown.
func TestShutdownWithoutCloseStreamsBlocks(t *testing.T) {
	srv, _, _ := newServerWithAPI(t, "pw", testProgram("t", "while true; do echo x; sleep 0.05; done"))
	r := openSSE(t, srv, "pw", "program=t&stream=stdout")
	defer r.resp.Body.Close()
	r.waitEvent(t, "output", 3*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := srv.Config.Shutdown(ctx)
	elapsed := time.Since(start)
	t.Logf("Shutdown without CloseStreams: err=%v elapsed=%v", err, elapsed)
	if err == nil {
		t.Error("expected a timeout (an active handler blocked Shutdown)")
	}
}

// A slot is released only when it was occupied: changing the limit from non-zero to 0 and back again must not corrupt the count.
func TestConnSlotNotReleasedWhenUncounted(t *testing.T) {
	srv, mgr := newServer(t, "pw", testProgram("t", "while true; do echo x; sleep 0.1; done"))

	cfg := *mgr.Config()
	cfg.Daemon.MaxConnections = 2
	if _, err := mgr.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}
	r1 := openSSE(t, srv, "pw", "program=t&stream=stdout") // occupies 1
	defer r1.resp.Body.Close()

	// Switch to unlimited: new connections are not counted now.
	cfg.Daemon.MaxConnections = 0
	if _, err := mgr.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}
	r2 := openSSE(t, srv, "pw", "program=t&stream=stdout")
	r2.resp.Body.Close() // closing must not decrement r1's slot
	time.Sleep(200 * time.Millisecond)

	cfg.Daemon.MaxConnections = 2
	if _, err := mgr.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}
	// r1 still occupies 1, so one more should be allowed and the second rejected.
	ok, denied := 0, 0
	for i := 0; i < 2; i++ {
		resp, err := http.Get(srv.URL + "/events?program=t&stream=stdout&password=pw")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			ok++
		} else {
			denied++
		}
	}
	if ok != 1 || denied != 1 {
		t.Errorf("ok=%d denied=%d, expected 1/1 (a slot was wrongly taken or not counted)", ok, denied)
	}
}

// An unknown stream should return invalid_params / 400, not internal_error / 404.
func TestUnknownStreamIsInvalidParams(t *testing.T) {
	srv, _ := newServer(t, "pw", testProgram("t", "sleep 30"))

	resp := call(t, srv, map[string]any{"method": "output", "params": map[string]any{"program": "t", "stream": "bogus"}})
	if resp.OK || resp.Error.Code != CodeInvalid {
		t.Errorf("output unknown stream = %+v", resp.Error)
	}

	r, err := http.Get(srv.URL + "/events?program=t&stream=bogus&password=pw")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusBadRequest {
		t.Errorf("events unknown stream status code = %d", r.StatusCode)
	}
}

// An authenticated SSE stream keeps pushing after a password reload; the old password stops working for new requests.
func TestPasswordReload(t *testing.T) {
	srv, mgr := newServer(t, "pw", testProgram("t", "while true; do echo x; sleep 0.05; done"))
	r := openSSE(t, srv, "pw", "program=t&stream=stdout")
	defer r.resp.Body.Close()
	r.waitEvent(t, "output", 3*time.Second)

	cfg := *mgr.Config()
	cfg.Daemon.Password = "newpw"
	if _, err := mgr.Reload(&cfg, false); err != nil {
		t.Fatal(err)
	}
	if callAuth(t, srv, "pw", map[string]any{"method": "ping"}).OK {
		t.Error("the old password should stop working")
	}
	if !callAuth(t, srv, "newpw", map[string]any{"method": "ping"}).OK {
		t.Error("the new password should work")
	}
	r.waitEvent(t, "output", 3*time.Second)
}

// A cross-origin preflight requires no password and echoes the origin; requests without Origin carry no CORS headers.
func TestCORS(t *testing.T) {
	srv, _ := newServer(t, "pw")
	req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/rpc", nil)
	req.Header.Set("Origin", "https://mui.kernelsu.org")
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("preflight status code = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://mui.kernelsu.org" {
		t.Errorf("ACAO = %q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Headers"); got == "" {
		t.Error("missing Access-Control-Allow-Headers")
	}

	plain, err := http.Get(srv.URL + "/rpc")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Body.Close()
	if plain.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Error("a request without Origin should not carry CORS headers")
	}
}

// An oversized request body returns parse_error instead of hanging.
func TestOversizedBody(t *testing.T) {
	srv, _ := newServer(t, "pw")
	body := `{"method":"ping","params":"` + repeat("a", 2<<20) + `"}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/rpc", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer pw")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("oversized request status code = %d", resp.StatusCode)
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gosup/internal/config"
	"gosup/internal/daemon"
)

// clientRequest and clientResponse mirror the server protocol.
type clientRequest struct {
	ID     int64  `json:"id,omitempty"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type clientResponse struct {
	ID     json.RawMessage `json:"id,omitempty"`
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type statusView struct {
	ID            string     `json:"id"`
	Enabled       bool       `json:"enabled"`
	State         string     `json:"state"`
	Running       bool       `json:"running"`
	PID           int        `json:"pid"`
	StartedAt     *time.Time `json:"started_at"`
	Restarts      int        `json:"restarts"`
	ExitCode      *int       `json:"exit_code"`
	Signal        string     `json:"signal"`
	NextRestartAt *time.Time `json:"next_restart_at"`
	LastError     string     `json:"last_error"`
	Command       string     `json:"command"`
	PendingReload bool       `json:"pending_reload"`
}

type listView struct {
	Programs []statusView `json:"programs"`
}

type outputView struct {
	ID              string `json:"id"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	StderrTruncated bool   `json:"stderr_truncated"`
}

// sseView is the payload pushed on /events (corresponds to the server's rpc.Event).
type sseView struct {
	Program string `json:"program"`
	Stream  string `json:"stream"`
	Data    string `json:"data"`
	Reset   bool   `json:"reset"`
	Lost    bool   `json:"lost"`
	Message string `json:"message"`
}

// reloadView corresponds to the server's ReloadResult.
type reloadView struct {
	Added           []string `json:"added"`
	Removed         []string `json:"removed"`
	Updated         []string `json:"updated"`
	Restarted       []string `json:"restarted"`
	Started         []string `json:"started"`
	Stopped         []string `json:"stopped"`
	PendingRestart  []string `json:"pending_restart"`
	RequiresRestart []string `json:"requires_restart"`
	DaemonUpdated   []string `json:"daemon_updated"`
	Warnings        []string `json:"warnings"`
	Unchanged       bool     `json:"unchanged"`
}

// endpoint holds connection information.
type endpoint struct {
	base     string // http://127.0.0.1:9090 or https://...
	password string
	noAuth   bool
	client   *http.Client
}

// resolveEndpoint derives connection information from the config file and command-line options.
func resolveEndpoint(opts *options, timeout time.Duration) (*endpoint, error) {
	cfg, err := config.Load(opts.confPath)
	if err != nil {
		// Connecting with --addr/--password is still allowed when the config file is unavailable.
		if opts.addr == "" || opts.password == "" {
			return nil, err
		}
		cfg = &config.Config{Daemon: config.Daemon{
			Listen: opts.addr, Password: opts.password, AllowNoAuth: opts.password == "",
		}}
	}
	addr := opts.addr
	if addr == "" {
		addr = cfg.Daemon.Listen
	}
	password := opts.password
	if password == "" && !cfg.Daemon.AllowNoAuth {
		password = cfg.Daemon.Password
	}
	scheme := "http"
	if cfg.Daemon.TLSCert != "" {
		scheme = "https"
	}
	return &endpoint{
		base:     scheme + "://" + addr,
		password: password,
		noAuth:   cfg.Daemon.AllowNoAuth && cfg.Daemon.Password == "" && cfg.Daemon.PasswordSHA256 == "",
		client:   &http.Client{Timeout: timeout},
	}, nil
}

// call sends a POST /rpc request.
func (e *endpoint) call(method string, params any, out any) error {
	body, err := json.Marshal(clientRequest{ID: time.Now().UnixNano(), Method: method, Params: params})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, e.base+"/rpc", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if !e.noAuth {
		req.Header.Set("Authorization", "Bearer "+e.password)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("connect %s: %w", e.base, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	var parsed clientResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if !parsed.OK {
		if parsed.Error != nil {
			return fmt.Errorf("%s: %s", parsed.Error.Code, parsed.Error.Message)
		}
		return fmt.Errorf("request failed (HTTP %d)", resp.StatusCode)
	}
	if out != nil && len(parsed.Result) > 0 {
		if err := json.Unmarshal(parsed.Result, out); err != nil {
			return fmt.Errorf("parse result: %w", err)
		}
	}
	return nil
}

// eventsURL builds the /events URL. EventSource cannot set custom headers, so the password goes in a query parameter.
func (e *endpoint) eventsURL(program, stream string, lines int) string {
	q := url.Values{}
	q.Set("program", program)
	q.Set("stream", stream)
	if lines > 0 {
		q.Set("lines", fmt.Sprint(lines))
	}
	if !e.noAuth {
		q.Set("password", e.password)
	}
	return e.base + "/events?" + q.Encode()
}

// runClient implements the client subcommands.
func runClient(command string, args []string, opts *options, timeout time.Duration) error {
	ep, err := resolveEndpoint(opts, timeout)
	if err != nil {
		return err
	}
	stream := opts.stream
	if stream == "" {
		stream = "both"
	}

	needID := func() (string, error) {
		if len(args) < 1 || strings.TrimSpace(args[0]) == "" {
			return "", fmt.Errorf("usage: gosup %s <id>", command)
		}
		return args[0], nil
	}

	switch command {
	case "list":
		var lv listView
		if err := ep.call("list", nil, &lv); err != nil {
			return err
		}
		if opts.jsonOut {
			return printJSON(lv)
		}
		fmt.Printf("%-24s %-8s %-8s %-7s %-10s %s\n", "ID", "ENABLED", "STATE", "PID", "UPTIME", "RESTARTS")
		for _, p := range lv.Programs {
			pid := "-"
			if p.PID > 0 {
				pid = fmt.Sprintf("%d", p.PID)
			}
			fmt.Printf("%-24s %-8t %-8s %-7s %-10s %d\n", p.ID, p.Enabled, p.State, pid, formatUptime(p), p.Restarts)
		}
		return nil
	case "ps":
		id, err := needID()
		if err != nil {
			return err
		}
		var sv statusView
		if err := ep.call("status", map[string]any{"program": id}, &sv); err != nil {
			return err
		}
		if opts.jsonOut {
			return printJSON(sv)
		}
		printStatus(sv)
		return nil
	case "logs":
		id, err := needID()
		if err != nil {
			return err
		}
		if opts.follow {
			return followEvents(ep, id, stream, opts.lines)
		}
		var ov outputView
		if err := ep.call("output", map[string]any{"program": id, "stream": stream, "lines": opts.lines}, &ov); err != nil {
			return err
		}
		if opts.jsonOut {
			return printJSON(ov)
		}
		return printOutput(ov, stream)
	case "start-program", "stop-program", "restart-program":
		id, err := needID()
		if err != nil {
			return err
		}
		var sv statusView
		if err := ep.call(strings.TrimSuffix(command, "-program"), map[string]any{"program": id}, &sv); err != nil {
			return err
		}
		if opts.jsonOut {
			return printJSON(sv)
		}
		printStatus(sv)
		return nil
	}
	return fmt.Errorf("unknown client command %q", command)
}

func printJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func printOutput(ov outputView, stream string) error {
	both := stream == "both"
	if stream == "stdout" || both {
		if ov.StdoutTruncated {
			fmt.Fprintln(os.Stderr, "--- stdout (truncated, newest output only) ---")
		} else {
			fmt.Fprintln(os.Stderr, "--- stdout ---")
		}
		_, _ = os.Stdout.WriteString(ov.Stdout)
		if ov.Stdout != "" && !strings.HasSuffix(ov.Stdout, "\n") {
			fmt.Println()
		}
	}
	if stream == "stderr" || both {
		if ov.StderrTruncated {
			fmt.Fprintln(os.Stderr, "--- stderr (truncated, newest output only) ---")
		} else {
			fmt.Fprintln(os.Stderr, "--- stderr ---")
		}
		_, _ = os.Stdout.WriteString(ov.Stderr)
		if ov.Stderr != "" && !strings.HasSuffix(ov.Stderr, "\n") {
			fmt.Println()
		}
	}
	return nil
}

// followEvents subscribes to /events and prints. Ctrl+C closes the connection directly, and the server cleans up accordingly.
func followEvents(ep *endpoint, program, stream string, lines int) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.eventsURL(program, stream, lines), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	// No overall timeout on long-lived connections; the lifetime is controlled by ctx and the connection itself.
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("subscribe failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}

	fmt.Fprintf(os.Stderr, "--- following %s (Ctrl+C to stop) ---\n", program)
	both := stream == "both"

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 4<<20)
	var evName, evData string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if evName == "" {
				continue
			}
			done, err := handleSSE(evName, evData, both)
			evName, evData = "", ""
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		case strings.HasPrefix(line, "event: "):
			evName = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			evData = strings.TrimPrefix(line, "data: ")
		}
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil // Ctrl+C
		}
		return err
	}
	return nil
}

// handleSSE handles a single SSE event. Returning done=true means following should end.
//
// SSE events arrive in write order, but stdout/stderr are two separate buffers,
// so in both mode they arrive interleaved. Instead of printing a separator header
// per stream here (which would switch headers on every line written), each line
// now gets a stream prefix, which is steadier to read.
func handleSSE(name, data string, both bool) (bool, error) {
	switch name {
	case "ping":
		return false, nil
	case "output", "reset", "lost":
		var ev sseView
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			fmt.Fprintf(os.Stderr, "gosup: cannot parse event: %v\n", err)
			return false, nil
		}
		prefix := ""
		if both {
			if ev.Stream == "stderr" {
				prefix = "[err] "
			} else {
				prefix = "[out] "
			}
		}
		if name == "reset" {
			fmt.Fprintf(os.Stderr, "--- %s: program restarted, output buffer reset ---\n", ev.Stream)
		}
		if name == "lost" || ev.Lost {
			fmt.Fprintf(os.Stderr, "--- %s: some output was dropped (buffer overflow) ---\n", ev.Stream)
		}
		for _, line := range splitLinesKeepEnds(ev.Data) {
			_, _ = os.Stdout.WriteString(prefix + line)
		}
		return false, nil
	case "error":
		var ev sseView
		_ = json.Unmarshal([]byte(data), &ev)
		return true, fmt.Errorf("follow error: %s", ev.Message)
	}
	return false, nil
}

// splitLinesKeepEnds splits s into lines, keeping the trailing newlines; the last segment may have none.
func splitLinesKeepEnds(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i+1])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// runReload triggers a config reload over HTTP; falls back to a local SIGHUP on failure.
func runReload(confPath, addr, password string, restart, jsonOut bool, timeout time.Duration) error {
	opts := &options{confPath: confPath, addr: addr, password: password}
	ep, err := resolveEndpoint(opts, timeout)
	var res reloadView
	if err == nil {
		err = ep.call("reload", map[string]any{"restart": restart}, &res)
	}
	if err != nil {
		// Fall back to SIGHUP when HTTP is unavailable: only works for local, same-machine operation.
		cfg, cerr := config.Load(confPath)
		if cerr != nil {
			return cerr
		}
		pid, perr := daemon.ReadPid(cfg.Daemon.PidFile)
		if perr != nil {
			return err
		}
		if serr := syscall.Kill(pid, syscall.SIGHUP); serr != nil {
			return fmt.Errorf("send SIGHUP: %w", serr)
		}
		fmt.Printf("sent SIGHUP to gosup (pid %d), check the logs for the reload result\n", pid)
		return nil
	}
	if jsonOut {
		return printJSON(res)
	}
	printReloadResult(res)
	return nil
}

// printReloadResult prints the reload result in human-readable form.
func printReloadResult(res reloadView) {
	printList := func(label string, items []string) {
		if len(items) > 0 {
			fmt.Printf("%-16s %s\n", label+":", strings.Join(items, ", "))
		}
	}
	if res.Unchanged {
		fmt.Println("config unchanged")
	} else {
		fmt.Println("config reloaded")
	}
	printList("added", res.Added)
	printList("removed", res.Removed)
	printList("updated", res.Updated)
	printList("started", res.Started)
	printList("stopped", res.Stopped)
	printList("restarted", res.Restarted)
	printList("pending", res.PendingRestart)
	printList("daemon updated", res.DaemonUpdated)
	printList("needs daemon restart", res.RequiresRestart)
	for _, w := range res.Warnings {
		fmt.Fprintf(os.Stderr, "warning: %s\n", w)
	}
	if len(res.PendingRestart) > 0 {
		fmt.Fprintln(os.Stderr, "hint: the startup settings of these programs were updated, but the running processes still use the old config; run `gosup reload --restart` or restart the programs to apply them")
	}
	if len(res.RequiresRestart) > 0 {
		fmt.Fprintln(os.Stderr, "hint: these [daemon] config keys take effect only after restarting the gosup daemon")
	}
}

func printStatus(p statusView) {
	pid := "-"
	if p.PID > 0 {
		pid = fmt.Sprintf("%d", p.PID)
	}
	fmt.Printf("id:           %s\n", p.ID)
	fmt.Printf("state:        %s\n", p.State)
	fmt.Printf("running:      %t\n", p.Running)
	fmt.Printf("pid:          %s\n", pid)
	if p.StartedAt != nil {
		fmt.Printf("started_at:   %s\n", p.StartedAt.Local().Format(time.RFC3339))
	}
	if p.Running {
		fmt.Printf("uptime:       %s\n", formatUptime(p))
	}
	fmt.Printf("restarts:     %d\n", p.Restarts)
	if p.ExitCode != nil {
		fmt.Printf("exit_code:    %d\n", *p.ExitCode)
	}
	if p.Signal != "" {
		fmt.Printf("signal:       %s\n", p.Signal)
	}
	if p.NextRestartAt != nil {
		fmt.Printf("next_restart: %s\n", p.NextRestartAt.Local().Format(time.RFC3339))
	}
	if p.LastError != "" {
		fmt.Printf("last_error:   %s\n", p.LastError)
	}
	if p.PendingReload {
		fmt.Printf("pending:      true (running processes still use the old config)\n")
	}
	fmt.Printf("command:      %s\n", p.Command)
}

// uptime is computed by subtracting started_at from the current wall clock.
//
// The server does not provide uptime: its CLOCK_MONOTONIC excludes device sleep,
// so on Android it is noticeably shorter than the duration the user perceives.
// Here we use the wall-clock difference, matching the device's "since boot"
// semantics.
func uptime(p statusView) time.Duration {
	if !p.Running || p.StartedAt == nil {
		return 0
	}
	d := time.Since(*p.StartedAt)
	if d < 0 {
		return 0 // client clock is behind the server
	}
	return d
}

func formatUptime(p statusView) string {
	if !p.Running || p.StartedAt == nil {
		return "-"
	}
	return uptime(p).Truncate(time.Second).String()
}

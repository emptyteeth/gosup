// Package rpc exposes the daemon's control interface over HTTP.
//
// Protocol:
//
//	POST /rpc    request-response. The request body is a JSON object, the response body is the same envelope.
//	GET  /events server push (SSE), used for log following and event notifications.
//	GET  /       one-line description.
//
// Request:
//
//	{"id": 1, "method": "status", "params": {"program": "web"}}
//
// Response:
//
//	{"id": 1, "ok": true, "result": {...}}
//	{"id": 1, "ok": false, "error": {"code": "not_found", "message": "..."}}
//
// Authentication: Authorization: Bearer <password>, or ?password=<password>.
// The SSE endpoint and the browser EventSource can only use the query parameter. Unauthenticated requests return 401.
package rpc

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"gosup/internal/config"
	"gosup/internal/logx"
	"gosup/internal/supervisor"
)

// Error codes.
const (
	CodeParse        = "parse_error"
	CodeUnauthorized = "unauthorized"
	CodeNotFound     = "not_found"
	CodeInvalid      = "invalid_params"
	CodeAlready      = "already_running"
	CodeNotRunning   = "not_running"
	CodeBusy         = "busy"
	CodeInternal     = "internal_error"
)

// maxBodyBytes limits the size of a single request.
const maxBodyBytes = 1 << 20

// Error is a business error carrying an error code.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newError(code string, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Request is a client request.
type Request struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Response is a server response.
type Response struct {
	ID     json.RawMessage `json:"id,omitempty"`
	OK     bool            `json:"ok"`
	Result any             `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

// programParams holds the parameters of methods that accept a program id.
type programParams struct {
	Program string `json:"program"`
}

// outputParams holds the parameters of program.output.
type outputParams struct {
	Program string `json:"program"`
	Stream  string `json:"stream"`
	// When Lines > 0, each stream returns only its last Lines lines; 0 means all.
	Lines int `json:"lines,omitempty"`
}

// OutputResult is the result of program.output.
type OutputResult struct {
	ID              string `json:"id"`
	Stdout          string `json:"stdout,omitempty"`
	Stderr          string `json:"stderr,omitempty"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	StderrTruncated bool   `json:"stderr_truncated"`
	StdoutTotal     int64  `json:"stdout_total_bytes"`
	StderrTotal     int64  `json:"stderr_total_bytes"`
}

// Event is the data body pushed over SSE. The SSE event: field corresponds to the field of the same name on Event.
type Event struct {
	Program string `json:"program,omitempty"`
	Stream  string `json:"stream,omitempty"`
	Data    string `json:"data,omitempty"`
	Reset   bool   `json:"reset,omitempty"`
	Lost    bool   `json:"lost,omitempty"`
	Message string `json:"message,omitempty"`
}

// Server is the HTTP server.
type Server struct {
	mgr *supervisor.Manager
	log *logx.Logger

	// connMu/connCount tracks SSE long-lived connections, bounded by max_connections.
	connMu    sync.Mutex
	connCount int

	// streamMu/streams records the cancel functions of active SSE streams, used by CloseStreams.
	streamMu sync.Mutex
	streams  map[context.Context]context.CancelFunc
}

// New creates the server.
func New(mgr *supervisor.Manager, log *logx.Logger) *Server {
	return &Server{
		mgr:     mgr,
		log:     log,
		streams: make(map[context.Context]context.CancelFunc),
	}
}

// Handler returns the http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", s.handleRPC)
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "gosup HTTP API:")
		fmt.Fprintln(w, "  POST /rpc     request/response JSON")
		fmt.Fprintln(w, "  GET  /events  server-sent events (log follow)")
	})
	return mux
}

// checkAuth verifies the password. A non-nil *Error means a 401 should be written.
func (s *Server) checkAuth(r *http.Request) *Error {
	d := s.mgr.DaemonConfig()
	pw, provided := passwordFromRequest(r)
	if !provided {
		if d.AllowNoAuth && d.Password == "" && d.PasswordSHA256 == "" {
			return nil
		}
		return newError(CodeUnauthorized, "missing password: use Authorization: Bearer or ?password=")
	}
	// When no password is configured, any supplied password must not be treated as "correct" — but if allow_no_auth is set, let it through.
	if d.AllowNoAuth && d.Password == "" && d.PasswordSHA256 == "" {
		return nil
	}
	if !validPassword(d, pw) {
		return newError(CodeUnauthorized, "wrong password")
	}
	return nil
}

// passwordFromRequest takes the password from the Authorization header or the query parameter.
func passwordFromRequest(r *http.Request) (string, bool) {
	if h := r.Header.Get("Authorization"); h != "" {
		if v, ok := strings.CutPrefix(h, "Bearer "); ok {
			return strings.TrimSpace(v), true
		}
	}
	if v, ok := r.URL.Query()["password"]; ok && len(v) > 0 {
		return v[0], true
	}
	return "", false
}

func validPassword(d config.Daemon, pw string) bool {
	if d.PasswordSHA256 != "" {
		got := config.SHA256Hex(pw)
		return subtle.ConstantTimeCompare([]byte(got), []byte(d.PasswordSHA256)) == 1
	}
	return subtle.ConstantTimeCompare([]byte(pw), []byte(d.Password)) == 1
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	// For cross-origin browser requests, the Authorization header triggers a preflight; it must be explicitly allowed.
	setCORS(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST, OPTIONS")
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	if e := s.checkAuth(r); e != nil {
		writeJSON(w, http.StatusUnauthorized, Response{OK: false, Error: e})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, Response{OK: false, Error: newError(CodeParse, "failed to read request: %v", err)})
		return
	}
	var req Request
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, Response{OK: false, Error: newError(CodeParse, "invalid JSON: %v", err)})
		return
	}

	resp := Response{ID: req.ID}
	result, cerr := s.call(r, req)
	if cerr != nil {
		resp.OK = false
		resp.Error = asError(cerr)
		writeJSON(w, statusFor(resp.Error), resp)
		return
	}
	resp.OK = true
	resp.Result = result
	writeJSON(w, http.StatusOK, resp)
}

// statusFor maps error codes to HTTP status codes, so callers can retry or distinguish by status code.
func statusFor(e *Error) int {
	switch e.Code {
	case CodeUnauthorized:
		return http.StatusUnauthorized
	case CodeNotFound:
		return http.StatusNotFound
	case CodeInvalid, CodeParse:
		return http.StatusBadRequest
	case CodeAlready, CodeBusy:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// setCORS allows any origin. gosup is meant for local use, and access control is carried by the password;
// no origin allowlist is implemented here, so a cross-origin page cannot call any method without the password.
func setCORS(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "600")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}

func asError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return newError(CodeInternal, "%v", err)
}

func (s *Server) call(r *http.Request, req Request) (any, error) {
	switch req.Method {
	case "ping":
		return map[string]any{"pong": true, "time": time.Now().UTC().Format(time.RFC3339Nano)}, nil
	case "list":
		return map[string]any{"programs": s.mgr.Statuses()}, nil
	case "status":
		p, err := programParam(req)
		if err != nil {
			return nil, err
		}
		st, err := s.mgr.Status(p)
		if err != nil {
			return nil, mapErr(err)
		}
		return st, nil
	case "start", "stop", "restart":
		p, err := programParam(req)
		if err != nil {
			return nil, err
		}
		if err := s.action(req.Method, p); err != nil {
			return nil, mapErr(err)
		}
		st, err := s.mgr.Status(p)
		if err != nil {
			return nil, mapErr(err)
		}
		return st, nil
	case "reload":
		return s.reload(req)
	case "output":
		return s.output(req)
	default:
		return nil, newError(CodeInvalid, "unknown method %q (available: ping, list, status, start, stop, restart, reload, output; for log following see GET /events)", req.Method)
	}
}

func (s *Server) action(method, program string) error {
	switch method {
	case "start":
		return s.mgr.StartProgram(program)
	case "stop":
		return s.mgr.StopProgram(program)
	default:
		return s.mgr.RestartProgram(program)
	}
}

// reload re-reads the config file and applies it.
func (s *Server) reload(req Request) (any, error) {
	var rp struct {
		Restart bool `json:"restart"`
	}
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &rp); err != nil {
			return nil, newError(CodeInvalid, "invalid parameters: %v", err)
		}
	}
	cfg, err := s.mgr.ReloadFromDisk()
	if err != nil {
		// Config errors are uniformly returned as invalid_params, so clients can distinguish "bad config" from "service failure".
		return nil, newError(CodeInvalid, "invalid config: %v", err)
	}
	res, err := s.mgr.Reload(cfg, rp.Restart)
	if err != nil {
		return nil, err
	}
	s.log.Infof("http", "config reloaded: +%d -%d ~%d", len(res.Added), len(res.Removed), len(res.Updated))
	return res, nil
}

func programParam(req Request) (string, error) {
	var p programParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return "", newError(CodeInvalid, "invalid parameters: %v", err)
		}
	}
	if strings.TrimSpace(p.Program) == "" {
		return "", newError(CodeInvalid, "missing parameter program")
	}
	return p.Program, nil
}

// output returns a snapshot of the output in the ring buffer. For live following, use GET /events.
func (s *Server) output(req Request) (any, error) {
	var p outputParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return nil, newError(CodeInvalid, "invalid parameters: %v", err)
		}
	}
	if strings.TrimSpace(p.Program) == "" {
		return nil, newError(CodeInvalid, "missing parameter program")
	}
	if p.Stream == "" {
		p.Stream = "both"
	}
	if p.Lines < 0 {
		return nil, newError(CodeInvalid, "lines must not be negative")
	}
	if !supervisor.ValidStream(p.Stream) {
		return nil, newError(CodeInvalid, "unknown output stream %q (must be one of stdout/stderr/both)", p.Stream)
	}
	out, err := s.mgr.Output(p.Program, p.Stream, p.Lines)
	if err != nil {
		return nil, mapErr(err)
	}
	return OutputResult{
		ID:              out.ID,
		Stdout:          string(out.Stdout),
		Stderr:          string(out.Stderr),
		StdoutTruncated: out.StdoutTruncated,
		StderrTruncated: out.StderrTruncated,
		StdoutTotal:     out.StdoutTotal,
		StderrTotal:     out.StderrTotal,
	}, nil
}

func mapErr(err error) error {
	// A business error with an error code is already final: pass it through directly.
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	switch {
	case errors.Is(err, supervisor.ErrNotFound):
		return newError(CodeNotFound, "%v", err)
	case errors.Is(err, supervisor.ErrAlreadyRunning):
		return newError(CodeAlready, "%v", err)
	case errors.Is(err, supervisor.ErrNotRunning):
		return newError(CodeNotRunning, "%v", err)
	case errors.Is(err, supervisor.ErrStopping):
		return newError(CodeBusy, "%v", err)
	}
	return err
}

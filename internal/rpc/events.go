package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"gosup/internal/output"
	"gosup/internal/supervisor"
)

// SSE (Server-Sent Events) event stream.
//
// One GET request corresponds to one output stream, which is reclaimed when the connection drops;
// no separate cancel endpoint is needed:
//
//	GET /events?program=web&stream=both&lines=200&password=...
//
// Event types:
//
//	event: output   data: {"program":"web","stream":"stdout","data":"..."}
//	event: reset    data: {"program":"web","stream":"stdout","message":"program restarted"}
//	event: lost     data: {"program":"web","stream":"stdout","message":"output dropped"}
//	event: ping     data: {"time":"..."}          once every 15 seconds, to keep the connection alive and detect drops
//	event: error    data: {"message":"..."}       the server then closes the connection
//
// Every event carries an id: (monotonically increasing); when a browser reconnects automatically it sends Last-Event-ID,
// and the server resumes from there (only valid within the lifetime of the same connection; after a process restart the numbering starts over).
const (
	ssePingInterval = 15 * time.Second
	ssePollInterval = 200 * time.Millisecond
	sseBatchSize    = 64 * 1024
	// sseWriteTimeout is the timeout for writing a single event. If the client stops reading,
	// the write fails and ends the connection, so the goroutine and the connection slot are not held for long.
	sseWriteTimeout = 10 * time.Second
)

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	// Set CORS first: all subsequent error responses need it too, otherwise the browser only sees a network error.
	setCORS(w, r)
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "use GET", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	if e := s.checkAuth(r); e != nil {
		writeJSON(w, http.StatusUnauthorized, Response{OK: false, Error: e})
		return
	}

	q := r.URL.Query()
	program := strings.TrimSpace(q.Get("program"))
	if program == "" {
		writeJSON(w, http.StatusBadRequest, Response{OK: false, Error: newError(CodeInvalid, "missing parameter program")})
		return
	}
	stream := q.Get("stream")
	if stream == "" {
		stream = "both"
	}
	if !supervisor.ValidStream(stream) {
		writeJSON(w, http.StatusBadRequest, Response{OK: false, Error: newError(CodeInvalid, "unknown output stream %q (must be one of stdout/stderr/both)", stream)})
		return
	}
	lines, err := strconv.Atoi(q.Get("lines"))
	if err != nil && q.Get("lines") != "" {
		writeJSON(w, http.StatusBadRequest, Response{OK: false, Error: newError(CodeInvalid, "lines must be an integer")})
		return
	}
	if lines < 0 {
		writeJSON(w, http.StatusBadRequest, Response{OK: false, Error: newError(CodeInvalid, "lines must not be negative")})
		return
	}

	// Connection limit: SSE connections are connections too, so they count toward the same limit.
	// A slot is counted only when actually occupied, and released only when it was occupied, so changing the
	// limit to 0 cannot wrongly decrement another connection's slot.
	d := s.mgr.DaemonConfig()
	counted := s.takeConnSlot(d.MaxConnections)
	if counted < 0 {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	if counted > 0 {
		defer s.releaseConnSlot()
	}

	stdout, stderr, err := s.mgr.OutputRings(program, stream)
	if err != nil {
		e := asError(mapErr(err))
		status := http.StatusNotFound
		if e.Code == CodeInvalid {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, Response{OK: false, Error: e})
		return
	}
	if stdout == nil && stderr == nil {
		writeJSON(w, http.StatusBadRequest, Response{OK: false, Error: newError(CodeInvalid, "missing output stream")})
		return
	}

	var cursors []*streamCursor
	var snapshot []eventFrame
	// The snapshot and the cursors are taken at the same instant: once the snapshot content has been
	// replayed, the cursors continue from the end of the snapshot, so there is no gap or duplicate between them.
	for _, pair := range []struct {
		name string
		ring *output.Ring
	}{{"stdout", stdout}, {"stderr", stderr}} {
		if pair.ring == nil {
			continue
		}
		snap := pair.ring.Snapshot()
		data, _ := output.TailLines(snap.Data, lines)
		if len(data) > 0 {
			snapshot = append(snapshot, eventFrame{
				Name: "output",
				Data: Event{Program: program, Stream: pair.name, Data: string(data)},
			})
		}
		cursors = append(cursors, &streamCursor{
			name:   pair.name,
			ring:   pair.ring,
			offset: snap.Total,
			gen:    snap.Generation,
		})
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Register this stream with the Server so shutdown cancels them before closing the HTTP server;
	// otherwise http.Server.Shutdown would wait until the timeout — it does not interrupt active handlers.
	ctx, unregister := s.trackStream(r.Context())
	defer unregister()

	streamer := newSSEStreamer(w, flusher)
	for _, f := range snapshot {
		if err := streamer.send(f); err != nil {
			return
		}
	}
	_ = ctx

	poll := time.NewTicker(ssePollInterval)
	defer poll.Stop()
	ping := time.NewTicker(ssePingInterval)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if err := streamer.send(eventFrame{Name: "ping", Data: Event{}}); err != nil {
				return
			}
		case <-poll.C:
			for _, c := range cursors {
				c.mu.Lock()
				data, next, gen, lost := c.ring.ReadSince(c.offset, c.gen, sseBatchSize)
				reset := gen != c.gen
				c.offset, c.gen = next, gen
				c.mu.Unlock()

				if reset {
					if err := streamer.send(eventFrame{Name: "reset", Data: Event{
						Program: program, Stream: c.name, Message: "program restarted; output buffer cleared",
					}}); err != nil {
						return
					}
				}
				if len(data) > 0 {
					if err := streamer.send(eventFrame{Name: "output", Data: Event{
						Program: program, Stream: c.name, Data: string(data), Lost: lost,
					}}); err != nil {
						return
					}
				}
			}
		}
	}
}

// streamCursor is the read position on an SSE connection.
type streamCursor struct {
	name   string
	ring   *output.Ring
	mu     sync.Mutex
	offset int64
	gen    uint64
}

// eventFrame is an event waiting to be written out.
type eventFrame struct {
	Name string
	ID   int64
	Data Event
}

// sseStreamer writes events in SSE format.
type sseStreamer struct {
	w       http.ResponseWriter
	flusher http.Flusher
	nextID  int64
}

func newSSEStreamer(w http.ResponseWriter, f http.Flusher) *sseStreamer {
	return &sseStreamer{w: w, flusher: f}
}

// send writes out one event. data must be single-line JSON (json.Marshal produces no newlines).
func (s *sseStreamer) send(frame eventFrame) error {
	b, err := json.Marshal(frame.Data)
	if err != nil {
		return err
	}
	s.nextID++
	// Reset the timeout on every write: a blocked write means the client is no longer reading.
	_ = http.NewResponseController(s.w).SetWriteDeadline(time.Now().Add(sseWriteTimeout))
	if _, err := fmt.Fprintf(s.w, "id: %d\nevent: %s\ndata: %s\n\n", s.nextID, frame.Name, b); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

// trackStream attaches an SSE stream's context to the Server and returns a cancellable context plus an unregister function.
func (s *Server) trackStream(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	s.streamMu.Lock()
	s.streams[ctx] = cancel
	s.streamMu.Unlock()
	return ctx, func() {
		s.streamMu.Lock()
		delete(s.streams, ctx)
		s.streamMu.Unlock()
		cancel()
	}
}

// CloseStreams cancels all active SSE streams. It should be called before shutting down the HTTP server.
func (s *Server) CloseStreams() {
	s.streamMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.streams))
	for _, cancel := range s.streams {
		cancels = append(cancels, cancel)
	}
	s.streamMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// takeConnSlot tries to take a connection slot.
//
// It returns 1 if the slot was taken (the caller releases it), 0 if no counting is needed (limit of 0 = unlimited),
// and -1 if the limit has been reached. Telling these three cases apart matters: the limit can change at runtime
// via reload, and if a slot were also released when nothing was counted, it would decrement another connection's slot.
func (s *Server) takeConnSlot(max int) int {
	if max <= 0 {
		return 0
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.connCount >= max {
		return -1
	}
	s.connCount++
	return 1
}

// releaseConnSlot releases a connection slot.
func (s *Server) releaseConnSlot() {
	s.connMu.Lock()
	if s.connCount > 0 {
		s.connCount--
	}
	s.connMu.Unlock()
}

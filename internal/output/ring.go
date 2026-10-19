// Package output provides a bounded in-memory ring buffer for holding child process stdout/stderr.
package output

import (
	"sync"
)

// Ring is a fixed-size ring byte buffer that supports concurrent writes/reads (keeping the newest content).
//
// Besides the content itself, it maintains two monotonic values:
//   - total: the cumulative number of bytes written, which never goes backwards and can serve as an absolute offset for incremental reads;
//   - gen: incremented on every Reset, indicating the buffer was cleared (the program restarted).
type Ring struct {
	mu    sync.Mutex
	buf   []byte
	size  int
	start int // start of the valid data written so far
	n     int // number of valid bytes
	trunc bool
	total int64
	gen   uint64
}

// Snapshot is the result of one full read.
type Snapshot struct {
	Data       []byte
	Truncated  bool  // whether older data was dropped for lack of capacity
	Total      int64 // cumulative bytes written
	Generation uint64
}

// NewRing creates a ring buffer retaining at most size bytes.
func NewRing(size int) *Ring {
	if size < 0 {
		size = 0
	}
	return &Ring{buf: make([]byte, size), size: size}
}

// Write implements io.Writer.
func (r *Ring) Write(p []byte) (int, error) {
	n := len(p)
	if r.size == 0 || n == 0 {
		r.mu.Lock()
		r.total += int64(n)
		if n > 0 {
			r.trunc = true
		}
		r.mu.Unlock()
		return len(p), nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.total += int64(n)
	if n > r.size {
		// A single write larger than capacity: keep only the last size bytes.
		copy(r.buf, p[n-r.size:])
		r.start = 0
		r.n = r.size
		r.trunc = true
		return len(p), nil
	}
	// Compute the writable position.
	end := (r.start + r.n) % r.size
	// Drop old data that exceeds capacity.
	overflow := r.n + n - r.size
	if overflow > 0 {
		r.start = (r.start + overflow) % r.size
		r.n -= overflow
		r.trunc = true
	}
	first := copy(r.buf[end:], p)
	if first < n {
		copy(r.buf, p[first:])
	}
	r.n += n
	return len(p), nil
}

// Snapshot returns the current content, whether truncation has occurred, and the cumulative bytes written.
func (r *Ring) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Snapshot{
		Data:       r.copyRange(0, r.n),
		Truncated:  r.trunc,
		Total:      r.total,
		Generation: r.gen,
	}
}

// ReadSince returns new data starting at the absolute offset offset.
//
// Return values:
//   - data: new content of at most max bytes (max <= 0 means unlimited);
//   - next: the offset the next read should use;
//   - newGen: the current generation;
//   - lost: the data before offset has been overwritten, so the content returned here is not contiguous.
//
// When gen differs from the current generation (the buffer was Reset), it returns all
// existing content in the buffer and sets next to the current total, so the caller can realign.
func (r *Ring) ReadSince(offset int64, gen uint64, max int) (data []byte, next int64, newGen uint64, lost bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gen != r.gen {
		data = r.copyRange(0, r.n)
		if max > 0 && len(data) > max {
			// Keep the newest content.
			data = data[len(data)-max:]
		}
		return data, r.total, r.gen, false
	}
	if offset > r.total {
		offset = r.total
	}
	lowest := r.total - int64(r.n)
	if offset < lowest {
		offset = lowest
		lost = true
	}
	want := r.total - offset
	if max > 0 && want > int64(max) {
		want = int64(max)
	}
	from := int(offset - lowest)
	return r.copyRange(from, from+int(want)), offset + want, r.gen, lost
}

// Resize changes the capacity, keeping the newest min(old content, new capacity) bytes.
func (r *Ring) Resize(size int) {
	if size < 0 {
		size = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if size == r.size {
		return
	}
	keep := r.n
	if keep > size {
		keep = size
		r.trunc = true
	}
	buf := make([]byte, size)
	if keep > 0 {
		copy(buf, r.copyRange(r.n-keep, r.n))
	}
	r.buf = buf
	r.size = size
	r.start = 0
	r.n = keep
}

// Len returns the current number of valid bytes.
func (r *Ring) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// Total returns the cumulative number of bytes written.
func (r *Ring) Total() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

// Generation returns the current generation.
func (r *Ring) Generation() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gen
}

// Reset clears the content (keeping capacity and cumulative counters) and starts a new generation.
func (r *Ring) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.start, r.n, r.trunc = 0, 0, false
	r.gen++
}

// copyRange copies the content of the logical window [from, to); the caller must hold the lock.
func (r *Ring) copyRange(from, to int) []byte {
	if from < 0 {
		from = 0
	}
	if to > r.n {
		to = r.n
	}
	if from >= to || r.size == 0 {
		return nil
	}
	ln := to - from
	out := make([]byte, ln)
	pos := (r.start + from) % r.size
	first := copy(out, r.buf[pos:min(pos+ln, r.size)])
	if first < ln {
		copy(out[first:], r.buf[:ln-first])
	}
	return out
}

// TailLines returns the last n lines of data, reporting whether earlier lines were dropped.
// When n <= 0 or there are not enough lines, it returns all content. A trailing newline does not count as an extra line.
func TailLines(data []byte, n int) ([]byte, bool) {
	if n <= 0 || len(data) == 0 {
		return data, false
	}
	end := len(data)
	if data[end-1] == '\n' {
		end--
	}
	count := 0
	for i := end; i > 0; i-- {
		if data[i-1] != '\n' {
			continue
		}
		count++
		if count == n {
			return data[i:], true
		}
	}
	return data, false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

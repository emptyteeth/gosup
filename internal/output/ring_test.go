package output

import (
	"bytes"
	"testing"
)

func TestRingKeepsLatest(t *testing.T) {
	r := NewRing(8)
	n, err := r.Write([]byte("abcdefghij"))
	if err != nil || n != 10 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	snap := r.Snapshot()
	if string(snap.Data) != "cdefghij" {
		t.Errorf("data = %q", snap.Data)
	}
	if !snap.Truncated {
		t.Error("should be marked as truncated")
	}
	if snap.Total != 10 {
		t.Errorf("total = %d", snap.Total)
	}
}

func TestRingWrap(t *testing.T) {
	r := NewRing(6)
	for _, s := range []string{"abc", "def", "ghi"} {
		if _, err := r.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	snap := r.Snapshot()
	if string(snap.Data) != "efghi" && string(snap.Data) != "defghi" {
		t.Errorf("data = %q", snap.Data)
	}
	if !snap.Truncated {
		t.Error("should be marked as truncated")
	}
}

func TestRingExact(t *testing.T) {
	r := NewRing(4)
	_, _ = r.Write([]byte("abcd"))
	snap := r.Snapshot()
	if string(snap.Data) != "abcd" || snap.Truncated {
		t.Errorf("data = %q trunc=%v", snap.Data, snap.Truncated)
	}
}

func TestRingLargeWrite(t *testing.T) {
	r := NewRing(4)
	big := bytes.Repeat([]byte("x"), 100)
	if _, err := r.Write(big); err != nil {
		t.Fatal(err)
	}
	if got := len(r.Snapshot().Data); got != 4 {
		t.Errorf("len = %d", got)
	}
}

func TestRingZeroSize(t *testing.T) {
	r := NewRing(0)
	if _, err := r.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	snap := r.Snapshot()
	if len(snap.Data) != 0 || !snap.Truncated || snap.Total != 3 {
		t.Errorf("data=%q trunc=%v total=%d", snap.Data, snap.Truncated, snap.Total)
	}
}

func TestReadSince(t *testing.T) {
	r := NewRing(10)
	_, _ = r.Write([]byte("hello"))
	data, next, gen, lost := r.ReadSince(0, r.Generation(), 0)
	if string(data) != "hello" || next != 5 || lost {
		t.Fatalf("data=%q next=%d gen=%d lost=%v", data, next, gen, lost)
	}
	_, _ = r.Write([]byte(" world"))
	data, next, gen, lost = r.ReadSince(next, gen, 0)
	if string(data) != " world" || next != 11 || lost {
		t.Fatalf("data=%q next=%d lost=%v", data, next, lost)
	}
	// Returns empty when there is no new data.
	data, _, _, _ = r.ReadSince(next, gen, 0)
	if len(data) != 0 {
		t.Errorf("data = %q", data)
	}
	// After exceeding capacity, data at the old offset is gone: the lowest available offset is now 11, so reading from 10 is flagged.
	_, _ = r.Write([]byte("0123456789"))
	data, _, _, lost = r.ReadSince(10, gen, 0)
	if !lost {
		t.Error("should report data loss")
	}
	if string(data) != "0123456789" {
		t.Errorf("data = %q", data)
	}
	// Reading from the lowest available offset is not considered lost.
	data, next, _, lost = r.ReadSince(11, gen, 0)
	if lost || string(data) != "0123456789" || next != 21 {
		t.Errorf("data=%q next=%d lost=%v", data, next, lost)
	}
}

func TestReadSinceReset(t *testing.T) {
	r := NewRing(10)
	_, _ = r.Write([]byte("old"))
	r.Reset()
	_, _ = r.Write([]byte("new"))
	// Read with the old generation (0): it should return the new content from the start.
	data, _, newGen, lost := r.ReadSince(3, 0, 0)
	if string(data) != "new" || newGen != 1 || lost {
		t.Fatalf("data=%q gen=%d lost=%v", data, newGen, lost)
	}
}

func TestReadSinceLimit(t *testing.T) {
	r := NewRing(100)
	_, _ = r.Write([]byte("0123456789"))
	data, next, _, _ := r.ReadSince(0, r.Generation(), 4)
	if string(data) != "0123" || next != 4 {
		t.Fatalf("data=%q next=%d", data, next)
	}
}

func TestResizeKeepsNewest(t *testing.T) {
	r := NewRing(10)
	_, _ = r.Write([]byte("0123456789"))
	r.Resize(4)
	snap := r.Snapshot()
	if string(snap.Data) != "6789" || !snap.Truncated {
		t.Errorf("data=%q trunc=%v", snap.Data, snap.Truncated)
	}
	r.Resize(20)
	_, _ = r.Write([]byte("ab"))
	if got := string(r.Snapshot().Data); got != "6789ab" {
		t.Errorf("data = %q", got)
	}
}

func TestTailLines(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
		cut  bool
	}{
		{"a\nb\nc\n", 2, "b\nc\n", true},
		{"a\nb\nc", 2, "b\nc", true},
		{"a\nb\n", 0, "a\nb\n", false},
		{"a\nb\n", 5, "a\nb\n", false},
		{"", 2, "", false},
		{"single", 1, "single", false},
	}
	for _, tc := range cases {
		got, cut := TailLines([]byte(tc.in), tc.n)
		if string(got) != tc.want || cut != tc.cut {
			t.Errorf("TailLines(%q, %d) = %q, %v", tc.in, tc.n, got, cut)
		}
	}
}

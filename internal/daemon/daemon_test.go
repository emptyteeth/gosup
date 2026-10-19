package daemon

import (
	"os"
	"path/filepath"
	"testing"
)

// After RedirectStdio, anything written to os.Stdout should land in the log file.
func TestRedirectStdio(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "out.log")

	// Save the original fd and restore it after the test to avoid polluting other tests.
	origStdout, err := os.OpenFile("/dev/stdout", os.O_WRONLY, 0)
	if err != nil {
		t.Skipf("cannot open /dev/stdout: %v", err)
	}
	defer origStdout.Close()
	orig := int(origStdout.Fd())

	logf, err := RedirectStdio(logPath)
	if err != nil {
		t.Fatalf("RedirectStdio: %v", err)
	}
	defer logf.Close()
	defer func() { _ = dup3(orig, 1) }()

	if _, err := os.Stdout.WriteString("redirected-line\n"); err != nil {
		t.Fatalf("write to stdout: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "redirected-line\n" {
		t.Errorf("log content = %q", data)
	}
}

// dup3 is available on both target architectures; the same fd returns success directly.
func TestDup3SameFD(t *testing.T) {
	if err := dup3(1, 1); err != nil {
		t.Errorf("dup3(1,1) = %v", err)
	}
}

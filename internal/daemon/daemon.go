// Package daemon implements daemonization on Linux without cgo.
//
// It uses the classic double fork:
//
//	gosup -d  ──fork──▶ watch child process (GOSUP_ROLE=watch) ──fork──▶ actual daemon (GOSUP_ROLE=daemon)
//
// The intermediate watch process only performs wait4 to reap the daemon, while the daemon
// calls setsid itself, detaching from the controlling terminal and the parent's session.
// The role is passed via an environment variable; any old value is cleared before adding the new one.
package daemon

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// envRole is the internal environment variable holding the role.
const envRole = "GOSUP_ROLE"

// Role returns the role of the current process.
type Role string

const (
	RoleNone   Role = ""
	RoleWatch  Role = "watch"
	RoleDaemon Role = "daemon"
)

// CurrentRole parses the role from the environment.
//
// The environment is used rather than command-line arguments: the daemon child must inherit
// the original command line (the user's arguments stay unchanged), and ForkExec replaces Env
// entirely, so clearing any variable of the same name before adding the new role is enough;
// unlike append, this avoids the ambiguity of "old value first, new value last".
func CurrentRole() Role {
	switch Role(os.Getenv(envRole)) {
	case RoleWatch:
		return RoleWatch
	case RoleDaemon:
		return RoleDaemon
	}
	return RoleNone
}

// cleanEnv returns a copy of the environment with the given variable removed.
func cleanEnv(env []string, key string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env))
	for _, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// Daemonize is called by the foreground process: it forks the intermediate process and returns immediately.
func Daemonize() error {
	if CurrentRole() != RoleNone {
		return errors.New("internal error: daemon subprocess must not daemonize again")
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer devNull.Close()

	pid, err := syscall.ForkExec(exe, os.Args, &syscall.ProcAttr{
		Env:   append(cleanEnv(os.Environ(), envRole), envRole+"="+string(RoleWatch)),
		Files: []uintptr{devNull.Fd(), devNull.Fd(), devNull.Fd()},
		Sys:   &syscall.SysProcAttr{Setsid: true},
	})
	if err != nil {
		return fmt.Errorf("fork failed: %w", err)
	}
	fmt.Fprintf(os.Stderr, "gosup: daemon started, intermediate process pid=%d\n", pid)
	return nil
}

// RunWatch is the main loop of the intermediate process: wait for the actual daemon to exit.
// This fully detaches the daemon from the original foreground process and leaves no zombie behind.
func RunWatch() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer devNull.Close()

	pid, err := syscall.ForkExec(exe, os.Args, &syscall.ProcAttr{
		Env:   append(cleanEnv(os.Environ(), envRole), envRole+"="+string(RoleDaemon)),
		Files: []uintptr{devNull.Fd(), devNull.Fd(), devNull.Fd()},
		Sys:   &syscall.SysProcAttr{Setsid: true},
	})
	if err != nil {
		return fmt.Errorf("failed to fork daemon: %w", err)
	}
	// Wait for the daemon to exit and reap it.
	var ws syscall.WaitStatus
	for {
		_, err := syscall.Wait4(pid, &ws, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		break
	}
	if ws.Signaled() {
		fmt.Fprintf(os.Stderr, "gosup daemon terminated by signal %v\n", ws.Signal())
	}
	return nil
}

// RedirectStdio redirects the current process's standard input and output to /dev/null and the log file.
func RedirectStdio(logFile string) (*os.File, error) {
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	logf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		devNull.Close()
		return nil, fmt.Errorf("failed to open log file %s: %w", logFile, err)
	}
	if err := dup3(int(devNull.Fd()), 0); err != nil {
		return nil, err
	}
	if err := dup3(int(logf.Fd()), 1); err != nil {
		return nil, err
	}
	if err := dup3(int(logf.Fd()), 2); err != nil {
		return nil, err
	}
	_ = devNull.Close()
	return logf, nil
}

// dup2 copies oldfd to newfd.
//
// dup3 (flags=0) is used instead of dup2: arm64 has no dup2 system call,
// whereas dup3 has standard library wrappers on both amd64 and arm64.
func dup3(oldfd, newfd int) error {
	if oldfd == newfd {
		return nil
	}
	return syscall.Dup3(oldfd, newfd, 0)
}

// PidLock uses flock to guarantee a single gosup instance per machine.
type PidLock struct {
	f *os.File
}

// Acquire takes the pid file lock; if an instance already exists, returns its pid.
func Acquire(path string) (*PidLock, int, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open pid file %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		b, _ := os.ReadFile(path)
		f.Close()
		pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
		if pid > 0 {
			return nil, pid, fmt.Errorf("gosup is already running (pid %d, pid file %s)", pid, path)
		}
		return nil, 0, fmt.Errorf("gosup is already running (pid file %s is locked)", path)
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, 0, err
	}
	if _, err := f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0); err != nil {
		f.Close()
		return nil, 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, 0, err
	}
	return &PidLock{f: f}, 0, nil
}

// Release unlocks and removes the pid file.
func (l *PidLock) Release(path string) {
	if l == nil || l.f == nil {
		return
	}
	_ = os.Remove(path)
	_ = l.f.Close()
	l.f = nil
}

// ReadPid reads the pid from the pid file.
func ReadPid(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("pid file %s has invalid contents", path)
	}
	return pid, nil
}

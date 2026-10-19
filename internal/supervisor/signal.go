package supervisor

import (
	"syscall"
)

// killGroup sends a signal to a process group. Does nothing when pgid is 0.
func killGroup(pgid int, sig syscall.Signal) error {
	if pgid <= 0 {
		return nil
	}
	// A negative pid means a process group; ESRCH means the group no longer
	// exists, ignored by the caller.
	return syscall.Kill(-pgid, sig)
}

var signalNameMap = map[syscall.Signal]string{
	syscall.SIGHUP:    "SIGHUP",
	syscall.SIGINT:    "SIGINT",
	syscall.SIGQUIT:   "SIGQUIT",
	syscall.SIGILL:    "SIGILL",
	syscall.SIGTRAP:   "SIGTRAP",
	syscall.SIGABRT:   "SIGABRT",
	syscall.SIGBUS:    "SIGBUS",
	syscall.SIGFPE:    "SIGFPE",
	syscall.SIGKILL:   "SIGKILL",
	syscall.SIGUSR1:   "SIGUSR1",
	syscall.SIGSEGV:   "SIGSEGV",
	syscall.SIGUSR2:   "SIGUSR2",
	syscall.SIGPIPE:   "SIGPIPE",
	syscall.SIGALRM:   "SIGALRM",
	syscall.SIGTERM:   "SIGTERM",
	syscall.SIGSTKFLT: "SIGSTKFLT",
	syscall.SIGCHLD:   "SIGCHLD",
	syscall.SIGCONT:   "SIGCONT",
	syscall.SIGSTOP:   "SIGSTOP",
	syscall.SIGTSTP:   "SIGTSTP",
	syscall.SIGTTIN:   "SIGTTIN",
	syscall.SIGTTOU:   "SIGTTOU",
	syscall.SIGURG:    "SIGURG",
	syscall.SIGXCPU:   "SIGXCPU",
	syscall.SIGXFSZ:   "SIGXFSZ",
	syscall.SIGVTALRM: "SIGVTALRM",
	syscall.SIGPROF:   "SIGPROF",
	syscall.SIGWINCH:  "SIGWINCH",
	syscall.SIGIO:     "SIGIO",
	syscall.SIGPWR:    "SIGPWR",
	syscall.SIGSYS:    "SIGSYS",
}

// SignalName returns the conventional name of a signal ("SIGTERM"), falling back
// to "SIG<n>". syscall.Signal.String prints "terminated" instead, which reads badly
// in logs and does not match the names used in status output.
func SignalName(sig syscall.Signal) string {
	if n, ok := signalNameMap[sig]; ok {
		return n
	}
	return "SIG" + itoa(int(sig))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

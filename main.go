// Command gosup is a process supervisor for Linux.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"gosup/internal/app"
	"gosup/internal/config"
	"gosup/internal/daemon"
	"gosup/internal/logx"
)

const usageText = `gosup - Linux process supervisor

Usage:
  gosup [-c <config.ini>] [options]
  gosup [-c <config.ini>] <command> [args]

Commands:
  run                 run in the foreground (default)
  start               daemonize and start in the background
  stop                stop a running daemon (SIGTERM)
  restart             stop then start in the background
  status              print daemon pid and whether it is running
  check               validate the configuration file and exit
  hash-password <pw>  print the sha256 hex digest for password_sha256
  reload              reload the configuration (SIGHUP) and print the result
  list                list programs (HTTP API)
  ps <id>             show one program's status (HTTP API)
  logs <id>           show captured stdout/stderr, optionally --lines / --follow
  start-program <id>  start one program (HTTP API)
  stop-program <id>   stop one program (HTTP API)
  restart-program <id> restart one program (HTTP API)

Options:
  -c, --config <file>   configuration file (default: gosup.ini)
  -d, --daemon          daemonize (same as "start")
  -f, --foreground      force foreground (in "logs", -f means --follow)
      --check           validate configuration and exit
      --verbose         log at debug level in the foreground
  -t, --timeout <dur>   timeout for local control and client calls (default: 15s)
      --addr <host:port> address for client commands (default: listen)
      --password <pw>    password for client commands (Bearer)
      --stream <name>    output stream for "logs": stdout, stderr or both
  -n, --lines <count>   only the last <count> lines for "logs" (0 = all)
      --follow          keep streaming new output for "logs" until Ctrl+C
      --restart         for "reload": restart programs whose config changed
      --json            print raw JSON for client commands
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gosup: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Intermediate process spawned by daemonizing: it only waits for the final daemon to exit.
	// The role is passed via an internal command-line argument; see internal/daemon.
	role := daemon.CurrentRole()
	if role == daemon.RoleWatch {
		return daemon.RunWatch()
	}
	isDaemonChild := role == daemon.RoleDaemon

	opts := &options{
		confPath: "gosup.ini",
		timeout:  15 * time.Second,
	}
	command, rest, err := parseArgs(os.Args[1:], opts)
	if err != nil {
		return err
	}
	// The start subcommand is equivalent to -d; -f/--foreground takes precedence (parseArgs already handled it).
	if command == "start" && !opts.foreground {
		opts.daemonize = true
	}
	// The daemon child reuses the original command line (e.g. "start"), so it must
	// explicitly switch to running in the foreground here; otherwise it would
	// daemonize again and fork forever.
	if isDaemonChild {
		command = "run"
		opts.daemonize = false
	}

	switch command {
	case "hash-password":
		if len(rest) != 1 {
			return fmt.Errorf("usage: gosup hash-password <password>")
		}
		fmt.Println(config.SHA256Hex(rest[0]))
		return nil
	case "check":
		_, err := config.Load(opts.confPath)
		if err != nil {
			return err
		}
		fmt.Printf("config valid: %s\n", abs(opts.confPath))
		return nil
	case "stop", "restart", "status":
		return runControl(command, opts.confPath, opts.timeout)
	case "reload":
		return runReload(opts.confPath, opts.addr, opts.password, opts.restart, opts.jsonOut, opts.timeout)
	case "list", "ps", "logs", "start-program", "stop-program", "restart-program":
		return runClient(command, rest, opts, opts.timeout)
	case "run", "start":
	default:
		return fmt.Errorf("unknown command %q\n\n%s", command, usageText)
	}

	cfg, err := config.Load(opts.confPath)
	if err != nil {
		return err
	}

	if opts.checkOnly {
		fmt.Printf("config valid: %s\n", abs(opts.confPath))
		return nil
	}

	// In the foreground an unwritable pid file is not fatal; in daemon mode the lock must be acquired.
	if opts.daemonize {
		if err := daemon.Daemonize(); err != nil {
			return err
		}
		return nil
	}

	return runServer(cfg, opts.verbose, isDaemonChild)
}

// runServer runs the main body. The daemon child redirects stdio and takes the pid file lock.
func runServer(cfg *config.Config, verbose, isDaemonChild bool) error {
	level := logx.ParseLevel(cfg.Daemon.LogLevel)
	if verbose {
		level = logx.Debug
	}

	var out *os.File = os.Stdout
	if isDaemonChild {
		logf, err := daemon.RedirectStdio(cfg.Daemon.LogFile)
		if err != nil {
			return err
		}
		defer logf.Close()
		out = logf
	}

	log := logx.New(out, level)

	// Single-instance guarantee: flock on the pid file.
	lock, _, err := daemon.Acquire(cfg.Daemon.PidFile)
	if err != nil {
		if isDaemonChild {
			return err
		}
		return fmt.Errorf("%w (change pid_file to run tests in parallel)", err)
	}
	defer lock.Release(cfg.Daemon.PidFile)

	d, err := app.New(cfg, log)
	if err != nil {
		return err
	}
	d.Manager().SetVerbose(verbose)
	return d.Run()
}

// runControl handles stop/restart/status (via the pid file and signals).
func runControl(command, confPath string, timeout time.Duration) error {
	cfg, err := config.Load(confPath)
	if err != nil {
		return err
	}
	pid, err := daemon.ReadPid(cfg.Daemon.PidFile)
	if command == "status" {
		if err != nil {
			fmt.Printf("gosup is not running (cannot read pid file %s)\n", cfg.Daemon.PidFile)
			return nil
		}
		if processAlive(pid) {
			fmt.Printf("gosup is running, pid=%d\n", pid)
			return nil
		}
		fmt.Printf("gosup is not running (pid %d from the pid file does not exist)\n", pid)
		return nil
	}
	running := err == nil && processAlive(pid)
	if !running {
		if command == "stop" {
			fmt.Println("gosup is not running")
			return nil
		}
		// restart while not running: equivalent to start.
		if err := daemon.Daemonize(); err != nil {
			return err
		}
		fmt.Println("gosup started")
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("send SIGTERM: %w", err)
	}
	// Wait for it to exit.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if processAlive(pid) {
		return fmt.Errorf("timed out waiting for gosup (pid %d) to exit", pid)
	}
	if command == "restart" {
		if err := daemon.Daemonize(); err != nil {
			return err
		}
		fmt.Println("gosup restarted")
		return nil
	}
	fmt.Println("gosup stopped")
	return nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

func abs(p string) string {
	a, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return a
}

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// options holds the command-line options.
type options struct {
	confPath   string
	daemonize  bool
	foreground bool
	verbose    bool
	checkOnly  bool
	timeout    time.Duration
	addr       string
	password   string
	stream     string
	lines      int
	follow     bool
	restart    bool
	jsonOut    bool
	// fShort is the raw value of -f: follow under logs, foreground under other commands.
	fShort bool
}

// parseArgs parses the command line.
//
// Unlike the standard flag package, options following the first positional
// argument (the subcommand) are still parsed, so both
// `gosup -c x.ini logs web --stream stderr` and `gosup logs --stream stderr -c x.ini`
// work. The returned rest holds the non-option arguments after the subcommand.
func parseArgs(argv []string, opts *options) (command string, rest []string, err error) {
	fs := flag.NewFlagSet("gosup", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usageText) }

	fs.StringVar(&opts.confPath, "c", "gosup.ini", "")
	fs.StringVar(&opts.confPath, "config", "gosup.ini", "")
	fs.BoolVar(&opts.daemonize, "d", false, "")
	fs.BoolVar(&opts.daemonize, "daemon", false, "")
	fs.BoolVar(&opts.fShort, "f", false, "")
	fs.BoolVar(&opts.foreground, "foreground", false, "")
	fs.BoolVar(&opts.verbose, "verbose", false, "")
	fs.BoolVar(&opts.checkOnly, "check", false, "")
	fs.DurationVar(&opts.timeout, "t", 15*time.Second, "")
	fs.DurationVar(&opts.timeout, "timeout", 15*time.Second, "")
	fs.StringVar(&opts.addr, "addr", "", "")
	fs.StringVar(&opts.password, "password", "", "")
	fs.StringVar(&opts.stream, "stream", "", "")
	fs.IntVar(&opts.lines, "lines", 0, "")
	fs.IntVar(&opts.lines, "n", 0, "")
	fs.BoolVar(&opts.follow, "follow", false, "")
	fs.BoolVar(&opts.restart, "restart", false, "")
	fs.BoolVar(&opts.jsonOut, "json", false, "")

	// Parse once before the subcommand to pick it out.
	if err := fs.Parse(argv); err != nil {
		return "", nil, err
	}
	args := fs.Args()
	command = "run"
	if len(args) > 0 {
		command = args[0]
		rest = args[1:]
	}

	// Parse again after the subcommand so options and positional arguments may
	// interleave, e.g. `gosup logs web --stream stderr`. The flag package stops
	// at the first positional argument, so separate options from positionals first.
	flagArgs, positionals := splitFlags(rest)
	fs2 := flag.NewFlagSet("gosup", flag.ContinueOnError)
	fs2.SetOutput(os.Stderr)
	fs2.Usage = func() {}
	fs2.StringVar(&opts.confPath, "c", opts.confPath, "")
	fs2.StringVar(&opts.confPath, "config", opts.confPath, "")
	fs2.BoolVar(&opts.daemonize, "d", opts.daemonize, "")
	fs2.BoolVar(&opts.daemonize, "daemon", opts.daemonize, "")
	fs2.BoolVar(&opts.fShort, "f", opts.fShort, "")
	fs2.BoolVar(&opts.foreground, "foreground", opts.foreground, "")
	fs2.BoolVar(&opts.verbose, "verbose", opts.verbose, "")
	fs2.BoolVar(&opts.checkOnly, "check", opts.checkOnly, "")
	fs2.DurationVar(&opts.timeout, "t", opts.timeout, "")
	fs2.DurationVar(&opts.timeout, "timeout", opts.timeout, "")
	fs2.StringVar(&opts.addr, "addr", opts.addr, "")
	fs2.StringVar(&opts.password, "password", opts.password, "")
	fs2.StringVar(&opts.stream, "stream", opts.stream, "")
	fs2.IntVar(&opts.lines, "lines", opts.lines, "")
	fs2.IntVar(&opts.lines, "n", opts.lines, "")
	fs2.BoolVar(&opts.follow, "follow", opts.follow, "")
	fs2.BoolVar(&opts.restart, "restart", opts.restart, "")
	fs2.BoolVar(&opts.jsonOut, "json", opts.jsonOut, "")
	if err := fs2.Parse(flagArgs); err != nil {
		return "", nil, err
	}
	rest = append(fs2.Args(), positionals...)
	// -f is both the short option for foreground and for follow: only treat it
	// as follow under the logs subcommand, since the flag package does not allow
	// the same name to be registered twice.
	if command == "logs" {
		opts.follow = opts.follow || opts.fShort
		opts.foreground = false
	} else if opts.fShort {
		opts.foreground = true
	}
	if opts.foreground {
		opts.daemonize = false
	}
	return command, rest, nil
}

// valueFlags lists the option names that take a value (without the leading '-').
var valueFlags = map[string]bool{
	"c": true, "config": true, "t": true, "timeout": true,
	"addr": true, "password": true, "stream": true,
	"lines": true, "n": true,
}

// splitFlags splits the arguments into options and positional arguments, preserving the order of each.
func splitFlags(args []string) (flags, positionals []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		if len(a) < 2 || a[0] != '-' {
			positionals = append(positionals, a)
			continue
		}
		flags = append(flags, a)
		name := strings.TrimLeft(a, "-")
		if strings.Contains(name, "=") {
			continue // --key=value carries its own value
		}
		if valueFlags[name] && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return flags, positionals
}

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "gosup.ini")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const minimalConfig = `
[daemon]
listen = 127.0.0.1:9100
password = secret

[program:web]
command = /bin/sh -c 'echo hi; sleep 100'
workdir = /tmp
restart = on-failure
restart_delay = 2
restart_max = 3
`

func TestLoadMinimal(t *testing.T) {
	cfg, err := Load(writeTemp(t, minimalConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Daemon.Listen != "127.0.0.1:9100" {
		t.Errorf("listen = %q", cfg.Daemon.Listen)
	}
	if cfg.Daemon.Password != "secret" {
		t.Errorf("password = %q", cfg.Daemon.Password)
	}
	if len(cfg.Programs) != 1 {
		t.Fatalf("programs = %d", len(cfg.Programs))
	}
	p := cfg.Programs[0]
	if p.ID != "web" {
		t.Errorf("id = %q", p.ID)
	}
	want := []string{"/bin/sh", "-c", "echo hi; sleep 100"}
	if !reflect.DeepEqual(p.Argv, want) {
		t.Errorf("argv = %q, want %q", p.Argv, want)
	}
	if p.Restart != RestartOnFailure {
		t.Errorf("restart = %q", p.Restart)
	}
	if p.RestartGap != 2*time.Second {
		t.Errorf("restart_delay = %v", p.RestartGap)
	}
	if p.RestartNum != 3 {
		t.Errorf("restart_max = %d", p.RestartNum)
	}
	if !p.Enabled {
		t.Error("enabled should default to true")
	}
	if p.Stdout != "ring" || p.Stderr != "ring" {
		t.Errorf("default output should be ring, got %q/%q", p.Stdout, p.Stderr)
	}
}

func TestLoadRejects(t *testing.T) {
	base := minimalConfig
	cases := []struct {
		name    string
		content string
	}{
		{"missing daemon", "[program:a]\ncommand = /bin/true\n"},
		{"missing password", "[daemon]\nlisten = 127.0.0.1:1\n\n[program:a]\ncommand = /bin/true\n"},
		{"unknown section", base + "\n[foo]\nbar = 1\n"},
		{"unknown key", base + "\n[program:x]\ncommand = /bin/true\nbogus = 1\n"},
		{"missing command", base + "\n[program:x]\nworkdir = /tmp\n"},
		{"bad restart", base + "\n[program:x]\ncommand = /bin/true\nrestart = sometimes\n"},
		{"bad bool", base + "\n[program:x]\ncommand = /bin/true\nenabled = maybe\n"},
		{"bad duration", base + "\n[program:x]\ncommand = /bin/true\nrestart_delay = abc\n"},
		{"bad cap", base + "\n[program:x]\ncommand = /bin/true\ncapabilities = not_a_cap\n"},
		{"bad signal", base + "\n[program:x]\ncommand = /bin/true\nstop_signal = SIGBOGUS\n"},
		{"dup id", base + "\n[program:web]\ncommand = /bin/true\n"},
		{"unknown daemon key", base + "\n[program:x]\ncommand = /bin/true\nlisen = typo\n"},
		{"bad stream", base + "\n[program:x]\ncommand = /bin/true\nstdout = relative/path\n"},
		{"bad env", base + "\n[program:x]\ncommand = /bin/true\nenv = NOEQUALS\n"},
		{"unclosed quote", base + "\n[program:x]\ncommand = /bin/true \"oops\n"},
		{"group without user", base + "\n[program:x]\ncommand = /bin/true\ngroup = root\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(writeTemp(t, tc.content)); err == nil {
				t.Fatal("expected an error, but the config was accepted")
			}
		})
	}
}

func TestUnknownKeyInDaemon(t *testing.T) {
	content := minimalConfig + "log_level = info\nlisen = typo\n"
	if _, err := Load(writeTemp(t, content)); err == nil {
		t.Fatal("a typo in [daemon] should be rejected")
	}
}

func TestShellMode(t *testing.T) {
	content := `
[daemon]
password = x

[program:a]
command = echo "a b" | tr a-z A-Z
shell = true
`
	cfg, err := Load(writeTemp(t, content))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/bin/sh", "-c", `echo "a b" | tr a-z A-Z`}
	if !reflect.DeepEqual(cfg.Programs[0].Argv, want) {
		t.Errorf("argv = %q", cfg.Programs[0].Argv)
	}
}

func TestPasswordSHA256(t *testing.T) {
	content := `
[daemon]
password_sha256 = ` + SHA256Hex("hunter2") + `

[program:a]
command = /bin/true
`
	cfg, err := Load(writeTemp(t, content))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Daemon.Password != "" {
		t.Errorf("plaintext password should not be kept after hashing the config")
	}
}

func TestCaps(t *testing.T) {
	caps, err := parseCaps([]string{"net_bind_service", "CAP_SYS_ADMIN", "5", "cap_net_raw"})
	if err != nil {
		t.Fatal(err)
	}
	want := []uintptr{10, 21, 5, 13}
	if !reflect.DeepEqual(caps, want) {
		t.Errorf("caps = %v, want %v", caps, want)
	}
	if _, err := parseCaps([]string{"bogus"}); err == nil {
		t.Error("an unknown capability should error")
	}
	if _, err := parseCaps([]string{"64"}); err == nil {
		t.Error("an out-of-range capability should error")
	}
}

func TestSplitList(t *testing.T) {
	got := SplitList(`A=1, B=2 , "C=x,y"`)
	want := []string{"A=1", "B=2", "C=x,y"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SplitList = %q, want %q", got, want)
	}
}

func TestParseDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"10":    10 * time.Second,
		"1.5":   1500 * time.Millisecond,
		"500ms": 500 * time.Millisecond,
		"2m":    2 * time.Minute,
	}
	for in, want := range cases {
		got, err := ParseDuration(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got != want {
			t.Errorf("%q = %v, want %v", in, got, want)
		}
	}
}

func TestSplitCommand(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`/bin/echo hello world`, []string{"/bin/echo", "hello", "world"}},
		{`/bin/echo "hello world"`, []string{"/bin/echo", "hello world"}},
		{`/bin/echo 'a b' c`, []string{"/bin/echo", "a b", "c"}},
		{`/bin/echo ""`, []string{"/bin/echo", ""}},
		{`/bin/echo a\ b`, []string{"/bin/echo", "a b"}},
	}
	for _, tc := range cases {
		got, err := SplitCommand(tc.in)
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%q = %q, want %q", tc.in, got, tc.want)
		}
	}
	if _, err := SplitCommand(`/bin/echo "unclosed`); err == nil {
		t.Error("an unclosed quote should error")
	}
}

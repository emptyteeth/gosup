package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTempFile writes a temporary file and returns its path.
func writeTempFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// On Android an app uid is not in passwd, so a plain number must be accepted directly.
func TestUserIDNumeric(t *testing.T) {
	r := &idResolver{passwdFiles: []string{"/nonexistent"}, groupFiles: []string{"/nonexistent"}}
	uid, gid, supp, err := r.userID("3005")
	if err != nil {
		t.Fatalf("numeric uid should be accepted: %v", err)
	}
	if uid != 3005 || gid != 3005 {
		t.Errorf("uid=%d gid=%d", uid, gid)
	}
	if len(supp) != 0 {
		t.Errorf("supp=%v", supp)
	}
}

func TestUserIDNumericNegative(t *testing.T) {
	r := defaultResolver()
	if _, _, _, err := r.userID("-1"); err == nil {
		t.Error("a negative uid should error")
	}
}

// When a name is in neither os/user nor passwd, the error message must give an actionable hint.
func TestUserIDUnknownMessage(t *testing.T) {
	r := &idResolver{passwdFiles: []string{"/nonexistent"}, groupFiles: []string{"/nonexistent"}}
	_, _, _, err := r.userID("glider-user")
	if err == nil {
		t.Fatal("an unknown user should error")
	}
	if !contains(err.Error(), "user = 3005") {
		t.Errorf("error message is missing a hint: %v", err)
	}
}

// Falls back to an Android /system/etc/passwd-style file.
func TestUserIDFallbackPasswd(t *testing.T) {
	dir := t.TempDir()
	passwd := writeTempFile(t, dir, "passwd", `root:x:0:0:root:/root:/bin/sh
system:x:1000:1000::/:/system/bin/sh
`)
	r := &idResolver{
		passwdFiles: []string{"/nonexistent", passwd},
		groupFiles:  []string{"/nonexistent"},
	}
	uid, gid, _, err := r.userID("system")
	if err != nil {
		t.Fatalf("should resolve from the fallback file: %v", err)
	}
	if uid != 1000 || gid != 1000 {
		t.Errorf("uid=%d gid=%d", uid, gid)
	}
}

func TestGroupIDNumericAndFallback(t *testing.T) {
	dir := t.TempDir()
	group := writeTempFile(t, dir, "group", `root:x:0:
inet:x:3003:
net_raw:x:3004:
`)
	r := &idResolver{passwdFiles: []string{"/nonexistent"}, groupFiles: []string{"/nonexistent", group}}

	if gid, err := r.groupID("3003"); err != nil || gid != 3003 {
		t.Errorf("numeric gid: %d %v", gid, err)
	}
	if gid, err := r.groupID("net_raw"); err != nil || gid != 3004 {
		t.Errorf("name gid: %d %v", gid, err)
	}
	if _, err := r.groupID("missing"); err == nil {
		t.Error("an unknown group should error")
	}
}

// Android scenario: user = 3005 with an explicit group, all numeric.
func TestResolveIdentityAndroidStyle(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root is required to test switching to another uid")
	}
	p := &Program{User: "3005", Group: "3005"}
	if err := resolveIdentity(p); err != nil {
		t.Fatalf("Android-style config should pass: %v", err)
	}
	if !p.SetCred || p.UID != 3005 || p.GID != 3005 {
		t.Errorf("SetCred=%v uid=%d gid=%d", p.SetCred, p.UID, p.GID)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// With only a numeric user configured, gid and supplementary groups should stay equal to the uid itself, without pulling in extra host groups.
func TestResolveIdentityNumericNoExtraGroups(t *testing.T) {
	r := &idResolver{passwdFiles: []string{"/nonexistent"}, groupFiles: []string{"/nonexistent"}}
	uid, gid, supp, err := r.userID("3005")
	if err != nil {
		t.Fatal(err)
	}
	if uid != 3005 || gid != 3005 || len(supp) != 0 {
		t.Errorf("uid=%d gid=%d supp=%v", uid, gid, supp)
	}
}

// When name resolution succeeds, supplementary groups come from os/user or are empty, and must not contain duplicate gids.
func TestResolveIdentityByNameDedup(t *testing.T) {
	p := &Program{User: "nobody"}
	if err := resolveIdentity(p); err != nil {
		t.Skipf("nobody user is not available on this machine: %v", err)
	}
	seen := map[uint32]bool{}
	for _, g := range p.SuppGIDs {
		if seen[g] {
			t.Errorf("duplicate supplementary group: %v", p.SuppGIDs)
		}
		seen[g] = true
	}
}

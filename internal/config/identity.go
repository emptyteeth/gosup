package config

import (
	"bufio"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
)

// idResolver resolves user/group configuration into numeric ids.
//
// On Android os/user cannot be relied upon: with GOOS=android Go forces a stub
// implementation that only reads /etc/passwd and /etc/group, and those files do not
// exist or are empty on Android. App uids (e.g. 3005) are not in them either -- they are
// assigned dynamically by the package manager. The strategy here is therefore:
//   - plain numbers are used directly as ids, without a lookup;
//   - names are first tried through os/user, and on failure looked up in
//     /system/etc/passwd (Android's read-only system user table);
//   - supplementary groups are left empty by default on Android because inferring them
//     from Groups in /proc/self/status is unreliable; specify numeric gids explicitly
//     with supplementary_groups when needed.
type idResolver struct {
	// passwdFiles/groupFiles are candidate files, tried in order.
	passwdFiles []string
	groupFiles  []string
}

func defaultResolver() *idResolver {
	return &idResolver{
		passwdFiles: []string{"/etc/passwd", "/system/etc/passwd"},
		groupFiles:  []string{"/etc/group", "/system/etc/group"},
	}
}

// userID resolves a user and returns the uid and primary gid.
//
// A plain numeric input is accepted directly (the only practical way to configure an app uid on Android).
func (r *idResolver) userID(spec string) (uid, gid int, supp []uint32, err error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return 0, 0, nil, fmt.Errorf("user name is empty")
	}
	if n, convErr := strconv.Atoi(spec); convErr == nil {
		if n < 0 {
			return 0, 0, nil, fmt.Errorf("invalid user id %q", spec)
		}
		// Numeric: use it directly. When no group is configured explicitly, gid equals uid,
		// matching Android's convention that an app uid == its primary gid.
		return n, n, nil, nil
	}

	// Try os/user first (the NSS path on desktop Linux).
	if u, lookupErr := user.Lookup(spec); lookupErr == nil {
		uid, gid, supp, err = parseUserEntry(u.Uid, u.Gid)
		if err == nil {
			if ids, gerr := u.GroupIds(); gerr == nil {
				for _, id := range ids {
					if n, e := strconv.Atoi(id); e == nil && n >= 0 {
						supp = append(supp, uint32(n))
					}
				}
			}
			return uid, gid, supp, nil
		}
	}

	// Fall back to the plain-text tables (Android's /system/etc/passwd).
	for _, path := range r.passwdFiles {
		entry, found := findPasswdEntry(path, spec)
		if !found {
			continue
		}
		return parseUserEntry(entry.uid, entry.gid)
	}
	return 0, 0, nil, fmt.Errorf("failed to find user %q: it is neither a numeric id nor present in %s (on Android, write the uid directly, e.g. user = 3005)",
		spec, strings.Join(r.passwdFiles, " or "))
}

func parseUserEntry(uidStr, gidStr string) (int, int, []uint32, error) {
	uid, err := strconv.Atoi(uidStr)
	if err != nil || uid < 0 {
		return 0, 0, nil, fmt.Errorf("invalid uid %q", uidStr)
	}
	gid, err := strconv.Atoi(gidStr)
	if err != nil || gid < 0 {
		return 0, 0, nil, fmt.Errorf("invalid gid %q", gidStr)
	}
	return uid, gid, nil, nil
}

// groupID resolves a group.
func (r *idResolver) groupID(spec string) (int, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return 0, fmt.Errorf("group is empty")
	}
	if n, convErr := strconv.Atoi(spec); convErr == nil {
		if n < 0 {
			return 0, fmt.Errorf("invalid group id %q", spec)
		}
		return n, nil
	}
	if g, err := user.LookupGroup(spec); err == nil {
		if n, e := strconv.Atoi(g.Gid); e == nil && n >= 0 {
			return n, nil
		}
	}
	for _, path := range r.groupFiles {
		if gid, found := findGroupEntry(path, spec); found {
			return gid, nil
		}
	}
	return 0, fmt.Errorf("failed to find group %q: it is neither a numeric id nor present in %s (on Android, write the gid directly)",
		spec, strings.Join(r.groupFiles, " or "))
}

// passwdEntry is one /etc/passwd-style line.
type passwdEntry struct {
	name, uid, gid string
}

// findPasswdEntry looks up a user by name or uid in the file.
func findPasswdEntry(path, spec string) (passwdEntry, bool) {
	f, err := os.Open(path)
	if err != nil {
		return passwdEntry{}, false
	}
	defer f.Close()

	var found passwdEntry
	ok := false
	forEachLine(f, func(line string) bool {
		if line == "" || strings.HasPrefix(line, "#") {
			return true
		}
		parts := strings.Split(line, ":")
		if len(parts) < 4 {
			return true
		}
		if parts[0] == spec || parts[2] == spec {
			found = passwdEntry{name: parts[0], uid: parts[2], gid: parts[3]}
			ok = true
			return false
		}
		return true
	})
	return found, ok
}

// findGroupEntry looks up a group by name or gid in the file.
func findGroupEntry(path, spec string) (int, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()

	gid := -1
	found := false
	forEachLine(f, func(line string) bool {
		if line == "" || strings.HasPrefix(line, "#") {
			return true
		}
		parts := strings.Split(line, ":")
		if len(parts) < 3 {
			return true
		}
		if parts[0] == spec || parts[2] == spec {
			n, err := strconv.Atoi(parts[2])
			if err != nil || n < 0 {
				return true
			}
			gid = n
			found = true
			return false
		}
		return true
	})
	return gid, found
}

// forEachLine calls fn for each line, stopping when fn returns false.
func forEachLine(f *os.File, fn func(line string) bool) {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4096), 1<<20)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if !fn(line) {
			return
		}
	}
}

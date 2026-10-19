# gosup

A small Linux process supervisor written in Go. It runs in daemon mode, reads an
INI file, keeps the configured programs running, and exposes an HTTP control API
with server-sent events for log streaming.

- Double-fork daemonization with an intermediate watcher process, so the daemon
  fully detaches from the launching shell and leaves no zombie behind.
- Per-program process groups (`setsid`), so stop/restart cleans up children too.
- Run-as user/group, supplementary groups, and Linux **ambient capabilities**.
- Restart policies with exponential backoff and a crash-loop limit.
- `stdout`/`stderr` captured in per-program ring buffers (and optionally to a file).
- HTTP API (`POST /rpc`) plus an SSE log stream (`GET /events`), and a built-in
  client (`gosup list|ps|logs|...`) that needs nothing but the stdlib.
- `flock` on the pid file, so only one daemon instance can run per pid file.

## Requirements

- Linux (uses `setsid`, process groups, `flock`, `CAP_*`, `SYS_DUP3`).
- Go 1.23+ to build. The resulting binary has no runtime dependencies.

## Build

```sh
make build          # CGO_ENABLED=0 go build -trimpath -ldflags='-s -w'
# or
CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o gosup .
```

`make build-all` produces `gosup-amd64` and `gosup-arm64`.

```sh
make build ARCH=arm64          # cross-compile a single architecture
CGO_ENABLED=0 GOARCH=arm64 go build -o gosup .
```

## Quick start

```sh
cp gosup.example.ini /etc/gosup.ini
$EDITOR /etc/gosup.ini           # at least set `password` and `pid_file`

gosup -c /etc/gosup.ini check    # validate the configuration
gosup -c /etc/gosup.ini start    # daemonize
gosup -c /etc/gosup.ini list
gosup -c /etc/gosup.ini logs web --stream stderr
gosup -c /etc/gosup.ini stop
```

Run in the foreground (useful while debugging):

```sh
gosup -c /etc/gosup.ini -f --verbose run
```

## Commands

| Command | Description |
| --- | --- |
| `run` | Run in the foreground (default). |
| `start` | Daemonize and run in the background. Same as `-d`. |
| `stop` | Send `SIGTERM` to the daemon and wait for it to exit. |
| `restart` | Stop the daemon if running, then start it again. |
| `status` | Show the daemon pid (from `pid_file`). |
| `check` | Validate the configuration and exit. |
| `hash-password <pw>` | Print the SHA-256 hex digest used by `password_sha256`. |
| `list` | List managed programs. |
| `ps <id>` | Show one program's state, pid, uptime, restarts, exit status. |
| `logs <id>` | Dump captured stdout/stderr. `--stream stdout\|stderr\|both`, `--lines N`, `--follow`. |
| `reload` | Re-read the configuration and apply it. `--restart` also restarts changed programs. |
| `start-program <id>` | Start one program. |
| `stop-program <id>` | Stop one program. |
| `restart-program <id>` | Restart one program. |

Global options:

```
-c, --config <file>      configuration file (default: gosup.ini)
-d, --daemon             daemonize (same as "start")
-f, --foreground         force foreground even with "start"
    --check              validate configuration and exit
    --verbose            debug logging in the foreground
-t, --timeout <dur>      timeout for stop/restart waiting and client calls (15s)
    --addr <host:port>   API address for client commands (default: listen)
    --password <pw>      password for client commands
    --stream <name>      stdout, stderr or both (default: both)
-n, --lines <count>      for "logs": only the last <count> lines (0 = all)
    --follow             for "logs": keep printing new output until Ctrl+C
    --restart            for "reload": restart programs whose config changed
    --json               print raw JSON for client commands
```

`-f` means `--follow` for `logs` and `--foreground` everywhere else.

Options may appear before or after the subcommand.

## Configuration

The file has one `[daemon]` section and any number of `[program:<id>]` sections.
Keys are case-insensitive. Unknown sections, unknown keys, and invalid values are
rejected by `check` and at startup.

### INI value rules

- `;` starts a comment only at the beginning of a line. Inline comments are
  **not** stripped, so `command = sh -c 'a; b'` is safe.
- Double-quote the value if you need leading/trailing spaces preserved:
  `password = "  spaced  "`.
- Lists (`env`, `capabilities`, `supplementary_groups`) are
  comma-separated; an element containing a comma can be double-quoted.
- Durations: a bare number means seconds (`10`, `0.5`); `500ms`, `2m`, `1h` are
  also accepted.

### `[daemon]`

| Key | Default | Description |
| --- | --- | --- |
| `listen` | `127.0.0.1:9090` | HTTP listen address. |
| `tls_cert` / `tls_key` | – | Both must be set to serve `https://`. |
| `password` | – | Access password (plaintext). |
| `password_sha256` | – | Access password as SHA-256 hex; takes precedence over `password`. |
| `password_file` | – | Read the password from a file (trailing whitespace trimmed). |
| `allow_no_auth` | `false` | Allow starting without any password. |
| `pid_file` | `/run/gosup.pid` | Pid file; also the `flock` target for single-instance. |
| `log_file` | `/var/log/gosup.log` | Where the daemon redirects stdout/stderr (appended; no rotation — see Known limitations). |
| `log_level` | `info` | `debug`, `info`, `warn`, `error`. |
| `state_dir` | `/run/gosup` | Per-program `<id>.json` pid/start-time records. |
| `stop_timeout` | `10` | Default per-program stop timeout. |
| `shutdown_timeout` | `30` | Total budget for stopping all programs when the daemon exits (they stop in parallel). Should be ≥ the largest `stop_timeout`. |
| `output_buffer_bytes` | `262144` | Ring buffer size per stream per program (min 4096). |
| `max_connections` | `16` | Concurrent SSE streams; extra requests get HTTP 503 (`0` = unlimited). |

At least one of `password`, `password_sha256`, or `allow_no_auth = true` is
required. Binding to a non-loopback address without authentication is a **bad
idea**; the control API can start and stop processes.

### `[program:<id>]`

| Key | Default | Description |
| --- | --- | --- |
| `command` | required | Executable and arguments, or a shell command with `shell = true`. |
| `shell` | `false` | Run `command` via `/bin/sh -c` (needed for pipes, redirections, `;`). |
| `workdir` | inherited | Working directory. Must exist. |
| `enabled` | `true` | Start automatically when the daemon starts. |
| `restart` | `always` | `always`, `on-failure`, `never`. |
| `restart_delay` | `1` | Initial delay before restarting (doubles on each failure). |
| `restart_delay_max` | `30` | Backoff cap. |
| `restart_max` | `0` | Give up after this many consecutive failures; `0` = unlimited. |
| `stable_after` | `60` | Running this long resets the failure counter. |
| `user` / `group` | – | Run as this user/group. Requires gosup to run as root (unless it is the current user). Accepts a name or a numeric id — see *Run-as identity on Android*. |
| `supplementary_groups` | – | Extra groups (names or numeric ids). |
| `capabilities` | – | Ambient capabilities, e.g. `net_bind_service, net_raw`. See below. |
| `stop_signal` | `SIGTERM` | Signal sent to the process group on stop. |
| `stop_timeout` | daemon's | Wait this long, then `SIGKILL`. Applied per process from the config it was started with (see below). |
| `stdout` / `stderr` | `ring` | `ring` (memory only), `discard`, or an absolute file path (append). |
| `stdin` | `/dev/null` | `/dev/null` or an absolute file path. |
| `env` | – | `KEY=value` entries, overriding the inherited environment. |

#### `command` parsing

By default `command` is split with shell-like quoting but **without** any
expansion:

```ini
command = /usr/local/bin/myapp --flag "value with spaces" 'single quoted'
```

Single quotes keep everything literal, double quotes support `\"` and `\\`, and a
backslash escapes the next character. To use pipes, redirections, variable
expansion, or `;`, set `shell = true`:

```ini
command = /bin/sh -c '...'   ; explicit, no shell option needed
# or
command = exec 2>&1; echo hi
shell = true
```

#### Run-as identity on Android

`user` and `group` accept either a name or a numeric id, and a bare number is used
**without any lookup**:

```ini
[program:myapp]
command = /usr/local/bin/myapp
user = 3005
group = 3005
```

Resolution order for `user`:

1. a bare number is taken as the uid, and the primary gid defaults to the same
   number (matching Android's uid == primary gid convention);
2. otherwise `os/user` (the NSS path on desktop Linux);
3. otherwise `/etc/passwd`, then `/system/etc/passwd` (the read-only system
   table on Android).

`group` and `supplementary_groups` follow the same order, falling back to
`/etc/group` and `/system/etc/group`. On Android, groups are usually not listed
there either, so write gids numerically.

Supplementary groups are not inferred on Android (the platform does not expose
them in the group files); list them explicitly if the program needs them.

#### Capabilities

`capabilities` is passed to `execve` as ambient capabilities
(`SysProcAttr.AmbientCaps`). Go sets them in `permitted` and `inheritable` and
raises them as ambient before `exec`, so they survive changing to `user`/`group`
and are inherited by the program (and its children). The daemon must run as root.

After the program exits, `CapEff` in `/proc/self/status` shows only those
capabilities, e.g. `net_bind_service, net_raw` → `CapEff: 0000000000002400`.

Accepted spellings: `net_bind_service`, `CAP_NET_BIND_SERVICE`, `10`, `cap_10`,
and `all` (0–40).

#### Startup failures

A process that fails to start (missing binary, missing working directory,
unopenable output file) is handled like a crash: with `restart = always` or
`on-failure` it retries with the same exponential backoff, so a service whose
dependency becomes available later (a late mount, a file that gets created)
starts on its own. With `restart = never` it stays stopped and `last_error`
explains why. `restart_max` bounds the retries as usual. A manual `start`
resets the failure counter.

`stop`/`restart` issued from the CLI also reset it, and `start` while the
program is waiting to restart runs it immediately.

#### Stop / cleanup semantics

Each program is started with `setsid`, so its pid is also its session and process
group id. Stopping sends `stop_signal` to the whole group (`-pgid`), waits
`stop_timeout`, then sends `SIGKILL` to the group. When the main process exits on
its own, any remaining processes in the group are also killed, which prevents
daemonized grandchildren from escaping supervision.

The same graceful path is used in every case — `gosup stop-program`,
`gosup restart-program`, `gosup reload --restart` (for programs whose config
changed), and when the daemon itself exits:

1. send `stop_signal` (default `SIGTERM`) to the process group;
2. wait up to `stop_timeout` for every process in the group to exit;
3. only then `SIGKILL` the group.

When the daemon exits, all programs are stopped **in parallel**; `shutdown_timeout`
is the total budget for this phase. A program that ignores `SIGTERM` for longer
than `shutdown_timeout` is killed, even if its own `stop_timeout` is larger. Set
`shutdown_timeout` at least as large as the largest `stop_timeout` you configure,
otherwise the shutdown phase truncates the per-program grace period.

Note that a program only gets to handle the signal once it has installed its
handler; a process that is still starting up when the stop arrives may be
terminated by the default signal action.

`stop_signal` and `stop_timeout` are captured when the process starts and used
for the whole lifetime of that instance. A `reload --restart` that changes either
value therefore stops the old process with the *old* settings and starts the new
one with the *new* settings. This makes "raise `stop_timeout`, then reload" work
on the first try; the trade-off is that lowering `stop_timeout` only affects
processes started afterwards.

## HTTP API

Two endpoints on `listen`:

| Endpoint | Purpose |
| --- | --- |
| `POST /rpc` | request/response, one JSON object in, one out |
| `GET /events` | server-sent events for log streaming |
| `GET /` | one-line description |

### Authentication

`Authorization: Bearer <password>` or `?password=<password>`. Both are accepted on
both endpoints. The query form exists because `EventSource` cannot set headers.

With `allow_no_auth = true` and no password configured, requests without a
password are accepted. A missing password when one is configured returns HTTP 401
with `unauthorized`. Passwords are compared in constant time and re-read from the
live config, so a `reload` that changes the password takes effect immediately.

### `POST /rpc`

Request:

```json
{"id": 1, "method": "status", "params": {"program": "web"}}
```

Response:

```json
{"id": 1, "ok": true, "result": {...}}
{"id": 1, "ok": false, "error": {"code": "not_found", "message": "..."}}
```

`id` is echoed verbatim (any JSON value) and may be omitted. HTTP status codes
mirror the error: `400` for `parse_error`/`invalid_params`, `401` for
`unauthorized`, `404` for `not_found`, `409` for `already_running`/`busy`,
`500` for `internal_error`.

| Method | `params` | `result` |
| --- | --- | --- |
| `ping` | – | `{"pong": true, "time": "..."}` |
| `list` | – | `{"programs": [Status, ...]}` |
| `status` | `{"program": "id"}` | `Status` |
| `start` | `{"program": "id"}` | `Status` (after starting) |
| `stop` | `{"program": "id"}` | `Status` (after stopping) |
| `restart` | `{"program": "id"}` | `Status` (after restarting) |
| `output` | `{"program": "id", "stream": "stdout\|stderr\|both", "lines": N}` | `Output` |
| `reload` | `{"restart": bool}` | `ReloadResult` |

`Status`:

```json
{
  "id": "web",
  "enabled": true,
  "state": "running",
  "running": true,
  "pid": 1234,
  "pgid": 1234,
  "started_at": "2026-10-20T10:00:00.123456789+08:00",
  "restarts": 3,
  "exit_code": null,
  "signal": "",
  "stopped_at": null,
  "next_restart_at": null,
  "last_error": "",
  "command": "...",
  "argv": ["..."],
  "workdir": "/srv/web",
  "user": "",
  "capabilities": [],
  "restart_policy": "always",
  "pending_reload": false
}
```

`state` is one of `stopped`, `starting`, `running`, `stopping`, `backoff`.
`exit_code` / `signal` describe the previous run. `next_restart_at` is set while
waiting for a backoff restart. `pending_reload` means the process is still running
with the previous config.

`started_at` is the wall-clock time the current instance started, with nanosecond
precision. **There is deliberately no `uptime` field**: clients compute it as
`now - started_at`. The daemon could have computed it with `time.Since`, but that
uses `CLOCK_MONOTONIC`, which does not advance while a device sleeps — on Android
the result is visibly shorter than the time a user believes the program has been
running. Subtracting the wall-clock `started_at` also counts suspend time, which
is the semantics callers generally expect.

Clients should:

- only compute an age when `running` is true (`started_at` is retained after a
  stop, so a stopped program would otherwise look like it is still counting up);
- clamp to zero, since the client clock may lag the server's.

`Output` (only the requested streams are present; the others are omitted):

```json
{
  "id": "web",
  "stdout": "line 1\nline 2\n",
  "stderr": "",
  "stdout_truncated": false,
  "stderr_truncated": false,
  "stdout_total_bytes": 14,
  "stderr_total_bytes": 0
}
```

`stdout`/`stderr` are the current contents of the ring buffer (newest bytes
kept), and `*_truncated` says whether older output was discarded — either by the
ring buffer or by `lines`. The buffer is reset whenever the program starts, so it
reflects the current instance. `lines > 0` keeps only the last N lines.

Error codes: `parse_error`, `unauthorized`, `not_found`, `invalid_params`,
`already_running`, `not_running`, `busy` (program is stopping),
`internal_error`.

### `GET /events` (SSE)

```
GET /events?program=web&stream=both&lines=200&password=secret
```

Returns `text/event-stream`. `stream` defaults to `both`; `lines` trims the initial
snapshot to the last N lines.

Events, in the order they are written:

```
id: 1
event: output
data: {"program":"web","stream":"stdout","data":"tick 1\n"}

id: 2
event: reset
data: {"program":"web","stream":"stdout","message":"program restarted; output buffer cleared"}

id: 3
event: ping
data: {}
```

- `output` carries new output (`lost: true` in the data if older bytes were
  dropped before delivery).
- `reset` means the program restarted and its ring buffer was cleared.
- `ping` every 15 s keeps proxies from closing an idle stream and lets the client
  notice a dead peer.
- `error` reports a fatal condition; the server then closes the stream.

The connection starts with a snapshot of the ring buffer (respecting `lines`),
then continues from the exact offset where the snapshot ended, so no bytes are
lost or duplicated between the two. Close the connection to stop the stream; the
server reclaims it from `r.Context()`. Because `EventSource` reconnects on its
own, the client should re-request the snapshot after a reconnect rather than
assume the stream picks up where it left off.

Each open stream counts against `max_connections`; exceeding it returns HTTP 503.
The count only includes streams that were admitted while a limit was set, so
changing the limit from `0` (unlimited) to a number does not retroactively count
existing streams.

On shutdown the daemon cancels every open stream before calling
`http.Server.Shutdown`, because `Shutdown` waits for active handlers instead of
interrupting them.

### Example with curl

```sh
curl -s localhost:9090/rpc -H 'Authorization: Bearer secret' \
  -d '{"id":1,"method":"list"}'
curl -sN 'localhost:9090/events?program=web&stream=stdout&password=secret'
```

## Daemonization

`daemon -d` / `start` performs a classic double fork using `fork`+`exec` of the
same binary (no `fork` in-process, so it is safe with Go's runtime):

```
gosup start                       (parent exits immediately)
  └─ intermediate watcher         GOSUP_ROLE=watch, setsid
       └─ daemon                  GOSUP_ROLE=daemon, setsid
            └─ managed programs   each in its own session
```

The watcher only calls `wait4` on the daemon, which prevents a zombie and keeps
the daemon detached from the original session. The daemon redirects fd 0 to
`/dev/null` and fd 1/2 to `log_file`.

`SIGTERM`, `SIGINT`, and `SIGQUIT` trigger a graceful shutdown: every program is
stopped with its configured signal (then `SIGKILL` after its timeout), the
HTTP server is closed, and the pid file is removed.

## Security notes

- The API can start arbitrary configured programs, so protect the password:
  use `password_sha256` (or `password_file` with restrictive permissions), bind
  to `127.0.0.1`, and put a TLS terminator in front if remote access is needed.
- Passwords are compared in constant time. Bearer headers are preferred; the
  `?password=` form used by `EventSource` can end up in proxy logs.
- CORS echoes any origin, because the API is meant for a local browser UI. That
  is safe only as long as the password is strong: any page that can reach the
  port can *try* the API, and cannot do anything without the password.
- Programs run as root unless `user` is set. Set `user` for anything that does
  not need `CAP_*` privileges.

## Repository layout

```
main.go, client.go, args.go   CLI entry point and HTTP client commands
internal/app                  wires the supervisor and HTTP server together
internal/config               INI parsing, validation, capabilities, shell splitting
internal/supervisor           process lifecycle, restart policy, ring output
internal/rpc                  HTTP server, JSON-RPC dispatch, SSE stream
internal/daemon               double fork, stdio redirection, pid file lock
internal/output               bounded ring buffer
internal/logx                 tiny leveled logger
```

## Tests

```sh
go test -race ./...
```

The supervisor and RPC tests start real child processes and exercise
start/stop/restart, restart limits, ring output, reload (add/remove/update,
pending restarts), line-limited output, SSE streams (snapshot, reset, limits,
auth), and the HTTP protocol.

## Known limitations

- Linux only. The process management code uses `syscall` directly.
- SSE output is polled from the ring buffers every 200 ms rather than pushed from
  the writer, so stdout and stderr events interleave by buffer, not by real time.
- On an automatic reconnect, `EventSource` resumes the same URL but the server
  cannot replay what was missed; clients should re-request `/rpc output` for a
  fresh snapshot.
- A `kill -9` of the daemon leaves the managed programs running (they are in
  their own sessions by design); restart the daemon and stop them manually if
  that happens.
- `log_file` is opened in append mode and never rotated. A crash-looping program
  can fill the device; point `log_file` at a rotated path (logrotate with
  `copytruncate`) or set `log_level` higher on constrained devices.

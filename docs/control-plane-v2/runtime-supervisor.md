# Control Plane v2 Runtime Supervisor v1

## Scope
- This is the bounded single-node service/supervisor layer for one always-on Linux machine.
- It owns one runtime root and one SQLite database at a time.
- It starts and supervises the localhost HTTP API plus one bounded worker loop.
- It does not replace `systemd`.
- It does not introduce multi-worker scheduling, leases, fencing, remote nodes, or distributed coordination.

## Chosen supervision model
- One supervisor process with managed threads.
- Thread 1: localhost HTTP API server.
- Thread 2: bounded worker-cycle loop.

Why this model:
- one PID for operators to start, stop, inspect, and lock
- no extra child-process protocol between supervisor and worker/API
- direct reuse of the existing importable API and worker primitives
- explicit shared runtime state JSON without building a separate control bus

This keeps v1 bounded and single-node while still making API and worker failures visible.

## Runtime responsibilities
- hold the single-instance lock for one runtime root
- write the active PID file
- persist structured runtime state JSON
- append structured runtime events to a JSONL log
- run the existing localhost API on `127.0.0.1` / `localhost`
- run bounded worker cycles continuously with a configurable poll interval
- expose `start`, `stop`, `status`, `restart`, and `foreground`

The supervisor does not duplicate application logic from:
- `control_plane_v2/http_api.py`
- `control_plane_v2/worker_loop.py`

## Defaults
- runtime root: `/home/dkar/workspace/runtime/control-plane-v2` when running from this repo layout
- state dir: `<runtime-root>/state`
- pid dir: `<runtime-root>/pid`
- log dir: `<runtime-root>/logs`
- artifact root: `<runtime-root>/artifacts`
- worker log root: `<runtime-root>/worker-logs`
- workspace root: parent directory of the control repo
- API bind: `127.0.0.1:8788`
- worker poll interval: `5s`
- max claims per cycle: `1`

## Operator commands

Background runtime:

```bash
cd /home/dkar/workspace/control
./scripts/start-control-plane-runtime \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --runtime-root /tmp/control-plane-runtime
```

Foreground debug mode:

```bash
cd /home/dkar/workspace/control
./scripts/run-control-plane-runtime-foreground \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --runtime-root /tmp/control-plane-runtime
```

Status:

```bash
cd /home/dkar/workspace/control
./scripts/status-control-plane-runtime \
  --runtime-root /tmp/control-plane-runtime
```

Stop:

```bash
cd /home/dkar/workspace/control
./scripts/stop-control-plane-runtime \
  --runtime-root /tmp/control-plane-runtime
```

Restart:

```bash
cd /home/dkar/workspace/control
./scripts/restart-control-plane-runtime \
  --runtime-root /tmp/control-plane-runtime
```

All commands support `--json` for machine-readable output.

## Background vs foreground
- `start-control-plane-runtime` writes the resolved runtime config, detaches a supervisor process, and returns after status reports `running`.
- `run-control-plane-runtime-foreground` keeps the supervisor attached to the terminal for local debugging.
- Both modes use the same lock, state, pid, and log semantics.

## Single-node lock semantics
- The supervisor holds an exclusive non-blocking lock on `<runtime-state-dir>/supervisor.lock`.
- The active supervisor PID is written to `<runtime-pid-dir>/supervisor.pid`.
- A second start against the same runtime root fails explicitly with `RUNTIME_SUPERVISOR_ALREADY_RUNNING`.
- Stale pid files do not grant liveness by themselves; status checks both PID liveness and lock ownership.

## Runtime status contract
`status-control-plane-runtime --json` reports at least:
- `supervisor_running`
- `supervisor_state`
- `pid`
- `started_at`
- `sqlite_db`
- `api_base_url`
- `api_status`
- `worker_mode`
- `worker_status`
- `last_worker_cycle`
- `log_paths`
- `lock_path`

`last_worker_cycle` is intentionally compact and includes:
- `cycle_state`
- `ended_reason`
- `started_at`
- `completed_at`
- `duration_seconds`
- bounded counters such as `claims_processed`, `ticks_executed`, `runs_progressed`
- links to the worker summary artifacts when the existing worker loop wrote them

## Failure handling
- API thread startup failure aborts supervisor startup and records structured error state.
- API thread failure after startup marks the supervisor `degraded`; the supervisor does not silently respawn it.
- Worker cycle failures are recorded in state plus JSONL events and retried on the next poll interval.
- Worker thread failure marks the supervisor `degraded`; it is not silently respawned.
- Graceful stop waits for the current bounded worker cycle to finish; this keeps runtime transitions inspectable.

## Logs and state
- runtime state JSON: `<runtime-state-dir>/runtime-state.json`
- persisted runtime config JSON: `<runtime-state-dir>/runtime-config.json`
- lock file: `<runtime-state-dir>/supervisor.lock`
- pid file: `<runtime-pid-dir>/supervisor.pid`
- structured events log: `<runtime-log-dir>/runtime-events.jsonl`
- background console log: `<runtime-log-dir>/runtime-console.log`
- worker summaries: `<worker-log-root>/ticks` and `<worker-log-root>/loops` via the existing worker loop

The events log uses simple size-based rotation with one `.1` backup. v1 intentionally stops there.

## How this fits the approved v1 shape
- one dedicated Linux machine
- one supervisor instance
- one API listener
- one worker loop
- one SQLite database
- one runtime root

That is the explicit service layer for the already-approved “one Linux machine always on” model.

## Out of scope
- systemd packaging or unit files
- multi-worker support
- remote nodes
- distributed coordination
- lease/fencing protocols
- Kubernetes operators
- heavy monitoring / observability stacks

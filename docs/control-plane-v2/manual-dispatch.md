# Control Plane v2 Manual Dispatch Adapter

## Scope
- This is the first bounded v2 dispatch path for claimed runs.
- It launches one `executor` or one `reviewer` action at a time.
- It does not add a full scheduler loop, auto-continue policy, or in-command reviewer semantic completion.

## Claim a run

```bash
cd /home/dkar/workspace/control
./scripts/claim-next-run --sqlite-db /tmp/control-plane-v2.sqlite --json > /tmp/claimed-run.json
```

- The adapter expects a claimed run.
- You can target the run by `--claim-json`, `--run-id`, or `--queue-item-id`.

## Runtime context contract

The executor dispatch needs legacy runtime context in JSON. The easiest path is a `context.json` file shaped like the legacy host payload plus the concrete paths the host-side scripts already require:

```json
{
  "project": "demo",
  "task_text": "Implement the claimed task.",
  "mode": "executor+reviewer",
  "branch_base": "main",
  "auto_commit": false,
  "source": "manual-v2-dispatch",
  "thread_label": "demo-dispatch",
  "constraints": ["Only modify the requested files."],
  "expected_output": ["A reviewer-ready handoff commit."],
  "project_repo_path": "/home/dkar/workspace/projects/demo",
  "executor_worktree_path": "/home/dkar/workspace/runtime/worktrees/demo-executor",
  "reviewer_worktree_path": "/home/dkar/workspace/runtime/worktrees/demo-reviewer",
  "instruction_profile": "default",
  "instruction_overlays": ["docs-only"],
  "instructions_repo_path": "/home/dkar/workspace/instructions"
}
```

- The adapter also accepts path/selector overrides on the CLI.
- After a successful executor dispatch, the adapter persists a dispatch result manifest and artifact refs, so reviewer dispatch can usually reuse the same context from `--run-id` alone.

## Dispatch executor

```bash
cd /home/dkar/workspace/control
./scripts/dispatch-executor-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --claim-json /tmp/claimed-run.json \
  --context-json /tmp/context.json \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --json
```

What happens:
- resolve the next dispatchable role
- preflight the legacy backend
- `start-step-run`
- create a run-level artifact tree under `<artifact-root>/<project>/<flow>/<run>/`
- launch the legacy executor backend in an isolated sandbox
- `finish-step-run` with `succeeded` or `failed`
- persist artifact refs for dispatch context, result manifest, resolved instruction manifest, logs, prompt copy, and report when present

If the backend cannot start at all, the adapter records a dispatch-failed requeue and does not keep a `step_run`.

## Dispatch reviewer

After executor success, dispatch reviewer separately:

```bash
cd /home/dkar/workspace/control
./scripts/dispatch-reviewer-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --json
```

- Reviewer dispatch is still a separate bounded action.
- The adapter disables legacy auto-completion while reusing the same backend launch path, so reviewer semantic outcome stays an explicit next step.
- Finish reviewer semantics separately with `./scripts/complete-reviewer-outcome`.

## Auto-detect next role

```bash
cd /home/dkar/workspace/control
./scripts/dispatch-next-for-claimed-run \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --run-id <run-id> \
  --artifact-root /tmp/control-plane-v2-artifacts \
  --json
```

Current role resolution is intentionally narrow:
- no `step_runs` => dispatch `executor`
- terminal `executor` and no `reviewer` in the run => dispatch `reviewer`
- active `step_run` => dispatch blocked
- terminal reviewer or terminal run outcome => dispatch blocked

## What is reused from legacy runtime

- `scripts/run-executor.sh`
- `scripts/run-reviewer.sh`
- prompt building and instruction resolution scripts
- commit handoff logic
- existing host-side Codex launch contract

The adapter does not reimplement the Codex launch sequence. It prepares sandbox state/task files, invokes the legacy backend, and captures the resulting artifacts and step lifecycle.

## Remaining gaps before a full worker loop

- no endless scheduler/worker loop
- no lease heartbeat or ownership token
- no automatic reviewer semantic completion inside dispatch
- no policy engine for auto-continue
- no deploy/smoke matrix beyond the focused dispatch smoke

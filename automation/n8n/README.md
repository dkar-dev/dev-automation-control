# n8n Workflow Package v1

This directory contains the v1 n8n workflow package for Control Plane v2 over the localhost HTTP API.

Contents:
- `workflows/control-plane-v2-submit-bounded-task-v1.json`
- `workflows/control-plane-v2-run-worker-until-idle-v1.json`
- `workflows/control-plane-v2-manual-control-v1.json`

Boundary:
- `n8n` calls only the Control Plane HTTP API on `127.0.0.1:8788` or `host.docker.internal:8788`.
- `n8n` does not read or mutate SQLite directly.
- `n8n` does not call legacy bridge endpoints on `127.0.0.1:8787`.
- `Code` nodes only normalize inputs and outputs. Orchestration logic stays in the control plane.
- The legacy bridge workflow export is compatibility-only and is not the forward path for new orchestration.

Import guidance and payload examples live under [`docs/n8n/`](/home/dkar/workspace/control/docs/n8n/README.md).

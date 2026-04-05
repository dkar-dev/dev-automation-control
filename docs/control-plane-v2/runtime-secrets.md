# Control Plane v2 Runtime Secrets / Sensitive Config Resolution v1

## Scope
- This is the bounded single-node secrets/config resolution layer for one Linux runtime.
- It resolves symbolic refs from approved local sources only.
- It returns raw values only to the immediate in-process caller that needs them.
- It does not implement Vault, KMS, cluster secret sync, remote node propagation, or any distributed secret-management model.

## Chosen config block and source model
- Project packages declare runtime-sensitive refs in one fixed block:
  - `runtime.yaml.runtime_value_refs_v1`
- Supported v1 sources:
  - environment variables via `env:<NAME>`
  - one local JSON secrets file via `file:<key>`
  - `inline_value` only for explicit non-secret config
- Default local file lookup order:
  - `<runtime_root>/secrets/runtime-secrets.json`
  - `<control_root>/.control-plane-runtime-secrets.json`
- An explicit file path can override discovery with:
  - CLI `--local-secrets-file`
  - API `local_secrets_file`
  - API env `CONTROL_PLANE_API_LOCAL_SECRETS_FILE`

Example project block:

```yaml
runtime_value_refs_v1:
  values:
    clickup_token:
      classification: secret
      required: true
      refs:
        - env:CLICKUP_TOKEN
        - file:clickup.token
      dispatch_env: CLICKUP_TOKEN
      description: ClickUp token for bounded dispatch tools

    github_token:
      classification: secret
      refs:
        - env:GITHUB_TOKEN
        - file:github.token
      dispatch_env: GITHUB_TOKEN

    deployment_webhook_url:
      classification: sensitive_config
      refs:
        - env:DEPLOYMENT_WEBHOOK_URL
        - file:deployment.webhook_url
      host_check_context_key: deployment_webhook_url

    repo_path:
      classification: plain_config
      inline_value: /home/example/workspace/projects/sample-project
      dispatch_env: PROJECT_REPO_PATH

    default_branch:
      classification: plain_config
      inline_value: main
      dispatch_env: DEFAULT_BRANCH
```

Example local secrets file:

```json
{
  "clickup.token": "token-from-local-file",
  "github.token": "ghp_local_only",
  "deployment.webhook_url": "https://deploy.example.internal/hook"
}
```

## Classification contract
- `secret`
  - allowed storage: env vars or local secrets file only
  - logging: redacted only
  - persistence: no raw persistence in SQLite, manifests, artifacts, status JSON, handoff bundles, or runtime logs
- `sensitive_config`
  - allowed storage: env vars or local secrets file only
  - logging: redacted only
  - persistence: no raw persistence in SQLite, manifests, artifacts, status JSON, handoff bundles, or runtime logs
- `plain_config`
  - allowed storage: env vars, local secrets file, or `inline_value`
  - logging: plain value may be shown
  - persistence: plain value may be stored when the consuming flow already persists normal runtime config

## Resolution contract
- The layer can:
  - resolve one declared key
  - resolve a selected bundle of keys
  - validate that required keys are resolvable
  - return safe inspection metadata
- Selection modes:
  - `all`
  - `dispatch_env`
  - `host_checks`
- Safe inspection metadata includes:
  - `key`
  - `classification`
  - `required`
  - `resolved`
  - `source_type`
  - `source_ref`
  - `description`
  - `dispatch_env`
  - `host_check_context_key`
  - `value`
- For `secret` and `sensitive_config`, `value` is always redacted in inspection output.

## Redaction policy
- One consistent marker format is used everywhere:
  - `[redacted:secret:<key>]`
  - `[redacted:sensitive_config:<key>]`
- Raw secret/sensitive values must not appear in:
  - SQLite rows
  - artifact manifests
  - dispatch stdout/stderr logs
  - release handoff bundles
  - deployable-green manifests
  - runtime supervisor state or event logs
  - HTTP inspection/status responses
  - worker summaries
- If a required ref is missing, the error is explicit but still redacted.
- v1 does not provide any raw-secret retrieval endpoint over HTTP.

## Runtime integration points
- Dispatch adapter:
  - resolves the `dispatch_env` selection
  - injects resolved values into the backend child environment only
  - redacts dispatch manifests and captured stdout/stderr afterward
- Host checks:
  - resolves the `host_checks` selection
  - merges resolved values into in-memory placeholder context only
  - redacts manifests and per-check observed data
- Runtime supervisor / local API:
  - threads `runtime_root` and optional `local_secrets_file` into the API and worker runtime config
  - lets one always-on single-node runtime use the same bounded source model everywhere

## CLI
- [`scripts/resolve-runtime-secrets`](/home/dkar/workspace/control/scripts/resolve-runtime-secrets)
- [`scripts/check-runtime-secrets`](/home/dkar/workspace/control/scripts/check-runtime-secrets)

Resolve with safe inspection output:

```bash
cd /home/dkar/workspace/control
./scripts/resolve-runtime-secrets \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --project-key sample-project \
  --runtime-root /tmp/control-plane-runtime \
  --selection all \
  --json
```

Validate required refs only:

```bash
cd /home/dkar/workspace/control
./scripts/check-runtime-secrets \
  --sqlite-db /tmp/control-plane-v2.sqlite \
  --project-key sample-project \
  --runtime-root /tmp/control-plane-runtime \
  --selection dispatch_env \
  --json
```

- CLI inspection never prints raw secret values in v1.
- There is intentionally no unsafe debug-print mode in this version.

## HTTP API
- `GET /v1/runtime/secrets/status`
- `POST /v1/runtime/secrets/check`

Examples:

```bash
curl -s "http://127.0.0.1:8788/v1/runtime/secrets/status?project_key=sample-project&selection=all"
```

```bash
curl -s http://127.0.0.1:8788/v1/runtime/secrets/check \
  -H 'Content-Type: application/json' \
  -d '{
    "project_key": "sample-project",
    "selection": "dispatch_env"
  }'
```

- These endpoints expose only redacted metadata.
- Application code that needs raw values must call the resolution layer directly in-process.

## Single-node v1 fit
- one Linux machine
- one runtime root
- one local secrets file surface
- one localhost API
- one bounded worker loop

This keeps the contract explicit and operable for the approved single-node v1 runtime, while leaving distributed secret management for a later design.

## Smoke coverage
- [`scripts/smoke-control-plane-v2-runtime-secrets.sh`](/home/dkar/workspace/control/scripts/smoke-control-plane-v2-runtime-secrets.sh) verifies:
  - env-based secret resolution
  - file-based secret resolution
  - explicit failure for missing required refs
  - redacted CLI and HTTP inspection output
  - dispatch env injection without raw secret persistence
  - host-check secret-backed header usage without raw secret persistence

## Out of scope
- Vault / KMS integration
- distributed secret sync
- raw secret storage in project packages or SQLite
- raw secret retrieval over HTTP
- remote-node secret resolution

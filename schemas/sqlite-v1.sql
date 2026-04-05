CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY CHECK (version >= 1),
  name TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY,
  project_key TEXT NOT NULL UNIQUE,
  package_root TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
  project_profile TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  milestone TEXT NOT NULL,
  flow_id TEXT NOT NULL,
  parent_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
  origin_type TEXT NOT NULL,
  origin_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
  origin_step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  status TEXT NOT NULL CHECK (
    status IN ('queued', 'running', 'paused', 'completed', 'failed', 'stopped', 'cancelled')
  ),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  queued_at TEXT,
  started_at TEXT,
  terminal_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_project_status
  ON runs (project_id, status, created_at);

CREATE INDEX IF NOT EXISTS idx_runs_flow
  ON runs (flow_id, created_at);

CREATE INDEX IF NOT EXISTS idx_runs_parent
  ON runs (parent_run_id);

CREATE INDEX IF NOT EXISTS idx_runs_origin_run
  ON runs (origin_run_id);

CREATE INDEX IF NOT EXISTS idx_runs_origin_step_run
  ON runs (origin_step_run_id);

CREATE INDEX IF NOT EXISTS idx_runs_scope
  ON runs (project_id, project_profile, workflow_id, milestone, created_at);

CREATE TABLE IF NOT EXISTS step_runs (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
  step_key TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
  previous_step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  status TEXT NOT NULL CHECK (
    status IN ('running', 'succeeded', 'failed', 'timed_out', 'cancelled')
  ),
  created_at TEXT NOT NULL,
  started_at TEXT NOT NULL,
  terminal_at TEXT,
  CHECK (
    (attempt_no = 1 AND previous_step_run_id IS NULL) OR
    (attempt_no > 1 AND previous_step_run_id IS NOT NULL)
  ),
  UNIQUE (run_id, step_key, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_step_runs_run
  ON step_runs (run_id, step_key, attempt_no);

CREATE INDEX IF NOT EXISTS idx_step_runs_status
  ON step_runs (status, started_at);

CREATE INDEX IF NOT EXISTS idx_step_runs_previous
  ON step_runs (previous_step_run_id);

CREATE TABLE IF NOT EXISTS queue_items (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL UNIQUE REFERENCES runs(id) ON DELETE RESTRICT,
  priority_class TEXT NOT NULL CHECK (
    priority_class IN ('system', 'interactive', 'background')
  ),
  status TEXT NOT NULL CHECK (
    status IN ('queued', 'claimed', 'paused', 'completed', 'cancelled')
  ),
  enqueued_at TEXT NOT NULL,
  available_at TEXT NOT NULL,
  claimed_at TEXT,
  terminal_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_items_ordering
  ON queue_items (status, priority_class, available_at, enqueued_at, id);

CREATE INDEX IF NOT EXISTS idx_queue_items_class
  ON queue_items (priority_class, status, enqueued_at);

CREATE TABLE IF NOT EXISTS artifact_refs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
  flow_id TEXT NOT NULL,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
  step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  artifact_kind TEXT NOT NULL,
  filesystem_path TEXT NOT NULL UNIQUE,
  media_type TEXT,
  size_bytes INTEGER CHECK (size_bytes IS NULL OR size_bytes >= 0),
  checksum_sha256 TEXT,
  created_at TEXT NOT NULL,
  cleaned_at TEXT,
  cleanup_status TEXT,
  cleanup_result_json TEXT,
  last_cleanup_error TEXT,
  CHECK (step_run_id IS NULL OR run_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_artifact_refs_run
  ON artifact_refs (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_artifact_refs_step_run
  ON artifact_refs (step_run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_artifact_refs_flow
  ON artifact_refs (flow_id, created_at);

CREATE TABLE IF NOT EXISTS runtime_cleanup_records (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
  flow_id TEXT NOT NULL,
  cleanup_scope TEXT NOT NULL CHECK (
    cleanup_scope IN ('worktree', 'branch')
  ),
  target_identity TEXT NOT NULL,
  target_path TEXT,
  git_repo_path TEXT,
  role_hint TEXT,
  cleaned_at TEXT,
  cleanup_status TEXT,
  cleanup_result_json TEXT,
  last_cleanup_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE (run_id, cleanup_scope, target_identity)
);

CREATE INDEX IF NOT EXISTS idx_runtime_cleanup_records_run
  ON runtime_cleanup_records (run_id, cleanup_scope, updated_at);

CREATE INDEX IF NOT EXISTS idx_runtime_cleanup_records_scope
  ON runtime_cleanup_records (cleanup_scope, cleanup_status, updated_at);

CREATE INDEX IF NOT EXISTS idx_runtime_cleanup_records_target
  ON runtime_cleanup_records (target_identity, cleanup_scope);

CREATE TABLE IF NOT EXISTS contract_manifests (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
  flow_id TEXT,
  run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
  step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  workflow_id TEXT NOT NULL,
  project_profile TEXT NOT NULL,
  contract_type TEXT NOT NULL CHECK (
    contract_type IN ('implementation_step', 'inspection_step', 'recovery_step', 'manual_followup_step')
  ),
  template_key TEXT NOT NULL,
  contract_json_path TEXT NOT NULL UNIQUE,
  prompt_text_path TEXT NOT NULL UNIQUE,
  manifest_json_path TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_contract_manifests_project
  ON contract_manifests (project_id, workflow_id, contract_type, created_at);

CREATE INDEX IF NOT EXISTS idx_contract_manifests_run
  ON contract_manifests (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_contract_manifests_flow
  ON contract_manifests (flow_id, created_at);

CREATE TABLE IF NOT EXISTS host_check_runs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
  flow_id TEXT NOT NULL,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
  step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  workflow_id TEXT NOT NULL,
  project_profile TEXT NOT NULL,
  verdict TEXT NOT NULL CHECK (
    verdict IN ('green', 'not_green', 'blocked')
  ),
  selected_total INTEGER NOT NULL CHECK (selected_total >= 0),
  required_total INTEGER NOT NULL CHECK (required_total >= 0),
  required_passed INTEGER NOT NULL CHECK (required_passed >= 0),
  required_failed INTEGER NOT NULL CHECK (required_failed >= 0),
  advisory_total INTEGER NOT NULL CHECK (advisory_total >= 0),
  advisory_failed INTEGER NOT NULL CHECK (advisory_failed >= 0),
  blocked_total INTEGER NOT NULL CHECK (blocked_total >= 0),
  manifest_json_path TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_host_check_runs_run
  ON host_check_runs (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_host_check_runs_flow
  ON host_check_runs (flow_id, created_at);

CREATE INDEX IF NOT EXISTS idx_host_check_runs_step_run
  ON host_check_runs (step_run_id, created_at);

CREATE TABLE IF NOT EXISTS green_decisions (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
  flow_id TEXT NOT NULL,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
  workflow_id TEXT NOT NULL,
  project_profile TEXT NOT NULL,
  decision_status TEXT NOT NULL CHECK (
    decision_status IN ('deployable_green', 'not_green', 'blocked')
  ),
  reviewer_verdict TEXT CHECK (
    reviewer_verdict IS NULL OR reviewer_verdict IN ('approved', 'changes_requested', 'blocked')
  ),
  reviewer_source_kind TEXT NOT NULL,
  reviewer_source_ref TEXT,
  reviewer_created_at TEXT,
  reviewer_step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  host_checks_verdict TEXT CHECK (
    host_checks_verdict IS NULL OR host_checks_verdict IN ('green', 'not_green', 'blocked')
  ),
  host_checks_source_kind TEXT NOT NULL,
  host_checks_source_ref TEXT,
  host_checks_created_at TEXT,
  host_check_run_id TEXT REFERENCES host_check_runs(id) ON DELETE RESTRICT,
  summary_text TEXT NOT NULL,
  rationale_text TEXT NOT NULL,
  next_action_hint TEXT,
  manifest_json_path TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_green_decisions_run
  ON green_decisions (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_green_decisions_flow
  ON green_decisions (flow_id, created_at);

CREATE INDEX IF NOT EXISTS idx_green_decisions_host_check_run
  ON green_decisions (host_check_run_id, created_at);

CREATE TABLE IF NOT EXISTS release_handoffs (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
  flow_id TEXT NOT NULL,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
  workflow_id TEXT NOT NULL,
  project_profile TEXT NOT NULL,
  milestone TEXT NOT NULL,
  decision_status TEXT NOT NULL CHECK (
    decision_status = 'deployable_green'
  ),
  commit_sha TEXT NOT NULL,
  commit_source_kind TEXT NOT NULL,
  commit_source_ref TEXT,
  reviewer_source_kind TEXT NOT NULL,
  reviewer_source_ref TEXT,
  reviewer_created_at TEXT,
  reviewer_step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  host_checks_source_kind TEXT NOT NULL,
  host_checks_source_ref TEXT,
  host_checks_created_at TEXT,
  host_check_run_id TEXT REFERENCES host_check_runs(id) ON DELETE RESTRICT,
  green_decision_id TEXT NOT NULL REFERENCES green_decisions(id) ON DELETE RESTRICT,
  green_decision_created_at TEXT NOT NULL,
  summary_text TEXT NOT NULL,
  rationale_text TEXT NOT NULL,
  manifest_json_path TEXT NOT NULL UNIQUE,
  summary_markdown_path TEXT NOT NULL UNIQUE,
  artifact_index_json_path TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_handoffs_run
  ON release_handoffs (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_release_handoffs_flow
  ON release_handoffs (flow_id, created_at);

CREATE INDEX IF NOT EXISTS idx_release_handoffs_green_decision
  ON release_handoffs (green_decision_id, created_at);

CREATE TABLE IF NOT EXISTS state_transitions (
  id TEXT PRIMARY KEY,
  entity_type TEXT NOT NULL CHECK (
    entity_type IN ('run', 'step_run', 'queue_item')
  ),
  run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
  step_run_id TEXT REFERENCES step_runs(id) ON DELETE RESTRICT,
  queue_item_id TEXT REFERENCES queue_items(id) ON DELETE RESTRICT,
  from_state TEXT,
  to_state TEXT NOT NULL,
  transition_type TEXT NOT NULL,
  reason_code TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL,
  CHECK (
    (entity_type = 'run' AND run_id IS NOT NULL AND step_run_id IS NULL AND queue_item_id IS NULL) OR
    (entity_type = 'step_run' AND run_id IS NULL AND step_run_id IS NOT NULL AND queue_item_id IS NULL) OR
    (entity_type = 'queue_item' AND run_id IS NULL AND step_run_id IS NULL AND queue_item_id IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_state_transitions_run
  ON state_transitions (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_state_transitions_step_run
  ON state_transitions (step_run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_state_transitions_queue_item
  ON state_transitions (queue_item_id, created_at);

CREATE TABLE IF NOT EXISTS run_snapshots (
  id TEXT PRIMARY KEY,
  snapshot_scope TEXT NOT NULL CHECK (
    snapshot_scope IN ('run', 'flow')
  ),
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
  flow_id TEXT NOT NULL,
  run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
  state_transition_id TEXT NOT NULL REFERENCES state_transitions(id) ON DELETE RESTRICT,
  snapshot_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  CHECK (
    (snapshot_scope = 'run' AND run_id IS NOT NULL) OR
    (snapshot_scope = 'flow' AND run_id IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_run_snapshots_run
  ON run_snapshots (run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_run_snapshots_flow
  ON run_snapshots (flow_id, created_at);

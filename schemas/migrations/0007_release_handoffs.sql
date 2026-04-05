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

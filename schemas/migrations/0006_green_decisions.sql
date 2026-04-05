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

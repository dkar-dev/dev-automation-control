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

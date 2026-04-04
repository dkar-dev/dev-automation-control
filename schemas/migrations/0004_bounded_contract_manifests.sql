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

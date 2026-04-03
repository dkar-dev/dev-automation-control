ALTER TABLE artifact_refs ADD COLUMN cleaned_at TEXT;

ALTER TABLE artifact_refs ADD COLUMN cleanup_status TEXT;

ALTER TABLE artifact_refs ADD COLUMN cleanup_result_json TEXT;

ALTER TABLE artifact_refs ADD COLUMN last_cleanup_error TEXT;

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

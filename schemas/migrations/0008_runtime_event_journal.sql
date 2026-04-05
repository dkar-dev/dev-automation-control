CREATE TABLE IF NOT EXISTS runtime_events (
  event_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  project_key TEXT,
  flow_id TEXT,
  run_id TEXT,
  step_run_id TEXT,
  severity TEXT NOT NULL CHECK (
    severity IN ('info', 'warning', 'error')
  ),
  summary TEXT NOT NULL,
  payload_redacted_json TEXT NOT NULL,
  source_module TEXT NOT NULL,
  CHECK (
    step_run_id IS NULL OR run_id IS NOT NULL
  )
);

CREATE INDEX IF NOT EXISTS idx_runtime_events_created
  ON runtime_events (created_at DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_events_project
  ON runtime_events (project_key, created_at DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_events_flow
  ON runtime_events (flow_id, created_at DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_events_run
  ON runtime_events (run_id, created_at DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_events_type
  ON runtime_events (event_type, created_at DESC, event_id DESC);

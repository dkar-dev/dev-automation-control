PRAGMA legacy_alter_table = ON;

ALTER TABLE runs RENAME TO runs__before_0002;

CREATE TABLE runs (
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

INSERT INTO runs (
  id,
  project_id,
  project_profile,
  workflow_id,
  milestone,
  flow_id,
  parent_run_id,
  origin_type,
  origin_run_id,
  origin_step_run_id,
  status,
  created_at,
  updated_at,
  queued_at,
  started_at,
  terminal_at
)
SELECT
  id,
  project_id,
  project_profile,
  workflow_id,
  milestone,
  flow_id,
  parent_run_id,
  origin_type,
  origin_run_id,
  origin_step_run_id,
  status,
  created_at,
  updated_at,
  queued_at,
  started_at,
  terminal_at
FROM runs__before_0002;

DROP TABLE runs__before_0002;

CREATE INDEX idx_runs_project_status
  ON runs (project_id, status, created_at);

CREATE INDEX idx_runs_flow
  ON runs (flow_id, created_at);

CREATE INDEX idx_runs_parent
  ON runs (parent_run_id);

CREATE INDEX idx_runs_origin_run
  ON runs (origin_run_id);

CREATE INDEX idx_runs_origin_step_run
  ON runs (origin_step_run_id);

CREATE INDEX idx_runs_scope
  ON runs (project_id, project_profile, workflow_id, milestone, created_at);

ALTER TABLE queue_items RENAME TO queue_items__before_0002;

CREATE TABLE queue_items (
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

INSERT INTO queue_items (
  id,
  run_id,
  priority_class,
  status,
  enqueued_at,
  available_at,
  claimed_at,
  terminal_at
)
SELECT
  id,
  run_id,
  priority_class,
  status,
  enqueued_at,
  available_at,
  claimed_at,
  terminal_at
FROM queue_items__before_0002;

DROP TABLE queue_items__before_0002;

CREATE INDEX idx_queue_items_ordering
  ON queue_items (status, priority_class, available_at, enqueued_at, id);

CREATE INDEX idx_queue_items_class
  ON queue_items (priority_class, status, enqueued_at);

PRAGMA legacy_alter_table = OFF;

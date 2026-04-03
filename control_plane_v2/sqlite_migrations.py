from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import sqlite3


MIGRATION_BASELINE_POLICY = "fresh_bootstrap_uses_latest_schema_snapshot_and_marks_all_known_migrations_applied"

SQLITE_MIGRATION_DATABASE_NOT_FOUND = "SQLITE_MIGRATION_DATABASE_NOT_FOUND"
SQLITE_MIGRATION_DISCOVERY_FAILED = "SQLITE_MIGRATION_DISCOVERY_FAILED"
SQLITE_MIGRATION_INVALID_STATE = "SQLITE_MIGRATION_INVALID_STATE"
SQLITE_MIGRATION_STATEMENT_INCOMPLETE = "SQLITE_MIGRATION_STATEMENT_INCOMPLETE"
SQLITE_MIGRATION_STORAGE_ERROR = "SQLITE_MIGRATION_STORAGE_ERROR"

SCHEMA_MIGRATIONS_TABLE = "schema_migrations"

_MIGRATION_FILENAME_RE = re.compile(r"^(?P<version>\d{4})_(?P<name>[a-z0-9_]+)\.sql$")
_MANAGED_TABLES = (
    "projects",
    "runs",
    "step_runs",
    "queue_items",
    "artifact_refs",
    "state_transitions",
    "run_snapshots",
)


@dataclass(frozen=True)
class SQLiteMigration:
    version: int
    name: str
    path: Path

    @property
    def version_label(self) -> str:
        return f"{self.version:04d}"

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "version_label": self.version_label,
            "name": self.name,
            "path": str(self.path),
        }


@dataclass(frozen=True)
class AppliedSQLiteMigration:
    version: int
    name: str
    applied_at: str

    @property
    def version_label(self) -> str:
        return f"{self.version:04d}"

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "version_label": self.version_label,
            "name": self.name,
            "applied_at": self.applied_at,
        }


@dataclass(frozen=True)
class SQLiteSchemaVersion:
    database_path: Path
    tracked: bool
    detected_state: str
    current_version: int
    current_name: str | None
    latest_version: int
    latest_name: str | None
    pending_migrations: tuple[SQLiteMigration, ...]
    applied_migrations: tuple[AppliedSQLiteMigration, ...]
    baseline_policy: str

    def to_dict(self) -> dict[str, object]:
        return {
            "database_path": str(self.database_path),
            "tracked": self.tracked,
            "detected_state": self.detected_state,
            "current_version": self.current_version,
            "current_version_label": f"{self.current_version:04d}",
            "current_name": self.current_name,
            "latest_version": self.latest_version,
            "latest_version_label": f"{self.latest_version:04d}",
            "latest_name": self.latest_name,
            "pending_migrations": [migration.to_dict() for migration in self.pending_migrations],
            "applied_migrations": [migration.to_dict() for migration in self.applied_migrations],
            "baseline_policy": self.baseline_policy,
        }


@dataclass(frozen=True)
class SQLiteMigrationApplyResult:
    database_path: Path
    schema_path: Path
    migrations_root: Path
    operation: str
    tracked_before: bool
    version_before: int
    version_after: int
    executed_migrations: tuple[SQLiteMigration, ...]
    recorded_migrations: tuple[AppliedSQLiteMigration, ...]
    tables: tuple[str, ...]
    schema_version: SQLiteSchemaVersion
    baseline_policy: str

    def to_dict(self) -> dict[str, object]:
        return {
            "database_path": str(self.database_path),
            "schema_path": str(self.schema_path),
            "migrations_root": str(self.migrations_root),
            "operation": self.operation,
            "tracked_before": self.tracked_before,
            "version_before": self.version_before,
            "version_before_label": f"{self.version_before:04d}",
            "version_after": self.version_after,
            "version_after_label": f"{self.version_after:04d}",
            "executed_migrations": [migration.to_dict() for migration in self.executed_migrations],
            "recorded_migrations": [migration.to_dict() for migration in self.recorded_migrations],
            "tables": list(self.tables),
            "schema_version": self.schema_version.to_dict(),
            "baseline_policy": self.baseline_policy,
        }


class SQLiteMigrationError(Exception):
    def __init__(self, code: str, message: str, database_path: Path, details: str | None = None) -> None:
        self.code = code
        self.message = message
        self.database_path = database_path
        self.details = details
        super().__init__(message)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "message": self.message,
            "database_path": str(self.database_path),
            "details": self.details,
        }


def list_sqlite_migrations(migrations_root: str | Path) -> tuple[SQLiteMigration, ...]:
    resolved_root = _resolve_existing_path(migrations_root)
    return _discover_migrations(resolved_root, resolved_root)


def get_sqlite_schema_version(
    database_path: str | Path,
    *,
    migrations_root: str | Path,
) -> SQLiteSchemaVersion:
    resolved_db_path = _resolve_existing_path(database_path)
    resolved_migrations_root = _resolve_existing_path(migrations_root)
    migrations = _discover_migrations(resolved_migrations_root, resolved_db_path)
    connection = _connect_database(resolved_db_path)
    try:
        return _inspect_schema_version(connection, resolved_db_path, migrations)
    finally:
        connection.close()


def migrate_sqlite_v1(
    database_path: str | Path,
    *,
    schema_path: str | Path,
    migrations_root: str | Path,
) -> SQLiteMigrationApplyResult:
    resolved_db_path = Path(database_path).expanduser().resolve()
    resolved_schema_path = _resolve_existing_path(schema_path)
    resolved_migrations_root = _resolve_existing_path(migrations_root)
    migrations = _discover_migrations(resolved_migrations_root, resolved_db_path)

    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = _connect_database(resolved_db_path)
    try:
        before = _inspect_schema_version(connection, resolved_db_path, migrations, allow_missing_database=True)
        tracked_before = before.tracked
        version_before = before.current_version

        executed_migrations: list[SQLiteMigration] = []
        recorded_migrations: list[AppliedSQLiteMigration] = []
        operation = "already_current"

        if before.detected_state == "empty_untracked":
            recorded_migrations.extend(_bootstrap_latest_snapshot(connection, resolved_db_path, resolved_schema_path, migrations))
            operation = "bootstrapped_latest_snapshot"
        elif before.detected_state == "legacy_untracked_v1":
            recorded_migrations.extend(_ensure_tracked_prefix(connection, resolved_db_path, migrations, up_to_version=1))
            pending = [migration for migration in migrations if migration.version > 1]
            executed_now, recorded_now = _apply_pending_migrations(connection, resolved_db_path, pending)
            executed_migrations.extend(executed_now)
            recorded_migrations.extend(recorded_now)
            operation = "migrated_existing"
        elif before.detected_state == "legacy_untracked_v2":
            recorded_migrations.extend(_ensure_tracked_prefix(connection, resolved_db_path, migrations, up_to_version=2))
            operation = "adopted_existing"
        elif before.detected_state == "tracked":
            pending = [migration for migration in migrations if migration.version > before.current_version]
            if pending:
                executed_now, recorded_now = _apply_pending_migrations(connection, resolved_db_path, pending)
                executed_migrations.extend(executed_now)
                recorded_migrations.extend(recorded_now)
                operation = "migrated_existing"
        else:
            raise SQLiteMigrationError(
                code=SQLITE_MIGRATION_INVALID_STATE,
                message="SQLite schema state is not migratable",
                database_path=resolved_db_path,
                details=f"detected_state={before.detected_state}",
            )

        after = _inspect_schema_version(connection, resolved_db_path, migrations, allow_missing_database=True)
        tables = _list_user_tables(connection)
    finally:
        connection.close()

    return SQLiteMigrationApplyResult(
        database_path=resolved_db_path,
        schema_path=resolved_schema_path,
        migrations_root=resolved_migrations_root,
        operation=operation,
        tracked_before=tracked_before,
        version_before=version_before,
        version_after=after.current_version,
        executed_migrations=tuple(executed_migrations),
        recorded_migrations=tuple(recorded_migrations),
        tables=tuple(tables),
        schema_version=after,
        baseline_policy=MIGRATION_BASELINE_POLICY,
    )


def _inspect_schema_version(
    connection: sqlite3.Connection,
    database_path: Path,
    migrations: tuple[SQLiteMigration, ...],
    *,
    allow_missing_database: bool = False,
) -> SQLiteSchemaVersion:
    latest_version = migrations[-1].version if migrations else 0
    latest_name = migrations[-1].name if migrations else None
    user_tables = _list_user_tables(connection)
    has_migration_table = SCHEMA_MIGRATIONS_TABLE in user_tables

    if not has_migration_table:
        detected_state, current_version = _detect_untracked_layout(connection, database_path, user_tables)
        return SQLiteSchemaVersion(
            database_path=database_path,
            tracked=False,
            detected_state=detected_state,
            current_version=current_version,
            current_name=_migration_name_for_version(migrations, current_version),
            latest_version=latest_version,
            latest_name=latest_name,
            pending_migrations=tuple(migration for migration in migrations if migration.version > current_version),
            applied_migrations=(),
            baseline_policy=MIGRATION_BASELINE_POLICY,
        )

    applied_migrations = _load_applied_migrations(connection, database_path, migrations)
    current_version = applied_migrations[-1].version if applied_migrations else 0
    current_name = applied_migrations[-1].name if applied_migrations else None

    if current_version == 0 and any(table for table in user_tables if table != SCHEMA_MIGRATIONS_TABLE):
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_INVALID_STATE,
            message="schema_migrations exists but does not describe the existing SQLite schema",
            database_path=database_path,
            details="Applied migrations table is empty while managed tables already exist.",
        )

    if current_version > latest_version:
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_INVALID_STATE,
            message="SQLite database is ahead of the available migration chain",
            database_path=database_path,
            details=f"current_version={current_version} latest_version={latest_version}",
        )

    if current_version > 0:
        detected_layout_state, detected_layout_version = _detect_untracked_layout(
            connection,
            database_path,
            user_tables,
            allow_tracked_table=True,
        )
        if detected_layout_state not in {"legacy_untracked_v1", "legacy_untracked_v2"}:
            raise SQLiteMigrationError(
                code=SQLITE_MIGRATION_INVALID_STATE,
                message="Tracked SQLite database has an invalid managed schema layout",
                database_path=database_path,
                details=f"layout_state={detected_layout_state}",
            )
        if detected_layout_version != current_version:
            raise SQLiteMigrationError(
                code=SQLITE_MIGRATION_INVALID_STATE,
                message="Tracked SQLite migration metadata does not match the actual schema layout",
                database_path=database_path,
                details=f"tracked_version={current_version} layout_version={detected_layout_version}",
            )

    return SQLiteSchemaVersion(
        database_path=database_path,
        tracked=True,
        detected_state="tracked",
        current_version=current_version,
        current_name=current_name,
        latest_version=latest_version,
        latest_name=latest_name,
        pending_migrations=tuple(migration for migration in migrations if migration.version > current_version),
        applied_migrations=applied_migrations,
        baseline_policy=MIGRATION_BASELINE_POLICY,
    )


def _apply_pending_migrations(
    connection: sqlite3.Connection,
    database_path: Path,
    migrations: list[SQLiteMigration],
) -> tuple[list[SQLiteMigration], list[AppliedSQLiteMigration]]:
    executed: list[SQLiteMigration] = []
    recorded: list[AppliedSQLiteMigration] = []
    for migration in migrations:
        applied = _apply_single_migration(connection, database_path, migration)
        executed.append(migration)
        recorded.append(applied)
    return executed, recorded


def _apply_single_migration(
    connection: sqlite3.Connection,
    database_path: Path,
    migration: SQLiteMigration,
) -> AppliedSQLiteMigration:
    sql_text = migration.path.read_text(encoding="utf-8")
    statements = tuple(_iter_sql_statements(sql_text, database_path))
    applied_at = _utc_now()
    try:
        connection.execute("PRAGMA foreign_keys = OFF;")
        connection.execute("BEGIN IMMEDIATE;")
        _ensure_schema_migrations_table(connection)
        for statement in statements:
            connection.execute(statement)
        _insert_migration_row(connection, migration, applied_at)
        _raise_if_foreign_key_check_fails(connection, database_path)
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_STORAGE_ERROR,
            message=f"SQLite migration failed while applying {migration.version_label}_{migration.name}",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.execute("PRAGMA foreign_keys = ON;")
    return AppliedSQLiteMigration(version=migration.version, name=migration.name, applied_at=applied_at)


def _bootstrap_latest_snapshot(
    connection: sqlite3.Connection,
    database_path: Path,
    schema_path: Path,
    migrations: tuple[SQLiteMigration, ...],
) -> list[AppliedSQLiteMigration]:
    sql_text = schema_path.read_text(encoding="utf-8")
    statements = tuple(_iter_sql_statements(sql_text, database_path))
    applied_at = _utc_now()
    try:
        connection.execute("PRAGMA foreign_keys = OFF;")
        connection.execute("BEGIN IMMEDIATE;")
        for statement in statements:
            connection.execute(statement)
        _ensure_schema_migrations_table(connection)
        for migration in migrations:
            _insert_migration_row(connection, migration, applied_at)
        _raise_if_foreign_key_check_fails(connection, database_path)
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_STORAGE_ERROR,
            message="SQLite latest-schema bootstrap failed",
            database_path=database_path,
            details=str(exc),
        ) from exc
    finally:
        connection.execute("PRAGMA foreign_keys = ON;")
    return [
        AppliedSQLiteMigration(version=migration.version, name=migration.name, applied_at=applied_at)
        for migration in migrations
    ]


def _ensure_tracked_prefix(
    connection: sqlite3.Connection,
    database_path: Path,
    migrations: tuple[SQLiteMigration, ...],
    *,
    up_to_version: int,
) -> list[AppliedSQLiteMigration]:
    if up_to_version <= 0:
        return []
    applied_at = _utc_now()
    try:
        connection.execute("BEGIN IMMEDIATE;")
        _ensure_schema_migrations_table(connection)
        for migration in migrations:
            if migration.version > up_to_version:
                break
            connection.execute(
                f"""
                INSERT OR IGNORE INTO {SCHEMA_MIGRATIONS_TABLE} (version, name, applied_at)
                VALUES (?, ?, ?)
                """,
                (migration.version, migration.name, applied_at),
            )
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_STORAGE_ERROR,
            message="SQLite migration metadata backfill failed",
            database_path=database_path,
            details=str(exc),
        ) from exc
    rows = connection.execute(
        f"""
        SELECT version, name, applied_at
        FROM {SCHEMA_MIGRATIONS_TABLE}
        WHERE version <= ?
        ORDER BY version
        """,
        (up_to_version,),
    ).fetchall()
    return [AppliedSQLiteMigration(version=int(row["version"]), name=row["name"], applied_at=row["applied_at"]) for row in rows]


def _load_applied_migrations(
    connection: sqlite3.Connection,
    database_path: Path,
    migrations: tuple[SQLiteMigration, ...],
) -> tuple[AppliedSQLiteMigration, ...]:
    _ensure_schema_migrations_table(connection)
    rows = connection.execute(
        f"""
        SELECT version, name, applied_at
        FROM {SCHEMA_MIGRATIONS_TABLE}
        ORDER BY version
        """
    ).fetchall()
    applied = tuple(
        AppliedSQLiteMigration(version=int(row["version"]), name=row["name"], applied_at=row["applied_at"])
        for row in rows
    )
    known_versions = {migration.version: migration for migration in migrations}
    applied_versions = [migration.version for migration in applied]
    if applied_versions != list(range(1, len(applied_versions) + 1)):
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_INVALID_STATE,
            message="SQLite migration history must be a contiguous prefix of the known migration chain",
            database_path=database_path,
            details=f"applied_versions={applied_versions}",
        )
    for migration in applied:
        known = known_versions.get(migration.version)
        if known is None:
            raise SQLiteMigrationError(
                code=SQLITE_MIGRATION_INVALID_STATE,
                message="SQLite migration history references an unknown migration version",
                database_path=database_path,
                details=f"version={migration.version}",
            )
        if known.name != migration.name:
            raise SQLiteMigrationError(
                code=SQLITE_MIGRATION_INVALID_STATE,
                message="SQLite migration history name does not match the known migration chain",
                database_path=database_path,
                details=f"version={migration.version} expected_name={known.name} actual_name={migration.name}",
            )
    return applied


def _detect_untracked_layout(
    connection: sqlite3.Connection,
    database_path: Path,
    user_tables: list[str] | None = None,
    *,
    allow_tracked_table: bool = False,
) -> tuple[str, int]:
    tables = set(user_tables if user_tables is not None else _list_user_tables(connection))
    if allow_tracked_table:
        tables.discard(SCHEMA_MIGRATIONS_TABLE)
    if not tables:
        return "empty_untracked", 0

    managed_present = {table for table in _MANAGED_TABLES if table in tables}
    if managed_present and managed_present != set(_MANAGED_TABLES):
        missing = sorted(set(_MANAGED_TABLES) - managed_present)
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_INVALID_STATE,
            message="SQLite database contains a partial managed schema",
            database_path=database_path,
            details=f"missing_tables={missing}",
        )
    if not managed_present:
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_INVALID_STATE,
            message="SQLite database does not contain a recognizable managed schema",
            database_path=database_path,
            details=f"user_tables={sorted(tables)}",
        )

    runs_sql = _load_table_sql(connection, "runs", database_path)
    queue_sql = _load_table_sql(connection, "queue_items", database_path)
    runs_has_paused = "paused" in runs_sql.lower()
    queue_has_paused = "paused" in queue_sql.lower()
    if runs_has_paused != queue_has_paused:
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_INVALID_STATE,
            message="SQLite database has a partial paused-state schema drift",
            database_path=database_path,
            details=f"runs_has_paused={runs_has_paused} queue_items_has_paused={queue_has_paused}",
        )
    if runs_has_paused and queue_has_paused:
        return "legacy_untracked_v2", 2
    return "legacy_untracked_v1", 1


def _discover_migrations(migrations_root: Path, database_path: Path) -> tuple[SQLiteMigration, ...]:
    if not migrations_root.exists() or not migrations_root.is_dir():
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_DISCOVERY_FAILED,
            message=f"SQLite migrations directory is missing: {migrations_root}",
            database_path=database_path,
        )

    migrations: list[SQLiteMigration] = []
    for path in sorted(migrations_root.glob("*.sql")):
        match = _MIGRATION_FILENAME_RE.match(path.name)
        if match is None:
            raise SQLiteMigrationError(
                code=SQLITE_MIGRATION_DISCOVERY_FAILED,
                message=f"Invalid SQLite migration filename: {path.name}",
                database_path=database_path,
                details="Expected <NNNN>_<name>.sql",
            )
        migrations.append(
            SQLiteMigration(
                version=int(match.group("version")),
                name=match.group("name"),
                path=path.resolve(),
            )
        )

    if not migrations:
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_DISCOVERY_FAILED,
            message=f"No SQLite migrations were discovered under: {migrations_root}",
            database_path=database_path,
        )

    versions = [migration.version for migration in migrations]
    expected = list(range(1, len(versions) + 1))
    if versions != expected:
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_DISCOVERY_FAILED,
            message="SQLite migrations must form a contiguous non-branching version chain",
            database_path=database_path,
            details=f"expected={expected} actual={versions}",
        )
    return tuple(migrations)


def _resolve_existing_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_DATABASE_NOT_FOUND,
            message=f"Path does not exist: {resolved}",
            database_path=resolved,
        )
    return resolved


def _connect_database(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    return connection


def _list_user_tables(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [row["name"] for row in rows]


def _load_table_sql(connection: sqlite3.Connection, table_name: str, database_path: Path) -> str:
    row = connection.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    if row is None or row["sql"] is None:
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_INVALID_STATE,
            message=f"SQLite managed table is missing or unreadable: {table_name}",
            database_path=database_path,
        )
    return row["sql"]


def _iter_sql_statements(sql_text: str, database_path: Path) -> tuple[str, ...]:
    statements: list[str] = []
    buffer = ""
    for line in sql_text.splitlines(keepends=True):
        buffer += line
        candidate = buffer.strip()
        if not candidate:
            continue
        if sqlite3.complete_statement(candidate):
            if _statement_has_sql(candidate):
                statements.append(candidate)
            buffer = ""
    if _statement_has_sql(buffer):
        raise SQLiteMigrationError(
            code=SQLITE_MIGRATION_STATEMENT_INCOMPLETE,
            message="SQLite migration file contains an incomplete SQL statement",
            database_path=database_path,
        )
    return tuple(statements)


def _statement_has_sql(candidate: str) -> bool:
    for line in candidate.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        return True
    return False


def _ensure_schema_migrations_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_MIGRATIONS_TABLE} (
          version INTEGER PRIMARY KEY CHECK (version >= 1),
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
        """
    )


def _insert_migration_row(connection: sqlite3.Connection, migration: SQLiteMigration, applied_at: str) -> None:
    connection.execute(
        f"""
        INSERT INTO {SCHEMA_MIGRATIONS_TABLE} (version, name, applied_at)
        VALUES (?, ?, ?)
        """,
        (migration.version, migration.name, applied_at),
    )


def _raise_if_foreign_key_check_fails(connection: sqlite3.Connection, database_path: Path) -> None:
    rows = connection.execute("PRAGMA foreign_key_check").fetchall()
    if not rows:
        return
    formatted = "; ".join(
        f"table={row[0]} rowid={row[1]} parent={row[2]} fk_index={row[3]}"
        for row in rows
    )
    raise SQLiteMigrationError(
        code=SQLITE_MIGRATION_INVALID_STATE,
        message="SQLite foreign key check failed after migration",
        database_path=database_path,
        details=formatted,
    )


def _migration_name_for_version(migrations: tuple[SQLiteMigration, ...], version: int) -> str | None:
    for migration in migrations:
        if migration.version == version:
            return migration.name
    return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

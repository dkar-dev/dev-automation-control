from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .sqlite_migrations import MIGRATION_BASELINE_POLICY, SQLiteMigrationApplyResult, migrate_sqlite_v1


@dataclass(frozen=True)
class SQLiteBootstrapResult:
    database_path: Path
    schema_path: Path
    migrations_root: Path
    tables: tuple[str, ...]
    current_version: int
    current_name: str | None
    operation: str
    baseline_policy: str

    def to_dict(self) -> dict[str, object]:
        return {
            "database_path": str(self.database_path),
            "schema_path": str(self.schema_path),
            "migrations_root": str(self.migrations_root),
            "tables": list(self.tables),
            "current_version": self.current_version,
            "current_version_label": f"{self.current_version:04d}",
            "current_name": self.current_name,
            "operation": self.operation,
            "baseline_policy": self.baseline_policy,
        }


def initialize_sqlite_v1(
    database_path: str | Path,
    schema_path: str | Path,
    migrations_root: str | Path,
) -> SQLiteBootstrapResult:
    migration_result = migrate_sqlite_v1(
        database_path,
        schema_path=schema_path,
        migrations_root=migrations_root,
    )
    return _bootstrap_result_from_migration(migration_result)


def _bootstrap_result_from_migration(result: SQLiteMigrationApplyResult) -> SQLiteBootstrapResult:
    return SQLiteBootstrapResult(
        database_path=result.database_path,
        schema_path=result.schema_path,
        migrations_root=result.migrations_root,
        tables=result.tables,
        current_version=result.schema_version.current_version,
        current_name=result.schema_version.current_name,
        operation=result.operation,
        baseline_policy=MIGRATION_BASELINE_POLICY,
    )

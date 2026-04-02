from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3


@dataclass(frozen=True)
class SQLiteBootstrapResult:
    database_path: Path
    schema_path: Path
    tables: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "database_path": str(self.database_path),
            "schema_path": str(self.schema_path),
            "tables": list(self.tables),
        }


def initialize_sqlite_v1(database_path: str | Path, schema_path: str | Path) -> SQLiteBootstrapResult:
    resolved_db_path = Path(database_path).expanduser().resolve()
    resolved_schema_path = Path(schema_path).expanduser().resolve()

    schema_sql = resolved_schema_path.read_text(encoding="utf-8")
    resolved_db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(resolved_db_path)
    try:
        connection.execute("PRAGMA foreign_keys = ON;")
        connection.executescript(schema_sql)
        connection.commit()
        tables = tuple(
            row[0]
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        )
    finally:
        connection.close()

    return SQLiteBootstrapResult(
        database_path=resolved_db_path,
        schema_path=resolved_schema_path,
        tables=tables,
    )

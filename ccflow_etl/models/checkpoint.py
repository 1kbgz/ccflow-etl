import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Literal, Optional

from ccflow import BaseModel
from pydantic import Field

__all__ = (
    "CheckpointRecord",
    "CheckpointStatus",
    "SQLiteCheckpointStore",
)

CheckpointStatus = Literal["planned", "running", "succeeded", "failed", "skipped"]


class CheckpointRecord(BaseModel):
    key: str
    status: CheckpointStatus
    updated_at: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SQLiteCheckpointStore(BaseModel):
    path: str

    def _connect(self):
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                metadata TEXT NOT NULL
            )
            """
        )
        return connection

    def get(self, key: str) -> Optional[CheckpointRecord]:
        with self._connect() as connection:
            row = connection.execute("SELECT key, status, updated_at, metadata FROM checkpoints WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        return CheckpointRecord(key=row[0], status=row[1], updated_at=row[2], metadata=json.loads(row[3]))

    def mark(self, key: str, status: CheckpointStatus, metadata: Optional[Dict[str, Any]] = None) -> CheckpointRecord:
        record = CheckpointRecord(
            key=key,
            status=status,
            updated_at=datetime.now(timezone.utc).isoformat(),
            metadata=metadata or {},
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO checkpoints (key, status, updated_at, metadata)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    metadata = excluded.metadata
                """,
                (record.key, record.status, record.updated_at, json.dumps(record.metadata, sort_keys=True)),
            )
        return record

    def mark_succeeded(self, key: str, metadata: Optional[Dict[str, Any]] = None) -> CheckpointRecord:
        return self.mark(key=key, status="succeeded", metadata=metadata)

    def should_skip(self, key: str) -> bool:
        record = self.get(key)
        return record is not None and record.status == "succeeded"

# dag_engine/state_store.py

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class NodeStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"   # downstream of a failed node that wasn't retried


@dataclass
class NodeRecord:
    node_id: str
    status: NodeStatus = NodeStatus.PENDING
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    attempt: int = 0

    def duration(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return round(self.finished_at - self.started_at, 4)
        return None

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "output": self.output,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_s": self.duration(),
            "attempt": self.attempt,
        }


class ExecutionStateStore:
    """
    Thread-safe in-memory state store for a single DAG execution run.
    Optionally persists to a JSON file after every write.
    """

    def __init__(self, execution_id: str, persist_path: str | None = None):
        self.execution_id = execution_id
        self.persist_path = persist_path
        self._records: Dict[str, NodeRecord] = {}
        self._lock = threading.Lock()
        self.created_at: float = time.time()

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def register_nodes(self, node_ids: list[str]) -> None:
        with self._lock:
            for nid in node_ids:
                if nid not in self._records:
                    self._records[nid] = NodeRecord(node_id=nid)
        self._persist()

    def reset_node(self, node_id: str) -> None:
        """Reset a single node back to PENDING (for retry / partial re-run)."""
        with self._lock:
            self._records[node_id] = NodeRecord(node_id=node_id)
        self._persist()

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def mark_running(self, node_id: str) -> None:
        with self._lock:
            rec = self._records[node_id]
            rec.status = NodeStatus.RUNNING
            rec.started_at = time.time()
            rec.attempt += 1
        self._persist()

    def mark_success(self, node_id: str, output: Dict[str, Any]) -> None:
        with self._lock:
            rec = self._records[node_id]
            rec.status = NodeStatus.SUCCESS
            rec.output = output
            rec.finished_at = time.time()
        self._persist()

    def mark_failed(self, node_id: str, error: str) -> None:
        with self._lock:
            rec = self._records[node_id]
            rec.status = NodeStatus.FAILED
            rec.error = error
            rec.finished_at = time.time()
        self._persist()

    def mark_skipped(self, node_id: str) -> None:
        with self._lock:
            self._records[node_id].status = NodeStatus.SKIPPED
        self._persist()

    # ------------------------------------------------------------------
    # Reads (no lock needed for immutable snapshots in CPython, but kept safe)
    # ------------------------------------------------------------------

    def get_status(self, node_id: str) -> NodeStatus:
        with self._lock:
            return self._records[node_id].status

    def get_output(self, node_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._records[node_id].output

    def is_done(self, node_id: str) -> bool:
        return self.get_status(node_id) in (NodeStatus.SUCCESS, NodeStatus.FAILED, NodeStatus.SKIPPED)

    def all_records(self) -> Dict[str, dict]:
        with self._lock:
            return {nid: rec.to_dict() for nid, rec in self._records.items()}

    def failed_nodes(self) -> list[str]:
        with self._lock:
            return [nid for nid, rec in self._records.items() if rec.status == NodeStatus.FAILED]

    def snapshot(self) -> dict:
        return {
            "execution_id": self.execution_id,
            "created_at": self.created_at,
            "nodes": self.all_records(),
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist(self) -> None:
        if not self.persist_path:
            return
        try:
            with open(self.persist_path, "w") as f:
                json.dump(self.snapshot(), f, indent=2)
        except OSError:
            pass  # non-fatal; engine continues

    @classmethod
    def load_from_file(cls, path: str) -> "ExecutionStateStore":
        with open(path) as f:
            data = json.load(f)
        store = cls(execution_id=data["execution_id"], persist_path=path)
        store.created_at = data["created_at"]
        for nid, rec_data in data["nodes"].items():
            rec = NodeRecord(
                node_id=nid,
                status=NodeStatus(rec_data["status"]),
                output=rec_data.get("output"),
                error=rec_data.get("error"),
                started_at=rec_data.get("started_at"),
                finished_at=rec_data.get("finished_at"),
                attempt=rec_data.get("attempt", 0),
            )
            store._records[nid] = rec
        return store
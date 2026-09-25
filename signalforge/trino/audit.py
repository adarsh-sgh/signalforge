"""Append-only record of every admission decision, plus the outcome of the ones that ran.

Auditable means two things here: a reviewer can see why a query was refused (rule, reason, the
statement itself, the estimate it was judged on), and the estimates can be checked against what
Trino actually read, so the byte budget can be calibrated instead of guessed.
"""
import json
import os
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol

from signalforge.trino.guard import Decision, summarize


@dataclass(frozen=True)
class Outcome:
    query_hash: str
    query_id: str
    state: str
    processed_bytes: Optional[int] = None
    processed_rows: Optional[int] = None
    wall_ms: Optional[int] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        return dict(self.__dict__)


class AuditLog(Protocol):
    def record(self, decision: Decision) -> None: ...
    def record_outcome(self, outcome: Outcome) -> None: ...
    def tail(self, limit: int = 50) -> List[Dict]: ...
    def summary(self) -> Dict: ...


class InMemoryAudit:
    def __init__(self) -> None:
        self.decisions: List[Decision] = []
        self.outcomes: List[Outcome] = []

    def record(self, decision: Decision) -> None:
        self.decisions.append(decision)

    def record_outcome(self, outcome: Outcome) -> None:
        self.outcomes.append(outcome)

    def tail(self, limit: int = 50) -> List[Dict]:
        return [d.to_dict() for d in self.decisions[-limit:]]

    def summary(self) -> Dict:
        return dict(summarize(self.decisions), outcomes=len(self.outcomes),
                    actual_bytes_scanned=sum(o.processed_bytes or 0 for o in self.outcomes))


class JsonlAudit(InMemoryAudit):
    """Same view, durable: one JSON object per line, decisions and outcomes interleaved in order.

    Kept in memory as well so `/decisions` and `/summary` stay cheap; on start the file is replayed
    only for its counts, not the statements, so a long-running service does not grow without bound.
    """

    def __init__(self, path: str) -> None:
        super().__init__()
        self.path = path
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()

    def _append(self, kind: str, payload: Dict) -> None:
        with self._lock, open(self.path, "a") as fh:
            fh.write(json.dumps(dict(payload, record=kind), sort_keys=True) + "\n")

    def record(self, decision: Decision) -> None:
        super().record(decision)
        self._append("decision", decision.to_dict())

    def record_outcome(self, outcome: Outcome) -> None:
        super().record_outcome(outcome)
        self._append("outcome", outcome.to_dict())

    def read_all(self) -> List[Dict]:
        if not os.path.exists(self.path):
            return []
        with open(self.path) as fh:
            return [json.loads(line) for line in fh if line.strip()]


def open_audit(path: Optional[str]) -> AuditLog:
    return JsonlAudit(path) if path else InMemoryAudit()

"""Protobuf <-> dict helpers shared by the producer, the Spark decode UDF and tests."""
import hashlib
from typing import Dict, Optional

from signalforge.events.signal_event_pb2 import SignalEvent

FIELDS = ("event_id", "entity_id", "signal_type", "score", "source", "ts")


def make_event_id(entity_id: str, signal_type: str, source: str, ts: int) -> str:
    """Deterministic id so a retried publish of the same observation dedups."""
    key = "%s|%s|%s|%d" % (entity_id, signal_type, source, ts)
    return hashlib.sha1(key.encode()).hexdigest()[:20]


def encode(entity_id: str, signal_type: str, score: float, source: str, ts: int,
           event_id: Optional[str] = None) -> bytes:
    ev = SignalEvent(
        event_id=event_id or make_event_id(entity_id, signal_type, source, ts),
        entity_id=entity_id, signal_type=signal_type, score=score, source=source, ts=ts,
    )
    return ev.SerializeToString()


def decode(raw: bytes) -> Optional[Dict]:
    """Return a plain dict, or None for undecodable / empty-key payloads (dropped as poison)."""
    if not raw:
        return None
    ev = SignalEvent()
    try:
        ev.ParseFromString(bytes(raw))  # Spark hands BinaryType over as bytearray
    except Exception:
        return None
    if not ev.entity_id or ev.ts <= 0:
        return None
    return {
        "event_id": ev.event_id or make_event_id(ev.entity_id, ev.signal_type, ev.source, ev.ts),
        "entity_id": ev.entity_id, "signal_type": ev.signal_type, "score": ev.score,
        "source": ev.source, "ts": ev.ts,
    }

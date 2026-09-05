"""Publish protobuf SignalEvents to Kafka. `python -m signalforge.producer --n 1000`."""
import argparse
import random
import time
from typing import List, Protocol, Tuple

from signalforge.config import settings
from signalforge.events.codec import encode

SIGNAL_TYPES = ("review", "rating", "return", "click")
SOURCES = ("web", "mobile", "partner")


class Producer(Protocol):
    def produce(self, topic: str, key: bytes, value: bytes) -> None: ...
    def flush(self) -> None: ...


class FakeProducer:
    """In-memory stand-in for confluent_kafka.Producer used by tests."""

    def __init__(self) -> None:
        self.messages: List[Tuple[str, bytes, bytes]] = []

    def produce(self, topic: str, key: bytes, value: bytes) -> None:
        self.messages.append((topic, key, value))

    def flush(self) -> None:
        pass


def kafka_producer(bootstrap: str) -> Producer:
    from confluent_kafka import Producer as _Producer  # imported lazily; needs librdkafka

    return _Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "acks": "all"})


def synth_event(rng: random.Random, n_entities: int, now_ms: int) -> dict:
    return {
        "entity_id": "ent-%04d" % rng.randrange(n_entities),
        "signal_type": rng.choice(SIGNAL_TYPES),
        "score": round(rng.gauss(3.5, 1.0), 3),
        "source": rng.choice(SOURCES),
        "ts": now_ms - rng.randrange(0, 6 * 3600 * 1000),
    }


def publish(producer: Producer, topic: str, n: int, n_entities: int = 200, seed: int = 0,
            duplicate_ratio: float = 0.05, rate: float = 0.0) -> int:
    """Emit n synthetic events; a slice are re-sent verbatim to exercise downstream dedup."""
    rng = random.Random(seed)
    now_ms = int(time.time() * 1000)
    sent = 0
    last = None
    for _ in range(n):
        if last is not None and rng.random() < duplicate_ratio:
            ev = last
        else:
            ev = last = synth_event(rng, n_entities, now_ms)
        producer.produce(topic, ev["entity_id"].encode(), encode(**ev))
        sent += 1
        if rate > 0:
            time.sleep(1.0 / rate)
    producer.flush()
    return sent


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--entities", type=int, default=200)
    ap.add_argument("--rate", type=float, default=0, help="events/sec, 0 = as fast as possible")
    ap.add_argument("--seed", type=int, default=int(time.time()))
    args = ap.parse_args()
    sent = publish(kafka_producer(settings.kafka_bootstrap), settings.kafka_topic,
                   args.n, args.entities, args.seed, rate=args.rate)
    print("published %d events to %s" % (sent, settings.kafka_topic))


if __name__ == "__main__":
    main()

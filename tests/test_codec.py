from signalforge.events.codec import decode, encode, make_event_id
from signalforge.producer import FakeProducer, publish


def test_roundtrip_and_poison():
    raw = encode("ent-1", "review", 4.2, "web", 1700000000000)
    d = decode(raw)
    assert d["entity_id"] == "ent-1" and d["score"] == 4.2 and d["ts"] == 1700000000000
    assert d["event_id"] == make_event_id("ent-1", "review", "web", 1700000000000)
    assert decode(bytearray(raw)) == d  # Spark passes BinaryType as bytearray
    assert decode(b"") is None and decode(b"\xff\xff\xff") is None
    assert decode(encode("", "review", 1.0, "web", 1)) is None  # missing entity id
    d2 = decode(encode("ent-1", "review", 4.2, "web", 1700000000000, tenant_id="acme"))
    assert d2["tenant_id"] == "acme" and d2["event_id"] != d["event_id"]  # same observation, other tenant


def test_publish_emits_decodable_protobuf_with_duplicates():
    p = FakeProducer()
    assert publish(p, "signals", 500, n_entities=10, seed=7, duplicate_ratio=0.2) == 500
    decoded = [decode(v) for _, _, v in p.messages]
    assert all(d is not None for d in decoded)
    assert all(k.decode() == d["tenant_id"] == "default" for (_, k, _), d in zip(p.messages, decoded))
    ids = [d["event_id"] for d in decoded]
    p2 = FakeProducer()
    publish(p2, "signals", 50, seed=1, n_tenants=3)
    assert {k.decode() for _, k, _ in p2.messages} == {"t-00", "t-01", "t-02"}  # keyed by tenant
    assert 50 < len(ids) - len(set(ids)) < 150  # roughly 20% redelivered

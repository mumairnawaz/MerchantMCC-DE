"""S12 — unit tests for src/cdc/checkpoint.py. Pure filesystem logic, no
Kafka needed.
"""

from src.cdc.checkpoint import read_checkpoint, write_checkpoint


def test_read_checkpoint_returns_none_when_never_written(tmp_path):
    assert read_checkpoint("finpay.finpay.clients", 0, root=tmp_path) is None


def test_write_then_read_round_trips(tmp_path):
    write_checkpoint("finpay.finpay.clients", 0, 41, root=tmp_path)
    assert read_checkpoint("finpay.finpay.clients", 0, root=tmp_path) == 41


def test_write_overwrites_previous_checkpoint(tmp_path):
    write_checkpoint("finpay.finpay.clients", 0, 10, root=tmp_path)
    write_checkpoint("finpay.finpay.clients", 0, 20, root=tmp_path)
    assert read_checkpoint("finpay.finpay.clients", 0, root=tmp_path) == 20


def test_different_partitions_are_independent(tmp_path):
    write_checkpoint("finpay.finpay.transactions", 0, 100, root=tmp_path)
    write_checkpoint("finpay.finpay.transactions", 1, 200, root=tmp_path)
    assert read_checkpoint("finpay.finpay.transactions", 0, root=tmp_path) == 100
    assert read_checkpoint("finpay.finpay.transactions", 1, root=tmp_path) == 200


def test_different_topics_are_independent(tmp_path):
    write_checkpoint("finpay.finpay.clients", 0, 11, root=tmp_path)
    write_checkpoint("finpay.finpay.transactions", 0, 3999, root=tmp_path)
    assert read_checkpoint("finpay.finpay.clients", 0, root=tmp_path) == 11
    assert read_checkpoint("finpay.finpay.transactions", 0, root=tmp_path) == 3999


def test_checkpoint_file_contains_expected_metadata(tmp_path):
    path = write_checkpoint("finpay.finpay.clients", 0, 5, root=tmp_path)
    import json

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["topic"] == "finpay.finpay.clients"
    assert data["partition"] == 0
    assert data["last_persisted_offset"] == 5
    assert data["updated_at_utc"]

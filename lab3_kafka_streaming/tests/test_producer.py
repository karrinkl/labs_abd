"""Kafka Producer: заглушка вместо брокера + недоступный брокер."""
import json

import pytest

import producer as P


class FakeMsg:
    def __init__(self, key, partition, offset):
        self._k, self._p, self._o = key, partition, offset

    def key(self):
        return self._k

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def timestamp(self):
        return (1, 1_767_225_600_000)


class FakeProducer:
    """Имитирует confluent_kafka.Producer: копит сообщения, подтверждает на flush()."""

    def __init__(self, fail=False, buffer_errors=0, stuck=0):
        self.sent, self.pending, self.fail = [], [], fail
        self.buffer_errors, self.stuck = buffer_errors, stuck

    def produce(self, topic, key, value, headers, on_delivery):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": dict(headers)})
        self.pending.append((key, on_delivery))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        for i, (key, cb) in enumerate(self.pending):
            cb("broker down" if self.fail else None, FakeMsg(key, i % 2, i // 2))
        self.pending = []
        return self.stuck


LINES = ['{"transaction_id": "S-1", "amount": 10}', '{"transaction_id": "S-2", "amount": 5}', '{"broken": ']


def test_produce_sends_key_value_headers_and_reports_offsets():
    fp = FakeProducer()
    rep = P.produce_lines({}, "t", LINES, batch_label="b1", producer=fp)
    assert [m["key"] for m in fp.sent] == [b"S-1", b"S-2", None]           # битый JSON — без ключа
    assert fp.sent[0]["value"] == LINES[0].encode()
    assert fp.sent[0]["headers"]["source"] == b"lab3-producer" and fp.sent[0]["headers"]["send_batch"] == b"b1"
    assert len(rep) == 3 and all(r["error"] == "" for r in rep)
    s = P.summarize_delivery(rep)
    assert s == {"sent": 3, "delivered": 3, "failed": 0,
                 "partitions": {0: {"min_offset": 0, "max_offset": 1, "count": 2},
                                1: {"min_offset": 0, "max_offset": 0, "count": 1}}}


def test_empty_batch_sends_nothing():
    fp = FakeProducer()
    assert P.produce_lines({}, "t", [], producer=fp) == [] and fp.sent == []


def test_buffer_full_is_retried():
    fp = FakeProducer(buffer_errors=2)
    rep = P.produce_lines({}, "t", LINES[:1], producer=fp)
    assert len(fp.sent) == 1 and rep[0]["error"] == ""


def test_delivery_errors_are_reported():
    rep = P.produce_lines({}, "t", LINES[:2], producer=FakeProducer(fail=True))
    assert P.summarize_delivery(rep)["failed"] == 2


def test_unconfirmed_messages_raise():
    with pytest.raises(P.KafkaUnavailableError):
        P.produce_lines({}, "t", LINES[:2], producer=FakeProducer(stuck=2), flush_timeout=0.1)


def test_repeated_message_is_sent_again_with_same_key():
    fp = FakeProducer()
    P.produce_lines({}, "t", [LINES[0], LINES[0]], producer=fp)
    assert fp.sent[0]["key"] == fp.sent[1]["key"] == b"S-1"   # отбраковка повтора — задача Spark


def test_unavailable_broker_gives_clear_error():
    pytest.importorskip("confluent_kafka")
    with pytest.raises(P.KafkaUnavailableError, match="недоступна"):
        P.check_connection({"bootstrap.servers": "127.0.0.1:1"}, "t", timeout=3)


def test_connection_from_env_requires_bootstrap(monkeypatch):
    monkeypatch.delenv("LAB3_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(P.KafkaSecurityError):
        P.connection_from_env()


def test_cli_reports_missing_config(monkeypatch, capsys, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LAB3_BOOTSTRAP_SERVERS", raising=False)
    assert P.main(["check"]) == 2
    assert "LAB3_BOOTSTRAP_SERVERS" in capsys.readouterr().err


def test_load_dotenv_does_not_override(monkeypatch, tmp_path):
    (tmp_path / ".env").write_text("LAB3_TOPIC=from-file\nLAB3_X='q'\n# comment\n", encoding="utf-8")
    monkeypatch.setenv("LAB3_TOPIC", "from-env")
    monkeypatch.delenv("LAB3_X", raising=False)
    P.load_dotenv(str(tmp_path / ".env"))
    import os
    assert os.environ["LAB3_TOPIC"] == "from-env" and os.environ["LAB3_X"] == "q"


def test_key_extraction():
    assert P._key_of(json.dumps({"transaction_id": "A"})) == b"A"
    assert P._key_of("[1,2]") is None and P._key_of("{") is None

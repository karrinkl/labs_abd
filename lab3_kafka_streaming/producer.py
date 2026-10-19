#!/usr/bin/env python3
"""Kafka Producer для отправки новых банковских транзакций в Aiven (ЛР № 3).

Используется из блокнота Colab (функции) и как самостоятельный скрипт на Arch Linux:

  python producer.py check                         # подключение и наличие топика
  python producer.py send --file data/new_transactions.jsonl
  python producer.py read --max 5                  # прочитать последние сообщения с метаданными

Параметры подключения берутся из переменных окружения (файл .env, см. .env.example):
LAB3_BOOTSTRAP_SERVERS, LAB3_TOPIC, LAB3_AUTH_MODE, LAB3_SECRETS_DIR,
LAB3_SASL_USERNAME, LAB3_SASL_PASSWORD.

Каждая строка файла JSON Lines отправляется как value «как есть» (UTF-8),
поэтому битые строки тоже попадают в топик — их должен отбраковать Spark.
Ключ сообщения — transaction_id (если строку удалось разобрать).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka_security import KafkaConnection, KafkaSecurityError


class KafkaUnavailableError(RuntimeError):
    """Брокер недоступен или топик не найден."""


def _confluent():
    try:
        import confluent_kafka
        import confluent_kafka.admin  # noqa: F401  (подмодуль AdminClient)
        return confluent_kafka
    except ImportError as exc:  # pragma: no cover - зависит от окружения
        raise RuntimeError("Не установлен confluent-kafka: pip install confluent-kafka==2.16.0") from exc


def check_connection(conf: dict, topic: str, timeout: float = 15.0) -> dict:
    """Запросить метаданные кластера. Возвращает число брокеров и партиций топика."""
    ck = _confluent()
    admin = ck.admin.AdminClient({**conf, "socket.connection.setup.timeout.ms": int(timeout * 1000),
                                  # реже переподключаться, чтобы не засорять вывод при недоступном брокере
                                  "reconnect.backoff.ms": 2000, "reconnect.backoff.max.ms": 10000})
    try:
        md = admin.list_topics(timeout=timeout)
    except ck.KafkaException as exc:
        raise KafkaUnavailableError(
            f"Kafka недоступна ({exc.args[0].name() if exc.args else exc}). Проверьте, что сервис Aiven "
            "в состоянии Running (бесплатный сервис выключается после простоя), адрес и порт, сертификаты.") from exc
    if topic not in md.topics:
        raise KafkaUnavailableError(f"Топик {topic!r} не найден. Создайте его в Aiven: Topics → Create topic. "
                                    f"Доступные топики: {sorted(t for t in md.topics if not t.startswith('__'))}")
    t = md.topics[topic]
    if t.error is not None:
        raise KafkaUnavailableError(f"Ошибка топика {topic}: {t.error}")
    return {"brokers": len(md.brokers), "topic": topic, "partitions": len(t.partitions),
            "replicas_per_partition": sorted({len(p.replicas) for p in t.partitions.values()})}


def read_lines(path: Path | str) -> list[str]:
    lines = [ln.rstrip("\n") for ln in Path(path).read_text(encoding="utf-8").splitlines()]
    return [ln for ln in lines if ln.strip()]


def _key_of(line: str) -> bytes | None:
    try:
        tid = json.loads(line).get("transaction_id")
        return str(tid).encode("utf-8") if tid else None
    except (json.JSONDecodeError, AttributeError):
        return None


def produce_lines(conf: dict, topic: str, lines: list[str], batch_label: str | None = None,
                  flush_timeout: float = 60.0, producer=None) -> list[dict]:
    """Отправить строки в топик, дождаться подтверждений и вернуть отчёт о доставке.

    Для каждого сообщения: ключ, партиция, offset, timestamp брокера, ошибка (если была).
    `producer` можно передать для тестов (объект с методами produce/poll/flush).
    """
    if not lines:
        return []
    batch_label = batch_label or f"send-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    if producer is None:
        ck = _confluent()
        producer = ck.Producer({**conf, "acks": "all", "linger.ms": 20, "message.timeout.ms": 30000,
                                "compression.type": "gzip"})
    reports: list[dict] = []

    def on_delivery(err, msg):
        ts_type, ts = msg.timestamp() if msg is not None else (None, None)
        reports.append({
            "key": (msg.key() or b"").decode("utf-8", "replace") if msg is not None else None,
            "partition": msg.partition() if msg is not None else None,
            "offset": msg.offset() if msg is not None else None,
            "timestamp_ms": ts, "timestamp_utc": datetime.fromtimestamp(ts / 1000, timezone.utc).isoformat()
            if ts and ts > 0 else None,
            "error": str(err) if err else "",
        })

    for i, line in enumerate(lines):
        headers = [("source", b"lab3-producer"), ("schema_version", b"1"),
                   ("content_type", b"application/json"), ("send_batch", batch_label.encode()),
                   ("sequence", str(i).encode())]
        while True:
            try:
                producer.produce(topic, key=_key_of(line), value=line.encode("utf-8"),
                                 headers=headers, on_delivery=on_delivery)
                break
            except BufferError:  # локальная очередь заполнена — дать отправиться
                producer.poll(0.5)
        producer.poll(0)
    remaining = producer.flush(flush_timeout)
    if remaining:
        raise KafkaUnavailableError(f"Не подтверждено сообщений: {remaining} за {flush_timeout} с — брокер недоступен?")
    return reports


def read_messages(conf: dict, topic: str, start_offsets: dict[int, int] | None = None,
                  max_messages: int = 10, timeout: float = 20.0) -> list[dict]:
    """Прочитать сообщения с метаданными (partition, offset, timestamp, headers, key, value).

    start_offsets: {partition: offset}; если None — последние max_messages сообщений каждой партиции.
    Используется отдельная временная группа, offset'ы не фиксируются (не мешает Spark).
    """
    ck = _confluent()
    consumer = ck.Consumer({**conf, "group.id": f"lab3-verify-{uuid.uuid4().hex[:8]}",
                            "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    try:
        md = consumer.list_topics(topic, timeout=timeout)
        if topic not in md.topics or md.topics[topic].error is not None:
            raise KafkaUnavailableError(f"Топик {topic} недоступен")
        parts = sorted(md.topics[topic].partitions)
        assignment = []
        for p in parts:
            low, high = consumer.get_watermark_offsets(ck.TopicPartition(topic, p), timeout=timeout)
            if start_offsets is not None:
                if p not in start_offsets:
                    continue
                off = start_offsets[p]
            else:
                off = max(low, high - max_messages)
            if off < high:
                assignment.append(ck.TopicPartition(topic, p, off))
        if not assignment:
            return []
        consumer.assign(assignment)
        out, deadline = [], time.time() + timeout
        while len(out) < max_messages * max(1, len(assignment)) and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                if all(_at_end(consumer, ck, tp, timeout) for tp in consumer.assignment()):
                    break
                continue
            if msg.error():
                continue
            ts_type, ts = msg.timestamp()
            out.append({
                "partition": msg.partition(), "offset": msg.offset(),
                "timestamp_utc": datetime.fromtimestamp(ts / 1000, timezone.utc).isoformat() if ts > 0 else None,
                "key": (msg.key() or b"").decode("utf-8", "replace"),
                "headers": {k: (v or b"").decode("utf-8", "replace") for k, v in (msg.headers() or [])},
                "value": msg.value().decode("utf-8", "replace"),
            })
        return sorted(out, key=lambda r: (r["partition"], r["offset"]))
    finally:
        consumer.close()


def _at_end(consumer, ck, tp, timeout) -> bool:
    pos = consumer.position([tp])[0].offset
    _, high = consumer.get_watermark_offsets(ck.TopicPartition(tp.topic, tp.partition), timeout=timeout)
    return pos >= high


def summarize_delivery(reports: list[dict]) -> dict:
    ok = [r for r in reports if not r["error"]]
    by_part: dict[int, list[int]] = {}
    for r in ok:
        by_part.setdefault(r["partition"], []).append(r["offset"])
    return {"sent": len(reports), "delivered": len(ok), "failed": len(reports) - len(ok),
            "partitions": {p: {"min_offset": min(v), "max_offset": max(v), "count": len(v)}
                           for p, v in sorted(by_part.items())}}


# ---------------------------------------------------------------------------
# CLI для Arch Linux
# ---------------------------------------------------------------------------
def load_dotenv(path: str = ".env"):
    """Минимальная загрузка .env (KEY=VALUE), без перезаписи уже заданных переменных."""
    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def connection_from_env() -> KafkaConnection:
    bootstrap = os.environ.get("LAB3_BOOTSTRAP_SERVERS", "")
    if not bootstrap:
        raise KafkaSecurityError("Не задана LAB3_BOOTSTRAP_SERVERS (скопируйте .env.example в .env)")
    return KafkaConnection(
        bootstrap_servers=bootstrap,
        auth_mode=os.environ.get("LAB3_AUTH_MODE", "certificate"),
        secrets_dir=Path(os.path.expanduser(os.environ.get("LAB3_SECRETS_DIR", "~/.config/lab3-kafka"))),
        sasl_username=os.environ.get("LAB3_SASL_USERNAME"),
        sasl_password=os.environ.get("LAB3_SASL_PASSWORD"),
    )


def main(argv=None):
    load_dotenv()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["check", "send", "read"])
    p.add_argument("--topic", default=os.environ.get("LAB3_TOPIC", "bank-transactions-v2"))
    p.add_argument("--file", default="data/new_transactions.jsonl")
    p.add_argument("--max", type=int, default=5)
    p.add_argument("--report", default="", help="CSV-отчёт о доставке (для send)")
    a = p.parse_args(argv)
    try:
        conn = connection_from_env()
        conf = conn.confluent_config()
        if a.command == "check":
            print(json.dumps(check_connection(conf, a.topic), ensure_ascii=False, indent=2))
        elif a.command == "send":
            check_connection(conf, a.topic)
            reports = produce_lines(conf, a.topic, read_lines(a.file))
            print(json.dumps(summarize_delivery(reports), ensure_ascii=False, indent=2))
            if a.report:
                import lab3_common
                lab3_common.write_csv_atomic(a.report, reports)
        else:
            for m in read_messages(conf, a.topic, max_messages=a.max):
                print(json.dumps(m, ensure_ascii=False))
    except (KafkaSecurityError, KafkaUnavailableError) as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

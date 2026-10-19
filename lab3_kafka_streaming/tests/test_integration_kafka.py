"""Сквозной тест с настоящим брокером Kafka по SSL (локальный стенд local_kafka/).

Запуск (Arch Linux, нужен Docker):
  ./local_kafka/gen_certs.sh && docker compose -f local_kafka/docker-compose.yml up -d
  LAB3_IT_BOOTSTRAP=localhost:9093 LAB3_IT_CERTS=local_kafka/certs pytest -m integration -v
"""
import os
import time
import uuid
from pathlib import Path

import pytest

import generate_data as G
import lab3_common as C

pytestmark = [pytest.mark.integration, pytest.mark.spark,
              pytest.mark.skipif(not os.environ.get("LAB3_IT_BOOTSTRAP"), reason="нет LAB3_IT_BOOTSTRAP (локальный стенд Kafka)")]


@pytest.fixture(scope="module")
def kafka(tmp_path_factory):
    import kafka_security as K
    from confluent_kafka.admin import AdminClient, NewTopic
    certs = Path(os.environ.get("LAB3_IT_CERTS", "local_kafka/certs"))
    conn = K.KafkaConnection(os.environ["LAB3_IT_BOOTSTRAP"], "certificate", tmp_path_factory.mktemp("secrets"))
    K.install_from_files(conn, certs / "ca.pem", certs / "service.cert", certs / "service.key")
    K.validate_pem_files(conn)
    K.build_java_stores(conn)
    topic = f"it-{uuid.uuid4().hex[:8]}"
    admin = AdminClient(conn.confluent_config())
    admin.create_topics([NewTopic(topic, 2, 1)])[topic].result(30)
    # дождаться выбора лидеров партиций: отправка в только что созданный топик иначе может не пройти
    deadline = time.time() + 30
    while time.time() < deadline:
        t = admin.list_topics(topic, timeout=10).topics.get(topic)
        if t is not None and t.error is None and len(t.partitions) == 2 and all(p.leader >= 0 for p in t.partitions.values()):
            break
        time.sleep(0.5)
    yield conn, topic
    admin.delete_topics([topic])


def test_ssl_produce_and_read_back(kafka):
    import producer as P
    conn, topic = kafka
    info = P.check_connection(conn.confluent_config(), topic)
    assert info["partitions"] == 2
    rep = P.produce_lines(conn.confluent_config(), topic, ['{"transaction_id": "IT-1", "amount": 1}'])
    s = P.summarize_delivery(rep)
    assert s["delivered"] == 1
    p, v = next(iter(s["partitions"].items()))
    msgs = P.read_messages(conn.confluent_config(), topic, {p: v["min_offset"]}, max_messages=1)
    assert msgs[0]["key"] == "IT-1" and msgs[0]["headers"]["source"] == "lab3-producer"


def test_structured_streaming_end_to_end(kafka, spark, tmp_path):
    import producer as P
    import spark_jobs as J
    conn, topic = kafka
    s = C.Settings(base_dir=str(tmp_path / "base"), work_dir=str(tmp_path / "work"), topic=topic,
                   trigger_seconds=2, shuffle_partitions=2, batch_tag="IT")
    s.ensure_dirs()
    G.generate_raw(tmp_path / "raw.csv", 3_000, 1, s.period_start, s.period_end)
    hist = J.clean_batch(J.read_raw_csv(spark, str(tmp_path / "raw.csv")), s)["clean"].cache()
    proc = J.StreamProcessor(spark, s, J.dimension_stats_df(hist).cache(), hist.select("transaction_id").cache())
    before = next(r for r in proc.combined_stats() if r["dimension"] == "total")["tx_count"]
    qs = J.start_streams(spark, conn.spark_options(), s, proc)
    try:
        G.generate_new(s.new_jsonl, 40, 3, s.new_period_start, "IT", invalid=5, repeats=2, malformed=1)
        rep = P.produce_lines(conn.confluent_config(), topic, P.read_lines(s.new_jsonl))
        errors = [r["error"] for r in rep if r["error"]]
        assert not errors, errors[:3]
        ids = {f"S-IT-{i:06d}" for i in range(1, 41)}
        w = J.wait_for_transactions(s, spark, ids, query=qs["accepted_stream"], timeout_s=120)
        assert w["complete"], w
        after = next(r for r in C.read_csv(s.results_dir / "updated_stream_statistics.csv") if r["dimension"] == "total")
        assert int(after["tx_count"]) == before + 40
        J.wait_for_new_input(qs["stream_type_counts"], -1, 1, timeout_s=60)
        time.sleep(s.trigger_seconds * 2)
        counted = spark.sql("SELECT SUM(tx_count) FROM stream_type_counts").first()[0]
        assert counted == 40
    finally:
        J.stop_streams(spark)
    assert not [q for q in spark.streams.active]
    assert (s.checkpoint_dir / "accepted_stream" / "offsets").is_dir()

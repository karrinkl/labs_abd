"""Spark: пакетная очистка/агрегаты и логика обработки микропакета (без Kafka)."""
import json
from datetime import datetime, timezone

import pytest

import generate_data as G
import lab3_common as C

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def batch(spark, tmp_path_factory):
    import spark_jobs as J
    d = tmp_path_factory.mktemp("batch")
    s = C.Settings(base_dir=str(d / "base"), work_dir=str(d / "work"), shuffle_partitions=2)
    G.generate_raw(d / "raw.csv", 8_000, 11, s.period_start, s.period_end)
    raw = J.read_raw_csv(spark, str(d / "raw.csv")).cache()
    res = J.clean_batch(raw, s)
    return s, raw, res


def test_schema_and_shape(batch):
    s, raw, _ = batch
    assert raw.columns == C.ALL_FIELDS
    assert all(t == "string" for _, t in raw.dtypes)
    assert raw.count() == 8_080


def test_quality_report_detects_problems(batch, spark):
    import spark_jobs as J
    s, raw, _ = batch
    q = {r["check"]: r["rows"] for r in J.quality_report(raw, s)}
    assert q["full_duplicate_rows"] > 0 and q["duplicated_transaction_ids"] > 0
    assert sum(v for k, v in q.items() if k.startswith("missing_")) > 0
    assert q["amount_not_numeric"] + q["amount_non_positive"] > 0
    assert q["timestamp_unparseable"] + q["timestamp_out_of_range"] > 0


def test_cleaning_balance_types_and_uniqueness(batch):
    _, _, res = batch
    clean = res["clean"]
    assert res["balance_ok"] and res["clean_rows"] < res["raw_rows"]
    assert clean.count() == clean.select("transaction_id").distinct().count()
    types = dict(clean.dtypes)
    assert types["amount"].startswith("decimal") and types["timestamp"] == "timestamp"
    assert clean.filter("amount <= 0 OR timestamp IS NULL OR currency IS NULL").count() == 0
    assert {r[0] for r in clean.select("currency").distinct().collect()} <= set(C.CURRENCIES)


def test_aggregates_are_consistent(batch):
    import spark_jobs as J
    _, _, res = batch
    clean = res["clean"]
    n = res["clean_rows"]
    assert sum(r["tx_count"] for r in J.group_statistics(clean, ["city"])) == n
    assert sum(r["tx_count"] for r in J.group_statistics(clean, ["currency"], "amount")) == n
    num = {r["column"]: r for r in J.numeric_statistics(clean)}
    assert set(num) == {"amount", "fee", "amount_eur"}                # идентификаторы исключены
    assert num["amount_eur"]["min"] <= num["amount_eur"]["median"] <= num["amount_eur"]["max"]
    top = J.top_categories(clean, 5)
    for col in J.CATEGORICAL:
        ranks = [r for r in top if r["column"] == col]
        assert 1 <= len(ranks) <= 5 and ranks == sorted(ranks, key=lambda r: -r["count"])
    dims = J.rows_from_dimension_df(J.dimension_stats_df(clean))
    assert next(r for r in dims if r["dimension"] == "total")["tx_count"] == n


# ---------------------------------------------------------------- микропакет потока
def _kafka_batch(spark, values, start_offset=0):
    """Статический DataFrame со схемой источника Kafka (как микропакет в foreachBatch)."""
    from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
    schema = StructType([StructField("key", BinaryType()), StructField("value", BinaryType()),
                         StructField("topic", StringType()), StructField("partition", IntegerType()),
                         StructField("offset", LongType()), StructField("timestamp", TimestampType())])
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(None, (v.encode() if v is not None else None), "t", 0, start_offset + i, now) for i, v in enumerate(values)]
    import spark_jobs as J
    return J.parse_messages(spark.createDataFrame(rows, schema))


def _tx(i, **kw):
    r = {"transaction_id": f"S-UT-{i:04d}", "client_id": "C1", "transaction_type": "payment", "amount": 100.5,
         "currency": "EUR", "city": "Moscow", "device_type": "mobile", "timestamp": "2026-01-02 10:00:00", "fee": 0}
    r.update(kw)
    return json.dumps({k: v for k, v in r.items() if v is not ...})


@pytest.fixture
def processor(spark, batch, tmp_path):
    import spark_jobs as J
    s0, _, res = batch
    s = C.Settings(base_dir=str(tmp_path / "base"), work_dir=str(tmp_path / "work"), shuffle_partitions=2)
    s.ensure_dirs()
    hist = res["clean"]
    return s, J.StreamProcessor(spark, s, J.dimension_stats_df(hist).cache(), hist.select("transaction_id").cache(),
                                ts_max="2030-01-01")


def _total(s):
    return int(next(r for r in C.read_csv(s.results_dir / "updated_stream_statistics.csv") if r["dimension"] == "total")["tx_count"])


def test_micro_batch_validation_and_edge_cases(spark, processor):
    s, proc = processor
    base = proc.combined_stats()
    base_total = next(r for r in base if r["dimension"] == "total")["tx_count"]
    values = [_tx(1), _tx(2), _tx(2),                         # повтор внутри пакета
              _tx(3, client_id=...),                          # нет обязательного поля
              _tx(4, amount="abc"), _tx(5, amount=-1),        # неверная сумма
              _tx(6, currency="XXX"), _tx(7, timestamp="2026-02-30 25:00:00"),
              '{"transaction_id": "S-UT-BROKEN", "amount": ', "", None]   # битый JSON / пустые
    proc(_kafka_batch(spark, values), 0)
    log = C.read_csv(s.results_dir / "stream_batches.csv")[-1]
    assert log["accepted"] == "2" and log["rows_in"] == str(len(values))
    for reason in ("duplicate_in_batch=1", "missing_client_id=1", "invalid_amount_format=1",
                   "non_positive_amount=1", "unknown_currency=1", "invalid_timestamp=1", "invalid_json=3"):
        assert reason in log["rejected_by_reason"], reason
    assert _total(s) == base_total + 2


def test_repeated_and_replayed_batches_do_not_double_count(spark, processor):
    s, proc = processor
    proc(_kafka_batch(spark, [_tx(10), _tx(11)]), 0)
    t1 = _total(s)
    proc(_kafka_batch(spark, [_tx(10), _tx(11)]), 0)         # повтор того же batch_id (восстановление после сбоя)
    assert _total(s) == t1
    proc(_kafka_batch(spark, [_tx(10), _tx(12)], 2), 1)      # повтор уже обработанной транзакции в новом пакете
    log = C.read_csv(s.results_dir / "stream_batches.csv")
    assert _total(s) == t1 + 1
    assert "duplicate_already_processed=1" in log[-1]["rejected_by_reason"]
    assert [r["batch_id"] for r in log] == ["0", "1"]          # журнал без дублей batch_id


def test_empty_micro_batch_is_noop(spark, processor):
    s, proc = processor
    proc(_kafka_batch(spark, []), 5)
    assert not (s.results_dir / "stream_batches.csv").exists()

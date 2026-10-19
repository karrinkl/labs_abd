"""PySpark: пакетная обработка CSV и Spark Structured Streaming из Kafka (ЛР № 3).

Ключевая идея: исходные данные (CSV и JSON из Kafka) читаются с ЯВНОЙ
схемой из строковых полей, после чего ОДНИ И ТЕ ЖЕ функции normalize() и
flag_rejects() безопасно приводят типы (try_cast / try_to_timestamp) и
назначают каждой некорректной записи одну причину отказа. Поэтому правила
очистки исторических и потоковых данных совпадают.

Модель «история + поток»:
  * очищенная история сохраняется в Parquet (clean_history.parquet);
  * принятые потоковые транзакции дописываются в устойчивое хранилище
    stream_transactions/batch_id=<N>/ внутри foreachBatch (повтор того же
    batch_id перезаписывает только его каталог — запись идемпотентна);
  * после каждого микропакета итоговые показатели ПЕРЕСЧИТЫВАЮТСЯ как
    агрегаты истории + агрегаты всех сохранённых потоковых транзакций.
Так результат не зависит от того, сколько раз перезапускался поток.
"""
from __future__ import annotations

import logging
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import lab3_common as C

log = logging.getLogger("lab3")

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in C.ALL_FIELDS])
JSON_SCHEMA = StructType([StructField(c, StringType(), True) for c in C.ALL_FIELDS]
                         + [StructField("_corrupt_record", StringType(), True)])
CATEGORICAL = ["transaction_type", "currency", "city", "device_type"]
NUMERIC = ["amount", "fee", "amount_eur"]
DIMENSIONS = ["city", "currency", "transaction_type", "device_type"]


# ===========================================================================
# SparkSession
# ===========================================================================
def kafka_package_coordinate(spark_version: str) -> str:
    """Координата Maven коннектора Kafka, совпадающая с версией Spark и Scala."""
    major = int(spark_version.split(".")[0])
    scala = "2.13" if major >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{spark_version}"


def get_spark(settings: C.Settings, app_name: str = "lab3-bank-transactions", with_kafka: bool = True,
              master: str = "local[*]", driver_memory: str = "4g") -> SparkSession:
    """Создать SparkSession один раз; при повторном запуске ячейки вернуть существующую.

    Пакет Kafka можно подключить только до старта JVM: если активная сессия
    создана без него, нужно перезапустить среду выполнения (Runtime → Restart).
    """
    import pyspark

    active = SparkSession.getActiveSession()
    pkg = settings.kafka_package or kafka_package_coordinate(pyspark.__version__)
    if active is not None:
        loaded = active.sparkContext.getConf().get("spark.jars.packages", "")
        if with_kafka and "spark-sql-kafka" not in loaded:
            raise RuntimeError("Активная SparkSession создана без коннектора Kafka. "
                               "Выполните Runtime → Restart session и запустите ячейки заново.")
        log.info("Используется существующая SparkSession (%s)", active.sparkContext.applicationId)
        return active
    b = (SparkSession.builder.appName(app_name).master(master)
         .config("spark.driver.memory", driver_memory)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.jars.ivy", str(Path(settings.work_dir).parent / ".ivy2")))
    if with_kafka:
        b = b.config("spark.jars.packages", pkg)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession: Spark %s, master %s, app %s, kafka package %s", spark.version,
             spark.sparkContext.master, spark.sparkContext.applicationId, pkg if with_kafka else "-")
    return spark


def environment_info(spark: SparkSession) -> dict:
    import platform
    import sys
    sc = spark.sparkContext
    jvm = sc._jvm.java.lang.System
    return {"spark_version": spark.version, "python": sys.version.split()[0], "platform": platform.platform(),
            "java_version": jvm.getProperty("java.version"), "java_vendor": jvm.getProperty("java.vendor"),
            "master": sc.master, "default_parallelism": sc.defaultParallelism, "app_id": sc.applicationId,
            "spark_ui": sc.uiWebUrl, "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "kafka_package": sc.getConf().get("spark.jars.packages", "")}


# ===========================================================================
# Общие правила нормализации и проверки
# ===========================================================================
def _blank_to_null(c):
    t = F.trim(F.col(c))
    return F.when(t == "", F.lit(None)).otherwise(t)


def _lit_map(d: dict, value_cast: str | None = None):
    items = []
    for k, v in d.items():
        val = F.lit(v).cast(value_cast) if value_cast else F.lit(v)
        items += [F.lit(k), val]
    return F.create_map(*items)


def normalize(df: DataFrame) -> DataFrame:
    """Строковые поля -> нормализованные и типизированные столбцы (исходные сохраняются)."""
    for c in C.ALL_FIELDS:
        df = df.withColumn(f"{c}_n", _blank_to_null(c))
    rates = _lit_map(C.EUR_RATES, "decimal(12,6)")
    cities = _lit_map({k.lower(): k for k in C.CITIES})
    ts_primary = F.try_to_timestamp(F.col("timestamp_n"), F.lit(C.SPARK_TIMESTAMP_FORMAT))
    ts_iso = F.try_to_timestamp(F.col("timestamp_n"), F.lit("yyyy-MM-dd'T'HH:mm:ss"))
    return (df
            .withColumn("amount_dec", F.expr("try_cast(amount_n AS DECIMAL(18,2))"))
            .withColumn("fee_dec", F.coalesce(F.expr("try_cast(fee_n AS DECIMAL(18,2))"), F.lit(0).cast("decimal(18,2)")))
            .withColumn("currency_norm", F.upper(F.col("currency_n")))
            .withColumn("rate_eur", F.try_element_at(rates, F.col("currency_norm")))
            .withColumn("city_norm", F.try_element_at(cities, F.lower(F.col("city_n"))))
            .withColumn("type_norm", F.lower(F.col("transaction_type_n")))
            .withColumn("device_norm", F.lower(F.col("device_type_n")))
            .withColumn("ts_parsed", F.coalesce(ts_primary, ts_iso))
            .withColumn("ts_primary_ok", ts_primary.isNotNull()))


def reject_rules(ts_min: str, ts_max: str) -> list[tuple[str, str]]:
    """Порядок важен: строка получает ПЕРВУЮ сработавшую причину (без двойного учёта)."""
    return [
        ("missing_transaction_id", "transaction_id_n IS NULL"),
        ("missing_client_id", "client_id_n IS NULL"),
        ("missing_amount", "amount_n IS NULL"),
        ("invalid_amount_format", "amount_dec IS NULL"),
        ("non_positive_amount", "amount_dec <= 0"),
        ("amount_too_large", f"amount_dec > {C.MAX_REASONABLE_AMOUNT}"),
        ("missing_timestamp", "timestamp_n IS NULL"),
        ("invalid_timestamp", "ts_parsed IS NULL"),
        ("timestamp_out_of_range",
         f"ts_parsed < TIMESTAMP '{ts_min} 00:00:00' OR ts_parsed >= TIMESTAMP '{ts_max} 00:00:00'"),
        ("missing_currency", "currency_n IS NULL"),
        ("unknown_currency", "rate_eur IS NULL"),
    ]


def flag_rejects(norm: DataFrame, ts_min: str, ts_max: str, extra_first: list | None = None) -> DataFrame:
    rules = (extra_first or []) + reject_rules(ts_min, ts_max)
    reason = F.lit(None).cast("string")
    for name, cond in reversed(rules):
        reason = F.when(F.expr(cond), F.lit(name)).otherwise(reason)
    return norm.withColumn("reject_reason", reason)


def to_clean(valid: DataFrame, keep: tuple[str, ...] = ()) -> DataFrame:
    """Итоговая типизированная схема; неизвестные категории -> 'unknown'.

    keep — дополнительные столбцы, которые нужно сохранить (например, метаданные Kafka).
    """
    known_types = list(C.TRANSACTION_TYPES)
    known_devices = list(C.DEVICE_TYPES)
    return valid.select(
        F.col("transaction_id_n").alias("transaction_id"),
        F.col("client_id_n").alias("client_id"),
        F.when(F.col("type_norm").isin(known_types), F.col("type_norm")).otherwise(F.lit(C.UNKNOWN)).alias("transaction_type"),
        F.col("amount_dec").alias("amount"),
        F.col("currency_norm").alias("currency"),
        F.coalesce(F.col("city_norm"), F.lit(C.UNKNOWN_CITY)).alias("city"),
        F.when(F.col("device_norm").isin(known_devices), F.col("device_norm")).otherwise(F.lit(C.UNKNOWN)).alias("device_type"),
        F.col("ts_parsed").alias("timestamp"),
        F.col("fee_dec").alias("fee"),
        (F.col("amount_dec") * F.col("rate_eur")).cast("decimal(18,2)").alias("amount_eur"),
        (F.col("fee_dec") * F.col("rate_eur")).cast("decimal(18,2)").alias("fee_eur"),
        *keep,
    )


def dedupe_by_id(valid: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Оставить одну запись на transaction_id (детерминированно), вернуть (kept, removed)."""
    w = Window.partitionBy("transaction_id_n").orderBy(F.col("ts_primary_ok").desc(), F.xxhash64(*C.ALL_FIELDS))
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    return ranked.filter("_rn = 1").drop("_rn"), ranked.filter("_rn > 1").drop("_rn")


# ===========================================================================
# Пакетная обработка
# ===========================================================================
def read_raw_csv(spark: SparkSession, path: str) -> DataFrame:
    return (spark.read.option("header", "true").option("mode", "PERMISSIVE").option("encoding", "UTF-8")
            .schema(RAW_SCHEMA).csv(path))


def quality_report(raw: DataFrame, settings: C.Settings) -> list[dict]:
    """Проверка качества исходных строк (до очистки)."""
    n = raw.count()
    norm = normalize(raw)
    c = F.col
    checks = []
    for col in C.ALL_FIELDS:
        checks.append((f"missing_{col}", col, f"{col}_n IS NULL", "NULL или пустая строка"))
    known_t, known_d = list(C.TRANSACTION_TYPES), list(C.DEVICE_TYPES)
    checks += [
        ("amount_not_numeric", "amount", "amount_n IS NOT NULL AND amount_dec IS NULL", "не число"),
        ("amount_non_positive", "amount", "amount_dec <= 0", "сумма <= 0"),
        ("amount_too_large", "amount", f"amount_dec > {C.MAX_REASONABLE_AMOUNT}", f"сумма > {C.MAX_REASONABLE_AMOUNT:,}"),
        ("timestamp_unparseable", "timestamp", "timestamp_n IS NOT NULL AND ts_parsed IS NULL", "не разбирается"),
        ("timestamp_iso_format", "timestamp", "ts_parsed IS NOT NULL AND NOT ts_primary_ok", "формат ISO с 'T' (исправимо)"),
        ("timestamp_out_of_range", "timestamp",
         f"ts_parsed < TIMESTAMP '{settings.period_start} 00:00:00' OR ts_parsed >= TIMESTAMP '{settings.period_end} 00:00:00'",
         f"вне [{settings.period_start}; {settings.period_end})"),
        ("currency_unknown", "currency", "currency_n IS NOT NULL AND rate_eur IS NULL", "нет в справочнике"),
        ("currency_wrong_case", "currency", "rate_eur IS NOT NULL AND currency <> currency_norm", "регистр (исправимо)"),
        ("city_unknown", "city", "city_n IS NOT NULL AND city_norm IS NULL", "нет в справочнике"),
        ("city_needs_normalization", "city", "city_norm IS NOT NULL AND city <> city_norm", "регистр/пробелы (исправимо)"),
        ("transaction_type_unknown", "transaction_type",
         f"type_norm IS NOT NULL AND type_norm NOT IN ({','.join(repr(t) for t in known_t)})", "нет в справочнике"),
        ("device_type_unknown", "device_type",
         f"device_norm IS NOT NULL AND device_norm NOT IN ({','.join(repr(t) for t in known_d)})", "нет в справочнике"),
        ("device_type_needs_normalization", "device_type",
         f"device_norm IN ({','.join(repr(t) for t in known_d)}) AND device_type <> device_norm", "регистр (исправимо)"),
    ]
    agg = norm.agg(*[F.sum(F.when(F.expr(cond), 1).otherwise(0)).alias(name) for name, _, cond, _ in checks]).first()
    rows = [{"check": name, "column": col, "rows": int(agg[name] or 0),
             "share_pct": round(100 * (agg[name] or 0) / n, 3) if n else 0, "description": desc}
            for name, col, _, desc in checks]
    distinct_rows = raw.dropDuplicates().count()
    dup_ids = (norm.filter(c("transaction_id_n").isNotNull()).groupBy("transaction_id_n").count()
               .filter("count > 1").agg(F.count(F.lit(1)).alias("ids"), F.sum("count").alias("rows")).first())
    rows += [
        {"check": "full_duplicate_rows", "column": "*", "rows": n - distinct_rows,
         "share_pct": round(100 * (n - distinct_rows) / n, 3) if n else 0, "description": "лишние копии полностью совпадающих строк"},
        {"check": "duplicated_transaction_ids", "column": "transaction_id", "rows": int(dup_ids["ids"] or 0),
         "share_pct": None, "description": f"id встречается >1 раза (строк с ними: {int(dup_ids['rows'] or 0)})"},
    ]
    return rows


def clean_batch(raw: DataFrame, settings: C.Settings) -> dict:
    """Очистка исторического набора. Возвращает DataFrame и сводку по шагам."""
    n_raw = raw.count()
    dedup = raw.dropDuplicates()                      # 1. полные дубликаты
    n_dedup = dedup.count()
    flagged = flag_rejects(normalize(dedup), settings.period_start, settings.period_end)
    reasons = {r["reject_reason"]: r["count"] for r in
               flagged.filter("reject_reason IS NOT NULL").groupBy("reject_reason").count().collect()}
    valid = flagged.filter("reject_reason IS NULL")
    kept, dup_ids = dedupe_by_id(valid)               # 3. частичные дубликаты по transaction_id
    clean = to_clean(kept)
    n_clean = clean.count()
    n_dup_ids = (n_dedup - sum(reasons.values())) - n_clean
    rejected = (flagged.filter("reject_reason IS NOT NULL").select(*C.ALL_FIELDS, "reject_reason")
                .unionByName(dup_ids.select(*C.ALL_FIELDS).withColumn("reject_reason", F.lit("duplicate_transaction_id"))))
    corrections = kept.agg(
        F.sum(F.when(F.col("city_n").isNull() | F.col("city_norm").isNull(), 1).otherwise(0)).alias("city_set_unknown"),
        F.sum(F.when(F.col("city_norm").isNotNull() & (F.col("city") != F.col("city_norm")), 1).otherwise(0)).alias("city_normalized"),
        F.sum(F.when(F.col("currency") != F.col("currency_norm"), 1).otherwise(0)).alias("currency_case_fixed"),
        F.sum(F.when(~F.col("ts_primary_ok"), 1).otherwise(0)).alias("timestamp_reformatted"),
        F.sum(F.when(F.col("type_norm").isNull() | ~F.col("type_norm").isin(list(C.TRANSACTION_TYPES)), 1).otherwise(0)).alias("type_set_unknown"),
        F.sum(F.when(F.col("device_norm").isNull() | ~F.col("device_norm").isin(list(C.DEVICE_TYPES)), 1).otherwise(0)).alias("device_set_unknown"),
    ).first().asDict()
    steps = [{"step": "0_raw_rows", "rows": n_raw, "description": "строк в исходном CSV (без заголовка)"},
             {"step": "1_full_duplicates_removed", "rows": n_raw - n_dedup, "description": "удалены полные дубликаты"}]
    for reason, cnt in sorted(reasons.items(), key=lambda kv: -kv[1]):
        steps.append({"step": f"2_rejected_{reason}", "rows": cnt, "description": "отклонено (одна причина на строку)"})
    steps += [{"step": "3_duplicate_transaction_id_removed", "rows": n_dup_ids,
               "description": "повтор transaction_id с другим оформлением"},
              {"step": "4_clean_rows", "rows": n_clean, "description": "строк после очистки"}]
    for k, v in corrections.items():
        steps.append({"step": f"fix_{k}", "rows": int(v or 0), "description": "исправлено в принятых строках (не удаление)"})
    balance_ok = n_raw == (n_raw - n_dedup) + sum(reasons.values()) + n_dup_ids + n_clean
    return {"clean": clean, "rejected": rejected, "steps": steps, "raw_rows": n_raw, "clean_rows": n_clean,
            "balance_ok": balance_ok}


def numeric_statistics(clean: DataFrame) -> list[dict]:
    out = []
    for col in NUMERIC:
        r = clean.agg(F.count(col).alias("n"), F.avg(col).alias("mean"), F.median(col).alias("median"),
                      F.stddev_samp(col).alias("std"), F.min(col).alias("min"), F.max(col).alias("max")).first()
        out.append({"column": col, "count": r["n"], "mean": _r(r["mean"]), "median": _r(r["median"]),
                    "stddev": _r(r["std"]), "min": _r(r["min"]), "max": _r(r["max"]),
                    "note": {"amount": "в исходной валюте операции (валюты смешаны — см. amount_eur)",
                             "fee": "комиссия в валюте операции",
                             "amount_eur": "сумма в EUR по условным курсам — сопоставимая величина"}[col]})
    return out


def _r(v, nd=2):
    return None if v is None else round(float(v), nd)


def top_categories(clean: DataFrame, n: int = 5) -> list[dict]:
    total = clean.count()
    rows = []
    for col in CATEGORICAL:
        for i, r in enumerate(clean.groupBy(col).count().orderBy(F.desc("count"), F.asc(col)).limit(n).collect(), 1):
            rows.append({"column": col, "rank": i, "value": r[col], "count": r["count"],
                         "share_pct": round(100 * r["count"] / total, 2)})
    return rows


def group_statistics(clean: DataFrame, by: list[str], amount_col: str = "amount_eur") -> list[dict]:
    g = (clean.groupBy(*by).agg(F.count(F.lit(1)).alias("tx_count"),
                                F.sum(amount_col).alias(f"{amount_col}_sum"),
                                F.avg(amount_col).alias(f"{amount_col}_avg"),
                                F.sum("fee_eur").alias("fee_eur_sum"))
         .orderBy(F.desc("tx_count"), *[F.asc(b) for b in by]))
    return [{**{b: r[b] for b in by}, "tx_count": r["tx_count"],
             f"{amount_col}_sum": _r(r[f"{amount_col}_sum"]), f"{amount_col}_avg": _r(r[f"{amount_col}_avg"]),
             "fee_eur_sum": _r(r["fee_eur_sum"])} for r in g.collect()]


def dimension_stats_df(clean: DataFrame) -> DataFrame:
    """Длинный формат (dimension, value, tx_count, amount_eur_sum) по всем измерениям."""
    parts = [clean.groupBy(F.lit(d).alias("dimension"), F.col(d).cast("string").alias("value"))
             .agg(F.count(F.lit(1)).alias("tx_count"), F.sum("amount_eur").alias("amount_eur_sum"))
             for d in DIMENSIONS]
    parts.append(clean.agg(F.lit("total").alias("dimension"), F.lit("all").alias("value"),
                           F.count(F.lit(1)).alias("tx_count"), F.sum("amount_eur").alias("amount_eur_sum")))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def rows_from_dimension_df(df: DataFrame) -> list[dict]:
    return [{"dimension": r["dimension"], "value": r["value"], "tx_count": int(r["tx_count"]),
             "amount_eur_sum": _r(r["amount_eur_sum"]),
             "avg_amount_eur": _r(r["amount_eur_sum"] / r["tx_count"]) if r["tx_count"] else None}
            for r in df.orderBy("dimension", F.desc("tx_count"), "value").collect()]


# ===========================================================================
# Потоковая обработка
# ===========================================================================
def kafka_stream(spark: SparkSession, kafka_options: dict, settings: C.Settings) -> DataFrame:
    return (spark.readStream.format("kafka").options(**kafka_options)
            .option("subscribe", settings.topic)
            .option("startingOffsets", settings.starting_offsets)
            .option("failOnDataLoss", "false")            # retention Aiven Free удаляет старые сообщения
            .option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
            .option("includeHeaders", "true")
            .load())


def parse_messages(kdf: DataFrame) -> DataFrame:
    """value (байты) -> строка -> JSON по явной схеме; метаданные Kafka сохраняются."""
    value = F.col("value").cast("string")
    parsed = F.from_json(value, JSON_SCHEMA, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"})
    return (kdf.select(F.col("key").cast("string").alias("kafka_key"), value.alias("raw_value"),
                       F.col("topic"), F.col("partition"), F.col("offset"),
                       F.col("timestamp").alias("kafka_timestamp"), parsed.alias("j"))
            .select("kafka_key", "raw_value", "topic", "partition", "offset", "kafka_timestamp", "j.*")
            .withColumn("json_ok", F.col("_corrupt_record").isNull()
                        & F.col("raw_value").isNotNull() & (F.trim(F.col("raw_value")) != "")))


def validate_stream_batch(parsed: DataFrame, ts_min: str, ts_max: str) -> DataFrame:
    norm = normalize(parsed)
    return flag_rejects(norm, ts_min, ts_max, extra_first=[("invalid_json", "NOT json_ok")])


class StreamProcessor:
    """Обработчик foreachBatch: проверка, дедупликация, сохранение, пересчёт итогов."""

    def __init__(self, spark: SparkSession, settings: C.Settings, history_stats: DataFrame, history_ids: DataFrame,
                 ts_max: str | None = None):
        self.spark, self.s = spark, settings
        self.history_stats = history_stats      # агрегаты истории (малый DataFrame, кэширован)
        self.history_ids = history_ids          # transaction_id истории (кэширован)
        self.ts_max = ts_max or (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        self.batch_log = settings.results_dir / "stream_batches.csv"
        self.updated_csv = settings.results_dir / "updated_stream_statistics.csv"

    # ---- хранилище потоковых транзакций
    def stored_stream(self, exclude_batch: int | None = None) -> DataFrame | None:
        if not _has_parquet(self.s.stream_store_dir):
            return None
        df = self.spark.read.parquet(str(self.s.stream_store_dir))
        return df.filter(F.col("batch_id") != exclude_batch) if exclude_batch is not None else df

    def combined_stats(self) -> list[dict]:
        stats = self.history_stats
        stored = self.stored_stream()
        if stored is not None:
            stats = stats.unionByName(dimension_stats_df(stored))
        combined = stats.groupBy("dimension", "value").agg(F.sum("tx_count").alias("tx_count"),
                                                           F.sum("amount_eur_sum").alias("amount_eur_sum"))
        return rows_from_dimension_df(combined)

    def __call__(self, batch_df: DataFrame, batch_id: int):
        t0 = time.time()
        rows_in = batch_df.count()
        if rows_in == 0:
            log.info("batch %s: пустой микропакет — новых сообщений нет", batch_id)
            return
        flagged = validate_stream_batch(batch_df, self.s.new_period_start, self.ts_max).cache()
        valid = flagged.filter("reject_reason IS NULL")
        kept, dup_in_batch = dedupe_by_id(valid)
        # повтор уже обработанных ранее (в прошлых пакетах или в истории)
        seen = self.history_ids
        prev = self.stored_stream(exclude_batch=batch_id)
        if prev is not None:
            seen = seen.unionByName(prev.select("transaction_id"))
        kept_new = kept.join(seen.withColumnRenamed("transaction_id", "transaction_id_n"),
                             on="transaction_id_n", how="left_anti")
        dup_seen = kept.join(seen.withColumnRenamed("transaction_id", "transaction_id_n"),
                             on="transaction_id_n", how="left_semi")
        accepted = (to_clean(kept_new, keep=("partition", "offset", "kafka_timestamp"))
                    .withColumnRenamed("partition", "kafka_partition")
                    .withColumnRenamed("offset", "kafka_offset")
                    .withColumn("processed_at", F.current_timestamp()))
        n_acc = accepted.count()
        # Перезапись каталога ТОЛЬКО этого batch_id: повтор пакета после сбоя идемпотентен.
        out_dir = self.s.stream_store_dir / f"batch_id={batch_id}"
        if n_acc:
            accepted.write.mode("overwrite").parquet(str(out_dir))
        elif out_dir.exists():
            shutil.rmtree(out_dir)
        rejected = (flagged.filter("reject_reason IS NOT NULL")
                    .select("raw_value", "partition", "offset", "kafka_timestamp", "reject_reason")
                    .unionByName(dup_in_batch.select("raw_value", "partition", "offset", "kafka_timestamp")
                                 .withColumn("reject_reason", F.lit("duplicate_in_batch")))
                    .unionByName(dup_seen.select("raw_value", "partition", "offset", "kafka_timestamp")
                                 .withColumn("reject_reason", F.lit("duplicate_already_processed"))))
        reasons = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}
        rej_dir = self.s.stream_rejected_dir / f"batch_id={batch_id}"
        if reasons:
            rejected.write.mode("overwrite").parquet(str(rej_dir))
        flagged.unpersist()
        stats = self.combined_stats()
        C.write_csv_atomic(self.updated_csv, stats)
        entry = {"batch_id": batch_id, "processed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 "rows_in": rows_in, "accepted": n_acc, "rejected": sum(reasons.values()),
                 "rejected_by_reason": ";".join(f"{k}={v}" for k, v in sorted(reasons.items())),
                 "seconds": round(time.time() - t0, 2)}
        _upsert_batch_log(self.batch_log, entry)
        log.info("batch %s: получено %s, принято %s, отклонено %s %s", batch_id, rows_in, n_acc,
                 entry["rejected"], reasons or "")


def _has_parquet(path: Path) -> bool:
    """Есть ли ЗАФИКСИРОВАННЫЕ данные: маркер _SUCCESS в каталоге или в каталогах batch_id=N.

    Файлы из _temporary (запись ещё идёт) не считаются: Spark их игнорирует,
    и попытка прочитать такой каталог закончилась бы UNABLE_TO_INFER_SCHEMA.
    """
    if not path.is_dir():
        return False
    if (path / "_SUCCESS").exists():
        return True
    return any((d / "_SUCCESS").exists() for d in path.glob("batch_id=*"))


def _upsert_batch_log(path: Path, entry: dict):
    rows = [r for r in (C.read_csv(path) if path.exists() else []) if str(r["batch_id"]) != str(entry["batch_id"])]
    rows.append({k: str(v) for k, v in entry.items()})
    rows.sort(key=lambda r: int(r["batch_id"]))
    C.write_csv_atomic(path, rows, fieldnames=list(entry))


def start_streams(spark: SparkSession, kafka_options: dict, settings: C.Settings, processor: StreamProcessor) -> dict:
    """Запустить два независимых потоковых запроса, каждый со своим checkpoint.

    accepted_stream — foreachBatch: проверка, сохранение, пересчёт итоговых показателей.
    type_counts     — агрегат по типам операций только по потоку (outputMode complete, memory sink).
    Уже запущенные запросы с теми же именами сначала корректно останавливаются.
    """
    stop_streams(spark)
    parsed = parse_messages(kafka_stream(spark, kafka_options, settings))
    q_main = (parsed.writeStream.queryName("accepted_stream")
              .foreachBatch(processor)
              .option("checkpointLocation", str(settings.checkpoint_dir / "accepted_stream"))
              .trigger(processingTime=f"{settings.trigger_seconds} seconds")
              .start())
    # Второй поток: потоковая агрегация с состоянием. dropDuplicates по transaction_id
    # хранит множество увиденных id в состоянии (для учебного объёма это приемлемо).
    valid = (validate_stream_batch(parse_messages(kafka_stream(spark, kafka_options, settings)),
                                   settings.new_period_start, processor.ts_max)
             .filter("reject_reason IS NULL").transform(to_clean).dropDuplicates(["transaction_id"]))
    q_types = (valid.groupBy("transaction_type")
               .agg(F.count(F.lit(1)).alias("tx_count"), F.sum("amount_eur").alias("amount_eur_sum"))
               .writeStream.queryName("stream_type_counts").outputMode("complete").format("memory")
               .option("checkpointLocation", str(settings.checkpoint_dir / "stream_type_counts"))
               .trigger(processingTime=f"{settings.trigger_seconds} seconds")
               .start())
    log.info("Запущены потоки: accepted_stream (%s), stream_type_counts (%s); startingOffsets=%s, trigger=%ss",
             q_main.id, q_types.id, settings.starting_offsets, settings.trigger_seconds)
    return {"accepted_stream": q_main, "stream_type_counts": q_types}


def stop_streams(spark: SparkSession, timeout_s: int = 60):
    for q in list(spark.streams.active):
        if q.name in ("accepted_stream", "stream_type_counts"):
            log.info("Остановка потока %s (%s)", q.name, q.id)
            q.stop()
            try:
                q.awaitTermination(timeout_s)
            except Exception as exc:  # остановленный запрос может вернуть исключение прошлого сбоя
                log.warning("Поток %s завершился с ошибкой: %s", q.name, exc)


def find_stored_ids(settings: C.Settings, spark: SparkSession, ids: set[str]) -> set[str]:
    """Какие из указанных transaction_id уже есть в хранилище потоковых транзакций."""
    if not ids or not _has_parquet(settings.stream_store_dir):
        return set()
    try:
        df = spark.read.parquet(str(settings.stream_store_dir)).select("transaction_id")
        return {r[0] for r in df.filter(F.col("transaction_id").isin(list(ids))).collect()}
    except Exception as exc:  # каталог как раз перезаписывается микропакетом — повторим при следующем опросе
        log.debug("Хранилище потока пока недоступно для чтения: %s", exc)
        return set()


def wait_for_transactions(settings: C.Settings, spark: SparkSession, expected_ids: set[str],
                          query=None, timeout_s: int = 180, poll_s: float = 3.0) -> dict:
    """Ждать, пока ожидаемые transaction_id появятся в хранилище потоковых транзакций."""
    deadline = time.time() + timeout_s
    found = set()
    while time.time() < deadline:
        if query is not None and query.exception() is not None:
            raise RuntimeError(f"Поток упал: {query.exception()}")
        found = find_stored_ids(settings, spark, expected_ids)
        if found >= expected_ids:
            break
        time.sleep(poll_s)
    # данные уже записаны, но foreachBatch ещё пересчитывает итоги — дождаться конца микропакета
    while query is not None and query.status.get("isTriggerActive") and time.time() < deadline:
        time.sleep(0.5)
    return {"expected": len(expected_ids), "found": len(found), "missing": sorted(expected_ids - found)[:10],
            "complete": found >= expected_ids}


def wait_for_new_input(query, after_batch_id: int, min_rows: int, timeout_s: int = 120, poll_s: float = 2.0) -> dict:
    """Ждать, пока запрос обработает хотя бы min_rows строк в микропакетах после after_batch_id."""
    deadline = time.time() + timeout_s
    rows, batches = 0, []
    while time.time() < deadline:
        if query.exception() is not None:
            raise RuntimeError(f"Поток упал: {query.exception()}")
        progress = [p for p in (query.recentProgress or []) if p.get("batchId", -1) > after_batch_id]
        rows = sum(int(p.get("numInputRows", 0)) for p in progress)
        batches = [p["batchId"] for p in progress if int(p.get("numInputRows", 0)) > 0]
        if rows >= min_rows and not query.status.get("isTriggerActive", False):
            break
        time.sleep(poll_s)
    return {"rows_processed": rows, "batches_with_data": batches, "complete": rows >= min_rows}


def last_batch_id(query) -> int:
    lp = query.lastProgress
    return int(lp["batchId"]) if lp else -1


# ===========================================================================
# Резервная копия рабочего каталога в Google Drive
# ===========================================================================
def backup_work_dir(settings: C.Settings) -> Path:
    """Скопировать checkpoint и хранилище потока в Drive (после остановки потоков)."""
    dst = settings.work_backup_dir
    tmp = dst.with_name(dst.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    for name in ("checkpoints", "stream_transactions", "stream_rejected"):
        src = Path(settings.work_dir) / name
        if src.exists():
            shutil.copytree(src, tmp / name)
    if dst.exists():
        shutil.rmtree(dst)
    os.replace(tmp, dst)
    return dst


def restore_work_dir(settings: C.Settings) -> bool:
    """Восстановить рабочий каталог из Drive, если локальная копия отсутствует (новая ВМ Colab)."""
    src = settings.work_backup_dir
    local_ckpt = Path(settings.work_dir) / "checkpoints"
    if not src.exists() or (local_ckpt.exists() and any(local_ckpt.iterdir())):
        return False
    for name in ("checkpoints", "stream_transactions", "stream_rejected"):
        if (src / name).exists():
            target = Path(settings.work_dir) / name
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(src / name, target)
    return True

"""Общая логика обработки банковских транзакций (ЛР № 2, вариант № 2).

Модуль импортируют spark_job.py и benchmark.py, поэтому в обоих режимах
(local[1] и Spark Standalone) выполняются одни и те же чтение, очистка,
фильтрация, группировка и сортировка.

Все преобразования выражены через DataFrame API и встроенные функции Spark —
Python-UDF не используются, поэтому код целиком оптимизируется Catalyst и
исполняется в JVM executors.
"""
from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import asdict, dataclass, field

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# --- Схема исходного CSV -------------------------------------------------------
# Сырые данные содержат нечисловые суммы и испорченные даты, поэтому исходный
# слой читается с ЯВНОЙ схемой из строковых полей, а типы приводятся затем
# безопасными функциями try_cast / try_to_timestamp (ошибка -> NULL + флаг).
RAW_COLUMNS = [
    "transaction_id", "client_id", "account_id", "amount", "currency",
    "city", "transaction_type", "transaction_timestamp", "channel",
]
REQUIRED_COLUMNS = RAW_COLUMNS[:8]
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])

# --- Справочники ---------------------------------------------------------------
KNOWN_CITIES = [
    "Amsterdam", "Berlin", "Dublin", "Lisbon", "London", "Madrid",
    "Paris", "Prague", "Rome", "Vienna", "Warsaw", "Zurich",
]
KNOWN_TYPES = ["deposit", "payment", "transfer", "withdrawal"]
KNOWN_CHANNELS = ["atm", "branch", "online", "pos"]
# Условные фиксированные курсы для приведения сумм к одной валюте (EUR).
# Не являются рыночными котировками; нужны, чтобы среднее не смешивало валюты.
EUR_RATES = {"EUR": "1.00", "USD": "0.92", "GBP": "1.17", "CHF": "1.05"}
TIMESTAMP_FORMATS = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "dd.MM.yyyy HH:mm:ss"]
UNKNOWN_CITY = "Unknown"
UNKNOWN_LABEL = "unknown"

# Порядок проверок: запись получает ОДНУ (первую сработавшую) причину отказа,
# поэтому строки с несколькими дефектами не учитываются дважды.
REJECT_RULES = [
    ("missing_transaction_id", "transaction_id_n IS NULL"),
    ("missing_client_id", "client_id_n IS NULL"),
    ("missing_account_id", "account_id_n IS NULL"),
    ("missing_amount", "amount_norm IS NULL"),
    ("invalid_amount_format", "amount_dec IS NULL"),
    ("non_positive_amount", "amount_dec <= 0"),
    ("missing_timestamp", "ts_norm IS NULL"),
    ("invalid_timestamp", "ts_parsed IS NULL"),
    ("timestamp_out_of_range", "ts_parsed < CAST('{start}' AS TIMESTAMP) OR ts_parsed >= CAST('{end}' AS TIMESTAMP)"),
    ("missing_currency", "currency_norm IS NULL"),
    ("unknown_currency", "rate_eur IS NULL"),
]


@dataclass
class JobConfig:
    data_path: str = os.environ.get("DATA_PATH", "data/transactions.csv")
    results_dir: str = os.environ.get("RESULTS_DIR", "results")
    amount_threshold: float = float(os.environ.get("AMOUNT_THRESHOLD", "100"))
    period_start: str = os.environ.get("DATA_START", "2025-01-01")
    period_end: str = os.environ.get("DATA_END", "2026-01-01")
    shuffle_partitions: int = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
    max_partition_bytes: str = os.environ.get("SPARK_MAX_PARTITION_BYTES", "32m")
    top_n: int = 5
    extra: dict = field(default_factory=dict)

    def as_dict(self):
        return asdict(self)


def build_spark(app_name: str, cfg: JobConfig, master: str | None = None) -> SparkSession:
    """SparkSession с одинаковыми настройками SQL для всех режимов.

    master=None означает «взять из spark-submit --master».
    """
    b = SparkSession.builder.appName(app_name)
    if master:
        b = b.master(master)
    b = (b.config("spark.sql.session.timeZone", "UTC")
          .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
          .config("spark.sql.files.maxPartitionBytes", cfg.max_partition_bytes)
          .config("spark.sql.ansi.enabled", "true"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Чтение CSV с явной схемой. PERMISSIVE: строки с неверным числом полей не
    роняют задание — недостающие поля становятся NULL и отбраковываются позже."""
    return (spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("encoding", "UTF-8")
            .schema(RAW_SCHEMA)
            .csv(path))


def _blank_to_null(c: str):
    trimmed = F.trim(F.col(c))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def normalize(raw: DataFrame) -> DataFrame:
    """Нормализация без потери исходных значений.

    * обрезка пробелов, пустая строка -> NULL;
    * единый регистр: валюта UPPER, тип/канал lower, город Initcap;
    * безопасное приведение суммы к DECIMAL(18,2) и времени к TIMESTAMP (UTC)
      по трём допустимым форматам;
    * флаги исправлений для статистики.
    """
    df = raw
    for c in RAW_COLUMNS:
        df = df.withColumn(f"{c}_n", _blank_to_null(c))
    rates = F.create_map(*[x for k, v in EUR_RATES.items() for x in (F.lit(k), F.lit(v).cast("decimal(10,4)"))])
    ts_candidates = [F.try_to_timestamp(F.col("transaction_timestamp_n"), F.lit(fmt)) for fmt in TIMESTAMP_FORMATS]
    df = (df
          .withColumn("amount_norm", F.col("amount_n"))
          .withColumn("amount_dec", F.expr("try_cast(amount_n AS DECIMAL(18,2))"))
          .withColumn("currency_norm", F.upper(F.col("currency_n")))
          .withColumn("rate_eur", F.try_element_at(rates, F.col("currency_norm")))
          .withColumn("city_cap", F.initcap(F.lower(F.col("city_n"))))
          .withColumn("type_low", F.lower(F.col("transaction_type_n")))
          .withColumn("channel_low", F.lower(F.col("channel_n")))
          .withColumn("ts_norm", F.col("transaction_timestamp_n"))
          .withColumn("ts_parsed", F.coalesce(*ts_candidates))
          .withColumn("ts_primary_ok", ts_candidates[0].isNotNull()))
    return df


def flag_rows(norm: DataFrame, cfg: JobConfig) -> DataFrame:
    """Добавляет reject_reason (первая сработавшая проверка или NULL)."""
    reason = F.lit(None).cast("string")
    for name, cond in reversed(REJECT_RULES):
        cond = cond.format(start=cfg.period_start, end=cfg.period_end)
        reason = F.when(F.expr(cond), F.lit(name)).otherwise(reason)
    return norm.withColumn("reject_reason", reason)


def clean(raw: DataFrame, cfg: JobConfig) -> dict:
    """Полный конвейер очистки. Возвращает словарь DataFrame:

    raw_dedup  — исходные строки без полных дубликатов;
    flagged    — нормализованные строки с reject_reason;
    kept       — принятые строки со всеми служебными столбцами (для статистики);
    clean      — итоговые корректные транзакции (одна строка на transaction_id);
    rejected   — отклонённые строки с причиной (включая дубли по transaction_id);
    """
    # 1. Полные дубликаты: строки, совпадающие по ВСЕМ исходным полям.
    raw_dedup = raw.dropDuplicates(RAW_COLUMNS)
    flagged = flag_rows(normalize(raw_dedup), cfg)

    valid = flagged.filter(F.col("reject_reason").isNull())
    # 2. Частичные дубликаты: тот же transaction_id, другое оформление полей.
    #    Оставляем одну запись; выбор детерминирован (не зависит от разбиения
    #    на партиции): сначала запись в основном формате даты, затем по хешу.
    w = Window.partitionBy("transaction_id_n").orderBy(
        F.col("ts_primary_ok").desc(), F.xxhash64(*RAW_COLUMNS))
    ranked = valid.withColumn("_rn", F.row_number().over(w))

    city_known = F.col("city_cap").isin(KNOWN_CITIES)
    kept = ranked.filter(F.col("_rn") == 1)
    clean_df = (kept
                .select(
                    F.col("transaction_id_n").alias("transaction_id"),
                    F.col("client_id_n").alias("client_id"),
                    F.col("account_id_n").alias("account_id"),
                    F.col("amount_dec").alias("amount"),
                    F.col("currency_norm").alias("currency"),
                    (F.col("amount_dec") * F.col("rate_eur")).cast("decimal(18,2)").alias("amount_eur"),
                    F.when(city_known, F.col("city_cap")).otherwise(F.lit(UNKNOWN_CITY)).alias("city"),
                    F.when(F.col("type_low").isin(KNOWN_TYPES), F.col("type_low"))
                     .otherwise(F.lit(UNKNOWN_LABEL)).alias("transaction_type"),
                    F.col("ts_parsed").alias("transaction_timestamp"),
                    F.when(F.col("channel_low").isin(KNOWN_CHANNELS), F.col("channel_low"))
                     .otherwise(F.lit(UNKNOWN_LABEL)).alias("channel"),
                ))
    dup_ids = (ranked.filter(F.col("_rn") > 1)
               .withColumn("reject_reason", F.lit("duplicate_transaction_id")))
    rejected = (flagged.filter(F.col("reject_reason").isNotNull()).select(*RAW_COLUMNS, "reject_reason")
                .unionByName(dup_ids.select(*RAW_COLUMNS, "reject_reason")))
    return {"raw_dedup": raw_dedup, "flagged": flagged, "kept": kept,
            "clean": clean_df, "rejected": rejected}


def city_stats(clean_df: DataFrame) -> DataFrame:
    """Группировка очищенных транзакций по городу и сортировка по средней сумме.

    Город «Unknown» (пропуск или нераспознанное значение) не участвует в
    рейтинге. Среднее считается по DECIMAL — результат точный и не зависит от
    порядка суммирования, поэтому совпадает в разных режимах запуска.
    Второй ключ сортировки (city) делает порядок однозначным при равенстве.
    """
    return (clean_df
            .filter(F.col("city") != UNKNOWN_CITY)
            .groupBy("city")
            .agg(F.avg("amount_eur").alias("avg_amount_eur"),
                 F.count(F.lit(1)).alias("transaction_count"))
            .orderBy(F.col("avg_amount_eur").desc(), F.col("city").asc()))


def benchmark_pipeline(spark: SparkSession, path: str, cfg: JobConfig) -> list[dict]:
    """Операция для сравнения режимов: чтение CSV -> нормализация и проверки ->
    удаление дубликатов -> группировка по городу -> глобальная сортировка ->
    collect() на драйвер. Запись файлов в замер не входит."""
    raw = read_raw(spark, path)
    stats = city_stats(clean(raw, cfg)["clean"])
    return [{"city": r["city"], "avg_amount_eur": str(r["avg_amount_eur"]),
             "transaction_count": int(r["transaction_count"])} for r in stats.collect()]


def app_info(spark: SparkSession) -> dict:
    """Сведения о приложении и executors (через REST API Spark UI драйвера)."""
    sc = spark.sparkContext
    info = {
        "spark_version": spark.version,
        "app_id": sc.applicationId,
        "app_name": sc.appName,
        "master": sc.master,
        "ui_url": sc.uiWebUrl,
        "default_parallelism": sc.defaultParallelism,
        "conf": {k: v for k, v in sorted(sc.getConf().getAll())
                 if k.startswith(("spark.executor", "spark.driver.memory", "spark.cores",
                                  "spark.sql.shuffle", "spark.sql.files.maxPartitionBytes",
                                  "spark.master", "spark.app.id"))
                 and "JavaOptions" not in k},
    }
    info["executors"] = rest_get(sc, "executors") or []
    return info


def rest_get(sc, endpoint: str):
    """GET к /api/v1/applications/<appId>/<endpoint> на UI драйвера."""
    if not sc.uiWebUrl:
        return None
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{endpoint}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # UI может быть отключён — это не ошибка расчёта
        return {"error": str(exc), "url": url}


def executor_summary(executors) -> list[dict]:
    keep = ("id", "hostPort", "isActive", "totalCores", "maxMemory", "totalTasks",
            "completedTasks", "failedTasks", "totalDuration", "totalGCTime",
            "totalShuffleRead", "totalShuffleWrite")
    out = []
    for e in executors if isinstance(executors, list) else []:
        row = {k: e.get(k) for k in keep}
        peak = e.get("peakMemoryMetrics") or {}
        row["peakJVMHeapMemory"] = peak.get("JVMHeapMemory")
        out.append(row)
    return out

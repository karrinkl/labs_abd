#!/usr/bin/env python3
"""ЛР № 2, вариант № 2 — обработка банковских транзакций в Apache Spark.

Этапы:
  1. Сведения о приложении (версия Spark, master, executors).
  2. Чтение CSV с явной схемой, схема DataFrame, первые записи, число строк.
  3. Проверка качества исходных данных (пропуски, дубликаты, суммы,
     категориальные значения, временные метки) — сводный отчёт.
  4. Очистка: нормализация, удаление дубликатов, отбраковка некорректных строк.
  5. Фильтрация amount_eur > x.
  6. Группировка по city (средняя сумма и количество) и сортировка.
  7. Spark SQL: топ городов по средней сумме транзакции.
  8. Сохранение результатов в results/<имя набора>/.

Запуск (в контейнере spark-client):
  python3 submit.py cluster spark_job.py --data data/transactions.csv
  python3 submit.py local1  spark_job.py --data data/transactions.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

from pyspark.sql import functions as F

import transactions_pipeline as tp

TOP_SQL = """
SELECT city,
       AVG(amount_eur)           AS avg_amount_eur,
       ROUND(AVG(amount_eur), 2) AS avg_amount_eur_rounded,
       COUNT(*)                  AS transaction_count
FROM transactions_clean
WHERE city IS NOT NULL
  AND city <> 'Unknown'
  AND amount_eur IS NOT NULL
  AND amount_eur > 0
GROUP BY city
ORDER BY AVG(amount_eur) DESC, city ASC   -- сортировка по точному, а не округлённому среднему
LIMIT {top_n}
""".strip()


def section(title: str):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78, flush=True)


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def rows_to_dicts(rows):
    out = []
    for r in rows:
        d = r.asDict()
        out.append({k: (str(v) if v is not None and not isinstance(v, (int, float, str)) else v)
                    for k, v in d.items()})
    return out


def parse_args():
    cfg = tp.JobConfig()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", default=cfg.data_path)
    p.add_argument("--results-dir", default=cfg.results_dir)
    p.add_argument("--threshold", type=float, default=cfg.amount_threshold,
                   help="порог x для фильтра amount_eur > x (EUR)")
    p.add_argument("--top-n", type=int, default=cfg.top_n)
    p.add_argument("--run-label", default="manual", help="метка режима (подставляет submit.py)")
    p.add_argument("--no-write", action="store_true", help="не записывать очищенные данные в data/")
    return p.parse_args(), cfg


def quality_report(norm, raw_count: int, raw_dedup_count: int, cfg) -> dict:
    """Проверки качества по ИСХОДНЫМ строкам (до удаления дубликатов)."""
    c = F.col
    known_currency = c("rate_eur").isNotNull()
    checks = {
        # пропуски (NULL или строка из пробелов) по обязательным полям
        **{f"missing_{col}": c(f"{col}_n").isNull() for col in tp.REQUIRED_COLUMNS},
        # суммы
        "amount_invalid_format": c("amount_n").isNotNull() & c("amount_dec").isNull(),
        "amount_negative": c("amount_dec") < 0,
        "amount_zero": c("amount_dec") == 0,
        # категориальные признаки
        "currency_unknown": c("currency_n").isNotNull() & ~known_currency,
        "currency_wrong_case": known_currency & (c("currency") != c("currency_norm")),
        "city_unrecognized": c("city_n").isNotNull() & ~c("city_cap").isin(tp.KNOWN_CITIES),
        "city_needs_normalization": c("city_cap").isin(tp.KNOWN_CITIES) & (c("city") != c("city_cap")),
        "transaction_type_unknown": c("type_low").isNotNull() & ~c("type_low").isin(tp.KNOWN_TYPES),
        "transaction_type_needs_normalization": c("type_low").isin(tp.KNOWN_TYPES) & (c("transaction_type") != c("type_low")),
        "channel_missing_or_unknown": c("channel_low").isNull() | ~c("channel_low").isin(tp.KNOWN_CHANNELS),
        # временные метки
        "timestamp_unparseable": c("ts_norm").isNotNull() & c("ts_parsed").isNull(),
        "timestamp_alt_format": c("ts_parsed").isNotNull() & ~c("ts_primary_ok"),
        "timestamp_out_of_range": (c("ts_parsed") < F.lit(cfg.period_start).cast("timestamp"))
                                  | (c("ts_parsed") >= F.lit(cfg.period_end).cast("timestamp")),
    }
    agg = norm.agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias(name) for name, cond in checks.items()],
                   F.min("ts_parsed").alias("ts_min"), F.max("ts_parsed").alias("ts_max"),
                   F.countDistinct("transaction_id_n").alias("distinct_transaction_ids"),
                   F.sum(F.when(c("transaction_id_n").isNotNull(), 1).otherwise(0)).alias("rows_with_id"),
                   ).collect()[0].asDict()
    dup_ids = (norm.filter(c("transaction_id_n").isNotNull())
               .groupBy("transaction_id_n").count().filter(c("count") > 1)
               .agg(F.count(F.lit(1)).alias("ids"), F.sum("count").alias("rows")).collect()[0])
    report = {
        "raw_rows": raw_count,
        "missing_values": {k.replace("missing_", ""): int(agg[k]) for k in checks if k.startswith("missing_")},
        "duplicates": {
            "exact_duplicate_rows": raw_count - raw_dedup_count,
            "transaction_ids_occurring_more_than_once": int(dup_ids["ids"] or 0),
            "rows_sharing_a_duplicated_transaction_id": int(dup_ids["rows"] or 0),
        },
        "amount": {k: int(agg[k]) for k in ("amount_invalid_format", "amount_negative", "amount_zero")},
        "categorical": {k: int(agg[k]) for k in checks if k.startswith(("currency_", "city_", "transaction_type_", "channel_"))},
        "timestamps": {
            "unparseable": int(agg["timestamp_unparseable"]),
            "alternative_format": int(agg["timestamp_alt_format"]),
            "out_of_range": int(agg["timestamp_out_of_range"]),
            "min_parsed": str(agg["ts_min"]), "max_parsed": str(agg["ts_max"]),
            "expected_interval_utc": [cfg.period_start, cfg.period_end],
        },
    }
    return report


def cleaning_stats(parts: dict, raw_count: int, raw_dedup_count: int) -> dict:
    flagged, kept, clean_df = parts["flagged"], parts["kept"], parts["clean"]
    reasons = {r["reject_reason"]: int(r["count"]) for r in
               flagged.filter(F.col("reject_reason").isNotNull()).groupBy("reject_reason").count().collect()}
    valid_count = raw_dedup_count - sum(reasons.values())
    clean_count = clean_df.count()
    dup_id_removed = valid_count - clean_count
    c = F.col
    corr = {
        "city_missing_filled_unknown": c("city_n").isNull(),
        "city_unrecognized_set_unknown": c("city_n").isNotNull() & ~c("city_cap").isin(tp.KNOWN_CITIES),
        "city_case_or_spaces_fixed": c("city_cap").isin(tp.KNOWN_CITIES) & (c("city") != c("city_cap")),
        "transaction_type_missing_or_unknown_set_unknown": c("type_low").isNull() | ~c("type_low").isin(tp.KNOWN_TYPES),
        "transaction_type_case_or_spaces_fixed": c("type_low").isin(tp.KNOWN_TYPES) & (c("transaction_type") != c("type_low")),
        "currency_case_fixed": c("currency") != c("currency_norm"),
        "timestamp_converted_to_standard_format": ~c("ts_primary_ok"),
        "channel_missing_or_unknown_set_unknown": c("channel_low").isNull() | ~c("channel_low").isin(tp.KNOWN_CHANNELS),
    }
    any_corr = None
    for cond in corr.values():
        any_corr = cond if any_corr is None else (any_corr | cond)
    agg = kept.agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias(k) for k, cond in corr.items()],
                   F.sum(F.when(any_corr, 1).otherwise(0)).alias("rows_with_any_correction")).collect()[0].asDict()
    stats = {
        "rows_before_cleaning": raw_count,
        "removed_exact_duplicates": raw_count - raw_dedup_count,
        "rejected_by_reason": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
        "rejected_total": sum(reasons.values()),
        "removed_duplicate_transaction_id": dup_id_removed,
        "rows_after_cleaning": clean_count,
        "corrected_in_kept_rows": {k: int(agg[k]) for k in corr},
        "rows_with_any_correction": int(agg["rows_with_any_correction"]),
    }
    removed = stats["removed_exact_duplicates"] + stats["rejected_total"] + dup_id_removed
    stats["balance_check"] = {
        "formula": "rows_before = exact_duplicates + rejected_total + duplicate_transaction_id + rows_after",
        "ok": raw_count == removed + clean_count,
    }
    return stats


def main():
    args, cfg = parse_args()
    cfg.data_path, cfg.amount_threshold, cfg.top_n = args.data, args.threshold, args.top_n
    out = Path(args.results_dir) / Path(args.data).stem
    out.mkdir(parents=True, exist_ok=True)
    timings = {}
    t_all = time.perf_counter()

    spark = tp.build_spark(f"lab2-transactions-{args.run_label}", cfg)
    sc = spark.sparkContext

    # ---------------------------------------------------------------- 1. приложение
    section("1. Spark-приложение")
    print(f"Spark version     : {spark.version}")
    print(f"Master            : {sc.master}")
    print(f"Application ID    : {sc.applicationId}")
    print(f"Spark UI драйвера : {sc.uiWebUrl}  (с хоста: http://localhost:4040)")
    print(f"defaultParallelism: {sc.defaultParallelism}")

    # ---------------------------------------------------------------- 2. чтение
    section("2. Чтение исходных данных (явная схема, все поля — строки)")
    t = time.perf_counter()
    raw = tp.read_raw(spark, args.data).cache()
    raw_count = raw.count()
    timings["read_and_count_s"] = time.perf_counter() - t
    raw.printSchema()
    raw.show(5, truncate=False)
    print(f"Исходных строк: {raw_count}; партиций при чтении: {raw.rdd.getNumPartitions()}")

    # ---------------------------------------------------------------- 3. качество
    section("3. Проверка качества исходных данных")
    t = time.perf_counter()
    parts = tp.clean(raw, cfg)
    raw_dedup_count = parts["raw_dedup"].count()
    norm_all = tp.normalize(raw)
    quality = quality_report(norm_all, raw_count, raw_dedup_count, cfg)
    print("Исходные значения валюты и типа (с проблемами регистра/пробелов):")
    raw.groupBy("currency").count().orderBy(F.desc("count")).show(12, truncate=False)
    raw.groupBy("transaction_type").count().orderBy(F.desc("count")).show(12, truncate=False)
    timings["quality_checks_s"] = time.perf_counter() - t
    print(json.dumps(quality, ensure_ascii=False, indent=2))

    # ---------------------------------------------------------------- 4. очистка
    section("4. Очистка данных")
    t = time.perf_counter()
    clean_df = parts["clean"].cache()
    cstats = cleaning_stats(parts, raw_count, raw_dedup_count)
    timings["cleaning_s"] = time.perf_counter() - t
    print(json.dumps(cstats, ensure_ascii=False, indent=2))
    clean_df.printSchema()
    clean_df.orderBy("transaction_id").show(5, truncate=False)
    (Path(out) / "quality_report.json").write_text(
        json.dumps({"dataset": args.data, "quality": quality, "cleaning": cstats},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    rejected_sample = parts["rejected"].orderBy("reject_reason", "transaction_id").limit(60).collect()
    write_csv(out / "rejected_sample.csv", rows_to_dicts(rejected_sample))

    # ---------------------------------------------------------------- 5. фильтрация
    section(f"5. Фильтрация: amount_eur > {cfg.amount_threshold:g} EUR")
    print("Тип столбца amount_eur:", dict(clean_df.dtypes)["amount_eur"],
          "— некорректные и неположительные суммы уже исключены на этапе очистки")
    q = clean_df.agg(F.percentile_approx("amount_eur", [0.25, 0.5, 0.75, 0.9, 0.99], 10000).alias("q"),
                     F.min("amount_eur").alias("min"), F.max("amount_eur").alias("max")).collect()[0]
    quantiles = dict(zip(["p25", "p50", "p75", "p90", "p99"], [str(v) for v in q["q"]]))
    quantiles.update({"min": str(q["min"]), "max": str(q["max"])})
    print("Распределение amount_eur:", quantiles)
    filtered = clean_df.filter(F.col("amount_eur") > F.lit(cfg.amount_threshold))
    before, after = cstats["rows_after_cleaning"], filtered.count()
    print(f"Записей до фильтрации : {before}")
    print(f"Порог x               : {cfg.amount_threshold:g} EUR")
    print(f"Записей после фильтра : {after} ({after / before:.1%})")
    examples = filtered.orderBy("transaction_id").limit(10)
    examples.show(truncate=False)
    write_csv(out / "filtered_sample.csv", rows_to_dicts(examples.collect()))
    filter_summary = {"threshold_eur": cfg.amount_threshold, "rows_before": before,
                      "rows_after": after, "share_after": after / before, "amount_eur_distribution": quantiles}
    (out / "filter_summary.json").write_text(json.dumps(filter_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---------------------------------------------------------------- 6. группировка и сортировка
    section("6. Группировка по city и сортировка по средней сумме (очищенный полный набор)")
    stats_df = tp.city_stats(clean_df)
    plan = stats_df._jdf.queryExecution().explainString(
        spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted"))
    (out / "plan_city_stats.txt").write_text(plan, encoding="utf-8")
    print("Физический план (сокращённо, полный — в plan_city_stats.txt):")
    print("\n".join(l for l in plan.splitlines() if "Exchange" in l or "HashAggregate" in l or "Sort" in l)[:3000])
    t = time.perf_counter()
    city_rows = stats_df.collect()
    timings["group_sort_collect_s"] = time.perf_counter() - t
    stats_df.withColumn("avg_amount_eur_rounded", F.round("avg_amount_eur", 2)).show(50, truncate=False)
    city_dicts = [{"rank": i + 1, "city": r["city"], "avg_amount_eur": str(r["avg_amount_eur"]),
                   "avg_amount_eur_rounded": f"{r['avg_amount_eur']:.2f}",
                   "transaction_count": r["transaction_count"]} for i, r in enumerate(city_rows)]
    write_csv(out / "city_stats.csv", city_dicts)
    unknown_city = clean_df.filter(F.col("city") == tp.UNKNOWN_CITY).count()
    print(f"Строк с city = '{tp.UNKNOWN_CITY}' (не участвуют в рейтинге): {unknown_city}")
    # Дополнительно: та же группировка по результату фильтра (крупные транзакции)
    over = tp.city_stats(filtered).collect()
    write_csv(out / "city_stats_over_threshold.csv",
              [{"rank": i + 1, "city": r["city"], "avg_amount_eur": str(r["avg_amount_eur"]),
                "transaction_count": r["transaction_count"]} for i, r in enumerate(over)])

    # ---------------------------------------------------------------- 7. Spark SQL
    section("7. Spark SQL: города с самой высокой средней суммой транзакции")
    clean_df.createOrReplaceTempView("transactions_clean")
    sql = TOP_SQL.format(top_n=cfg.top_n)
    (out / "top_cities.sql").write_text(sql + ";\n", encoding="utf-8")
    print(sql)
    top = spark.sql(sql)
    top.show(truncate=False)
    top_rows = top.collect()
    n_cities = len(city_rows)
    if n_cities < cfg.top_n:
        print(f"В данных только {n_cities} городов — выводятся все доступные.")
    top_dicts = [{"rank": i + 1, "city": r["city"], "avg_amount_eur": str(r["avg_amount_eur"]),
                  "avg_amount_eur_rounded": str(r["avg_amount_eur_rounded"]),
                  "transaction_count": r["transaction_count"]} for i, r in enumerate(top_rows)]
    write_csv(out / "top_cities_sql.csv", top_dicts)
    consistent = [(d["city"], d["avg_amount_eur"], d["transaction_count"]) for d in top_dicts] == \
                 [(d["city"], d["avg_amount_eur"], d["transaction_count"]) for d in city_dicts[:cfg.top_n]]
    print(f"Результат SQL совпадает с DataFrame API: {consistent}")

    # ---------------------------------------------------------------- 8. запись и сведения о кластере
    if not args.no_write:
        section("8. Запись очищенных и отклонённых данных (выполняют executors)")
        t = time.perf_counter()
        name = Path(args.data).stem
        clean_df.write.mode("overwrite").parquet(f"data/clean_{name}.parquet")
        parts["rejected"].coalesce(1).write.mode("overwrite").option("header", "true").csv(f"data/rejected_{name}")
        timings["write_s"] = time.perf_counter() - t
        print(f"data/clean_{name}.parquet, data/rejected_{name}/")

    section("9. Executors приложения (REST API Spark UI)")
    info = tp.app_info(spark)
    info["executors"] = tp.executor_summary(info["executors"])
    for e in info["executors"]:
        print(f"  executor {e['id']:>6} | {e['hostPort']:<24} | active={e['isActive']} | cores={e['totalCores']}"
              f" | maxMemory={(e['maxMemory'] or 0) / 1024 ** 2:.0f} MiB | tasks={e['completedTasks']}")
    timings["total_s"] = time.perf_counter() - t_all
    info.update({"run_label": args.run_label, "dataset": args.data, "timings_s": timings,
                 "sql_matches_dataframe": consistent, "config": cfg.as_dict()})
    (out / f"app_info_{args.run_label}.json").write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nРезультаты сохранены в {out}/ ; время этапов, с: "
          + ", ".join(f"{k}={v:.2f}" for k, v in timings.items()))
    spark.stop()


if __name__ == "__main__":
    main()

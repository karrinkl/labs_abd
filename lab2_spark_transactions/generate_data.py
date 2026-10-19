#!/usr/bin/env python3
"""Генератор синтетических банковских транзакций (ЛР № 2, вариант № 2).

Данные полностью синтетические: идентификаторы, суммы и время генерируются
случайно и не соответствуют реальным клиентам, счетам или операциям.

В набор намеренно вносятся проблемы качества с заданными долями:
  * пропуски (пустые значения) в отдельных полях;
  * полные дубликаты строк и частичные дубликаты (тот же transaction_id,
    но отличается форматирование полей);
  * некорректные значения: отрицательные и нулевые суммы, нечисловые суммы,
    неизвестные валюты и типы транзакций, неверные и выходящие за интервал
    временные метки;
  * исправимые отклонения: лишние пробелы, другой регистр, альтернативный
    формат даты.

Результат — CSV с заголовком (data/<имя>.csv) и статистика генерации
(data/<имя>.stats.json). Нужна только стандартная библиотека Python 3.10+.

Примеры:
  python3 generate_data.py --rows 10000 --name transactions_test     # тестовый
  python3 generate_data.py --rows 2000000 --name transactions        # основной
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

FIELDS = [
    "transaction_id", "client_id", "account_id", "amount", "currency",
    "city", "transaction_type", "transaction_timestamp", "channel",
]
CURRENCIES = {"EUR": 0.55, "USD": 0.25, "GBP": 0.12, "CHF": 0.08}
# Города и множитель «типичной» суммы: создаёт различия между городами,
# которые затем должен обнаружить анализ. Значения условные.
CITIES = {
    "Berlin": 1.00, "Paris": 1.05, "London": 1.20, "Madrid": 0.90,
    "Rome": 0.95, "Warsaw": 0.75, "Vienna": 1.00, "Prague": 0.80,
    "Amsterdam": 1.10, "Lisbon": 0.85, "Dublin": 1.05, "Zurich": 1.35,
}
CITY_WEIGHTS = {
    "Berlin": 14, "Paris": 13, "London": 15, "Madrid": 10, "Rome": 9, "Warsaw": 8,
    "Vienna": 6, "Prague": 5, "Amsterdam": 7, "Lisbon": 5, "Dublin": 4, "Zurich": 4,
}
# Тип транзакции: (вес, медиана суммы, разброс логнормального распределения)
TX_TYPES = {
    "payment": (0.45, 45.0, 0.9),
    "withdrawal": (0.20, 120.0, 0.7),
    "transfer": (0.20, 350.0, 1.0),
    "deposit": (0.15, 600.0, 0.9),
}
CHANNELS = {"online": 0.45, "pos": 0.30, "atm": 0.15, "branch": 0.10}

INVALID_KINDS = [
    "negative_amount", "zero_amount", "non_numeric_amount", "unknown_currency",
    "unknown_city", "unknown_transaction_type", "malformed_timestamp", "timestamp_out_of_range",
]
FIXABLE_KINDS = ["whitespace_and_case", "lowercase_currency", "alt_timestamp_format"]
MISSING_FIELDS = [
    "transaction_id", "client_id", "account_id", "amount", "currency",
    "city", "transaction_type", "transaction_timestamp",
]


def env(name: str, default):
    value = os.environ.get(name)
    return type(default)(value) if value not in (None, "") else default


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=env("DATA_ROWS", 200_000),
                   help="число базовых транзакций (без учёта добавленных дубликатов)")
    p.add_argument("--name", default=env("DATA_NAME", "transactions"), help="имя файла без расширения")
    p.add_argument("--out-dir", default="data")
    p.add_argument("--seed", type=int, default=env("SEED", 42))
    p.add_argument("--clients", type=int, default=None, help="число клиентов (по умолчанию rows/20)")
    p.add_argument("--start", default=env("DATA_START", "2025-01-01"), help="начало интервала (UTC)")
    p.add_argument("--end", default=env("DATA_END", "2026-01-01"), help="конец интервала, не включая (UTC)")
    p.add_argument("--missing-rate", type=float, default=env("MISSING_RATE", 0.02))
    p.add_argument("--invalid-rate", type=float, default=env("INVALID_RATE", 0.015))
    p.add_argument("--fixable-rate", type=float, default=env("FIXABLE_RATE", 0.02))
    p.add_argument("--duplicate-rate", type=float, default=env("DUPLICATE_RATE", 0.01))
    p.add_argument("--partial-duplicate-share", type=float, default=0.4,
                   help="доля частичных дубликатов среди всех дубликатов")
    return p.parse_args()


def weighted(rng: random.Random, mapping: dict):
    keys = list(mapping)
    weights = [mapping[k] if not isinstance(mapping[k], tuple) else mapping[k][0] for k in keys]
    return rng.choices(keys, weights=weights, k=1)[0]


def make_clients(rng: random.Random, n_clients: int):
    """Клиент: домашний город и 1–3 счёта в одной валюте каждый."""
    clients = []
    cities = list(CITY_WEIGHTS)
    city_w = [CITY_WEIGHTS[c] for c in cities]
    account_seq = 0
    for i in range(1, n_clients + 1):
        city = rng.choices(cities, weights=city_w, k=1)[0]
        accounts = []
        for _ in range(rng.choices([1, 2, 3], weights=[0.6, 0.3, 0.1], k=1)[0]):
            account_seq += 1
            currency = "GBP" if city == "London" and rng.random() < 0.7 else (
                "CHF" if city == "Zurich" and rng.random() < 0.7 else weighted(rng, CURRENCIES))
            accounts.append((f"ACC{account_seq:09d}", currency))
        clients.append((f"CL{i:07d}", city, accounts))
    return clients


def base_record(rng, seq, clients, start, span_seconds):
    client_id, home_city, accounts = clients[rng.randrange(len(clients))]
    account_id, currency = accounts[rng.randrange(len(accounts))]
    city = home_city if rng.random() < 0.85 else rng.choice(list(CITIES))
    tx_type = weighted(rng, TX_TYPES)
    _, median, sigma = TX_TYPES[tx_type]
    amount = rng.lognormvariate(math.log(median * CITIES[city]), sigma)
    amount = round(min(max(amount, 0.5), 250_000.0), 2)
    ts = start + timedelta(seconds=rng.randrange(span_seconds))
    return {
        "transaction_id": f"TX{seq:010d}",
        "client_id": client_id,
        "account_id": account_id,
        "amount": f"{amount:.2f}",
        "currency": currency,
        "city": city,
        "transaction_type": tx_type,
        "transaction_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "channel": weighted(rng, CHANNELS),
    }


def apply_invalid(rng, rec, kind):
    if kind == "negative_amount":
        rec["amount"] = f"-{rec['amount']}"
    elif kind == "zero_amount":
        rec["amount"] = rng.choice(["0", "0.00"])
    elif kind == "non_numeric_amount":
        rec["amount"] = rng.choice(["abc", "N/A", "12,50", "1.2.3", "ten"])
    elif kind == "unknown_currency":
        rec["currency"] = rng.choice(["XYZ", "EU", "RUBX", "$$$"])
    elif kind == "unknown_city":
        rec["city"] = rng.choice(["Atlantis", "N/A", "???", "Gotham"])
    elif kind == "unknown_transaction_type":
        rec["transaction_type"] = rng.choice(["refund??", "cashback_x", "unknown_op"])
    elif kind == "malformed_timestamp":
        rec["transaction_timestamp"] = rng.choice(
            ["2025-13-45 25:61:00", "not a date", "2025/02/30", "31-31-2025 10:00"])
    elif kind == "timestamp_out_of_range":
        rec["transaction_timestamp"] = rng.choice(
            ["2031-05-01 12:00:00", "1999-12-31 23:59:59", "2019-07-15 08:30:00"])


def apply_fixable(rng, rec, kind, ts_fmt_alt="%d.%m.%Y %H:%M:%S"):
    if kind == "whitespace_and_case":
        rec["city"] = rng.choice([f"  {rec['city']} ", rec["city"].upper(), rec["city"].lower()])
        rec["transaction_type"] = rng.choice([rec["transaction_type"].upper(), f" {rec['transaction_type']}"])
    elif kind == "lowercase_currency":
        rec["currency"] = rec["currency"].lower()
    elif kind == "alt_timestamp_format":
        dt = datetime.strptime(rec["transaction_timestamp"], "%Y-%m-%d %H:%M:%S")
        rec["transaction_timestamp"] = rng.choice([dt.strftime(ts_fmt_alt), dt.strftime("%Y-%m-%dT%H:%M:%S")])


def partial_duplicate(rng, rec):
    """Тот же transaction_id, но запись «пришла повторно» в другом оформлении.

    Возвращает None, если изменить оформление нельзя (например, все поля-кандидаты
    пусты) — тогда вызывающий код записывает полный дубликат.
    """
    variants = ["city_case", "ts_iso", "amount_format"]
    rng.shuffle(variants)
    for variant in variants:
        dup = dict(rec)
        if variant == "city_case" and dup["city"].strip():
            dup["city"] = dup["city"].strip().upper()
        elif variant == "ts_iso" and " " in dup["transaction_timestamp"].strip():
            dup["transaction_timestamp"] = dup["transaction_timestamp"].strip().replace(" ", "T", 1)
        elif variant == "amount_format" and dup["amount"].strip():
            dup["amount"] = dup["amount"] + ("0" if "." in dup["amount"] else ".00")
        if dup != rec:
            return dup
    return None


def main():
    a = parse_args()
    rng = random.Random(a.seed)
    start = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(a.end).replace(tzinfo=timezone.utc)
    span = int((end - start).total_seconds())
    n_clients = a.clients or max(50, a.rows // 20)
    clients = make_clients(rng, n_clients)

    out_dir = Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{a.name}.csv"
    stats_path = out_dir / f"{a.name}.stats.json"

    stats = Counter()
    missing_by_field = Counter()
    invalid_by_kind = Counter()
    fixable_by_kind = Counter()
    t0 = time.time()
    dup_pool = []  # кандидаты для дубликатов (храним ограниченную выборку)
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for seq in range(1, a.rows + 1):
            rec = base_record(rng, seq, clients, start, span)
            issues = 0
            if rng.random() < a.fixable_rate:
                kind = rng.choice(FIXABLE_KINDS)
                apply_fixable(rng, rec, kind)
                fixable_by_kind[kind] += 1
                stats["rows_with_fixable_deviation"] += 1
            if rng.random() < a.invalid_rate:
                kind = rng.choice(INVALID_KINDS)
                apply_invalid(rng, rec, kind)
                invalid_by_kind[kind] += 1
                stats["rows_with_invalid_value"] += 1
                issues += 1
            if rng.random() < a.missing_rate:
                field = rng.choice(MISSING_FIELDS)
                rec[field] = rng.choice(["", "   "]) if field not in ("transaction_id",) else ""
                missing_by_field[field] += 1
                stats["rows_with_missing_value"] += 1
                issues += 1
            if issues > 1:
                stats["rows_with_missing_and_invalid"] += 1
            w.writerow(rec)
            stats["base_rows"] += 1
            # резервуарная выборка для дубликатов
            if len(dup_pool) < 50_000:
                dup_pool.append(rec)
            elif rng.random() < 50_000 / seq:
                dup_pool[rng.randrange(50_000)] = rec
        n_dup = int(round(a.rows * a.duplicate_rate))
        for _ in range(n_dup):
            src = dup_pool[rng.randrange(len(dup_pool))]
            dup = partial_duplicate(rng, src) if rng.random() < a.partial_duplicate_share else None
            if dup is not None:
                w.writerow(dup)
                stats["partial_duplicates_added"] += 1
            else:
                w.writerow(src)
                stats["exact_duplicates_added"] += 1

    total = stats["base_rows"] + stats["exact_duplicates_added"] + stats["partial_duplicates_added"]
    result = {
        "file": str(csv_path),
        "format": "CSV, UTF-8, заголовок, разделитель ','",
        "seed": a.seed,
        "interval_utc": [a.start, a.end],
        "fields": FIELDS,
        "requested_rates": {
            "missing_rate": a.missing_rate, "invalid_rate": a.invalid_rate,
            "fixable_rate": a.fixable_rate, "duplicate_rate": a.duplicate_rate,
            "partial_duplicate_share": a.partial_duplicate_share,
        },
        "clients": n_clients,
        "base_rows": stats["base_rows"],
        "exact_duplicates_added": stats["exact_duplicates_added"],
        "partial_duplicates_added": stats["partial_duplicates_added"],
        "total_rows_in_file": total,
        "rows_with_missing_value": stats["rows_with_missing_value"],
        "missing_by_field": dict(sorted(missing_by_field.items())),
        "rows_with_invalid_value": stats["rows_with_invalid_value"],
        "invalid_by_kind": dict(sorted(invalid_by_kind.items())),
        "rows_with_missing_and_invalid": stats["rows_with_missing_and_invalid"],
        "rows_with_fixable_deviation": stats["rows_with_fixable_deviation"],
        "fixable_by_kind": dict(sorted(fixable_by_kind.items())),
        "note": ("Дубликаты копируют уже записанные строки (вместе с их возможными "
                 "дефектами), поэтому итоговые числа проблем в файле немного больше "
                 "базовых счётчиков."),
        "file_size_mb": round(csv_path.stat().st_size / 1024 / 1024, 2),
        "generation_seconds": round(time.time() - t0, 2),
    }
    stats_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

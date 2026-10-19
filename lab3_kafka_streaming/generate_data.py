#!/usr/bin/env python3
"""Генератор синтетических банковских транзакций (ЛР № 3, вариант № 2).

Два режима:
  raw — основной исторический набор (CSV, ~500 000 строк) с пропусками,
        дубликатами и некорректными значениями;
  new — небольшой пакет новых транзакций для отправки в Kafka (JSON Lines + CSV),
        включает несколько некорректных сообщений, повторы и битый JSON.

Данные полностью синтетические, генерация воспроизводима (seed).
Нужна только стандартная библиотека Python 3.10+.

  python generate_data.py raw --rows 500000 --out data/raw_data.csv
  python generate_data.py new --rows 300 --out data/new_transactions.jsonl
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import lab3_common as C

RUB_PER_UNIT = {"RUB": 1.0, "USD": 88.0, "EUR": 95.0, "CNY": 12.2}  # условные
TYPE_PROFILE = {  # медиана суммы в рублях, разброс логнормального распределения, ставка комиссии
    "payment": (1_500, 1.0, 0.0), "transfer": (8_000, 1.1, 0.005), "withdrawal": (5_000, 0.8, 0.01),
    "deposit": (20_000, 1.0, 0.0), "refund": (1_200, 0.9, 0.0),
}
CITY_LEVEL = {"Moscow": 1.30, "Saint Petersburg": 1.15, "Kaliningrad": 0.95, "Vladivostok": 1.05}
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 4, 7, 9, 10, 10, 10, 11, 11, 10, 10, 10, 11, 12, 12, 10, 7, 4, 2]

INVALID_KINDS = [
    "negative_amount", "zero_amount", "non_numeric_amount", "huge_amount", "unknown_currency",
    "unknown_city", "unknown_device", "unknown_type", "malformed_timestamp", "timestamp_out_of_range",
]
FIXABLE_KINDS = ["case_and_spaces", "lowercase_currency", "iso_timestamp"]


def pick(rng: random.Random, weights: dict) -> str:
    keys = list(weights)
    return rng.choices(keys, weights=[weights[k] for k in keys], k=1)[0]


class TransactionFactory:
    """Порождает корректные транзакции; дефекты вносятся отдельно."""

    def __init__(self, rng: random.Random, n_clients: int, start: datetime, span_seconds: int):
        self.rng, self.start, self.span = rng, start, span_seconds
        cities = list(C.CITIES)
        self.clients = []
        for i in range(1, n_clients + 1):
            home = rng.choices(cities, weights=[C.CITIES[c] for c in cities], k=1)[0]
            currency = pick(rng, C.CURRENCIES)
            device = pick(rng, C.DEVICE_TYPES)
            self.clients.append((f"C{i:07d}", home, currency, device))
        self.day_count = max(1, span_seconds // 86_400)

    def make(self, transaction_id: str) -> dict:
        rng = self.rng
        client_id, home, currency, device = self.clients[rng.randrange(len(self.clients))]
        city = home if rng.random() < 0.9 else pick(rng, C.CITIES)
        if rng.random() < 0.15:
            currency = pick(rng, C.CURRENCIES)
        if rng.random() < 0.2:
            device = pick(rng, C.DEVICE_TYPES)
        tx_type = pick(rng, C.TRANSACTION_TYPES)
        if tx_type == "withdrawal" and rng.random() < 0.7:
            device = "atm"
        median, sigma, fee_rate = TYPE_PROFILE[tx_type]
        rub = rng.lognormvariate(math.log(median * CITY_LEVEL.get(city, 1.0)), sigma)
        amount = round(max(rub / RUB_PER_UNIT[currency], 0.5), 2)
        fee = round(amount * fee_rate, 2)
        day = rng.randrange(self.day_count)
        hour = rng.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]
        ts = self.start + timedelta(days=day, hours=hour, seconds=rng.randrange(3600))
        return {
            "transaction_id": transaction_id, "client_id": client_id, "transaction_type": tx_type,
            "amount": f"{amount:.2f}", "currency": currency, "city": city, "device_type": device,
            "timestamp": ts.strftime(C.TIMESTAMP_FORMAT), "fee": f"{fee:.2f}",
        }


def apply_invalid(rng, rec, kind):
    if kind == "negative_amount":
        rec["amount"] = "-" + rec["amount"]
    elif kind == "zero_amount":
        rec["amount"] = "0.00"
    elif kind == "non_numeric_amount":
        rec["amount"] = rng.choice(["abc", "1,250.00", "N/A", "12.5.1"])
    elif kind == "huge_amount":
        rec["amount"] = f"{rng.uniform(5e7, 9e8):.2f}"
    elif kind == "unknown_currency":
        rec["currency"] = rng.choice(["XXX", "RUR", "$$"])
    elif kind == "unknown_city":
        rec["city"] = rng.choice(["Atlantis", "N/A", "Gotham"])
    elif kind == "unknown_device":
        rec["device_type"] = rng.choice(["smart_fridge", "fax"])
    elif kind == "unknown_type":
        rec["transaction_type"] = rng.choice(["chargeback_x", "???"])
    elif kind == "malformed_timestamp":
        rec["timestamp"] = rng.choice(["2025-02-30 12:00:00", "31/12/2025 10:00", "not_a_date", "2025-13-01 25:00:00"])
    elif kind == "timestamp_out_of_range":
        rec["timestamp"] = rng.choice(["2031-01-01 00:00:00", "1999-12-31 23:59:59"])


def apply_fixable(rng, rec, kind):
    if kind == "case_and_spaces":
        rec["city"] = rng.choice([rec["city"].upper(), f"  {rec['city']} ", rec["city"].lower()])
        rec["device_type"] = rec["device_type"].upper()
    elif kind == "lowercase_currency":
        rec["currency"] = rec["currency"].lower()
    elif kind == "iso_timestamp":
        rec["timestamp"] = rec["timestamp"].replace(" ", "T")


def partial_duplicate(rng, rec):
    """Повтор той же транзакции с иным оформлением (регистр города, формат даты)."""
    dup = dict(rec)
    if dup["city"].strip():
        dup["city"] = dup["city"].strip().upper()
    elif " " in dup["timestamp"]:
        dup["timestamp"] = dup["timestamp"].replace(" ", "T")
    return dup if dup != rec else None


def generate_raw(out: Path, rows: int, seed: int, start: str, end: str,
                 missing_rate=0.02, invalid_rate=0.01, fixable_rate=0.02, duplicate_rate=0.01,
                 partial_share=0.3) -> dict:
    """Основной набор. Возвращает и сохраняет статистику генерации (<out>.stats.json)."""
    rng = random.Random(seed)
    t0 = time.time()
    s, e = datetime.fromisoformat(start), datetime.fromisoformat(end)
    factory = TransactionFactory(rng, max(100, rows // 25), s, int((e - s).total_seconds()))
    stats, missing, invalid, fixable = Counter(), Counter(), Counter(), Counter()
    pool = []
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=C.ALL_FIELDS)
        w.writeheader()
        for i in range(1, rows + 1):
            rec = factory.make(f"T{i:09d}")
            if rng.random() < fixable_rate:
                k = rng.choice(FIXABLE_KINDS)
                apply_fixable(rng, rec, k)
                fixable[k] += 1
            if rng.random() < invalid_rate:
                k = rng.choice(INVALID_KINDS)
                apply_invalid(rng, rec, k)
                invalid[k] += 1
            if rng.random() < missing_rate:
                f = rng.choice(C.REQUIRED_FIELDS)
                rec[f] = rng.choice(["", " "])
                missing[f] += 1
            w.writerow(rec)
            stats["base_rows"] += 1
            if len(pool) < 20_000:
                pool.append(rec)
            elif rng.random() < 20_000 / i:
                pool[rng.randrange(20_000)] = rec
        for _ in range(int(round(rows * duplicate_rate))):
            src = pool[rng.randrange(len(pool))]
            dup = partial_duplicate(rng, src) if rng.random() < partial_share else None
            if dup is not None:
                w.writerow(dup)
                stats["partial_duplicates"] += 1
            else:
                w.writerow(src)
                stats["exact_duplicates"] += 1
    result = {
        "file": str(out), "seed": seed, "period": [start, end], "fields": C.ALL_FIELDS,
        "base_rows": stats["base_rows"], "exact_duplicates_added": stats["exact_duplicates"],
        "partial_duplicates_added": stats["partial_duplicates"],
        "total_rows_in_file": stats["base_rows"] + stats["exact_duplicates"] + stats["partial_duplicates"],
        "rows_with_missing_value": sum(missing.values()), "missing_by_field": dict(sorted(missing.items())),
        "rows_with_invalid_value": sum(invalid.values()), "invalid_by_kind": dict(sorted(invalid.items())),
        "rows_with_fixable_deviation": sum(fixable.values()), "fixable_by_kind": dict(sorted(fixable.items())),
        "rates": {"missing": missing_rate, "invalid": invalid_rate, "fixable": fixable_rate,
                  "duplicate": duplicate_rate, "partial_share": partial_share},
        "file_size_mb": round(out.stat().st_size / 2 ** 20, 2),
        "seconds": round(time.time() - t0, 2),
    }
    C.write_json(out.with_suffix(".stats.json"), result)
    return result


def generate_new(out_jsonl: Path, rows: int, seed: int, start: str, batch_tag: str,
                 invalid=5, repeats=3, malformed=2, out_csv: Path | None = None) -> dict:
    """Пакет новых транзакций для Kafka.

    В JSON Lines попадают: `rows` корректных транзакций, `invalid` некорректных
    (неверная сумма, пропущенное обязательное поле, неизвестная валюта, плохая дата),
    `repeats` повторов уже включённых транзакций (тот же transaction_id) и
    `malformed` строк с битым JSON. Порядок перемешивается детерминированно.
    """
    rng = random.Random(seed)
    s = datetime.fromisoformat(start)
    factory = TransactionFactory(rng, max(50, rows // 3), s, 86_400)
    good = [factory.make(f"S-{batch_tag}-{i:06d}") for i in range(1, rows + 1)]
    for r in good:  # в JSON суммы — числа, как у реального источника
        r["amount"], r["fee"] = float(r["amount"]), float(r["fee"])
    bad = []
    kinds = ["negative_amount", "missing_client_id", "unknown_currency", "bad_timestamp", "amount_as_text"]
    for j in range(invalid):
        r = factory.make(f"S-{batch_tag}-X{j + 1:04d}")
        r["amount"], r["fee"] = float(r["amount"]), float(r["fee"])
        kind = kinds[j % len(kinds)]
        if kind == "negative_amount":
            r["amount"] = -abs(r["amount"])
        elif kind == "missing_client_id":
            del r["client_id"]
        elif kind == "unknown_currency":
            r["currency"] = "XXX"
        elif kind == "bad_timestamp":
            r["timestamp"] = "2026-02-30 25:61:00"
        else:
            r["amount"] = "abc"
        bad.append(r)
    dups = [dict(good[rng.randrange(len(good))]) for _ in range(min(repeats, len(good)))]
    lines = [json.dumps(r, ensure_ascii=False) for r in good + bad + dups]
    lines += ['{"transaction_id": "S-%s-BROKEN-%d", "amount": ' % (batch_tag, k + 1) for k in range(malformed)]
    rng.shuffle(lines)
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    out_jsonl.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if out_csv is not None:
        with out_csv.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=C.ALL_FIELDS, extrasaction="ignore")
            w.writeheader()
            for line in lines:
                try:
                    w.writerow(json.loads(line))
                except json.JSONDecodeError:
                    continue
    result = {"file": str(out_jsonl), "seed": seed, "batch_tag": batch_tag, "valid_transactions": rows,
              "invalid_transactions": invalid, "repeated_messages": len(dups),
              "malformed_json_lines": malformed, "total_messages": len(lines),
              "expected_accepted": rows, "start": start}
    C.write_json(out_jsonl.with_suffix(".stats.json"), result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="mode", required=True)
    s = C.Settings()
    r = sub.add_parser("raw", help="основной набор CSV")
    r.add_argument("--rows", type=int, default=s.raw_rows)
    r.add_argument("--seed", type=int, default=s.seed)
    r.add_argument("--out", type=Path, default=Path("data/raw_data.csv"))
    r.add_argument("--start", default=s.period_start)
    r.add_argument("--end", default=s.period_end)
    r.add_argument("--missing-rate", type=float, default=0.02)
    r.add_argument("--invalid-rate", type=float, default=0.01)
    r.add_argument("--duplicate-rate", type=float, default=0.01)
    n = sub.add_parser("new", help="пакет новых транзакций для Kafka (JSON Lines)")
    n.add_argument("--rows", type=int, default=s.new_rows)
    n.add_argument("--seed", type=int, default=s.new_seed)
    n.add_argument("--out", type=Path, default=Path("data/new_transactions.jsonl"))
    n.add_argument("--start", default=s.new_period_start)
    n.add_argument("--batch-tag", default=s.batch_tag)
    n.add_argument("--invalid", type=int, default=5)
    n.add_argument("--repeats", type=int, default=3)
    n.add_argument("--malformed", type=int, default=2)
    a = p.parse_args()
    if a.mode == "raw":
        res = generate_raw(a.out, a.rows, a.seed, a.start, a.end, a.missing_rate, a.invalid_rate,
                           duplicate_rate=a.duplicate_rate)
    else:
        res = generate_new(a.out, a.rows, a.seed, a.start, a.batch_tag, a.invalid, a.repeats,
                           a.malformed, out_csv=a.out.with_suffix(".csv"))
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

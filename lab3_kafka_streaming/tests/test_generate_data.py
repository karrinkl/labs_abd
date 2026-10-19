"""Генератор: схема, масштаб, воспроизводимость, наличие проблем качества."""
import csv
import hashlib
import json
from datetime import datetime

import generate_data as G
import lab3_common as C


def _rows(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_raw_has_all_required_fields_and_expected_scale(tmp_path):
    out = tmp_path / "raw.csv"
    stats = G.generate_raw(out, 20_000, 1, "2025-01-01", "2026-01-01")
    rows = _rows(out)
    assert list(rows[0].keys()) == C.ALL_FIELDS
    assert set(C.REQUIRED_FIELDS) <= set(rows[0].keys())
    assert len(rows) == stats["total_rows_in_file"] == 20_000 + 200  # 1 % дубликатов
    assert (tmp_path / "raw.stats.json").exists()


def test_generation_is_reproducible_and_seed_dependent(tmp_path):
    def digest(seed, name):
        G.generate_raw(tmp_path / name, 3_000, seed, "2025-01-01", "2026-01-01")
        return hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()

    assert digest(7, "a.csv") == digest(7, "b.csv")
    assert digest(7, "c.csv") != digest(8, "d.csv")


def test_raw_contains_missing_duplicates_and_invalid_values(tmp_path):
    out = tmp_path / "raw.csv"
    stats = G.generate_raw(out, 30_000, 3, "2025-01-01", "2026-01-01")
    rows = _rows(out)
    blanks = sum(1 for r in rows for f in C.REQUIRED_FIELDS if not r[f].strip())
    ids = [r["transaction_id"] for r in rows if r["transaction_id"].strip()]
    full = [tuple(r.values()) for r in rows]

    def bad_amount(v):
        try:
            return float(v) <= 0
        except ValueError:
            return bool(v.strip())

    def bad_ts(v):
        try:
            datetime.strptime(v, C.TIMESTAMP_FORMAT)
            return False
        except ValueError:
            return "T" not in v and bool(v.strip())

    assert blanks >= stats["rows_with_missing_value"] * 0.9 > 0
    assert len(ids) - len(set(ids)) > 0, "нет повторов transaction_id"
    assert len(full) - len(set(full)) > 0, "нет полных дубликатов"
    assert sum(bad_amount(r["amount"]) for r in rows) > 0
    assert sum(bad_ts(r["timestamp"]) for r in rows) > 0
    assert sum(bool(r["currency"].strip()) and r["currency"] not in C.CURRENCIES for r in rows) > 0


def test_valid_rows_have_correct_formats(tmp_path):
    out = tmp_path / "clean.csv"
    G.generate_raw(out, 2_000, 5, "2025-01-01", "2026-01-01", missing_rate=0, invalid_rate=0,
                   fixable_rate=0, duplicate_rate=0)
    for r in _rows(out):
        ts = datetime.strptime(r["timestamp"], C.TIMESTAMP_FORMAT)
        assert datetime(2025, 1, 1) <= ts < datetime(2026, 1, 1)
        assert float(r["amount"]) > 0 and float(r["fee"]) >= 0
        assert r["currency"] in C.CURRENCIES and r["city"] in C.CITIES
        assert r["transaction_type"] in C.TRANSACTION_TYPES and r["device_type"] in C.DEVICE_TYPES


def test_new_batch_composition(tmp_path):
    out = tmp_path / "new.jsonl"
    st = G.generate_new(out, 50, 9, "2026-01-01", "T01", invalid=5, repeats=3, malformed=2, out_csv=tmp_path / "new.csv")
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == st["total_messages"] == 60
    parsed, broken = [], 0
    for ln in lines:
        try:
            parsed.append(json.loads(ln))
        except json.JSONDecodeError:
            broken += 1
    assert broken == 2
    ids = [p["transaction_id"] for p in parsed]
    assert len(ids) - len(set(ids)) == 3                      # повторы
    assert any("client_id" not in p for p in parsed)          # нет обязательного поля
    assert any(p.get("amount") == "abc" for p in parsed)      # неверный тип
    assert all(i.startswith("S-T01-") for i in ids)
    assert len(_rows(tmp_path / "new.csv")) == 58             # битые строки в CSV не попадают


def test_empty_new_batch(tmp_path):
    out = tmp_path / "empty.jsonl"
    st = G.generate_new(out, 0, 1, "2026-01-01", "E", invalid=0, repeats=0, malformed=0)
    assert st["total_messages"] == 0
    import producer as P
    assert P.read_lines(out) == []

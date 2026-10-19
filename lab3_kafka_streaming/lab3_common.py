"""Общие настройки, справочники и вспомогательные функции ЛР № 3 (вариант № 2).

Модуль не зависит от Spark и Kafka: его импортируют генератор, продюсер,
Spark-задания, тесты и блокнот Colab. Все настраиваемые параметры собраны в
dataclass Settings; значения по умолчанию можно переопределить переменными
окружения с префиксом LAB3_ (см. .env.example).
"""
from __future__ import annotations

import csv
import json
import logging
import os
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Схема варианта № 2
# ---------------------------------------------------------------------------
REQUIRED_FIELDS = [
    "transaction_id", "client_id", "transaction_type", "amount",
    "currency", "city", "device_type", "timestamp",
]
EXTRA_FIELDS = ["fee"]  # комиссия банка: второй осмысленный числовой признак
ALL_FIELDS = REQUIRED_FIELDS + EXTRA_FIELDS

TRANSACTION_TYPES = {"payment": 0.42, "transfer": 0.24, "withdrawal": 0.16, "deposit": 0.12, "refund": 0.06}
CURRENCIES = {"RUB": 0.62, "USD": 0.15, "EUR": 0.14, "CNY": 0.09}
CITIES = {
    "Moscow": 0.30, "Saint Petersburg": 0.16, "Novosibirsk": 0.08, "Yekaterinburg": 0.08,
    "Kazan": 0.07, "Nizhny Novgorod": 0.06, "Krasnodar": 0.06, "Samara": 0.05,
    "Rostov-on-Don": 0.05, "Vladivostok": 0.04, "Kaliningrad": 0.03, "Tomsk": 0.02,
}
DEVICE_TYPES = {"mobile": 0.52, "desktop": 0.21, "atm": 0.12, "pos_terminal": 0.10, "tablet": 0.05}

# Условные фиксированные курсы для приведения сумм к одной валюте (EUR).
# Не являются рыночными котировками; нужны, чтобы не складывать рубли с долларами.
EUR_RATES = {"RUB": "0.0105", "USD": "0.92", "EUR": "1.00", "CNY": "0.13"}
MAX_REASONABLE_AMOUNT = 10_000_000  # сумма больше — явная ошибка ввода
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"  # формат в данных
SPARK_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
UNKNOWN = "unknown"
UNKNOWN_CITY = "Unknown"


# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------
def _env(name: str, default):
    raw = os.environ.get(f"LAB3_{name}")
    if raw in (None, ""):
        return default
    if isinstance(default, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    return type(default)(raw)


@dataclass
class Settings:
    # Корневой каталог проекта в Google Drive (или локально при отладке)
    base_dir: str = _env("BASE_DIR", "/content/drive/MyDrive/lab3_bank")
    # Локальный рабочий каталог Spark (checkpoint, хранилище потоковых транзакций).
    # Держится на диске ВМ Colab и синхронизируется в Drive (см. README, раздел «Хранение»).
    work_dir: str = _env("WORK_DIR", "/content/lab3_work")
    secrets_dir: str = _env("SECRETS_DIR", "/content/kafka_secrets")  # НЕ в Google Drive

    # Генерация данных
    seed: int = _env("SEED", 42)
    raw_rows: int = _env("RAW_ROWS", 500_000)
    new_rows: int = _env("NEW_ROWS", 300)
    new_seed: int = _env("NEW_SEED", 2026)
    batch_tag: str = _env("BATCH_TAG", "B001")
    period_start: str = _env("PERIOD_START", "2025-01-01")
    period_end: str = _env("PERIOD_END", "2026-01-01")  # не включается
    new_period_start: str = _env("NEW_PERIOD_START", "2026-01-01")

    # Kafka (Aiven)
    topic: str = _env("TOPIC", "bank-transactions-v2")
    auth_mode: str = _env("AUTH_MODE", "certificate")  # certificate | sasl
    sasl_mechanism: str = _env("SASL_MECHANISM", "SCRAM-SHA-256")
    consumer_group: str = _env("CONSUMER_GROUP", "lab3-verify")

    # Spark Structured Streaming
    trigger_seconds: int = _env("TRIGGER_SECONDS", 5)
    starting_offsets: str = _env("STARTING_OFFSETS", "earliest")
    max_offsets_per_trigger: int = _env("MAX_OFFSETS_PER_TRIGGER", 5000)
    shuffle_partitions: int = _env("SHUFFLE_PARTITIONS", 4)
    kafka_package: str = _env("KAFKA_PACKAGE", "")  # пусто = по версии PySpark

    extra: dict = field(default_factory=dict)

    # ---- производные пути
    @property
    def data_dir(self) -> Path:
        return Path(self.base_dir) / "lab3_data"

    @property
    def results_dir(self) -> Path:
        return Path(self.base_dir) / "lab3_results"

    @property
    def intermediate_dir(self) -> Path:
        """Промежуточные данные в Drive: очищенная история и копия рабочего каталога."""
        return Path(self.base_dir) / "lab3_intermediate"

    @property
    def raw_csv(self) -> Path:
        return self.data_dir / "raw_data.csv"

    @property
    def new_jsonl(self) -> Path:
        return self.data_dir / "new_transactions.jsonl"

    @property
    def new_csv(self) -> Path:
        return self.data_dir / "new_transactions.csv"

    @property
    def clean_history_dir(self) -> Path:
        return Path(self.work_dir) / "clean_history.parquet"

    @property
    def stream_store_dir(self) -> Path:
        return Path(self.work_dir) / "stream_transactions"

    @property
    def stream_rejected_dir(self) -> Path:
        return Path(self.work_dir) / "stream_rejected"

    @property
    def checkpoint_dir(self) -> Path:
        return Path(self.work_dir) / "checkpoints"

    @property
    def work_backup_dir(self) -> Path:
        return self.intermediate_dir / "work_backup"

    def ensure_dirs(self):
        for p in (self.data_dir, self.results_dir, self.intermediate_dir,
                  Path(self.work_dir), self.checkpoint_dir):
            p.mkdir(parents=True, exist_ok=True)

    def as_dict(self):
        d = asdict(self)
        d.pop("extra", None)
        return d


# ---------------------------------------------------------------------------
# Журнал выполнения
# ---------------------------------------------------------------------------
def setup_logging(log_file: Path | str | None, level=logging.INFO) -> logging.Logger:
    """Логгер lab3: консоль + файл (дозапись). Повторный вызов не дублирует обработчики."""
    logger = logging.getLogger("lab3")
    logger.setLevel(level)
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    have_console = any(getattr(h, "_lab3", None) == "console" for h in logger.handlers)
    if not have_console:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(fmt)
        h._lab3 = "console"
        logger.addHandler(h)
    if log_file:
        log_file = str(log_file)
        for h in list(logger.handlers):
            if getattr(h, "_lab3", None) == "file" and h.baseFilename != os.path.abspath(log_file):
                logger.removeHandler(h)
                h.close()
        if not any(getattr(h, "_lab3", None) == "file" for h in logger.handlers):
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(fmt)
            fh._lab3 = "file"
            logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Файлы результатов
# ---------------------------------------------------------------------------
def write_csv_atomic(path: Path | str, rows: list[dict], fieldnames: list[str] | None = None):
    """Записать небольшой CSV атомарно: временный файл + os.replace.

    Повторный запуск перезаписывает файл целиком, а читатель никогда не увидит
    наполовину записанный файл.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.chmod(tmp, 0o644)  # mkstemp создаёт файл с правами 600
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def read_csv(path: Path | str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_json(path: Path | str, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, default=str)
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)


def compare_dimension_stats(before: list[dict], after: list[dict]) -> list[dict]:
    """Таблица изменений «до/после» по ключу (dimension, value)."""
    def key(r):
        return r["dimension"], r["value"]

    b = {key(r): r for r in before}
    a = {key(r): r for r in after}
    out = []
    for k in sorted(set(b) | set(a)):
        rb, ra = b.get(k, {}), a.get(k, {})
        cb, ca = int(rb.get("tx_count", 0) or 0), int(ra.get("tx_count", 0) or 0)
        sb, sa = float(rb.get("amount_eur_sum", 0) or 0), float(ra.get("amount_eur_sum", 0) or 0)
        out.append({
            "dimension": k[0], "value": k[1],
            "tx_count_before": cb, "tx_count_after": ca, "tx_count_delta": ca - cb,
            "amount_eur_sum_before": round(sb, 2), "amount_eur_sum_after": round(sa, 2),
            "amount_eur_sum_delta": round(sa - sb, 2),
            "avg_amount_eur_before": round(sb / cb, 2) if cb else None,
            "avg_amount_eur_after": round(sa / ca, 2) if ca else None,
        })
    return out


def show_rows(rows: list[dict], limit: int = 20, title: str | None = None):
    """Показать список словарей таблицей (pandas в Colab, иначе текстом)."""
    if title:
        print(title)
    rows = rows[:limit]
    try:
        import pandas as pd
        from IPython.display import display
        display(pd.DataFrame(rows))
    except Exception:
        if not rows:
            print("(нет строк)")
            return
        cols = list(rows[0])
        print(" | ".join(cols))
        for r in rows:
            print(" | ".join(str(r.get(c, "")) for c in cols))

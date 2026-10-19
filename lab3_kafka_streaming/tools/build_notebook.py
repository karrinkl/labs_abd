#!/usr/bin/env python3
"""Собирает lab3_bank_transactions.ipynb из модулей проекта.

Модули .py — единственный источник кода: они встраиваются в блокнот ячейками
%%writefile, поэтому в Colab достаточно загрузить один .ipynb, а локальные
тесты проверяют тот же самый код.

  python tools/build_notebook.py            # -> lab3_bank_transactions.ipynb
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODULES = ["lab3_common.py", "generate_data.py", "kafka_security.py", "producer.py", "spark_jobs.py"]
cells: list[dict] = []


def md(text: str):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": text.strip("\n")})


def code(text: str):
    cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
                  "source": text.strip("\n")})


md("""
# ЛР № 3. Потоковая обработка банковских транзакций: Apache Kafka (Aiven) + Spark Structured Streaming

**Вариант № 2 — банковские транзакции.** Поля: `transaction_id`, `client_id`, `transaction_type`, `amount`, `currency`, `city`, `device_type`, `timestamp` (+ дополнительное `fee` — комиссия).

**Поток данных:** генератор → `raw_data.csv` в Google Drive → пакетная обработка PySpark → продюсер отправляет новые транзакции в топик Aiven Kafka по SSL → Spark Structured Streaming читает топик, проверяет сообщения и сохраняет принятые → итоговые показатели (история + поток) пересчитываются после каждого микропакета → результаты, промежуточные данные и журнал сохраняются в Google Drive.

**Перед запуском** (подробно — README.md, раздел «Aiven»):
1. Сервис Aiven for Apache Kafka в состоянии **Running**, создан топик `bank-transactions-v2`.
2. Скачаны `ca.pem`, `service.cert`, `service.key` (Quick connect → Python). Их можно сохранить в «Секретах» Colab (значок ключа слева) или загрузить в ячейке раздела 7. **Не кладите их в Google Drive.**
3. В «Секретах» Colab задан `KAFKA_BOOTSTRAP_SERVERS` (Service URI вида `host:port`) с доступом для этого блокнота.

**Порядок:** выполняйте ячейки сверху вниз (Runtime → Run all). Повторный запуск ячеек безопасен: SparkSession не создаётся заново, потоки перед новым запуском останавливаются, генерация пропускается, если данные уже есть.

Среда — **Google Colab** (одна ВМ, Spark в режиме `local[*]`), а не Google Cloud Dataproc и не распределённый кластер.
""")

md("## 0. Подготовка среды")
code(r'''
# Определение среды, установка зависимостей, проверка Java/OpenSSL/keytool (повторный запуск безопасен)
import importlib, os, shutil, subprocess, sys, tempfile

IN_COLAB = "google.colab" in sys.modules
CODE_DIR = "/content/lab3_code" if IN_COLAB else os.environ.get("LAB3_CODE_DIR", tempfile.mkdtemp(prefix="lab3_code_"))
os.makedirs(CODE_DIR, exist_ok=True)
if CODE_DIR not in sys.path:
    sys.path.insert(0, CODE_DIR)

try:
    import confluent_kafka  # noqa: F401
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "confluent-kafka==2.16.0"], check=True)
import confluent_kafka
import pyspark

def _ver(cmd, must_contain=""):
    p = subprocess.run(cmd, capture_output=True, text=True)
    lines = (p.stdout + p.stderr).strip().splitlines()
    return next((l for l in lines if must_contain in l), lines[0] if lines else "?")

java_line = _ver(["java", "-version"], 'version "')
print("Среда            :", "Google Colab" if IN_COLAB else "локально (отладка)")
print("Python           :", sys.version.split()[0])
print("PySpark          :", pyspark.__version__, "(проверено с 4.0.4)")
print("Java             :", java_line)
_ck = confluent_kafka.version()
print("confluent-kafka  :", _ck if isinstance(_ck, str) else _ck[0], "/ librdkafka", confluent_kafka.libversion()[0])
print("OpenSSL          :", _ver(["openssl", "version"]))
print("keytool          :", shutil.which("keytool") or "НЕ НАЙДЕН")
import re
m = re.search(r'version "(\d+)', java_line)
if not m or int(m.group(1)) < 17:
    raise RuntimeError("PySpark 4 требует Java 17+. В Colab по умолчанию установлен OpenJDK 21 — проверьте вывод java -version.")
''')

md("""
## 1. Код проекта

Ячейки ниже записывают модули проекта в `/content/lab3_code` (локальный диск ВМ). Это те же файлы, что лежат в репозитории; блокнот собирается из них скриптом `tools/build_notebook.py`. Ячейки можно свернуть — достаточно их выполнить.
""")
for mod in MODULES:
    src = (ROOT / mod).read_text(encoding="utf-8")
    code(f"%%writefile {{CODE_DIR}}/{mod}\n{src}")
code(r'''
# Импорт модулей (reload — чтобы повторный запуск подхватил изменения)
import lab3_common as C, generate_data as G, kafka_security as K, producer as P, spark_jobs as J
for _m in (C, G, K, P, J):
    importlib.reload(_m)
print("Модули загружены из", CODE_DIR)
''')

md("## 2. Настройки (все параметры — в одном месте)")
code(r'''
settings = C.Settings(
    base_dir="/content/drive/MyDrive/lab3_bank" if IN_COLAB else os.environ["LAB3_BASE_DIR"],
    work_dir="/content/lab3_work" if IN_COLAB else os.environ.get("LAB3_WORK_DIR", "/tmp/lab3_work"),
    secrets_dir="/content/kafka_secrets" if IN_COLAB else os.environ.get("LAB3_SECRETS_DIR", "/tmp/lab3_secrets"),
    seed=42, raw_rows=500_000,                 # основной набор (~500 000 строк)
    new_rows=300, new_seed=2026, batch_tag="B001",  # пакет новых транзакций для Kafka
    topic="bank-transactions-v2",
    auth_mode="certificate",                   # "certificate" (ca.pem + service.cert + service.key) или "sasl"
    trigger_seconds=5, starting_offsets="earliest", shuffle_partitions=4,
)
REGENERATE_RAW = False          # True — пересоздать raw_data.csv (иначе используется существующий)
CREATE_TOPIC_IF_MISSING = os.environ.get("LAB3_CREATE_TOPIC", "0") == "1"  # в Aiven топик создаётся в UI
RUN_RESEND_DEMO = True          # повторная отправка того же пакета (проверка идемпотентности)
print({k: v for k, v in settings.as_dict().items()})
''')

md("## 3. Google Drive и журнал выполнения")
code(r'''
if IN_COLAB:
    from google.colab import drive
    drive.mount("/content/drive")
settings.ensure_dirs()
log = C.setup_logging(settings.results_dir / "execution_log.txt")
log.info("==== Запуск блокнота ЛР3; среда=%s; base_dir=%s", "colab" if IN_COLAB else "local", settings.base_dir)
for p in (settings.data_dir, settings.results_dir, settings.intermediate_dir, settings.work_dir):
    print(p)
''')

md("## 4. Генерация синтетических данных")
code(r'''
if settings.raw_csv.exists() and not REGENERATE_RAW:
    raw_stats = json.load(open(settings.raw_csv.with_suffix(".stats.json"), encoding="utf-8")) \
        if settings.raw_csv.with_suffix(".stats.json").exists() else {"file": str(settings.raw_csv)}
    log.info("raw_data.csv уже существует — генерация пропущена (REGENERATE_RAW=False)")
else:
    raw_stats = G.generate_raw(settings.raw_csv, settings.raw_rows, settings.seed,
                               settings.period_start, settings.period_end)
    log.info("Сгенерирован raw_data.csv: %s строк, %.1f МБ", raw_stats["total_rows_in_file"], raw_stats["file_size_mb"])
new_stats = G.generate_new(settings.new_jsonl, settings.new_rows, settings.new_seed, settings.new_period_start,
                           settings.batch_tag, out_csv=settings.new_csv)
log.info("Пакет для Kafka: %s сообщений (%s корректных)", new_stats["total_messages"], new_stats["valid_transactions"])
print(json.dumps(raw_stats, ensure_ascii=False, indent=1))
print(json.dumps(new_stats, ensure_ascii=False, indent=1))
with open(settings.raw_csv, encoding="utf-8") as fh:
    for _ in range(4):
        print(fh.readline().rstrip())
''')
cells[-1]["source"] = "import json\n" + cells[-1]["source"]

md("## 5. SparkSession")
code(r'''
spark = J.get_spark(settings)          # повторный запуск вернёт существующую сессию
env_info = J.environment_info(spark)
C.write_json(settings.results_dir / "environment.json", env_info)
for k, v in env_info.items():
    print(f"{k:20}: {v}")
''')

md("""
## 6. Пакетная обработка исторических данных

CSV читается с **явной схемой**: все поля — строки (в данных есть «грязные» значения). Типы приводятся безопасно: `try_cast` для сумм, `try_to_timestamp` для дат. Ошибка преобразования даёт NULL и причину отказа, а не падение задания (в Spark 4 по умолчанию включён режим ANSI).
""")
code(r'''
raw = J.read_raw_csv(spark, str(settings.raw_csv)).cache()
n_rows, n_cols = raw.count(), len(raw.columns)
raw.printSchema()
raw.show(5, truncate=False)
print(f"Строк: {n_rows}, столбцов: {n_cols}")
log.info("Прочитан raw_data.csv: %s строк x %s столбцов", n_rows, n_cols)
''')
code(r'''
quality = J.quality_report(raw, settings)
C.write_csv_atomic(settings.results_dir / "data_quality_report.csv", quality)
C.show_rows(quality, 40, "Отчёт о качестве исходных данных (data_quality_report.csv)")
''')
code(r'''
res = J.clean_batch(raw, settings)
clean = res["clean"].cache()
C.write_csv_atomic(settings.results_dir / "cleaning_summary.csv", res["steps"])
C.show_rows(res["steps"], 40, "Очистка (cleaning_summary.csv)")
print(f"До очистки: {res['raw_rows']}, после: {res['clean_rows']}, баланс сходится: {res['balance_ok']}")
assert res["balance_ok"], "сумма удалённых и оставшихся строк не равна исходной"
sample = [r.asDict() for r in res["rejected"].orderBy("reject_reason").limit(50).collect()]
C.write_csv_atomic(settings.results_dir / "rejected_sample.csv", sample)
log.info("Очистка: %s -> %s строк", res["raw_rows"], res["clean_rows"])
''')
code(r'''
num = J.numeric_statistics(clean)
C.write_csv_atomic(settings.results_dir / "numeric_statistics.csv", num)
C.show_rows(num, title="Числовая статистика (numeric_statistics.csv)")
print("Исключены из числовой статистики: transaction_id, client_id — идентификаторы (среднее бессмысленно); "
      "timestamp — время (см. min/max ниже).")
clean.agg(F.min("timestamp").alias("min_timestamp"), F.max("timestamp").alias("max_timestamp")).show()
''')
cells[-1]["source"] = "from pyspark.sql import functions as F\n" + cells[-1]["source"]
code(r'''
top = J.top_categories(clean, 5)
C.write_csv_atomic(settings.results_dir / "top_categories.csv", top)
C.show_rows(top, 25, "Топ-5 значений категориальных столбцов (top_categories.csv)")
''')
code(r'''
groups = {
    "city_statistics.csv": J.group_statistics(clean, ["city"]),
    "currency_statistics.csv": J.group_statistics(clean, ["currency"], amount_col="amount"),
    "transaction_type_statistics.csv": J.group_statistics(clean, ["transaction_type"]),
    "city_currency_type_statistics.csv": J.group_statistics(clean, ["city", "currency", "transaction_type"]),
}
for name, rows in groups.items():
    C.write_csv_atomic(settings.results_dir / name, rows)
C.show_rows(groups["city_statistics.csv"], 15, "Транзакции по городам (city_statistics.csv; суммы в EUR по условным курсам)")
C.show_rows(groups["currency_statistics.csv"], 10, "По валютам (суммы в исходной валюте)")
C.show_rows(groups["transaction_type_statistics.csv"], 10, "По типам операций")
''')
code(r'''
# Очищенная история: Parquet на локальном диске (для Spark) + копия в Drive (промежуточные данные)
clean.write.mode("overwrite").parquet(str(settings.clean_history_dir))
drive_copy = settings.intermediate_dir / "clean_history.parquet"
if drive_copy.exists():
    shutil.rmtree(drive_copy)
shutil.copytree(settings.clean_history_dir, drive_copy)
history = spark.read.parquet(str(settings.clean_history_dir))
history_stats = J.dimension_stats_df(history).cache()
history_ids = history.select("transaction_id").cache()
history_stats.count(); history_ids.count()
C.write_csv_atomic(settings.results_dir / "history_dimension_statistics.csv", J.rows_from_dimension_df(history_stats))
log.info("История сохранена: %s; агрегаты по измерениям: %s строк", drive_copy, history_stats.count())
''')

md("""
## 7. Подключение к Kafka в Aiven (SSL)

* Python-продюсер (librdkafka) использует PEM-файлы напрямую.
* Spark Kafka connector работает в JVM, поэтому из PEM создаются `client.truststore.p12` (CA) и `client.keystore.p12` (сертификат + ключ). Пароли хранилищ генерируются случайно на сессию.
* Секреты лежат только в `/content/kafka_secrets` на диске ВМ (права 700/600) и в Google Drive не попадают. Содержимое ключа и пароли не выводятся.
""")
code(r'''
def _secret(name, prompt=None, hidden=False):
    if IN_COLAB:
        from google.colab import userdata
        try:
            v = userdata.get(name)
            if v:
                return v.strip()
        except Exception:
            pass
        if prompt:
            import getpass
            return (getpass.getpass(prompt) if hidden else input(prompt)).strip()
        return None
    return os.environ.get("LAB3_" + name.replace("KAFKA_", ""), "").strip() or None

bootstrap = _secret("KAFKA_BOOTSTRAP_SERVERS", "Service URI Aiven (host:port): ")
conn = K.KafkaConnection(bootstrap_servers=bootstrap, auth_mode=settings.auth_mode,
                         secrets_dir=settings.secrets_dir,
                         sasl_username=_secret("KAFKA_SASL_USERNAME", "SASL username: ") if settings.auth_mode == "sasl" else None,
                         sasl_password=_secret("KAFKA_SASL_PASSWORD", "SASL password: ", hidden=True) if settings.auth_mode == "sasl" else None)
if IN_COLAB:
    source = K.load_from_colab(conn)
else:
    K.install_from_files(conn, os.environ["LAB3_CA_FILE"], os.environ.get("LAB3_CERT_FILE"), os.environ.get("LAB3_KEY_FILE"))
    source = "local-files"
cert_info = K.validate_pem_files(conn)
stores = K.build_java_stores(conn)
spark_kafka_options = conn.spark_options()     # содержит пароли — не печатать
producer_conf = conn.confluent_config()
print("Источник сертификатов:", source)
print("Подключение:", conn.describe())
print("Сертификаты (открытые сведения):", json.dumps(cert_info, ensure_ascii=False, indent=1))
print("Созданы хранилища:", [os.path.basename(v) for v in stores.values()])
log.info("SSL настроен: %s", conn.describe())
''')
code(r'''
# Проверка подключения и топика
if CREATE_TOPIC_IF_MISSING:     # только для локального стенда; в Aiven топик создаётся в веб-интерфейсе
    from confluent_kafka.admin import AdminClient, NewTopic
    _admin = AdminClient(producer_conf)
    if settings.topic not in _admin.list_topics(timeout=15).topics:
        _admin.create_topics([NewTopic(settings.topic, 2, 1)])[settings.topic].result()
topic_info = P.check_connection(producer_conf, settings.topic)
print(topic_info)
log.info("Kafka доступна: %s", topic_info)
''')
code(r'''
# Граничный случай: недоступный брокер — понятная ошибка вместо зависания
try:
    P.check_connection({**producer_conf, "bootstrap.servers": "localhost:1"}, settings.topic, timeout=5)
except P.KafkaUnavailableError as exc:
    print("Ожидаемая ошибка обработана:", str(exc)[:160])
''')

md("""
## 8. Spark Structured Streaming

* Kafka **хранит** сообщения в топике (retention), а Spark читает их **микропакетами**: раз в `trigger` секунд он берёт offset'ы, появившиеся с прошлого пакета, и обрабатывает их как небольшой DataFrame.
* `startingOffsets="earliest"` действует только при **первом** запуске с пустым checkpoint. Дальше Spark продолжает с offset'ов, записанных в checkpoint. Повторная обработка уже прочитанных сообщений безопасна: дубликаты по `transaction_id` отбрасываются.
* Запрос `accepted_stream` (`foreachBatch`) проверяет сообщения, дописывает принятые транзакции в устойчивое хранилище Parquet и **пересчитывает** итоговые показатели «история + поток».
* Запрос `stream_type_counts` — потоковая агрегация с состоянием (`groupBy` + `outputMode("complete")`, memory sink) только по потоку.
* У каждого запроса свой checkpoint. Checkpoint не удаляется при повторном запуске.
""")
code(r'''
restored = J.restore_work_dir(settings)
print("Рабочий каталог восстановлен из Drive:", restored)
processor = J.StreamProcessor(spark, settings, history_stats, history_ids)
initial_stats = processor.combined_stats()
C.write_csv_atomic(settings.results_dir / "initial_stream_statistics.csv", initial_stats)
C.show_rows([r for r in initial_stats if r["dimension"] in ("total", "transaction_type")], 10,
            "Показатели ДО новых сообщений (initial_stream_statistics.csv)")
''')
code(r'''
queries = J.start_streams(spark, spark_kafka_options, settings, processor)
q_main, q_types = queries["accepted_stream"], queries["stream_type_counts"]
time.sleep(settings.trigger_seconds * 3)
print("Статус accepted_stream:", q_main.status)
print("Последний прогресс:", {k: q_main.lastProgress.get(k) for k in ("batchId", "numInputRows")} if q_main.lastProgress else None)
print("Если новых сообщений нет, Spark ждёт: isDataAvailable=False, микропакеты с данными не создаются.")
''')
cells[-1]["source"] = "import time\n" + cells[-1]["source"]
code(r'''
# Отправка новых транзакций в Kafka (Producer)
expected_ids = {f"S-{settings.batch_tag}-{i:06d}" for i in range(1, settings.new_rows + 1)}
already_ids = J.find_stored_ids(settings, spark, expected_ids)
expected_delta = len(expected_ids - already_ids)
if already_ids:
    print(f"ВНИМАНИЕ: {len(already_ids)} транзакций пакета {settings.batch_tag} уже обработаны ранее — они будут "
          "отклонены как повторы. Для новой порции измените batch_tag в разделе 2.")
lines = P.read_lines(settings.new_jsonl)
reports = P.produce_lines(producer_conf, settings.topic, lines, batch_label=f"lab3-{settings.batch_tag}")
C.write_csv_atomic(settings.results_dir / "kafka_delivery_report.csv", reports)
delivery = P.summarize_delivery(reports)
print(json.dumps(delivery, ensure_ascii=False, indent=1))
log.info("Отправлено в Kafka: %s", delivery)
assert delivery["failed"] == 0, "часть сообщений не доставлена"
''')
code(r'''
# Независимая проверка доставки: читаем отправленные сообщения обратно с метаданными
start_offsets = {p: v["min_offset"] for p, v in delivery["partitions"].items()}
msgs = P.read_messages(producer_conf, settings.topic, start_offsets, max_messages=len(lines))
C.write_csv_atomic(settings.results_dir / "kafka_messages_sample.csv",
                   [{**m, "headers": json.dumps(m["headers"])} for m in msgs])
C.show_rows([{**m, "value": m["value"][:80]} for m in msgs], 6, "Сообщения в топике: partition / offset / timestamp / headers / key / value")
print("Прочитано из топика:", len(msgs), "из", len(lines))
''')
code(r'''
# Ожидание обработки микропакетами Spark
wait = J.wait_for_transactions(settings, spark, expected_ids, query=q_main, timeout_s=240)
print(wait)
J.wait_for_new_input(q_types, -1, 1, timeout_s=60)
C.show_rows(C.read_csv(settings.results_dir / "stream_batches.csv"), 20, "Журнал микропакетов (stream_batches.csv)")
assert wait["complete"], "не все новые транзакции обработаны Spark — см. журнал и статус потока"
''')
code(r'''
print("Потоковая агрегация с состоянием (memory sink, outputMode complete):")
spark.sql("SELECT * FROM stream_type_counts ORDER BY tx_count DESC").show(truncate=False)
rej = spark.read.parquet(str(settings.stream_rejected_dir)) if J._has_parquet(settings.stream_rejected_dir) else None
rej_summary = []
if rej is not None:
    rej_summary = [r.asDict() for r in rej.groupBy("reject_reason").count().orderBy("reject_reason").collect()]
    C.write_csv_atomic(settings.results_dir / "stream_rejected_summary.csv", rej_summary)
    C.show_rows(rej_summary, title="Отклонённые сообщения потока (битый JSON, нет поля, повтор, неверные значения)")
''')
code(r'''
# Сравнение показателей до и после
updated_stats = C.read_csv(settings.results_dir / "updated_stream_statistics.csv")
comparison = C.compare_dimension_stats(initial_stats, updated_stats)
C.write_csv_atomic(settings.results_dir / "analysis_comparison.csv", comparison)
changed = [r for r in comparison if r["tx_count_delta"] != 0]
C.show_rows([r for r in changed if r["dimension"] in ("total", "transaction_type", "currency")], 15,
            "Изменения после новых сообщений (analysis_comparison.csv)")
total = next(r for r in comparison if r["dimension"] == "total")
print(f"Всего транзакций: {total['tx_count_before']} -> {total['tx_count_after']} (+{total['tx_count_delta']}; "
      f"ожидалось +{expected_delta})")
log.info("Итоги обновлены: total %s -> %s", total["tx_count_before"], total["tx_count_after"])
''')
code(r'''
# Повторная отправка того же пакета: все сообщения — повторы, показатели не должны измениться
if RUN_RESEND_DEMO:
    before = {(r["dimension"], r["value"]): r["tx_count"] for r in C.read_csv(settings.results_dir / "updated_stream_statistics.csv")}
    b0 = J.last_batch_id(q_main)
    P.produce_lines(producer_conf, settings.topic, lines, batch_label=f"lab3-{settings.batch_tag}-resend")
    print(J.wait_for_new_input(q_main, b0, len(lines), timeout_s=180))
    after = {(r["dimension"], r["value"]): r["tx_count"] for r in C.read_csv(settings.results_dir / "updated_stream_statistics.csv")}
    print("Показатели после повторной отправки не изменились:", before == after)
    C.show_rows(C.read_csv(settings.results_dir / "stream_batches.csv")[-3:], title="Последние микропакеты")
''')
code(r'''
# Корректная остановка потоков и резервная копия checkpoint/хранилища в Google Drive
J.stop_streams(spark)
print("Активные потоки:", [q.name for q in spark.streams.active])
backup = J.backup_work_dir(settings)
log.info("Потоки остановлены; рабочий каталог скопирован в %s", backup)
print("Резервная копия:", backup)
''')

md("## 9. Результаты в Google Drive")
code(r'''
for d in (settings.data_dir, settings.results_dir):
    print(f"\n{d}")
    for p in sorted(d.iterdir()):
        if p.is_file():
            print(f"  {p.name:42} {p.stat().st_size / 1024:10.1f} КБ")
print("\nПромежуточные данные:", sorted(p.name for p in settings.intermediate_dir.iterdir()))
''')

md("## 10. Самопроверка")
code(r'''
checks = []
def check(name, ok, detail=""):
    checks.append({"check": name, "result": "PASS" if ok else "FAIL", "detail": detail})

check("CSV содержит все обязательные поля", set(C.REQUIRED_FIELDS) <= set(raw.columns), ",".join(raw.columns))
check("масштаб данных ~500 000 строк", 400_000 <= n_rows <= 600_000, str(n_rows))
qmap = {r["check"]: r["rows"] for r in quality}
check("обнаружены пропуски", sum(v for k, v in qmap.items() if k.startswith("missing_")) > 0)
check("обнаружены дубликаты", qmap["full_duplicate_rows"] > 0, str(qmap["full_duplicate_rows"]))
check("очистка: баланс строк сходится", res["balance_ok"], f"{res['raw_rows']} -> {res['clean_rows']}")
check("SparkSession создана", spark.version.startswith("4."), spark.version)
city_sum = sum(r["tx_count"] for r in groups["city_statistics.csv"])
check("сумма по городам = числу очищенных строк", city_sum == res["clean_rows"], str(city_sum))
check("подключение к Kafka", topic_info["partitions"] >= 1, json.dumps(topic_info))
check("сообщения доставлены", delivery["failed"] == 0 and delivery["delivered"] == len(lines), str(delivery["delivered"]))
check("сообщения прочитаны из топика", len(msgs) == len(lines), str(len(msgs)))
check("Spark обработал новые транзакции", wait["complete"], f"{wait['found']}/{wait['expected']}")
check("итоги изменились на число новых принятых", total["tx_count_delta"] == expected_delta,
      f"{total['tx_count_delta']} (ожидалось {expected_delta})")
check("битый JSON отклонён", any(r["reject_reason"] == "invalid_json" for r in rej_summary))
check("сообщение без обязательного поля отклонено", any(r["reject_reason"] == "missing_client_id" for r in rej_summary))
check("повторы отклонены", any(r["reject_reason"].startswith("duplicate") for r in rej_summary))
check("результаты сохранены в Drive", all((settings.results_dir / f).exists() for f in [
    "data_quality_report.csv", "numeric_statistics.csv", "top_categories.csv", "city_statistics.csv",
    "initial_stream_statistics.csv", "updated_stream_statistics.csv", "analysis_comparison.csv", "execution_log.txt"]))
C.write_csv_atomic(settings.results_dir / "notebook_checks.csv", checks)
C.show_rows(checks, 30)
log.info("Самопроверка: %s PASS из %s", sum(c["result"] == "PASS" for c in checks), len(checks))
''')

md("""
## 11. Повторный запуск и остановка

* **Ещё одна порция данных:** измените `batch_tag` (например, `B002`) в разделе 2 и выполните ячейки 4 и 8.
* **Новая сессия Colab:** выполните все ячейки. Checkpoint и хранилище потока восстановятся из `lab3_intermediate/work_backup`, и Spark продолжит с сохранённых offset'ов.
* **Начать поток «с нуля»:** удалите `lab3_intermediate/work_backup` и `/content/lab3_work`. Тогда при `startingOffsets="earliest"` Spark заново прочитает топик (в пределах retention), а дубликаты по `transaction_id` не исказят итоги.
* `startingOffsets="latest"` при пустом checkpoint пропустит сообщения, отправленные до запуска потока.
* Остановка в любой момент: `J.stop_streams(spark)`, затем `spark.stop()`.
""")

nb = {"nbformat": 4, "nbformat_minor": 5,
      "metadata": {"colab": {"provenance": [], "toc_visible": True},
                   "kernelspec": {"name": "python3", "display_name": "Python 3"},
                   "language_info": {"name": "python"}},
      "cells": cells}
for i, c in enumerate(nb["cells"]):
    c["id"] = f"cell-{i:03d}"
out = ROOT / "lab3_bank_transactions.ipynb"
out.write_text(json.dumps(nb, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
print(f"written {out} ({len(cells)} cells)")

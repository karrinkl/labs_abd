# ЛР № 3 — потоковая обработка банковских транзакций: Apache Kafka (Aiven) + Spark Structured Streaming

Вариант № 2 — банковские транзакции.

| Что | Где выполняется |
|---|---|
| Основная работа: генерация, пакетная обработка PySpark, Spark Structured Streaming, сохранение результатов | **Google Colab** (блокнот `lab3_bank_transactions.ipynb`) |
| Брокер сообщений | **Aiven for Apache Kafka** (тариф Free), подключение по SSL |
| Данные, результаты, журнал, промежуточные данные | **Google Drive** (`MyDrive/lab3_bank/`) |
| Подготовка, локальные тесты, необязательная отправка с ноутбука, необязательный локальный стенд | **Arch Linux** |

> **Методичка не была приложена.** Проект выполнен по требованиям, перечисленным в задании. 15 контрольных вопросов в `control_questions.md` сформулированы по теме работы — **сверьте их с методичкой** и при необходимости замените формулировки.

---

## 1. Состав проекта

```text
lab3_kafka_streaming/
├── lab3_bank_transactions.ipynb   основной блокнот Colab (код модулей встроен, загружать один файл)
├── generate_data.py               генератор: raw (CSV ~500 000 строк) и new (JSON Lines для Kafka)
├── producer.py                    Kafka Producer (confluent-kafka): check / send / read; функции для блокнота
├── kafka_security.py              SSL/SASL: проверка PEM, keystore/truststore PKCS12, конфигурации клиентов
├── spark_jobs.py                  PySpark: пакетная обработка, Structured Streaming, foreachBatch, резервная копия
├── lab3_common.py                 настройки (Settings), справочники, журнал, атомарная запись CSV, сравнение
├── requirements.txt               локальные зависимости (confluent-kafka, pytest)
├── requirements-spark.txt         необязательно: PySpark 4.0.4 для локальных Spark-тестов (Python 3.13)
├── .env.example                   шаблон переменных для producer.py на ноутбуке (без секретов)
├── .gitignore                     ключи, сертификаты, .env, окружения, данные
├── pytest.ini, tests/             40 автотестов (генератор, SSL, продюсер, Spark, сквозной с Kafka)
├── local_kafka/                   необязательный локальный стенд Kafka с mTLS (Docker) для проверки без облака
├── tools/
│   ├── build_notebook.py          собирает блокнот из модулей .py (единый источник кода)
│   ├── run_notebook_local.py      выполняет весь блокнот локально против local_kafka
│   └── build_report.sh, …         сборка report.docx из report.md
├── examples/expected_output/      образцы результатов контрольного прогона на локальном стенде
├── report.md, report.docx, report.pdf   отчёт
└── control_questions.md           ответы на 15 контрольных вопросов
```

Почему модули, а не только блокнот: тот же код проверяется автотестами, а `producer.py` можно запустить с ноутбука. Блокнот собирается из модулей (`python tools/build_notebook.py`) и записывает их в Colab ячейками `%%writefile`, поэтому в Colab нужен только `.ipynb`.

## 2. Технический анализ и принятые решения

### 2.1 Версии (проверено)

| Компонент | Версия | Обоснование |
|---|---|---|
| Google Colab | Ubuntu 24.04, Python 3.13, **OpenJDK 21**, OpenSSL 3.0, **PySpark 4.0.4 предустановлен** | по публичному списку пакетов образа Colab (`googlecolab/backend-info`); блокнот проверяет фактические версии и Java ≥ 17 |
| PySpark / Spark | **4.0.4** (Scala 2.13) | предустановлен в Colab; поддерживает Python 3.9–3.13 и Java 17/21 |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4` | версия обязана совпадать с Spark, суффикс — с версией Scala; блокнот вычисляет её из `pyspark.__version__` |
| kafka-clients (JVM) | 3.9.1 | транзитивная зависимость коннектора; поддерживает PKCS12 и PEM-хранилища |
| Python Kafka-клиент | **confluent-kafka 2.16.0** (librdkafka 2.16) | готовые колёса для cp313 (Colab) и cp314 (Arch); читает PEM напрямую, отчёт о доставке с partition/offset |
| Aiven for Apache Kafka | тариф **Free** | до 5 топиков по **2 партиции**, 250 КиБ/с, фиксированные настройки (retention и др.), отключение после **24 ч** без активности, только через Console |

### 2.2 Типичные проблемы примеров и как они решены

| Проблема | Решение в проекте |
|---|---|
| Colab называют Dataproc | Colab — одна ВМ, Spark в режиме `local[*]`. Это **не** Dataproc и не распределённый кластер; в отчёте это так и описано |
| Устаревший коннектор `spark-sql-kafka-0-10_2.12:3.x` при PySpark 4 | несовпадение версий даёт `NoSuchMethodError` / `ClassNotFound`; координата вычисляется из версии PySpark |
| `spark.jars.packages` задаётся после создания сессии | пакет подключается только до старта JVM; `get_spark()` переиспользует сессию и сообщает, если её нужно перезапустить |
| Ручное создание JKS через `keytool` с паролями в командной строке | PKCS12 (формат по умолчанию в Java 9+); пароли случайные на сессию, передаются через переменные окружения; ключ не выводится |
| `outputMode("complete")` без агрегации | `complete` используется только для `groupBy`-агрегата; поток с `foreachBatch` — без агрегации (`append`) |
| `union` статического и потокового DataFrame как «история» | история хранится в Parquet; принятые потоковые транзакции дописываются в устойчивое хранилище; итоги **пересчитываются** после каждого микропакета |
| `startingOffsets="latest"` теряет сообщения, отправленные до запуска потока | `earliest` + дедупликация по `transaction_id`; после первого запуска позиция берётся из checkpoint |
| Удаление checkpoint при каждом запуске | checkpoint не удаляется; копия сохраняется в Drive и восстанавливается в новой сессии |
| Checkpoint Spark прямо в Google Drive (FUSE) | Spark пишет на локальный диск ВМ (`/content/lab3_work`); после остановки потоков каталог копируется в Drive |
| Заявления о гарантии exactly-once | не заявляются. Запись в хранилище идемпотентна по `batch_id` и `transaction_id`, итоговые показатели не задваиваются; журнал CSV и копия в Drive — «как минимум один раз» |

### 2.3 Архитектура и поток данных

```text
generate_data.py ──raw_data.csv──▶ Google Drive ──▶ PySpark batch: схема → качество → очистка → статистика
                                                          │                        └─▶ lab3_results/*.csv
                                                          └─▶ clean_history.parquet + агрегаты истории
new_transactions.jsonl ─▶ producer.py ─SSL─▶ Aiven Kafka topic bank-transactions-v2 (2 партиции)
                                                          │
                       Spark Structured Streaming (readStream, trigger 5 с, checkpoint)
                       ├─ accepted_stream: from_json по явной схеме → проверка → дедупликация →
                       │     stream_transactions/batch_id=N (Parquet) → пересчёт «история + поток»
                       │     → updated_stream_statistics.csv, stream_batches.csv, stream_rejected/
                       └─ stream_type_counts: groupBy(transaction_type) outputMode complete → memory
```

## 3. Подготовка на Arch Linux

Всё, кроме пунктов 3.6–3.8, необязательно для выполнения основной части в Colab. Но этот раздел позволяет проверить проект локально.

```bash
# 3.1 Python (в Arch — 3.14) и git
python --version
sudo pacman -S --needed python git

# 3.2 Виртуальное окружение и зависимости (confluent-kafka, pytest)
cd lab3_kafka_streaming
python -m venv .venv
source .venv/bin/activate            # fish: source .venv/bin/activate.fish
pip install -r requirements.txt

# 3.3 Проверка структуры проекта и сборки блокнота
ls; python tools/build_notebook.py   # пересобирает .ipynb из модулей

# 3.4 Генерация данных локально (необязательно: блокнот генерирует их сам в Colab)
python generate_data.py raw --rows 500000 --out data/raw_data.csv      # ~10 с, ~40 МБ
python generate_data.py new --rows 300 --out data/new_transactions.jsonl
python generate_data.py raw --rows 500000 --seed 7 --out data/raw_seed7.csv   # другой seed

# 3.5 Тесты без Spark и без облака (ожидается: 31 passed, 9 skipped)
pytest -q
```

Необязательно — Spark-тесты локально. Нужны Java 17+ и Python 3.13: PySpark 4.0.4 официально не поддерживает системный Python 3.14.

```bash
sudo pacman -S --needed jdk21-openjdk uv
archlinux-java status                 # активна java-21-openjdk? иначе:
sudo archlinux-java set java-21-openjdk
uv venv --python 3.13 .venv-spark && source .venv-spark/bin/activate
uv pip install -r requirements-spark.txt
pytest -q -m "not integration"        # ожидается: 38 passed, 2 deselected
```

Необязательно — локальный стенд Kafka с SSL (mTLS как у Aiven) и сквозной тест или полный прогон блокнота:

```bash
sudo pacman -S --needed docker docker-compose && sudo systemctl start docker.service
./local_kafka/gen_certs.sh                                    # тестовые CA и сертификаты (только для стенда)
docker compose -f local_kafka/docker-compose.yml up -d        # Kafka 3.9.1, SSL, порт 9093
LAB3_IT_BOOTSTRAP=localhost:9093 LAB3_IT_CERTS=local_kafka/certs pytest -q   # 40 passed
python tools/run_notebook_local.py                            # весь блокнот, результаты в local_run/
docker compose -f local_kafka/docker-compose.yml down
```

### 3.6 Отправка в Aiven с ноутбука (необязательно, вместо ячейки блокнота)

```bash
mkdir -p ~/.config/lab3-kafka && chmod 700 ~/.config/lab3-kafka
mv ~/Downloads/ca.pem ~/Downloads/service.cert ~/Downloads/service.key ~/.config/lab3-kafka/
chmod 600 ~/.config/lab3-kafka/*
cp .env.example .env && nano .env      # LAB3_BOOTSTRAP_SERVERS=<Service URI из Aiven>
python producer.py check               # брокеры и партиции топика
python producer.py send --file data/new_transactions.jsonl --report data/delivery.csv
python producer.py read --max 3        # сообщения с partition/offset/timestamp/headers
```

## 4. Настройка Aiven for Apache Kafka (веб-интерфейс)

1. **Регистрация и проект.** Откройте https://console.aiven.io → Sign up (почта или Google). При регистрации создаётся организация и проект; можно создать свой проект: **Projects → Create project**.
2. **Сервис Kafka.** В проекте: **Services → Create service → Aiven for Apache Kafka®**.
3. **Тариф и регион.** **Service tier → Free**. Провайдер облака выбирается автоматически, доступен только выбор группы регионов (берите ближайшую, например Europe). Задайте **Service name**, например `lab3-kafka`, проверьте **Service summary** → **Create service**. Банковская карта для Free не нужна. Если Free недоступен, не обходите ограничения: используйте пробный период Aiven или локальный стенд из раздела 3 и отметьте это в отчёте.
4. **Ожидание Running.** Статус сменится с *Rebuilding* на **Running** (обычно несколько минут). Бесплатный сервис **выключается после 24 часов без активности** — перед работой проверьте статус и при необходимости нажмите **Power on**.
5. **Топик.** **Topics** (в новом интерфейсе: **Manage stream → Topics**) → **Create topic** → **Topic name**: `bank-transactions-v2`. Включите **Enable advanced configurations** и посмотрите доступные значения:
   - **Partitions**: 2 (максимум на Free);
   - **Replication**: значение по умолчанию, предлагаемое сервисом;
   - **Retention** и **max.message.bytes** на Free фиксированы — оставьте по умолчанию.
   
   Запишите **фактические** значения в раздел 6 отчёта. Наши сообщения весят около 300 байт, что намного меньше любого лимита.
6. **Адрес подключения.** **Overview → Connection information**: **Host**, **Port**, **Service URI** (`host:port`). Это и есть Bootstrap Servers.
7. **Сертификаты.** **Overview → Quick connect → Python**, способ аутентификации **Client certificate**. Нажмите:
   - **Download CA certificate** → `ca.pem`;
   - **Download service certificate** → `service.cert`;
   - **Download service access key** → `service.key`.
   
   Те же файлы есть в **Connection information**. Если доступен только SASL, выберите его, скопируйте пользователя и пароль и задайте `auth_mode="sasl"` в блокноте. Порт для SASL другой — берите Service URI из вкладки SASL.
8. **Безопасная передача в Colab.** Секреты Colab: значок 🔑 **Secrets** слева → **Add new secret**:
   - `KAFKA_BOOTSTRAP_SERVERS` = Service URI;
   - `KAFKA_CA_PEM`, `KAFKA_SERVICE_CERT`, `KAFKA_SERVICE_KEY` = содержимое файлов целиком.
   
   Для каждого секрета включите **Notebook access**. Если вставка многострочного ключа неудобна, не создавайте три PEM-секрета: блокнот откроет окно **загрузки файлов**. Файлы кладутся в `/content/kafka_secrets` с правами 600 и **не попадают в Google Drive**.
9. **Проверка отправки и получения.** Это делает блокнот (разделы 7–8): `check_connection` (брокеры и партиции), отправка с отчётом о доставке, чтение обратно с метаданными.
10. **Проверка в Aiven UI.** Сервис → **Topics** → `bank-transactions-v2`:
    - вкладка **Messages** (в старом интерфейсе — значок «показать сообщения») → **Fetch messages**, формат **JSON**;
    - видны **partition**, **offset**, **timestamp**, **key**, **headers** (`source`, `schema_version`, `send_batch`, `sequence`) и **value** (payload);
    - для просмотра нужен **Apache Kafka REST API (Karapace)**: **Service settings → Advanced / Connection information → Apache Kafka REST → Enable** (на Free REST Proxy входит в тариф);
    - общее число сообщений видно на вкладке **Topics** и в **Metrics**.
    
    Если просмотр сообщений недоступен на вашем тарифе, доказательством служат вывод ячейки «Независимая проверка доставки» и файл `lab3_results/kafka_messages_sample.csv` с теми же метаданными.

## 5. Запуск в Google Colab

1. https://colab.research.google.com → **File → Upload notebook** → `lab3_bank_transactions.ipynb`.
2. **Runtime → Change runtime type → CPU** (GPU не нужен).
3. Настройте секреты (раздел 4, п. 8).
4. **Runtime → Run all**. При монтировании Drive разрешите доступ (окно Google). При запросе загрузите `ca.pem`, `service.cert`, `service.key`.
5. Ход выполнения (время ориентировочное):

| Раздел | Что происходит | Результат в Drive |
|---|---|---|
| 0–2 | проверка Java/PySpark, установка confluent-kafka, запись модулей, настройки | — |
| 3 | монтирование Drive, журнал | `lab3_results/execution_log.txt` |
| 4 | генерация ~505 000 строк (~15 с) и 310 сообщений | `lab3_data/raw_data.csv`, `new_transactions.jsonl/.csv` |
| 5 | SparkSession + загрузка коннектора Kafka из Maven (~30 с) | `environment.json` |
| 6 | схема, качество, очистка, статистика, группировки (~1–2 мин) | `data_quality_report.csv`, `cleaning_summary.csv`, `numeric_statistics.csv`, `top_categories.csv`, `city_statistics.csv`, … |
| 7 | сертификаты, keystore/truststore, проверка подключения | — (секреты только на ВМ) |
| 8 | потоки, отправка, проверка доставки, ожидание микропакетов, сравнение, повторная отправка, остановка | `initial_/updated_stream_statistics.csv`, `analysis_comparison.csv`, `stream_batches.csv`, `kafka_*.csv`, `lab3_intermediate/` |
| 9–10 | список файлов, самопроверка (16 пунктов) | `notebook_checks.csv` |

Повторный **Run all** безопасен:
- генерация пропускается;
- SparkSession переиспользуется;
- потоки перед запуском останавливаются;
- checkpoint восстанавливается из Drive;
- повторно отправленные сообщения отклоняются как дубликаты.

Чтобы отправить **новую** порцию, поменяйте `batch_tag="B002"` в разделе 2.

## 6. Сценарий демонстрации и скриншоты (делаете сами)

| № | Действие | Скриншот |
|---|---|---|
| 1 | Aiven Console → сервис → **Overview**: статус Running, тариф, регион | `01_aiven_service_running.png` |
| 2 | **Topics → bank-transactions-v2** → параметры (partitions, replication, retention, max message size) | `02_topic_config.png` |
| 3 | Colab, раздел 6: вывод схемы, числа строк и отчёта качества | `03_batch_processing.png` |
| 4 | Раздел 6: статистика, топ-5, города | `04_initial_results.png` |
| 5 | Раздел 8: запуск потоков и статус `Waiting for next trigger` | `05_stream_started.png` |
| 6 | Раздел 8: отчёт о доставке (sent/delivered, partitions/offsets) | `06_messages_sent.png` |
| 7 | Aiven → топик → **Messages → Fetch messages**: список | `07_aiven_messages.png` |
| 8 | Раскрыть одно сообщение: payload, partition, offset, timestamp, headers | `08_aiven_payload_metadata.png` |
| 9 | Раздел 8: журнал микропакетов, memory sink, таблица изменений | `09_updated_results.png` |
| 10 | Google Drive → `MyDrive/lab3_bank/lab3_results` со списком файлов | `10_drive_results.png` |

Сохраните снимки в `screenshots/` рядом с отчётом. `./tools/build_report.sh` вставит их в DOCX вместо жёлтых заглушек.

Демонстрация считается успешной, только если одновременно выполнено следующее:
- продюсер получил подтверждения;
- сообщения прочитаны из топика;
- Spark обработал 300 новых транзакций (журнал `stream_batches.csv`);
- `total` в `analysis_comparison.csv` вырос на 300.

## 7. Где лежат файлы

| Место | Файлы |
|---|---|
| Ноутбук (Arch) | репозиторий проекта; `~/.config/lab3-kafka/` — сертификаты для `producer.py` (вне репозитория); `.env` |
| Colab (ВМ, временно) | `/content/lab3_code` — модули; `/content/kafka_secrets` — PEM и PKCS12 (600); `/content/lab3_work` — checkpoint и хранилище потока для Spark |
| Google Drive `MyDrive/lab3_bank/` | `lab3_data/` (raw_data.csv, new_transactions.*), `lab3_results/` (CSV-результаты, execution_log.txt, environment.json), `lab3_intermediate/` (clean_history.parquet, work_backup с checkpoint) |

Spark пишет Parquet в каталоги из множества файлов `part-*.parquet`. Итоговые таблицы небольшие, поэтому их собирают на драйвер и записывают одним CSV атомарно (временный файл, затем `os.replace`). Перезаписывать весь набор данных на каждом микропакете нельзя: это дорого и неидемпотентно. Поэтому каждый микропакет пишет только свой каталог `batch_id=N`.

## 8. Тесты и их фактические результаты

| Набор | Что проверяет | Где выполнялся | Результат |
|---|---|---|---|
| `test_generate_data.py` (6) | поля, масштаб, seed, пропуски/дубликаты/ошибки, пакет для Kafka, пустой пакет | среда подготовки (Python 3.13) | пройдено |
| `test_common_and_security.py` (14) | атомарная запись, сравнение, журнал, формат bootstrap, отсутствие секретов в repr/логах, PEM (свой/чужой ключ, не-PEM, отсутствует), PKCS12, восстановление PEM из одной строки | там же | пройдено |
| `test_producer.py` (11) | ключ/заголовки/offset'ы (заглушка вместо брокера), пустой пакет, переполнение буфера, ошибки доставки, повторное сообщение, **недоступный брокер**, CLI | там же | пройдено |
| `test_spark.py` (7) | схема, качество, баланс очистки, агрегаты; микропакет: битый JSON, нет поля, неверные значения, повтор в пакете и между пакетами, повтор того же `batch_id`, пустой пакет | Spark 4.0.4, Java 21 | пройдено |
| `test_integration_kafka.py` (2) | SSL-отправка и чтение; **Structured Streaming end-to-end** через keystore/truststore PKCS12 | локальный Kafka 3.9.1 с mTLS (Docker) | пройдено |
| Блокнот целиком (`tools/run_notebook_local.py`) | все разделы, самопроверка 16 пунктов; повторный Run all; восстановление checkpoint из «Drive» | локальный стенд | 16/16 PASS во всех трёх прогонах |
| Минимальное окружение (только `requirements.txt`) | всё, кроме Spark | Python 3.13, без PySpark | 31 passed, 9 skipped (с понятной причиной) |

В ходе проверки найдены и исправлены два плавающих сбоя:
- чтение хранилища потока во время записи микропакета (`UNABLE_TO_INFER_SCHEMA`): теперь учитываются только каталоги с маркером `_SUCCESS`;
- отправка в только что созданный топик до выбора лидера партиций (в тестовой фикстуре).

После исправлений полный набор прошёл 4 раза подряд: 40/40.

**Требуют реального Aiven и Colab** (выполняет студент): подключение к Aiven по SSL, просмотр сообщений в Aiven UI, монтирование Google Drive, Colab Secrets/загрузка файлов, загрузка коннектора из Maven в Colab. Код этих шагов проверен на эквивалентном локальном стенде, но с реальным Aiven и Colab в среде подготовки не запускался.

## 9. Типичные ошибки

| Симптом | Причина и исправление |
|---|---|
| `Kafka недоступна (_TRANSPORT)` / таймаут | сервис Free выключен после простоя → **Power on** в Aiven; неверный Service URI/порт (для SASL порт другой) |
| `SSL handshake failed`, `bad certificate` | не те файлы или файлы от другого сервиса — скачайте заново из Quick connect; режим `auth_mode` не совпадает с выбранным способом |
| `service.key не соответствует service.cert` | ключ и сертификат от разных загрузок — скачайте оба заново |
| `Топик ... не найден` | создайте `bank-transactions-v2` в Aiven (Topics → Create topic); автосоздание отключено |
| `Активная SparkSession создана без коннектора Kafka` | **Runtime → Restart session** и Run all |
| `Failed to find data source: kafka` / ошибки Ivy | нет доступа к Maven Central или неверная версия пакета; проверьте вывод раздела 5 |
| `NoSuchMethodError` в kafka | версия коннектора не совпадает со Spark; не задавайте `LAB3_KAFKA_PACKAGE` вручную |
| `Spark обработал новые транзакции: FAIL` | поток упал (см. исключение в выводе) или пакет уже обрабатывался — смените `batch_tag` |
| `Current batch is falling behind` | предупреждение: обработка микропакета (~7–9 с на CPU Colab) дольше триггера 5 с; на результат не влияет |
| Сообщения не видны в Aiven UI | включите Apache Kafka REST API (Karapace); иначе используйте `kafka_messages_sample.csv` |
| `java: command not found` (Arch) | `sudo pacman -S jdk21-openjdk` и `sudo archlinux-java set java-21-openjdk` |
| `pyspark` не ставится в `.venv` на Python 3.14 | используйте `uv venv --python 3.13 .venv-spark` |

## 10. Чек-лист сдачи

- [ ] Сервис Aiven Running, топик `bank-transactions-v2` создан, параметры записаны в отчёт (раздел 6).
- [ ] Блокнот выполнен в Colab полностью, самопроверка — 16/16 PASS.
- [ ] В Drive есть `lab3_data/` и `lab3_results/` со всеми CSV и `execution_log.txt`.
- [ ] Сделаны 10 скриншотов, отчёт пересобран (`./tools/build_report.sh`) или скриншоты вставлены вручную.
- [ ] В отчёте числа заменены на результаты вашего запуска (места помечены `[ЗАПОЛНИТЬ]`), заполнен титульный лист.
- [ ] Контрольные вопросы сверены с методичкой.
- [ ] Сертификаты и `.env` не попали в git, Drive и отчёт.

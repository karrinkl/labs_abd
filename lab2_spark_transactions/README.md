# ЛР № 2 «Распределённые вычисления в Apache Spark» — вариант № 2 (банковские транзакции)

Проект генерирует синтетические банковские транзакции, проверяет и очищает их в PySpark, выполняет фильтрацию `amount_eur > x`, группировку по `city` со средней суммой и количеством, сортировку и Spark SQL-запрос «города с самой высокой средней суммой транзакции». Он также сравнивает время выполнения в режимах `local[1]` и Spark Standalone (Docker: Master + Worker).

**Основной способ запуска — Docker Compose.** Код и `spark-submit` выполняются в контейнере `spark-client` того же образа, что Master и Worker. Поэтому на хосте не нужны ни Java, ни PySpark, а версии драйвера и executors гарантированно совпадают.

## Версии компонентов (зафиксированы в `.env.example` и `docker-compose.yml`)

| Компонент | Версия | Где задано / как проверено |
|---|---|---|
| Apache Spark | 4.1.3 (Scala 2.13) | образ `spark:4.1.3-scala2.13-java21-python3-ubuntu` (Docker Official Image) |
| Java | 21 (Eclipse Temurin 21.0.12) | внутри образа; Spark 4.1 поддерживает Java 17/21 |
| Python (драйвер и executors) | 3.10.12 | внутри образа; PySpark 4.1 требует Python ≥ 3.10 |
| PySpark на хосте (необязательно) | 4.1.3 | `requirements.txt`; поддерживает Python 3.10–3.14 (в Arch сейчас 3.14) |
| Docker Engine / Compose | пакеты Arch `docker`, `docker-compose` | проверено с Docker 29 и Compose v5 |

Ветка 4.1 выбрана как стабильная, с патч-релизами; для перехода на 4.2.x достаточно поменять `SPARK_IMAGE` в `.env` (и `pyspark==` в `requirements.txt`).

## Структура

| Файл / каталог | Назначение |
|---|---|
| `generate_data.py` | генератор синтетических транзакций (только stdlib): размер, seed, доли пропусков, дубликатов и ошибок задаются параметрами |
| `transactions_pipeline.py` | общая логика: явная схема, нормализация, проверки, очистка, `city_stats()`, операция для замера, REST API Spark UI |
| `spark_job.py` | основное приложение PySpark: качество данных → очистка → фильтр → группировка/сортировка → Spark SQL → запись результатов |
| `benchmark.py` | сравнение `local[1]` и кластера: отдельный процесс на режим, прогрев + N повторов, медиана, сверка результатов, метрики CPU/памяти |
| `spark_modes.py`, `submit.py` | единое описание режимов (`local1`, `local_all`, `cluster`, `cluster_1core`) и запуск `spark-submit` с их ресурсами |
| `check_cluster.py` | состояние Master: зарегистрированные Worker, ядра, память, приложения (JSON API Master) |
| `scripts/docker_stats.sh` | необязательный сбор `docker stats` (CPU/память контейнеров) во время замера |
| `docker-compose.yml`, `.env.example` | кластер: `spark-master`, `spark-worker`, необязательный `spark-worker-2`, `spark-client` |
| `requirements.txt` | необязательные зависимости хоста (PySpark для IDE, python-docx для сборки отчёта) |
| `report.md`, `report.docx` | отчёт; `tools/` — сборка DOCX |
| `control_questions.md` | ответы на 15 контрольных вопросов |
| `data/` | сгенерированные CSV, очищенный Parquet, отклонённые записи (в git не хранятся) |
| `results/` | ваши результаты: `results/<набор>/`, `results/benchmark/`, `results/logs/`, `results/screenshots/` |
| `results/reference_run/` | контрольный прогон, выполненный при подготовке проекта (см. его `README.md`) — **не ваши результаты** |

## 0. Что где выполнять

- **«Хост»** — ваш терминал в Arch Linux, в каталоге проекта `lab2_spark_transactions/`.
- **«Контейнер»** — команды вида `docker compose exec spark-client …`; их тоже запускаете на хосте, а выполняются они внутри `spark-client` (рабочий каталог `/opt/project` = каталог проекта).
- Команды с `sudo` меняют систему (установка пакетов, запуск службы). Остальное выполняется от обычного пользователя.

---

## 1. Проверка и установка инструментов (хост)

```bash
docker --version; docker compose version     # есть ли Docker и плагин Compose
python --version                              # Python для генератора/скриптов на хосте (необязательно)
nproc; free -h                                # сколько ядер и памяти (нужно ≥ 4 ядер и ≥ 8 ГБ)
```

Если Docker отсутствует:

```bash
sudo pacman -S --needed docker docker-compose     # Docker Engine + плагин docker compose
sudo systemctl start docker.service               # запустить демон сейчас
# sudo systemctl enable docker.service            # (по желанию) автозапуск при загрузке
systemctl status docker.service --no-pager        # ожидается: active (running)
sudo docker run --rm hello-world                  # проверка: "Hello from Docker!"
```

**Права на Docker.** Либо добавляйте `sudo` перед каждой командой `docker`, либо добавьте себя в группу `docker`:

```bash
sudo usermod -aG docker "$USER"    # затем выйти из сеанса и войти снова (или перезагрузиться)
```

ArchWiki предупреждает, что участник группы `docker` фактически получает права root. Решайте сами; ниже команды приведены без `sudo`. Если вы выбрали `sudo`, добавляйте его перед `docker`.

Типичные ошибки:
- `Cannot connect to the Docker daemon` — служба не запущена: `sudo systemctl start docker.service`.
- `permission denied … docker.sock` — нет прав: используйте `sudo` или группу `docker` (после повторного входа).
- `docker: 'compose' is not a docker command` — не установлен пакет `docker-compose`.

Java и PySpark на хосте **не нужны**. Если хотите запускать PySpark прямо на хосте (необязательно): `sudo pacman -S --needed jdk21-openjdk` и проверка `java -version`.

## 2. Виртуальное окружение Python (хост, необязательно)

Нужно только для автодополнения PySpark в IDE и для сборки `report.docx` из `report.md`.

```bash
python -m venv .venv
source .venv/bin/activate          # fish: source .venv/bin/activate.fish
```

Ожидается приглашение с префиксом `(.venv)`. Если `python -m venv` выдаёт ошибку, установите Python: `sudo pacman -S --needed python`.

## 3. Установка зависимостей (хост, необязательно)

```bash
pip install -r requirements.txt
python -c "import pyspark, docx; print(pyspark.__version__)"    # ожидается 4.1.3
```

Ошибка `externally-managed-environment` означает, что окружение не активировано (шаг 2) — не устанавливайте пакеты в системный Python.

## 4. Подготовка конфигурации и запуск кластера (хост)

```bash
cp .env.example .env
sed -i "s/^HOST_UID=.*/HOST_UID=$(id -u)/; s/^HOST_GID=.*/HOST_GID=$(id -g)/" .env
mkdir -p data results/logs results/screenshots
docker compose pull                 # скачать образ (~0,8 ГБ, один раз)
docker compose up -d                # Master, Worker и клиент в фоне
docker compose ps                   # все три сервиса Up, spark-master (healthy)
```

`HOST_UID/HOST_GID` нужны, чтобы контейнеры работали от вашего пользователя. Тогда файлы в `data/` и `results/` принадлежат вам, и их можно удалить без `sudo`.

Типичные ошибки:
- `toomanyrequests` / `429 Too Many Requests` при `pull` — лимит анонимных загрузок Docker Hub. Выполните `docker login` или в `.env` укажите зеркало того же официального образа: `SPARK_IMAGE=mirror.gcr.io/library/spark:4.1.3-scala2.13-java21-python3-ubuntu`.
- `Bind for 0.0.0.0:8080 failed: port is already allocated` — порт занят другой программой. Найдите её: `ss -ltnp | grep 8080`. Либо замените левую часть в `docker-compose.yml` (например, `"18080:8080"`) и открывайте UI на новом порту.
- Ресурсы: по умолчанию Worker отдаёт 2 ядра и 2 ГБ, executor использует 2 ядра и 1 ГБ. На слабом ПК уменьшите `SPARK_WORKER_*`/`SPARK_EXECUTOR_*` в `.env` и выполните `docker compose up -d` повторно.

## 5. Проверка Master и Worker (хост)

Статус `Up` у контейнеров ещё не значит, что Worker зарегистрировался у Master. Проверьте одним из способов:

```bash
docker compose logs spark-worker | grep "Successfully registered with master"
docker compose exec spark-client python3 check_cluster.py --save results/cluster_state.json
curl -s http://localhost:8080/json/ | python -m json.tool | head -30     # тот же JSON API с хоста
```

Ожидается: `Master status: ALIVE`, `Workers alive: 1`, строка Worker с `state=ALIVE cores=2 memory=2048 MiB`.

В браузере: **http://localhost:8080** — Spark Master UI. Раздел **Workers** — Worker в состоянии ALIVE, столбцы Cores и Memory показывают доступные ресурсы. Ниже — **Running/Completed Applications**. Worker UI — **http://localhost:8081**.

Если Worker не появился, посмотрите `docker compose logs spark-worker` (ошибки подключения к `spark-master:7077`) и проверьте, что `spark-master` в состоянии `healthy`.

## 6. Генерация тестового датасета (контейнер)

```bash
docker compose exec spark-client python3 generate_data.py --rows 10000 --name transactions_test
```

Ожидается: файлы `data/transactions_test.csv` (~0,9 МБ, 10 100 строк) и `data/transactions_test.stats.json`. В выводе — число добавленных дубликатов, пропусков по полям и некорректных значений по видам. Повторный запуск с тем же `--seed` (по умолчанию 42) даёт тот же файл: проверить можно через `sha256sum data/transactions_test.csv`.

## 7. Генерация основного датасета (контейнер)

```bash
docker compose exec spark-client python3 generate_data.py --rows 2000000 --name transactions
docker compose exec spark-client python3 generate_data.py --rows 500000  --name transactions_medium   # для анализа влияния размера
```

Основной набор — 2 020 000 строк, ~171 МБ, генерация ~45 с. При достаточных ресурсах объём можно увеличить: `--rows 10000000` даст ~0,9 ГБ.

Параметры: `--missing-rate`, `--invalid-rate`, `--fixable-rate`, `--duplicate-rate`, `--seed`, `--start`, `--end`. Справка — `python3 generate_data.py --help`.

## 8. Запуск приложения в распределённом режиме (контейнер)

```bash
docker compose exec spark-client bash -c \
  "python3 submit.py cluster spark_job.py --data data/transactions.csv 2>&1 | tee results/logs/spark_job_cluster.log"
```

`submit.py` печатает фактическую команду `spark-submit` (`--master spark://spark-master:7077 --executor-memory 1g --executor-cores 2 …`).

Пока задание выполняется (около 3 минут на основном наборе):
- **http://localhost:8080** — в Running Applications появилось приложение `lab2-transactions-cluster`;
- **http://localhost:4040** — Spark UI приложения: вкладки Jobs, Stages, SQL / DataFrame, Executors.

В конце вывода раздел «9. Executors приложения» перечисляет executor `driver` и executor `0` на Worker (`cores=2`). Те же данные сохраняются в `results/transactions/app_info_cluster.json`.

Ожидаемые результаты в `results/transactions/`:
- `quality_report.json` — отчёт о качестве данных и очистке;
- `filter_summary.json`, `filtered_sample.csv` — результат фильтрации;
- `city_stats.csv` — группировка и сортировка; `city_stats_over_threshold.csv` — дополнительно, по отфильтрованным данным;
- `top_cities.sql`, `top_cities_sql.csv` — Spark SQL;
- `plan_city_stats.txt` — физический план;
- `rejected_sample.csv` — примеры отклонённых записей.

Полные очищенные и отклонённые данные записываются executors в `data/clean_transactions.parquet` и `data/rejected_transactions/`.

Ошибки:
- `Initial job has not accepted any resources` — Worker не зарегистрирован или ресурсов меньше, чем запрошено (`SPARK_EXECUTOR_*` больше `SPARK_WORKER_*`). Выполните шаг 5.
- `Permission denied` при записи — неверные `HOST_UID/HOST_GID` в `.env` или каталоги созданы через `sudo`. Исправьте `.env`, выполните `docker compose up -d` и `sudo chown -R $(id -u):$(id -g) data results`.

## 9. Запуск в локальном режиме `local[1]` (контейнер)

```bash
docker compose exec spark-client bash -c \
  "python3 submit.py local1 spark_job.py --data data/transactions.csv --no-write 2>&1 | tee results/logs/spark_job_local1.log"
```

В `local[1]` драйвер и единственный поток задач находятся в одной JVM. В разделе «Executors» будет только `driver` с `cores=1`. Результаты `city_stats.csv` и `top_cities_sql.csv` должны совпасть с кластерным запуском:

```bash
cp results/transactions/city_stats.csv /tmp/city_local1.csv     # после local1
# … затем снова запуск cluster (шаг 8) и:
diff /tmp/city_local1.csv results/transactions/city_stats.csv && echo "совпадает"
```

`local_all` (`local[*]`) — многопоточный локальный режим. Его можно запускать для справки, но однопоточным он не является.

## 10. Сравнение производительности (контейнер)

```bash
# необязательно, во втором терминале на хосте — CPU/память контейнеров:
./scripts/docker_stats.sh results/benchmark/docker_stats.csv 2

docker compose exec spark-client bash -c \
  "python3 benchmark.py --data data/transactions_test.csv data/transactions_medium.csv data/transactions.csv \
   --modes local1 cluster cluster_1core --repeats 5 2>&1 | tee results/logs/benchmark.log"
```

Длительность — около 20–30 минут для трёх наборов (зависит от ПК). Для быстрой проверки: `--data data/transactions_test.csv --repeats 2`.

В конце печатается таблица, а результаты сохраняются в `results/benchmark/`:
- `summary.md` — готовые таблицы для отчёта;
- `summary.csv` — медиана, среднее, min/max, стандартное отклонение, партиции, executors, speedup, метрики задач;
- `runs.csv` — каждый повтор;
- `raw.json` — всё, включая конфигурацию и списки executors.

Строка `результаты всех режимов совпадают: True` обязательна. При `False` скрипт завершится с ошибкой, и сравнивать время нельзя.

Во время замера не запускайте других тяжёлых программ — они искажают время. Режим `cluster_1core` (кластер, ограниченный одним ядром) показывает накладные расходы распределённого режима без выигрыша от параллелизма.

## 11. Сохранение результатов (хост)

Всё, что нужно для отчёта, уже лежит в `results/`. Чтобы сохранить копию вне проекта:

```bash
tar czf ~/lab2_results_$(date +%F).tar.gz results/
```

## 12. Spark UI — где что смотреть

| UI | Адрес на хосте | Порт в контейнере | Что там |
|---|---|---|---|
| Spark Master | http://localhost:8080 | spark-master:8080 | Workers (ALIVE, Cores, Memory), Running/Completed Applications |
| Spark Worker | http://localhost:8081 | spark-worker:8081 | executors, запущенные на этом Worker |
| Spark UI приложения (драйвер) | http://localhost:4040 | spark-client:4040 | Jobs, Stages, Storage, Environment, **Executors**, SQL / DataFrame |
| REST API приложения | http://localhost:4040/api/v1/applications | — | JSON; список executors: `…/applications/<appId>/executors` |
| JSON API Master | http://localhost:8080/json/ | — | Worker, ядра, память, приложения |

Spark UI приложения доступен, **только пока приложение работает**. Для неторопливого изучения запустите интерактивную сессию:

```bash
docker compose exec spark-client bash -c "python3 submit.py cluster spark_job.py --data data/transactions.csv" &
# или интерактивно:
docker compose exec spark-client pyspark --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-client --conf spark.driver.bindAddress=0.0.0.0
```

Ссылки на Worker и executors внутри Master UI ведут на внутренние адреса Docker-сети (`172.x.x.x`). С хоста открывайте Worker через http://localhost:8081.

## 13. Скриншоты для отчёта (делаете сами)

Сохраняйте в `results/screenshots/` под указанными именами — так они автоматически подставятся в отчёт при сборке (`tools/build_report.sh`).

| Файл | Что открыть | Что должно быть видно |
|---|---|---|
| `01_docker_ps.png` | терминал: `docker compose ps` | три контейнера `Up`, `spark-master (healthy)`, порты |
| `02_master_ui_workers.png` | http://localhost:8080 | Status ALIVE, Alive Workers: 1, строка Worker ALIVE, Cores 2, Memory 2.0 GiB |
| `03_master_ui_running_app.png` | http://localhost:8080 во время шага 8 | Running Applications: `lab2-transactions-cluster`, Cores, Memory per Executor |
| `04_app_ui_executors.png` | http://localhost:4040 → Executors, во время шага 8 | executor `driver` и executor `0` (адрес Worker), Cores 2, Status Active, Tasks |
| `05_app_ui_jobs_dag.png` | http://localhost:4040 → Jobs → любой job → DAG Visualization | этапы, разделённые shuffle |
| `06_dataframe_results.png` | терминал с выводом шага 8, разделы 5–6 | фильтр (до/после, порог) и таблица городов |
| `07_sql_result.png` | терминал, раздел 7 | SQL-запрос и топ-5 городов, «совпадает с DataFrame API: True» |
| `08_benchmark_table.png` | терминал после шага 10 или `results/benchmark/summary.md` | таблица времени и speedup |

## 14. Подготовка отчёта (хост)

1. Откройте `report.md`: разделы с результатами заполнены данными **контрольного прогона** из `results/reference_run/`. Замените их своими значениями из `results/` — места помечены `[ЗАПОЛНИТЬ …]`, а таблицы можно скопировать из `results/benchmark/summary.md`.
2. Заполните титульный лист (поля в квадратных скобках).
3. Положите скриншоты в `results/screenshots/` (шаг 13).
4. Соберите DOCX:

```bash
sudo pacman -S --needed pandoc-cli          # конвертер Markdown -> DOCX
source .venv/bin/activate                   # python-docx из requirements.txt
./tools/build_report.sh                     # -> report.docx
```

Если pandoc ставить не хочется, отредактируйте готовый `report.docx` в LibreOffice Writer (`sudo pacman -S libreoffice-fresh`).

## 15. Остановка кластера и очистка (хост)

```bash
docker compose down                         # остановить и удалить контейнеры и сеть (данные в data/ и results/ остаются)
docker compose --profile two-workers down   # если запускали второй Worker
```

Удаление сгенерированных данных проекта (только внутри каталога проекта; результаты отчёта в `results/` остаются):

```bash
rm -rf data/*.csv data/*.stats.json data/clean_*.parquet data/rejected_*
```

Освободить место, удалив образ Spark (~2 ГБ на диске), если он больше не нужен:

```bash
docker image rm spark:4.1.3-scala2.13-java21-python3-ubuntu
```

Остановить службу Docker: `sudo systemctl stop docker.service docker.socket`.

---

## Дополнительно: второй Worker

```bash
docker compose --profile two-workers up -d          # spark-worker-2, UI http://localhost:8082
docker compose exec spark-client python3 check_cluster.py    # Workers alive: 2
# чтобы приложение использовало оба Worker, увеличьте общий лимит ядер:
sed -i 's/^SPARK_TOTAL_EXECUTOR_CORES=.*/SPARK_TOTAL_EXECUTOR_CORES=4/' .env && docker compose up -d spark-client
```

Оба Worker работают на **одном** физическом компьютере, поэтому делят его ядра. Ускорение по сравнению с одним Worker возможно, только если у ПК есть свободные ядра.

## Критерии готовности (самопроверка)

- [ ] В CSV есть все 8 обязательных полей и `channel`; генератор вносит пропуски, полные и частичные дубликаты, некорректные значения (`*.stats.json`).
- [ ] `spark_job.py` выводит версию Spark, схему, первые строки, число строк, отчёт о качестве и баланс очистки (`balance_check.ok = true`).
- [ ] Фильтр `amount_eur > x` (x = `AMOUNT_THRESHOLD`), группировка по `city` (среднее и количество), сортировка по убыванию среднего.
- [ ] Spark SQL возвращает топ-5 городов и совпадает с DataFrame API.
- [ ] Master UI показывает Worker ALIVE; в Spark UI видны executors на Worker.
- [ ] `benchmark.py` выполнен для `local1` и `cluster`, результаты совпадают, есть таблица времени и speedup.
- [ ] Отчёт заполнен вашими результатами и скриншотами; титульный лист заполнен.

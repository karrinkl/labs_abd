# Контрольный прогон (выполнен при подготовке проекта)

Это **реальные** результаты запуска кода проекта, но получены они **не на вашем компьютере**. Они нужны, чтобы показать, что проект работает, и как образец заполнения отчёта. В отчёт по лабораторной вставляйте результаты своего запуска — каталог `results/` вне `reference_run/`.

## Среда выполнения

| Параметр | Значение |
|---|---|
| Дата | 19.10.2026 |
| Хост | облачный Linux-контейнер (ядро 6.18), 4 vCPU Intel Xeon @ 2,80 ГГц, 15,7 ГиБ ОЗУ |
| Docker | Engine 29.8.2, Compose v5.6.0 (демон запущен внутри среды подготовки) |
| Образ | `spark:4.1.3-scala2.13-java21-python3-ubuntu`; из-за лимита Docker Hub (HTTP 429) скачан через зеркало `mirror.gcr.io/library/spark` и помечен исходным тегом |
| Кластер | `docker-compose.yml` без изменений, `.env` из `.env.example` (HOST_UID=1000): 1 Master, 1 Worker (2 ядра, 2 ГБ), executor 2 ядра / 1 ГБ, driver 1 ГБ |
| Данные | `generate_data.py` с seed 42: `transactions_test` (10 000), `transactions_medium` (500 000), `transactions` (2 000 000 базовых строк) |

Хост — это не Arch Linux, а виртуальная машина с другими CPU и диском. Абсолютные времена на вашем ПК будут другими; соотношения режимов тоже могут отличаться.

## Содержимое

| Файл | Откуда |
|---|---|
| `cluster_state.json` | `check_cluster.py --save` после запуска кластера |
| `transactions/` | `python3 submit.py cluster spark_job.py --data data/transactions.csv` (+ `app_info_local1.json` из запуска `local1`) |
| `transactions_test/` | тот же скрипт на тестовом наборе |
| `benchmark/` | `benchmark.py --data data/transactions_test.csv data/transactions_medium.csv data/transactions.csv --modes local1 local_all cluster cluster_1core --repeats 5` |
| `logs/` | полный вывод запусков (строки уровня INFO удалены) |
| `data_stats/` | статистика генератора (`*.stats.json`) |

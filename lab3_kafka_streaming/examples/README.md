# Образцы выходных данных

`expected_output/` — **реальные** результаты выполнения `lab3_bank_transactions.ipynb` целиком (`tools/run_notebook_local.py`) в среде подготовки проекта. Это не Colab и не Aiven:
- вместо Google Drive — локальный каталог (в путях заменён на `<BASE_DIR>`);
- вместо Aiven — локальный брокер Kafka 3.9.1 с SSL и обязательной клиентской аутентификацией (`local_kafka/`);
- Spark 4.0.4, Java 21, Python 3.13, seed 42.

Генерация и очистка детерминированы. Поэтому при тех же параметрах (seed 42, 500 000 строк, пакет `B001`) пакетные результаты и итоги «до/после» в Colab должны совпасть. Различаться будут время, offset'ы и партиции сообщений, а также `environment.json`.

| Файл | Содержание |
|---|---|
| `lab3_data/raw_data_head.csv`, `new_transactions_head.jsonl` | первые строки данных (полные файлы создаёт генератор) |
| `lab3_data/*.stats.json` | статистика генерации |
| `lab3_results/data_quality_report.csv` … `transaction_type_statistics.csv` | пакетная обработка |
| `initial_stream_statistics.csv`, `updated_stream_statistics.csv`, `analysis_comparison.csv` | показатели до и после потока, таблица изменений |
| `stream_batches.csv`, `stream_rejected_summary.csv` | журнал микропакетов и причины отклонения |
| `kafka_delivery_report.csv`, `kafka_messages_sample.csv` | первые 20 и 10 строк отчёта о доставке и прочитанных сообщений |
| `city_currency_type_statistics.csv` | первые 30 строк |
| `execution_log.txt`, `notebook_checks.csv` | журнал и самопроверка (16/16 PASS) |

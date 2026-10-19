# Источники и использованные технические материалы

Дата обращения ко всем электронным ресурсам: **19.10.2026**. Доступность каждого URL проверена в эту дату (HTTP 200).
Единственное исключение — github.com: из среды подготовки он был недоступен (ответ 403 от сетевого прокси), поэтому ссылку на C4-PlantUML (п. 25) стоит один раз открыть вручную.
Номера совпадают с разделом 14 отчёта.

## 1. Основной источник (методичка)

| № | Источник | Что взято |
|---|---|---|
| 1 | Методические указания к лабораторной работе № 1 «Выбор и обоснование архитектуры системы хранения и обработки данных для бизнес-задачи». **[Выходные данные — заполнить: автор(ы), учебное заведение, год]** | Задания 1–9, структура отчёта, вариант № 2, контрольные вопросы, требования к диаграммам C4 |

## 2. Дополнительные материалы по архитектурам данных

| № | Источник | URL | Что использовано |
|---|---|---|---|
| 2 | IBM. What Is a Data Warehouse? | https://www.ibm.com/think/topics/data-warehouse | Определение DWH, OLAP, ETL |
| 3 | IBM. What is Data Ingestion? | https://www.ibm.com/think/topics/data-ingestion | Пакетный и потоковый приём данных |
| 4 | IBM. What Is a Data Lake? | https://www.ibm.com/think/topics/data-lake | Data Lake, Schema-on-Read, Data Swamp |
| 5 | IBM. What Is a Data Lakehouse? | https://www.ibm.com/think/topics/data-lakehouse | Lakehouse, табличные форматы |
| 6 | IBM. What Is a Data Mesh? | https://www.ibm.com/think/topics/data-mesh | Принципы Data Mesh |
| 7 | IBM. OLTP vs. OLAP | https://www.ibm.com/think/topics/oltp-vs-olap | Контрольный вопрос 1 |
| 8 | Armbrust M., Ghodsi A., Xin R., Zaharia M. Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics. CIDR 2021 | https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf | Мотивация Lakehouse, проблема двухуровневой архитектуры Lake + DWH |
| 9 | Dehghani Z. How to Move Beyond a Monolithic Data Lake to a Distributed Data Mesh (2019) | https://martinfowler.com/articles/data-monolith-to-mesh.html | Исходная статья о Data Mesh |
| 10 | Dehghani Z. Data Mesh Principles and Logical Architecture (2020) | https://martinfowler.com/articles/data-mesh-principles.html | Четыре принципа, федеративное управление, платформа самообслуживания |
| 18 | Databricks. What is Medallion Architecture? | https://www.databricks.com/glossary/medallion-architecture | Слои Bronze / Silver / Gold |

## 3. Документация по технологиям стека

| № | Источник | URL | Что использовано |
|---|---|---|---|
| 11 | Apache Kafka. Documentation | https://kafka.apache.org/documentation/ | Топики, партиции, `acks=all`, идемпотентный продюсер, retention, режим KRaft |
| 12 | Apache Spark. Structured Streaming Programming Guide | https://spark.apache.org/docs/latest/streaming/index.html | Микробатчи, watermark, `dropDuplicatesWithinWatermark`, checkpoint, источник Kafka |
| 13 | Apache Iceberg. Documentation | https://iceberg.apache.org/docs/latest/ | Скрытое партиционирование, эволюция схемы, `MERGE INTO`, процедуры обслуживания таблиц |
| 14 | Apache Iceberg. Table Spec | https://iceberg.apache.org/spec/ | Метаданные, снапшоты, манифесты, атомарный коммит (контрольный вопрос 7) |
| 15 | Apache Iceberg. Spark Structured Streaming | https://iceberg.apache.org/docs/latest/spark-structured-streaming/ | Потоковая запись в Iceberg, компактизация мелких файлов |
| 16 | Delta Lake. Documentation | https://docs.delta.io/latest/index.html | Альтернативный табличный формат, журнал транзакций `_delta_log` |
| 17 | Trino. Iceberg connector | https://trino.io/docs/current/connector/iceberg.html | Типы каталогов (REST, Hive Metastore, Glue, Nessie) |
| 19 | Fluent Bit. Kafka Producer (output plugin) | https://docs.fluentbit.io/manual/pipeline/outputs/kafka | Доставка JSON-логов в Kafka |
| 20 | Apache Airflow. Documentation | https://airflow.apache.org/docs/apache-airflow/stable/index.html | Оркестрация, backfill |
| 21 | Apache Superset. Documentation | https://superset.apache.org/docs/intro | BI-система |
| 27 | JSON Schema. Specification | https://json-schema.org/specification | Валидация JSON-событий |

## 4. Нотация C4 и инструменты для диаграмм

| № | Источник | URL | Что использовано |
|---|---|---|---|
| 22 | Brown S. The C4 model for visualising software architecture | https://c4model.com/ | Уровни модели C4 |
| 23 | C4 model. System context diagram | https://c4model.com/diagrams/system-context | Состав диаграммы уровня 1 (контрольный вопрос 13) |
| 24 | C4 model. Container diagram | https://c4model.com/diagrams/container | Понятие контейнера (контрольный вопрос 14) |
| 25 | C4-PlantUML | https://github.com/plantuml-stdlib/C4-PlantUML | Макросы `Person`, `System`, `Container`, `ContainerDb`, `ContainerQueue`, `Rel` |
| 26 | PlantUML. Standard Library | https://plantuml.com/stdlib | Подключение `!include <C4/...>` без доступа к сети |

## 5. Что взято из методички, а что — дополнительные рекомендации

- **Из методички:** формулировка варианта № 2 (задача, объём 100–300 млн событий/мес., источники), состав заданий, перечень архитектур для сравнения, структура отчёта, требования к диаграммам C4, перечень метрик и 15 контрольных вопросов.
- **Дополнительные технические рекомендации автора проекта** (основаны на документации из разделов 3–4): конкретный стек (Kafka KRaft, Spark Structured Streaming, Iceberg + REST Catalog, S3-совместимое хранилище, Trino, Airflow, Fluent Bit, FastAPI, Superset), слои Bronze/Silver/Gold, многоуровневая дедупликация, окна воронки, статистические формулы для A/B-тестов (стандартный z-критерий для двух долей), расчётные оценки нагрузки.
- **Условные данные:** числа в разделе 12.3 отчёта и оценки размера событий являются иллюстративными и не результатами измерений.

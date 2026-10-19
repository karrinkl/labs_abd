"""Режимы запуска Spark и их ресурсы — единственное место, где они заданы.

Используется submit.py (ручной запуск spark_job.py) и benchmark.py, поэтому
ресурсы режима в замерах и при обычном запуске совпадают. Значения берутся из
переменных окружения (см. .env.example), которые docker-compose.yml передаёт
в контейнер spark-client.
"""
from __future__ import annotations

import os

SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def modes() -> dict[str, dict]:
    driver_mem = _env("SPARK_DRIVER_MEMORY", "1g")
    master_url = _env("SPARK_MASTER_URL", "spark://spark-master:7077")
    exec_mem = _env("SPARK_EXECUTOR_MEMORY", "1g")
    exec_cores = _env("SPARK_EXECUTOR_CORES", "2")
    total_cores = _env("SPARK_TOTAL_EXECUTOR_CORES", exec_cores)
    driver_host = _env("SPARK_DRIVER_HOST", "spark-client")
    cluster_common = [
        "--master", master_url,
        "--deploy-mode", "client",
        "--driver-memory", driver_mem,
        "--conf", f"spark.driver.host={driver_host}",
        "--conf", "spark.driver.bindAddress=0.0.0.0",
    ]
    return {
        "local1": {
            "description": "локальный однопоточный режим local[1]: драйвер и единственный поток задач в одной JVM",
            "args": ["--master", "local[1]", "--driver-memory", driver_mem],
            "threads": 1,
        },
        "local_all": {
            "description": "локальный многопоточный режим local[*] (справочно; НЕ однопоточный)",
            "args": ["--master", "local[*]", "--driver-memory", driver_mem],
            "threads": os.cpu_count(),
        },
        "cluster": {
            "description": "Spark Standalone в Docker: все выделенные ядра Worker",
            "args": cluster_common + [
                "--executor-memory", exec_mem,
                "--executor-cores", exec_cores,
                "--total-executor-cores", total_cores,
            ],
            "executor_memory": exec_mem, "executor_cores": exec_cores,
            "total_executor_cores": total_cores,
        },
        "cluster_1core": {
            "description": "Spark Standalone в Docker, ограничение 1 ядро (накладные расходы без параллелизма)",
            "args": cluster_common + [
                "--executor-memory", exec_mem,
                "--executor-cores", "1",
                "--total-executor-cores", "1",
            ],
            "executor_memory": exec_mem, "executor_cores": "1", "total_executor_cores": "1",
        },
    }


def submit_command(mode: str, script: str, script_args: list[str] | None = None) -> list[str]:
    m = modes()
    if mode not in m:
        raise SystemExit(f"неизвестный режим {mode!r}; доступны: {', '.join(m)}")
    return [os.path.join(SPARK_HOME, "bin", "spark-submit"), *m[mode]["args"], script, *(script_args or [])]

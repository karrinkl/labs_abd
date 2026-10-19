#!/usr/bin/env python3
"""НЕОБЯЗАТЕЛЬНО: выполнить весь блокнот на Arch Linux против локального стенда Kafka с SSL.

Нужны: окружение .venv-spark (requirements-spark.txt), Java 17+, Docker и запущенный
local_kafka (./local_kafka/gen_certs.sh && docker compose -f local_kafka/docker-compose.yml up -d).

  python tools/run_notebook_local.py               # результаты -> local_run/
  python tools/run_notebook_local.py --out /tmp/lab3_local

Блокнот работает в «режиме отладки»: вместо Google Drive — каталог local_run/,
вместо Aiven — localhost:9093 с тестовыми сертификатами из local_kafka/certs,
топик создаётся автоматически. Выполненный блокнот сохраняется в <out>/executed.ipynb.
"""
import argparse
import os
import time
from pathlib import Path

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parent.parent


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=str(ROOT / "local_run"))
    p.add_argument("--bootstrap", default="localhost:9093")
    a = p.parse_args()
    out = Path(a.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    certs = ROOT / "local_kafka" / "certs"
    os.environ.update(
        LAB3_BASE_DIR=str(out / "drive"), LAB3_WORK_DIR=str(out / "work"), LAB3_SECRETS_DIR=str(out / "secrets"),
        LAB3_CODE_DIR=str(out / "code"), LAB3_BOOTSTRAP_SERVERS=a.bootstrap, LAB3_CA_FILE=str(certs / "ca.pem"),
        LAB3_CERT_FILE=str(certs / "service.cert"), LAB3_KEY_FILE=str(certs / "service.key"), LAB3_CREATE_TOPIC="1")
    nb = nbformat.read(ROOT / "lab3_bank_transactions.ipynb", as_version=4)
    t0 = time.time()
    client = NotebookClient(nb, timeout=1200, kernel_name="python3", resources={"metadata": {"path": str(out)}})
    try:
        client.execute()
        print(f"Блокнот выполнен за {time.time() - t0:.0f} с; результаты: {out / 'drive' / 'lab3_results'}")
    finally:
        nbformat.write(nb, out / "executed.ipynb")


if __name__ == "__main__":
    main()

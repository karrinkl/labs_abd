#!/usr/bin/env python3
"""Запуск PySpark-скрипта в выбранном режиме через spark-submit.

Выполняется внутри контейнера spark-client:
  python3 submit.py cluster spark_job.py --data data/transactions.csv
  python3 submit.py local1  spark_job.py --data data/transactions.csv

Режимы: local1, local_all, cluster, cluster_1core (см. spark_modes.py).
Флаг --dry-run печатает команду spark-submit, не запуская её.
"""
import os
import shlex
import sys

from spark_modes import submit_command


def main():
    args = sys.argv[1:]
    dry = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]
    if len(args) < 2:
        raise SystemExit(__doc__)
    mode, script, rest = args[0], args[1], args[2:]
    cmd = submit_command(mode, script, rest + ["--run-label", mode])
    print("$", " ".join(shlex.quote(c) for c in cmd), flush=True)
    if not dry:
        os.execv(cmd[0], cmd)


if __name__ == "__main__":
    main()

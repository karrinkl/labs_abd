#!/usr/bin/env bash
# Необязательный мониторинг CPU и памяти контейнеров во время benchmark.py.
# Запускается на ХОСТЕ в отдельном терминале; останавливается Ctrl+C.
#
#   ./scripts/docker_stats.sh                         # -> results/benchmark/docker_stats.csv
#   ./scripts/docker_stats.sh results/my_stats.csv 2  # свой файл и интервал, с
#
# docker stats показывает загрузку CPU в процентах от ОДНОГО ядра
# (200 % = два полностью занятых ядра) и текущее потребление памяти.
set -euo pipefail
cd "$(dirname "$0")/.."
out="${1:-results/benchmark/docker_stats.csv}"
interval="${2:-1}"
mkdir -p "$(dirname "$out")"
[ -s "$out" ] || echo "timestamp_utc,container,cpu_percent,mem_usage,mem_percent" > "$out"
echo "Запись в $out каждые ${interval} с; остановка — Ctrl+C"
while true; do
  ts="$(date -u +%Y-%m-%dT%H:%M:%S)"
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}' \
      spark-master spark-worker spark-client $(docker ps --format '{{.Names}}' | grep -x spark-worker-2 || true) \
    | sed "s|^|${ts},|; s|%||g" >> "$out"
  sleep "$interval"
done

#!/usr/bin/env python3
"""Сравнение локального (local[1]) и распределённого (Spark Standalone) режимов.

Запуск — в контейнере spark-client (кластер должен быть поднят):
  python3 benchmark.py --data data/transactions.csv
  python3 benchmark.py --data data/transactions_test.csv data/transactions.csv \\
                       --modes local1 cluster cluster_1core --repeats 5

Как устроен замер
-----------------
* Каждый режим запускается ОТДЕЛЬНЫМ процессом spark-submit (новая JVM и новое
  приложение Spark) с ресурсами из spark_modes.py.
* Измеряемая операция одинакова для всех режимов (transactions_pipeline.
  benchmark_pipeline): чтение CSV -> нормализация и проверки -> удаление
  дубликатов -> группировка по city -> глобальная сортировка -> collect().
  Запись файлов в замер не входит.
* Время старта SparkSession (JVM, регистрация у Master, получение executors)
  измеряется отдельно и в основное время не включается.
* Первый прогон — прогревочный (JIT, загрузка классов, кэш ОС) и не учитывается;
  затем выполняется N измеряемых повторов, в таблицу идёт медиана.
* Результаты агрегации всех режимов сравниваются построчно (город, точное
  среднее DECIMAL, количество) — ускорение имеет смысл только при совпадении.

Дополнительно собираются метрики Spark REST API (суммарное время задач,
CPU-время задач, объём shuffle, пиковая память JVM executors) и CPU/RSS
процессов клиента по /proc — без сторонних библиотек.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent


# ============================================================================ child
def run_child(a):
    """Выполняется внутри spark-submit: один режим, прогрев + N повторов."""
    t0 = time.perf_counter()
    import transactions_pipeline as tp  # импорт здесь: родителю pyspark не нужен

    cfg = tp.JobConfig()
    spark = tp.build_spark(f"lab2-benchmark-{a.mode}", cfg)
    sc = spark.sparkContext
    # В режиме кластера дождаться регистрации executors, чтобы не мерить их запуск.
    if not sc.master.startswith("local"):
        deadline = time.time() + 60
        while time.time() < deadline:
            ex = tp.rest_get(sc, "executors")
            if isinstance(ex, list) and any(e.get("id") != "driver" for e in ex):
                break
            time.sleep(0.2)
    session_start_s = time.perf_counter() - t0

    input_partitions = tp.read_raw(spark, a.data).rdd.getNumPartitions()
    timings, result = [], None
    for i in range(a.warmup + a.repeats):
        group = f"warmup-{i}" if i < a.warmup else f"timed-{i - a.warmup}"
        sc.setJobGroup(group, group)
        t = time.perf_counter()
        rows = tp.benchmark_pipeline(spark, a.data, cfg)
        dt = time.perf_counter() - t
        if i >= a.warmup:
            timings.append(dt)
        if result is None:
            result = rows
        elif rows != result:
            raise SystemExit("результат изменился между повторами — замер некорректен")
        print(f"[{a.mode}] {group}: {dt:.3f} s", flush=True)

    time.sleep(2)  # REST API Spark UI обновляется асинхронно
    jobs = tp.rest_get(sc, "jobs") or []
    stages = tp.rest_get(sc, "stages") or []
    timed_stage_ids = {sid for j in jobs if isinstance(j, dict) and str(j.get("jobGroup", "")).startswith("timed")
                       for sid in j.get("stageIds", [])}
    m = {"executor_run_time_s": 0.0, "executor_cpu_time_s": 0.0, "input_mb": 0.0,
         "shuffle_read_mb": 0.0, "shuffle_write_mb": 0.0, "tasks": 0}
    for s in stages if isinstance(stages, list) else []:
        if s.get("stageId") in timed_stage_ids and s.get("status") == "COMPLETE":
            m["executor_run_time_s"] += s.get("executorRunTime", 0) / 1000
            m["executor_cpu_time_s"] += s.get("executorCpuTime", 0) / 1e9
            m["input_mb"] += s.get("inputBytes", 0) / 2 ** 20
            m["shuffle_read_mb"] += s.get("shuffleReadBytes", 0) / 2 ** 20
            m["shuffle_write_mb"] += s.get("shuffleWriteBytes", 0) / 2 ** 20
            m["tasks"] += s.get("numCompleteTasks", 0)
    n = max(a.repeats, 1)
    per_run = {k: (v / n) for k, v in m.items()}  # среднее на один измеряемый прогон
    executors = tp.executor_summary(tp.rest_get(sc, "executors") or [])
    out = {
        "mode": a.mode,
        "master": sc.master,
        "spark_version": spark.version,
        "app_id": sc.applicationId,
        "default_parallelism": sc.defaultParallelism,
        "input_partitions": input_partitions,
        "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
        "max_partition_bytes": spark.conf.get("spark.sql.files.maxPartitionBytes"),
        "executor_memory_conf": sc.getConf().get("spark.executor.memory", None),
        "executor_cores_conf": sc.getConf().get("spark.executor.cores", None),
        "cores_max_conf": sc.getConf().get("spark.cores.max", None),
        "driver_memory_conf": sc.getConf().get("spark.driver.memory", None),
        "executors": executors,
        "session_start_s": session_start_s,
        "timings_s": timings,
        "spark_metrics_per_run": per_run,
        "result": result,
    }
    Path(a.out).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    spark.stop()


# ============================================================================ parent
class ProcSampler(threading.Thread):
    """CPU и RSS дерева процессов клиента (spark-submit -> JVM -> python) по /proc."""

    def __init__(self, root_pid: int, interval: float = 0.5):
        super().__init__(daemon=True)
        self.root, self.interval = root_pid, interval
        self.peak_rss_mb, self.cpu_s, self._stop = 0.0, 0.0, threading.Event()
        self._cpu_by_pid = {}
        self.tick = os.sysconf("SC_CLK_TCK")

    def _tree(self):
        children = {}
        for d in os.listdir("/proc"):
            if d.isdigit():
                try:
                    with open(f"/proc/{d}/stat") as fh:
                        ppid = int(fh.read().rsplit(")", 1)[1].split()[1])
                    children.setdefault(ppid, []).append(int(d))
                except (OSError, IndexError, ValueError):
                    pass
        out, stack = [], [self.root]
        while stack:
            p = stack.pop()
            out.append(p)
            stack.extend(children.get(p, []))
        return out

    def run(self):
        while not self._stop.is_set():
            rss = 0
            for pid in self._tree():
                try:
                    with open(f"/proc/{pid}/stat") as fh:
                        f = fh.read().rsplit(")", 1)[1].split()
                    self._cpu_by_pid[pid] = (int(f[11]) + int(f[12])) / self.tick
                    with open(f"/proc/{pid}/status") as fh:
                        for line in fh:
                            if line.startswith("VmRSS:"):
                                rss += int(line.split()[1]) / 1024
                except (OSError, IndexError, ValueError):
                    pass
            self.peak_rss_mb = max(self.peak_rss_mb, rss)
            self.cpu_s = sum(self._cpu_by_pid.values())
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()


def host_info():
    mem_total = None
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    mem_total = round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    return {"cpu_count_visible": os.cpu_count(), "mem_total_gib_visible": mem_total}


def master_state():
    url = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
    try:
        with urllib.request.urlopen(url + "/json/", timeout=5) as r:
            s = json.loads(r.read().decode("utf-8"))
        return {"url": s.get("url"), "status": s.get("status"), "alive_workers": s.get("aliveworkers"),
                "cores": s.get("cores"), "memory_mib": s.get("memory"),
                "workers": [{"id": w["id"], "cores": w["cores"], "memory_mib": w["memory"], "state": w["state"]}
                            for w in s.get("workers", [])]}
    except Exception as exc:
        return {"error": str(exc)}


def count_rows(path: str) -> int:
    with open(path, "rb") as fh:
        return sum(1 for _ in fh) - 1


def run_parent(a):
    from spark_modes import modes, submit_command

    out_dir = Path(a.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    probe = out_dir / ".write_test"
    try:  # проверить права на запись ДО многоминутных замеров
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as exc:
        sys.exit(f"Нет прав на запись в {out_dir}: {exc}. Проверьте владельца каталога (ls -ld {out_dir}).")
    mode_defs = modes()
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cluster = master_state() if any(m.startswith("cluster") for m in a.modes) else None
    if cluster is not None and (cluster.get("status") != "ALIVE" or not cluster.get("alive_workers")):
        sys.exit(f"Кластер недоступен или без Worker: {cluster}. Запустите docker compose up -d.")

    runs = []
    for data in a.data:
        rows = count_rows(data)
        for mode in a.modes:
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False, dir="/tmp") as tmp:
                child_out = tmp.name
            cmd = submit_command(mode, str(HERE / "benchmark.py"),
                                 ["--child", "--mode", mode, "--data", data, "--out", child_out,
                                  "--repeats", str(a.repeats), "--warmup", str(a.warmup)])
            print(f"\n=== {data} | {mode}: {mode_defs[mode]['description']}", flush=True)
            t = time.perf_counter()
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            sampler = ProcSampler(proc.pid)
            sampler.start()
            log_lines = []
            for line in proc.stdout:
                log_lines.append(line)
                if line.startswith(f"[{mode}]"):
                    print("  " + line.rstrip(), flush=True)
            proc.wait()
            sampler.stop()
            wall = time.perf_counter() - t
            if proc.returncode != 0:
                print("".join(log_lines[-40:]))
                sys.exit(f"режим {mode} завершился с ошибкой {proc.returncode}")
            child = json.loads(Path(child_out).read_text(encoding="utf-8"))
            os.unlink(child_out)
            child.update({"dataset": data, "rows_in_file": rows,
                          "file_mb": round(os.path.getsize(data) / 2 ** 20, 1),
                          "description": mode_defs[mode]["description"],
                          "process_wall_s": wall,
                          "client_peak_rss_mb": round(sampler.peak_rss_mb, 1),
                          "client_cpu_s": round(sampler.cpu_s, 2)})
            runs.append(child)
            # сохранить результат режима сразу: сбой на следующих шагах его не потеряет
            part = out_dir / f"run_{Path(data).stem}_{mode}.json"
            part.write_text(json.dumps(child, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---- сверка результатов и сводка
    summary = []
    for data in a.data:
        group = [r for r in runs if r["dataset"] == data]
        reference = group[0]["result"]
        base = next((r for r in group if r["mode"] == "local1"), None)
        base_median = statistics.median(base["timings_s"]) if base else None
        for r in group:
            ts = r["timings_s"]
            med = statistics.median(ts)
            execs = [e for e in r["executors"] if e["id"] != "driver"]
            summary.append({
                "dataset": data,
                "rows_in_file": r["rows_in_file"],
                "file_mb": r["file_mb"],
                "mode": r["mode"],
                "master": r["master"],
                "workers": (cluster or {}).get("alive_workers") if r["mode"].startswith("cluster") else 0,
                "executors": len(execs) if execs else 0,
                "task_slots": sum(e["totalCores"] or 0 for e in execs) if execs else r["default_parallelism"],
                "executor_memory": r["executor_memory_conf"] or "— (задачи в JVM драйвера)",
                "driver_memory": r["driver_memory_conf"],
                "input_partitions": r["input_partitions"],
                "shuffle_partitions": r["shuffle_partitions"],
                "repeats": len(ts),
                "median_s": round(med, 3),
                "mean_s": round(statistics.mean(ts), 3),
                "min_s": round(min(ts), 3),
                "max_s": round(max(ts), 3),
                "stdev_s": round(statistics.stdev(ts), 3) if len(ts) > 1 else 0.0,
                "session_start_s": round(r["session_start_s"], 2),
                "speedup_vs_local1": round(base_median / med, 2) if base_median else None,
                "task_time_s_per_run": round(r["spark_metrics_per_run"]["executor_run_time_s"], 2),
                "task_cpu_s_per_run": round(r["spark_metrics_per_run"]["executor_cpu_time_s"], 2),
                "shuffle_write_mb_per_run": round(r["spark_metrics_per_run"]["shuffle_write_mb"], 2),
                "tasks_per_run": round(r["spark_metrics_per_run"]["tasks"]),
                "peak_executor_heap_mb": round(max((e["peakJVMHeapMemory"] or 0) for e in r["executors"]) / 2 ** 20, 1)
                if r["executors"] else None,
                "client_peak_rss_mb": r["client_peak_rss_mb"],
                "client_cpu_s": r["client_cpu_s"],
                "results_match_reference": r["result"] == reference,
            })
    all_match = all(s["results_match_reference"] for s in summary)

    with (out_dir / "runs.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["dataset", "mode", "repeat", "seconds"])
        for r in runs:
            for i, t in enumerate(r["timings_s"], 1):
                w.writerow([r["dataset"], r["mode"], i, f"{t:.4f}"])
    with (out_dir / "summary.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    meta = {"started_utc": started, "host_visible_from_client_container": host_info(),
            "cluster": cluster, "repeats": a.repeats, "warmup": a.warmup,
            "measured_operation": ("read CSV -> normalize/validate -> deduplicate -> groupBy(city) "
                                   "avg/count -> orderBy(avg desc) -> collect; writes excluded; "
                                   "SparkSession start measured separately"),
            "results_match_across_modes": all_match}
    (out_dir / "raw.json").write_text(json.dumps({"meta": meta, "runs": runs}, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    (out_dir / "summary.md").write_text(render_markdown(summary, meta), encoding="utf-8")
    print("\n" + render_markdown(summary, meta))
    if not all_match:
        sys.exit("ВНИМАНИЕ: результаты режимов НЕ совпадают — сравнение времени некорректно")


def render_markdown(summary, meta):
    lines = [
        f"Замер: {meta['started_utc']} UTC; повторов {meta['repeats']} (+{meta['warmup']} прогревочный); "
        f"время — медиана; результаты всех режимов совпадают: **{meta['results_match_across_modes']}**.",
        "", "Операция: " + meta["measured_operation"], "",
        "| Набор | Строк | Режим | Конфигурация | Время, с (медиана) | min–max, с | Старт сессии, с | Партиций (вход / shuffle) | Speedup = T_local1 / T |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for s in summary:
        if s["mode"].startswith("local"):
            conf = f"{s['master']}, слотов задач {s['task_slots']}, driver {s['driver_memory']}"
        else:
            conf = (f"Standalone: Worker {s['workers']}, executors {s['executors']}, "
                    f"ядер {s['task_slots']}, executor {s['executor_memory']}, driver {s['driver_memory']}")
        rows = f"{s['rows_in_file']:,}".replace(",", " ")
        lines.append(f"| {Path(s['dataset']).name} | {rows} | {s['mode']} | {conf} | "
                     f"{s['median_s']:.3f} | {s['min_s']:.3f}–{s['max_s']:.3f} | {s['session_start_s']:.1f} | "
                     f"{s['input_partitions']} / {s['shuffle_partitions']} | {s['speedup_vs_local1']} |")
    lines += ["", "| Набор | Режим | Время задач, с/прогон | CPU задач, с/прогон | Shuffle write, МБ/прогон | Задач/прогон | Пик heap executor, МБ | Пик RSS клиента, МБ | CPU клиента, с (весь процесс) |",
              "|---|---|---|---|---|---|---|---|---|"]
    for s in summary:
        lines.append(f"| {Path(s['dataset']).name} | {s['mode']} | {s['task_time_s_per_run']} | {s['task_cpu_s_per_run']} | "
                     f"{s['shuffle_write_mb_per_run']} | {s['tasks_per_run']} | {s['peak_executor_heap_mb']} | "
                     f"{s['client_peak_rss_mb']} | {s['client_cpu_s']} |")
    return "\n".join(lines) + "\n"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", nargs="+", default=["data/transactions.csv"])
    p.add_argument("--modes", nargs="+", default=["local1", "cluster"],
                   help="local1, local_all, cluster, cluster_1core")
    p.add_argument("--repeats", type=int, default=int(os.environ.get("BENCH_REPEATS", "5")))
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--out", default="results/benchmark")
    # служебные параметры дочернего процесса
    p.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--mode", help=argparse.SUPPRESS)
    a = p.parse_args()
    if a.child:
        a.data = a.data[0]
        run_child(a)
    else:
        run_parent(a)


if __name__ == "__main__":
    main()

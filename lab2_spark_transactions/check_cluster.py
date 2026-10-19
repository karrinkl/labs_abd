#!/usr/bin/env python3
"""Проверка состояния Spark Standalone через JSON-API Master (только stdlib).

Подтверждает не просто статус контейнеров «Up», а то, что Worker
ЗАРЕГИСТРИРОВАН у Master: выводит список Worker, их ядра и память, а также
запущенные и завершённые приложения.

  docker compose exec spark-client python3 check_cluster.py        # из контейнера
  python check_cluster.py --url http://localhost:8080               # с хоста
  python check_cluster.py --save results/cluster_state.json         # сохранить JSON
"""
import argparse
import json
import os
import sys
import urllib.request


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=os.environ.get("SPARK_MASTER_UI", "http://localhost:8080"))
    p.add_argument("--save", help="сохранить ответ Master в файл JSON")
    a = p.parse_args()
    try:
        with urllib.request.urlopen(a.url.rstrip("/") + "/json/", timeout=10) as r:
            state = json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        sys.exit(f"Master недоступен по {a.url}: {exc}")

    print(f"Master URL     : {state.get('url')}")
    print(f"Master status  : {state.get('status')}")
    print(f"Workers alive  : {state.get('aliveworkers')}")
    print(f"Cores total    : {state.get('cores')} (used {state.get('coresused')})")
    print(f"Memory total   : {state.get('memory')} MiB (used {state.get('memoryused')} MiB)")
    print("\nWorkers:")
    for w in state.get("workers", []):
        print(f"  {w['id']:<45} {w['host']:<15} state={w['state']:<6} "
              f"cores={w['cores']} (used {w['coresused']}) memory={w['memory']} MiB (used {w['memoryused']})")
    for key, title in (("activeapps", "Active applications"), ("completedapps", "Completed applications")):
        apps = state.get(key, [])
        print(f"\n{title}: {len(apps)}")
        for app in apps[-10:]:
            print(f"  {app['id']:<28} {app['name']:<32} state={app['state']:<9} cores={app.get('cores')} "
                  f"memoryperexecutor={app.get('memoryperexecutor', app.get('memoryperslave'))} MiB "
                  f"duration={app.get('duration', 0) / 1000:.1f} s")
    if a.save:
        os.makedirs(os.path.dirname(a.save) or ".", exist_ok=True)
        with open(a.save, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2)
        print(f"\nСохранено: {a.save}")
    if state.get("status") != "ALIVE" or not state.get("aliveworkers"):
        sys.exit("ВНИМАНИЕ: Master не ALIVE или нет зарегистрированных Worker")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Готовит report.md к конвертации: скриншоты, которых ещё нет в
results/screenshots/, заменяются заметной заглушкой с подписью, а не
пропадают из документа.

python3 tools/prepare_md.py report.md build/report.prepared.md
"""
import re
import sys
from pathlib import Path

IMG = re.compile(r"^!\[(?P<cap>[^\]]*)\]\((?P<path>results/screenshots/[^)\s]+)\)(?P<attr>\{[^}]*\})?\s*$")


def main(src, dst):
    root = Path(src).resolve().parent
    out, missing = [], []
    for line in Path(src).read_text(encoding="utf-8").splitlines():
        m = IMG.match(line)
        if m and not (root / m["path"]).exists():
            missing.append(m["path"])
            out.append(f"[МЕСТО ДЛЯ СКРИНШОТА: {m['path']}]")
            out.append("")
            out.append(f"{m['cap']}")
            continue
        out.append(line)
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    Path(dst).write_text("\n".join(out) + "\n", encoding="utf-8")
    if missing:
        print("Нет скриншотов (вставлены заглушки):", *missing, sep="\n  ")


if __name__ == "__main__":
    main(*sys.argv[1:3])

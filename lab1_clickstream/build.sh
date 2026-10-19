#!/usr/bin/env bash
# Пересборка диаграмм и отчёта.
# Arch Linux: sudo pacman -S --needed plantuml graphviz pandoc-cli python-docx libreoffice-fresh poppler
set -euo pipefail
cd "$(dirname "$0")"

# 1. Диаграммы C4 (SVG — для просмотра, PNG 180 dpi — для вставки в DOCX)
PLANTUML="${PLANTUML:-plantuml}"          # можно указать: PLANTUML="java -jar /путь/plantuml.jar"
$PLANTUML -checkonly c4_context.puml c4_container.puml
$PLANTUML -tsvg c4_context.puml c4_container.puml
$PLANTUML -tpng -Sdpi=180 c4_context.puml c4_container.puml

# 2. Шаблон стилей (создаётся один раз)
[ -f tools/reference.docx ] || python3 tools/make_reference.py

# 3. Markdown -> DOCX и постобработка
build_docx() {
  pandoc report.md -f markdown -t docx --reference-doc=tools/reference.docx \
    --highlight-style=monochrome -o report.docx
  python3 tools/postprocess_docx.py report.docx "$@"
}
build_docx

# 4. Номера страниц в содержании (нужны soffice и pdftotext; иначе — F9 в Word)
if command -v soffice >/dev/null && command -v pdftotext >/dev/null; then
  tmp="$(mktemp)"
  for _ in 1 2; do                       # второй проход учитывает длину самого содержания
    python3 tools/toc_pages.py report.docx > "$tmp"
    build_docx --pages "$tmp"
  done
  rm -f "$tmp"
fi
echo "Готово: report.docx"

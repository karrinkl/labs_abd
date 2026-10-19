#!/usr/bin/env bash
# Сборка report.docx из report.md.
# Arch Linux: sudo pacman -S --needed pandoc-cli python-docx
#   (необязательно, для номеров страниц в содержании: libreoffice-fresh poppler)
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f tools/reference.docx ] || python3 tools/make_reference.py
python3 tools/prepare_md.py report.md build/report.prepared.md

build_docx() {
  pandoc build/report.prepared.md -f markdown -t docx --resource-path=. \
    --reference-doc=tools/reference.docx --highlight-style=monochrome -o report.docx
  python3 tools/postprocess_docx.py report.docx "$@"
}
build_docx
if command -v soffice >/dev/null && command -v pdftotext >/dev/null; then
  tmp="$(mktemp)"
  for _ in 1 2; do
    python3 tools/toc_pages.py report.docx > "$tmp"
    build_docx --pages "$tmp"
  done
  rm -f "$tmp"
else
  echo "LibreOffice/pdftotext не найдены: номера страниц в содержании обновите в Word (F9)"
fi
rm -rf build
echo "Готово: report.docx"

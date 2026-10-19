#!/usr/bin/env python3
"""Определяет номера страниц заголовков для содержания report.docx.

Документ конвертируется в PDF через LibreOffice (сам DOCX не изменяется),
затем в тексте каждой страницы ищутся заголовки уровней 1–2.
Результат — JSON {текст заголовка: номер страницы} в stdout.

Запуск: python3 tools/toc_pages.py report.docx > pages.json
Требуются: libreoffice-fresh (soffice), poppler (pdftotext), python-docx.
"""
import json
import pathlib
import re
import subprocess
import sys
import tempfile

from docx import Document


def norm(text):
    return re.sub(r"[\s­]+", "", text)


def main(path):
    path = pathlib.Path(path).resolve()
    doc = Document(str(path))
    found = False
    headings = []
    for p in doc.paragraphs:
        name = p.style.name if p.style is not None else ""
        if name in ("Heading 1", "Heading 2") and p.text.strip():
            headings.append(p.text.strip())
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            ["soffice", "--headless", f"-env:UserInstallation=file://{tmp}/profile",
             "--convert-to", "pdf", "--outdir", tmp, str(path)],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        pdf = pathlib.Path(tmp) / (path.stem + ".pdf")
        text = subprocess.run(["pdftotext", "-enc", "UTF-8", str(pdf), "-"],
                              check=True, capture_output=True, text=True).stdout
    pages = [norm(t) for t in text.split("\f")]
    # Первый раздел начинается с новой страницы: ищем страницу, которая
    # начинается с первого заголовка (страницы содержания пропускаются).
    start = next(i for i, t in enumerate(pages) if t.startswith(norm(headings[0])))
    result, cur = {}, start
    for h in headings:
        key = norm(h)
        for i in range(cur, len(pages)):
            if key in pages[i]:
                result[h] = i + 1
                cur = i
                found = True
                break
    if not found:
        sys.exit("заголовки не найдены в PDF")
    json.dump(result, sys.stdout, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "report.docx")

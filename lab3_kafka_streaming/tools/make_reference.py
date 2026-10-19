#!/usr/bin/env python3
"""Создаёт tools/reference.docx — шаблон стилей для pandoc.

Оформление: Times New Roman 14 pt, межстрочный интервал 1,5, абзацный отступ
1,25 см, поля 30/15/20/20 мм. Стили титульного листа задаются отдельно, чтобы
поля [в квадратных скобках] было удобно заменить в Word / LibreOffice.

Запуск: python3 tools/make_reference.py  (нужны pandoc и python-docx)
"""
import pathlib
import subprocess

from docx import Document
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.shared import Cm, Mm, Pt, RGBColor

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / "reference.docx"
FONT = "Times New Roman"


def set_font(style, size=None, bold=None, color_black=True):
    style.font.name = FONT
    rpr = style.element.get_or_add_rPr()
    rfonts = rpr.find(qn("w:rFonts"))
    if rfonts is None:
        rfonts = rpr.makeelement(qn("w:rFonts"), {})
        rpr.append(rfonts)
    for attr in ("w:ascii", "w:hAnsi", "w:cs", "w:eastAsia"):
        rfonts.set(qn(attr), FONT)
    for attr in ("w:asciiTheme", "w:hAnsiTheme", "w:cstheme", "w:eastAsiaTheme"):
        rfonts.attrib.pop(qn(attr), None)
    if size:
        style.font.size = Pt(size)
    if bold is not None:
        style.font.bold = bold
    if color_black:
        style.font.color.rgb = RGBColor(0, 0, 0)


def para_style(doc, name):
    # Ищем по styleId: python-docx переводит «Heading 1» во внутреннее «heading 1»,
    # а в шаблоне pandoc имя записано иначе — поиск по имени создал бы дубликат.
    style_id = name.replace(" ", "")
    for st in doc.styles:
        if st.type == WD_STYLE_TYPE.PARAGRAPH and (st.style_id == style_id or st.name == name):
            return st
    return doc.styles.add_style(name, WD_STYLE_TYPE.PARAGRAPH)


def main():
    subprocess.run(
        ["pandoc", "-o", str(OUT), "--print-default-data-file", "reference.docx"],
        check=True,
    )
    doc = Document(str(OUT))

    for s in doc.sections:
        s.page_height, s.page_width = Mm(297), Mm(210)
        s.left_margin, s.right_margin = Mm(30), Mm(15)
        s.top_margin, s.bottom_margin = Mm(20), Mm(20)

    for name in ("Normal", "Body Text", "First Paragraph", "Compact"):
        st = para_style(doc, name)
        set_font(st, 14)
        pf = st.paragraph_format
        pf.line_spacing = 1.5
        pf.space_before, pf.space_after = Pt(0), Pt(0)
        if name != "Compact":
            pf.first_line_indent = Cm(1.25)
            pf.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY

    for name, size in (("Heading 1", 16), ("Heading 2", 14), ("Heading 3", 14)):
        st = para_style(doc, name)
        set_font(st, size, bold=True)
        st.font.italic = False
        pf = st.paragraph_format
        pf.space_before, pf.space_after = Pt(12), Pt(6)
        pf.first_line_indent = Cm(1.25)
        pf.keep_with_next = True
        pf.alignment = WD_ALIGN_PARAGRAPH.LEFT

    for name in ("Title", "Subtitle", "Author", "Date"):
        set_font(para_style(doc, name), 14)

    cap = para_style(doc, "Image Caption")
    set_font(cap, 12)
    cap.font.italic = False
    cap.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
    cap.paragraph_format.first_line_indent = Cm(0)
    cap.paragraph_format.space_after = Pt(12)

    fig = para_style(doc, "Captioned Figure")
    fig.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
    fig.paragraph_format.first_line_indent = Cm(0)
    fig.paragraph_format.keep_with_next = True

    code = para_style(doc, "Source Code")
    code.font.size = Pt(9)
    code.paragraph_format.line_spacing = 1.0

    # Стили титульного листа (используются в report.md через custom-style).
    title_styles = {
        "Title Page": (WD_ALIGN_PARAGRAPH.CENTER, 14, False, 0),
        "Title Page Main": (WD_ALIGN_PARAGRAPH.CENTER, 16, True, 0),
        "Title Page Right": (WD_ALIGN_PARAGRAPH.LEFT, 14, False, 9),
        "Title Page Bottom": (WD_ALIGN_PARAGRAPH.CENTER, 14, False, 0),
    }
    for name, (align, size, bold, left_cm) in title_styles.items():
        st = para_style(doc, name)
        st.base_style = doc.styles["Normal"]
        set_font(st, size, bold=bold)
        pf = st.paragraph_format
        pf.alignment = align
        pf.first_line_indent = Cm(0)
        pf.left_indent = Cm(left_cm)
        pf.line_spacing = 1.5
        pf.space_after = Pt(6)

    doc.save(str(OUT))
    print(f"written {OUT}")


if __name__ == "__main__":
    main()

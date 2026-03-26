"""Classify each PDF page as text-rich, scanned, or mixed."""

from __future__ import annotations

import fitz

from equation_schema import PageKind, PageTriageResult


def _page_image_area_ratio(page: fitz.Page) -> float:
    """Rough fraction of page area covered by embedded images."""
    page_area = float(page.rect.width * page.rect.height) or 1.0
    total = 0.0
    for img in page.get_images(full=True):
        try:
            xref = img[0]
            rects = page.get_image_rects(xref)
            for r in rects:
                total += r.width * r.height
        except Exception:
            continue
    return min(1.0, total / page_area)


def triage_page(page: fitz.Page, page_index0: int) -> PageTriageResult:
    text = page.get_text("text") or ""
    text_chars = len(text.replace(" ", ""))
    img_ratio = _page_image_area_ratio(page)
    has_text_layer = text_chars >= 40  # heuristic

    if has_text_layer and img_ratio < 0.35:
        kind = PageKind.text_rich
    elif not has_text_layer and img_ratio > 0.25:
        kind = PageKind.scanned
    else:
        kind = PageKind.mixed

    return PageTriageResult(
        page_index0=page_index0,
        page_num1=page_index0 + 1,
        page_kind=kind,
        text_char_count=text_chars,
        image_area_ratio=img_ratio,
        has_text_layer=has_text_layer,
    )

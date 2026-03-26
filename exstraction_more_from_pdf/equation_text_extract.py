"""Extract plain text from a PDF region via PyMuPDF (text-first path)."""

from __future__ import annotations

import fitz


def text_quality_score(s: str) -> float:
    s = (s or "").strip()
    if len(s) < 2:
        return 0.0
    score = min(5.0, len(s) / 40.0)
    if "=" in s or any(c in s for c in "∫∑∏√∂∇"):
        score += 2.0
    # Penalize obvious paragraph prose
    if len(s) > 400:
        score -= 1.0
    return max(0.0, min(10.0, score))


def extract_text_in_bbox(page: fitz.Page, bbox: tuple[float, float, float, float]) -> str:
    rect = fitz.Rect(bbox) & page.rect
    return (page.get_text("text", clip=rect) or "").strip()

"""Structural verification for extracted equation records."""

from __future__ import annotations

import fitz

from equation_schema import EquationRecord, VerificationStatus


def verify_equation_record(
    rec: EquationRecord,
    page: fitz.Page,
) -> EquationRecord:
    prior = list(rec.verification_flags)
    flags: list[str] = []
    pr = page.rect
    x0, y0, x1, y1 = rec.bbox
    page_rect = (pr.x0, pr.y0, pr.x1, pr.y1)

    if x1 <= x0 or y1 <= y0:
        flags.append("invalid_bbox_dims")

    margin = 2.0
    if (
        x0 < page_rect[0] - margin
        or y0 < page_rect[1] - margin
        or x1 > page_rect[2] + margin
        or y1 > page_rect[3] + margin
    ):
        flags.append("bbox_outside_page_margins")

    tg = (rec.text_guess or "").strip()
    lg = (rec.latex_guess or "").strip()
    if not tg and not lg:
        flags.append("empty_extraction")

    if len(tg) > 8000 or len(lg) > 8000:
        flags.append("excessive_length")

    open_b = (tg + lg).count("{") + (tg + lg).count("[")
    close_b = (tg + lg).count("}") + (tg + lg).count("]")
    if abs(open_b - close_b) > 6:
        flags.append("bracket_imbalance_heuristic")

    combined = prior + flags
    rec.verification_flags = combined
    rec.verification_status = (
        VerificationStatus.passed if not combined else VerificationStatus.failed
    )
    return rec

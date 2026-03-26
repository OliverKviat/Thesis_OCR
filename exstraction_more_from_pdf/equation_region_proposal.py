"""Propose equation regions (bbox) on a page — v1 display-focused."""

from __future__ import annotations

import re
import fitz

from equation_schema import PageKind, PageTriageResult, RegionProposal

_MATH_LINE = re.compile(r"[=∫∑∏√∂∇∈≤≥]|\\[a-zA-Z]+")
_DISPLAY_HINT = re.compile(r"^\s*\(?\s*\d+(?:\.\d+)*\s*\)\s*$")


def _math_score_for_text(s: str) -> float:
    if not s.strip():
        return 0.0
    score = 0.0
    score += min(3.0, s.count("=") * 0.5)
    score += min(4.0, len(_MATH_LINE.findall(s)) * 0.6)
    if any(c in s for c in "∫∑∏√∂∇∞"):
        score += 1.5
    if len(s) < 200 and score > 0:
        score += 0.5
    return score


def propose_regions_text_rich(
    page: fitz.Page,
    page_index0: int,
    *,
    min_line_score: float = 0.85,
) -> list[RegionProposal]:
    """Merge consecutive math-like lines into candidate display-equation bboxes."""
    d = page.get_text("dict")
    proposals: list[RegionProposal] = []
    lines_meta: list[tuple[tuple[float, float, float, float], float]] = []

    for block in d.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            spans = line.get("spans", [])
            if not spans:
                continue
            text = "".join(s.get("text", "") for s in spans)
            sc = _math_score_for_text(text)
            if _DISPLAY_HINT.match(text.strip()):
                sc = max(sc, 1.0)
            if sc < min_line_score:
                continue
            x0 = min(s["bbox"][0] for s in spans)
            y0 = min(s["bbox"][1] for s in spans)
            x1 = max(s["bbox"][2] for s in spans)
            y1 = max(s["bbox"][3] for s in spans)
            lines_meta.append(((x0, y0, x1, y1), sc))

    if not lines_meta:
        text = page.get_text("text") or ""
        if _math_score_for_text(text) >= 2.0:
            r = page.rect
            band = fitz.Rect(
                r.x0 + r.width * 0.08,
                r.y0 + r.height * 0.2,
                r.x1 - r.width * 0.08,
                r.y1 - r.height * 0.15,
            )
            return [
                RegionProposal(
                    page_num1=page_index0 + 1,
                    page_index0=page_index0,
                    region_id=f"p{page_index0+1:03d}_r001",
                    bbox=(band.x0, band.y0, band.x1, band.y1),
                    proposal_method="fallback_page_band",
                    proposal_score=1.0,
                )
            ]
        return []

    # Group consecutive lines (reading order already in dict) into regions
    rid = 0
    i = 0
    while i < len(lines_meta):
        group = [lines_meta[i][0]]
        group_score = lines_meta[i][1]
        j = i + 1
        while j < len(lines_meta):
            _, y0a, _, y1a = lines_meta[j - 1][0]
            x0b, y0b, x1b, y1b = lines_meta[j][0]
            gap = y0b - y1a
            if gap < 14.0:  # same cluster
                group.append(lines_meta[j][0])
                group_score += lines_meta[j][1]
                j += 1
            else:
                break
        x0 = min(b[0] for b in group)
        y0 = min(b[1] for b in group)
        x1 = max(b[2] for b in group)
        y1 = max(b[3] for b in group)
        pad = 4.0
        rect = fitz.Rect(x0 - pad, y0 - pad, x1 + pad, y1 + pad) & page.rect
        rid += 1
        proposals.append(
            RegionProposal(
                page_num1=page_index0 + 1,
                page_index0=page_index0,
                region_id=f"p{page_index0+1:03d}_r{rid:03d}",
                bbox=(rect.x0, rect.y0, rect.x1, rect.y1),
                proposal_method="text_line_cluster",
                proposal_score=min(10.0, group_score),
            )
        )
        i = j

    return proposals


def propose_regions_scanned(
    page: fitz.Page,
    page_index0: int,
) -> list[RegionProposal]:
    """Single full-page region for vision (v1)."""
    r = page.rect
    return [
        RegionProposal(
            page_num1=page_index0 + 1,
            page_index0=page_index0,
            region_id=f"p{page_index0+1:03d}_r001",
            bbox=(r.x0, r.y0, r.x1, r.y1),
            proposal_method="full_page_scanned",
            proposal_score=1.0,
        )
    ]


def propose_regions_for_page(
    page: fitz.Page,
    triage: PageTriageResult,
) -> list[RegionProposal]:
    if triage.page_kind == PageKind.scanned:
        return propose_regions_scanned(page, triage.page_index0)
    return propose_regions_text_rich(page, triage.page_index0)

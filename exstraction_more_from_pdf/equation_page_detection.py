"""Recall-first candidate page detection (scored ensemble)."""

from __future__ import annotations

import re

import fitz

from equation_schema import PageCandidate


_MATH_SYMBOLS = re.compile(
    r"[∫∑∏√∂∇∞±×÷∈∉≤≥≠≈∼∀∃αβγδεζηθλμνπρστφχψωΓΔΘΛΞΠΣΦΨΩ]"
)
_EQ_NUM = re.compile(
    r"\(\s*\d+(?:\.\d+)*\s*\)|\[\s*\d+(?:\.\d+)*\s*\]|(?:^|\s)Eq\.?\s*\(?\s*\d",
    re.IGNORECASE,
)
_MATH_WORDS = re.compile(
    r"\b(equation|ligning|subject\s+to|s\.t\.|minimize|maximize|arg\s*min|arg\s*max|"
    r"stochastic|differential|matrix|constraint|theorem|proof|lemma|corollary)\b",
    re.IGNORECASE,
)


def _score_page_text(text: str) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = 0.0
    t = text
    n_eq = t.count("=")
    if n_eq:
        score += min(4.0, n_eq * 0.45)
        reasons.append(f"equals={n_eq}")
    sym_count = len(_MATH_SYMBOLS.findall(t))
    if sym_count:
        score += min(5.0, sym_count * 0.8)
        reasons.append(f"unicode_math_syms≈{sym_count}")
    if _EQ_NUM.search(t):
        score += 2.5
        reasons.append("equation_number_pattern")
    if _MATH_WORDS.search(t):
        score += 1.5
        reasons.append("math_keyword")
    # LaTeX-like escapes sometimes appear in text layer
    if "\\int" in t or "\\sum" in t or "\\frac" in t:
        score += 2.0
        reasons.append("latex_escape")
    words = max(1, len(t.split()))
    sym_ratio = (n_eq + sym_count) / max(words, 1)
    if sym_ratio > 0.08:
        score += 1.5
        reasons.append("high_symbol_ratio")
    return score, reasons


def detect_candidate_pages(
    doc: fitz.Document,
    *,
    min_score: float = 1.25,
    max_pages: int | None = None,
) -> list[PageCandidate]:
    """
    Return pages sorted by descending score. Recall-oriented: low min_score.
    """
    out: list[PageCandidate] = []
    n = len(doc) if max_pages is None else min(len(doc), max_pages)
    for i in range(n):
        page = doc[i]
        text = page.get_text("text") or ""
        score, reasons = _score_page_text(text)
        if score >= min_score:
            out.append(
                PageCandidate(
                    page_num1=i + 1,
                    page_index0=i,
                    score=score,
                    reasons=reasons,
                )
            )
    out.sort(key=lambda c: c.score, reverse=True)
    return out

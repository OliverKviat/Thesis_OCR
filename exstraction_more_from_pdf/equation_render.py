"""Render page / region crops to PNG bytes and content hashes."""

from __future__ import annotations

import hashlib
from typing import Tuple

import fitz


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def render_page_png(
    page: fitz.Page,
    dpi: int = 220,
) -> Tuple[bytes, str]:
    """Full page raster (fallback / scanned)."""
    mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    data = pix.tobytes("png")
    return data, sha256_bytes(data)


def render_region_png(
    page: fitz.Page,
    bbox: tuple[float, float, float, float],
    *,
    dpi: int = 300,
    padding: float = 3.0,
) -> Tuple[bytes, str]:
    """Crop region with padding; PNG output."""
    x0, y0, x1, y1 = bbox
    rect = fitz.Rect(x0 - padding, y0 - padding, x1 + padding, y1 + padding) & page.rect
    mat = fitz.Matrix(dpi / 72.0, dpi / 72.0)
    pix = page.get_pixmap(matrix=mat, clip=rect, alpha=False)
    data = pix.tobytes("png")
    return data, sha256_bytes(data)

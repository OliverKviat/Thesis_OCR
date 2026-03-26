"""v1 schema for thesis equation extraction (strict, versioned)."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Literal

# --- Pipeline versions (bump when behavior/schema changes) ---
PIPELINE_VERSION = "pipe_v2_text_only"
DETECTOR_VERSION = "det_v1"
PROPOSAL_VERSION = "prop_v1"
EXTRACTOR_TEXT_VERSION = "text_v1"


class PageKind(str, Enum):
    text_rich = "text_rich"
    scanned = "scanned"
    mixed = "mixed"


class DisplayType(str, Enum):
    display = "display"
    inline = "inline"
    unknown = "unknown"


class ContentType(str, Enum):
    equation = "equation"
    optimization_problem = "optimization_problem"
    constraint_set = "constraint_set"
    definition = "definition"
    identity = "identity"
    expression = "expression"
    chemistry = "chemistry"
    unknown = "unknown"


class MathFamily(str, Enum):
    algebraic = "algebraic"
    matrix = "matrix"
    ode = "ode"
    pde = "pde"
    regression = "regression"
    probabilistic = "probabilistic"
    optimization = "optimization"
    unknown = "unknown"


class SourceMode(str, Enum):
    text = "text"


class VerificationStatus(str, Enum):
    passed = "passed"
    failed = "failed"
    skipped = "skipped"


def normalize_equation_label(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    m = re.search(r"\(?\s*(\d+(?:\.\d+)*)\s*\)?", s)
    if m:
        return m.group(1)
    m2 = re.search(r"(\d+(?:\.\d+)*)", s)
    return m2.group(1) if m2 else None


@dataclass
class PageTriageResult:
    page_index0: int
    page_num1: int
    page_kind: PageKind
    text_char_count: int
    image_area_ratio: float
    has_text_layer: bool


@dataclass
class PageCandidate:
    page_num1: int
    page_index0: int
    score: float
    reasons: list[str]


@dataclass
class RegionProposal:
    page_num1: int
    page_index0: int
    region_id: str
    bbox: tuple[float, float, float, float]
    proposal_method: str
    proposal_score: float


@dataclass
class EquationRecord:
    blob_path: str
    pdf_sha256: str
    page_index0: int
    page_num1: int
    region_id: str
    bbox: tuple[float, float, float, float]
    proposal_score: float
    display_type: DisplayType
    content_type: ContentType
    math_family: MathFamily
    label_raw: str | None
    label_normalized: str | None
    nickname_raw: str | None
    latex_guess: str | None
    text_guess: str | None
    is_multiline: bool
    source_mode: SourceMode
    verification_status: VerificationStatus
    verification_flags: list[str] = field(default_factory=list)
    detector_version: str = DETECTOR_VERSION
    proposal_version: str = PROPOSAL_VERSION
    extractor_version: str = EXTRACTOR_TEXT_VERSION
    prompt_version: str | None = None
    page_image_hash: str | None = None
    crop_image_hash: str | None = None
    raw_model_response: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["display_type"] = self.display_type.value
        d["content_type"] = self.content_type.value
        d["math_family"] = self.math_family.value
        d["source_mode"] = self.source_mode.value
        d["verification_status"] = self.verification_status.value
        return d


@dataclass
class PdfRunManifest:
    pipeline_version: str
    blob_path: str
    pdf_sha256: str
    page_count: int
    detector_version: str
    proposal_version: str
    extractor_version: str
    status: Literal["complete", "failed"]
    error_summary: str | None
    candidate_pages: list[int]
    region_count: int
    equation_count: int
    triage_summary: dict[str, int]  # page_kind counts

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)

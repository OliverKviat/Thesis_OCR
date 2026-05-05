### ============= LOADS & IMPORTS ============= ###
import os
from pathlib import Path
import json
import re
    

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

### ============= SETTINGS ============= ###
def find_project_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "Data").exists():
            return candidate
    raise FileNotFoundError("Could not find project root containing Data/")

ROOT = find_project_root()


JSON_PATH = ROOT / "Data" / "gcp_order" / "helper_files" / "department_classification.json"

META_PATH = ROOT / "Data" / "gcp_order" / "dtu_findit" / "master_thesis_meta" / "thesis_meta_combined.parquet"
METRICS_PATH = ROOT / "Data" / "crawl2_files" / "extracted_metrics_unified.parquet"

EXPORT_PATH = ROOT / "Data" / "crawl2_files"
EXPORT_FILENAME = "thesis_meta_all_metrics_except_grade.parquet"

THRESHOLD = 0.35
EXPORT = True

### ============= Cosin-similarity for department matching ============= ###
STOP_PHRASES = [
    "technical university of denmark",
    "technical university",
    "dtu",
]

def norm_text(value) -> str:
    if pd.isna(value):
        return ""
    value = str(value).lower().strip()
    for phrase in STOP_PHRASES:
        value = value.replace(phrase, " ")
    value = re.sub(r"\b(?:department|faculty|institute|center|centre)\b", " ", value)
    value = re.sub(r"[^a-zæøå0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def flatten_values(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return value
    return [str(value)]


def _as_english_text(value):
    if isinstance(value, dict):
        for key in ("en", "eng", "english"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
            if isinstance(candidate, list) and candidate:
                first = next((item for item in candidate if isinstance(item, str) and item.strip()), None)
                if first:
                    return first.strip()
        for candidate in value.values():
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return ""
    if isinstance(value, list):
        first = next((item for item in value if isinstance(item, str) and item.strip()), None)
        return first.strip() if first else ""
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def load_departments(json_path: Path) -> pd.DataFrame:
    """Load the English department label and English aliases from JSON."""
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    rows = []
    for item in data:
        department_en = _as_english_text(item.get("department"))
        title_en = _as_english_text(item.get("title"))
        sections_en = item.get("sections") or []
        canonical_department = department_en or title_en
        if canonical_department:
            rows.append({"department_en": canonical_department, "alias": canonical_department})
        if title_en:
            rows.append({"department_en": canonical_department, "alias": title_en})
        if isinstance(sections_en, dict):
            sections_en = sections_en.get("en") or []
        for section in sections_en:
            section_en = _as_english_text(section)
            if section_en:
                rows.append({"department_en": canonical_department, "alias": section_en})
    df = pd.DataFrame(rows)
    df["alias_norm"] = df["alias"].astype(str).map(norm_text)
    df = df[df["alias_norm"].str.len() > 0].drop_duplicates(subset=["department_en", "alias_norm"])
    return df.reset_index(drop=True)

alias_df = load_departments(JSON_PATH)
df_meta = pd.read_parquet(META_PATH)

# Build TF-IDF vectorizer on normalized aliases
vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5))
alias_texts = alias_df["alias_norm"].fillna("").tolist()
alias_vectors = vectorizer.fit_transform(alias_texts)


def score_text(text: str):
    """Score text against department aliases, return (department_en, score, alias_matched)."""
    q = norm_text(text)
    if not q:
        return None, 0.0, None
    qv = vectorizer.transform([q])
    sims = (qv @ alias_vectors.T).toarray().ravel()
    best_idx = int(np.argmax(sims))
    best_score = float(sims[best_idx]) if sims.size else 0.0
    best_alias = alias_df.iloc[best_idx]["alias"] if best_score > 0 else None
    best_dept = alias_df.iloc[best_idx]["department_en"] if best_score > 0 else None
    return best_dept, best_score, best_alias


def match_affiliations(text: str):
    """Match affiliations by splitting on | and scoring each segment."""
    if not isinstance(text, str) or not text:
        return None, 0.0, None
    parts = [p.strip() for p in re.split(r"\s*\|\s*", text) if p.strip()]
    best = (None, 0.0, None)
    for p in parts:
        dept, score, alias = score_text(p)
        if score > best[1]:
            best = (dept, score, alias)
    return best

# Prepare metadata columns
df_meta["primary_member_id_s"] = df_meta["primary_member_id_s"].astype(str)

# Compute matches
publisher_match = []
aff_match = []

for idx, row in df_meta.iterrows():
    pub = str(row.get("Publisher", "")).strip() if pd.notna(row.get("Publisher")) else ""
    aff = str(row.get("Affiliations", "")).strip() if pd.notna(row.get("Affiliations")) else ""

    dept_p, score_p, alias_p = score_text(pub) if pub else (None, 0.0, None)
    dept_a, score_a, alias_a = match_affiliations(aff) if aff else (None, 0.0, None)

    # Publisher first, fallback to affiliations
    if dept_p and score_p >= THRESHOLD:
        publisher_match.append((dept_p, score_p, alias_p, pub))
    else:
        publisher_match.append((None, 0.0, None, pub))

    aff_match.append((dept_a, score_a, alias_a, aff))

# Expand into columns
df_meta["Department_new"] = df_meta.apply(
    lambda r: publisher_match[r.name][0] if publisher_match[r.name][0] else aff_match[r.name][0],
    axis=1
)
df_meta["department_match_score"] = df_meta.apply(
    lambda r: publisher_match[r.name][1] if publisher_match[r.name][1] >= THRESHOLD else aff_match[r.name][1],
    axis=1
)
df_meta["department_match_alias"] = df_meta.apply(
    lambda r: publisher_match[r.name][2] if publisher_match[r.name][1] >= THRESHOLD else aff_match[r.name][2],
    axis=1
)

result = df_meta[["primary_member_id_s", "Department_new", "department_match_score", "department_match_alias"]].copy()

print(f"Rows: {len(df_meta)}")
print(f"Matched: {result['Department_new'].notna().sum()}")
print("\nLow-confidence matches:")
print(result[result['department_match_score'] < THRESHOLD * 2].head(10))

# author count column
# each auther is separated by a ";", counting the ";" in the string, if there is 0, the author count is 1, if there is 1, the author count is 2, and so on
df_meta["num_authors"] = df_meta["Author"].apply(lambda x: str(x).count(";") + 1 if pd.notna(x) else 0)


### ============= xx ============= ###
# load metrics df from parquet
df_metrics = pd.read_parquet(METRICS_PATH)

# add new column called ID_metric with values from filename wihout the file exstention (.txt).
df_metrics["ID_metric"] = df_metrics["filename"].str.split(".txt").str[0]

# moving the column "ID_metric" to the front
cols = df_metrics.columns.tolist()
cols.insert(1, cols.pop(cols.index("ID_metric")))
df_metrics = df_metrics[cols]

# drop columns
drop_col = ["match_trigger", "corrupt_cid", "linguistics_backend"]
df_metrics = df_metrics.drop(columns=drop_col, errors="ignore")

# drop overlapping metadata columns from metrics so the merge keeps the df_meta version only
overlap_cols = ["Author", "abstract_ts", "num_authors", "Publication Year", "primary_member_id_s", "Title", "Department_new"]
df_metrics = df_metrics.drop(columns=overlap_cols, errors="ignore")

### ============= Join and Export ============= ###
relevant_meta_columns = ["abstract_ts", "Author", "num_authors", "Publication Year", "primary_member_id_s", "Title", "Department_new", "ID"]

# Ensure matching data types for merge keys
df_meta["ID"] = df_meta["ID"].astype(str)

# merging the rinsed metadata and metrics dataframes on member_id_ss (df_meta_rinsed) and 'member_id_ss_metrics' (master_thesis_metrics_analysis) for collumns in relevant_meta_columns in df_meta_rinsed
master_thesis_metrics_analysis = pd.merge(
    df_metrics,
    df_meta[relevant_meta_columns],
    left_on="ID_metric",
    right_on="ID",
    how="inner",
)

master_thesis_metrics_analysis = master_thesis_metrics_analysis.drop(columns=["ID_metric"], errors="ignore")

#export to parquet in location: Data/master_thesis_metrics_analysis.parquet
if EXPORT:
    export_path = EXPORT_PATH / EXPORT_FILENAME 
    master_thesis_metrics_analysis.to_parquet(export_path, index=False)
    print(f"Exported enriched dataset to: {export_path}")
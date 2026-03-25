# ==== IMPORTS ====
import re
import pypdf
import pandas as pd
import spacy

from pypdf import PdfReader
from openai import OpenAI
from pathlib import Path

from typing import Dict, List


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent



# ==== SYSTEM INSTRUCTION ====
system_instruction = "**Role:** Academic Metadata Extractor\n**Task:** Identify Thesis Supervisor(s) from first few pages of MSc.\n**Handling:** Ignore PDF artifacts like \"S u p e r v i s o r\" or \"D r . J o h n\". Treat spaced-out letters as single words.\n**Instructions:**\n1. Extract the full names of individuals labeled as \"Supervisor\", \"Supervisors\", \"Advisor\", \"Advisors\", or \"Vejleder\".\n2. Exclude academic titles (e.g., Professor, Doctor, Dr.) or associations (eg., Head of, Director, Professor) if present.\n3. Output ONLY the names as a comma-separated list.\n5. **Constraint:** No preamble, no conversational filler, no explanations."

# ==== FUNCTIONS ====
def extract_and_clean_text(pdf_path, max_pages=3):
    reader = PdfReader(pdf_path)
    raw_text = ""
    
    # Extract only the first few pages (where supervisors are listed)
    for i in range(min(len(reader.pages), max_pages)):
        raw_text += reader.pages[i].extract_text() + "\n"
    
    # REGEX: Fixes "S u p e r v i s o r" -> "Supervisor" 
    # This looks for single characters separated by spaces and joins them
    cleaned = re.sub(r'(?<=\b\w) (?=\w\b)', '', raw_text)
    
    # Remove excessive newlines and tabs for a cleaner prompt string
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned

def send_to_model(text_for_ai):
    client = OpenAI(
        base_url = "http://localhost:5272/v1/",
        api_key = "unused", # required for the API but not used
    )

    response = client.chat.completions.create(
        messages = [
            {
                "role": "system",
                "content": system_instruction,
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": text_for_ai
                    },
                ],
            },
        ],
        model = "qwen2.5-1.5b-instruct-generic-cpu:4",
    )

    answer = response.choices[0].message.content.strip()
    return answer

def extract_supervisors(pdf_path, max_pages=5):
    with open(pdf_path, 'rb') as file:
        try:
            reader = pypdf.PdfReader(file)
        except Exception as e:
            print(f"Error reading PDF file: {e}")

    text_for_ai = extract_and_clean_text(pdf_path, max_pages)

    supervisor = send_to_model(text_for_ai)
    
    return supervisor

# ==== CHOOSE FILES TO PROCESS ====
folder_path = PROJECT_ROOT / "Data" / "RAW_test"
pdf_files = sorted(folder_path.glob("*.pdf"))

if not pdf_files:
    raise FileNotFoundError(f"No PDF files found in: {folder_path.resolve()}")

total_files = len(pdf_files)
print(f"Found {total_files} PDF file(s) in {folder_path.resolve()}")

while True:
    user_input = input(
        f"How many files do you want to process? (1-{total_files}, or 'all'): "
    ).strip().lower()

    if user_input == "all":
        num_to_process = total_files
        break

    if user_input in {"q", "quit", "exit"}:
        raise SystemExit("Execution terminated by user. No files were processed.")

    try:
        num_to_process = int(user_input)
    except ValueError:
        print("Invalid input. Enter a positive integer, 'all', or 'q' to quit.")
        continue

    if 1 <= num_to_process <= total_files:
        break

    print(f"Please enter a value between 1 and {total_files}, or 'all'.")

selected_pdf_files = pdf_files[:num_to_process]
print(f"Selected {len(selected_pdf_files)} file(s) for processing.")

# ==== PROCESS FILES ====
list_of_supervisors = []

for current_file_number, pdf_path in enumerate(selected_pdf_files, start=1):
    print(f"\n=== Processing file {current_file_number} of {len(selected_pdf_files)} ===")
    print(f"Current file is: {pdf_path.name}")

    supervisor = extract_supervisors(str(pdf_path), max_pages=2)
    print(f"Extracted supervisor(s): {supervisor}")

    list_of_supervisors.append({
        "file": pdf_path.name,
        "supervisor(s)": supervisor,
    })

# ==== AUTHOR VALIDATION: REMOVE AUTHOR FROM SUPERVISOR CANDIDATES ====
meta_path = PROJECT_ROOT / "Data" / "gcp_order" / "dtu_findit" / "extraction_and_processing" / "master_thesis_metrics_analysis.csv"
meta_df = pd.read_csv(meta_path, sep=";", dtype=str, low_memory=False).fillna("")

# Build id -> author lookup from metadata.
author_lookup = {
    row["member_id_ss"].strip(): row["Author"].strip()
    for _, row in meta_df.iterrows()
    if row.get("member_id_ss", "").strip()
}

try:
    nlp_validation = spacy.load("en_core_web_sm")
    print("spaCy model loaded successfully for NLP validation.")
except Exception:
    nlp_validation = None
    print("spaCy model could not be loaded. NLP validation will be skipped.")

NAME_REGEX = re.compile(r"\b[A-ZÆØÅ][a-zæøåA-ZÆØÅ\-']+(?:\s+[A-ZÆØÅ][a-zæøåA-ZÆØÅ\-']+){1,4}\b")

# Institutional exclusion words to split on before NLP.
INSTITUTIONAL_EXCLUSIONS = {
    "dtu",
    "technical university of denmark",
    "university",
    "department",
    "engineering",
    "institute",
    "college",
    "faculty",
    "school",
    "renewable energy",
    "energy",
    "master thesis",
    "thesis",
    "siemens gamesa",
}


def strip_leading_labels_and_titles(text: str) -> str:
    value = text
    value = re.sub(
        r"(?i)^\s*(?:supervisors?|main\s+supervisor|co-?supervisor|advisors?|advisers?|vejledere?|supervised by)\s*[:\-]?\s*",
        "",
        value,
    )
    value = re.sub(
        r"(?i)\b(?:associate\s+professor|assistant\s+professor|professor|prof\.?|ph\.?d\.?|msc|bsc|dr\.?)\b",
        "",
        value,
    )
    value = re.sub(r"\s+", " ", value)
    return value.strip(" ,.-:;\t")


def clean_candidate_name(candidate: str) -> str:
    candidate = strip_leading_labels_and_titles(candidate)
    candidate = re.sub(r"\s+", " ", candidate)
    return candidate.strip(" ,.-:;\t")


def canonicalize_name(name: str) -> str:
    lowered = name.lower().strip()
    lowered = re.sub(r"[^a-zæøå\s\-']", "", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def person_tokens(name: str) -> List[str]:
    canon = canonicalize_name(name).replace("-", " ")
    return [tok for tok in canon.split() if tok]


def _token_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if len(a) == 1 and b.startswith(a):
        return True
    if len(b) == 1 and a.startswith(b):
        return True
    return False


def names_match(candidate: str, author: str) -> bool:
    c = canonicalize_name(candidate)
    a = canonicalize_name(author)

    if not c or not a:
        return False
    if c == a:
        return True

    ctoks = person_tokens(candidate)
    atoks = person_tokens(author)
    if len(ctoks) < 2 or len(atoks) < 2:
        return False

    # Strong requirement: surname match.
    if not _token_match(ctoks[-1], atoks[-1]):
        return False

    overlap = 0
    for ct in ctoks:
        if any(_token_match(ct, at) for at in atoks):
            overlap += 1

    overlap_ratio = overlap / max(len(ctoks), len(atoks))
    return overlap_ratio >= 0.6


def split_author_names(author_value: str) -> List[str]:
    names: List[str] = []
    for part in [p.strip() for p in str(author_value).split("|") if p.strip()]:
        cleaned = clean_candidate_name(part)
        if not cleaned:
            continue

        names.append(cleaned)

        if "," in cleaned:
            pieces = [x.strip() for x in cleaned.split(",") if x.strip()]
            if len(pieces) >= 2:
                names.append(clean_candidate_name(" ".join(pieces[1:] + [pieces[0]])))

    # Deduplicate by canonicalized form.
    out: List[str] = []
    seen = set()
    for name in names:
        key = canonicalize_name(name)
        if key and key not in seen:
            seen.add(key)
            out.append(name)
    return out


def split_on_institutional_words(text: str) -> List[str]:
    """Split text on institutional exclusion words and return clean fragments."""
    result = [text]
    for excl_word in sorted(INSTITUTIONAL_EXCLUSIONS, key=len, reverse=True):
        new_result = []
        for fragment in result:
            parts = re.split(re.escape(excl_word), fragment, flags=re.IGNORECASE)
            new_result.extend([p.strip() for p in parts if p.strip()])
        result = new_result
    return [p for p in result if p]


def extract_supervisor_candidates(raw_value: str) -> List[str]:
    text = clean_candidate_name(str(raw_value))
    if not text:
        return []

    candidates: List[str] = []

    # Split on institutional words first to isolate name fragments.
    fragments = split_on_institutional_words(text)

    if nlp_validation is not None:
        # Apply NLP to each fragment separately for more accurate PERSON detection.
        for fragment in fragments:
            doc = nlp_validation(fragment)
            for ent in doc.ents:
                if ent.label_ == "PERSON":
                    cand = clean_candidate_name(ent.text)
                    if cand:
                        candidates.append(cand)

    # Also apply regex matching to all fragments.
    for fragment in fragments:
        for match in NAME_REGEX.finditer(fragment):
            cand = clean_candidate_name(match.group(0))
            if cand:
                candidates.append(cand)

    # Fallback split in case NLP/regex miss mixed formatting.
    if not candidates:
        rough_parts = re.split(r"\s*(?:\||;| and | & )\s*", text, flags=re.IGNORECASE)
        candidates = [clean_candidate_name(p) for p in rough_parts if clean_candidate_name(p)]

    out: List[str] = []
    seen = set()
    for cand in candidates:
        key = canonicalize_name(cand)
        if key and key not in seen:
            seen.add(key)
            out.append(cand)
    return out


def extract_member_id_from_filename(file_name: str) -> str:
    file_str = str(file_name)

    # Most files use 24-char object-id style before underscore.
    m = re.search(r"([0-9a-f]{24})(?=_)", file_str, flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()

    # Fallback to token before first underscore.
    token = file_str.split("_", 1)[0].strip()
    return token.lower()


filtered_list_of_supervisors: List[Dict[str, str]] = []
removed_pairs = 0

for entry in list_of_supervisors:
    file_name = str(entry.get("file", ""))
    raw_supervisors = str(entry.get("supervisor(s)", ""))

    member_id = extract_member_id_from_filename(file_name)
    author_value = author_lookup.get(member_id, "")
    author_names = split_author_names(author_value)

    supervisor_candidates = extract_supervisor_candidates(raw_supervisors)

    kept_candidates: List[str] = []
    for candidate in supervisor_candidates:
        is_author = any(names_match(candidate, author_name) for author_name in author_names)
        if is_author:
            removed_pairs += 1
        else:
            kept_candidates.append(candidate)

    filtered_list_of_supervisors.append({
        "file": file_name,
        "supervisor(s)": ", ".join(kept_candidates),
    })

list_of_supervisors = filtered_list_of_supervisors
print(f"Author-validation removed {removed_pairs} candidate(s).")

# ==== POST-FILTER: DROP ONE-WORD SUPERVISOR CANDIDATES ====
filtered_again: List[Dict[str, str]] = []
removed_one_word = 0

for entry in list_of_supervisors:
    file_name = str(entry.get("file", ""))
    raw_supervisors = str(entry.get("supervisor(s)", ""))

    raw_parts = [p.strip() for p in re.split(r"\s*,\s*", raw_supervisors) if p.strip()]
    kept: List[str] = []
    seen = set()

    for part in raw_parts:
        cand = clean_candidate_name(part)
        if not cand:
            continue

        # Enforce at least first + last name (>= 2 words).
        if len([tok for tok in cand.split() if tok]) < 2:
            removed_one_word += 1
            continue

        key = canonicalize_name(cand)
        if key and key not in seen:
            seen.add(key)
            kept.append(cand)

    filtered_again.append({
        "file": file_name,
        "supervisor(s)": ", ".join(kept),
    })

list_of_supervisors = filtered_again
print(f"Removed {removed_one_word} one-word candidate(s).")


# ==== LOAD ORIGINAL METRICS DATAFRAME FOR MERGE ====
data_file = PROJECT_ROOT / "Data" / "gcp_order" / "dtu_findit" / "extraction_and_processing" / "master_thesis_metrics_analysis.csv"
df = pd.read_csv(data_file,  sep=";", encoding="utf-8")

# ==== MERGE SUPERVISOR DATA WITH METRICS ====
# Convert list_of_supervisors to DataFrame
supervisors_df = pd.DataFrame(list_of_supervisors)

# Merge with master_thesis_metrics_analysis matching 'file' with 'pdf_file'
master_thesis_metrics_analysis = df.merge(
    supervisors_df,
    left_on='pdf_file',
    right_on='file',
    how='inner'
)

# drop the collumn "file"
master_thesis_metrics_analysis = master_thesis_metrics_analysis.drop(columns=['file'])

print(f"Final merged DataFrame has {len(master_thesis_metrics_analysis)} entries with extracted supervisors.")

print(master_thesis_metrics_analysis)
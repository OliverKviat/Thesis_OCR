#!/usr/bin/env python3
"""Extract key sections from PDF and save to CSV."""
import argparse
import csv
import os
import re
from pathlib import Path
from pypdf import PdfReader
from extract_toc import (
    extract_title_from_filename,
    extract_from_text,
    truncate_at_appendix,
)

# Fixed section order for output
OUTPUT_SECTIONS = [
    "Introduction",
    "Methods",
    "Results",
    "Discussion",
    "Conclusion",
]

# Section name variations (without numbering)
SECTION_KEYWORDS = {
    "Introduction": [
        "introduction",
        "background",
    ],
    "Methods": [
        "methods",
        "methodology",
        "material",
        "materials and methods",
    ],
    "Results": [
        "results",
        "findings",
    ],
    "Discussion": [
        "discussion",
        "interpretation",
    ],
    "Conclusion": [
        "conclusion",
        "conclusions",
        "concluding remarks",
    ],
}


def strip_leading_number(title: str) -> str:
    """Remove leading numbers and punctuation from title.
    E.g., '1 Introduction' -> 'introduction', '3.2.1 Results' -> 'results'
    """
    # Remove leading digits, dots, spaces
    cleaned = re.sub(r"^[\d\.\s]+", "", title).strip()
    return cleaned


def normalize_title(title: str) -> str:
    """Normalize title to lowercase and strip numbers for matching."""
    return strip_leading_number(title).lower().strip()


def is_main_section(title: str) -> bool:
    """Check if title is a main section (single digit followed by space).
    E.g., '1 Introduction', '2 Methods', but NOT '2.1 Results' or '3.1.1 Discussion'
    """
    return bool(re.match(r"^\d[\s]", title.strip()))


def find_section_pages(toc_entries):
    """
    Find page ranges for key sections using ONLY main numbered sections.
    Returns dict: {section_name: (start_page, end_page, title, next_title)}
    Page range extends from section start to just before the NEXT MAIN section.
    """
    # Filter to only main sections
    main_sections = [(t, p, i) for i, (t, p) in enumerate(toc_entries) if is_main_section(t)]
    
    found_sections = {}
    for section, keywords in SECTION_KEYWORDS.items():
        for j, (orig_title, page, orig_idx) in enumerate(main_sections):
            norm_title = normalize_title(orig_title)
            if any(kw in norm_title for kw in keywords):
                # Found a main section matching this keyword
                # End page is determined by the NEXT MAIN section's page
                if j + 1 < len(main_sections):
                    end_page = main_sections[j + 1][1] - 1
                    next_title = main_sections[j + 1][0]
                else:
                    end_page = None  # until EOF
                    next_title = None

                found_sections[section] = (page, end_page, orig_title, next_title)
                break

    return found_sections


def extract_section_text(path, page_start, page_end, section_title, next_section_title=None):
    """Extract text from page range, trimming by section titles.
    
    Args:
        path: PDF file path
        page_start: Start page number (1-indexed)
        page_end: End page number (1-indexed)
        section_title: The section heading to search for (to trim before)
        next_section_title: The next section heading to search for (to trim after)
    """
    reader = PdfReader(path)
    num_pages = len(reader.pages)
    
    # Convert to 0-indexed
    start_idx = max(0, page_start - 1)
    end_idx = min(num_pages - 1, page_end - 1) if page_end else num_pages - 1

    texts = []
    for i in range(start_idx, end_idx + 1):
        try:
            txt = reader.pages[i].extract_text() or ""
        except Exception:
            txt = ""
        texts.append(txt)

    combined = "\n\n".join(texts)
    
    # Trim from the start: find where section_title appears and extract from there
    if section_title:
        # Search for the section title (case-insensitive)
        pattern = re.escape(section_title)
        match = re.search(pattern, combined, re.IGNORECASE)
        if match:
            combined = combined[match.start():]
    
    # Trim from the end: find where next_section_title appears and stop before it
    if next_section_title:
        pattern = re.escape(next_section_title)
        match = re.search(pattern, combined, re.IGNORECASE)
        if match:
            combined = combined[:match.start()]
    
    return combined.strip()


def process_pdf(path):
    """Process a single PDF and return section data."""
    filename = os.path.basename(path)
    title = extract_title_from_filename(filename)

    # Extract TOC
    toc_entries = extract_from_text(path, max_pages=15)
    toc_entries = truncate_at_appendix(toc_entries)

    if not toc_entries:
        print(f"  {filename}: No TOC found")
        return None

    # Find section pages
    sections_found = find_section_pages(toc_entries)

    # Extract section text (initialize all sections as empty)
    section_data = {"Title": title}
    for section in OUTPUT_SECTIONS:
        section_data[section] = ""

    # Fill in found sections
    for section, (start_page, end_page, section_title, next_title) in sections_found.items():
        try:
            text = extract_section_text(path, start_page, end_page, section_title, next_title)
            section_data[section] = text
        except Exception as e:
            print(f"  {section}: ERROR - {e}")

    return section_data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", "-f", help="Single PDF file to scan")
    ap.add_argument("--folder", "-d", help="Folder containing PDFs to scan")
    ap.add_argument(
        "--output", "-o", default="Data/Processed/sections.csv", help="Output CSV path"
    )
    args = ap.parse_args()

    pdf_files = []
    if args.file:
        pdf_files = [args.file]
    elif args.folder:
        pdf_files = sorted(Path(args.folder).glob("*.pdf"))
    else:
        ap.error("Please provide either --file or --folder")

    if not pdf_files:
        print("No PDF files found.")
        return

    # Process all PDFs
    all_data = []
    for pdf_path in pdf_files:
        print(f"Processing: {os.path.basename(pdf_path)}")
        section_data = process_pdf(str(pdf_path))
        if section_data:
            all_data.append(section_data)

    if not all_data:
        print("No data extracted.")
        return

    # Write to CSV
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = ["Title"] + OUTPUT_SECTIONS
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in all_data:
            writer.writerow(row)

    print(f"\nSaved {len(all_data)} documents to {output_path}")


if __name__ == "__main__":
    main()

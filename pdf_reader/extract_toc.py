#!/usr/bin/env python3
"""Extract table of contents (TOC) from a PDF using pypdf outlines or text heuristic."""
import argparse
import os
import re
from pypdf import PdfReader


def flatten_outlines(outlines, reader, entries=None):
    if entries is None:
        entries = []
    for o in outlines:
        if isinstance(o, list):
            flatten_outlines(o, reader, entries)
        else:
            # o is a Destination-like object
            title = getattr(o, "title", str(o))
            try:
                page_num = reader.get_destination_page_number(o) + 1
            except Exception:
                # fallback: try to access page attribute
                page_num = getattr(getattr(o, "page", None), "idnum", None)
                if page_num is None:
                    page_num = None
            entries.append((title, page_num))
    return entries


def extract_from_outlines(path):
    reader = PdfReader(path)
    outlines = []
    try:
        outlines = reader.outlines
    except Exception:
        outlines = []
    if outlines:
        entries = flatten_outlines(outlines, reader)
        return entries
    return []


def extract_from_text(path, max_pages=15):
    reader = PdfReader(path)
    num_pages = len(reader.pages)
    pages_to_scan = min(num_pages, max_pages)

    # collect text from first pages
    texts = []
    for i in range(pages_to_scan):
        try:
            txt = reader.pages[i].extract_text() or ""
        except Exception:
            txt = ""
        texts.append(txt)

    combined = "\n\n".join(texts)
    # find 'contents' heading
    m = re.search(r"^\s*contents\b", combined, flags=re.I | re.M)
    if not m:
        # try approximate: first page occurrences
        start_idx = 0
    else:
        start_idx = m.start()

    # take substring from heading (or from start) up to some length
    snippet = combined[start_idx:start_idx + 20000]
    lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]

    toc_candidates = []
    # basic heuristic: lines that end with a page number
    for ln in lines:
        # common patterns: "1. Introduction ........ 1" or "1 Introduction 1"
        m = re.match(r"(?P<title>.+?)\s+(\.{2,}|\s+)\s*(?P<page>\d{1,4})$", ln)
        if not m:
            m = re.match(r"(?P<title>.+?)\s+(?P<page>\d{1,4})$", ln)
        if m:
            title = m.group("title").strip().rstrip('.')
            page = int(m.group("page"))
            toc_candidates.append((title, page))

    return toc_candidates


def extract_title_from_filename(filename: str) -> str:
    """
    Extract English title from filename.
    Removes file ID prefix and translation (everything from " (translated " onward).
    """
    # Remove the PDF extension
    name_without_ext = filename.rsplit('.pdf', 1)[0]
    
    # Remove the ID prefix (everything before the first underscore and the underscore itself)
    if '_' in name_without_ext:
        name_without_id = name_without_ext.split('_', 1)[1]
    else:
        name_without_id = name_without_ext
    
    # Remove everything from " (translated " onward
    if ' (translated ' in name_without_id:
        title = name_without_id.split(' (translated ', 1)[0]
    else:
        title = name_without_id
    
    return title.strip()


def is_appendix_title(title: str) -> bool:
    """Return True if the title looks like an appendix heading."""
    if not title:
        return False
    t = title.strip()
    # explicit keyword
    if "appendix" in t.lower():
        return True
    # single-letter section like 'A' or 'A.' or 'A Appendix A' or 'A.1'
    if re.match(r"^[A-Z](?:\.|\d|\s)", t):
        return True
    return False


def truncate_at_appendix(entries):
    """Return entries up to (but not including) the first appendix-looking title."""
    for i, (title, page) in enumerate(entries):
        if is_appendix_title(title):
            return entries[:i]
    return entries


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", "-f", required=True, help="PDF file to scan")
    args = ap.parse_args()

    path = args.file
    # print title (extracted from filename) as first element
    title = extract_title_from_filename(os.path.basename(path))
    print(f"Title: {title}")

    outlines = []
    try:
        outlines = extract_from_outlines(path)
    except Exception as e:
        outlines = []

    if outlines:
        outlines = truncate_at_appendix(outlines)
        print("Found outlines/bookmarks (TOC-like entries):")
        for title, p in outlines:
            print(f"- {title} -> page {p}")
        return

    print("No outlines found; falling back to text-based TOC search...")
    text_toc = extract_from_text(path)
    if text_toc:
        text_toc = truncate_at_appendix(text_toc)
        print("Found TOC-like lines from text heuristic:")
        for title, p in text_toc:
            print(f"- {title} -> page {p}")
    else:
        print("No TOC entries found by heuristic.")


if __name__ == "__main__":
    main()

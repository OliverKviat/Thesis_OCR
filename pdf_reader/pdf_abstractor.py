#!/usr/bin/env python3
"""
PDF Reader and Abstract Extractor

Extracts abstract from academic PDF papers.
Supports batch processing to CSV for Excel export.
Supports batch processing for JSON export for LLM training.
Supports CLI options for single file info and full content reading.
"""

import sys
from pathlib import Path
import pypdf
import re
import json
from typing import Dict, List, Optional
from extract_toc import extract_from_text

def is_toc_page(page_text: str) -> bool:
    """
    Detect if a page is a Table of Contents page.
    TOC pages typically have:
    - 'Contents' or 'Table of Contents' heading
    - Multiple lines with page numbers (format: "text ... number" or "text number")
    - Dense dot patterns connecting text to page numbers
    """
    # Look for TOC-like heading
    if re.search(r'^\s*(table\s+of\s+)?contents\b', page_text, re.IGNORECASE | re.MULTILINE):
        return True
    
    # Look for dense pattern of lines ending with page numbers (typical TOC pattern)
    lines = page_text.split('\n')
    lines_with_numbers = 0
    for line in lines:
        # Pattern: text followed by dots/spaces and page number
        if re.search(r'\s+(\.{2,}|\s+)\s*\d{1,4}\s*$', line) or re.search(r'.+\s+\d{1,4}\s*$', line):
            lines_with_numbers += 1
    
    # If >30% of lines look like TOC entries, it's likely a TOC page
    if len(lines) > 5 and lines_with_numbers / len(lines) > 0.3:
        return True
    
    return False


def extract_abstract_from_toc(pdf_path: str, reader: pypdf.PdfReader) -> tuple[int, int]:
    """
    Extract TOC to find where main content starts and where abstract is.
    Returns (first_main_section_page, search_end_page).
    search_end_page is where to stop searching for abstract.
    If not found, returns (-1, -1).
    """
    try:
        toc_entries = extract_from_text(pdf_path, max_pages=15)
        
        first_main_section_page = -1
        search_end_page = -1
        
        for title, page in toc_entries:
            # Look for the first numbered section
            if re.match(r'^\d\s', title):  # Single digit followed by space = main section
                first_main_section_page = page if page else -1
                # If first section is Abstract, we want to include it in search
                if first_main_section_page > 0 and 'abstract' in title.lower():
                    search_end_page = first_main_section_page + 1  # Include abstract page
                else:
                    # Abstract should be before this, stop search just before this section
                    search_end_page = first_main_section_page - 1 if first_main_section_page > 0 else -1
                break
        
        return (first_main_section_page, search_end_page)
    except Exception:
        return (-1, -1)


def search_section_by_keyword(reader: pypdf.PdfReader, keyword: str, max_pages: int = 10) -> str:
    """
    Search for a section with a specific keyword in the first N pages.
    Returns the section content if found, otherwise empty string.
    """
    search_end = min(max_pages, len(reader.pages))
    
    for i in range(search_end):
        page = reader.pages[i]
        page_text = page.extract_text().strip()
        
        # Skip if this looks like a TOC page
        if is_toc_page(page_text):
            continue
        
        keyword_lower = keyword.lower()
        
        # Look for page starting with keyword
        if re.match(rf'^\s*{re.escape(keyword)}\s*$', page_text[:100], re.IGNORECASE):
            content = re.sub(rf'^\s*{re.escape(keyword)}\s*', '', page_text, flags=re.IGNORECASE)
            return content.strip()
        
        # Look for numbered keyword like "1 Summary"
        elif re.match(rf'^\s*\d+\s+{re.escape(keyword)}\b', page_text, re.IGNORECASE):
            content = re.sub(rf'^\s*\d+\s+{re.escape(keyword)}\s*', '', page_text, flags=re.IGNORECASE)
            return content.strip()
        
        # Look for keyword with colon like "Summary:"
        elif re.match(rf'^\s*{re.escape(keyword)}:', page_text, re.IGNORECASE):
            match = re.search(rf'{re.escape(keyword)}\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
            if match:
                content = match.group(1).strip()
                content = re.sub(r'\s+', ' ', content)
                return content
        
        # Look for keyword appearing in page with reasonable length
        elif keyword_lower in page_text.lower() and len(page_text.split()) < 600:
            match = re.search(rf'{re.escape(keyword)}\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
            if match:
                content = match.group(1).strip()
                content = re.sub(r'\s+', ' ', content)
                return content
    
    return ""


def extract_abstract_from_pages(pdf_path: str) -> str:
    """
    Extract abstract from dedicated abstract page.
    Looks for 'Abstract' heading followed by content.
    Uses TOC to determine where to search and avoids extracting from TOC pages.
    If abstract not found, searches for alternative terms: summary, résumé, resume.
    """
    try:
        with open(pdf_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            
            # First, try to use TOC to find where main content starts
            first_main_section_page, search_end_page = extract_abstract_from_toc(pdf_path, reader)
            
            # Determine search range
            # Abstract is typically in the front matter (first ~10 pages) before main numbered sections
            if search_end_page > 0:
                # TOC gave us a clue - search from start up to first main section
                # Add buffer to account for document numbering differences (add 5 pages)
                search_start = 0
                search_end = min(search_end_page + 5, len(reader.pages))
            else:
                # If we can't find anything in TOC, search the first 20 pages
                search_start = 0
                search_end = min(20, len(reader.pages))
            
            for i in range(search_start, search_end):
                page = reader.pages[i]
                page_text = page.extract_text().strip()
                
                # Skip if this looks like a TOC page
                if is_toc_page(page_text):
                    continue
                
                # Look for pages that start with "Abstract" (case insensitive)
                if re.match(r'^\s*abstract\s*$', page_text[:50], re.IGNORECASE):
                    # This page likely contains only "Abstract" heading and the abstract
                    # Remove the "Abstract" heading and return the rest
                    abstract_text = re.sub(r'^\s*abstract\s*', '', page_text, flags=re.IGNORECASE)
                    return abstract_text.strip()
                
                # Alternative: look for "1 Abstract" or "Abstract:" pattern
                elif re.match(r'^\s*1\s+abstract\b', page_text, re.IGNORECASE):
                    # Handle numbered abstract section like "1 Abstract"
                    abstract_text = re.sub(r'^\s*1\s+abstract\s*', '', page_text, flags=re.IGNORECASE)
                    return abstract_text.strip()
                
                # Alternative: look for pages where "Abstract" appears and the page is relatively short
                elif ('abstract' in page_text.lower() and 
                      len(page_text.split()) < 500):  # Less than 500 words = likely abstract page
                    
                    # Extract text after "Abstract" heading
                    match = re.search(r'abstract\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
                    if match:
                        abstract_text = match.group(1).strip()
                        # Clean up common artifacts
                        abstract_text = re.sub(r'\s+', ' ', abstract_text)  # Multiple spaces to single
                        return abstract_text
            
            # If no abstract found, search for alternative keywords in first 10 pages (preface)
            alternative_keywords = [
                "abstract",
                "summary",
                "summary (english)",
                "resume",
                "resumé"
            ]
            
            for keyword in alternative_keywords:
                result = search_section_by_keyword(reader, keyword, max_pages=10)
                if result:
                    return result
            
            return "Abstract not found"
    
    except Exception as e:
        return f"Error extracting abstract: {str(e)}"


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


def process_single_pdf(pdf_path: str) -> Dict[str, str]:
    """
    Extract title and abstract from single PDF.
    Returns dict with title_filename and abstract.
    """
    filename = Path(pdf_path).name
    title_filename = extract_title_from_filename(filename)
        
    return {
        'filename': filename,
        'title_filename': title_filename,
        'abstract': extract_abstract_from_pages(pdf_path),
        'file_path': pdf_path
    }


def read_pdf(pdf_path: str, max_pages: int = None) -> str:
    """
    Read text from PDF file.
    max_pages: None for all, or specify number of pages to read.
    """
    try:
        with open(pdf_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            
            total_pages = len(reader.pages)
            pages_to_read = min(max_pages, total_pages) if max_pages else total_pages
            
            print(f"Reading PDF: {Path(pdf_path).name}")
            print(f"Total pages: {total_pages}")
            print(f"Reading pages: {pages_to_read}")
            print("=" * 50)
            
            text = ""
            for i in range(pages_to_read):
                page = reader.pages[i]
                page_text = page.extract_text()
                text += f"\n--- PAGE {i + 1} ---\n"
                text += page_text
                print(f"Processed page {i + 1}")
            
            return text
            
    except Exception as e:
        print(f"ERROR: Error reading PDF: {e}")
        return ""


def process_all_pdfs_to_csv(raw_data_dir: Path, output_file: str = "extracted_metadata.csv"):
    """
    Batch process all PDFs in directory and save to CSV.
    """
    processed_data_dir = Path("Data/Processed")
    pdf_files = list(raw_data_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {raw_data_dir}")
        return
    
    # Prepare CSV content
    csv_lines = []
    csv_lines.append("Filename,Title,Abstract")
    abstracts_found = 0
    
    print(f"Processing {len(pdf_files)} PDF files...")
    print("=" * 50)
    
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            print(f"[{i}/{len(pdf_files)}] Processing: {pdf_path.name}")
            
            result = process_single_pdf(str(pdf_path))
            
            # Clean data for CSV (escape quotes, remove newlines) 
            filename = result['filename']
            title_filename = result['title_filename'].replace('"', '""').replace('\n', ' ').replace('\r', ' ')
            abstract = result['abstract'].replace('"', '""').replace('\n', ' ').replace('\r', ' ')
            
            # Add to CSV (wrap in quotes to handle commas)
            csv_line = f'"{filename}","{title_filename}","{abstract}"'
            csv_lines.append(csv_line)
            
            # Count abstracts found
            if 'not found' not in abstract.lower():
                abstracts_found += 1
            
            print(f"   Title (filename): {title_filename[:50]}{'...' if len(title_filename) > 50 else ''}")
            print(f"   Abstract: {'Found' if 'not found' not in abstract.lower() else 'Not found'}")
            print()
            
        except Exception as e:
            print(f"   Error: {e}")
            csv_lines.append(f'"{pdf_path.name}","ERROR","ERROR"')
    
    # Write CSV file
    output_path = processed_data_dir / output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(csv_lines))
    
    print("=" * 50)
    print(f"Results saved to: {output_path}")
    print(f"Processed {len(pdf_files)} files")
    print(f"Abstracts found: {abstracts_found}/{len(pdf_files)}")


def process_all_pdfs_to_json(raw_data_dir: Path, output_file: str = "extracted_metadata.json"):
    """
    Batch process all PDFs in directory and save to JSON.
    Extracts the same data as CSV: filename, title, abstract.
    """
    processed_data_dir = Path("Data/Processed")
    pdf_files = list(raw_data_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {raw_data_dir}")
        return
    
    # Prepare JSON content as list of documents
    documents = []
    abstracts_found = 0
    
    print(f"Processing {len(pdf_files)} PDF files...")
    print("=" * 50)
    
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            print(f"[{i}/{len(pdf_files)}] Processing: {pdf_path.name}")
            
            result = process_single_pdf(str(pdf_path))
            
            # Add to JSON (keep full data without cleaning)
            document = {
                "filename": result['filename'],
                "title": result['title_filename'],
                "abstract": result['abstract']
            }
            documents.append(document)
            
            # Count abstracts found
            if 'not found' not in result['abstract'].lower():
                abstracts_found += 1
            
            print(f"   Title: {result['title_filename'][:50]}{'...' if len(result['title_filename']) > 50 else ''}")
            print(f"   Abstract: {'Found' if 'not found' not in result['abstract'].lower() else 'Not found'}")
            print()
            
        except Exception as e:
            print(f"   Error: {e}")
            error_document = {
                "filename": pdf_path.name,
                "title": "ERROR",
                "abstract": f"Error extracting abstract: {str(e)}"
            }
            documents.append(error_document)
    
    # Write JSON file
    output_path = processed_data_dir / output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(documents, f, indent=2, ensure_ascii=False)
    
    print("=" * 50)
    print(f"Results saved to: {output_path}")
    print(f"Processed {len(pdf_files)} files")
    print(f"Abstracts found: {abstracts_found}/{len(pdf_files)}")



def show_single_pdf_info(pdf_path: Path):
    """
    Display extracted metadata for a single PDF.
    """
    print(f"Analyzing: {pdf_path.name}")
    print("=" * 50)
    
    result = process_single_pdf(str(pdf_path))
    
    print(f"TITLE (from filename): {result['title_filename']}")
    print()
    print(f"ABSTRACT:")
    print(f"{result['abstract']}")
    print()
    print("=" * 50)


def main():
    """CLI entry point for PDF reading, abstract extraction and CSV and JSON export."""
    
    # Define the raw and processed data directories
    raw_data_dir = Path("Data/RAW_test")

    
    if not raw_data_dir.exists():
        print(f"ERROR: Directory not found: {raw_data_dir}")
        sys.exit(1)
    
    # Get all PDF files
    pdf_files = list(raw_data_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"ERROR: No PDF files found in {raw_data_dir}")
        sys.exit(1)
    
    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        # Export all PDFs metadata to CSV
        if command in ['--export', '--csv', '--excel']:
            process_all_pdfs_to_csv(raw_data_dir)
            return
        
        # Export all PDFs metadata to JSON
        elif command in ['--json']:
            process_all_pdfs_to_json(raw_data_dir)
            return
        
        # Extract metadata for single file
        elif command in ['--info', '--meta']:
            if len(sys.argv) < 3:
                print("ERROR: Please specify filename for --info")
                print("Usage: python pdf_reader/pdf_abstractor.py --info '<filename>'")
                sys.exit(1)
            
            filename = sys.argv[2]
            pdf_path = raw_data_dir / filename
            
            if not pdf_path.exists():
                print(f"ERROR: File not found: {filename}")
                print("Available files:")
                for pdf_file in pdf_files:
                    print(f"  - {pdf_file.name}")
                sys.exit(1)
            
            show_single_pdf_info(pdf_path)
            return
        
        # Original functionality - read PDF content
        else:
            filename = sys.argv[1]
            pdf_path = raw_data_dir / filename
            
            if not pdf_path.exists():
                print(f"ERROR: File not found: {filename}")
                print("Available files:")
                for pdf_file in pdf_files:
                    print(f"  - {pdf_file.name}")
                sys.exit(1)
            
            # Check for --pages option
            max_pages = None
            if len(sys.argv) > 2:
                pages_arg = sys.argv[2].lower()
                if pages_arg == "--first5" or pages_arg == "--5":
                    max_pages = 5
                elif pages_arg == "--full" or pages_arg == "--all":
                    max_pages = None
                else:
                    print(f"ERROR: Unknown option '{sys.argv[2]}'")
                    print("Valid options: --first5, --full")
                    sys.exit(1)
                        
            text = read_pdf(pdf_path, max_pages)
            if text:
                print("\n" + "=" * 50)
                print("EXTRACTED TEXT:")
                print("=" * 50)
                print(text)
    
    else:
        # Show available options and files
        print(f"Found {len(pdf_files)} PDF files")
        print("=" * 50)
        print("Available PDF files:")
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"  {i}. {pdf_file.name}")
        
        print("\nUsage Options:")
        print("  Read PDF content:")
        print(f"     python pdf_reader/pdf_abstractor.py '<filename>' [--first5|--full]")
        print("  Extract title and abstract from single PDF:")
        print(f"     python pdf_reader/pdf_abstractor.py --info '<filename>'")
        print("  Export all PDFs to CSV:")
        print(f"     python pdf_reader/pdf_abstractor.py --csv")
        print("  Export all PDFs to JSON (for LLM training):")
        print(f"     python pdf_reader/pdf_abstractor.py --json")

if __name__ == "__main__":
    main()
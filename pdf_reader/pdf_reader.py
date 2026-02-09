#!/usr/bin/env python3
"""
PDF Reader and Metadata Extractor

Extracts title (from filename), author, and abstract from academic PDF papers.
Supports batch processing to CSV for Excel export.
"""

import sys
from pathlib import Path
import pypdf
import re
from typing import Dict, List, Optional


def extract_simple_metadata(pdf_path: str) -> Dict[str, str]:
    """
    Extract basic metadata from PDF.
    """
    try:
        with open(pdf_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            metadata = reader.metadata or {}
            return {
                'title': metadata.get('/Title', '').strip(),
                'author': metadata.get('/Author', '').strip(),
                'subject': metadata.get('/Subject', '').strip()
            }
    except:
        return {'title': '', 'author': '', 'subject': ''}


def extract_abstract_from_pages(pdf_path: str) -> str:
    """
    Extract abstract from dedicated abstract page.
    Looks for 'Abstract' heading followed by content.
    """
    try:
        with open(pdf_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            
            for i, page in enumerate(reader.pages):
                page_text = page.extract_text().strip()
                
                # Look for pages that start with "Abstract" (case insensitive)
                if re.match(r'^\s*abstract\s*$', page_text[:50], re.IGNORECASE):
                    # This page likely contains only "Abstract" heading and the abstract
                    # Remove the "Abstract" heading and return the rest
                    abstract_text = re.sub(r'^\s*abstract\s*', '', page_text, flags=re.IGNORECASE)
                    return abstract_text.strip()
                
                # Alternative: look for pages where "Abstract" appears and the page is relatively short
                elif ('abstract' in page_text.lower() and 
                      len(page_text.split()) < 300):  # Less than 300 words = likely abstract page
                    
                    # Extract text after "Abstract" heading
                    match = re.search(r'abstract\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
                    if match:
                        abstract_text = match.group(1).strip()
                        # Clean up common artifacts
                        abstract_text = re.sub(r'\s+', ' ', abstract_text)  # Multiple spaces to single
                        return abstract_text
            
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


def search_title_in_pdf_pages(pdf_path: str, search_title: str, max_pages: int = 10) -> bool:
    """
    Search for title in first N pages of PDF.
    Also tries merging consecutive lines to handle multi-line titles.
    """
    try:
        with open(pdf_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            pages_to_check = min(max_pages, len(reader.pages))
            
            for i in range(pages_to_check):
                page_text = reader.pages[i].extract_text()
                lines = [line.strip() for line in page_text.split('\n') if line.strip()]
                
                # Check 1: Exact match in full page text
                if search_title.lower() in page_text.lower():
                    return True
                
                # Check 2: Try merging consecutive lines (handles multi-line titles)
                for j in range(len(lines)):
                    # Merge current line with next lines until we reach search title length
                    merged = lines[j]
                    for k in range(j + 1, min(j + 5, len(lines))):  # Try up to 4 consecutive lines
                        merged += " " + lines[k]
                        if search_title.lower() in merged.lower():
                            return True
        
        return False
    
    except:
        return False


def extract_title_from_first_pages(pdf_path: str) -> str:
    """
    Extract title from PDF content (first 1-2 pages).
    Used when filename title not found in PDF.
    """
    try:
        with open(pdf_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            
            # Get text from first two pages
            first_pages_text = ""
            for i in range(min(2, len(reader.pages))):
                first_pages_text += reader.pages[i].extract_text() + "\n"
            
            # Split into lines and find potential titles
            lines = [line.strip() for line in first_pages_text.split('\n') if line.strip()]
            
            # Terms to exclude from title extraction
            excluded_terms = [
                'Technical University of Denmark', 'DTU', 'Master Thesis', 'MSc Thesis', 
                'Thesis', 'MSc', 'DTU Compute', 'University', 
                'Department', 'Faculty', 'Technical University of Denmark (DTU)'
            ]
            
            for line in lines[:10]:  # Check first 10 lines
                # Skip very short lines, author lines, institutional lines
                if (len(line) > 10 and len(line) < 200 and 
                    not re.search(r'^(by|author|university|department)', line, re.IGNORECASE) and
                    not re.search(r'@|\d{4}|email', line, re.IGNORECASE) and
                    not any(term.lower() in line.lower() for term in excluded_terms)):
                    return line
            
        return "Title not found"
    
    except:
        return "Error extracting title"


def extract_author_from_metadata_or_text(pdf_path: str) -> str:
    """
    Extract author from PDF metadata or text.
    Filters out institutional terms (DTU, University, etc).
    """
    # Try metadata first
    metadata = extract_simple_metadata(pdf_path)
    
    # Filter out institutional/generic terms that shouldn't be considered authors
    excluded_terms = [
        'Technical University of Denmark', 'DTU', 'Master Thesis', 'MSc Thesis', 
        'Thesis', 'MSc', 'DTU Compute', 'University', 
        'Department', 'Faculty', 'Technical University of Denmark (DTU)'
    ]
    
    if metadata['author']:
        author_text = metadata['author'].strip()
        
        # If author is ONLY an institutional term, skip it
        if any(author_text.lower() == term.lower() for term in excluded_terms):
            pass  # Skip to text extraction
        # If author contains actual names (has commas, parentheses, or person-like patterns)
        elif (',' in author_text or '(' in author_text or 
              re.search(r'[A-Z][a-z]+\s+[A-Z][a-z]+', author_text)):
            # Extract just the name parts, remove institutional affiliations
            # Split by common delimiters and take the first name-like part
            name_parts = re.split(r'[,()&]', author_text)
            for part in name_parts:
                part = part.strip()
                if (re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+', part) and 
                    not any(term.lower() in part.lower() for term in excluded_terms)):
                    return part
        # Simple name without institutional terms
        elif not any(term.lower() in author_text.lower() for term in excluded_terms):
            return author_text
    
    # Try text extraction
    try:
        with open(pdf_path, 'rb') as file:
            reader = pypdf.PdfReader(file)
            first_page_text = reader.pages[0].extract_text()
            
            # Look for common author patterns
            author_patterns = [
                r'(?:by|author[s]?)\s*:?\s*([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'^([A-Z][a-z]+\s+[A-Z][a-z]+)$',  # Name on its own line
                r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*\n',  # Name followed by newline
            ]
            
            for pattern in author_patterns:
                match = re.search(pattern, first_page_text[:1000], re.MULTILINE | re.IGNORECASE)
                if match:
                    potential_author = match.group(1).strip()
                    # Apply same filtering to text-extracted authors
                    if not any(term.lower() in potential_author.lower() for term in excluded_terms):
                        return potential_author
    
    except:
        pass
    
    return "Author not found"


def process_single_pdf(pdf_path: str) -> Dict[str, str]:
    """
    Extract title, author, and abstract from single PDF.
    Returns dict with title_filename, title_pdf, title_found_in_pdf, author, abstract.
    """
    filename = Path(pdf_path).name
    title_filename = extract_title_from_filename(filename)
    
    # Step 1: Search for filename title in first 10 pages
    title_found_in_pdf = search_title_in_pdf_pages(pdf_path, title_filename)
    
    # Step 2: If not found in PDF, extract title from PDF content
    title_pdf = ""
    if not title_found_in_pdf:
        title_pdf = extract_title_from_first_pages(pdf_path)
    
    return {
        'filename': filename,
        'title_filename': title_filename,
        'title_pdf': title_pdf if title_pdf else "",
        'title_found_in_pdf': title_found_in_pdf,
        'author': extract_author_from_metadata_or_text(pdf_path), 
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
    csv_lines.append("Filename,Title_From_Filename,Title_From_PDF,Title_Match,Author,Abstract,File_Path")
    
    print(f"Processing {len(pdf_files)} PDF files...")
    print("=" * 50)
    
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            print(f"[{i}/{len(pdf_files)}] Processing: {pdf_path.name}")
            
            result = process_single_pdf(str(pdf_path))
            
            # Clean data for CSV (escape quotes, remove newlines) 
            filename = result['filename']
            title_filename = result['title_filename'].replace('"', '""').replace('\n', ' ').replace('\r', ' ')
            title_pdf = result['title_pdf'].replace('"', '""').replace('\n', ' ').replace('\r', ' ')
            title_match = "Yes" if result['title_found_in_pdf'] else "No"
            author = result['author'].replace('"', '""').replace('\n', ' ').replace('\r', ' ')
            abstract = result['abstract'].replace('"', '""').replace('\n', ' ').replace('\r', ' ')
            file_path = result['file_path']
            
            # Add to CSV (wrap in quotes to handle commas)
            csv_line = f'"{filename}","{title_filename}","{title_pdf}","{title_match}","{author}","{abstract}","{file_path}"'
            csv_lines.append(csv_line)
            
            print(f"   Title (filename): {title_filename[:50]}{'...' if len(title_filename) > 50 else ''}")
            print(f"   Found in PDF: {title_match}")
            if title_pdf:
                print(f"   Title (PDF): {title_pdf[:50]}{'...' if len(title_pdf) > 50 else ''}")
            print(f"   Author: {author}")
            print(f"   Abstract: {'Found' if 'not found' not in abstract.lower() else 'Not found'}")
            print()
            
        except Exception as e:
            print(f"   Error: {e}")
            csv_lines.append(f'"{pdf_path.name}","ERROR","ERROR","ERROR","ERROR","ERROR","{str(pdf_path)}"')
    
    # Write CSV file
    output_path = processed_data_dir / output_file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(csv_lines))
    
    print("=" * 50)
    print(f"Results saved to: {output_path}")
    print(f"You can open this file directly in Excel!")
    print(f"Processed {len(pdf_files)} files")


def show_single_pdf_info(pdf_path: Path):
    """
    Display extracted metadata for a single PDF.
    """
    print(f"Analyzing: {pdf_path.name}")
    print("=" * 50)
    
    result = process_single_pdf(str(pdf_path))
    
    print(f"TITLE (from filename): {result['title_filename']}")
    print(f"Found in PDF: {result['title_found_in_pdf']}")
    if result['title_pdf']:
        print(f"TITLE (from PDF): {result['title_pdf']}")
    print()
    print(f"AUTHOR: {result['author']}")
    print()
    print(f"ABSTRACT:")
    print(f"{result['abstract']}")
    print()
    print("=" * 50)


def main():
    """CLI entry point for PDF reading and metadata extraction."""
    
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
        
        # Extract metadata for single file
        elif command in ['--info', '--meta']:
            if len(sys.argv) < 3:
                print("ERROR: Please specify filename for --info")
                print("Usage: python pdf_reader.py --info '<filename>'")
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
        print(f"     python pdf_reader.py '<filename>' [--first5|--full]")
        print("  Extract metadata (title, author, abstract):")
        print(f"     python pdf_reader.py --info '<filename>'")
        print("  Export all PDFs to CSV for Excel:")
        print(f"     python pdf_reader.py --export")

if __name__ == "__main__":
    main()
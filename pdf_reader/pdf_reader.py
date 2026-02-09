#!/usr/bin/env python3
"""
Simple PDF Reader

Task 1: Open a PDF and read it with a PDF reader.
Access files from Data/RAW_test folder and output to terminal.
"""

import sys
from pathlib import Path
import pypdf


def read_pdf(pdf_path: str, max_pages: int = None) -> str:
    """
    Read text from a PDF file using pypdf.
    
    Args:
        pdf_path: Path to the PDF file
        max_pages: Maximum number of pages to read 
        (--first5 for first 5 pages, --full for all pages, defaults to None which means read all pages)
        
    Returns:
        Extracted text content
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


def main():
    """Main function to read PDFs from Data/RAW_test folder."""
    
    # Define the data directory
    data_dir = Path("../pdf_reader/Data/RAW_test") 
    
    if not data_dir.exists():
        print(f"ERROR: Directory not found: {data_dir}")
        sys.exit(1)
    
    # Get all PDF files
    pdf_files = list(data_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"ERROR: No PDF files found in {data_dir}")
        sys.exit(1)
    
    print(f"Found {len(pdf_files)} PDF files")
    print("=" * 50)
    
    # If command line argument provided, read specific file
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        pdf_path = data_dir / filename
        
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
        # Show available files
        print("Available PDF files:")
        for i, pdf_file in enumerate(pdf_files, 1):
            print(f"  {i}. {pdf_file.name}")
        
        print("\nUsage:")
        print(f"  uv run python pdf_reader.py '<filename>' [--first5|--full]")


if __name__ == "__main__":
    main()
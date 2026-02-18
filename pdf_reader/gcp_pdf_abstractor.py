#!/usr/bin/env python3
"""
GCP PDF Reader and Abstract Extractor

Streams PDFs directly from GCP bucket, extracts metadata, and uploads results.
Uses concurrent processing for speed and efficiency.
No local downloads required.
"""

import sys
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import time

import pypdf
from google.cloud import storage


class GCPPDFExtractor:
    """Extract metadata from PDFs stored in GCP bucket."""
    
    def __init__(self, bucket_name: str = "thesis_archive_bucket", 
                 output_bucket: Optional[str] = None,
                 max_workers: int = 15,
                 verbose: bool = True):
        """
        Initialize GCP PDF extractor.
        
        Args:
            bucket_name: Source bucket name
            output_bucket: Output bucket (default: same as source)
            max_workers: Number of concurrent workers
            verbose: Print progress
        """
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.output_bucket = output_bucket or bucket_name
        self.max_workers = max_workers
        self.verbose = verbose
        
        self.processed = 0
        self.abstracts_found = 0
        self.errors = 0
    
    def _log(self, message: str):
        """Log message if verbose."""
        if self.verbose:
            print(message)
    
    def _extract_title_from_filename(self, filename: str) -> str:
        """Extract English title from filename."""
        name_without_ext = filename.rsplit('.pdf', 1)[0]
        
        if '_' in name_without_ext:
            name_without_id = name_without_ext.split('_', 1)[1]
        else:
            name_without_id = name_without_ext
        
        if ' (translated ' in name_without_id:
            title = name_without_id.split(' (translated ', 1)[0]
        else:
            title = name_without_id
        
        return title.strip()
    
    def _is_toc_page(self, page_text: str) -> bool:
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
    
    def _extract_toc_from_reader(self, reader: pypdf.PdfReader, max_pages: int = 15) -> List[Tuple[str, Optional[int]]]:
        """
        Extract table of contents from PDF reader using text heuristic.
        Adapted from extract_toc.py for streaming/BytesIO support.
        Returns list of (title, page_number) tuples.
        """
        try:
            num_pages = len(reader.pages)
            pages_to_scan = min(num_pages, max_pages)
            
            # Collect text from first pages
            texts = []
            for i in range(pages_to_scan):
                try:
                    txt = reader.pages[i].extract_text() or ""
                except Exception:
                    txt = ""
                texts.append(txt)
            
            combined = "\n\n".join(texts)
            
            # Find 'contents' heading
            m = re.search(r"^\s*contents\b", combined, flags=re.IGNORECASE | re.MULTILINE)
            if not m:
                start_idx = 0
            else:
                start_idx = m.start()
            
            # Take substring from heading (or from start) up to some length
            snippet = combined[start_idx:start_idx + 20000]
            lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]
            
            toc_candidates = []
            # Basic heuristic: lines that end with a page number
            for ln in lines:
                # Common patterns: "1. Introduction ........ 1" or "1 Introduction 1"
                m = re.match(r"(?P<title>.+?)\s+(\.{2,}|\s+)\s*(?P<page>\d{1,4})$", ln)
                if not m:
                    m = re.match(r"(?P<title>.+?)\s+(?P<page>\d{1,4})$", ln)
                if m:
                    title = m.group("title").strip().rstrip('.')
                    page = int(m.group("page"))
                    toc_candidates.append((title, page))
            
            return toc_candidates
        except Exception:
            return []
    
    def _extract_abstract_from_toc(self, reader: pypdf.PdfReader) -> Tuple[int, int]:
        """
        Extract TOC to find where main content starts and where abstract is.
        Returns (first_main_section_page, search_end_page).
        search_end_page is where to stop searching for abstract.
        If not found, returns (-1, -1).
        """
        try:
            toc_entries = self._extract_toc_from_reader(reader, max_pages=15)
            
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
    
    def _search_section_by_keyword(self, reader: pypdf.PdfReader, keyword: str, max_pages: int = 10) -> str:
        """
        Search for a section with a specific keyword in the first N pages.
        Returns the section content if found, otherwise empty string.
        All searches are case-insensitive.
        """
        search_end = min(max_pages, len(reader.pages))
        
        for i in range(search_end):
            page = reader.pages[i]
            page_text = page.extract_text().strip()
            
            # Skip if this looks like a TOC page
            if self._is_toc_page(page_text):
                continue
            
            # Look for page starting with keyword (case-insensitive) with word boundary
            if re.match(rf'^\s*{re.escape(keyword)}\b\s*$', page_text[:100], re.IGNORECASE):
                content = re.sub(rf'^\s*{re.escape(keyword)}\b\s*', '', page_text, flags=re.IGNORECASE)
                return content.strip()
            
            # Look for numbered keyword like "1 Summary" (case-insensitive)
            elif re.match(rf'^\s*\d+\s+{re.escape(keyword)}\b', page_text, re.IGNORECASE):
                content = re.sub(rf'^\s*\d+\s+{re.escape(keyword)}\s*', '', page_text, flags=re.IGNORECASE)
                return content.strip()
            
            # Look for keyword with colon like "Summary:" (case-insensitive) with word boundary
            elif re.search(rf'^\s*{re.escape(keyword)}\b:', page_text, re.IGNORECASE):
                match = re.search(rf'\b{re.escape(keyword)}\b\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
                if match:
                    content = match.group(1).strip()
                    content = re.sub(r'\s+', ' ', content)
                    return content
            
            # Look for keyword on its own line (even if not at page start) - case-insensitive with word boundary
            elif re.search(rf'^\s*{re.escape(keyword)}\b\s*$', page_text, re.IGNORECASE | re.MULTILINE):
                match = re.search(rf'^\s*{re.escape(keyword)}\b\s*\n([\s\S]*)', page_text, re.IGNORECASE | re.MULTILINE)
                if match:
                    content = match.group(1).strip()
                    # Limit to reasonable length to avoid capturing too much
                    words = content.split()
                    if len(words) > 600:
                        content = ' '.join(words[:600])
                    content = re.sub(r'\s+', ' ', content)
                    return content
            
            # Look for keyword appearing in page with reasonable length (case-insensitive) with word boundary
            elif bool(re.search(rf'\b{re.escape(keyword)}\b', page_text, re.IGNORECASE)) and len(page_text.split()) < 600:
                match = re.search(rf'\b{re.escape(keyword)}\b\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
                if match:
                    content = match.group(1).strip()
                    content = re.sub(r'\s+', ' ', content)
                    return content
        
        return ""
    
    def _extract_abstract_from_bytes(self, pdf_bytes: bytes, filename: str) -> str:
        """
        Extract abstract from PDF bytes.
        Implements improved search with TOC awareness and fallback keywords.
        """
        try:
            pdf_file = BytesIO(pdf_bytes)
            reader = pypdf.PdfReader(pdf_file)
            
            # First, try to use TOC to find where main content starts
            first_main_section_page, search_end_page = self._extract_abstract_from_toc(reader)
            
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
            
            # First pass: Search for "Abstract" with specific patterns
            for i in range(search_start, search_end):
                page = reader.pages[i]
                page_text = page.extract_text().strip()
                
                # Skip if this looks like a TOC page
                if self._is_toc_page(page_text):
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
                elif (bool(re.search(r'\babstract\b', page_text, re.IGNORECASE)) and 
                      len(page_text.split()) < 800):  # Less than 800 words = likely abstract page
                    
                    # Extract text after "Abstract" heading (with word boundary)
                    match = re.search(r'\babstract\b\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
                    if match:
                        abstract_text = match.group(1).strip()
                        # Clean up common artifacts
                        abstract_text = re.sub(r'\s+', ' ', abstract_text)  # Multiple spaces to single
                        return abstract_text
            
            # Second pass: If no abstract found, search for alternative keywords in first 10 pages
            # Note: "abstract" is not included here as it's already extensively searched in the first pass
            alternative_keywords = [
                "summary",
                "summary (english)",
                "abstract ",
                "preface",
                "resumé"
            ]
            
            for keyword in alternative_keywords:
                result = self._search_section_by_keyword(reader, keyword, max_pages=10)
                if result:
                    return result
            
            return "Abstract not found"
        
        except Exception as e:
            return f"Error extracting abstract: {str(e)}"
    
    def _process_single_pdf(self, blob_path: str) -> Dict[str, str]:
        """
        Download and process single PDF from GCP.
        
        Returns dict with filename, title, abstract.
        """
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            
            if not blob.exists():
                return {
                    'filename': Path(blob_path).name,
                    'title': 'ERROR',
                    'abstract': 'File not found in bucket',
                    'path': blob_path,
                    'error': True
                }
            
            # Stream PDF from GCP to memory
            pdf_bytes = blob.download_as_bytes()
            
            filename = Path(blob_path).name
            title = self._extract_title_from_filename(filename)
            abstract = self._extract_abstract_from_bytes(pdf_bytes, filename)
            
            return {
                'filename': filename,
                'title': title,
                'abstract': abstract,
                'path': blob_path,
                'error': False
            }
        
        except Exception as e:
            return {
                'filename': Path(blob_path).name,
                'title': 'ERROR',
                'abstract': f'Error: {str(e)}',
                'path': blob_path,
                'error': True
            }
    
    def process_bucket_prefix(self, prefix: str = "dtu_findit/master_thesis/",
                               start_index: int = 0,
                               max_files: Optional[int] = None,
                               output_prefix: str = "extracted_data/") -> Tuple[List[Dict], int, int, int]:
        """
        Process all PDFs with given prefix from bucket.
        
        Args:
            prefix: GCS prefix to search (e.g., "dtu_findit/master_thesis/")
            start_index: Start processing from this file index
            max_files: Maximum files to process (None = all)
            output_prefix: Where to save results in bucket
            
        Returns:
            (documents, abstracts_found, not_found, errors)
        """
        self._log(f"Connecting to bucket: {self.bucket_name}")
        self._log(f"Prefix: {prefix}")
        self._log(f"Workers: {self.max_workers}")
        self._log("=" * 50)
        
        # List all PDFs
        bucket = self.client.bucket(self.bucket_name)
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        
        pdf_paths = []
        for blob in blobs:
            if blob.name.endswith('.pdf'):
                pdf_paths.append(blob.name)
        
        if max_files:
            pdf_paths = pdf_paths[start_index:start_index + max_files]
        else:
            pdf_paths = pdf_paths[start_index:]
        
        self._log(f"Found {len(pdf_paths)} PDF files to process")
        self._log("=" * 50)
        
        documents = []
        abstracts_found = 0
        abstracts_not_found = 0
        errors = 0
        
        # Process in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_pdf, path): (i, path) 
                for i, path in enumerate(pdf_paths)
            }
            
            for future in as_completed(futures):
                i, path = futures[future]
                try:
                    result = future.result()
                    documents.append(result)
                    
                    if result['error']:
                        errors += 1
                    elif 'not found' in result['abstract'].lower() or result['abstract'].startswith('Error'):
                        abstracts_not_found += 1
                    else:
                        abstracts_found += 1
                    
                    # Print progress every 50 files
                    if (i + 1) % 50 == 0:
                        self._log(f"[{i+1}/{len(pdf_paths)}] Processed - "
                                 f"Found: {abstracts_found}, Not found: {abstracts_not_found}, Errors: {errors}")
                
                except Exception as e:
                    self._log(f"   Worker error: {e}")
                    errors += 1
        
        self._log("=" * 50)
        self._log(f"Processing complete!")
        self._log(f"Total: {len(pdf_paths)} files")
        self._log(f"Abstracts found: {abstracts_found}/{len(pdf_paths)}")
        self._log(f"Abstracts not found: {abstracts_not_found}/{len(pdf_paths)}")
        self._log(f"Errors: {errors}/{len(pdf_paths)}")
        
        return documents, abstracts_found, abstracts_not_found, errors
    
    def save_results_to_gcp(self, documents: List[Dict], 
                            output_prefix: str = "extracted_data/",
                            csv_filename: str = "extracted_metadata.csv",
                            json_filename: str = "extracted_metadata.json"):
        """Save CSV and JSON results to GCP bucket."""
        
        self._log(f"\nSaving results to gs://{self.output_bucket}/{output_prefix}")
        
        # Generate CSV
        csv_lines = ["Filename,Title,Abstract"]
        for doc in documents:
            title = doc['title'].replace('"', '""').replace('\n', ' ')
            abstract = doc['abstract'].replace('"', '""').replace('\n', ' ')
            csv_line = f'"{doc["filename"]}","{title}","{abstract}"'
            csv_lines.append(csv_line)
        
        csv_content = '\n'.join(csv_lines)
        
        # Save CSV to GCP
        bucket = self.client.bucket(self.output_bucket)
        csv_blob = bucket.blob(f"{output_prefix}{csv_filename}")
        csv_blob.upload_from_string(csv_content, content_type='text/csv')
        self._log(f"✓ Saved CSV: gs://{self.output_bucket}/{output_prefix}{csv_filename}")
        
        # Save JSON to GCP
        json_blob = bucket.blob(f"{output_prefix}{json_filename}")
        json_blob.upload_from_string(
            json.dumps(documents, indent=2, ensure_ascii=False),
            content_type='application/json'
        )
        self._log(f"✓ Saved JSON: gs://{self.output_bucket}/{output_prefix}{json_filename}")


def main():
    """CLI entry point."""
    
    if len(sys.argv) < 2:
        print("Usage: python gcp_pdf_abstractor.py [OPTIONS]")
        print("\nOptions:")
        print("  --all              Process all 6,300+ PDFs (default)")
        print("  --test             Process first 10 PDFs (for testing)")
        print("  --sample N         Process first N PDFs")
        print("  --workers N        Number of concurrent workers (default: 15)")
        print("  --prefix PATH      GCS prefix (default: dtu_findit/master_thesis/)")
        print("  --output-prefix P  Output prefix (default: extracted_data/)")
        print("  --bucket BUCKET    Source bucket (default: thesis_archive_bucket)")
        print("\nExamples:")
        print("  python gcp_pdf_abstractor.py --test")
        print("  python gcp_pdf_abstractor.py --sample 100 --workers 20")
        print("  python gcp_pdf_abstractor.py --all")
        return
    
    # Parse arguments
    max_files = None
    max_workers = 15
    prefix = "dtu_findit/master_thesis/"
    output_prefix = "extracted_data/"
    bucket_name = "thesis_archive_bucket"
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i].lower()
        
        if arg == "--test":
            max_files = 10
        elif arg == "--sample" and i + 1 < len(sys.argv):
            max_files = int(sys.argv[i + 1])
            i += 1
        elif arg == "--workers" and i + 1 < len(sys.argv):
            max_workers = int(sys.argv[i + 1])
            i += 1
        elif arg == "--prefix" and i + 1 < len(sys.argv):
            prefix = sys.argv[i + 1]
            i += 1
        elif arg == "--output-prefix" and i + 1 < len(sys.argv):
            output_prefix = sys.argv[i + 1]
            i += 1
        elif arg == "--bucket" and i + 1 < len(sys.argv):
            bucket_name = sys.argv[i + 1]
            i += 1
        
        i += 1
    
    # Run extraction
    extractor = GCPPDFExtractor(bucket_name=bucket_name, max_workers=max_workers)
    
    start_time = time.time()
    documents, abstracts_found, abstracts_not_found, errors = extractor.process_bucket_prefix(
        prefix=prefix,
        max_files=max_files,
        output_prefix=output_prefix
    )
    elapsed = time.time() - start_time
    
    # Save results
    extractor.save_results_to_gcp(
        documents,
        output_prefix=output_prefix
    )
    
    # Summary
    print(f"\n{'='*50}")
    print(f"Time elapsed: {elapsed:.1f}s ({elapsed/60:.1f}m)")
    if max_files:
        print(f"Speed: {max_files / elapsed:.1f} files/sec")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()

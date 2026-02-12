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
    
    def _extract_abstract_from_bytes(self, pdf_bytes: bytes, filename: str) -> str:
        """Extract abstract from PDF bytes."""
        try:
            pdf_file = BytesIO(pdf_bytes)
            reader = pypdf.PdfReader(pdf_file)
            
            for i, page in enumerate(reader.pages):
                page_text = page.extract_text().strip()
                
                # Look for pages that start with "Abstract"
                if re.match(r'^\s*abstract\s*$', page_text[:50], re.IGNORECASE):
                    abstract_text = re.sub(r'^\s*abstract\s*', '', page_text, flags=re.IGNORECASE)
                    return abstract_text.strip()
                
                # Alternative: look for "Abstract" with relatively short page
                elif ('abstract' in page_text.lower() and len(page_text.split()) < 500):
                    match = re.search(r'abstract\s*:?\s*([\s\S]*)', page_text, re.IGNORECASE)
                    if match:
                        abstract_text = match.group(1).strip()
                        abstract_text = re.sub(r'\s+', ' ', abstract_text)
                        return abstract_text
            
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
                               output_prefix: str = "extracted_data/") -> Tuple[List[Dict], int, int]:
        """
        Process all PDFs with given prefix from bucket.
        
        Args:
            prefix: GCS prefix to search (e.g., "dtu_findit/master_thesis/")
            start_index: Start processing from this file index
            max_files: Maximum files to process (None = all)
            output_prefix: Where to save results in bucket
            
        Returns:
            (documents, abstracts_found, errors)
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
                        status = "ERROR"
                    else:
                        abstracts_found += 1
                        status = "✓"
                    
                    # Print progress every 50 files
                    if (i + 1) % 50 == 0:
                        self._log(f"[{i+1}/{len(pdf_paths)}] Processed - "
                                 f"Found: {abstracts_found}, Errors: {errors}")
                
                except Exception as e:
                    self._log(f"   Worker error: {e}")
                    errors += 1
        
        self._log("=" * 50)
        self._log(f"Processing complete!")
        self._log(f"Total: {len(pdf_paths)} files")
        self._log(f"Abstracts found: {abstracts_found}/{len(pdf_paths)}")
        self._log(f"Errors: {errors}")
        
        return documents, abstracts_found, errors
    
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
    documents, abstracts_found, errors = extractor.process_bucket_prefix(
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

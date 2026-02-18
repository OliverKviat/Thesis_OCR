#!/usr/bin/env python3
"""
Text cleaner of PDF Abstracts Exstracted from LOCAL storrage.

Cleans text, and stores results.
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

class GCPTextCleaner:
    """Clean text stored in GCP bucket."""
    
    def __init__(self, bucket_name: str = "thesis_archive_bucket", 
                 output_bucket: Optional[str] = None,
                 max_workers: int = 15,
                 verbose: bool = True):
        """
        Initialize GCP text cleaner.
        
        Args:
            bucket_name: Source bucket name
            output_bucket: Output bucket (default: same as source)
            #max_workers: Number of concurrent workers
            verbose: Print progress
        """
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.output_bucket = output_bucket or bucket_name
        #self.max_workers = max_workers
        self.verbose = verbose
        
        self.processed = 0
        self.cleaned = 0
        self.errors = 0

def main():
    """CLI entry point."""
    
    if len(sys.argv) < 2:
        print("Usage: python gcp_text_cleaner.py [OPTIONS]")
        print("\nOptions:")
        print("  --all              Process all 6,300+ lines (default)")
        print("  --test             Process first 10 lines (for testing)")
        print("  --sample N         Process first N lines")
        #print("  --workers N        Number of concurrent workers (default: 15)")
        print("  --prefix PATH      GCS prefix (default: dtu_findit/master_thesis/)")
        print("  --output-prefix P  Output prefix (default: extracted_data/cleaned_data/)")
        print("  --bucket BUCKET    Source bucket (default: thesis_archive_bucket)")
        print("\nExamples:")
        print("  python gcp_text_cleaner.py --test")
        print("  python gcp_text_cleaner.py --sample 100")
        print("  python gcp_text_cleaner.py --all")
        return
    
    # Parse arguments
    max_lines = None
    #max_workers = 15
    prefix = "dtu_findit/master_thesis/"
    output_prefix = "extracted_data/cleaned_data/"
    bucket_name = "thesis_archive_bucket"
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i].lower()
        
        if arg == "--test":
            max_lines = 10
        elif arg == "--sample" and i + 1 < len(sys.argv):
            max_lines = int(sys.argv[i + 1])
            i += 1
        #elif arg == "--workers" and i + 1 < len(sys.argv):
        #    max_workers = int(sys.argv[i + 1])
        #    i += 1
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
    extractor = GCPTextCleaner(
        bucket_name=bucket_name,
        prefix=prefix,
        max_lines=max_lines
    )
    
    
    
    # Save results
    extractor.save_results_to_gcp(
        output_prefix=output_prefix
    )
    
    # Summary
    print(f"Processed {extractor.processed} documents")
    print(f"Cleaned {extractor.cleaned} documents")
    print(f"Errors: {extractor.errors}")

if __name__ == "__main__":
    main()

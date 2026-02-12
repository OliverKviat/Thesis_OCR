#!/usr/bin/env python3
"""
This script tests the connection to the GCP Bucket and lists files.
"""

import sys
from google.cloud import storage
from pathlib import Path


def test_gcp_connection(bucket_name: str = "thesis_archive_bucket"):
    """
    Test connection to GCP bucket and list files.
    """
    try:
        print(f"Testing connection to GCP bucket: {bucket_name}")
        print("=" * 50)
        
        # Initialize client
        client = storage.Client()
        print("✓ Google Cloud Storage client initialized")
        
        # Get bucket
        bucket = client.bucket(bucket_name)
        print(f"✓ Connected to bucket: {bucket_name}")
        
        # List blobs in the thesis folder
        prefix = "dtu_findit/master_thesis/"
        print(f"\nListing files in: gs://{bucket_name}/{prefix}")
        print("=" * 50)
        
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        
        pdf_count = 0
        sample_files = []
        
        for i, blob in enumerate(blobs):
            if blob.name.endswith('.pdf'):
                pdf_count += 1
                if len(sample_files) < 5:  # Keep first 5 samples
                    sample_files.append(blob.name)
                if i % 100 == 0 and i > 0:
                    print(f"... processed {i} files ...", end='\r')
        
        print(f"\n✓ Found {pdf_count} PDF files in the bucket")
        
        if sample_files:
            print(f"\nFirst 5 PDF files found:")
            for file in sample_files:
                print(f"  - {file}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error connecting to GCP: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure you're authenticated: gcloud auth login")
        print("2. Set default project: gcloud config set project YOUR_PROJECT_ID")
        print("3. Or set GOOGLE_APPLICATION_CREDENTIALS env var to your service account key")
        return False


def test_file_streaming(bucket_name: str = "thesis_archive_bucket", 
                        blob_path: str = None):
    """
    Test streaming a single file from bucket.
    """
    try:
        if not blob_path:
            print("Please provide a blob_path to test file streaming")
            return False
        
        print(f"\nTesting file streaming from: gs://{bucket_name}/{blob_path}")
        print("=" * 50)
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        # Check if file exists
        if not blob.exists():
            print(f"✗ File does not exist: {blob_path}")
            return False
        
        print(f"✓ File found: {blob_path}")
        print(f"✓ File size: {blob.size / (1024 * 1024):.2f} MB")
        
        # Download to memory (first 1MB for testing)
        print("Downloading first 1MB to test...")
        chunk = blob.download_as_bytes(end_byte=1024*1024)
        print(f"✓ Successfully streamed {len(chunk) / (1024*1024):.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"✗ Error streaming file: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test-file":
            if len(sys.argv) < 3:
                print("Usage: python GCP_connector.py --test-file <blob_path>")
                sys.exit(1)
            blob_path = sys.argv[2]
            test_file_streaming(blob_path=blob_path)
        else:
            bucket = sys.argv[1]
            test_gcp_connection(bucket)
    else:
        test_gcp_connection()

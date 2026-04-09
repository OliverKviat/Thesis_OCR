#!/usr/bin/env python3
"""Load thesis parquet dataframes directly from GCS.

This script connects to the ``thesis_archive_bucket`` bucket and loads the two
parquet files used throughout the exploratory notebook into pandas DataFrames.

Authentication relies on Google Application Default Credentials, so you can use
either a local ADC login or a service-account key via ``GOOGLE_APPLICATION_CREDENTIALS``.
"""

from __future__ import annotations

from io import BytesIO
from typing import Tuple

import pandas as pd
from google.cloud import storage

BUCKET_NAME = "thesis_archive_bucket"
PREFIX = "dtu_findit/extraction_and_processing"
ALL_FINAL_FILE = "thesis_meta_all_metrics_except_grade_filtered_27032026.parquet"
FILTERED_FINAL_FILE = "thesis_meta_all_metrics_with_84pct_grades_08042026.parquet"


def load_parquet_from_gcs(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    """Load a parquet blob from GCS into a pandas DataFrame."""
    blob = bucket.blob(blob_name)

    if not blob.exists():
        raise FileNotFoundError(f"Blob not found in GCS: gs://{bucket.name}/{blob_name}")

    data = blob.download_as_bytes()
    return pd.read_parquet(BytesIO(data))


def load_thesis_dataframes() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load the two thesis metric tables from the thesis archive bucket."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    all_final = load_parquet_from_gcs(
        bucket,
        f"{PREFIX}/{ALL_FINAL_FILE}",
    )
    filtered_final = load_parquet_from_gcs(
        bucket,
        f"{PREFIX}/{FILTERED_FINAL_FILE}",
    )

    return all_final, filtered_final


def main() -> None:
    all_final, filtered_final = load_thesis_dataframes()

    print(f"Loaded all_final with shape: {all_final.shape}")
    print(f"Loaded filtered_final with shape: {filtered_final.shape}")

    print("\nall_final columns:")
    print(all_final.columns.tolist())

    print("\nfiltered_final columns:")
    print(filtered_final.columns.tolist())


if __name__ == "__main__":
    main()
Pipeline run order for extracting and enriching metrics

1) Extract text from PDFs
- Run: `exstraction_n_processing_crawl2/LOCAL_pdf2txt.py` (or your preferred PDF->TXT step)

2) Extract metrics from TXT files
- Run: `exstraction_n_processing_crawl2/LOCAL_txt_metrics_extractor.py`
- Output: `Data/extracted_metrics_unified.csv`
````
uv run exstraction_n_processing_crawl2/LOCAL_txt_metrics_exstractor.py --input-dir Data/TXT_test --workers 8
````

3) Enrich metrics with metadata and department classification
- Run: `exstraction_n_processing_crawl2/LOCAL_enrich_w_meta.py`
- This step merges metadata, assigns `Department_new` using TF-IDF + cosine similarity and joins it together with the metrics.

Notes:
- Adjust `--threshold` if you want different matching sensitivity (default 0.35).

Pipeline run order for extracting and enriching metrics

1) Extract text from PDFs
- Run: `exstraction_n_processing_crawl2/LOCAL_pdf2txt.py` (or your preferred PDF->TXT step)

2) Extract metrics from TXT files
- Run: `exstraction_n_processing_crawl2/LOCAL_txt_metrics_extractor.py`
- Output: `Data/extracted_metrics_unified.parquet`
***OBS: ***
* change the input directory to the desired location with the `--input-dir` CLI command.
````
uv run exstraction_n_processing_crawl2/LOCAL_txt_metrics_exstractor.py --input-dir Data/TXT_test --workers 8
````

3) Enrich metrics with metadata and department classification
- Run: `exstraction_n_processing_crawl2/LOCAL_enrich_w_meta.py`
- This step merges metadata, assigns `Department_new` using TF-IDF + cosine similarity and joins it together with the metrics.
- Output: `thesis_meta_all_metrics_except_grade.parquet`
***OBS:***
* Change the `METRICS_PATH`, `EXPORT_PATH` and `EXPORT_FILENAME` to the desired.
* Set `MERGE_HOW = 1` *# 1: inner, 2: left* to set what merge method is to be used.
*Notes:*
- Adjust `--threshold` if you want different matching sensitivity (default 0.35).

4) Append Supervisors and match endpoint (columns and column names for seamles integration into analysis scripts)
- Run: `exstraction_n_processing_crawl2/LOCAL_endpoint_match_n_export.py`
- Output: `crawl2_thesis_meta_all_metrics_except_grade.parquet`
***OBS:***
* Set `MERGE_HOW = 1` *# 1: inner, 2: left* to set what merge method is to be used.
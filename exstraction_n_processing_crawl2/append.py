from pathlib import Path
import pandas as pd

meta_initial_path = Path("Data/crawl2_files/meta_findit/meta_findit_all_merged.csv")
meta_pisoglort_path = Path("Data/crawl2_files/meta_findit/resten_af_pisoglort.csv")

meta_initial_df = pd.read_csv(meta_initial_path, sep=";", encoding="utf-8", low_memory=False)
meta_pisoglort_df = pd.read_csv(meta_pisoglort_path, sep=";", encoding="utf-8", low_memory=False)

meta_FINAL = pd.concat([meta_initial_df, meta_pisoglort_df], ignore_index=True)

export_path = Path("Data/crawl2_files/meta_findit/meta_findit_all_merged_FINAL.csv")
meta_FINAL.to_csv(export_path, sep=";", index=False, encoding="utf-8")
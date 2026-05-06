import pandas as pd
from pathlib import Path

metrics_path = Path("Data/crawl2_files/extracted_metrics_unified.parquet")
meta_path = Path("Data/crawl2_files/meta_findit/meta_findit_all_merged_v2.csv")

df_metrics = pd.read_parquet(metrics_path)
df_meta = pd.read_csv(meta_path, sep=";", dtype=str)

df_metrics2 = df_metrics.drop_duplicates(subset=["filename"], keep="first").copy()

print(f"Rows in metrics DataFrame: {len(df_metrics)}")
print(f"Rows in metrics DataFrame after dropping duplicates: {len(df_metrics2)}")
#print(f"Rows in meta DataFrame: {len(df_meta)}")

#df_metrics.info()
#df_meta.info()

# listing the recods in df_metrics["filename"] that are not in df_meta["ID"]
filenames_metrics = set(df_metrics["filename"].apply(lambda x: x.split(".txt")[0]).astype(str))
filenames_meta = set(df_meta["ID"].astype(str))

missing_filenames = filenames_metrics - filenames_meta
print(f"Number of records in df_metrics that are missing in df_meta based on 'filename': {len(missing_filenames)}")
for item in missing_filenames:
    print(item)
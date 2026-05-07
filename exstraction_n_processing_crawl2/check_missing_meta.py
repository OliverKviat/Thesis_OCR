import pandas as pd
from pathlib import Path

step_1_path = Path("Data/crawl2_files/extracted_metrics_unified.parquet")
step_2_path = Path("Data/crawl2_files/thesis_meta_all_metrics_except_grade_and_supervisor_LEFT.parquet")
step_3_path = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/data/crawl2/BASE_DATA_BIG.parquet")

step_1 = pd.read_parquet(step_1_path)
print(f"Rows in step_1 DataFrame: {len(step_1)}")
print(f"Unique filenames in step_1 DataFrame: {step_1['filename'].nunique()}")
step_2 = pd.read_parquet(step_2_path)
print(f"Rows in step_2 DataFrame: {len(step_2)}")
print(f"Unique filenames in step_2 DataFrame: {step_2['filename'].nunique()}")
step_3 = pd.read_parquet(step_3_path)
print(f"Rows in step_3 DataFrame: {len(step_3)}")
print(f"Unique filenames in step_3 DataFrame: {step_3['filename'].nunique()}")

print("\n========== BREAK ==========\n")

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
from pathlib import Path
import numpy as np
import pandas as pd

# ==== MERGE SETTING ==== 
MERGE_HOW = 2 # 1: inner, 2: left

if MERGE_HOW == 1:
    MERGE_HOW_STR = ["inner", "INNER"]
elif MERGE_HOW == 2:
    MERGE_HOW_STR = ["left", "LEFT"]

# ==== SETTINGS ====
REPO_ROOT = Path(__file__).resolve().parents[1]
IMPORT_PATH_INTERNAL = REPO_ROOT / "Data" / "crawl2_files"
EXPORT = False
EXPORT_PATH = "/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/data/crawl2/"
FILE_EXPORT_NAME = f"BASE_DATA_BIG.parquet"

# ==== FILES ====
FILE_INTERNAL = f"thesis_meta_all_metrics_except_grade_and_supervisor_{MERGE_HOW_STR[1]}.parquet"
FILE_INTERNAL_UNI = "extracted_metrics_unified.parquet"
SUPERVISOR_PARQUET_PATH = "unique_supervisors_fixed_again.parquet"


def load_csv_to_df(csv_path, sep=";", verbose=True):
    try:
        df = pd.read_csv(csv_path, encoding="utf-8", sep=sep)
        if verbose:
            print(f"Successfully loaded CSV from {csv_path}")
            print(f"DataFrame shape: {df.shape}")
            print(f"DataFrame columns: {df.columns.tolist()}\n")
        return df
    except Exception as e:
        print(f"Error loading CSV from {csv_path}: {e}")
        return None


def load_parquet_to_df(parquet_path, na=False, verbose=True):
    try:
        df = pd.read_parquet(parquet_path)
        if verbose:
            print(f"Successfully loaded Parquet from {parquet_path}")
            print(f"DataFrame shape: {df.shape}")
            if na:
                print(f"DataFrame N/A counts:\n{df.isna().sum()}\n")
            print(f"DataFrame columns: {df.columns.tolist()}\n")
        return df
    except Exception as e:
        print(f"Error loading Parquet from {parquet_path}: {e}")
        return None


# ==== LOAD DATAFRAMES ====
df_crawl2 = load_parquet_to_df(IMPORT_PATH_INTERNAL / FILE_INTERNAL, verbose=False)
if df_crawl2 is None:
    raise FileNotFoundError(f"Could not load crawl2 parquet from {IMPORT_PATH_INTERNAL / FILE_INTERNAL}")

# ==== COLUMNS TO DROP ====
drop_columns = [
    "access_ss",
    "Affiliations",
    "collection_facet",
    "format",
    "fulltext_availability_facet",
    "ISBN",
    "Journal Page",
    "isolanguage_facet",
    "Publisher",
    "Source",
    "source_all_ss",
    "match_trigger",
    "equation_pipeline_version",
    "pdf_file_analysis",
    "num_tot_pages_analysis",
    "num_cont_pages_analysis",
    "num_words_full_analysis",
    "num_words_cont_analysis",
    "abstract_ts_analysis",
    "Author_analysis",
    "Publication Year_analysis",
    "primary_member_id_s_analysis",
    "Title_analysis",
    "department_match_fragment",
    "department_match_source",
    "department_match_score",
    "linguistics_backend",
    "department_match_alias",
    "corrupt_cid",
    "Title",
    "Author",
]

# ==== DROP COLUMNS & REMOVE DUPLICATES ====
df_crawl2 = df_crawl2.drop(columns=drop_columns, errors="ignore")
df_crawl2 = df_crawl2.drop_duplicates(subset=["ID"], keep="first")

# ==== DISPLAY INFO ====
print("\nCrawl2 DataFrame Info:")
print(f"Number of columns: {len(df_crawl2.columns)}")
print(df_crawl2.columns)

# ==== LOAD SUPERVISOR DATAFRAME ====
#df_supervisors = pd.read_csv((IMPORT_PATH_INTERNAL / SUPERVISOR_CSV_PATH), sep=",", dtype={"ID": "str"}) #load_csv_to_df(IMPORT_PATH_INTERNAL / SUPERVISOR_CSV_PATH, sep=",", verbose=False)
df_supervisors = load_parquet_to_df(IMPORT_PATH_INTERNAL / SUPERVISOR_PARQUET_PATH, verbose=False)
if df_supervisors is None:
    raise FileNotFoundError(f"Could not load supervisor Parquet from {IMPORT_PATH_INTERNAL / SUPERVISOR_PARQUET_PATH}")

# Drop the following columns from df_supervisors
drop_supervisor_columns = ["YEAR", "TYPES", "PUBLISHER", "Publication Year"]
df_supervisors = df_supervisors.drop(columns=drop_supervisor_columns, errors="ignore")

#print(f"df_crawl2['ID'] dtype: {df_crawl2['ID'].dtypes}")
#print(f"df_supervisors['ID'] dtype: {df_supervisors['ID'].dtypes}")

# Match on df_crawl2["ID"] and df_supervisors["ID"]
df_merged = pd.merge(
    df_crawl2,
    df_supervisors,
    left_on="ID",
    right_on="ID",
    how=MERGE_HOW_STR[0],
)

df_merged = df_merged.drop(columns=["record_id"], errors="ignore")

# ==== DORP INSA METRICS COLUMNS ====
# dropping all columns of df_merged that are 100% empty (all values are NaN)
df_notna = df_merged.dropna(axis=1, how="all")
print(f"\nColumns dropped from merged DataFrame due to being 100% empty: {set(df_merged.columns) - set(df_notna.columns)}")
print(f"Number of columns after dropping empty columns: {len(df_notna.columns)}")
print(f"Number of rows in merged DataFrame: {len(df_notna)}")

# ==== EXPORT UNIFIED DATAFRAME ====
if EXPORT:
    export_path = EXPORT_PATH + FILE_EXPORT_NAME
    try:
        df_notna.to_parquet(export_path, index=False)
        print(f"Successfully exported unified DataFrame to {export_path}")
    except Exception as e:
        print(f"Error exporting unified DataFrame to {export_path}: {e}")

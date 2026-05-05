from pathlib import Path

import pandas as pd

# ==== SETTINGS ====
REPO_ROOT = Path(__file__).resolve().parents[1]
IMPORT_PATH_INTERNAL = REPO_ROOT / "Data" / "crawl2_files"
IMPORT_SUPERVISOR_PATH = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/maks/")
EXPORT = True
EXPORT_PATH = IMPORT_PATH_INTERNAL
FILE_EXPORT_NAME = "crawl2_thesis_meta_all_metrics_except_grade.parquet"

# ==== FILES ====
FILE_INTERNAL = "thesis_meta_all_metrics_except_grade.parquet"
FILE_INTERNAL_UNI = "extracted_metrics_unified.parquet"
SUPERVISOR_CSV_PATH = "Supervisor_information.csv"


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
df_supervisors = load_csv_to_df(IMPORT_SUPERVISOR_PATH / SUPERVISOR_CSV_PATH, sep=",", verbose=False)
if df_supervisors is None:
    raise FileNotFoundError(f"Could not load supervisor CSV from {IMPORT_SUPERVISOR_PATH / SUPERVISOR_CSV_PATH}")

# Drop the following columns from df_supervisors
drop_supervisor_columns = ["YEAR", "TYPES", "PUBLISHER"]
df_supervisors = df_supervisors.drop(columns=drop_supervisor_columns, errors="ignore")

# Match on df_supervisors["record_id"] and df_crawl2["primary_member_id_s"]
df_merged = pd.merge(
    df_crawl2,
    df_supervisors,
    left_on="primary_member_id_s",
    right_on="record_id",
    how="left",
)

df_merged = df_merged.drop(columns=["record_id"], errors="ignore")

# ==== EXPORT UNIFIED DATAFRAME ====
if EXPORT:
    export_path = EXPORT_PATH / FILE_EXPORT_NAME
    try:
        df_merged.to_parquet(export_path, index=False)
        print(f"Successfully exported unified DataFrame to {export_path}")
    except Exception as e:
        print(f"Error exporting unified DataFrame to {export_path}: {e}")

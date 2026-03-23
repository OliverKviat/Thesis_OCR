# ==== IMPORTS ====
import pandas as pd
from pathlib import Path

# ==== LOADS ====
# Load the master thesis metrics analysis data
candidate_paths_metrics = [
    Path("../Data/gcp_order/dtu_findit/extraction_and_processing/master_thesis_metrics_analysis.csv"),
    Path("Data/gcp_order/dtu_findit/extraction_and_processing/master_thesis_metrics_analysis.csv"),
]

csv_path_metrics = next((p for p in candidate_paths_metrics if p.exists()), None)
if csv_path_metrics is None:
    raise FileNotFoundError("Could not find master_thesis_metrics_analysis.csv in expected locations.")

master_thesis_metrics_analysis = pd.read_csv(csv_path_metrics, sep=";", engine="python", on_bad_lines="skip")
print(f"Loaded master_thesis_metrics_analysis with {len(master_thesis_metrics_analysis)} entries and shape {master_thesis_metrics_analysis.shape}.\n")

candidate_paths_handin = [
    Path("../Data/gcp_order/dtu_findit/extraction_and_processing/handin_month_summary.csv"),
    Path("Data/gcp_order/dtu_findit/extraction_and_processing/handin_month_summary.csv"),
]

csv_path_handin = next((p for p in candidate_paths_handin if p.exists()), None)
if csv_path_handin is None:
    raise FileNotFoundError("Could not find handin_month_summary.csv in expected locations.")

handin_months = pd.read_csv(csv_path_handin, sep=";", engine="python", on_bad_lines="skip")
print(f"Loaded handin_months with {len(handin_months)} entries and shape {handin_months.shape}.\n")

merged_dfs = pd.merge(
    master_thesis_metrics_analysis,
    handin_months,
    left_on="pdf_file",
    right_on="filename",
    how="inner",
)

# drop the redundant 'filename' and 'corrupt_cid' columns after the merge
merged_dfs.drop(columns=["filename", "corrupt_cid"], inplace=True)

# rename column "extracted_month" to "handin_month"
#merged_dfs.rename(columns={"extracted_month": "handin_month"}, inplace=True)

master_thesis_metrics_analysis_v2 = merged_dfs

# export master_thesis_metrics_analysis_v2 to a new CSV file
output_path = Path("Data/gcp_order/dtu_findit/extraction_and_processing/master_thesis_metrics_analysis_v2.csv")
master_thesis_metrics_analysis_v2.to_csv(output_path, sep=";", index=False)
print(f"Exported merged DataFrame to: {output_path}")
import json
import pandas as pd
import csv
import re


### Imporing the JSON file with the departments and sections of DTU
with open('../Data/Departments_DTU_all.json', 'r') as f:
    dep = json.load(f)
    print(f"Loaded {len(dep)} departments from JSON file.")

### Importing the csv file with all metadata
csv_path = "../Data/Exploring_meta/thesis_meta_combined_filtered.csv"
df_csv = pd.read_csv(csv_path, sep=";", encoding="utf-8")

print("Loaded the metadata with a shape of: ", df_csv.shape)
#display(df_csv.head())

### Lookup from JSON (dep) and map Publisher -> Department_new in df_csv
def _flatten_text(value):
    """Return a flat list of strings from nested str/list/dict structures."""
    out = []
    if value is None:
        return out
    if isinstance(value, str):
        out.extend([v.strip() for v in value.split("|") if v.strip()])
    elif isinstance(value, list):
        for v in value:
            out.extend(_flatten_text(v))
    elif isinstance(value, dict):
        for v in value.values():
            out.extend(_flatten_text(v))
    return out

def _norm(s):
    s = str(s).strip().lower()
    s = re.sub(r"^dtu\s+", "", s)         # remove leading "DTU "
    s = re.sub(r"\s+", " ", s)            # normalize spaces
    return s

def _department_value(dep_item):
    d = dep_item.get("department")
    if isinstance(d, dict):
        return d.get("en") or d.get("da") or next((str(v) for v in d.values() if v), None)
    return d

# 1) Create alias -> department lookup from title + sections
alias_to_department = {}

for item in dep:
    department_val = _department_value(item)

    aliases = []
    aliases.extend(_flatten_text(item.get("title")))
    aliases.extend(_flatten_text(item.get("sections")))

    for a in aliases:
        alias_to_department[_norm(a)] = department_val

# 2) Map each Publisher to department
def map_publisher_to_department(publisher):
    if pd.isna(publisher):
        return pd.NA
    
    p_norm = _norm(publisher)

    # exact match first
    if p_norm in alias_to_department:
        return alias_to_department[p_norm]

    # fallback: contains match either direction
    for alias, dep_val in alias_to_department.items():
        if alias in p_norm or p_norm in alias:
            return dep_val

    return pd.NA

df_csv["Department_new"] = df_csv["Publisher"].apply(map_publisher_to_department)

print("Matched rows:", df_csv["Department_new"].notna().sum(), "/", len(df_csv))
display(df_csv[["Publisher", "Department_new"]].head(20))

# Optional: overwrite CSV with new column
df_csv.to_csv(csv_path, sep=";", encoding="utf-8", index=False)
print(f"Saved updated file: {csv_path}")

### Test for unmatched rows:
if df_csv["Department_new"].notna().sum() - len(df_csv) != 0:
    # Follow-up: show unmatched publishers (where Department_new is missing)
    print("Not all rows were matched, the unmatched rows are:")
    print()
    if "df_csv" not in globals():
        raise NameError("df_csv is not defined. Run the CSV loading cell first.")
    if "Department_new" not in df_csv.columns:
        raise KeyError("Department_new column not found. Run the mapping cell first.")

    unmatched_publishers = (
        df_csv.loc[df_csv["Department_new"].isna(), "Publisher"]
        .fillna("Missing")
        .astype(str)
        .str.strip()
        .replace("", "Missing")
        .value_counts()
        .rename_axis("Publisher")
        .reset_index(name="Count")
    )

    print(f"Unmatched rows: {unmatched_publishers['Count'].sum()} / {len(df_csv)}")
    display(unmatched_publishers.head(30))
import json
from pathlib import Path
import pandas as pd

txt_dir = Path("/Users/oliver/Desktop/MSc_Speciale/ThesisDataRepo/maks/data/thesis_txts")
appended_path = Path("Data/crawl2_files/meta_findit/meta_findit_all_merged_v2.csv")
manifest_path = Path("Data/crawl2_files/bulk_download_manifest.jsonl")


# 1) IDs from txt filenames
txt_ids = sorted({p.stem for p in txt_dir.glob("*.txt")})
txt_ids_set = set(txt_ids)
print("txt files:", len(txt_ids))

# 2) IDs covered by appended.csv (match on ID OR primary_member_id_s)
usecols = ["ID", "primary_member_id_s"]
md = pd.read_csv(appended_path, sep=";", usecols=usecols, dtype=str)

md = md.dropna(subset=["ID"]).copy()
md["ID"] = md["ID"].astype(str).str.strip()
md["primary_member_id_s"] = md["primary_member_id_s"].fillna("").astype(str).str.strip()

ids_by_id = set(md["ID"].unique())
ids_by_primary = set(x for x in md["primary_member_id_s"].unique() if x)

covered_by_appended = txt_ids_set & (ids_by_id | ids_by_primary)
missing_in_appended = sorted(txt_ids_set - (ids_by_id | ids_by_primary))

print("txt IDs covered by appended.csv (ID or primary_member_id_s):", len(covered_by_appended))
print("txt IDs missing in appended.csv (ID or primary_member_id_s):", len(missing_in_appended))
print("sample missing:", missing_in_appended[:20])

# 3) For the missing ones, check manifest coverage
manifest_ids = set()
with manifest_path.open("r", encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        rid = obj.get("rft_dat_id")
        if rid:
            manifest_ids.add(str(rid).strip())

missing_but_in_manifest = [x for x in missing_in_appended if x in manifest_ids]
missing_not_in_manifest = [x for x in missing_in_appended if x not in manifest_ids]

print("missing IDs found in manifest:", len(missing_but_in_manifest))
print("missing IDs NOT found in manifest:", missing_not_in_manifest[:50])

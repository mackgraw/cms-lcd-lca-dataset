from __future__ import annotations
import pandas as pd
from datetime import datetime

def compute_code_changes(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> pd.DataFrame:
    key = ["doc_id","code_system","code"]
    prev = prev_df[key + ["coverage_flag"]].drop_duplicates().set_index(key)
    curr = curr_df[key + ["coverage_flag"]].drop_duplicates().set_index(key)

    added_keys = curr.index.difference(prev.index)
    removed_keys = prev.index.difference(curr.index)
    common = curr.index.intersection(prev.index)

    rows = []
    for k in added_keys:
        rows.append({
            "doc_id": k[0], "code_system": k[1], "code": k[2],
            "change_type": "Added", "prev_flag": None, "curr_flag": curr.loc[k, "coverage_flag"],
            "change_date": datetime.utcnow().date().isoformat()
        })
    for k in removed_keys:
        rows.append({
            "doc_id": k[0], "code_system": k[1], "code": k[2],
            "change_type": "Removed", "prev_flag": prev.loc[k, "coverage_flag"], "curr_flag": None,
            "change_date": datetime.utcnow().date().isoformat()
        })
    for k in common:
        pv = prev.loc[k, "coverage_flag"]
        cv = curr.loc[k, "coverage_flag"]
        if str(pv) != str(cv):
            rows.append({
                "doc_id": k[0], "code_system": k[1], "code": k[2],
                "change_type": "FlagChanged", "prev_flag": pv, "curr_flag": cv,
                "change_date": datetime.utcnow().date().isoformat()
            })

    return pd.DataFrame(rows)

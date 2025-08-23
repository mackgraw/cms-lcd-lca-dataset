#!/usr/bin/env python3
import argparse
import csv
from collections import namedtuple

KEY = ("doc_type", "doc_id", "code_system", "code")

def read_norm(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def index_flags(rows):
    """Return dict[key_tuple] = coverage_flag (may be empty string)."""
    d = {}
    for row in rows:
        k = tuple(row.get(k, "") for k in KEY)
        d[k] = row.get("coverage_flag", "")
    return d

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prev", required=True, help="prev/codes_normalized.csv")
    ap.add_argument("--curr", required=True, help="dataset/codes_normalized.csv")
    ap.add_argument("--out", required=True, help="dataset/codes_changes.csv")
    args = ap.parse_args()

    prev = index_flags(read_norm(args.prev))
    curr = index_flags(read_norm(args.curr))

    # unified set of keys
    all_keys = set(prev.keys()) | set(curr.keys())

    out_cols = [
        "change_type", "doc_type", "doc_id",
        "code_system", "code",
        "prev_flag", "curr_flag"
    ]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_cols)
        w.writeheader()

        for k in sorted(all_keys):
            p = prev.get(k)
            c = curr.get(k)
            if p is None and c is not None:
                change = "Added"
            elif p is not None and c is None:
                change = "Removed"
            elif p != c:
                change = "FlagChanged"
            else:
                continue

            doc_type, doc_id, code_system, code = k
            w.writerow({
                "change_type": change,
                "doc_type": doc_type,
                "doc_id": doc_id,
                "code_system": code_system,
                "code": code,
                "prev_flag": "" if p is None else p,
                "curr_flag": "" if c is None else c,
            })

if __name__ == "__main__":
    main()

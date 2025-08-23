#!/usr/bin/env python3
"""
Combine per-shard CSVs produced by scripts/harvest_shard.py into:
  dataset/document_codes.csv
  dataset/document_nocodes.csv

Looks in --in (default: shards_in/**) for:
  document_codes_shard_*.csv
  document_nocodes_shard_*.csv
"""

import csv, glob, os
from pathlib import Path
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("--in",  dest="inp", default="shards_in", help="dir with downloaded shard artifacts")
ap.add_argument("--out", dest="out", default="dataset",   help="output dir (dataset)")
args = ap.parse_args()

inp = Path(args.inp)
out = Path(args.out)
out.mkdir(parents=True, exist_ok=True)

def cat(pattern_glob: str, out_path: Path, header: list[str]):
    files = sorted(glob.glob(pattern_glob, recursive=True))
    if not files:
        print(f"[warn] no matches for {pattern_glob}")
        return False
    print(f"[info] combining {len(files)} files -> {out_path}")
    with out_path.open("w", newline="", encoding="utf-8") as fout:
        w = csv.DictWriter(fout, fieldnames=header)
        w.writeheader()
        rows = 0
        for fp in files:
            with open(fp, newline="", encoding="utf-8") as fin:
                r = csv.DictReader(fin)
                # If incoming header differs in order, we still write by column name.
                for row in r:
                    w.writerow({k: row.get(k, "") for k in header})
                    rows += 1
        print(f"[done] wrote {rows} rows to {out_path}")
    return True

codes_header   = ["document_type","document_id","document_display_id","document_version","endpoint","row_json"]
nocodes_header = ["document_type","document_id","document_display_id","document_version","reason"]

cat(os.path.join(args.inp, "**", "document_codes_shard_*_of_*.csv"),
    out / "document_codes.csv", codes_header)

cat(os.path.join(args.inp, "**", "document_nocodes_shard_*_of_*.csv"),
    out / "document_nocodes.csv", nocodes_header)

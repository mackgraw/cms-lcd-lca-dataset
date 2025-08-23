#!/usr/bin/env python3
import csv, os, glob

DATASET_DIR  = os.environ.get("DATASET_DIR", "dataset")
OUT_DIR      = os.environ.get("SAMPLE_DIR", "SAMPLE")
SAMPLE_ROWS  = int(os.environ.get("SAMPLE_ROWS", "200"))

os.makedirs(OUT_DIR, exist_ok=True)
csv_files = sorted(glob.glob(os.path.join(DATASET_DIR, "*.csv")))
if not csv_files:
    raise SystemExit(f"No CSV files found in {DATASET_DIR}")

for path in csv_files:
    base = os.path.basename(path)
    out  = os.path.join(OUT_DIR, base)
    with open(path, newline="", encoding="utf-8") as fin, \
         open(out,  "w", newline="", encoding="utf-8") as fout:
        r = csv.reader(fin); w = csv.writer(fout)
        try:
            header = next(r)
        except StopIteration:
            header = []
        if header:
            w.writerow(header)
        for i, row in enumerate(r, start=1):
            if i > SAMPLE_ROWS: break
            w.writerow(row)

print(f"Created samples for {len(csv_files)} files in {OUT_DIR}/")

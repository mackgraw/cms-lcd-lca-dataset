#!/usr/bin/env python3
"""
Merge shard outputs into dataset/*.csv by filename.
- Looks under --in (default 'shards_in') for any *.csv
- Groups by basename and concatenates, keeping the first header only
- Writes to --out (default 'dataset')
"""
import csv, os, sys, glob
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("--in", dest="inp", default="shards_in")
ap.add_argument("--out", dest="out", default="dataset")
args = ap.parse_args()

os.makedirs(args.out, exist_ok=True)

# Collect all csv files from shard directories
csv_paths = [p for p in glob.glob(os.path.join(args.inp, "**", "*.csv"), recursive=True)]
if not csv_paths:
    print(f"No shard CSVs found under {args.inp}. If you ran fallback mode, only shard 0 copies data.", file=sys.stderr)

# Group by basename
groups = {}
for p in csv_paths:
    base = os.path.basename(p)
    groups.setdefault(base, []).append(p)

wrote_any = False
for base, files in sorted(groups.items()):
    out_path = os.path.join(args.out, base)
    print(f"Merging {len(files)} shard files into {out_path}")
    with open(out_path, "w", newline="", encoding="utf-8") as fout:
        writer = None
        for idx, fp in enumerate(files):
            with open(fp, newline="", encoding="utf-8") as fin:
                reader = csv.reader(fin)
                try:
                    header = next(reader)
                except StopIteration:
                    continue
                if writer is None:
                    writer = csv.writer(fout)
                    writer.writerow(header)
                else:
                    # skip header rows that are identical length (common case)
                    pass
                for row in reader:
                    writer.writerow(row)
    wrote_any = True

# If nothing merged (e.g., fallback shard 0 only), move shard_0 files into dataset/
if not wrote_any:
    shard0 = os.path.join(args.inp, "shard_0")
    if os.path.isdir(shard0):
        moved = 0
        for p in glob.glob(os.path.join(shard0, "*.csv")):
            dest = os.path.join(args.out, os.path.basename(p))
            print(f"Copy {p} -> {dest}")
            with open(p, "rb") as fin, open(dest, "wb") as fout:
                fout.write(fin.read())
            moved += 1
        if moved == 0:
            print("No CSVs to move from shard_0.", file=sys.stderr)
    else:
        print(f"{shard0} not found; nothing to merge.", file=sys.stderr)

print("Done.")

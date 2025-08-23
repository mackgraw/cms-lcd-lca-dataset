#!/usr/bin/env python3
import argparse
import csv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="dataset/document_codes.csv")
    ap.add_argument("--out", dest="out", required=True, help="dataset/codes_normalized.csv")
    args = ap.parse_args()

    cols_out = ["doc_type", "doc_id", "code_system", "code", "description", "coverage_flag"]

    with open(args.out, "w", newline="", encoding="utf-8") as fout:
        w = csv.DictWriter(fout, fieldnames=cols_out)
        w.writeheader()

        with open(args.inp, newline="", encoding="utf-8") as fin:
            r = csv.DictReader(fin)
            # Accept several likely column spellings from the shard output
            # and normalize to the stable schema.
            for row in r:
                doc_type = row.get("document_type") or row.get("doc_type") or ""
                doc_id = row.get("document_id") or row.get("doc_id") or row.get("article_id") or ""
                code_system = row.get("code_system") or ""
                code = row.get("code") or ""
                description = row.get("description") or ""
                coverage_flag = row.get("coverage_flag") or ""

                w.writerow({
                    "doc_type": doc_type,
                    "doc_id": str(doc_id),
                    "code_system": code_system,
                    "code": code,
                    "description": description,
                    "coverage_flag": coverage_flag,
                })

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Sharded coverage harvester wrapper (manifest-driven).

- Partitions repo files into SHARD_TOTAL buckets by hashing the path.
- Writes this shard's files to a manifest.
- Invokes your harvester with `--manifest <path>` so it ONLY processes those files.

Env:
  SHARD_INDEX (int) : 0-based index for this shard
  SHARD_TOTAL (int) : total number of shards
  SHARD_BATCH_SIZE  : optional, how many files per batch to send (default 200)
  HARVEST_CMD       : optional, command to run (default: python -m scripts.run_once)
"""
import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import List

def hash_mod(path: str, mod: int) -> int:
    h = hashlib.md5(path.encode("utf-8")).hexdigest()
    return int(h, 16) % mod

def repo_files() -> List[str]:
    # Prefer git ls-files for deterministic lists
    try:
        out = subprocess.check_output(["git", "ls-files"], text=True)
        files = [ln.strip() for ln in out.splitlines() if ln.strip()]
        return files
    except Exception:
        # Fallback to walking the tree
        return [str(p.as_posix()) for p in Path(".").rglob("*") if p.is_file()]

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def main():
    shard_index = int(os.environ.get("SHARD_INDEX", "0"))
    shard_total = int(os.environ.get("SHARD_TOTAL", "1"))
    batch_size  = int(os.environ.get("SHARD_BATCH_SIZE", "200"))
    harvest_cmd = os.environ.get("HARVEST_CMD", f"{sys.executable} -m scripts.run_once")

    files = repo_files()
    targeted = [f for f in files if hash_mod(f, shard_total) == shard_index]

    print(f"Total files in repo: {len(files)}")
    print(f"Shard {shard_index}/{shard_total}: {len(targeted)} files")

    logs_dir = Path(".harvest_logs"); logs_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir = Path(".harvest_manifests"); manifests_dir.mkdir(parents=True, exist_ok=True)

    overall_rc = 0
    for idx, batch in enumerate(chunked(targeted, batch_size), start=1):
        manifest_path = manifests_dir / f"manifest_shard{shard_index:02d}_batch{idx:03d}.txt"
        manifest_path.write_text("\n".join(batch), encoding="utf-8")
        print(f"[INFO] Shard {shard_index}: batch {idx} with {len(batch)} files -> {manifest_path}")

        # Run harvester with the manifest
        cmd = f'{harvest_cmd} --manifest "{manifest_path.as_posix()}"'
        print(f"[INFO] Running: {cmd}")
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            overall_rc = result.returncode
            print(f"[WARN] Harvester returned {result.returncode} for batch {idx}", file=sys.stderr)

    sys.exit(overall_rc)

if __name__ == "__main__":
    main()

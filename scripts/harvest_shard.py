#!/usr/bin/env python3
"""
Sharded coverage harvester wrapper.

Partitions repository files into SHARD_TOTAL buckets by hashing the file path.
Runs the underlying harvester for each batch of files that falls into this shard.

Environment variables:
  SHARD_INDEX (int) : which shard to run (0-based)
  SHARD_TOTAL (int) : how many shards in total
  SHARD_BATCH_SIZE  : optional, number of files per batch (default: 50)

Adjust the `run_harvest_for_files()` function if your entrypoint differs.
"""
import hashlib
import os
import subprocess
import sys
from pathlib import Path

def hash_mod(path: str, mod: int) -> int:
    h = hashlib.md5(path.encode("utf-8")).hexdigest()
    return int(h, 16) % mod

def get_repo_files() -> list[str]:
    try:
        out = subprocess.check_output(["git", "ls-files"], text=True)
        files = [line.strip() for line in out.splitlines() if line.strip()]
        # Optionally filter to certain extensions:
        # files = [f for f in files if f.endswith(('.py', '.md', '.rst'))]
        return files
    except Exception:
        # Fallback: walk filesystem if git is not available
        files = []
        for p in Path(".").rglob("*"):
            if p.is_file():
                files.append(str(p.as_posix()))
        return files

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def run_harvest_for_files(files: list[str]) -> int:
    """
    Change this to match your harvester CLI.
    Below we assume your main entry point is `python -m scripts.run_once`
    and that it can take a single file via `--file`.
    """
    rc = 0
    for f in files:
        print(f"::group::Harvest {f}")
        try:
            cmd = [sys.executable, "-m", "scripts.run_once", "--file", f]
            result = subprocess.run(cmd, check=False)
            if result.returncode != 0:
                print(f"❗ Harvester failed for {f} with code {result.returncode}", file=sys.stderr)
                rc = result.returncode
        finally:
            print("::endgroup::")
    return rc

def main():
    shard_index = int(os.environ.get("SHARD_INDEX", "0"))
    shard_total = int(os.environ.get("SHARD_TOTAL", "1"))
    batch_size = int(os.environ.get("SHARD_BATCH_SIZE", "50"))

    files = get_repo_files()
    targeted = [f for f in files if hash_mod(f, shard_total) == shard_index]

    print(f"Total files in repo: {len(files)}")
    print(f"Shard {shard_index}/{shard_total}: {len(targeted)} files")

    logs_dir = Path(".harvest_logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    overall_rc = 0
    for idx, batch in enumerate(chunked(targeted, batch_size), start=1):
        print(f"Running batch {idx} with {len(batch)} files")
        rc = run_harvest_for_files(batch)
        if rc != 0:
            overall_rc = rc

    sys.exit(overall_rc)

if __name__ == "__main__":
    main()

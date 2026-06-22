#!/usr/bin/env python3
"""
Regenerate manifest.json for a BF4 db-export bundle by scanning the existing
tables/*.parquet files.

Use this when a bundle was produced by multiple partial exports (e.g., a
full export + a focused `--table` resume + a manually merged parent), and
the only manifest covers a subset of files.

The new manifest is written in place. The existing manifest.json (if any)
is backed up as manifest.json.bak.

Usage:
    python regen_manifest.py --bundle /path/to/bundle
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--bundle", required=True,
        help="Bundle directory (must contain tables/ subdir)",
    )
    ap.add_argument(
        "--biofilter-version", default="4.1.4",
        help="BF4 version tag for the manifest",
    )
    ap.add_argument(
        "--schema-version", default=None,
        help="Schema version tag (defaults to --biofilter-version)",
    )
    ap.add_argument(
        "--engine", default="postgresql",
        help="Source engine name written to the manifest",
    )
    args = ap.parse_args()

    schema_version = args.schema_version or args.biofilter_version

    bundle = Path(args.bundle).resolve()
    tables_dir = bundle / "tables"
    if not tables_dir.is_dir():
        raise SystemExit(f"ERROR: {tables_dir} not found")

    entries = []
    print(f"Scanning {tables_dir}...")
    for f in sorted(tables_dir.glob("*.parquet")):
        try:
            pf = pq.ParquetFile(f)
            rows = pf.metadata.num_rows
        except Exception as exc:
            print(f"  WARN: {f.name} unreadable: {exc}")
            rows = None
        entries.append({
            "name": f.stem,
            "rows": rows,
            "file": f"tables/{f.name}",
        })
        rows_str = f"{rows:,}" if isinstance(rows, int) else "?"
        print(f"  {f.name:60s} rows={rows_str}")

    manifest = {
        "biofilter_version": args.biofilter_version,
        "schema_version": schema_version,
        "engine": args.engine,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tables": entries,
    }

    manifest_path = bundle / "manifest.json"
    if manifest_path.exists():
        backup = bundle / "manifest.json.bak"
        manifest_path.rename(backup)
        print(f"\nBacked up existing manifest → {backup.name}")

    with manifest_path.open("w") as fh:
        json.dump(manifest, fh, indent=2)

    total_rows = sum(e["rows"] for e in entries if isinstance(e["rows"], int))
    print(f"\nWrote {manifest_path}")
    print(f"  {len(entries)} tables, total {total_rows:,} rows")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
POC: Biofilter 4 annotation queries via DuckDB on the Parquet bundle.

Goal
----
Validate that the BF4 Parquet bundle (produced by `biofilter db export`)
can serve read-only annotation workloads directly through DuckDB, with
no SQLite/PG import phase, at acceptable performance.

The script runs an `annotation_master_variant`-style query against three
input sizes and reports wall-clock time + peak memory:

  - 5 rsIDs       (typical interactive query)
  - 100 rsIDs     (small batch)
  - 10,000 rsIDs  (large batch)

Setup
-----
Inside the BF4 venv on the LPC (or any machine with the bundle):

    pip install duckdb pyarrow         # if not already

Run
---

    python poc_duckdb_annotation.py \\
        --tables-dir /project/hall_shared/biofilter/databases/20260514/bundle/tables

Optional flags:

    --sizes 5,100,10000        comma-separated input sizes to test
    --threads 4                DuckDB worker threads (default: auto)
    --memory-limit 16GB        cap DuckDB memory (default: auto)
    --rsid-file <path>         use a real rsID list instead of random sampling

What the POC measures
---------------------

For each input size, runs three query "stages" against the bundle:

  Stage A — lookup variant ids by rsID via entity_aliases
  Stage B — Stage A + JOIN variant_molecular_effects (consequences)
  Stage C — Stage B + LEFT JOIN variant_gene_regulatory_evidence

Each stage prints elapsed seconds and result row count. Peak Python
memory is reported at the end of each size.

Interpretation
--------------

If Stage C completes in < 60 s for 10,000 rsIDs on cold cache, the
strategy is viable for the typical BF4 report workload. Sub-second for
5 rsIDs is expected.

Schema notes
------------

This script assumes the column names BF4 emits in 4.1.4. If column names
in the bundle differ (e.g., the schema evolved), the SQL will fail with
a clear "column not found" error. Adjust the SQL constants below to
match.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import tracemalloc
from pathlib import Path

import duckdb


# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------

# Which parquet files become DuckDB views. We use only the consolidated
# parents (no `_chr_N` children) — Stage 1 of the ADR-001 layout.
SKIP_PATTERNS = ("_chr_",)

# SQL fragments for the three stages. These match the column shapes
# produced by BF4 4.1.4's `db export`. Adjust if schemas evolve.

STAGE_A_SQL = """
SELECT
    vm.rsid                AS input_value,
    vm.variant_id,
    vm.chromosome,
    vm.position_start,
    vm.reference_allele,
    vm.alternate_allele
FROM variant_masters vm
WHERE vm.rsid IN ({placeholders})
"""

STAGE_B_SQL = """
WITH found AS (
    SELECT
        vm.rsid        AS input_value,
        vm.variant_id,
        vm.chromosome,
        vm.position_start
    FROM variant_masters vm
    WHERE vm.rsid IN ({placeholders})
)
SELECT
    f.input_value,
    f.variant_id,
    f.chromosome,
    f.position_start,
    vme.gene_symbol,
    vme.transcript_id,
    vme.consequence_raw
FROM found f
LEFT JOIN variant_molecular_effects vme
       ON vme.variant_id = f.variant_id
      AND vme.chromosome = f.chromosome
"""

STAGE_C_SQL = """
WITH found AS (
    SELECT
        vm.rsid        AS input_value,
        vm.variant_id,
        vm.chromosome
    FROM variant_masters vm
    WHERE vm.rsid IN ({placeholders})
),
effects AS (
    SELECT
        f.input_value,
        f.variant_id,
        f.chromosome,
        vme.gene_symbol,
        vme.transcript_id,
        vme.consequence_raw
    FROM found f
    LEFT JOIN variant_molecular_effects vme
           ON vme.variant_id = f.variant_id
          AND vme.chromosome = f.chromosome
)
SELECT
    e.*,
    vgre.gene_id          AS regulatory_gene_id,
    vgre.qtl_type,
    vgre.beta
FROM effects e
LEFT JOIN variant_gene_regulatory_evidence vgre
       ON vgre.variant_id = e.variant_id
      AND vgre.chromosome = e.chromosome
"""


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def log(msg: str) -> None:
    print(f"[poc] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def open_duckdb(threads: int | None, memory_limit: str | None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    if threads:
        con.execute(f"PRAGMA threads={threads}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    # Helpful for big aggregates over huge parquets
    con.execute("PRAGMA enable_object_cache=true")
    return con


def register_views(con: duckdb.DuckDBPyConnection, tables_dir: Path) -> list[str]:
    parquets = sorted(tables_dir.glob("*.parquet"))
    # Use only the consolidated parents — exclude per-chromosome children
    parquets = [
        p for p in parquets if not any(pat in p.stem for pat in SKIP_PATTERNS)
    ]
    if not parquets:
        raise SystemExit(f"No parquet files found in {tables_dir}")

    log(f"Registering {len(parquets)} parent views from {tables_dir}")
    for p in parquets:
        # Use POSIX path with forward slashes; works on Linux + macOS.
        path_str = str(p).replace("'", "''")
        con.execute(
            f"CREATE OR REPLACE VIEW {p.stem} AS "
            f"SELECT * FROM read_parquet('{path_str}')"
        )
    return [p.stem for p in parquets]


def sample_rsids(con: duckdb.DuckDBPyConnection, n: int) -> list[str]:
    """
    Pull N rsIDs from variant_masters. We use LIMIT rather than SAMPLE
    because (a) DuckDB SAMPLE doesn't accept parameterized N and (b) on
    a 152M-row parquet, true random sampling has to scan the whole file —
    LIMIT just grabs the first N rows that have an rsID, which is fine
    for a performance POC.
    """
    rows = con.execute(
        f"""
        SELECT rsid
        FROM variant_masters
        WHERE rsid IS NOT NULL
        LIMIT {int(n)}
        """
    ).fetchall()
    return [r[0] for r in rows]


def load_rsids_from_file(path: Path, n: int) -> list[str]:
    """Take the first N non-empty lines from a file as rsIDs."""
    out: list[str] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.append(line)
            if len(out) >= n:
                break
    return out


def run_stage(con, label: str, sql_template: str, rsids: list[str]) -> tuple[float, int]:
    # DuckDB doesn't bind list params into IN(...) easily, so quote inline.
    # rsIDs are alphanumeric — no SQL-injection concern with this dataset.
    quoted = ",".join(f"'{r}'" for r in rsids)
    sql = sql_template.format(placeholders=quoted)
    t0 = time.time()
    rows = con.execute(sql).fetchall()
    elapsed = time.time() - t0
    return elapsed, len(rows)


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="POC: BF4 annotation queries via DuckDB on Parquet bundle",
    )
    ap.add_argument(
        "--tables-dir", required=True, type=Path,
        help="Path to the bundle's tables/ directory (contains *.parquet)",
    )
    ap.add_argument(
        "--sizes", default="5,100,10000",
        help="Comma-separated input sizes to test (default: 5,100,10000)",
    )
    ap.add_argument(
        "--threads", type=int, default=None,
        help="DuckDB worker threads (default: auto)",
    )
    ap.add_argument(
        "--memory-limit", default=None,
        help="DuckDB memory limit, e.g. '16GB' (default: auto)",
    )
    ap.add_argument(
        "--rsid-file", type=Path, default=None,
        help="Read rsIDs from this file (one per line) instead of random sample",
    )
    ap.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for rsID sampling (default: 42)",
    )
    args = ap.parse_args()

    if not args.tables_dir.is_dir():
        log(f"ERROR: --tables-dir not a directory: {args.tables_dir}")
        return 1

    sizes = [int(s.strip()) for s in args.sizes.split(",") if s.strip()]
    random.seed(args.seed)

    log("Opening DuckDB connection (in-memory catalog, parquet on disk)")
    con = open_duckdb(args.threads, args.memory_limit)

    views = register_views(con, args.tables_dir)
    log(f"Sample of registered views: {views[:5]} ... (total {len(views)})")

    # Surface schema for the three tables our SQL hits, so failures are
    # easy to diagnose if columns evolved.
    for tbl in ("entity_aliases", "variant_masters",
                "variant_molecular_effects",
                "variant_gene_regulatory_evidence"):
        try:
            cols = con.execute(f"DESCRIBE {tbl}").fetchall()
            col_names = ", ".join(c[0] for c in cols[:8])
            log(f"  {tbl}: {col_names}{' ...' if len(cols) > 8 else ''}")
        except Exception as exc:
            log(f"  {tbl}: ERROR — {exc}")

    print()
    header = f"{'Size':>8s} {'Stage':>6s} {'Rows':>14s} {'Seconds':>10s} {'Peak MB':>10s}"
    print(header)
    print("=" * len(header))

    for size in sizes:
        log(f"--- size = {size} ---")

        # Get the rsID list (file or sample)
        if args.rsid_file:
            rsids = load_rsids_from_file(args.rsid_file, size)
            if len(rsids) < size:
                log(f"  WARN: rsid-file only had {len(rsids)} entries (asked for {size})")
        else:
            t0 = time.time()
            rsids = sample_rsids(con, size)
            log(f"  Sampled {len(rsids)} rsIDs in {time.time()-t0:.2f}s")

        if not rsids:
            log("  no rsIDs — skipping this size")
            continue

        tracemalloc.start()

        for stage_label, sql_tpl in (
            ("A", STAGE_A_SQL),
            ("B", STAGE_B_SQL),
            ("C", STAGE_C_SQL),
        ):
            try:
                seconds, rows = run_stage(con, stage_label, sql_tpl, rsids)
                _, peak = tracemalloc.get_traced_memory()
                peak_mb = peak / 1024 / 1024
                print(
                    f"{size:>8d} {stage_label:>6s} {rows:>14,d} "
                    f"{seconds:>10.2f} {peak_mb:>10.1f}"
                )
            except Exception as exc:
                print(
                    f"{size:>8d} {stage_label:>6s} {'ERROR':>14s} "
                    f"{'-':>10s} {'-':>10s}  ({exc})"
                )

        tracemalloc.stop()

    print()
    log("POC done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

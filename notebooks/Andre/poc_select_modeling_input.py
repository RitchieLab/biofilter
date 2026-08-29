#!/usr/bin/env python3
"""
Select the modeling input: coding variants filtered down to a tractable set.

Takes the per-variant CSV produced by bf4_coding_overlap.py (or any list of
chr:pos:ref:alt) and emits a variant list ready for `biofilter report run
--name variant_modeling --input-file <out>`.

Why this step exists
--------------------
variant_modeling is O(V^2) within pathway-connected gene pairs. Feeding it all
355,795 coding variants from the ADSP list produces ~4.3 billion variant pairs
(measured), which is not a report, it is a denial of service. Restricting to
variants that actually change the protein cuts the input by ~99% and is also
the scientifically defensible seed set for an interaction model.

Reads the BF4 Parquet bundle directly through DuckDB. No database server.

Usage
-----
    module load biofilter/4.2.0

    # protein-altering only (default) — the usual choice
    python bf4_select_modeling_input.py \
        --input adsp_per_variant.csv \
        --out modeling_input.txt \
        --out-detail modeling_input_detail.csv

    # everything with a coding consequence, synonymous included
    python bf4_select_modeling_input.py --input adsp_per_variant.csv \
        --groups missense,frameshift,stop,splice,start_stop,inframe,\
protein_altering,transcript_loss,coding_other,synonymous \
        --out modeling_input.txt

    # loss-of-function only — the smallest, highest-confidence seed set
    python bf4_select_modeling_input.py --input adsp_per_variant.csv \
        --groups frameshift,stop,start_stop,transcript_loss,splice \
        --out modeling_input.txt

Input
-----
Either the per-variant CSV from bf4_coding_overlap.py (detected by its
`input_variant` header; `coding_by_either` is honoured when present) or a plain
one-variant-per-line list. Variants must carry ref and alt — `chr:pos` alone
cannot be matched to an annotation.

Output
------
`--out`        one `chr:pos:ref:alt` per line, no header. Feed to --input-file.
`--out-detail` one row per (variant, gene) with the consequence that qualified
               it, so the seed set is auditable.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

# Consequence groups whose members change the protein product.
# 'synonymous' is deliberately excluded — it is a coding consequence that does
# not alter the amino acid sequence.
PROTEIN_ALTERING = [
    "missense",
    "frameshift",
    "stop",
    "splice",
    "start_stop",
    "inframe",
    "protein_altering",
    "transcript_loss",
    "coding_other",
]


def resolve_bundle(explicit: str | None) -> Path:
    raw = explicit or os.environ.get("BIOFILTER_DB_URI", "")
    if not raw:
        sys.exit(
            "No bundle path. Pass --bundle, or `module load biofilter/4.2.0` "
            "so BIOFILTER_DB_URI is set."
        )
    if raw.startswith("parquet://"):
        raw = "/" + raw[len("parquet://") :].lstrip("/")
    path = Path(raw).expanduser().resolve()
    if not path.is_dir():
        sys.exit(f"Bundle tables dir not found: {path}")
    return path


def table(bundle: Path, name: str) -> str:
    """DuckDB read_parquet() expression for a bundle table (flat or partitioned)."""
    flat = bundle / f"{name}.parquet"
    if flat.exists():
        return f"read_parquet('{flat}')"
    part = bundle / name
    if part.is_dir():
        return f"read_parquet('{part}/**/*.parquet', hive_partitioning = true)"
    sys.exit(f"Table not found in bundle: {name}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="per-variant CSV or plain variant list")
    ap.add_argument("--bundle", default=None, help="bundle tables/ dir")
    ap.add_argument(
        "--groups",
        default=",".join(PROTEIN_ALTERING),
        help="comma-separated variant_consequence_groups names to keep",
    )
    ap.add_argument(
        "--max-severity-rank",
        type=int,
        default=None,
        help="keep only consequences at or above this VEP severity rank "
        "(lower rank = more severe). Consequence GROUPS are coarse: 'splice' "
        "spans rank 2 (splice_acceptor, true LoF) through rank 18 "
        "(splice_polypyrimidine_tract, intronic and weak), and 'start_stop' "
        "holds both start_lost (7) and start_retained (20). Rank 14 is the "
        "natural protein-altering cutoff.",
    )
    ap.add_argument(
        "--canonical-only",
        action="store_true",
        help="only count consequences on canonical transcripts (drops predicted "
        "RefSeq XM_ transcripts; smaller and more conservative). NOTE: "
        "dtp_variant_gnomad does not populate this column — the script aborts "
        "rather than silently returning an empty seed set.",
    )
    ap.add_argument("--out", required=True, help="variant list for --input-file")
    ap.add_argument("--out-detail", default=None, help="optional audit CSV")
    ap.add_argument("--threads", type=int, default=4)
    args = ap.parse_args()

    bundle = resolve_bundle(args.bundle)
    src = Path(args.input).expanduser().resolve()
    if not src.exists():
        sys.exit(f"Input file not found: {src}")

    groups = [g.strip() for g in args.groups.split(",") if g.strip()]
    if not groups:
        sys.exit("--groups resolved to an empty list.")
    groups_sql = ", ".join(f"'{g}'" for g in groups)

    con = duckdb.connect()
    con.execute(f"SET threads = {args.threads}")
    con.execute("SET preserve_insertion_order = false")

    # ------------------------------------------------------------------
    # 1. Read the input, honouring the coding_by_either column when present
    # ------------------------------------------------------------------
    header = src.open(encoding="utf-8").readline()
    is_overlap_csv = "input_variant" in header

    if is_overlap_csv:
        where = "WHERE coding_by_either" if "coding_by_either" in header else ""
        con.execute(
            f"""
            CREATE TEMP TABLE input_raw AS
            SELECT input_variant AS raw
            FROM read_csv_auto('{src}') {where}
            """
        )
    else:
        con.execute(
            f"""
            CREATE TEMP TABLE input_raw AS
            SELECT trim(column0) AS raw
            FROM read_csv('{src}', header = false,
                          columns = {{'column0': 'VARCHAR'}},
                          ignore_errors = true, auto_detect = false)
            WHERE column0 IS NOT NULL AND trim(column0) <> ''
            """
        )

    con.execute(
        r"""
        CREATE TEMP TABLE variants AS
        SELECT
            raw,
            CASE upper(regexp_extract(raw, '^(?:chr)?([0-9]{1,2}|[XYxyMm][Tt]?)[:_\-]', 1))
                WHEN 'X' THEN 23 WHEN 'Y' THEN 24
                WHEN 'M' THEN 25 WHEN 'MT' THEN 25
                ELSE TRY_CAST(upper(regexp_extract(raw, '^(?:chr)?([0-9]{1,2}|[XYxyMm][Tt]?)[:_\-]', 1)) AS BIGINT)
            END AS chromosome,
            TRY_CAST(regexp_extract(raw, '^(?:chr)?(?:[0-9]{1,2}|[XYxyMm][Tt]?)[:_\-](\d+)', 1) AS BIGINT) AS pos,
            nullif(upper(regexp_extract(raw, '^[^:]+:\d+:([A-Za-z*\-]+):', 1)), '') AS ref,
            nullif(upper(regexp_extract(raw, '^[^:]+:\d+:[A-Za-z*\-]+:([A-Za-z*\-]+)', 1)), '') AS alt
        FROM input_raw
        """
    )

    n_in = con.execute("SELECT count(*) FROM variants").fetchone()[0]
    n_alleles = con.execute(
        "SELECT count(*) FROM variants WHERE ref IS NOT NULL AND alt IS NOT NULL"
    ).fetchone()[0]

    # ------------------------------------------------------------------
    # 2. Match to variant_masters, then to the consequence dimension
    # ------------------------------------------------------------------
    vm = table(bundle, "variant_masters")
    vme = table(bundle, "variant_molecular_effects")
    vc = table(bundle, "variant_consequences")
    vcg = table(bundle, "variant_consequence_groups")

    canonical_clause = ""
    if args.canonical_only:
        # dtp_variant_gnomad does not populate `canonical` (verified 100% NULL
        # over 140.6 M chr1 rows). Filtering on it silently yields nothing, and
        # an empty seed set downstream reads as "no protein-altering variants"
        # rather than "the column is empty". Fail loudly instead.
        has_canonical = con.execute(
            f"SELECT count(canonical) FROM {vme} LIMIT 1"
        ).fetchone()[0]
        if not has_canonical:
            sys.exit(
                "--canonical-only requested, but variant_molecular_effects."
                "canonical is NULL throughout this bundle (dtp_variant_gnomad "
                "never populates it). Re-run without --canonical-only, and use "
                "--max-severity-rank 14 to tighten the seed set instead."
            )
        canonical_clause = "AND coalesce(e.canonical, false)"

    severity_clause = ""
    if args.max_severity_rank is not None:
        severity_clause = f"AND c.severity_rank <= {args.max_severity_rank}"

    con.execute(
        f"""
        CREATE TEMP TABLE selected AS
        WITH matched AS (
            SELECT v.raw, v.chromosome,
                   TRY_CAST(m.variant_id AS BIGINT) AS variant_id
            FROM variants v
            JOIN {vm} m
              ON TRY_CAST(m.chromosome AS BIGINT) = v.chromosome
             AND m.position_start = v.pos
             AND upper(m.reference_allele) = v.ref
             AND upper(m.alternate_allele) = v.alt
            WHERE v.ref IS NOT NULL AND v.alt IS NOT NULL
        )
        SELECT DISTINCT
            mt.raw,
            e.gene_symbol,
            c.name           AS consequence,
            c.severity_rank  AS severity_rank,
            g.name           AS consequence_group,
            e.transcript_id,
            e.canonical,
            e.mane_select
        FROM matched mt
        JOIN {vme} e
          ON TRY_CAST(e.chromosome AS BIGINT) = mt.chromosome
         AND e.variant_id = mt.variant_id
         {canonical_clause}
        JOIN {vc}  c ON c.id = e.consequence_id
        JOIN {vcg} g ON g.id = c.consequence_group_id
        WHERE g.name IN ({groups_sql})
          {severity_clause}
        """
    )

    n_kept = con.execute("SELECT count(DISTINCT raw) FROM selected").fetchone()[0]
    n_genes = con.execute(
        "SELECT count(DISTINCT gene_symbol) FROM selected WHERE gene_symbol IS NOT NULL"
    ).fetchone()[0]

    # ------------------------------------------------------------------
    # 3. Report
    # ------------------------------------------------------------------
    pct = (100.0 * n_kept / n_alleles) if n_alleles else 0.0
    print()
    print(f"Bundle          : {bundle}")
    print(f"Input           : {src}")
    print(f"Groups kept     : {', '.join(groups)}")
    print(f"Max severity    : {args.max_severity_rank or 'no limit'}")
    print(f"Canonical only  : {args.canonical_only}")
    print("-" * 60)
    print(f"input variants           : {n_in:,}")
    print(f"  with ref/alt           : {n_alleles:,}")
    print(f"selected variants        : {n_kept:,}  ({pct:.2f}%)")
    print(f"distinct genes hit       : {n_genes:,}")
    print("-" * 60)

    # Per-consequence, not per-group. The group level hides that 'splice'
    # mixes rank-2 loss-of-function with rank-18 intronic proximity calls,
    # and that 'start_stop' mixes start_lost with the silent start_retained.
    print("\nBreakdown by consequence (a variant can appear under several):")
    print(
        con.execute(
            """
            SELECT severity_rank, consequence, consequence_group,
                   count(DISTINCT raw) AS variants
            FROM selected GROUP BY 1, 2, 3 ORDER BY severity_rank
            """
        ).df().to_string(index=False)
    )

    # Pair-count estimate: the number that decides whether the model is runnable.
    est = con.execute(
        """
        WITH per_gene AS (
            SELECT gene_symbol, count(DISTINCT raw) AS n
            FROM selected WHERE gene_symbol IS NOT NULL GROUP BY 1
        )
        SELECT sum(n) AS variant_gene_links, round(avg(n), 2) AS avg_per_gene,
               max(n) AS max_per_gene
        FROM per_gene
        """
    ).df()
    print("\nSeed density (drives the pair count in variant_modeling):")
    print(est.to_string(index=False))

    con.execute(
        f"COPY (SELECT DISTINCT raw FROM selected ORDER BY raw) "
        f"TO '{args.out}' (HEADER false, DELIMITER ',')"
    )
    print(f"\nmodeling input -> {args.out}   ({n_kept:,} variants)")

    if args.out_detail:
        con.execute(
            f"""COPY (
                SELECT raw AS input_variant, gene_symbol, consequence,
                       severity_rank, consequence_group, transcript_id,
                       canonical, mane_select
                FROM selected ORDER BY severity_rank, gene_symbol, raw
            ) TO '{args.out_detail}' (HEADER, DELIMITER ',')"""
        )
        print(f"audit detail   -> {args.out_detail}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
How many input variants fall inside a protein-coding gene?

Definition used (option C): positional overlap between the variant position
and the gene body interval (GRCh38) of a gene whose HGNC locus_group is
"protein-coding gene". This is annotation-independent: it does not require the
variant to exist in gnomAD, so every input line gets classified.

Reads the BF4 Parquet bundle directly through DuckDB. No database server.

Usage
-----
    module load biofilter/4.2.0
    python bf4_coding_overlap.py --input adsp_variants.csv --window 0

    # explicit bundle path (otherwise taken from $BIOFILTER_DB_URI)
    python bf4_coding_overlap.py \
        --input adsp_variants.csv \
        --bundle /project/hall_shared/biofilter/databases/<date>/bundle/tables \
        --window 0 \
        --out-summary summary.csv \
        --out-detail per_variant.csv

Input format
------------
One variant per line, first column used. Accepted:
    1:633963:C:T      chr1:633963:C:T      1:633963
X / Y / MT are mapped to 23 / 24 / 25. Anything that does not parse (a header
line, a blank field, a malformed ID) is counted under `unparseable` and
excluded from the percentages — check that number before trusting the result.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb

BUILD = 38
CODING_LOCUS_GROUP = "protein-coding gene"


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
    ap.add_argument("--input", required=True, help="CSV/TXT with one variant per line")
    ap.add_argument("--bundle", default=None, help="bundle tables/ dir")
    ap.add_argument(
        "--window",
        type=int,
        default=0,
        help="bp added to each side of the gene body (default 0 = gene body only)",
    )
    ap.add_argument("--out-summary", default="coding_overlap_summary.csv")
    ap.add_argument(
        "--out-detail",
        default=None,
        help="optional per-variant CSV (one row per input variant)",
    )
    ap.add_argument(
        "--with-annotation",
        action="store_true",
        help="also classify via gnomAD VEP transcript annotations "
        "(follows RefSeq/Ensembl transcript models; needs the variant in the DB)",
    )
    ap.add_argument("--threads", type=int, default=4)
    args = ap.parse_args()

    bundle = resolve_bundle(args.bundle)
    src = Path(args.input).expanduser().resolve()
    if not src.exists():
        sys.exit(f"Input file not found: {src}")

    con = duckdb.connect()
    con.execute(f"SET threads = {args.threads}")
    con.execute("SET preserve_insertion_order = false")

    # ------------------------------------------------------------------
    # 1. Parse the input list
    # ------------------------------------------------------------------
    con.execute(
        f"""
        CREATE TEMP TABLE input_raw AS
        SELECT trim(column0) AS raw
        FROM read_csv('{src}', header = false, columns = {{'column0': 'VARCHAR'}},
                      ignore_errors = true, auto_detect = false)
        WHERE column0 IS NOT NULL AND trim(column0) <> ''
        """
    )
    # Drop a header line if the first token is not variant-shaped.
    con.execute(
        r"""
        CREATE TEMP TABLE variants AS
        WITH parsed AS (
            SELECT
                raw,
                upper(regexp_extract(raw, '^(?:chr)?([0-9]{1,2}|[XYxyMm][Tt]?)[:_\-]', 1)) AS chr_txt,
                TRY_CAST(regexp_extract(raw, '^(?:chr)?(?:[0-9]{1,2}|[XYxyMm][Tt]?)[:_\-](\d+)', 1) AS BIGINT) AS pos
            FROM input_raw
        )
        SELECT
            raw,
            CASE chr_txt
                WHEN 'X' THEN 23 WHEN 'Y' THEN 24
                WHEN 'M' THEN 25 WHEN 'MT' THEN 25
                ELSE TRY_CAST(chr_txt AS BIGINT)
            END AS chromosome,
            pos,
            nullif(upper(regexp_extract(raw, '^[^:]+:\d+:([A-Za-z*\-]+):', 1)), '') AS ref,
            nullif(upper(regexp_extract(raw, '^[^:]+:\d+:[A-Za-z*\-]+:([A-Za-z*\-]+)', 1)), '') AS alt
        FROM parsed
        """
    )

    total = con.execute("SELECT count(*) FROM variants").fetchone()[0]
    parsed_ok = con.execute(
        "SELECT count(*) FROM variants WHERE chromosome BETWEEN 1 AND 25 AND pos > 0"
    ).fetchone()[0]
    distinct_in = con.execute(
        "SELECT count(DISTINCT (chromosome, pos)) FROM variants "
        "WHERE chromosome BETWEEN 1 AND 25 AND pos > 0"
    ).fetchone()[0]

    # ------------------------------------------------------------------
    # 2. Gene intervals, split by locus group
    # ------------------------------------------------------------------
    el = table(bundle, "entity_locations")
    gm = table(bundle, "gene_masters")
    glg = table(bundle, "gene_locus_groups")

    con.execute(
        f"""
        CREATE TEMP TABLE gene_intervals AS
        SELECT
            g.entity_id,
            g.symbol,
            lg.name              AS locus_group,
            l.chromosome         AS chromosome,
            l.start_pos - {args.window} AS win_start,
            l.end_pos   + {args.window} AS win_end
        FROM {gm} g
        JOIN {el} l  ON l.entity_id = g.entity_id AND l.build = {BUILD}
        LEFT JOIN {glg} lg ON lg.id = g.locus_group_id
        WHERE l.chromosome IS NOT NULL
          AND l.start_pos IS NOT NULL
          AND l.end_pos IS NOT NULL
        """
    )
    coding_genes = con.execute(
        "SELECT count(DISTINCT entity_id) FROM gene_intervals WHERE locus_group = ?",
        [CODING_LOCUS_GROUP],
    ).fetchone()[0]

    # ------------------------------------------------------------------
    # 3. Range join — one row per input variant
    # ------------------------------------------------------------------
    con.execute(
        f"""
        CREATE TEMP TABLE hits AS
        SELECT
            v.raw,
            v.chromosome,
            v.pos,
            count(DISTINCT CASE WHEN gi.locus_group = '{CODING_LOCUS_GROUP}'
                                THEN gi.entity_id END)                       AS n_coding_genes,
            count(DISTINCT gi.entity_id)                                     AS n_any_genes,
            string_agg(DISTINCT CASE WHEN gi.locus_group = '{CODING_LOCUS_GROUP}'
                                     THEN gi.symbol END, ';')                AS coding_gene_symbols,
            string_agg(DISTINCT gi.locus_group, ';')                         AS locus_groups_hit
        FROM variants v
        LEFT JOIN gene_intervals gi
               ON gi.chromosome = v.chromosome
              AND v.pos BETWEEN gi.win_start AND gi.win_end
        WHERE v.chromosome BETWEEN 1 AND 25 AND v.pos > 0
        GROUP BY 1, 2, 3
        """
    )

    in_coding, in_gene_noncoding, intergenic = con.execute(
        """
        SELECT
            count(*) FILTER (WHERE n_coding_genes > 0),
            count(*) FILTER (WHERE n_coding_genes = 0 AND n_any_genes > 0),
            count(*) FILTER (WHERE n_any_genes = 0)
        FROM hits
        """
    ).fetchone()

    genes_touched = con.execute(
        f"""
        SELECT count(DISTINCT gi.entity_id)
        FROM variants v
        JOIN gene_intervals gi
          ON gi.chromosome = v.chromosome
         AND v.pos BETWEEN gi.win_start AND gi.win_end
        WHERE gi.locus_group = '{CODING_LOCUS_GROUP}'
        """
    ).fetchone()[0]

    # ------------------------------------------------------------------
    # 3b. Annotation-based classification (optional)
    #
    # Uses the VEP transcript annotations gnomAD ships (variant_molecular_
    # effects). This follows RefSeq/Ensembl *transcript* models rather than
    # the gene-body interval, so it catches variants that fall in extended
    # transcripts reaching outside the canonical gene span. Requires the
    # variant to exist in variant_masters (exact chr/pos/ref/alt).
    # ------------------------------------------------------------------
    ann = {}
    if args.with_annotation:
        vm = table(bundle, "variant_masters")
        vme = table(bundle, "variant_molecular_effects")
        vb = table(bundle, "variant_biotypes")

        con.execute(
            f"""
            CREATE TEMP TABLE ann_hits AS
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
            SELECT
                mt.raw,
                count(DISTINCT CASE WHEN b.name = 'protein_coding'
                                    THEN e.gene_symbol END) AS n_coding_genes_vep,
                string_agg(DISTINCT CASE WHEN b.name = 'protein_coding'
                                         THEN e.gene_symbol END, ';') AS vep_gene_symbols
            FROM matched mt
            LEFT JOIN {vme} e
                   ON TRY_CAST(e.chromosome AS BIGINT) = mt.chromosome
                  AND e.variant_id = mt.variant_id
            LEFT JOIN {vb} b ON b.id = e.biotype_id
            GROUP BY 1
            """
        )
        matched_db = con.execute("SELECT count(*) FROM ann_hits").fetchone()[0]
        coding_vep = con.execute(
            "SELECT count(*) FROM ann_hits WHERE n_coding_genes_vep > 0"
        ).fetchone()[0]
        either, only_vep, only_pos = con.execute(
            """
            SELECT
              count(*) FILTER (WHERE h.n_coding_genes > 0
                                  OR coalesce(a.n_coding_genes_vep, 0) > 0),
              count(*) FILTER (WHERE h.n_coding_genes = 0
                                 AND coalesce(a.n_coding_genes_vep, 0) > 0),
              count(*) FILTER (WHERE h.n_coding_genes > 0
                                 AND coalesce(a.n_coding_genes_vep, 0) = 0)
            FROM hits h LEFT JOIN ann_hits a ON a.raw = h.raw
            """
        ).fetchone()
        ann = {
            "matched_in_variant_masters": matched_db,
            "not_in_variant_masters": parsed_ok - matched_db,
            "coding_by_vep_annotation": coding_vep,
            "coding_by_either_method": either,
            "coding_by_vep_only": only_vep,
            "coding_by_position_only": only_pos,
        }

    # ------------------------------------------------------------------
    # 4. Report
    # ------------------------------------------------------------------
    pct = (100.0 * in_coding / parsed_ok) if parsed_ok else 0.0
    summary = [
        ("input_lines", total),
        ("parsed_ok", parsed_ok),
        ("unparseable", total - parsed_ok),
        ("distinct_positions", distinct_in),
        ("window_bp", args.window),
        ("build", BUILD),
        ("protein_coding_genes_in_bundle", coding_genes),
        ("variants_in_protein_coding_gene", in_coding),
        ("variants_in_protein_coding_gene_pct", round(pct, 2)),
        ("variants_in_other_gene_only", in_gene_noncoding),
        ("variants_intergenic", intergenic),
        ("distinct_protein_coding_genes_hit", genes_touched),
    ]
    summary += list(ann.items())

    width = max(len(k) for k, _ in summary)
    print()
    print(f"Bundle : {bundle}")
    print(f"Input  : {src}")
    print("-" * (width + 20))
    for k, v in summary:
        print(f"{k:<{width}} : {v}")
    print("-" * (width + 20))

    con.execute(
        "CREATE TEMP TABLE summary_t (metric VARCHAR, value VARCHAR)"
    )
    con.executemany(
        "INSERT INTO summary_t VALUES (?, ?)", [(k, str(v)) for k, v in summary]
    )
    con.execute(f"COPY summary_t TO '{args.out_summary}' (HEADER, DELIMITER ',')")
    print(f"summary -> {args.out_summary}")

    print("\nBreakdown of what the non-coding hits landed on:")
    print(
        con.execute(
            """
            SELECT locus_groups_hit AS locus_groups, count(*) AS variants
            FROM hits
            WHERE n_coding_genes = 0 AND n_any_genes > 0
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
            """
        ).df().to_string(index=False)
    )

    if args.out_detail:
        if args.with_annotation:
            detail_sql = """
                SELECT h.raw AS input_variant, h.chromosome, h.pos AS position,
                       h.n_coding_genes > 0        AS coding_by_position,
                       coalesce(a.n_coding_genes_vep, 0) > 0
                                                   AS coding_by_annotation,
                       (h.n_coding_genes > 0
                        OR coalesce(a.n_coding_genes_vep, 0) > 0)
                                                   AS coding_by_either,
                       a.raw IS NOT NULL           AS found_in_db,
                       h.coding_gene_symbols       AS position_gene_symbols,
                       a.vep_gene_symbols          AS annotation_gene_symbols,
                       h.n_any_genes, h.locus_groups_hit
                FROM hits h
                LEFT JOIN ann_hits a ON a.raw = h.raw
                ORDER BY h.chromosome, h.pos
            """
        else:
            detail_sql = """
                SELECT raw AS input_variant, chromosome, pos AS position,
                       n_coding_genes > 0 AS coding_by_position,
                       coding_gene_symbols AS position_gene_symbols,
                       n_any_genes, locus_groups_hit
                FROM hits ORDER BY chromosome, pos
            """
        con.execute(
            f"COPY ({detail_sql}) TO '{args.out_detail}' (HEADER, DELIMITER ',')"
        )
        print(f"\nper-variant -> {args.out_detail}")


if __name__ == "__main__":
    main()

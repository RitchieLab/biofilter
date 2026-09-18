"""
Build a SUBSET parquet bundle from biofilter_dev for the read-only spike.

Layout mirrors what export_full_clone() produces today (flat: one .parquet
per parent table), so the spike measures the CURRENT parquet:// backend
without conflating results with the future partitioning change.

Variant tables are windowed to keep the bundle small while still carrying
real, referentially-consistent data.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

PG_URI = os.environ.get(
    "SPIKE_PG_URI", "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
)

# Window on chr1 chosen to sit inside a gene-dense region.
CHROM = 1
POS_LO = 1_000_000
POS_HI = 6_000_000

OUT = Path(sys.argv[1] if len(sys.argv) > 1 else "./spike_bundle")
TABLES_DIR = OUT / "tables"

# Parent/plain tables (partition children excluded — the flat bundle stores
# the consolidated parent, which is what the ORM addresses).
FULL_TABLES = [
    "biofilter_metadata",
    "chemical_masters",
    "disease_group_memberships",
    "disease_groups",
    "disease_masters",
    "entities",
    "entity_aliases",
    "entity_groups",
    "entity_locations",
    "entity_relationship_types",
    "entity_relationships",
    "etl_data_sources",
    "etl_packages",
    "etl_source_systems",
    "gene_group_memberships",
    "gene_groups",
    "gene_locus_groups",
    "gene_locus_types",
    "gene_masters",
    "genome_assemblies",
    "go_masters",
    "go_relations",
    "omic_status",
    "pathway_masters",
    "protein_entities",
    "protein_masters",
    "protein_pfam_links",
    "protein_pfams",
    "system_config",
    "variant_biotypes",
    "variant_consequence_categories",
    "variant_consequence_groups",
    "variant_consequences",
    "variant_impacts",
    "variant_gwas",
    "variant_gwas_snp",
    "variant_snp_merges",
]

# Windowed by (chromosome, position) directly.
WINDOWED_BY_POS = ["variant_masters", "variant_regulatory_elements"]

# Windowed indirectly: keyed to the variant_ids selected above.
WINDOWED_BY_VARIANT = [
    "variant_molecular_effects",
    "variant_effect_predictions",
    "variant_gene_regulatory_evidence",
]

CHUNK = 250_000


def has_columns(conn, table: str, cols: list[str]) -> bool:
    got = (
        conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=:t"
            ),
            {"t": table},
        )
        .scalars()
        .all()
    )
    return all(c in got for c in cols)


def stream_to_parquet(conn, sql: str, out_path: Path) -> int:
    """Stream a query to a single parquet file. Returns row count."""
    writer = None
    schema = None
    total = 0
    sconn = conn.execution_options(stream_results=True)
    try:
        for chunk in pd.read_sql(text(sql), sconn, chunksize=CHUNK):
            tbl = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                # Promote all-null columns to string so later chunks cast.
                if any(pa.types.is_null(f.type) for f in tbl.schema):
                    fields, cols = [], []
                    for i, f in enumerate(tbl.schema):
                        if pa.types.is_null(f.type):
                            fields.append(
                                pa.field(f.name, pa.string(), nullable=True)
                            )
                            cols.append(tbl.column(i).cast(pa.string()))
                        else:
                            fields.append(f)
                            cols.append(tbl.column(i))
                    tbl = pa.Table.from_arrays(cols, schema=pa.schema(fields))
                schema = tbl.schema
                writer = pq.ParquetWriter(
                    str(out_path), schema=schema, compression="zstd"
                )
            elif tbl.schema != schema:
                tbl = tbl.cast(schema, safe=False)
            writer.write_table(tbl)
            total += len(chunk)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        pd.DataFrame().to_parquet(out_path, index=False)
    return total


def main() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    engine = create_engine(PG_URI)
    meta: list[dict] = []

    with engine.connect() as conn:
        # 1) Full small tables
        for t in FULL_TABLES:
            out = TABLES_DIR / f"{t}.parquet"
            n = stream_to_parquet(conn, f'SELECT * FROM "{t}"', out)
            meta.append(
                {
                    "name": t,
                    "rows": n,
                    "file": f"tables/{t}.parquet",
                    "subset": "full",
                }
            )
            print(f"  {t:42s} {n:>10,}")

        # 2) Position-windowed variant tables
        for t in WINDOWED_BY_POS:
            if not has_columns(conn, t, ["chromosome"]):
                print(f"  {t:42s} SKIPPED (no chromosome col)")
                continue
            poscol = (
                "position_start"
                if has_columns(conn, t, ["position_start"])
                else None
            )
            where = f"chromosome = {CHROM}"
            if poscol:
                where += f" AND {poscol} BETWEEN {POS_LO} AND {POS_HI}"
            out = TABLES_DIR / f"{t}.parquet"
            n = stream_to_parquet(
                conn, f'SELECT * FROM "{t}" WHERE {where}', out
            )
            meta.append(
                {
                    "name": t,
                    "rows": n,
                    "file": f"tables/{t}.parquet",
                    "subset": f"chr{CHROM}:{POS_LO}-{POS_HI}",
                }
            )
            print(f"  {t:42s} {n:>10,}  (windowed)")

        # 3) Variant-id-keyed tables, joined back to the windowed masters
        vm_filter = (
            f"SELECT variant_id FROM variant_masters "
            f"WHERE chromosome = {CHROM} "
            f"AND position_start BETWEEN {POS_LO} AND {POS_HI}"
        )
        for t in WINDOWED_BY_VARIANT:
            if not has_columns(conn, t, ["chromosome", "variant_id"]):
                print(f"  {t:42s} SKIPPED (no chromosome/variant_id)")
                continue
            sql = (
                f'SELECT * FROM "{t}" '
                f"WHERE chromosome = {CHROM} AND variant_id IN ({vm_filter})"
            )
            out = TABLES_DIR / f"{t}.parquet"
            n = stream_to_parquet(conn, sql, out)
            meta.append(
                {
                    "name": t,
                    "rows": n,
                    "file": f"tables/{t}.parquet",
                    "subset": f"chr{CHROM} variants in window",
                }
            )
            print(f"  {t:42s} {n:>10,}  (windowed)")

    manifest = {
        "biofilter_version": "4.2.0",
        "schema_version": "spike",
        "engine": "postgresql",
        "created_at": "spike",
        "note": f"SUBSET bundle for read-only spike. Window chr{CHROM}:{POS_LO}-{POS_HI}",
        "tables": meta,
    }
    (OUT / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(f"\nBundle written to {OUT.resolve()}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Merge variant_molecular_effects_chr_*.parquet (25 partition children) into a
single consolidated variant_molecular_effects.parquet.

This replaces the BF4 db-export's aggregated-parent file, which is too slow
to generate via PG's UNION over 25 partitions.

Reads each child in turn and writes incrementally to the parent file using
ParquetWriter (streaming append). Schema is taken from the first non-empty
child and used to cast subsequent ones — handles the null-column promotion
case the same way the transfer.py patch does.

Usage (inside the venv):
    python merge_variant_molecular_effects.py \\
        --in /project/hall_shared/biofilter/databases/20260514/bundle/tables \\
        --out /project/hall_shared/biofilter/databases/20260514/bundle/tables/variant_molecular_effects.parquet
"""

import argparse
import sys
import time
from glob import glob
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def log(msg):
    print(f"[merge] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def promote_null_columns(table: pa.Table) -> pa.Table:
    """Replace null-type columns with nullable string so later chunks cast cleanly."""
    if not any(pa.types.is_null(f.type) for f in table.schema):
        return table

    new_fields = []
    new_columns = []
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            new_fields.append(pa.field(field.name, pa.string(), nullable=True))
            new_columns.append(table.column(i).cast(pa.string()))
        else:
            new_fields.append(field)
            new_columns.append(table.column(i))

    return pa.Table.from_arrays(new_columns, schema=pa.schema(new_fields))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--in", "-i", dest="in_dir", required=True,
        help="Directory containing variant_molecular_effects_chr_*.parquet files",
    )
    ap.add_argument(
        "--out", "-o", dest="out_path", required=True,
        help="Output path for consolidated variant_molecular_effects.parquet",
    )
    ap.add_argument(
        "--prefix", default="variant_molecular_effects_chr_",
        help="Filename prefix for partition children",
    )
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_path = Path(args.out_path)

    pattern = str(in_dir / f"{args.prefix}*.parquet")
    files = sorted(
        glob(pattern),
        key=lambda p: int(p.rsplit("_chr_", 1)[1].split(".")[0]),
    )

    if not files:
        log(f"ERROR: no files matched {pattern}")
        sys.exit(1)

    log(f"Found {len(files)} partition files")
    log(f"Output: {out_path}")

    writer = None
    schema = None
    total_rows = 0
    batch_size = 200_000
    t0 = time.time()

    for f in files:
        ti = time.time()
        pf = pq.ParquetFile(f)
        file_rows_target = pf.metadata.num_rows
        size_mb = Path(f).stat().st_size / 1024 / 1024
        rows_written = 0

        # Stream the source parquet in batches so we never materialize the
        # whole file in memory. chr_1 alone has ~140M rows.
        for batch in pf.iter_batches(batch_size=batch_size):
            batch_table = pa.Table.from_batches([batch])

            if writer is None:
                batch_table = promote_null_columns(batch_table)
                schema = batch_table.schema
                writer = pq.ParquetWriter(str(out_path), schema=schema)
            elif batch_table.schema != schema:
                try:
                    batch_table = batch_table.cast(schema, safe=False)
                except Exception:
                    # Promote nulls in the incoming batch and try again
                    batch_table = promote_null_columns(batch_table)
                    try:
                        batch_table = batch_table.cast(schema, safe=False)
                    except Exception as exc2:
                        log(
                            f"  ERROR: schema cast failed for "
                            f"{Path(f).name}: {exc2}"
                        )
                        log("  skipping remaining batches of this file")
                        break

            writer.write_table(batch_table)
            rows_written += batch_table.num_rows

        total_rows += rows_written
        log(
            f"  + {Path(f).name:50s} "
            f"rows={rows_written:>12,}/{file_rows_target:<12,} "
            f"size={size_mb:>7.1f}MB "
            f"elapsed={time.time()-ti:.1f}s"
        )

    if writer is not None:
        writer.close()

    out_size_mb = out_path.stat().st_size / 1024 / 1024
    log(
        f"Done. Wrote {total_rows:,} rows, "
        f"{out_size_mb:.1f}MB, total {time.time()-t0:.1f}s"
    )


if __name__ == "__main__":
    main()

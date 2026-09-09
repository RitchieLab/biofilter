"""
Per-chromosome parquet output for the variant DTPs.

Under ADR-003 the parquet a DTP writes *is* the artifact — there is no
load step that reshapes it — so the layout it lands in is the layout the
bundle serves. Each chromosome becomes one self-describing file:

    <base>/<table>_chr<N>.parquet

Chosen over hive directories (`<table>/chromosome=N/part-0.parquet`)
after measuring both on the 71.7 M-row AlphaMissense output:

    layout   size      1 chrom   3 chroms   full scan   non-chrom filter
    hive     888.9 MB   10.7 ms    7.8 ms     22.8 ms      40.5 ms
    flat     835.2 MB   17.7 ms   21.1 ms     24.0 ms      34.4 ms

Hive prunes a chromosome predicate faster because it discards
directories without opening files, but the absolute gap is milliseconds,
flat is 6% smaller (the constant chromosome column costs almost nothing
under RLE), and flat wins when the predicate is not on chromosome.

What decided it was not speed: one file per chromosome gives the
manifest one entry with one checksum, with none of the parent-plus-
children ambiguity that made the 4.2.0 bundle store 15.6 GB twice; and
`variant_masters_chr21.parquet` names its own table and partition, so
the same name can be used in reports and model mappings, where
`part-0.parquet` means nothing without its parent directory.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pyarrow as pa
import pyarrow.parquet as pq


class ChromosomeFileWriter:
    """
    Route rows to one open writer per chromosome, one file each.

    Keeping the writers open is what makes this usable from a streaming
    transform: a genome-wide source arrives chromosome by chromosome but
    nothing guarantees it, and buffering whole chromosomes to find out
    would cost gigabytes. One writer per chromosome caps the overhead at
    the row-group buffer times the number of chromosomes.

    Unlike a hive layout, the chromosome column stays in the file — there
    is no partition path for a reader to recover it from.
    """

    def __init__(
        self,
        base_dir: Path,
        schema: pa.Schema,
        table_name: str,
        *,
        compression: str = "zstd",
        chromosome_column: str = "chromosome",
    ):
        self.base_dir = Path(base_dir)
        self.schema = schema
        self.table_name = table_name
        self.compression = compression
        self.chromosome_column = chromosome_column
        self._writers: Dict[int, pq.ParquetWriter] = {}
        self._counts: Dict[int, int] = {}
        self._closed: List[int] = []

    def path_for(self, chrom: int) -> Path:
        """
        File a chromosome lands in.

        Chromosomes are numbered the way BF4 stores them (X=23, Y=24),
        matching `variant_masters` in the existing bundles, so the file
        name and the column agree.
        """
        return self.base_dir / f"{self.table_name}_chr{chrom}.parquet"

    def write_rows(self, rows: List[dict]) -> None:
        """Write a batch of dicts, splitting it by chromosome."""
        if not rows:
            return
        buckets: Dict[int, List[dict]] = {}
        for row in rows:
            chrom = row.get(self.chromosome_column)
            if chrom is None:
                continue
            buckets.setdefault(int(chrom), []).append(row)

        for chrom, bucket in buckets.items():
            self._write_table(
                chrom, pa.Table.from_pylist(bucket, schema=self.schema)
            )

    def write_table(self, table: pa.Table) -> None:
        """Write an Arrow table, splitting it by chromosome."""
        if table.num_rows == 0:
            return
        col = table.column(self.chromosome_column)
        for chrom in set(col.to_pylist()):
            if chrom is None:
                continue
            mask = pa.compute.equal(col, chrom)
            self._write_table(int(chrom), table.filter(mask))

    def _write_table(self, chrom: int, table: pa.Table) -> None:
        writer = self._writers.get(chrom)
        if writer is None:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(
                self.path_for(chrom),
                self.schema,
                compression=self.compression,
            )
            self._writers[chrom] = writer
            self._counts[chrom] = 0
        writer.write_table(table)
        self._counts[chrom] += table.num_rows

    @property
    def rows_written(self) -> int:
        return sum(self._counts.values())

    @property
    def chromosomes(self) -> List[int]:
        """Chromosomes written; valid before and after close()."""
        return self._closed or sorted(self._writers)

    def counts(self) -> Dict[int, int]:
        return dict(self._counts)

    def total_bytes(self) -> int:
        return sum(
            self.path_for(c).stat().st_size
            for c in self.chromosomes
            if self.path_for(c).exists()
        )

    def close(self) -> None:
        """
        Close every open writer, keeping the counts.

        `chromosomes`, `counts()` and `total_bytes()` are read *after*
        closing, to report what the run produced, so the bookkeeping must
        outlive the writer handles.
        """
        for writer in self._writers.values():
            writer.close()
        self._closed = sorted(self._writers)
        self._writers.clear()

    def __enter__(self) -> "ChromosomeFileWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

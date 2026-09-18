"""
Reading a cohort's own variant file.

The bundle is what Biofilter knows; a cohort file is what the user
actually measured. Three shapes turn up, and only one of them carries
genotypes:

| file | carries | used for |
| --- | --- | --- |
| VCF / VCF.gz | variants **and** genotypes | binning, allele frequencies |
| PLINK `.bim` | variants only | intersecting, PLINK `--extract` |
| plain list | rsIDs or `chr:pos` | intersecting |

DuckDB has no VCF reader, so `cyvcf2` parses and numpy does the
per-variant genotype arithmetic. Everything downstream of that — placing
variants on genes, assigning bins, aggregating — is SQL against the
bundle, because that is where the size is.

Reading is two-phase on purpose. Case/control allele counts cannot be
computed until the phenotype file has been matched against the sample
names, and the sample names are only known once the file is open.
"""

from __future__ import annotations

import csv
import gzip
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import numpy as np
import pyarrow as pa

from biofilter.modules.report.reports._variants import parse_chromosome

#: Cohort file formats this module reads.
FORMATS = ("vcf", "bim", "list")

_RSID = re.compile(r"^rs\d+$", re.IGNORECASE)


class CohortFormatError(ValueError):
    """The cohort file is not a shape this report can read."""


@dataclass
class Cohort:
    """One cohort file, read."""

    #: One row per (variant, alternate allele), with allele counts when
    #: the file carried genotypes.
    variants: pa.Table
    #: Sparse carriers: one row per (variant, sample) where the sample
    #: carries at least one copy. Absent for files without genotypes.
    carriers: Optional[pa.Table]
    samples: list[str]
    source_format: str
    #: Rows the reader skipped, and why. A cohort file is somebody's
    #: real data; saying nothing about what was dropped is not an option.
    skipped: dict[str, int]

    @property
    def chromosomes(self) -> list[int]:
        column = self.variants.column("chromosome").to_pylist()
        return sorted({c for c in column if c is not None})


def detect_format(path: Path) -> str:
    """By extension, since the contents of a `.bim` and a list look alike."""
    name = path.name.lower()
    if name.endswith((".vcf", ".vcf.gz", ".vcf.bgz")):
        return "vcf"
    if name.endswith(".bim"):
        return "bim"
    return "list"


class CohortReader:
    """
    Open a cohort file, then read it.

    Two phases: `samples` has to be available before reading, so the
    caller can match a phenotype file against it and say which columns
    are cases and which are controls.
    """

    def __init__(self, path: str | Path, source_format: Optional[str] = None) -> None:
        self.path = Path(path).expanduser()
        if not self.path.is_file():
            raise FileNotFoundError(f"Cohort file not found: {self.path}")
        self.source_format = (source_format or detect_format(self.path)).lower()
        if self.source_format not in FORMATS:
            raise CohortFormatError(
                f"Unknown cohort format {self.source_format!r}. "
                f"Expected one of {FORMATS}."
            )
        self._vcf = None
        self.samples: list[str] = []
        if self.source_format == "vcf":
            self._vcf = _open_vcf(self.path)
            self.samples = list(self._vcf.samples)

    @property
    def has_genotypes(self) -> bool:
        return self.source_format == "vcf" and bool(self.samples)

    # ------------------------------------------------------------------
    def read(
        self,
        *,
        case_positions: Sequence[int] = (),
        control_positions: Sequence[int] = (),
        sample_positions: Optional[Sequence[int]] = None,
        max_variants: Optional[int] = None,
    ) -> Cohort:
        if self.source_format == "vcf":
            return self._read_vcf(
                case_positions=np.asarray(case_positions, dtype=np.int64),
                control_positions=np.asarray(control_positions, dtype=np.int64),
                sample_positions=(
                    np.arange(len(self.samples), dtype=np.int64)
                    if sample_positions is None
                    else np.asarray(sample_positions, dtype=np.int64)
                ),
                max_variants=max_variants,
            )
        if self.source_format == "bim":
            return self._read_bim(max_variants)
        return self._read_list(max_variants)

    # ------------------------------------------------------------------
    def _read_vcf(
        self,
        *,
        case_positions: np.ndarray,
        control_positions: np.ndarray,
        sample_positions: np.ndarray,
        max_variants: Optional[int],
    ) -> Cohort:
        rows: dict[str, list[Any]] = {
            key: []
            for key in (
                "row_id", "chromosome", "position", "end_position",
                "reference_allele", "alternate_allele", "variant_id",
                "ac_overall", "an_overall",
                "ac_case", "an_case", "ac_control", "an_control",
            )
        }
        carrier_row: list[int] = []
        carrier_sample: list[int] = []
        carrier_count: list[int] = []
        skipped = {
            "unparsable_chromosome": 0,
            "no_alternate_allele": 0,
            "no_genotypes": 0,
            "no_called_alleles": 0,
        }

        row_id = 0
        for record in self._vcf:
            chromosome = parse_chromosome(record.CHROM)
            if chromosome is None:
                skipped["unparsable_chromosome"] += 1
                continue

            reference = str(record.REF or "").strip().upper()
            alternates = [
                str(a).strip().upper() for a in (record.ALT or []) if str(a).strip()
            ]
            if not reference or not alternates:
                skipped["no_alternate_allele"] += 1
                continue

            # cyvcf2 gives (allele_1, allele_2, phased) per sample. -1 is
            # a no-call, and a no-call is not a reference call: it must
            # leave the denominator rather than inflate it.
            raw = record.genotypes
            if not raw:
                skipped["no_genotypes"] += 1
                continue
            alleles = _allele_matrix(raw, sample_positions)
            called = (alleles >= 0).sum(axis=1)

            position = int(record.POS)
            end_position = position + max(len(reference), 1) - 1
            variant_id = (str(record.ID).strip() or None) if record.ID else None

            for allele_index, alternate in enumerate(alternates, start=1):
                copies = (alleles == allele_index).sum(axis=1)
                an_overall = int(called.sum())
                if an_overall <= 0:
                    skipped["no_called_alleles"] += 1
                    continue

                rows["row_id"].append(row_id)
                rows["chromosome"].append(chromosome)
                rows["position"].append(position)
                rows["end_position"].append(end_position)
                rows["reference_allele"].append(reference)
                rows["alternate_allele"].append(alternate)
                rows["variant_id"].append(variant_id)
                rows["ac_overall"].append(int(copies.sum()))
                rows["an_overall"].append(an_overall)
                for label, positions in (
                    ("case", case_positions),
                    ("control", control_positions),
                ):
                    if positions.size:
                        rows[f"ac_{label}"].append(int(copies[positions].sum()))
                        rows[f"an_{label}"].append(int(called[positions].sum()))
                    else:
                        rows[f"ac_{label}"].append(None)
                        rows[f"an_{label}"].append(None)

                # Only carriers are emitted. For a rare variant almost
                # every sample is a reference call, so the dense matrix
                # is mostly zeros nobody needs.
                (carriers,) = np.nonzero(copies)
                if carriers.size:
                    carrier_row.extend([row_id] * int(carriers.size))
                    carrier_sample.extend(
                        int(sample_positions[i]) for i in carriers.tolist()
                    )
                    carrier_count.extend(int(copies[i]) for i in carriers.tolist())

                row_id += 1
                if max_variants is not None and row_id >= max_variants:
                    return self._build(rows, carrier_row, carrier_sample,
                                       carrier_count, skipped)

        return self._build(rows, carrier_row, carrier_sample, carrier_count, skipped)

    def _build(
        self,
        rows: dict[str, list[Any]],
        carrier_row: list[int],
        carrier_sample: list[int],
        carrier_count: list[int],
        skipped: dict[str, int],
    ) -> Cohort:
        variants = pa.table(
            {
                "row_id": pa.array(rows["row_id"], pa.int64()),
                "chromosome": pa.array(rows["chromosome"], pa.int32()),
                "position": pa.array(rows["position"], pa.int64()),
                "end_position": pa.array(rows["end_position"], pa.int64()),
                "reference_allele": pa.array(rows["reference_allele"], pa.string()),
                "alternate_allele": pa.array(rows["alternate_allele"], pa.string()),
                "variant_id": pa.array(rows["variant_id"], pa.string()),
                "ac_overall": pa.array(rows["ac_overall"], pa.int64()),
                "an_overall": pa.array(rows["an_overall"], pa.int64()),
                "ac_case": pa.array(rows["ac_case"], pa.int64()),
                "an_case": pa.array(rows["an_case"], pa.int64()),
                "ac_control": pa.array(rows["ac_control"], pa.int64()),
                "an_control": pa.array(rows["an_control"], pa.int64()),
            }
        )
        carriers = pa.table(
            {
                "row_id": pa.array(carrier_row, pa.int64()),
                "sample_index": pa.array(carrier_sample, pa.int64()),
                "alt_count": pa.array(carrier_count, pa.int32()),
            }
        )
        return Cohort(
            variants=variants,
            carriers=carriers,
            samples=self.samples,
            source_format=self.source_format,
            skipped=skipped,
        )

    # ------------------------------------------------------------------
    def _read_bim(self, max_variants: Optional[int]) -> Cohort:
        """
        PLINK `.bim`: chromosome, id, centimorgans, position, allele 1, 2.

        Allele 1 is the minor allele and allele 2 the major one, which is
        the reverse of how a VCF names them — so allele 2 is the
        reference here, not allele 1.
        """
        rows: list[tuple] = []
        skipped = {"unparsable_chromosome": 0, "short_line": 0}
        with _open_text(self.path) as handle:
            for line in handle:
                fields = line.split()
                if len(fields) < 6:
                    if line.strip():
                        skipped["short_line"] += 1
                    continue
                chromosome = parse_chromosome(fields[0])
                if chromosome is None:
                    skipped["unparsable_chromosome"] += 1
                    continue
                rows.append(
                    (
                        len(rows),
                        chromosome,
                        int(fields[3]),
                        fields[1].strip() or None,
                        fields[5].strip().upper(),
                        fields[4].strip().upper(),
                    )
                )
                if max_variants is not None and len(rows) >= max_variants:
                    break
        return self._identity_only(rows, skipped)

    def _read_list(self, max_variants: Optional[int]) -> Cohort:
        """One value per line: an rsID, `chr:pos`, or `chr:pos:ref:alt`."""
        rows: list[tuple] = []
        skipped = {"unrecognised": 0}
        with _open_text(self.path) as handle:
            for line in handle:
                text = line.strip()
                if not text or text.startswith("#"):
                    continue
                parsed = _parse_list_entry(text, len(rows))
                if parsed is None:
                    skipped["unrecognised"] += 1
                    continue
                rows.append(parsed)
                if max_variants is not None and len(rows) >= max_variants:
                    break
        return self._identity_only(rows, skipped)

    def _identity_only(self, rows: list[tuple], skipped: dict[str, int]) -> Cohort:
        """A file with no genotypes still has to produce the same shape."""
        empty = [None] * len(rows)
        variants = pa.table(
            {
                "row_id": pa.array([r[0] for r in rows], pa.int64()),
                "chromosome": pa.array([r[1] for r in rows], pa.int32()),
                "position": pa.array([r[2] for r in rows], pa.int64()),
                "end_position": pa.array([r[2] for r in rows], pa.int64()),
                "reference_allele": pa.array([r[4] for r in rows], pa.string()),
                "alternate_allele": pa.array([r[5] for r in rows], pa.string()),
                "variant_id": pa.array([r[3] for r in rows], pa.string()),
                "ac_overall": pa.array(empty, pa.int64()),
                "an_overall": pa.array(empty, pa.int64()),
                "ac_case": pa.array(empty, pa.int64()),
                "an_case": pa.array(empty, pa.int64()),
                "ac_control": pa.array(empty, pa.int64()),
                "an_control": pa.array(empty, pa.int64()),
            }
        )
        return Cohort(
            variants=variants,
            carriers=None,
            samples=[],
            source_format=self.source_format,
            skipped=skipped,
        )


# ----------------------------------------------------------------------
def _allele_matrix(raw: list, sample_positions: np.ndarray) -> np.ndarray:
    """
    The called alleles, as an (n_selected, 2) integer matrix.

    cyvcf2 hands back a list per sample whose last element is the phase
    flag, not an allele — including it would count a phased genotype as
    a third copy. Left as a plain list rather than an object array:
    `np.asarray(..., dtype=object)` on equal-length calls builds a 2-D
    array instead, and then testing one call for emptiness raises
    "truth value of an array is ambiguous".
    """
    matrix = np.full((len(sample_positions), 2), -1, dtype=np.int16)
    for index, position in enumerate(sample_positions.tolist()):
        call = raw[position] if position < len(raw) else None
        if call is None:
            continue
        for slot in (0, 1):
            if len(call) > slot:
                value = call[slot]
                if isinstance(value, (int, np.integer)) and value >= 0:
                    matrix[index, slot] = int(value)
    return matrix


def _parse_list_entry(text: str, row_id: int) -> Optional[tuple]:
    if _RSID.match(text):
        return (row_id, None, None, text.lower(), None, None)

    parts = re.split(r"[:\-_\s]+", text)
    if len(parts) >= 2:
        chromosome = parse_chromosome(parts[0])
        if chromosome is not None and parts[1].isdigit():
            reference = parts[2].upper() if len(parts) > 2 else None
            alternate = parts[3].upper() if len(parts) > 3 else None
            return (row_id, chromosome, int(parts[1]), None, reference, alternate)
    return None


def _open_vcf(path: Path):
    try:
        from cyvcf2 import VCF
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise RuntimeError(
            "Reading a VCF needs cyvcf2. It is a declared dependency: "
            "`poetry install` should provide it."
        ) from exc
    return VCF(str(path))


def _open_text(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open(encoding="utf-8", errors="replace")


def read_phenotype(
    path: str | Path,
    *,
    sample_column: str,
    value_column: str,
    control_values: Iterable[Any],
    case_values: Iterable[Any] = (),
) -> dict[str, str]:
    """
    Sample name to `case` / `control` / `unknown`.

    With no explicit case values, anything that is not a control is a
    case — which is how the relational version behaved, and changing it
    silently would reclassify somebody's cohort.
    """
    controls = {str(v).strip() for v in control_values}
    cases = {str(v).strip() for v in case_values}
    classes: dict[str, str] = {}

    with _open_text(Path(path).expanduser()) as handle:
        sample = handle.read(8192)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(handle, dialect=dialect)
        missing = [
            column
            for column in (sample_column, value_column)
            if column not in (reader.fieldnames or [])
        ]
        if missing:
            raise ValueError(
                f"Phenotype file has no column(s) {missing}. It has: "
                f"{list(reader.fieldnames or [])}."
            )
        for row in reader:
            name = str(row.get(sample_column, "")).strip()
            if not name:
                continue
            value = str(row.get(value_column, "")).strip()
            if value in controls:
                classes[name] = "control"
            elif cases:
                classes[name] = "case" if value in cases else "unknown"
            else:
                classes[name] = "case"
    return classes

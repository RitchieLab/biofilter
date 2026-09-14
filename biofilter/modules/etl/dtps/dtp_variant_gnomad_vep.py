"""
gnomAD v4 exome + genome releases — VEP annotation.

One data source per chromosome (`gnomad_vep_chr<N>`), one output parquet
holding one row per (variant x transcript x consequence).

Both callsets are read because a variant may be present in the exome, in
the genome, or in both, and the VEP block is only carried by these two
files — the joint release has none. The VEP `Format` string is identical
across the two (46 fields, same order), so no per-source field mapping is
needed; only a precedence rule when a variant appears in both.

This DTP never reads the joint file. Rows are keyed by the natural key
(chromosome, position, ref, alt), so this branch and
`dtp_variant_gnomad_joint` are independent and can run in any order.

See ADR-003 (adr/0003-parquet-native-build-pipeline.md).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter
from biofilter.modules.etl.dtps.gnomad_shared import (
    GNOMAD_BASE,
    build_filters,
    chromosome_from_datasource_name,
    describe_filters,
    load_field_config,
    passes_filters,
    selected_fields,
)

# The exome and genome callsets share a release number, which is ahead of
# the joint release (4.1.1 vs 4.1). Keeping it here rather than in the
# seed means a version bump is one edit instead of 48.
VEP_RELEASE = "4.1.1"

VEP_URL_TEMPLATES = {
    "exomes": (
        f"{GNOMAD_BASE}/{{release}}/vcf/exomes/"
        f"gnomad.exomes.v{{release}}.sites.chr{{chrom}}.vcf.bgz"
    ),
    "genomes": (
        f"{GNOMAD_BASE}/{{release}}/vcf/genomes/"
        f"gnomad.genomes.v{{release}}.sites.chr{{chrom}}.vcf.bgz"
    ),
}

# The 46 VEP subfields, in the order gnomAD declares them in the `vep`
# INFO header. Identical between the exome and genome callsets, verified
# against v4.1.1 chr21.
VEP_FIELDS: List[str] = [
    "Allele", "Consequence", "IMPACT", "SYMBOL", "Gene", "Feature_type",
    "Feature", "BIOTYPE", "EXON", "INTRON", "HGVSc", "HGVSp",
    "cDNA_position", "CDS_position", "Protein_position", "Amino_acids",
    "Codons", "ALLELE_NUM", "DISTANCE", "STRAND", "FLAGS", "VARIANT_CLASS",
    "SYMBOL_SOURCE", "HGNC_ID", "CANONICAL", "MANE_SELECT",
    "MANE_PLUS_CLINICAL", "TSL", "APPRIS", "CCDS", "ENSP",
    "UNIPROT_ISOFORM", "SOURCE", "DOMAINS", "miRNA", "HGVS_OFFSET",
    "PUBMED", "MOTIF_NAME", "MOTIF_POS", "HIGH_INF_POS",
    "MOTIF_SCORE_CHANGE", "TRANSCRIPTION_FACTORS", "LoF", "LoF_filter",
    "LoF_flags", "LoF_info",
]



@dataclass
class GnomadVepConfig:
    release: str = VEP_RELEASE
    parquet_compression: str = "zstd"
    max_retries: int = 5
    chunk_size: int = 8 * 1024 * 1024

    # Which callsets to pull. Both by default: a variant may be in either
    # or both, so dropping one loses annotation rather than duplicating it.
    sources: List[str] = field(
        default_factory=lambda: ["exomes", "genomes"]
    )

    batch_size: int = 250_000

    # Directory the partitioned parquet lands in, and the table name the
    # bundle will serve it as.
    table_name: str = "variant_molecular_effects"

    # Path to a JSON overriding the packaged field-selection config.
    config_override: Optional[str] = None


class DTP(DTPBase):
    def __init__(
        self,
        logger=None,
        debug_mode: bool = False,
        datasource=None,
        package=None,
        session=None,
        db=None,
        config: Optional[GnomadVepConfig] = None,
    ):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db
        self.config = config or GnomadVepConfig()

        self.dtp_name = "dtp_variant_gnomad_vep"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "4.0.0"
        self.compatible_schema_max = "4.3.0"

    # ------------------------------------------------------------------
    # Source resolution
    # ------------------------------------------------------------------
    def source_files(self) -> Dict[str, str]:
        """Map of {callset: url} for this data source."""
        chrom = chromosome_from_datasource_name(self.data_source.name)
        if chrom is None:
            raise ValueError(
                f"Cannot derive a chromosome from data source "
                f"'{self.data_source.name}'. Expected a name ending in "
                f"'chr<N>', e.g. 'gnomad_vep_chr21'."
            )

        unknown = set(self.config.sources) - set(VEP_URL_TEMPLATES)
        if unknown:
            raise ValueError(
                f"Unknown gnomAD callset(s): {sorted(unknown)}. "
                f"Expected any of {sorted(VEP_URL_TEMPLATES)}."
            )

        return {
            label: VEP_URL_TEMPLATES[label].format(
                release=self.config.release, chrom=chrom
            )
            for label in self.config.sources
        }

    # ------------------------------------------------------------------
    # EXTRACT
    # ------------------------------------------------------------------
    def extract(self, raw_dir: str):
        self.check_compatibility()

        name = self.data_source.name
        self.logger.log(
            f"📦 Starting extraction of {name} (gnomAD VEP "
            f"v{self.config.release}, {len(self.config.sources)} "
            f"callsets)...",
            "INFO",
        )

        try:
            sources = self.source_files()
        except ValueError as exc:
            self.logger.log(f"❌ {exc}", "ERROR")
            return False, str(exc), None

        landing_path = self.get_path(raw_dir)

        # Sequential, for the reason documented in the joint DTP: the link
        # saturates on a single stream, so parallel callsets only split
        # the same bandwidth. Resuming is what matters here.
        hashes: Dict[str, str] = {}
        for label, url in sources.items():
            self.logger.log(f"→ callset '{label}'", "INFO")
            ok, msg, md5 = self.http_download_resumable(
                url,
                str(landing_path),
                max_retries=self.config.max_retries,
                chunk_size=self.config.chunk_size,
            )
            if not ok:
                self.logger.log(msg, "ERROR")
                return False, msg, None
            if md5:
                hashes[label] = md5

        file_hash = self._composite_hash(hashes)
        msg = (
            f"✅ {name} extracted to {landing_path} "
            f"({len(sources)} callsets)"
        )
        self.logger.log(msg, "INFO")
        return True, msg, file_hash

    @staticmethod
    def _composite_hash(hashes: Dict[str, str]) -> Optional[str]:
        """
        Fold the per-callset hashes into the single value the ETL package
        tracks. Changes if either callset changes.
        """
        if not hashes:
            return None
        joined = "|".join(f"{k}={hashes[k]}" for k in sorted(hashes))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # TRANSFORM / LOAD
    # ------------------------------------------------------------------
    @staticmethod
    def _scalar(value):
        """
        Collapse a VCF INFO value to a scalar. cyvcf2 returns a tuple for
        `Number=A` fields even at biallelic sites.
        """
        if isinstance(value, (list, tuple)):
            return value[0] if value else None
        return value

    @staticmethod
    def _chrom_to_int(chrom: str) -> int:
        """X and Y become 23 and 24, matching the existing bundles."""
        if chrom == "X":
            return 23
        if chrom == "Y":
            return 24
        return int(chrom)

    def transform(self, raw_dir: str, processed_dir: str):
        """
        Read both callsets and write one row per
        (variant x transcript x consequence).

        The VEP block gnomAD ships is packed — annotations separated by
        `,` and, inside one annotation, multiple consequence terms joined
        by `&`. Both are exploded here, so a query filters on a single
        consequence string instead of pattern-matching a packed field.

        Callsets are read in the configured precedence order and a
        variant already annotated is not re-read from the next callset.
        Measured on chr21: only 1.9% of variants appear in both, and
        their VEP blocks are byte-identical in 5,781 of 5,782 cases, so
        this is a deterministic tie-break rather than a content choice.
        """
        from cyvcf2 import VCF

        chrom = chromosome_from_datasource_name(self.data_source.name)
        cfg = load_field_config(self.dtp_name, self.config.config_override)
        keep = selected_fields(cfg, "vep_fields")
        filters = build_filters(cfg)
        sep = cfg.get("explode", {}).get("consequence_separator", "&")
        order = cfg.get("precedence", {}).get("order", self.config.sources)

        raw_path = self.get_path(raw_dir)
        sources = self.source_files()
        missing = [
            lbl for lbl in sources
            if not (raw_path / Path(sources[lbl]).name).is_file()
        ]
        if missing:
            msg = (
                f"❌ Raw VCF(s) not found for callset(s) {missing} in "
                f"{raw_path}. Run extract first."
            )
            self.logger.log(msg, "ERROR")
            return False, msg

        out_dir = Path(processed_dir) / self.data_source.source_system.name
        out_dir = out_dir / self.data_source.name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir

        self.logger.log(
            f"🔧 Transforming {len(sources)} callset(s) → {out_path.name} "
            f"({len(keep)} VEP fields, {describe_filters(filters)})",
            "INFO",
        )

        chrom_num = self._chrom_to_int(chrom)
        schema = self._arrow_schema(keep)

        # rsID side table. The joint callset publishes no rsID at all (its
        # ID column is empty on every record), so this is the only place in
        # the variant branch where it can be produced. It is a table of its
        # own rather than a column stamped onto variant_masters: stamping
        # would make the joint branch depend on this one having run first,
        # and a missing map would degrade to silent nulls — the exact
        # failure this fixes.
        rsid_cfg = cfg.get("rsid_map", {}) or {}
        rsid_filters = build_filters(rsid_cfg)
        rsid_sink = None
        rsid_batch: List[dict] = []
        rsid_stats = {"rows": 0}
        if rsid_cfg.get("enabled", False):
            rsid_sink = ChromosomeFileWriter(
                out_dir,
                self._rsid_schema(),
                rsid_cfg.get("table_name", "variant_rsid"),
                compression=self.config.parquet_compression,
            )

        writer = None
        batch: List[dict] = []
        annotated: set = set()
        stats = {"rows": 0, "variants": 0, "skipped_dup": 0}

        try:
            for label in [x for x in order if x in sources]:
                vcf_path = raw_path / Path(sources[label]).name
                vcf = VCF(str(vcf_path))
                fields = self._vep_format(vcf)
                idx = {name: i for i, name in enumerate(fields)}
                per_source = 0

                for record in vcf:
                    raw_vep = record.INFO.get("vep")
                    if not raw_vep:
                        continue
                    ac = self._scalar(record.INFO.get(filters["ac_field"]))
                    af = self._scalar(record.INFO.get(filters["af_field"]))

                    # Captured before the AC filter on purpose: measured on
                    # chr21, only ~25% of rsIDs survive AC>=5, and the ones
                    # dropped are the rare variants that most need an
                    # identifier.
                    if rsid_sink is not None and record.ID:
                        r_ac = self._scalar(
                            record.INFO.get(rsid_filters["ac_field"])
                        )
                        r_af = self._scalar(
                            record.INFO.get(rsid_filters["af_field"])
                        )
                        if passes_filters(r_ac, r_af, rsid_filters):
                            rsid_batch.extend(
                                {
                                    "chromosome": chrom_num,
                                    "position": record.POS,
                                    "reference_allele": record.REF,
                                    "alternate_allele": alt,
                                    "rsid": record.ID,
                                }
                                for alt in record.ALT
                            )
                            if len(rsid_batch) >= self.config.batch_size:
                                rsid_sink.write_rows(rsid_batch)
                                rsid_stats["rows"] += len(rsid_batch)
                                rsid_batch = []

                    if not passes_filters(ac, af, filters):
                        continue

                    for alt in record.ALT:
                        key = (record.POS, record.REF, alt)
                        if key in annotated:
                            stats["skipped_dup"] += 1
                            continue
                        rows = self._explode(
                            raw_vep, idx, keep, sep, alt,
                            chrom, chrom_num, record, label,
                        )
                        if not rows:
                            continue
                        annotated.add(key)
                        batch.extend(rows)
                        per_source += len(rows)
                        stats["variants"] += 1

                    if len(batch) >= self.config.batch_size:
                        writer = self._write_batch(writer, batch, out_path, schema)
                        stats["rows"] += len(batch)
                        batch = []

                self.logger.log(
                    f"   {label}: {per_source:,} annotation rows", "INFO"
                )

            if batch:
                writer = self._write_batch(writer, batch, out_path, schema)
                stats["rows"] += len(batch)
            if rsid_sink is not None and rsid_batch:
                rsid_sink.write_rows(rsid_batch)
                rsid_stats["rows"] += len(rsid_batch)
        finally:
            if writer is not None:
                writer.close()
            if rsid_sink is not None:
                rsid_sink.close()

        if writer is None:
            msg = (
                f"❌ No VEP rows passed {describe_filters(filters)} "
                f"for {self.data_source.name}"
            )
            self.logger.log(msg, "ERROR")
            return False, msg

        rsid_note = ""
        if rsid_sink is not None:
            kept = self._dedupe_rsid_files(rsid_sink)
            rsid_note = (
                f"; {rsid_sink.table_name}: {kept:,} keys "
                f"({rsid_stats['rows']:,} before dedupe, "
                f"{rsid_sink.total_bytes() / 1024 ** 2:.1f} MB)"
            )

        size_mb = writer.total_bytes() / 1024 ** 2
        files = ", ".join(
            p.name for p in map(writer.path_for, writer.chromosomes)
        )
        fanout = stats["rows"] / max(stats["variants"], 1)
        msg = (
            f"✅ {files}: {stats['rows']:,} rows over "
            f"{stats['variants']:,} variants ({fanout:.1f}x fanout), "
            f"{stats['skipped_dup']:,} already annotated by an earlier "
            f"callset, {size_mb:.1f} MB{rsid_note}"
        )
        self.logger.log(msg, "INFO")
        return True, msg

    @staticmethod
    def _rsid_schema():
        """Schema of the rsID side table."""
        import pyarrow as pa

        return pa.schema([
            pa.field("chromosome", pa.int32()),
            pa.field("position", pa.int64()),
            pa.field("reference_allele", pa.string()),
            pa.field("alternate_allele", pa.string()),
            pa.field("rsid", pa.string()),
        ])

    @staticmethod
    def _dedupe_rsid_files(sink) -> int:
        """
        Collapse duplicate keys, rewriting each chromosome file in place.

        A variant present in both callsets is written twice, with the same
        rsID — it is dbSNP's identifier for that allele, not something the
        callset decides. Left undeduped, a join against this table would
        multiply rows on the ~1.9% of variants that appear in both.

        Done as a pass over the finished file rather than with an
        in-memory seen-set: chr21 alone yields 9.85 M keys, and a Python
        set of tuples at that scale costs gigabytes on the larger
        chromosomes.
        """
        import duckdb

        total = 0
        con = duckdb.connect()
        try:
            for chrom in sink.chromosomes:
                path = sink.path_for(chrom)
                if not path.exists():
                    continue
                tmp = path.with_suffix(".dedupe.parquet")
                con.execute(
                    f"COPY (SELECT DISTINCT * FROM read_parquet('{path}')) "
                    f"TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)"
                )
                total += con.execute(
                    f"SELECT count(*) FROM read_parquet('{tmp}')"
                ).fetchone()[0]
                tmp.replace(path)
        finally:
            con.close()
        return total

    @staticmethod
    def _vep_format(vcf) -> List[str]:
        """
        Read the VEP subfield order from this file's own header rather
        than trusting the module constant — the two callsets agree today,
        but a future release could reorder them, and silently mapping
        values to the wrong columns is worse than failing.
        """
        desc = vcf.get_header_type("vep")["Description"]
        return desc.split("Format: ")[1].strip('"').split("|")

    def _explode(
        self, raw_vep, idx, keep, sep, alt, chrom, chrom_num, record, label,
    ) -> List[dict]:
        """Expand one packed VEP block into one row per consequence term."""
        rows: List[dict] = []
        variant_key = f"{chrom}:{record.POS}:{record.REF}:{alt}"

        for annotation in str(raw_vep).split(","):
            parts = annotation.split("|")
            if len(parts) < len(idx):
                parts += [""] * (len(idx) - len(parts))

            def value(name):
                i = idx.get(name)
                if i is None:
                    return None
                v = parts[i].strip()
                return v or None

            # gnomAD reports the ALT this annotation belongs to; keep only
            # the annotations for the allele being written.
            allele = value("Allele")
            if allele and not self._allele_matches(allele, record.REF, alt):
                continue

            base = {
                "chromosome": chrom_num,
                "position": record.POS,
                "reference_allele": record.REF,
                "alternate_allele": alt,
                "variant_key": variant_key,
                "callset": label,
            }
            for name in keep:
                if name != "Consequence":
                    base[name.lower()] = value(name)

            terms = (value("Consequence") or "").split(sep)
            for term in terms:
                term = term.strip()
                if not term:
                    continue
                row = dict(base)
                row["consequence"] = term
                rows.append(row)

        return rows

    @staticmethod
    def _allele_matches(allele: str, ref: str, alt: str) -> bool:
        """
        Match VEP's `Allele` against the VCF ALT.

        VEP left-trims the shared leading base on indels and writes `-`
        for a deletion, so a plain equality check would drop every indel
        annotation.
        """
        if allele == alt:
            return True
        if allele == "-":
            return len(alt) < len(ref)
        if len(alt) > len(ref) and alt.startswith(ref):
            return allele == alt[len(ref):]
        if len(alt) < len(ref) and ref.startswith(alt):
            return allele == "-"
        return allele == alt[1:] if len(alt) > 1 else False

    @staticmethod
    def _arrow_schema(keep: List[str]):
        """
        Declare the output schema instead of inferring it per batch.

        Every VEP subfield is a string in the packed block, so the types
        are known without consulting the header. Inference breaks here:
        `MANE_PLUS_CLINICAL` is empty for the first few million rows, gets
        inferred as `null`, and the writer then rejects the batch where it
        finally carries a value.
        """
        import pyarrow as pa

        cols = [
            pa.field("chromosome", pa.int32()),
            pa.field("position", pa.int64()),
            pa.field("reference_allele", pa.string()),
            pa.field("alternate_allele", pa.string()),
            pa.field("variant_key", pa.string()),
            pa.field("callset", pa.string()),
        ]
        cols += [
            pa.field(name.lower(), pa.string())
            for name in keep
            if name != "Consequence"
        ]
        cols.append(pa.field("consequence", pa.string()))
        return pa.schema(cols)

    def _write_batch(self, writer, batch: List[dict], out_path: Path, schema):
        """Append a batch through the chromosome-partitioned sink."""
        if writer is None:
            writer = ChromosomeFileWriter(
                out_path,
                schema,
                self.config.table_name,
                compression=self.config.parquet_compression,
            )
        writer.write_rows(batch)
        return writer

    def load(self, processed_dir=None):
        raise NotImplementedError(
            "load() is not used by this DTP. Under ADR-003 the variant "
            "branch writes parquet directly and is not staged through a "
            "relational database."
        )

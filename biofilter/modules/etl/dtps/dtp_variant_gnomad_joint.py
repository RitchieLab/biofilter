"""
gnomAD v4 joint release — variant frequencies.

One data source per chromosome (`gnomad_joint_chr<N>`), one output parquet.
The joint release carries every variant observed in the exome or the genome
callset, with the combined frequency fields, but no VEP annotation — that
lives in the exome/genome VCFs and is handled by
`dtp_variant_gnomad_vep`. The two are joined downstream on the natural key
(chromosome, position, ref, alt), so neither depends on the other and they
can be built in any order.

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
    SOURCE_TAG,
    build_filters,
    chromosome_from_datasource_name,
    describe_filters,
    load_field_config,
    passes_filters,
    selected_fields,
)

# gnomAD publishes the joint callset under its own release number, which
# trails the exome/genome release (4.1 vs 4.1.1). Pinning it here rather
# than in the seed keeps a version bump to one edit instead of 24.
JOINT_RELEASE = "4.1"

JOINT_URL_TEMPLATE = (
    f"{GNOMAD_BASE}/{{release}}/vcf/joint/"
    f"gnomad.joint.v{{release}}.sites.chr{{chrom}}.vcf.bgz"
)



@dataclass
class GnomadJointConfig:
    release: str = JOINT_RELEASE
    parquet_compression: str = "zstd"
    max_retries: int = 5
    chunk_size: int = 8 * 1024 * 1024
    batch_size: int = 250_000

    # Directory the partitioned parquet lands in, and the table name the
    # bundle will serve it as.
    table_name: str = "variant_masters"

    # Path to a JSON overriding the packaged field-selection config, for
    # building a bundle with a different selection or different AC/AF
    # bounds without editing the shipped default.
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
        config: Optional[GnomadJointConfig] = None,
    ):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db
        self.config = config or GnomadJointConfig()

        self.dtp_name = "dtp_variant_gnomad_joint"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "4.0.0"
        self.compatible_schema_max = "4.3.0"

    # ------------------------------------------------------------------
    # Source resolution
    # ------------------------------------------------------------------
    def source_files(self) -> Dict[str, str]:
        """
        Map of {label: url} for this data source.

        The joint branch has a single file per chromosome; it is returned
        as a mapping so extract() iterates over the same shape as the VEP
        DTP, which has two.
        """
        chrom = chromosome_from_datasource_name(self.data_source.name)
        if chrom is None:
            raise ValueError(
                f"Cannot derive a chromosome from data source "
                f"'{self.data_source.name}'. Expected a name ending in "
                f"'chr<N>', e.g. 'gnomad_joint_chr21'."
            )

        url = JOINT_URL_TEMPLATE.format(
            release=self.config.release, chrom=chrom
        )
        return {"joint": url}

    # ------------------------------------------------------------------
    # EXTRACT
    # ------------------------------------------------------------------
    def extract(self, raw_dir: str):
        self.check_compatibility()

        name = self.data_source.name
        self.logger.log(
            f"📦 Starting extraction of {name} (gnomAD joint "
            f"v{self.config.release})...",
            "INFO",
        )

        try:
            sources = self.source_files()
        except ValueError as exc:
            self.logger.log(f"❌ {exc}", "ERROR")
            return False, str(exc), None

        landing_path = self.get_path(raw_dir)

        # Downloads run sequentially on purpose. Measured against this
        # origin, four parallel range streams moved 66.6 MB/s against
        # 64.2 MB/s for a single stream — the link saturates either way,
        # so concurrency buys ~4% and costs the ability to reason about
        # partial state. Resuming is what actually matters over a 1.5 TB
        # full-genome pull.
        hashes: Dict[str, str] = {}
        for label, url in sources.items():
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
        msg = f"✅ {name} extracted to {landing_path}"
        self.logger.log(msg, "INFO")
        return True, msg, file_hash

    @staticmethod
    def _composite_hash(hashes: Dict[str, str]) -> Optional[str]:
        """
        Fold per-file hashes into the single value the ETL package tracks.

        Order-independent so the same set of inputs always yields the same
        digest, and sensitive to any one of them changing — which is the
        semantics the extract skip-logic needs.
        """
        if not hashes:
            return None
        joined = "|".join(f"{k}={hashes[k]}" for k in sorted(hashes))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # TRANSFORM / LOAD
    # ------------------------------------------------------------------
    def _scalar(self, value):
        """
        Collapse a VCF INFO value to a scalar.

        cyvcf2 hands back a tuple for `Number=A` fields even when the site
        is biallelic. Sites are decomposed one ALT per row here, so the
        first element is the value for the allele being written.
        """
        if isinstance(value, (list, tuple)):
            return value[0] if value else None
        return value

    def transform(self, raw_dir: str, processed_dir: str):
        """
        Read the joint VCF and write one parquet row per (variant, ALT).

        Multi-allelic sites are decomposed so the natural key
        (chromosome, position, ref, alt) identifies exactly one row —
        which is what the VEP parquet joins against.
        """
        from cyvcf2 import VCF

        chrom = chromosome_from_datasource_name(self.data_source.name)
        cfg = load_field_config(self.dtp_name, self.config.config_override)
        fields = selected_fields(cfg, "info_fields")
        filters = build_filters(cfg)

        raw_path = self.get_path(raw_dir)
        vcf_name = Path(self.source_files()["joint"]).name
        vcf_path = raw_path / vcf_name
        if not vcf_path.is_file():
            msg = f"❌ Raw VCF not found: {vcf_path}. Run extract first."
            self.logger.log(msg, "ERROR")
            return False, msg

        out_dir = Path(processed_dir) / self.data_source.source_system.name
        out_dir = out_dir / self.data_source.name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir

        self.logger.log(
            f"🔧 Transforming {vcf_name} → {out_path.name} "
            f"({len(fields)} INFO fields, {describe_filters(filters)})",
            "INFO",
        )

        chrom_num = self._chrom_to_int(chrom)
        schema = self._arrow_schema(cfg, fields)
        ac_field = filters["ac_field"]
        af_field = filters["af_field"]

        writer = None
        batch: List[dict] = []
        seen = kept = 0
        try:
            for record in VCF(str(vcf_path)):
                info = dict(record.INFO)
                ac = self._scalar(info.get(ac_field))
                af = self._scalar(info.get(af_field))
                for alt in record.ALT:
                    seen += 1
                    if not passes_filters(ac, af, filters):
                        continue
                    row = {
                        "chromosome": chrom_num,
                        "position": record.POS,
                        "reference_allele": record.REF,
                        "alternate_allele": alt,
                        "variant_key": (
                            f"{chrom}:{record.POS}:{record.REF}:{alt}"
                        ),
                        "rsid": record.ID,
                        "quality_filter": (
                            ";".join(record.FILTERS)
                            if record.FILTERS
                            else "PASS"
                        ),
                    }
                    for name in fields:
                        row[name.lower()] = self._scalar(info.get(name))
                    batch.append(row)
                    kept += 1

                if len(batch) >= self.config.batch_size:
                    writer = self._write_batch(writer, batch, out_path, schema)
                    batch = []

            if batch:
                writer = self._write_batch(writer, batch, out_path, schema)
        finally:
            if writer is not None:
                writer.close()

        if writer is None:
            msg = (
                f"❌ No variants passed {describe_filters(filters)} "
                f"in {vcf_name} ({seen:,} alleles read)"
            )
            self.logger.log(msg, "ERROR")
            return False, msg

        size_mb = writer.total_bytes() / 1024 ** 2
        files = ", ".join(p.name for p in map(writer.path_for, writer.chromosomes))  # noqa: E501
        msg = (
            f"✅ {files}: {kept:,} of {seen:,} alleles kept "
            f"({100 * kept / seen:.1f}%), {size_mb:.1f} MB"
        )
        self.logger.log(msg, "INFO")
        return True, msg

    @staticmethod
    def _chrom_to_int(chrom: str) -> int:
        """
        Map a chromosome token to the integer encoding BF4 stores.

        X and Y become 23 and 24, matching `variant_masters` in the
        existing bundles.
        """
        if chrom == "X":
            return 23
        if chrom == "Y":
            return 24
        return int(chrom)

    @staticmethod
    def _arrow_schema(cfg: dict, fields: List[str]):
        """
        Declare the output schema from the config's VCF types.

        Inferring it per batch does not work: a column that happens to be
        entirely null in the first batch is inferred as `null` type, and
        the writer then rejects the next batch where it holds strings.
        Deriving it from the header types up front also means a column
        stays typed even when a whole chromosome has no value for it.
        """
        import pyarrow as pa

        vcf_to_arrow = {
            "Integer": pa.int64(),
            "Float": pa.float64(),
            "Flag": pa.bool_(),
            "String": pa.string(),
            "Character": pa.string(),
        }
        declared = {f["name"]: f.get("type") for f in cfg.get("info_fields", [])}  # noqa: E501

        cols = [
            pa.field("chromosome", pa.int32()),
            pa.field("position", pa.int64()),
            pa.field("reference_allele", pa.string()),
            pa.field("alternate_allele", pa.string()),
            pa.field("variant_key", pa.string()),
            pa.field("rsid", pa.string()),
            pa.field("quality_filter", pa.string()),
        ]
        for name in fields:
            arrow_type = vcf_to_arrow.get(declared.get(name), pa.string())
            cols.append(pa.field(name.lower(), arrow_type))
        return pa.schema(cols)

    def _write_batch(self, writer, batch: List[dict], out_path: Path, schema):
        """
        Append a batch, opening the chromosome-partitioned sink lazily.

        This data source covers a single chromosome, so the sink produces
        one `chromosome=<N>/part-0.parquet` — the same layout the
        genome-wide DTPs emit, which is what lets bundle assembly treat
        every variant table identically.
        """
        if writer is None:
            writer = ChromosomeFileWriter(
                out_path,
                schema,
                self.config.table_name,
                source=SOURCE_TAG,
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

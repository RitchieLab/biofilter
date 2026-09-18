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
import json
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
                source=SOURCE_TAG,
                compression=self.config.parquet_compression,
            )

        # Predictor side table. The in-silico scores are INFO fields of
        # these same VCFs, beside the CSQ block rather than inside it —
        # the joint callset has none of them. Kept apart from
        # variant_masters because that table comes from the joint
        # callset, and a variant in both exomes and genomes has two
        # values with no principled rule for choosing between them; the
        # `callset` column keeps both.
        pred_cfg = cfg.get("predictors", {}) or {}
        pred_filters = build_filters(pred_cfg)
        pred_fields = [
            f["name"] for f in pred_cfg.get("fields", []) if f.get("load")
        ]
        pred_sink = None
        pred_batch: List[dict] = []
        pred_stats = {"rows": 0}
        if pred_cfg.get("enabled", False) and pred_fields:
            pred_sink = ChromosomeFileWriter(
                out_dir,
                self._predictor_schema(pred_fields),
                pred_cfg.get("table_name", "variant_predictions"),
                source=SOURCE_TAG,
                compression=self.config.parquet_compression,
            )
            self.logger.log(
                f"   predictors: {len(pred_fields)} field(s) "
                f"({', '.join(pred_fields)})",
                "INFO",
            )

        # Resolved before reading a byte of VCF: this is what both
        # tables are filtered against, and discovering it is missing
        # after an hour of transform helps nobody.
        self._check_prefilter_bound(cfg, filters)
        master_path = self._variant_master_path(cfg, processed_dir, chrom)
        self.logger.log(f"   filtering against {master_path.name}", "INFO")

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

                    # Before the AC filter, as with the rsID map: a
                    # predictor is most worth having on a rare variant,
                    # and these are the rows the filter removes.
                    if pred_sink is not None:
                        p_ac = self._scalar(
                            record.INFO.get(pred_filters["ac_field"])
                        )
                        p_af = self._scalar(
                            record.INFO.get(pred_filters["af_field"])
                        )
                        if passes_filters(p_ac, p_af, pred_filters):
                            scores = {
                                name: self._scalar(record.INFO.get(name))
                                for name in pred_fields
                            }
                            # A record with no score at all is a row of
                            # nulls keyed by a variant — nothing to say.
                            if any(v is not None for v in scores.values()):
                                pred_batch.extend(
                                    {
                                        "chromosome": chrom_num,
                                        "position": record.POS,
                                        "reference_allele": record.REF,
                                        "alternate_allele": alt,
                                        **scores,
                                    }
                                    for alt in record.ALT
                                )
                                if len(pred_batch) >= self.config.batch_size:
                                    pred_sink.write_rows(pred_batch)
                                    pred_stats["rows"] += len(pred_batch)
                                    pred_batch = []

                    if not passes_filters(ac, af, filters):
                        continue

                    for alt in record.ALT:
                        key = (record.POS, record.REF, alt)
                        if key in annotated:
                            stats["skipped_dup"] += 1
                            continue
                        rows = self._explode(
                            raw_vep, idx, keep, sep, alt,
                            chrom, chrom_num, record,
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
            if pred_sink is not None and pred_batch:
                pred_sink.write_rows(pred_batch)
                pred_stats["rows"] += len(pred_batch)
        finally:
            if writer is not None:
                writer.close()
            if rsid_sink is not None:
                rsid_sink.close()
            if pred_sink is not None:
                pred_sink.close()

        if writer is None:
            msg = (
                f"❌ No VEP rows passed {describe_filters(filters)} "
                f"for {self.data_source.name}"
            )
            self.logger.log(msg, "ERROR")
            return False, msg

        rsid_note = ""
        if rsid_sink is not None:
            kept = self._dedupe_files(rsid_sink)
            rsid_note = (
                f"; {rsid_sink.table_name}: {kept:,} keys "
                f"({rsid_stats['rows']:,} before dedupe, "
                f"{rsid_sink.total_bytes() / 1024 ** 2:.1f} MB)"
            )

        # Both tables are reduced to the variants the bundle carries,
        # against the file that defines them.
        before_effects = stats["rows"]
        stats["rows"] = self._filter_effects_to_master(writer, master_path)
        effects_note = (
            f", from {before_effects:,} over {stats['variants']:,} variants "
            f"read"
        )

        pred_note = ""
        if pred_sink is not None:
            pred_kept = self._reduce_predictor_files(
                pred_sink, pred_fields, master_path
            )
            pred_note = (
                f"; {pred_sink.table_name}: {pred_kept:,} variants "
                f"({pred_stats['rows']:,} before, "
                f"{pred_sink.total_bytes() / 1024 ** 2:.1f} MB)"
            )

        size_mb = writer.total_bytes() / 1024 ** 2
        files = ", ".join(
            p.name for p in map(writer.path_for, writer.chromosomes)
        )
        # Counted after the filter, so the two numbers describe the same
        # rows. `stats["variants"]` counts what was read, which is a
        # larger set — reporting a fanout across the two would divide
        # kept rows by read variants and understate it.
        self._warn_unranked_consequences(writer)
        kept_variants = self._count_variants(writer)
        fanout = stats["rows"] / max(kept_variants, 1)
        msg = (
            f"✅ {files}: {stats['rows']:,} rows over "
            f"{kept_variants:,} variants ({fanout:.1f}x fanout), "
            f"{stats['skipped_dup']:,} already annotated by an earlier "
            f"callset{effects_note}, {size_mb:.1f} MB{rsid_note}{pred_note}"
        )
        self.logger.log(msg, "INFO")
        return True, msg

    @staticmethod
    def _predictor_schema(fields: List[str]):
        """
        Schema of the predictor side table.

        Declared rather than inferred. Parquet types a column from the
        first batch it sees, and a predictor that is null for every
        record in that batch — `phylop` on a run of unscored sites, say
        — would be typed `null` and then reject every later batch that
        did carry a value.
        """
        import pyarrow as pa

        return pa.schema([
            pa.field("chromosome", pa.int32()),
            pa.field("position", pa.int64()),
            pa.field("reference_allele", pa.string()),
            pa.field("alternate_allele", pa.string()),
            # No `callset` column. It was here on the assumption that
            # exomes and genomes could disagree and that nothing could
            # choose between them. They do not disagree — see
            # `_dedupe_files` — so the column recorded which VCF a row
            # happened to be read from, and bought a duplicate row for
            # every variant in both.
            *[pa.field(name, pa.float64()) for name in fields],
        ])

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

    def _check_prefilter_bound(self, cfg, filters) -> None:
        """
        Refuse a pre-filter that could lose a variant the joint kept.

        The AC threshold is decided in one place — `min_ac` in the joint
        DTP's config, which is what puts a variant in `variant_masters`.
        The two `min_ac` settings on this side are not second decisions;
        they bound the intermediate, and they are only safe while they
        stay at or below what the joint's threshold implies.

        `AC_joint = AC_exomes + AC_genomes`, so a variant reaching
        `min_ac` in the joint must reach `ceil(min_ac / 2)` in at least
        one callset. Filtering above that silently annotates less than
        the bundle carries: at 5 here against the joint's 5 it left
        45,974 chr22 variants — 1.6% — with no VEP at all, which is what
        sent this code back to the drawing board.

        Checked rather than derived, so the coupling is visible in the
        config instead of computed out of sight.
        """
        joint = load_field_config("dtp_variant_gnomad_joint", None)
        joint_min = (joint.get("filters") or {}).get("min_ac")
        if joint_min is None:
            return

        bound = (int(joint_min) + 1) // 2
        offenders = [
            (label, value)
            for label, value in (
                ("filters.min_ac", filters.get("min_ac")),
                ("predictors.filters.min_ac",
                 ((cfg.get("predictors") or {}).get("filters") or {}).get("min_ac")),
            )
            if value is not None and int(value) > bound
        ]
        if not offenders:
            return

        listed = ", ".join(f"{label}={value}" for label, value in offenders)
        raise ValueError(
            f"{listed} would drop variants the joint callset keeps. "
            f"The joint filters at AC_joint >= {joint_min}, and since "
            f"AC_joint = AC_exomes + AC_genomes, such a variant need only "
            f"reach {bound} in one callset. Set these to {bound} or lower "
            f"(null keeps everything); the semi-join against "
            f"variant_masters is what does the real filtering."
        )

    def _variant_master_path(self, cfg, processed_dir, chrom) -> Path:
        """
        The `variant_masters` parquet this chromosome's joint DTP wrote.

        A deliberate exception to ADR-003's rule that the two data
        sources must not depend on each other. The rule exists so the
        branches can run in any order and neither can silently degrade
        the other; it is relaxed here because the joint, exome and genome
        callsets are three files of one gnomAD release, read by two DTPs
        only because they are shaped differently.

        What the dependency buys is exactness. The alternative was to sum
        `AC_exomes + AC_genomes` and keep what reached the threshold,
        which is a *proxy* for what the joint callset contains: the joint
        applies its own QC, so a variant can clear the sum and still not
        be in the bundle. Filtering against the file itself cannot be
        wrong about it.

        Missing is a hard failure, not a fallback. A fallback here would
        write a table filtered by a different rule than the one it
        claims, which is the whole class of defect this work has been
        removing.
        """
        master_cfg = cfg.get("variant_master") or {}
        source = master_cfg.get("source", "gnomad_joint_chr{chrom}").format(
            chrom=chrom
        )
        directory = (
            Path(processed_dir) / self.data_source.source_system.name / source
        )
        matches = sorted(directory.glob("variant_masters*.parquet"))
        if not matches:
            raise FileNotFoundError(
                f"{self.data_source.name} filters against the variants "
                f"'{source}' produced, and none are under {directory}. "
                f"Run '{source}' first — in a plan the joint source for a "
                f"chromosome must precede its VEP source. If it has "
                f"already run and been assembled into a bundle, its "
                f"parquet was moved there; re-run it with "
                f"--keep-processed, or rebuild both together."
            )
        return matches[0]

    def _warn_unranked_consequences(self, writer) -> None:
        """
        Name any consequence term the severity seed does not rank.

        `variant_consequences` is what gives the bundle an ordering, and
        it is joined by name. A term VEP emits that the seed does not
        list joins to nothing: an inner join drops the row, a left join
        ranks it null and sorts it last. Either way the variant quietly
        stops being the most severe thing it is.

        Checked here because it is cheap here — one DISTINCT over a
        low-cardinality column of the file just written, 31 values on
        chr22 — and because this is the moment the mismatch is created.

        A warning, not a failure: a new Sequence Ontology term is a
        reason to update the seed, not to throw away a chromosome's
        transform.
        """
        import duckdb

        seed = (
            Path(__file__).resolve().parents[2]
            / "db" / "seed" / "initial_variant_consequences.json"
        )
        try:
            data = json.loads(seed.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 — a missing seed is not fatal here
            return
        rows = data if isinstance(data, list) else data.get("variant_consequences", [])  # noqa: E501
        ranked = {r["name"] for r in rows}
        if not ranked:
            return

        observed: set = set()
        con = duckdb.connect()
        try:
            for chrom in writer.chromosomes:
                path = writer.path_for(chrom)
                if not path.exists():
                    continue
                observed.update(
                    r[0] for r in con.execute(
                        f"SELECT DISTINCT consequence FROM read_parquet('{path}')"  # noqa: E501
                    ).fetchall()
                )
        finally:
            con.close()

        unranked = sorted(t for t in observed if t and t not in ranked)
        if not unranked:
            return

        self.logger.log(
            f"⚠️  {len(unranked)} consequence term(s) have no severity_rank "
            f"in initial_variant_consequences.json: "
            f"{', '.join(unranked)}. Reports that order by severity will "
            f"treat them as unranked. Add them to the seed.",
            "WARNING",
        )

    @staticmethod
    def _count_variants(writer) -> int:
        """Distinct variants in what the writer actually kept."""
        import duckdb

        key = "chromosome, position, reference_allele, alternate_allele"
        total = 0
        con = duckdb.connect()
        try:
            for chrom in writer.chromosomes:
                path = writer.path_for(chrom)
                if not path.exists():
                    continue
                total += con.execute(
                    f"SELECT count(*) FROM (SELECT DISTINCT {key} "
                    f"FROM read_parquet('{path}'))"
                ).fetchone()[0]
        finally:
            con.close()
        return total

    @staticmethod
    def _filter_effects_to_master(writer, master_path) -> int:
        """
        Drop annotations for variants the bundle does not carry.

        The pre-filter on the main path keeps a record whose *own*
        callset reaches `min_ac`, set to 3 rather than 5 on purpose: a
        variant can only reach a combined AC of 5 if one callset has at
        least 3, so 3 cannot lose a variant the joint callset kept, while
        5 dropped the ones that clear the combined bar without either
        callset reaching it — 45,974 on chr22, 1.6% of the bundle's
        variants, left with no annotation at all.

        So the pre-filter is deliberately loose and this is where it is
        made exact, against the `variant_masters` the joint DTP wrote.

        A semi-join rather than the grouping the predictors use: this
        table has ~14 rows per variant and all of them survive together.
        """
        import duckdb
        import pyarrow.parquet as pq

        key = [
            "chromosome", "position", "reference_allele", "alternate_allele",
        ]
        on = " AND ".join(f"e.{c} = v.{c}" for c in key)

        total = 0
        con = duckdb.connect()
        try:
            for chrom in writer.chromosomes:
                path = writer.path_for(chrom)
                if not path.exists():
                    continue
                tmp = path.with_suffix(".kept.parquet")
                reader = con.execute(
                    f"SELECT e.* FROM read_parquet('{path}') e "
                    f"SEMI JOIN read_parquet('{master_path}') v ON {on}"
                ).to_arrow_reader()
                w = pq.ParquetWriter(
                    tmp, writer.schema, compression=writer.compression
                )
                try:
                    for batch in reader:
                        w.write_batch(batch)
                        total += batch.num_rows
                finally:
                    w.close()
                tmp.replace(path)
        finally:
            con.close()
        return total

    @staticmethod
    def _reduce_predictor_files(sink, fields, master_path) -> int:
        """
        One row per variant, restricted to the variants the bundle holds.

        Two steps in one grouping pass.

        *Collapse.* A variant in both callsets is written twice with the
        same scores — measured on chr22, all 901,714 such variants are
        byte-identical across all eight predictors, because a predictor
        is computed from the reference and the allele, not by the
        callset. `max()` over an identical pair is that value.

        *Restrict.* A semi-join against the `variant_masters` this
        chromosome's joint DTP wrote. gnomAD scores every variant it
        publishes, five times more than the bundle carries, and a
        predictor for a variant no table can join to is dead weight.
        """
        import duckdb
        import pyarrow.parquet as pq

        key = [
            "chromosome", "position", "reference_allele", "alternate_allele",
        ]
        scores = ", ".join(f"max({f}) AS {f}" for f in fields)
        on = " AND ".join(f"p.{c} = v.{c}" for c in key)

        total = 0
        con = duckdb.connect()
        try:
            for chrom in sink.chromosomes:
                path = sink.path_for(chrom)
                if not path.exists():
                    continue
                tmp = path.with_suffix(".reduced.parquet")
                reader = con.execute(
                    f"SELECT p.* FROM ("
                    f"  SELECT {', '.join(key)}, {scores} "
                    f"  FROM read_parquet('{path}') "
                    f"  GROUP BY {', '.join(key)}"
                    f") p SEMI JOIN read_parquet('{master_path}') v ON {on}"
                ).to_arrow_reader()
                writer = pq.ParquetWriter(
                    tmp, sink.schema, compression=sink.compression
                )
                try:
                    for batch in reader:
                        writer.write_batch(batch)
                        total += batch.num_rows
                finally:
                    writer.close()
                tmp.replace(path)
        finally:
            con.close()
        return total

    @staticmethod
    def _dedupe_files(sink) -> int:
        """
        Collapse duplicate keys, rewriting each chromosome file in place.

        Both side tables need this, for the same reason. A variant present
        in both callsets is written twice, and what is written is the same
        both times: an rsID is dbSNP's identifier for that allele, and a
        predictor score is computed from the reference and the allele.
        Neither is something the callset decides. Left undeduped, a join
        against either table multiplies rows on the variants that appear
        in both.

        Measured on chr22: of 15,551,628 distinct variants, 901,714 are in
        both callsets, and in **every one** of them all eight predictors
        are identical — no disagreement, and no case of one callset
        scoring where the other is null. Joined to `variant_masters` the
        undeduped table returned 3,383,651 rows for 2,889,803 variants.

        Done as a pass over the finished file rather than with an
        in-memory seen-set: chr21 alone yields 9.85 M keys, and a Python
        set of tuples at that scale costs gigabytes on the larger
        chromosomes.

        DuckDB does the DISTINCT, but the file is written by pyarrow from
        its output, streamed batch by batch. `COPY ... TO ... (FORMAT
        parquet)` would be shorter and writes a file with no key-value
        metadata — which silently stripped the `biofilter_table` stamp
        the sink had put in the footer, so the build fell back to parsing
        the name and registered the table as `variant_rsid_gnomad`.
        """
        import duckdb
        import pyarrow.parquet as pq

        total = 0
        con = duckdb.connect()
        try:
            for chrom in sink.chromosomes:
                path = sink.path_for(chrom)
                if not path.exists():
                    continue
                tmp = path.with_suffix(".dedupe.parquet")
                reader = con.execute(
                    f"SELECT DISTINCT * FROM read_parquet('{path}')"
                ).to_arrow_reader()
                writer = pq.ParquetWriter(
                    tmp, sink.schema, compression=sink.compression
                )
                try:
                    for batch in reader:
                        writer.write_batch(batch)
                        total += batch.num_rows
                finally:
                    writer.close()
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
        self, raw_vep, idx, keep, sep, alt, chrom, chrom_num, record,
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
            # No `callset`. It recorded which VCF a row was read from,
            # not anything about the annotation: a variant in both is
            # annotated once, from whichever `precedence` reads first,
            # and measured on chr22 the two callsets produce identical
            # VEP output for 3,038 of the 3,039 alleles they share. What
            # differs between them is which variants they contain, and
            # that is what `variant_masters` already records, per callset.
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

# dtp_variant_gnomad_cyvcf2.py
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from cyvcf2 import VCF

from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.kdc.manifest_writer import KDSManifestWriter


import glob
import pandas as pd
from sqlalchemy import insert as generic_insert


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
@dataclass
class GnomadCyvcf2Config:
    """
    gnomAD VCF -> Parquet transform config.

    - variants: 1 row per (chrom,pos,ref,alt) [uses first ALT only]
    - consequences: many rows per variant (exploded VEP field)
    """

    chunk_size: int = 200_000

    # INFO key for VEP payload in gnomAD
    vep_info_key: str = "vep"

    # If True: extract ALL INFO fields (except excluded)
    # If False: only extract keys listed in info_allowlist
    extract_all_info: bool = False
    info_allowlist: Optional[List[str]] = field(
        default_factory=lambda: [
            "variant_type",
            "allele_type",
            "AC",
            "AN",
            "AF",
            "grpmax",
            "grpmax_af",
            "cadd_raw_score",
            "cadd_phred",
            "revel_max",
            "spliceai_ds_max",
            "pangolin_largest_ds",
            "polyphen_max",
            "sift_max",
        ]
    )

    # Exclusions
    info_exclude_keys: Tuple[str, ...] = ("vep", "")
    info_exclude_prefixes: Tuple[str, ...] = (
        "VRS_",
        "age_hist",
        "gq_hist",
        "dp_hist",
        "ab_hist",
    )

    # Output file naming
    variants_prefix: str = "variants_part_"
    consequences_prefix: str = "consequences_part_"
    parquet_compression: str = "snappy"
    min_qual = 1


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
# # transform._build_atomic_consequence_rows
def _truthy_vep_flag(v: Optional[str]) -> Optional[bool]:
    """
    Convert VEP-style YES/empty to boolean/None.
    """
    if v is None:
        return None
    s = str(v).strip().upper()
    if s == "YES":
        return True
    if s == "":
        return None
    return None


# transform
def resolve_file_chromosome(vcf_path: Path, datasource_name: str) -> Optional[int]:  # noqa E501
    """
    Resolve chromosome once per file from datasource name or file name.
    Expected patterns like:
      - gnomad_chr1
      - ...chr1...
      - ...chromosome_1...
    Returns BF4 integer convention:
      1..22 -> 1..22, X -> 23, Y -> 24, MT/M -> 25
    """
    import re

    candidates = [datasource_name or "", vcf_path.name]

    for text in candidates:
        m = re.search(
            r"(?:chr|chromosome[_-]?)(\d+|X|Y|M|MT)\b", text, flags=re.IGNORECASE  # noqa E501
        )
        if not m:
            continue
        c = m.group(1).upper()
        if c == "X":
            return 23
        if c == "Y":
            return 24
        if c in {"M", "MT"}:
            return 25
        return int(c)

    return None


# TODO Revisar
def infer_variant_type(ref: str, alt: Optional[str]) -> str:
    if not alt:
        return "unknown"
    if len(ref) == 1 and len(alt) == 1:
        return "snv"
    if len(ref) != len(alt):
        return "indel"
    if len(ref) > 1 and len(alt) > 1:
        return "mnv"
    return "other"


# transform
def _parse_info_header_types(raw_header: str) -> Dict[str, str]:
    """
    Parse VCF header INFO lines and return: {INFO_ID: Type}
    Example:
      ##INFO=<ID=AC,Number=A,Type=Integer,Description="...">
    """
    info_types: Dict[str, str] = {}
    for line in (raw_header or "").splitlines():
        if not line.startswith("##INFO=<"):
            continue
        m_id = re.search(r"\bID=([^,>]+)", line)
        m_type = re.search(r"\bType=([^,>]+)", line)
        if not m_id:
            continue
        key = m_id.group(1).strip()
        vtype = m_type.group(1).strip() if m_type else "String"
        info_types[key] = vtype
    return info_types


# transform
def _cast_info_value(v: Any, vcf_type: str) -> Any:
    """
    Safe cast INFO values to Python primitives/lists.
    """
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return list(v)

    try:
        if vcf_type == "Integer":
            return int(v)
        if vcf_type == "Float":
            return float(v)
        if vcf_type == "Flag":
            return bool(v)
        return v
    except Exception:
        return None


# transform
def _parse_vep_header_format(vcf: VCF, vep_key: str) -> List[str]:
    """
    Extract VEP schema (Format: ...) from INFO/<vep_key> header line.
    Returns list of field names in order.
    """
    raw = vcf.raw_header or ""
    for line in raw.splitlines():
        if not line.startswith("##INFO=<"):
            continue
        if re.search(rf"\bID={re.escape(vep_key)}\b", line) is None:
            continue
        m = re.search(r"Format:\s*([^\">]+)", line)
        if not m:
            return []
        fmt = m.group(1).strip()
        return [x.strip() for x in fmt.split("|") if x.strip()]
    return []


# transform
def _parse_vep_rows(
    vep_value: Optional[str],
    vep_fields: List[str],
) -> List[Dict[str, str]]:
    """
    Parse VEP value (comma-separated rows, pipe-separated fields) into list of dicts.  # noqa E501
    """
    if not vep_value or not vep_fields:
        return []
    rows: List[Dict[str, str]] = []
    for chunk in str(vep_value).split(","):
        parts = chunk.split("|")
        row: Dict[str, str] = {}
        for i, k in enumerate(vep_fields):
            row[k] = parts[i] if i < len(parts) else ""
        rows.append(row)
    return rows


VEP_CONSEQUENCE_SEVERITY_ORDER: List[str] = [
    "transcript_ablation",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",
    "transcript_amplification",
    "feature_elongation",
    "feature_truncation",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",
    "splice_donor_5th_base_variant",
    "splice_region_variant",
    "splice_donor_region_variant",
    "splice_polypyrimidine_tract_variant",
    "incomplete_terminal_codon_variant",
    "start_retained_variant",
    "stop_retained_variant",
    "synonymous_variant",
    "coding_sequence_variant",
    "mature_miRNA_variant",
    "5_prime_UTR_variant",
    "3_prime_UTR_variant",
    "non_coding_transcript_exon_variant",
    "intron_variant",
    "NMD_transcript_variant",
    "non_coding_transcript_variant",
    "coding_transcript_variant",
    "upstream_gene_variant",
    "downstream_gene_variant",
    "TFBS_ablation",
    "TFBS_amplification",
    "TF_binding_site_variant",
    "regulatory_region_ablation",
    "regulatory_region_amplification",
    "regulatory_region_variant",
    "intergenic_variant",
    "sequence_variant",
]

VEP_CONSEQUENCE_RANK: Dict[str, int] = {
    term: idx + 1 for idx, term in enumerate(VEP_CONSEQUENCE_SEVERITY_ORDER)
}

IMPACT_RANK: Dict[str, int] = {
    "HIGH": 1,
    "MODERATE": 2,
    "LOW": 3,
    "MODIFIER": 4,
}


# transform._build_atomic_consequence_rows
def _split_consequences(value: Optional[str]) -> List[str]:
    if value is None:
        return []
    parts = [item.strip() for item in str(value).split("&") if item and item.strip()]  # noqa E501
    return parts


# transform._build_atomic_consequence_rows
def _consequence_rank(term: Optional[str]) -> Optional[int]:
    if not term:
        return None
    return VEP_CONSEQUENCE_RANK.get(term)


# transform._build_atomic_consequence_rows
def _impact_rank(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    return IMPACT_RANK.get(str(value).strip().upper())


# transform._build_atomic_consequence_rows
def _classify_consequence(term: Optional[str]) -> Tuple[Optional[str], Optional[str]]:  # noqa E501
    if not term:
        return None, None

    direct_group_map = {
        "transcript_ablation": ("transcript_loss", "coding"),
        "transcript_amplification": ("transcript_change", "coding"),
        "feature_elongation": ("structural_other", "structural_other"),
        "feature_truncation": ("structural_other", "structural_other"),
        "splice_acceptor_variant": ("splice", "coding"),
        "splice_donor_variant": ("splice", "coding"),
        "splice_donor_5th_base_variant": ("splice", "coding"),
        "splice_region_variant": ("splice", "coding"),
        "splice_donor_region_variant": ("splice", "coding"),
        "splice_polypyrimidine_tract_variant": ("splice", "coding"),
        "stop_gained": ("stop", "coding"),
        "stop_lost": ("stop", "coding"),
        "start_lost": ("start_stop", "coding"),
        "start_retained_variant": ("start_stop", "coding"),
        "stop_retained_variant": ("synonymous", "coding"),
        "frameshift_variant": ("frameshift", "coding"),
        "inframe_insertion": ("inframe", "coding"),
        "inframe_deletion": ("inframe", "coding"),
        "missense_variant": ("missense", "coding"),
        "protein_altering_variant": ("protein_altering", "coding"),
        "synonymous_variant": ("synonymous", "coding"),
        "coding_sequence_variant": ("coding_other", "coding"),
        "incomplete_terminal_codon_variant": ("coding_other", "coding"),
        "5_prime_UTR_variant": ("utr", "non_coding"),
        "3_prime_UTR_variant": ("utr", "non_coding"),
        "mature_miRNA_variant": ("non_coding", "non_coding"),
        "non_coding_transcript_exon_variant": ("non_coding", "non_coding"),
        "non_coding_transcript_variant": ("non_coding", "non_coding"),
        "coding_transcript_variant": ("non_coding", "non_coding"),
        "NMD_transcript_variant": ("non_coding", "non_coding"),
        "intron_variant": ("intronic", "non_coding"),
        "upstream_gene_variant": ("upstream_downstream", "regulatory"),
        "downstream_gene_variant": ("upstream_downstream", "regulatory"),
        "TFBS_ablation": ("regulatory", "regulatory"),
        "TFBS_amplification": ("regulatory", "regulatory"),
        "TF_binding_site_variant": ("regulatory", "regulatory"),
        "regulatory_region_ablation": ("regulatory", "regulatory"),
        "regulatory_region_amplification": ("regulatory", "regulatory"),
        "regulatory_region_variant": ("regulatory", "regulatory"),
        "intergenic_variant": ("intergenic", "intergenic"),
        "sequence_variant": ("other", "structural_other"),
    }

    if term in direct_group_map:
        return direct_group_map[term]

    if "splice" in term:
        return "splice", "coding"
    if "missense" in term:
        return "missense", "coding"
    if "synonymous" in term:
        return "synonymous", "coding"
    if "intron" in term:
        return "intronic", "non_coding"
    if "utr" in term.lower():
        return "utr", "non_coding"
    if "regulatory" in term or "tf_binding" in term.lower():
        return "regulatory", "regulatory"
    if "intergenic" in term:
        return "intergenic", "intergenic"
    if "upstream" in term or "downstream" in term:
        return "upstream_downstream", "regulatory"

    return "other", "structural_other"


# transform
def _build_atomic_consequence_rows(
    *,
    variant_key: str,
    chrom: int,
    pos: int,
    ref: str,
    alt: str,
    vep_rows: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    atomic_rows: List[Dict[str, Any]] = []
    per_annotation_min_rank: Dict[int, Optional[int]] = {}
    per_annotation_min_term: Dict[int, Optional[str]] = {}
    min_rank_for_variant: Optional[int] = None
    min_term_for_variant: Optional[str] = None

    for annotation_index, r in enumerate(vep_rows):
        lof_conf = (r.get("LoF") or "").strip() or None
        impact = r.get("IMPACT") or None
        impact_rank = _impact_rank(impact)
        consequence_raw = r.get("Consequence") or None
        atomic_terms = _split_consequences(consequence_raw) or [None]

        local_min_rank: Optional[int] = None
        local_min_term: Optional[str] = None

        for consequence in atomic_terms:
            consequence_rank = _consequence_rank(consequence)
            if consequence_rank is not None and (
                local_min_rank is None or consequence_rank < local_min_rank
            ):
                local_min_rank = consequence_rank
                local_min_term = consequence

            group, category = _classify_consequence(consequence)

            atomic_rows.append(
                {
                    "annotation_index": annotation_index,
                    "variant_key": variant_key,
                    "chrom": chrom,
                    "pos": pos,
                    "ref": ref,
                    "alt": alt,
                    "allele": r.get("Allele") or None,
                    "feature_type": r.get("Feature_type") or None,
                    "gene_id_raw": r.get("Gene") or None,
                    "gene_symbol_raw": r.get("SYMBOL") or None,
                    "transcript_id_raw": r.get("Feature") or None,
                    "gene_id": r.get("Gene") or None,
                    "transcript_id": r.get("Feature") or None,
                    "consequence_raw": consequence_raw,
                    "consequence": consequence,
                    "impact": impact,
                    "impact_rank": impact_rank,
                    "biotype": r.get("BIOTYPE") or None,
                    "variant_class": r.get("VARIANT_CLASS") or None,
                    "consequence_group": group,
                    "consequence_category": category,
                    "consequence_rank": consequence_rank,
                    "canonical": _truthy_vep_flag(r.get("CANONICAL")),
                    "mane_select": _truthy_vep_flag(r.get("MANE_SELECT")),
                    "mane_plus_clinical": _truthy_vep_flag(r.get("MANE_PLUS_CLINICAL")),  # noqa E501
                    "hgvsc": r.get("HGVSc") or None,
                    "hgvsp": r.get("HGVSp") or None,
                    "cdna_position": r.get("cDNA_position") or None,
                    "cds_position": r.get("CDS_position") or None,
                    "protein_position": r.get("Protein_position") or None,
                    "amino_acids": r.get("Amino_acids") or None,
                    "codons": r.get("Codons") or None,
                    "ensp": r.get("ENSP") or None,
                    "lof_flag": lof_conf in {"HC", "LC"},
                    "lof_confidence": lof_conf,
                    "lof_filter": r.get("LoF_filter") or None,
                    "lof_flags": r.get("LoF_flags") or None,
                    "lof_info": r.get("LoF_info") or None,
                }
            )

        per_annotation_min_rank[annotation_index] = local_min_rank
        per_annotation_min_term[annotation_index] = local_min_term

        if local_min_rank is not None and (
            min_rank_for_variant is None or local_min_rank < min_rank_for_variant  # noqa E501
        ):
            min_rank_for_variant = local_min_rank
            min_term_for_variant = local_min_term

    for row in atomic_rows:
        annotation_index = row["annotation_index"]
        annotation_rank = per_annotation_min_rank.get(annotation_index)
        row["most_severe_consequence_per_annotation"] = per_annotation_min_term.get(  # noqa E501
            annotation_index
        )
        row["most_severe_consequence_per_variant"] = min_term_for_variant
        row["is_most_severe_for_annotation"] = (
            row.get("consequence_rank") is not None
            and annotation_rank is not None
            and row["consequence_rank"] == annotation_rank
        )
        row["is_most_severe_for_variant"] = (
            row.get("consequence_rank") is not None
            and min_rank_for_variant is not None
            and row["consequence_rank"] == min_rank_for_variant
        )

    return atomic_rows


# tranform
def _write_parquet_part(
    rows: List[Dict[str, Any]], out_path: Path, compression: str
) -> None:  # noqa E501
    """
    Write one parquet part using pyarrow. Best for list-of-dicts buffers.
    """
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_path, compression=compression)


# -----------------------------------------------------------------------------
# DTP
# -----------------------------------------------------------------------------
class DTP(DTPBase):
    """
    gnomAD DTP using cyvcf2.

    Output:
      processed/{source_system}/{data_source}/
        variants/
          variants_part_0000.parquet
          variants_part_0001.parquet
          ...
          _manifest.variants.json
        consequences/
          consequences_part_0000.parquet
          ...
          _manifest.consequences.json
    """

    def __init__(
        self,
        logger=None,
        debug_mode: bool = False,
        datasource=None,
        package=None,
        session=None,
        db=None,
        use_conflict_csv: bool = False,
        config: Optional[GnomadCyvcf2Config] = None,
    ):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db
        self.use_conflict_csv = use_conflict_csv

        # Exten
        self.config = config or GnomadCyvcf2Config()

        self.dtp_name = "dtp_variant_gnomad"
        self.dtp_version = "0.4.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "4.0.0"

    # --------------------------
    # EXTRACT
    # --------------------------

    def extract(self, raw_dir: str):
        self.check_compatibility()

        msg = f"📦 Starting extraction of {self.data_source.name} (gnomAD VCF)..."  # noqa E501
        self.logger.log(msg, "INFO")

        source_url = self.data_source.source_url
        if not source_url:
            msg = "❌ datasource.source_url is empty"
            self.logger.log(msg, "ERROR")
            return False, msg, None

        landing_path = os.path.join(
            raw_dir,
            self.data_source.source_system.name,
            self.data_source.name,
        )
        os.makedirs(landing_path, exist_ok=True)

        try:
            current_hash = None
            try:
                current_hash = self.get_md5_from_url_file(f"{source_url}.md5")
            except Exception:
                current_hash = None

            if source_url.startswith("/") or source_url.startswith("file://"):
                msg = (
                    "ℹ️ Local source_url detected. "
                    "Consider implementing local staging here (copy/symlink)."
                )
                self.logger.log(msg, "WARNING")
                return True, msg, current_hash

            status, dl_msg = self.http_download(source_url, landing_path)
            if not status:
                self.logger.log(dl_msg, "ERROR")
                return False, dl_msg, current_hash

            msg = f"✅ {self.data_source.name} downloaded to {landing_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # --------------------------
    # TRANSFORM
    # --------------------------

    def transform(self, raw_dir: str, processed_dir: str):

        t0 = time.time()
        msg = f"⚙️  Starting transform of {self.data_source.name} (cyvcf2)..."
        self.logger.log(msg, "INFO")

        # Check Compatibility
        self.check_compatibility()

        try:
            raw_base = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            if not raw_base.exists():
                msg = f"❌ Raw dir not found: {raw_base}"
                self.logger.log(msg, "ERROR")
                return False, msg

            candidates = (
                list(raw_base.glob("*.vcf.bgz"))
                + list(raw_base.glob("*.vcf.gz"))
                + list(raw_base.glob("*.vcf"))
            )
            if not candidates:
                msg = f"❌ No VCF found in {raw_base}"
                self.logger.log(msg, "ERROR")
                return False, msg
            vcf_path = candidates[0]

            out_base = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            variants_dir = out_base / "variants"
            cons_dir = out_base / "consequences"
            variants_dir.mkdir(parents=True, exist_ok=True)
            cons_dir.mkdir(parents=True, exist_ok=True)

        except Exception as e:
            msg = f"❌ Error constructing paths: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Extend config settings
        cfg = self.config
        chunk_size = cfg.chunk_size

        part = 0  # Manager chunk files sequences
        n_rows = 0
        n_skipped = 0
        variant_rows: List[Dict[str, Any]] = []
        consequence_rows: List[Dict[str, Any]] = []

        # Get Chrom from VCF FIles / Data Source
        # Without chrm, stop the process
        chrom = resolve_file_chromosome(vcf_path, self.data_source.name)
        if chrom is None:
            # raise ValueError(
            #     f"Chromosome mismatch in file {vcf_path}"
            # )
            self.logger.log(f"❌ Chromosome mismatch in file {vcf_path}", "ERROR")  # noqa E501
            return False, msg

        # Inside method to save chunks
        def flush():
            try:
                nonlocal part, variant_rows, consequence_rows
                if not variant_rows and not consequence_rows:
                    return
                if variant_rows:
                    _write_parquet_part(
                        variant_rows,
                        variants_dir
                        / f"{cfg.variants_prefix}{part:04d}.parquet",  # noqa E501
                        cfg.parquet_compression,
                    )
                if consequence_rows:
                    _write_parquet_part(
                        consequence_rows,
                        cons_dir
                        / f"{cfg.consequences_prefix}{part:04d}.parquet",  # noqa E501
                        cfg.parquet_compression,
                    )
                variant_rows = []
                consequence_rows = []
                part += 1
            except Exception as e:
                msg = f"❌ Flush error: {str(e)}"
                self.logger.log(msg, "ERROR")
                # return
                raise

        try:
            # Read vcf with cyvcf2 software
            vcf = VCF(str(vcf_path))

            # INFO typing from header
            info_types = _parse_info_header_types(vcf.raw_header)

            # Decide which INFO keys to extract
            if cfg.extract_all_info:
                info_keys = [
                    k
                    for k in info_types.keys()
                    if k not in cfg.info_exclude_keys
                    and not any(
                        k.startswith(p) for p in cfg.info_exclude_prefixes
                    )  # noqa E501
                ]
            else:
                allow = cfg.info_allowlist or []
                info_keys = [
                    k
                    for k in allow
                    if k in info_types
                    and k not in cfg.info_exclude_keys
                    and not any(
                        k.startswith(p) for p in cfg.info_exclude_prefixes
                    )  # noqa E501
                ]

            # VEP schema
            # (this is the "csq_schema_fields" we want in the manifest)
            vep_fields = _parse_vep_header_format(vcf, cfg.vep_info_key)
            if not vep_fields:
                raise ValueError(
                    f" Could not find VEP schema in header for INFO/{cfg.vep_info_key}."  # noqa E501
                )
        except Exception as e:
            self.logger.log(f"❌ Error to read {vcf_path}: {e}", "ERROR")
            return False, msg

        try:
            for var in vcf:
                pos = int(var.POS)
                ref = var.REF

                # No load variant with filter or qualy
                var_filter = var.FILTER
                if var_filter not in (None, "PASS", ".", ""):
                    n_skipped += 1
                    continue

                var_qual = var.QUAL
                if var_qual not in (None, "PASS", ".", ""):
                    n_skipped += 1
                    continue

                # Assumption: gnomAD file already represents one ALT per record
                if not var.ALT:
                    n_skipped += 1
                    continue
                if len(var.ALT) > 1:
                    raise ValueError(
                        f"Unexpected multi-allelic record found in {vcf_path.name} at {var.CHROM}:{var.POS}."  # noqa E501
                        "Current transform assumes one ALT per record."
                    )
                alt = var.ALT[0]
                if alt is None:
                    n_skipped += 1
                    continue

                rsid = var.ID if (var.ID and var.ID != ".") else None
                # vkey = _variant_key(chrom, pos, ref, alt)
                # a = alt if alt else "."
                vkey = str(f"{chrom}:{pos}:{ref}:{alt}")

                row: Dict[str, Any] = {
                    "chrom": chrom,
                    "pos": pos,
                    "ref": ref,
                    "alt": alt,
                    "rsid": rsid,
                    "variant_key": vkey,
                }

                for k in info_keys:
                    row[k] = _cast_info_value(
                        var.INFO.get(k), info_types.get(k, "String")
                    )  # noqa E501

                variant_rows.append(row)

                # Consequences
                # preserve raw VEP fields and explode atomic conseq rows
                vep_val = var.INFO.get(cfg.vep_info_key)
                vep_rows = _parse_vep_rows(vep_val, vep_fields)

                # This method could be a bottleneck.
                consequence_rows.extend(
                    _build_atomic_consequence_rows(
                        variant_key=vkey,
                        chrom=chrom,
                        pos=pos,
                        ref=ref,
                        alt=alt,
                        vep_rows=vep_rows,
                    )
                )

                n_rows += 1
                # Save chunk files
                if n_rows % chunk_size == 0:
                    flush()
                    # break
            # Save remaining records
            flush()

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # ---------------------------------------------------------
        # KDC: Write manifests (one asset per folder)
        # ---------------------------------------------------------
        try:
            release_tag = (
                getattr(self.data_source, "release_tag", None) or "manual"
            )  # noqa E501
            assembly = (
                getattr(self.data_source, "grch_version", None) or "GRCh38"
            )  # noqa E501

            common_params = {
                "chunk_size": cfg.chunk_size,
                "parquet_compression": cfg.parquet_compression,
                "vep_info_key": cfg.vep_info_key,
                "vcf_filename": vcf_path.name,
                "vcf_source_url": getattr(
                    self.data_source, "source_url", None
                ),  # noqa E501
                "data_source_id": getattr(self.data_source, "id", None),  # noqa E501
                "alt_policy": "first_alt_only",
                "info_extract_all": cfg.extract_all_info,
                "info_keys_count": len(info_keys),
            }

            KDSManifestWriter.write(
                output_dir=variants_dir,
                source_system=self.data_source.source_system.name,
                data_source=self.data_source.name,
                asset="variants",
                release=release_tag,
                assembly=assembly,
                path_pattern=f"{cfg.variants_prefix}*.parquet",
                partitioning=[],
                dtp_name=self.dtp_name,
                dtp_version=self.dtp_version,
                parameters=common_params,
                overwrite=True,
            )

            KDSManifestWriter.write(
                output_dir=cons_dir,
                source_system=self.data_source.source_system.name,
                data_source=self.data_source.name,
                asset="consequences",
                release=release_tag,
                assembly=assembly,
                path_pattern=f"{cfg.consequences_prefix}*.parquet",
                partitioning=[],
                dtp_name=self.dtp_name,
                dtp_version=self.dtp_version,
                parameters={
                    **common_params,
                    "csq_schema_fields": list(vep_fields),
                },
                overwrite=True,
            )

        except Exception as e:
            msg = f"⚠️ KDC failed: {str(e)}"
            self.logger.log(msg, "WARNING")
            # return False, msg

        dt = time.time() - t0
        msg = (
            f"✅ Transform done: {self.data_source.name} "
            f"elapsed={dt:.1f}s out={out_base} parts={part} rows={n_rows} skipped={n_skipped}"  # noqa E501
        )
        self.logger.log(msg, "INFO")

        return True, msg

    # --------------------------
    # LOAD
    # --------------------------

    def _normalize_rsid(self, value):
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            return None
        return value.split(";")[0]

    def _normalize_variant_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Accept both old transform schema and newer BF4-aligned schema.
        Normalize columns and filter rows that exceed current DB-safe limits.
        """
        if df.empty:
            return df.copy()

        rename_map = {
            "chrom": "chromosome",
            "pos": "position_start",
            "ref": "reference_allele",
            "alt": "alternate_allele",
        }
        df = df.rename(
            columns={k: v for k, v in rename_map.items() if k in df.columns}
        ).copy()

        # Ensure required columns exist
        required = [
            "chromosome",
            "position_start",
            "reference_allele",
            "alternate_allele",
        ]
        missing_required = [c for c in required if c not in df.columns]
        if missing_required:
            raise ValueError(
                f"Variant parquet is missing required columns: {missing_required}"  # noqa E501
            )

        # position_end (vectorized)
        if "position_end" not in df.columns:
            df["position_end"] = (
                df["position_start"].astype("int64")
                + df["reference_allele"].astype(str).str.len()
                - 1
            )

        # allele_type fallback
        if "allele_type" not in df.columns:
            if "variant_type" in df.columns:
                df["allele_type"] = df["variant_type"]
            else:
                df["allele_type"] = None

        # af fallback
        if "af" not in df.columns and "af_global" in df.columns:
            df["af"] = df["af_global"]

        # Normalize rsid
        if "rsid" in df.columns:
            df["rsid"] = df["rsid"].apply(self._normalize_rsid)

        # Normalize null-like values
        df = df.where(pd.notnull(df), None)

        # Temporary DB safeguard: keep only alleles that fit current schema
        ref_max_len = 64
        alt_max_len = 64

        ref_len = df["reference_allele"].astype(str).str.len()
        alt_len = df["alternate_allele"].astype(str).str.len()

        mask = (
            df["chromosome"].notna()
            & df["position_start"].notna()
            & df["position_end"].notna()
            & df["reference_allele"].notna()
            & df["alternate_allele"].notna()
            & (ref_len <= ref_max_len)
            & (alt_len <= alt_max_len)
        )

        df = df[mask].copy()

        return df

    def _normalize_consequence_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Accept both old transform consequence schema and newer BF4-aligned schema.  # noqa E501
        Normalize columns and filter rows that exceed current DB-safe limits.
        """
        if df.empty:
            return df.copy()

        rename_map = {
            "chrom": "chromosome",
            "feature": "transcript_id",
        }
        df = df.rename(
            columns={k: v for k, v in rename_map.items() if k in df.columns}
        ).copy()

        # Raw fallbacks
        if "gene_id" not in df.columns and "gene_id_raw" in df.columns:
            df["gene_id"] = df["gene_id_raw"]
        if "gene_symbol" not in df.columns and "gene_symbol_raw" in df.columns:
            df["gene_symbol"] = df["gene_symbol_raw"]
        if "transcript_id" not in df.columns and "transcript_id_raw" in df.columns:  # noqa E501
            df["transcript_id"] = df["transcript_id_raw"]

        optional_defaults = {
            "canonical": None,
            "mane_select": None,
            "mane_plus_clinical": None,
            "feature_type": None,
            "consequence_raw": None,
            "consequence_group": None,
            "consequence_category": None,
            "consequence_rank": None,
            "impact_rank": None,
            "most_severe_consequence_per_annotation": None,
            "most_severe_consequence_per_variant": None,
            "is_most_severe_for_annotation": None,
            "is_most_severe_for_variant": None,
            "impact": None,
            "biotype": None,
            "variant_class": None,
            "lof_flag": None,
            "lof_confidence": None,
            "lof_filter": None,
            "lof_flags": None,
            "lof_info": None,
        }
        for col, default in optional_defaults.items():
            if col not in df.columns:
                df[col] = default

        # Normalize text columns used by dimensions
        for col in [
            "consequence",
            "consequence_group",
            "consequence_category",
            "impact",
            "biotype",
            "transcript_id",
            "gene_id",
            "gene_symbol",
        ]:
            if col in df.columns:
                df[col] = df[col].map(
                    lambda x: (
                        str(x).strip() if x is not None and str(x).strip() else None  # noqa E501
                    )
                )

        if "impact" in df.columns:
            df["impact"] = df["impact"].map(lambda x: x.upper() if x else None)

        df = df.where(pd.notnull(df), None)

        limits = {
            "variant_key": 64,
            "gene_id": 32,
            "gene_symbol": 64,
            "transcript_id": 32,
            "feature_type": 32,
            "consequence_raw": 255,
            "consequence": 64,
            "impact": 16,
            "biotype": 64,
            "variant_class": 16,
            "consequence_group": 64,
            "consequence_category": 64,
            "hgvsc": 128,
            "hgvsp": 128,
            "cdna_position": 32,
            "cds_position": 32,
            "protein_position": 32,
            "amino_acids": 32,
            "codons": 64,
            "ensp": 32,
            "lof_confidence": 8,
            "lof_filter": 128,
            "lof_flags": 256,
        }

        mask = pd.Series(True, index=df.index)

        for col, max_len in limits.items():
            if col in df.columns:
                mask &= df[col].isna() | (df[col].astype(str).str.len() <= max_len)  # noqa E501

        if "variant_key" in df.columns:
            mask &= df["variant_key"].notna()
        if "transcript_id" in df.columns:
            mask &= df["transcript_id"].notna()
        if "consequence" in df.columns:
            mask &= df["consequence"].notna()

        return df[mask].copy()

    def _get_insert_for_dialect(self, table, dialect_name: str):
        if dialect_name == "sqlite":
            return sqlite_insert(table)
        if dialect_name == "postgresql":
            return pg_insert(table)
        return generic_insert(table)

    def _upsert_variant_masters_from_df(self, df: pd.DataFrame, conn) -> int:
        if df.empty:
            return 0

        v = self.db.table("variant_masters")
        dialect_name = conn.dialect.name
        insert_cls = self._get_insert_for_dialect(v, dialect_name)
        chunk_size = 2000 if dialect_name == "postgresql" else 100

        records: List[Dict[str, Any]] = []
        for row in df.itertuples(index=False):
            records.append(
                {
                    "chromosome": int(row.chromosome),
                    "position_start": int(row.position_start),
                    "position_end": int(row.position_end),
                    "reference_allele": row.reference_allele,
                    "alternate_allele": row.alternate_allele,
                    "rsid": getattr(row, "rsid", None),
                    "variant_type": getattr(row, "variant_type", None),
                    "allele_type": getattr(row, "allele_type", None),
                    "ac": getattr(row, "AC", None),
                    "an": getattr(row, "AN", None),
                    "af": getattr(row, "AF", None),
                    "grpmax": getattr(row, "grpmax", None),
                    "grpmax_af": getattr(row, "grpmax_af", None),
                    "cadd_raw_score": getattr(row, "cadd_raw_score", None),
                    "cadd_phred": getattr(row, "cadd_phred", None),
                    "revel_max": getattr(row, "revel_max", None),
                    "spliceai_ds_max": getattr(row, "spliceai_ds_max", None),
                    "pangolin_largest_ds": getattr(row, "pangolin_largest_ds", None),  # noqa E501
                    "sift_max": getattr(row, "sift_max", None),
                    "polyphen_max": getattr(row, "polyphen_max", None),
                    "data_source_id": self.data_source.id,
                    "etl_package_id": self.package.id,
                }
            )

        processed = 0
        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]  # noqa E501
            stmt = insert_cls.values(chunk)

            if dialect_name == "postgresql":
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[
                        "chromosome",
                        "position_start",
                        "position_end",
                        "reference_allele",
                        "alternate_allele",
                    ]
                )
            elif dialect_name == "sqlite":
                stmt = stmt.prefix_with("OR IGNORE")

            conn.execute(stmt)
            processed += len(chunk)

        return processed

    def _resolve_variant_ids_for_df(
        self,
        df: pd.DataFrame,
        conn,
    ) -> Dict[str, Tuple[int, int]]:
        """
        Resolve variant_key -> (chromosome, variant_id) for the current batch only.  # noqa E501

        Strategy:
        - group input rows by chromosome
        - fetch candidate rows from variant_masters using a positional envelope
        - build an in-memory natural-key map
        - resolve each input variant_key from that map

        Natural key:
        (chromosome, position_start, position_end, reference_allele, alternate_allele)  # noqa E501
        """
        if df.empty:
            return {}

        v = self.db.table("variant_masters")
        out: Dict[str, Tuple[int, int]] = {}

        for chrom, df_chr in df.groupby("chromosome"):
            if df_chr.empty:
                continue

            chrom = int(chrom)
            min_start = int(df_chr["position_start"].min())
            max_start = int(df_chr["position_start"].max())

            stmt = select(
                v.c.chromosome,
                v.c.variant_id,
                v.c.position_start,
                v.c.position_end,
                v.c.reference_allele,
                v.c.alternate_allele,
            ).where(
                and_(
                    v.c.chromosome == chrom,
                    v.c.position_start >= min_start,
                    v.c.position_start <= max_start,
                )
            )

            res = conn.execute(stmt).fetchall()

            db_map = {
                (
                    int(r.chromosome),
                    int(r.position_start),
                    int(r.position_end),
                    r.reference_allele,
                    r.alternate_allele,
                ): (int(r.chromosome), int(r.variant_id))
                for r in res
            }

            for row in df_chr.itertuples(index=False):
                natural_key = (
                    int(row.chromosome),
                    int(row.position_start),
                    int(row.position_end),
                    row.reference_allele,
                    row.alternate_allele,
                )

                resolved = db_map.get(natural_key)
                if resolved is not None:
                    out[row.variant_key] = resolved

        return out

    def _init_dimension_caches(self, conn) -> Dict[str, Dict[str, int]]:
        return {
            "group": self._load_dimension_cache(conn, "variant_consequence_groups"),  # noqa E501
            "category": self._load_dimension_cache(
                conn, "variant_consequence_categories"
            ),
            "consequence": self._load_dimension_cache(conn, "variant_consequences"),  # noqa E501
            "impact": self._load_dimension_cache(conn, "variant_impacts"),
            "biotype": self._load_dimension_cache(conn, "variant_biotypes"),
        }

    def _load_dimension_cache(
        self,
        conn,
        table_name: str,
        key_column: str = "name",
    ) -> Dict[str, int]:
        table = self.db.table(table_name)
        rows = conn.execute(select(table.c.id, getattr(table.c, key_column))).fetchall()  # noqa E501

        cache: Dict[str, int] = {}
        for row in rows:
            key = getattr(row, key_column)
            if key is None:
                continue
            cache[str(key).strip()] = int(row.id)

        return cache

    def _get_or_create_dimension_value(
        self,
        conn,
        *,
        table_name: str,
        cache: Dict[str, int],
        value: Optional[str],
        extra_values: Optional[Dict[str, Any]] = None,
        key_column: str = "name",
    ) -> Optional[int]:
        if value is None:
            return None

        value = str(value).strip()
        if not value:
            return None

        cached_id = cache.get(value)
        if cached_id is not None:
            return cached_id

        table = self.db.table(table_name)

        row = conn.execute(
            select(table.c.id).where(getattr(table.c, key_column) == value)
        ).fetchone()
        if row:
            cache[value] = int(row.id)
            return int(row.id)

        insert_values = {key_column: value}
        if extra_values:
            insert_values.update(
                {k: v for k, v in extra_values.items() if k in table.c}
            )

        dialect_name = conn.dialect.name
        insert_stmt = self._get_insert_for_dialect(table, dialect_name).values(
            insert_values
        )

        if dialect_name == "postgresql":
            insert_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=[key_column]
            )
        elif dialect_name == "sqlite":
            insert_stmt = insert_stmt.prefix_with("OR IGNORE")

        conn.execute(insert_stmt)

        row = conn.execute(
            select(table.c.id).where(getattr(table.c, key_column) == value)
        ).fetchone()
        if not row:
            raise RuntimeError(
                f"Failed to resolve dimension value '{value}' for table '{table_name}'"  # noqa E501
            )

        cache[value] = int(row.id)
        return int(row.id)

    def _map_dimension_ids_to_consequence_df(
        self,
        df: pd.DataFrame,
        dim_caches: Dict[str, Dict[str, int]],
    ) -> pd.DataFrame:
        """
        Map already-primed dimension names to IDs in a vectorized way.

        Assumes _prime_dimension_caches_from_df() has already been called, so
        missing values should be rare. Any unresolved values remain as None and
        can still be handled by fallback logic if needed.
        """
        if df.empty:
            return df.copy()

        out = df.copy()

        group_cache = dim_caches["group"]
        category_cache = dim_caches["category"]
        consequence_cache = dim_caches["consequence"]
        impact_cache = dim_caches["impact"]
        biotype_cache = dim_caches["biotype"]

        # Normalize values exactly as the caches expect
        if "consequence" in out.columns:
            out["_consequence_key"] = out["consequence"].map(
                lambda x: str(x).strip() if x is not None and str(x).strip() else None  # noqa E501
            )
            out["consequence_id"] = out["_consequence_key"].map(consequence_cache)  # noqa E501

        if "impact" in out.columns:
            out["_impact_key"] = out["impact"].map(
                lambda x: (
                    str(x).strip().upper() if x is not None and str(x).strip() else None  # noqa E501
                )
            )
            out["impact_id"] = out["_impact_key"].map(impact_cache)

        if "biotype" in out.columns:
            out["_biotype_key"] = out["biotype"].map(
                lambda x: str(x).strip() if x is not None and str(x).strip() else None  # noqa E501
            )
            out["biotype_id"] = out["_biotype_key"].map(biotype_cache)

        if "consequence_group" in out.columns:
            out["_group_key"] = out["consequence_group"].map(
                lambda x: (
                    str(x).strip() if x is not None and str(x).strip() else "other"  # noqa E501
                )
            )
            out["consequence_group_id"] = out["_group_key"].map(group_cache)

        if "consequence_category" in out.columns:
            out["_category_key"] = out["consequence_category"].map(
                lambda x: (
                    str(x).strip()
                    if x is not None and str(x).strip()
                    else "structural_other"
                )
            )
            out["consequence_category_id"] = out["_category_key"].map(category_cache)  # noqa E501

        return out

    def _get_or_create_consequence_group(
        self,
        conn,
        cache: Dict[str, int],
        group_name: Optional[str],
    ) -> Optional[int]:
        fallback = str(group_name).strip() if group_name else "other"
        return self._get_or_create_dimension_value(
            conn,
            table_name="variant_consequence_groups",
            cache=cache,
            value=fallback,
        )

    def _get_or_create_consequence_category(
        self,
        conn,
        cache: Dict[str, int],
        category_name: Optional[str],
    ) -> Optional[int]:
        fallback = str(category_name).strip() if category_name else "structural_other"  # noqa E501
        return self._get_or_create_dimension_value(
            conn,
            table_name="variant_consequence_categories",
            cache=cache,
            value=fallback,
        )

    def _get_or_create_impact(
        self,
        conn,
        cache: Dict[str, int],
        impact_name: Optional[str],
        impact_rank: Optional[int],
    ) -> Optional[int]:
        if not impact_name:
            return None

        impact_name = str(impact_name).strip().upper()
        rank = (
            int(impact_rank)
            if impact_rank is not None
            else IMPACT_RANK.get(impact_name, 999)
        )

        return self._get_or_create_dimension_value(
            conn,
            table_name="variant_impacts",
            cache=cache,
            value=impact_name,
            extra_values={"severity_rank": rank},
        )

    def _get_or_create_biotype(
        self,
        conn,
        cache: Dict[str, int],
        biotype_name: Optional[str],
    ) -> Optional[int]:
        if not biotype_name:
            return None

        biotype_name = str(biotype_name).strip()
        if not biotype_name:
            return None

        return self._get_or_create_dimension_value(
            conn,
            table_name="variant_biotypes",
            cache=cache,
            value=biotype_name,
        )

    def _get_or_create_consequence(
        self,
        conn,
        consequence_cache: Dict[str, int],
        group_cache: Dict[str, int],
        category_cache: Dict[str, int],
        *,
        consequence_name: Optional[str],
        severity_rank: Optional[int],
        group_name: Optional[str],
        category_name: Optional[str],
    ) -> Optional[int]:
        if not consequence_name:
            return None

        consequence_name = str(consequence_name).strip()
        if not consequence_name:
            return None

        cached_id = consequence_cache.get(consequence_name)
        if cached_id is not None:
            return cached_id

        table = self.db.table("variant_consequences")

        row = conn.execute(
            select(table.c.id).where(table.c.name == consequence_name)
        ).fetchone()
        if row:
            consequence_cache[consequence_name] = int(row.id)
            return int(row.id)

        group_id = self._get_or_create_consequence_group(conn, group_cache, group_name)  # noqa E501
        category_id = self._get_or_create_consequence_category(
            conn, category_cache, category_name
        )
        rank = int(severity_rank) if severity_rank is not None else 999

        extra_values = {
            "severity_rank": rank,
            "is_active": True,
        }
        if "consequence_group_id" in table.c:
            extra_values["consequence_group_id"] = group_id
        if "consequence_category_id" in table.c:
            extra_values["consequence_category_id"] = category_id

        consequence_id = self._get_or_create_dimension_value(
            conn,
            table_name="variant_consequences",
            cache=consequence_cache,
            value=consequence_name,
            extra_values=extra_values,
        )
        return consequence_id

    def _prime_dimension_caches_from_df(
        self,
        df: pd.DataFrame,
        conn,
        dim_caches: Dict[str, Dict[str, int]],
    ) -> None:
        """
        Warm up caches using unique values from the current consequence dataframe.  # noqa E501
        Only missing values trigger inserts/selects.
        """
        if df.empty:
            return

        group_cache = dim_caches["group"]
        category_cache = dim_caches["category"]
        consequence_cache = dim_caches["consequence"]
        impact_cache = dim_caches["impact"]
        biotype_cache = dim_caches["biotype"]

        # 1) Groups
        if "consequence_group" in df.columns:
            group_values = (
                df["consequence_group"]
                .dropna()
                .map(lambda x: str(x).strip() if x is not None else None)
                .dropna()
                .unique()
                .tolist()
            )
            for group_name in group_values:
                if group_name not in group_cache:
                    self._get_or_create_consequence_group(conn, group_cache, group_name)  # noqa E501

        # 2) Categories
        if "consequence_category" in df.columns:
            category_values = (
                df["consequence_category"]
                .dropna()
                .map(lambda x: str(x).strip() if x is not None else None)
                .dropna()
                .unique()
                .tolist()
            )
            for category_name in category_values:
                if category_name not in category_cache:
                    self._get_or_create_consequence_category(
                        conn, category_cache, category_name
                    )

        # 3) Impacts
        if "impact" in df.columns:
            impact_cols = ["impact"]
            if "impact_rank" in df.columns:
                impact_cols.append("impact_rank")

            impact_df = df[impact_cols].dropna(subset=["impact"]).copy()
            if not impact_df.empty:
                impact_df["impact"] = impact_df["impact"].map(
                    lambda x: str(x).strip().upper() if x is not None else None
                )
                impact_df = impact_df.dropna(subset=["impact"]).drop_duplicates()  # noqa E501

                for row in impact_df.itertuples(index=False):
                    impact_name = row.impact
                    if impact_name not in impact_cache:
                        impact_rank = getattr(row, "impact_rank", None)
                        self._get_or_create_impact(
                            conn,
                            impact_cache,
                            impact_name,
                            impact_rank,
                        )

        # 4) Biotypes
        if "biotype" in df.columns:
            biotype_values = (
                df["biotype"]
                .dropna()
                .map(lambda x: str(x).strip() if x is not None else None)
                .dropna()
                .unique()
                .tolist()
            )
            for biotype_name in biotype_values:
                if biotype_name not in biotype_cache:
                    self._get_or_create_biotype(conn, biotype_cache, biotype_name)  # noqa E501

        # 5) Consequences
        # required_cols = {
        #     "consequence",
        #     "consequence_group",
        #     "consequence_category",
        #     "consequence_rank",
        # }
        # available_cols = [c for c in required_cols if c in df.columns]

        if "consequence" in df.columns:
            cons_cols = ["consequence"]
            for c in ["consequence_rank", "consequence_group", "consequence_category"]:  # noqa E501
                if c in df.columns:
                    cons_cols.append(c)

            cons_df = df[cons_cols].dropna(subset=["consequence"]).copy()
            if not cons_df.empty:
                cons_df["consequence"] = cons_df["consequence"].map(
                    lambda x: str(x).strip() if x is not None else None
                )
                if "consequence_group" in cons_df.columns:
                    cons_df["consequence_group"] = cons_df["consequence_group"].map(  # noqa E501
                        lambda x: (
                            str(x).strip() if x is not None and str(x).strip() else None  # noqa E501
                        )
                    )
                if "consequence_category" in cons_df.columns:
                    cons_df["consequence_category"] = cons_df[
                        "consequence_category"
                    ].map(
                        lambda x: (
                            str(x).strip() if x is not None and str(x).strip() else None  # noqa E501
                        )
                    )

                cons_df = cons_df.dropna(subset=["consequence"]).drop_duplicates()  # noqa E501

                for row in cons_df.itertuples(index=False):
                    consequence_name = row.consequence
                    if consequence_name not in consequence_cache:
                        self._get_or_create_consequence(
                            conn,
                            consequence_cache,
                            group_cache,
                            category_cache,
                            consequence_name=consequence_name,
                            severity_rank=getattr(row, "consequence_rank", None),  # noqa E501
                            group_name=getattr(row, "consequence_group", None),
                            category_name=getattr(row, "consequence_category", None),  # noqa E501
                        )

    def _none_if_nan(self, value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        return value

    def _upsert_variant_molecular_effects_from_df(
        self,
        df: pd.DataFrame,
        variant_id_map: Dict[str, Tuple[int, int]],
        conn,
        dim_caches: Dict[str, Dict[str, int]],
    ) -> int:
        if df.empty:
            return 0

        vme = self.db.table("variant_molecular_effects")
        dialect_name = conn.dialect.name
        insert_cls = self._get_insert_for_dialect(vme, dialect_name)
        chunk_size = 1000 if dialect_name == "postgresql" else 100

        group_cache = dim_caches["group"]
        category_cache = dim_caches["category"]
        consequence_cache = dim_caches["consequence"]
        impact_cache = dim_caches["impact"]
        biotype_cache = dim_caches["biotype"]

        # 1. Warm up caches from unique parquet values
        self._prime_dimension_caches_from_df(df, conn, dim_caches)

        # 2. Vectorized mapping of IDs
        df = self._map_dimension_ids_to_consequence_df(df, dim_caches)

        records: List[Dict[str, Any]] = []
        processed = 0

        for row in df.itertuples(index=False):
            vkey = getattr(row, "variant_key", None)
            if not vkey:
                continue

            resolved_variant = variant_id_map.get(vkey)
            if resolved_variant is None:
                continue

            transcript_id = getattr(row, "transcript_id", None) or getattr(
                row, "transcript_id_raw", None
            )
            consequence_name = getattr(row, "consequence", None)
            if not transcript_id or not consequence_name:
                continue

            chromosome, variant_id = resolved_variant

            # Prefer vectorized IDs first
            consequence_id = self._none_if_nan(getattr(row, "consequence_id", None))  # noqa E501
            impact_id = self._none_if_nan(getattr(row, "impact_id", None))
            biotype_id = self._none_if_nan(getattr(row, "biotype_id", None))

            # Fallback only if something was not resolved
            if consequence_id is None:
                consequence_id = self._get_or_create_consequence(
                    conn,
                    consequence_cache,
                    group_cache,
                    category_cache,
                    consequence_name=consequence_name,
                    severity_rank=getattr(row, "consequence_rank", None),
                    group_name=getattr(row, "consequence_group", None),
                    category_name=getattr(row, "consequence_category", None),
                )
            if consequence_id is None:
                continue

            if impact_id is None:
                impact_id = self._get_or_create_impact(
                    conn,
                    impact_cache,
                    getattr(row, "impact", None),
                    getattr(row, "impact_rank", None),
                )

            if biotype_id is None:
                biotype_id = self._get_or_create_biotype(
                    conn,
                    biotype_cache,
                    getattr(row, "biotype", None),
                )

            record = {
                "chromosome": chromosome,
                "variant_id": variant_id,
                "variant_key": vkey,
                "gene_id": getattr(row, "gene_id_raw", None)
                or getattr(row, "gene_id", None),
                "gene_symbol": getattr(row, "gene_symbol_raw", None)
                or getattr(row, "gene_symbol", None),
                "transcript_id": transcript_id,
                "feature_type": getattr(row, "feature_type", None),
                # "consequence_raw": getattr(row, "consequence_raw", None),
                "consequence_id": consequence_id,
                "impact_id": impact_id,
                "biotype_id": biotype_id,
                "lof_flag": getattr(row, "lof_flag", None),
                "lof_confidence": getattr(row, "lof_confidence", None),
                "lof_filter": getattr(row, "lof_filter", None),
                "lof_flags": getattr(row, "lof_flags", None),
                "lof_info": getattr(row, "lof_info", None),
                "data_source_id": self.data_source.id,
                "etl_package_id": self.package.id,
            }

            record = {k: self._none_if_nan(v) for k, v in record.items() if k in vme.c}  # noqa E501
            records.append(record)

            if len(records) >= chunk_size:
                stmt = insert_cls.values(records)
                if dialect_name == "postgresql":
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=[
                            "chromosome",
                            "variant_id",
                            "transcript_id",
                            "consequence_id",
                        ]
                    )
                elif dialect_name == "sqlite":
                    stmt = stmt.prefix_with("OR IGNORE")

                try:
                    conn.execute(stmt)
                except Exception as e:
                    print(e)
                processed += len(records)
                records = []

        if records:
            stmt = insert_cls.values(records)
            if dialect_name == "postgresql":
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[
                        "chromosome",
                        "variant_id",
                        "transcript_id",
                        "consequence_id",
                    ]
                )
            elif dialect_name == "sqlite":
                stmt = stmt.prefix_with("OR IGNORE")

            conn.execute(stmt)
            processed += len(records)

        return processed

    def load(self, processed_dir=None):

        t0 = time.time()
        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

        total_variants = 0
        total_effects = 0
        total_warnings = 0

        try:
            if not processed_dir:
                msg = "⚠️ processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg

            base_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )
            variants_dir = base_path / "variants"
            consequences_dir = base_path / "consequences"

            variant_files = sorted(
                glob.glob(str(variants_dir / "variants_part_*.parquet"))
            )
            consequence_files = sorted(
                glob.glob(str(consequences_dir / "consequences_part_*.parquet"))  # noqa E501
            )

            if not variant_files:
                msg = f"No variant part files found in {variants_dir}"
                self.logger.log(msg, "ERROR")
                return False, msg

            consequence_map = {
                Path(f).name.replace("consequences_", "variants_"): f
                for f in consequence_files
            }

            msg = f"📄 Found {len(variant_files)} paired variant part files to load"  # noqa E501
            self.logger.log(msg, "INFO")

        except Exception as e:
            msg = f"⚠️ Failed to prepare processed data paths: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        try:
            with self.db.engine.begin() as conn:
                dim_caches = self._init_dimension_caches(conn)
        except Exception as e:
            msg = f"⚠️ Failed to read dimension tables: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg

        try:
            self.db_write_mode()
            # TODO: seria possivel apagar so os indices dessa particao???
            # self.drop_indexes(self.get_variant_master_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️ Failed to switch DB to write mode: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg

        for variant_file in variant_files:
            variant_name = Path(variant_file).name
            consequence_file = consequence_map.get(variant_name)

            self.logger.log(f"📂 Processing {variant_name}", "INFO")

            try:
                variant_columns = [
                    "chrom",
                    "pos",
                    "ref",
                    "alt",
                    "rsid",
                    "variant_key",
                    "variant_type",  #
                    "allele_type",  #
                    "AC",  #
                    "AN",  #
                    "AF",  #
                    "grpmax",  #
                    # "grpmax_af",  #
                    "cadd_raw_score",  #
                    "cadd_phred",  #
                    "revel_max",  #
                    "spliceai_ds_max",  #
                    "pangolin_largest_ds",  #
                    "polyphen_max",  #
                    "sift_max",  #
                    # "position_end",
                ]
                df_variants = pd.read_parquet(
                    variant_file,
                    columns=variant_columns,
                    engine="pyarrow",
                )
                if df_variants.empty:
                    self.logger.log(
                        f"⚠️ Empty variant file (skipped): {variant_name}",
                        "WARNING",
                    )
                    continue

                df_variants = self._normalize_variant_df(df_variants)
                if df_variants.empty:
                    self.logger.log(
                        f"⚠️ Variant file produced no valid rows after normalization: {variant_name}",  # noqa E501
                        "WARNING",
                    )
                    continue

                df_consequences = None
                if consequence_file:
                    consequence_columns = [
                        "chrom",
                        "variant_key",
                        "gene_id_raw",
                        "gene_symbol_raw",
                        "gene_id",
                        # "gene_symbol",
                        "transcript_id",
                        "transcript_id_raw",
                        "feature_type",
                        "consequence_raw",
                        "consequence",
                        "consequence_group",
                        "consequence_category",
                        "consequence_rank",
                        "impact",
                        "impact_rank",
                        "biotype",
                        "lof_flag",
                        "lof_confidence",
                        "lof_filter",
                        "lof_flags",
                        "lof_info",
                    ]
                    df_consequences = pd.read_parquet(
                        consequence_file,
                        columns=consequence_columns,
                        engine="pyarrow",
                    )
                    if not df_consequences.empty:
                        df_consequences = self._normalize_consequence_df(
                            df_consequences
                        )
                        if df_consequences.empty:
                            df_consequences = None

                with self.db.engine.begin() as conn:
                    processed_variant_rows = self._upsert_variant_masters_from_df(  # noqa E501
                        df_variants,
                        conn,
                    )

                    variant_id_map = self._resolve_variant_ids_for_df(df_variants, conn)  # noqa E501

                    loaded_effects = 0
                    if df_consequences is not None and not df_consequences.empty:  # noqa E501
                        loaded_effects = self._upsert_variant_molecular_effects_from_df(  # noqa E501
                            df_consequences,
                            variant_id_map,
                            conn,
                            dim_caches=dim_caches,
                        )
                        total_effects += loaded_effects

                total_variants += processed_variant_rows

                self.logger.log(
                    f"✅ Processed {variant_name} "
                    f"(variants={processed_variant_rows}, "
                    f"resolved_variant_ids={len(variant_id_map)}, "
                    f"effects={loaded_effects})",
                    "INFO",
                )

            except Exception as e:
                total_warnings += 1
                self.logger.log(f"❌ Load failed for {variant_name}: {e}", "ERROR")  # noqa E501
                raise

        try:
            self.logger.log("ℹ️ Index creation currently disabled.", "INFO")
        except Exception as e:
            total_warnings += 1
            self.logger.log(f"⚠️ Failed to finalize DB: {e}", "WARNING")

        # total load time
        dt = time.time() - t0
        msg = (f"elapsed={dt:.1f}s")
        self.logger.log(msg, "INFO")

        if total_warnings == 0:
            msg = (
                f"✅ Processed {total_variants} variant rows and {total_effects} "  # noqa E501
                f"molecular effect rows from {len(variant_files)} part file(s)."  # noqa E501
            )
            self.logger.log(msg, "SUCCESS")
            return True, msg

        msg = (
            f"⚠️ Processed {total_variants} variant rows and {total_effects} "
            f"molecular effect rows with {total_warnings} warning(s). Check logs."  # noqa E501
        )
        self.logger.log(msg, "WARNING")
        return True, msg

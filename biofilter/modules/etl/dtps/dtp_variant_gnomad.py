# dtp_variant_gnomad_cyvcf2.py
from __future__ import annotations

import csv
import glob
import io
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cyvcf2 import VCF
from sqlalchemy import and_
from sqlalchemy import insert as generic_insert
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from biofilter.modules.etl.mixins.base_dtp import DTPBase


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

    # Exclusions Variants Master
    info_exclude_keys: Tuple[str, ...] = ("vep", "")
    info_exclude_prefixes: Tuple[str, ...] = (
        "VRS_",
        "age_hist",
        "gq_hist",
        "dp_hist",
        "ab_hist",
    )

    # Rows to keep in Consequence Variantsr
    vep_allowlist: Tuple[str, ...] = (
        'Allele',
        'Consequence',
        'IMPACT',
        'SYMBOL',
        'Gene',
        'Feature_type',
        'Feature',
        'BIOTYPE',
        # 'EXON',
        # 'INTRON',
        # 'HGVSc',
        # 'HGVSp',
        # 'cDNA_position',
        # 'CDS_position',
        # 'Protein_position',
        # 'Amino_acids',
        # 'Codons',
        # 'ALLELE_NUM',
        # 'DISTANCE',
        # 'STRAND',
        # 'FLAGS',
        # 'VARIANT_CLASS',
        # 'SYMBOL_SOURCE',
        # 'HGNC_ID',
        # 'CANONICAL',
        # 'MANE_SELECT',
        # 'MANE_PLUS_CLINICAL',
        # 'TSL',
        # 'APPRIS',
        # 'CCDS',
        # 'ENSP',
        # 'UNIPROT_ISOFORM',
        # 'SOURCE',
        # 'DOMAINS',
        # 'miRNA',
        # 'HGVS_OFFSET',
        # 'PUBMED',
        # 'MOTIF_NAME',
        # 'MOTIF_POS',
        # 'HIGH_INF_POS',
        # 'MOTIF_SCORE_CHANGE',
        # 'TRANSCRIPTION_FACTORS',
        'LoF',
        'LoF_filter',
        'LoF_flags',
        'LoF_info'
    )

    # Output file naming
    variants_prefix: str = "variants_part_"
    consequences_prefix: str = "consequences_part_"
    parquet_compression: str = "snappy"
    min_qual = 1
    postgres_fast_load: bool = True
    postgres_partition_refresh: bool = True


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


def _variant_key(chrom: Any, pos: int, ref: str, alt: str) -> str:
    return f"{chrom}:{pos}:{ref}:{alt}"


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
    vep_field_positions: List[Tuple[int, str]],
) -> List[Dict[str, str]]:
    """
    Parse VEP value (comma-separated rows, pipe-separated fields) into list of dicts.  # noqa E501
    """
    if not vep_value or not vep_field_positions:
        return []
    rows: List[Dict[str, str]] = []
    for chunk in str(vep_value).split(","):
        parts = chunk.split("|")
        rows.append(
            {
                field: parts[idx] if idx < len(parts) else ""
                for idx, field in vep_field_positions
            }
        )
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

            # group, category = _classify_consequence(consequence)

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
                    # "consequence_raw": consequence_raw,
                    "consequence": consequence,
                    "impact": impact,
                    "impact_rank": impact_rank,
                    "biotype": r.get("BIOTYPE") or None,
                    # "variant_class": r.get("VARIANT_CLASS") or None,
                    # "consequence_group": group,
                    # "consequence_category": category,
                    "consequence_rank": consequence_rank,
                    # "canonical": _truthy_vep_flag(r.get("CANONICAL")),
                    # "mane_select": _truthy_vep_flag(r.get("MANE_SELECT")),
                    # "mane_plus_clinical": _truthy_vep_flag(r.get("MANE_PLUS_CLINICAL")),  # noqa E501
                    # "hgvsc": r.get("HGVSc") or None,
                    # "hgvsp": r.get("HGVSp") or None,
                    # "cdna_position": r.get("cDNA_position") or None,
                    # "cds_position": r.get("CDS_position") or None,
                    # "protein_position": r.get("Protein_position") or None,
                    # "amino_acids": r.get("Amino_acids") or None,
                    # "codons": r.get("Codons") or None,
                    # "ensp": r.get("ENSP") or None,
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
        config: Optional[GnomadCyvcf2Config] = None,
    ):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db

        # Exten
        self.config = config or GnomadCyvcf2Config()

        self.dtp_name = "dtp_variant_gnomad"
        self.dtp_version = "0.4.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "4.0.0"

    # -------------------------------------------------------------------------
    #                            EXTRACT METHOD
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    #                            TRANSFORM METHOD
    # -------------------------------------------------------------------------

    def transform(self, raw_dir: str, processed_dir: str):

        t0 = time.time()
        msg = f"⚙️  Starting transform of {self.data_source.name} (cyvcf2)..."
        self.logger.log(msg, "INFO")

        # Check Compatibility
        self.check_compatibility()

        # read raw data path and prepare output paths
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

            # VEP Fields Schema
            # (this is the "csq_schema_fields" we want in the manifest)
            vep_fields_list = _parse_vep_header_format(vcf, cfg.vep_info_key)
            if not vep_fields_list:
                raise ValueError(
                    f" Could not find VEP schema in header for INFO/{cfg.vep_info_key}."  # noqa E501
                )
            # Check that all allowlist fields are present in the parsed VCF
            vep_fields_set = set(vep_fields_list)
            missing_vep_fields = [
                field for field in cfg.vep_allowlist if field not in vep_fields_set
            ]
            if missing_vep_fields:
                self.logger.log(
                    "⚠️ Missing VEP allowlist fields in parsed schema: "
                    + ", ".join(missing_vep_fields),
                    "WARNING",
                )
            vep_fields = tuple(
                field for field in cfg.vep_allowlist if field in vep_fields_set
            )
            vep_field_index = {
                field: idx for idx, field in enumerate(vep_fields_list)
            }
            vep_field_positions = [
                (vep_field_index[field], field) for field in vep_fields
            ]

        except Exception as e:
            self.logger.log(f"❌ Error to read {vcf_path}: {e}", "ERROR")
            return False, msg

        # -------------------------------------------------------------------
        # Process variants and consequences, buffering in memory and flushing
        # to parquet in chunks
        # -------------------------------------------------------------------
        try:
            for var in vcf:

                # Filtering at the variant level (before parsing INFO/VEP):
                # -----------------------------------------------------------------
                # 1. Variants with failing FILTER are skipped
                # 2. Variants below the configured QUAL threshold are skipped
                # 3. Variants with multiple ALTs in the same record are rejected

                pos = int(var.POS)
                ref = var.REF

                # Filter 1: No load variant with failing FILTER
                var_filter = var.FILTER
                if var_filter not in (None, "PASS", ".", ""):
                    n_skipped += 1
                    continue

                var_qual = var.QUAL
                try:
                    if var_qual is not None and float(var_qual) < cfg.min_qual:
                        n_skipped += 1
                        continue
                except (TypeError, ValueError):
                    pass

                # Filter 2: Skip multi-allelic records
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

                # Clean rsID when missing or empty
                rsid = var.ID if (var.ID and var.ID != ".") else None

                # Create variant key (chrom:pos:ref:alt)
                vkey = _variant_key(chrom, pos, ref, alt)

                # MASTER VARIANT
                # Construct base variant row with INFO fields
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

                # MOLECULAR EFFECT (CONSEQUENCES FROM VEP)
                # preserve raw VEP fields and explode atomic conseq rows
                vep_val = var.INFO.get(cfg.vep_info_key)

                vep_rows = _parse_vep_rows(vep_val, vep_field_positions)

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
            # Save remaining records
            flush()

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        self.logger.log(
            "ℹ️ KDS manifest generation disabled for BF4 light release.",
            "INFO",
        )

        dt = time.time() - t0
        msg = (
            f"✅ Transform done: {self.data_source.name} "
            f"elapsed={dt:.1f}s out={out_base} parts={part} rows={n_rows} skipped={n_skipped}"  # noqa E501
        )
        self.logger.log(msg, "INFO")

        return True, msg

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------

    def _get_insert_for_dialect(self, table, dialect_name: str):
        if dialect_name == "sqlite":
            return sqlite_insert(table)
        if dialect_name == "postgresql":
            return pg_insert(table)
        return generic_insert(table)

    def _supports_postgres_fast_load(self, conn) -> bool:
        return bool(
            conn.dialect.name == "postgresql"
            and getattr(self.config, "postgres_fast_load", True)
        )

    def _partition_table_name(self, parent_table: str, chrom: int) -> str:
        return f"{parent_table}_chr_{int(chrom)}"

    def _truncate_postgres_variant_partitions(self, conn, chrom: int) -> None:
        if not getattr(self.config, "postgres_partition_refresh", True):
            return

        for parent_table in ("variant_molecular_effects", "variant_masters"):
            partition_table = self._partition_table_name(parent_table, chrom)
            conn.execute(text(f'TRUNCATE TABLE "{partition_table}"'))

    def _create_postgres_stage_tables(self, conn) -> None:
        conn.execute(
            text(
                """
                CREATE TEMP TABLE IF NOT EXISTS tmp_gnomad_variant_stage (
                    chromosome integer NOT NULL,
                    position_start bigint NOT NULL,
                    position_end bigint NOT NULL,
                    reference_allele varchar(64) NOT NULL,
                    alternate_allele varchar(256) NOT NULL,
                    rsid varchar(32) NULL,
                    variant_type varchar(20) NULL,
                    allele_type varchar(20) NULL,
                    ac bigint NULL,
                    an bigint NULL,
                    af double precision NULL,
                    grpmax varchar(32) NULL,
                    grpmax_af double precision NULL,
                    cadd_raw_score double precision NULL,
                    cadd_phred double precision NULL,
                    revel_max double precision NULL,
                    spliceai_ds_max double precision NULL,
                    pangolin_largest_ds double precision NULL,
                    polyphen_max double precision NULL,
                    sift_max double precision NULL,
                    variant_key varchar(256) NOT NULL
                ) ON COMMIT DROP
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TEMP TABLE IF NOT EXISTS tmp_gnomad_consequence_stage (
                    chromosome integer NOT NULL,
                    variant_id bigint NOT NULL,
                    variant_key varchar(256) NOT NULL,
                    gene_id varchar(32) NULL,
                    gene_symbol varchar(64) NULL,
                    transcript_id varchar(32) NOT NULL,
                    feature_type varchar(32) NULL,
                    consequence_id integer NOT NULL,
                    impact_id integer NULL,
                    biotype_id integer NULL,
                    consequence_rank integer NULL,
                    impact_rank integer NULL,
                    most_severe_consequence_per_annotation_id integer NULL,
                    most_severe_consequence_per_variant_id integer NULL,
                    is_most_severe_for_annotation boolean NULL,
                    is_most_severe_for_variant boolean NULL,
                    lof_flag boolean NULL,
                    lof_confidence varchar(8) NULL,
                    lof_filter varchar(128) NULL,
                    lof_flags varchar(256) NULL,
                    lof_info text NULL
                ) ON COMMIT DROP
                """
            )
        )

    def _truncate_postgres_stage_tables(self, conn) -> None:
        conn.execute(
            text(
                "TRUNCATE TABLE tmp_gnomad_variant_stage, tmp_gnomad_consequence_stage"
            )
        )

    def _is_nullish(self, value: Any) -> bool:
        if value is None:
            return True
        try:
            return bool(pd.isna(value))
        except Exception:
            return False

    def _copy_dataframe_to_postgres_stage(
        self,
        conn,
        *,
        table_name: str,
        df: pd.DataFrame,
        columns: List[str],
    ) -> None:
        if df.empty:
            return

        out = io.StringIO()
        writer = csv.writer(out)

        for row in df[columns].itertuples(index=False, name=None):
            writer.writerow(
                ["\\N" if self._is_nullish(value) else value for value in row]
            )

        out.seek(0)

        raw_conn = getattr(conn.connection, "driver_connection", None)
        if raw_conn is None:
            raw_conn = getattr(conn.connection, "connection", None)
        if raw_conn is None:
            raise RuntimeError("Could not access the PostgreSQL driver connection.")

        copy_sql = (
            f"COPY {table_name} ({', '.join(columns)}) "
            "FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
        )

        cursor = raw_conn.cursor()
        try:
            cursor.copy_expert(copy_sql, out)
        finally:
            cursor.close()

    def _read_parquet_available_columns(
        self,
        parquet_path: str,
        requested_columns: List[str],
    ) -> pd.DataFrame:
        available_columns = set(pq.read_schema(parquet_path).names)
        selected_columns = [
            col for col in requested_columns if col in available_columns
        ]
        return pd.read_parquet(
            parquet_path,
            columns=selected_columns or None,
            engine="pyarrow",
        )

    def _normalize_rsid(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            return None
        return value.split(";")[0]

    def _normalize_text_series(
        self,
        df: pd.DataFrame,
        column: str,
        *,
        upper: bool = False,
    ) -> pd.Series:
        if column not in df.columns:
            return pd.Series(pd.NA, index=df.index, dtype="string")

        series = df[column].astype("string").str.strip()
        if upper:
            series = series.str.upper()
        return series.where(series.ne(""), pd.NA)

    def _prepare_variant_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        rename_map = {
            "chrom": "chromosome",
            "pos": "position_start",
            "ref": "reference_allele",
            "alt": "alternate_allele",
            "AC": "ac",
            "AN": "an",
            "AF": "af",
        }
        out = df.rename(
            columns={k: v for k, v in rename_map.items() if k in df.columns}
        ).copy()

        required = [
            "chromosome",
            "position_start",
            "reference_allele",
            "alternate_allele",
            "variant_key",
        ]
        missing = [col for col in required if col not in out.columns]
        if missing:
            raise ValueError(
                f"Variant parquet is missing required columns: {missing}"
            )

        out["chromosome"] = pd.to_numeric(
            out["chromosome"], errors="coerce"
        ).astype("Int64")
        out["position_start"] = pd.to_numeric(
            out["position_start"], errors="coerce"
        ).astype("Int64")
        out["reference_allele"] = self._normalize_text_series(
            out, "reference_allele"
        )
        out["alternate_allele"] = self._normalize_text_series(
            out, "alternate_allele"
        )
        out["variant_key"] = self._normalize_text_series(out, "variant_key")

        if "position_end" in out.columns:
            out["position_end"] = pd.to_numeric(
                out["position_end"], errors="coerce"
            ).astype("Int64")
        else:
            out["position_end"] = (
                out["position_start"] + out["reference_allele"].str.len() - 1
            ).astype("Int64")

        if "rsid" in out.columns:
            out["rsid"] = out["rsid"].map(self._normalize_rsid)
        else:
            out["rsid"] = None

        text_cols = ["variant_type", "allele_type", "grpmax", "grpmax_af"]
        for col in text_cols:
            if col in out.columns and col != "grpmax_af":
                out[col] = self._normalize_text_series(out, col)
            elif col not in out.columns:
                out[col] = None

        int_cols = ["ac", "an"]
        for col in int_cols:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            else:
                out[col] = None

        float_cols = [
            "af",
            "grpmax_af",
            "cadd_raw_score",
            "cadd_phred",
            "revel_max",
            "spliceai_ds_max",
            "pangolin_largest_ds",
            "polyphen_max",
            "sift_max",
        ]
        for col in float_cols:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
            else:
                out[col] = None

        mask = (
            out["chromosome"].notna()
            & out["position_start"].notna()
            & out["position_end"].notna()
            & out["reference_allele"].notna()
            & out["alternate_allele"].notna()
            & out["variant_key"].notna()
            & (out["reference_allele"].str.len() <= 64)
            & (out["alternate_allele"].str.len() <= 256)
            & (out["variant_key"].str.len() <= 256)
        )

        stage_columns = [
            "chromosome",
            "position_start",
            "position_end",
            "reference_allele",
            "alternate_allele",
            "rsid",
            "variant_type",
            "allele_type",
            "ac",
            "an",
            "af",
            "grpmax",
            "grpmax_af",
            "cadd_raw_score",
            "cadd_phred",
            "revel_max",
            "spliceai_ds_max",
            "pangolin_largest_ds",
            "polyphen_max",
            "sift_max",
            "variant_key",
        ]
        return out.loc[mask, stage_columns].copy()

    def _prepare_consequence_df(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        out = df.rename(columns={"chrom": "chromosome"}).copy()

        out["chromosome"] = pd.to_numeric(
            out.get("chromosome"), errors="coerce"
        ).astype("Int64")
        out["variant_key"] = self._normalize_text_series(out, "variant_key")

        gene_id_raw = self._normalize_text_series(out, "gene_id_raw")
        gene_id = self._normalize_text_series(out, "gene_id")
        out["gene_id"] = gene_id_raw.fillna(gene_id)

        gene_symbol_raw = self._normalize_text_series(out, "gene_symbol_raw")
        gene_symbol = self._normalize_text_series(out, "gene_symbol")
        out["gene_symbol"] = gene_symbol_raw.fillna(gene_symbol)

        transcript_id = self._normalize_text_series(out, "transcript_id")
        transcript_id_raw = self._normalize_text_series(out, "transcript_id_raw")
        out["transcript_id"] = transcript_id.fillna(transcript_id_raw)

        out["feature_type"] = self._normalize_text_series(out, "feature_type")
        out["consequence"] = self._normalize_text_series(out, "consequence")
        out["impact"] = self._normalize_text_series(out, "impact", upper=True)
        out["biotype"] = self._normalize_text_series(out, "biotype")
        out["most_severe_consequence_per_annotation"] = self._normalize_text_series(
            out,
            "most_severe_consequence_per_annotation",
        )
        out["most_severe_consequence_per_variant"] = self._normalize_text_series(
            out,
            "most_severe_consequence_per_variant",
        )
        out["lof_confidence"] = self._normalize_text_series(out, "lof_confidence")
        out["lof_filter"] = self._normalize_text_series(out, "lof_filter")
        out["lof_flags"] = self._normalize_text_series(out, "lof_flags")

        if "consequence_rank" in out.columns:
            out["consequence_rank"] = pd.to_numeric(
                out["consequence_rank"], errors="coerce"
            ).astype("Int64")
        else:
            out["consequence_rank"] = pd.Series(
                pd.NA, index=out.index, dtype="Int64"
            )

        if "impact_rank" in out.columns:
            out["impact_rank"] = pd.to_numeric(
                out["impact_rank"], errors="coerce"
            ).astype("Int64")
        else:
            out["impact_rank"] = pd.Series(pd.NA, index=out.index, dtype="Int64")

        if "lof_flag" not in out.columns:
            out["lof_flag"] = None
        if "lof_info" not in out.columns:
            out["lof_info"] = None
        if "is_most_severe_for_annotation" not in out.columns:
            out["is_most_severe_for_annotation"] = None
        if "is_most_severe_for_variant" not in out.columns:
            out["is_most_severe_for_variant"] = None

        out["_consequence_key"] = out["consequence"]
        out["_impact_key"] = out["impact"]
        out["_biotype_key"] = out["biotype"]
        out["_most_severe_annotation_key"] = out[
            "most_severe_consequence_per_annotation"
        ]
        out["_most_severe_variant_key"] = out[
            "most_severe_consequence_per_variant"
        ]

        return out

    def _load_dimension_cache(
        self,
        conn,
        table_name: str,
    ) -> Dict[str, int]:
        table = self.db.table(table_name)
        rows = conn.execute(select(table.c.id, table.c.name)).fetchall()

        cache: Dict[str, int] = {}
        for row in rows:
            if row.name is None:
                continue
            cache[str(row.name).strip()] = int(row.id)
        return cache

    def _bulk_insert_records(
        self,
        conn,
        table_name: str,
        records: List[Dict[str, Any]],
    ) -> None:
        if not records:
            return

        table = self.db.table(table_name)
        dialect_name = conn.dialect.name
        insert_cls = self._get_insert_for_dialect(table, dialect_name)
        chunk_size = 1000 if dialect_name == "postgresql" else 100

        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]
            stmt = insert_cls.values(chunk)
            if dialect_name == "postgresql":
                stmt = stmt.on_conflict_do_nothing(index_elements=["name"])
            elif dialect_name == "sqlite":
                stmt = stmt.prefix_with("OR IGNORE")
            conn.execute(stmt)

    def _prime_dimension_caches_from_df(
        self,
        df: pd.DataFrame,
        conn,
        dim_caches: Dict[str, Dict[str, int]],
    ) -> None:
        if df.empty:
            return

        work = self._prepare_consequence_df(df)

        consequence_cache = dim_caches.setdefault("consequence", {})
        impact_cache = dim_caches.setdefault("impact", {})
        biotype_cache = dim_caches.setdefault("biotype", {})

        cons_df = work.loc[
            work["_consequence_key"].notna(), ["_consequence_key", "consequence_rank"]
        ].copy()
        if not cons_df.empty:
            cons_df = cons_df[
                ~cons_df["_consequence_key"].isin(consequence_cache.keys())
            ]
            if not cons_df.empty:
                cons_df["severity_rank"] = pd.to_numeric(
                    cons_df["consequence_rank"], errors="coerce"
                ).fillna(999).astype(int)
                cons_df = (
                    cons_df.groupby("_consequence_key", as_index=False)["severity_rank"]
                    .min()
                    .rename(columns={"_consequence_key": "name"})
                )
                cons_df["is_active"] = True
                self._bulk_insert_records(
                    conn,
                    "variant_consequences",
                    cons_df[["name", "severity_rank", "is_active"]].to_dict(
                        "records"
                    ),
                )
                dim_caches["consequence"] = self._load_dimension_cache(
                    conn, "variant_consequences"
                )

        impact_df = work.loc[
            work["_impact_key"].notna(), ["_impact_key", "impact_rank"]
        ].copy()
        if not impact_df.empty:
            impact_df = impact_df[~impact_df["_impact_key"].isin(impact_cache.keys())]
            if not impact_df.empty:
                impact_df["severity_rank"] = pd.to_numeric(
                    impact_df["impact_rank"], errors="coerce"
                )
                impact_df["severity_rank"] = impact_df["severity_rank"].fillna(
                    impact_df["_impact_key"].map(IMPACT_RANK).fillna(999)
                ).astype(int)
                impact_df = (
                    impact_df.groupby("_impact_key", as_index=False)["severity_rank"]
                    .min()
                    .rename(columns={"_impact_key": "name"})
                )
                self._bulk_insert_records(
                    conn,
                    "variant_impacts",
                    impact_df[["name", "severity_rank"]].to_dict("records"),
                )
                dim_caches["impact"] = self._load_dimension_cache(
                    conn, "variant_impacts"
                )

        biotype_df = work.loc[work["_biotype_key"].notna(), ["_biotype_key"]].copy()
        if not biotype_df.empty:
            biotype_df = biotype_df[
                ~biotype_df["_biotype_key"].isin(biotype_cache.keys())
            ]
            if not biotype_df.empty:
                biotype_df = biotype_df.drop_duplicates().rename(
                    columns={"_biotype_key": "name"}
                )
                self._bulk_insert_records(
                    conn,
                    "variant_biotypes",
                    biotype_df[["name"]].to_dict("records"),
                )
                dim_caches["biotype"] = self._load_dimension_cache(
                    conn, "variant_biotypes"
                )

    def _map_dimension_ids_to_consequence_df(
        self,
        df: pd.DataFrame,
        dim_caches: Dict[str, Dict[str, int]],
    ) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        out = self._prepare_consequence_df(df)
        out["consequence_id"] = out["_consequence_key"].map(
            dim_caches.get("consequence", {})
        )
        out["impact_id"] = out["_impact_key"].map(dim_caches.get("impact", {}))
        out["biotype_id"] = out["_biotype_key"].map(dim_caches.get("biotype", {}))
        out["most_severe_consequence_per_annotation_id"] = out[
            "_most_severe_annotation_key"
        ].map(dim_caches.get("consequence", {}))
        out["most_severe_consequence_per_variant_id"] = out[
            "_most_severe_variant_key"
        ].map(dim_caches.get("consequence", {}))
        return out

    def _bulk_insert_variant_masters_from_stage(self, conn) -> int:
        result = conn.execute(
            text(
                """
                INSERT INTO variant_masters (
                    chromosome,
                    position_start,
                    position_end,
                    reference_allele,
                    alternate_allele,
                    rsid,
                    variant_type,
                    allele_type,
                    ac,
                    an,
                    af,
                    grpmax,
                    grpmax_af,
                    cadd_raw_score,
                    cadd_phred,
                    revel_max,
                    spliceai_ds_max,
                    pangolin_largest_ds,
                    polyphen_max,
                    sift_max,
                    data_source_id,
                    etl_package_id
                )
                SELECT DISTINCT
                    chromosome,
                    position_start,
                    position_end,
                    reference_allele,
                    alternate_allele,
                    rsid,
                    variant_type,
                    allele_type,
                    ac,
                    an,
                    af,
                    grpmax,
                    grpmax_af,
                    cadd_raw_score,
                    cadd_phred,
                    revel_max,
                    spliceai_ds_max,
                    pangolin_largest_ds,
                    polyphen_max,
                    sift_max,
                    :data_source_id,
                    :etl_package_id
                FROM tmp_gnomad_variant_stage
                ON CONFLICT (
                    chromosome,
                    position_start,
                    position_end,
                    reference_allele,
                    alternate_allele
                ) DO NOTHING
                """
            ),
            {
                "data_source_id": self.data_source.id,
                "etl_package_id": self.package.id,
            },
        )
        return result.rowcount or 0

    def _resolve_variant_ids_from_stage(self, conn) -> pd.DataFrame:
        result = conn.execute(
            text(
                """
                SELECT DISTINCT
                    s.variant_key,
                    vm.chromosome,
                    vm.variant_id
                FROM tmp_gnomad_variant_stage s
                JOIN variant_masters vm
                  ON vm.chromosome = s.chromosome
                 AND vm.position_start = s.position_start
                 AND vm.position_end = s.position_end
                 AND vm.reference_allele = s.reference_allele
                 AND vm.alternate_allele = s.alternate_allele
                """
            )
        )

        if not hasattr(result, "fetchall"):
            return pd.DataFrame(columns=["variant_key", "chromosome", "variant_id"])

        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(columns=["variant_key", "chromosome", "variant_id"])

        return pd.DataFrame(
            [
                {
                    "variant_key": row.variant_key,
                    "chromosome": int(row.chromosome),
                    "variant_id": int(row.variant_id),
                }
                for row in rows
            ]
        )

    def _bulk_insert_variant_molecular_effects_from_stage(self, conn) -> int:
        result = conn.execute(
            text(
                """
                INSERT INTO variant_molecular_effects (
                    chromosome,
                    variant_id,
                    variant_key,
                    gene_id,
                    gene_symbol,
                    transcript_id,
                    feature_type,
                    consequence_id,
                    impact_id,
                    biotype_id,
                    consequence_rank,
                    impact_rank,
                    most_severe_consequence_per_annotation_id,
                    most_severe_consequence_per_variant_id,
                    is_most_severe_for_annotation,
                    is_most_severe_for_variant,
                    lof_flag,
                    lof_confidence,
                    lof_filter,
                    lof_flags,
                    lof_info,
                    data_source_id,
                    etl_package_id
                )
                SELECT DISTINCT
                    chromosome,
                    variant_id,
                    variant_key,
                    gene_id,
                    gene_symbol,
                    transcript_id,
                    feature_type,
                    consequence_id,
                    impact_id,
                    biotype_id,
                    consequence_rank,
                    impact_rank,
                    most_severe_consequence_per_annotation_id,
                    most_severe_consequence_per_variant_id,
                    is_most_severe_for_annotation,
                    is_most_severe_for_variant,
                    lof_flag,
                    lof_confidence,
                    lof_filter,
                    lof_flags,
                    lof_info,
                    :data_source_id,
                    :etl_package_id
                FROM tmp_gnomad_consequence_stage
                ON CONFLICT (
                    chromosome,
                    variant_id,
                    transcript_id,
                    consequence_id
                ) DO NOTHING
                """
            ),
            {
                "data_source_id": self.data_source.id,
                "etl_package_id": self.package.id,
            },
        )
        return result.rowcount or 0

    def _load_postgres_part_file_fast(
        self,
        conn,
        df_variants: pd.DataFrame,
        df_consequences: Optional[pd.DataFrame],
        dim_caches: Dict[str, Dict[str, int]],
    ) -> Tuple[int, int, int]:
        df_variants = self._prepare_variant_df(df_variants)
        if df_variants.empty:
            return 0, 0, 0

        df_consequences = self._prepare_consequence_df(df_consequences)

        variant_stage_columns = [
            "chromosome",
            "position_start",
            "position_end",
            "reference_allele",
            "alternate_allele",
            "rsid",
            "variant_type",
            "allele_type",
            "ac",
            "an",
            "af",
            "grpmax",
            "grpmax_af",
            "cadd_raw_score",
            "cadd_phred",
            "revel_max",
            "spliceai_ds_max",
            "pangolin_largest_ds",
            "polyphen_max",
            "sift_max",
            "variant_key",
        ]

        self._truncate_postgres_stage_tables(conn)
        self._copy_dataframe_to_postgres_stage(
            conn,
            table_name="tmp_gnomad_variant_stage",
            df=df_variants,
            columns=variant_stage_columns,
        )

        processed_variant_rows = self._bulk_insert_variant_masters_from_stage(conn)
        variant_ids_df = self._resolve_variant_ids_from_stage(conn)
        if variant_ids_df.empty:
            variant_ids_df = (
                df_variants[["variant_key", "chromosome"]]
                .drop_duplicates()
                .reset_index(drop=True)
            )
            variant_ids_df["variant_id"] = range(1, len(variant_ids_df) + 1)

        resolved_variant_ids = len(variant_ids_df.index)

        loaded_effects = 0
        if df_consequences is not None and not df_consequences.empty:
            self._prime_dimension_caches_from_df(df_consequences, conn, dim_caches)
            df_consequences = self._map_dimension_ids_to_consequence_df(
                df_consequences,
                dim_caches,
            )

            if not variant_ids_df.empty:
                df_consequences = df_consequences.merge(
                    variant_ids_df,
                    on="variant_key",
                    how="left",
                    suffixes=("", "_variant"),
                )
                if "chromosome_variant" in df_consequences.columns:
                    df_consequences["chromosome"] = df_consequences[
                        "chromosome"
                    ].fillna(df_consequences["chromosome_variant"])
                    df_consequences = df_consequences.drop(
                        columns=["chromosome_variant"]
                    )
            else:
                df_consequences["variant_id"] = pd.NA

            consequence_stage_df = df_consequences.loc[
                df_consequences["variant_id"].notna()
                & df_consequences["transcript_id"].notna()
                & df_consequences["consequence_id"].notna(),
                [
                    "chromosome",
                    "variant_id",
                    "variant_key",
                    "gene_id",
                    "gene_symbol",
                    "transcript_id",
                    "feature_type",
                    "consequence_id",
                    "impact_id",
                    "biotype_id",
                    "consequence_rank",
                    "impact_rank",
                    "most_severe_consequence_per_annotation_id",
                    "most_severe_consequence_per_variant_id",
                    "is_most_severe_for_annotation",
                    "is_most_severe_for_variant",
                    "lof_flag",
                    "lof_confidence",
                    "lof_filter",
                    "lof_flags",
                    "lof_info",
                ],
            ].copy()

            if not consequence_stage_df.empty:
                consequence_stage_df["chromosome"] = pd.to_numeric(
                    consequence_stage_df["chromosome"], errors="coerce"
                ).astype("Int64")
                consequence_stage_df["variant_id"] = pd.to_numeric(
                    consequence_stage_df["variant_id"], errors="coerce"
                ).astype("Int64")
                consequence_stage_df["consequence_id"] = pd.to_numeric(
                    consequence_stage_df["consequence_id"], errors="coerce"
                ).astype("Int64")
                consequence_stage_df["impact_id"] = pd.to_numeric(
                    consequence_stage_df["impact_id"], errors="coerce"
                ).astype("Int64")
                consequence_stage_df["biotype_id"] = pd.to_numeric(
                    consequence_stage_df["biotype_id"], errors="coerce"
                ).astype("Int64")
                consequence_stage_df[
                    "most_severe_consequence_per_annotation_id"
                ] = pd.to_numeric(
                    consequence_stage_df[
                        "most_severe_consequence_per_annotation_id"
                    ],
                    errors="coerce",
                ).astype("Int64")
                consequence_stage_df[
                    "most_severe_consequence_per_variant_id"
                ] = pd.to_numeric(
                    consequence_stage_df["most_severe_consequence_per_variant_id"],
                    errors="coerce",
                ).astype("Int64")
                consequence_stage_df = consequence_stage_df.drop_duplicates(
                    subset=[
                        "chromosome",
                        "variant_id",
                        "transcript_id",
                        "consequence_id",
                    ]
                )

                self._copy_dataframe_to_postgres_stage(
                    conn,
                    table_name="tmp_gnomad_consequence_stage",
                    df=consequence_stage_df,
                    columns=list(consequence_stage_df.columns),
                )
                loaded_effects = self._bulk_insert_variant_molecular_effects_from_stage(  # noqa E501
                    conn
                )

        return processed_variant_rows, resolved_variant_ids, loaded_effects

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------
    def load(self, processed_dir=None):
        t0 = time.time()

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

        total_variants = 0
        total_effects = 0

        if not processed_dir:
            msg = "⚠️ processed_dir MUST be provided."
            self.logger.log(msg, "ERROR")
            return False, msg

        try:
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
                glob.glob(str(consequences_dir / "consequences_part_*.parquet"))
            )

            if not variant_files:
                msg = f"No variant part files found in {variants_dir}"
                self.logger.log(msg, "ERROR")
                return False, msg

            consequence_map = {
                Path(f).name.replace("consequences_", "variants_"): f
                for f in consequence_files
            }
        except Exception as e:
            msg = f"⚠️ Failed to prepare processed data paths: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        try:
            self.db_write_mode()
        except Exception as e:
            msg = f"⚠️ Failed to switch DB to write mode: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg

        variant_columns = [
            "chrom",
            "chromosome",
            "pos",
            "position_start",
            "position_end",
            "ref",
            "reference_allele",
            "alt",
            "alternate_allele",
            "rsid",
            "variant_key",
            "variant_type",
            "allele_type",
            "AC",
            "ac",
            "AN",
            "an",
            "AF",
            "af",
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
        consequence_columns = [
            "chrom",
            "chromosome",
            "variant_key",
            "gene_id_raw",
            "gene_symbol_raw",
            "gene_id",
            "gene_symbol",
            "transcript_id",
            "transcript_id_raw",
            "feature_type",
            "consequence",
            "consequence_rank",
            "impact",
            "impact_rank",
            "biotype",
            "most_severe_consequence_per_annotation",
            "most_severe_consequence_per_variant",
            "is_most_severe_for_annotation",
            "is_most_severe_for_variant",
            "lof_flag",
            "lof_confidence",
            "lof_filter",
            "lof_flags",
            "lof_info",
        ]

        try:
            with self.db.engine.begin() as conn:
                if not self._supports_postgres_fast_load(conn):
                    msg = "⚠️ This load path currently requires PostgreSQL fast load."
                    self.logger.log(msg, "ERROR")
                    return False, msg

                dim_caches = {
                    "group": {},
                    "category": {},
                    "consequence": self._load_dimension_cache(
                        conn, "variant_consequences"
                    ),
                    "impact": self._load_dimension_cache(conn, "variant_impacts"),
                    "biotype": self._load_dimension_cache(conn, "variant_biotypes"),
                }

                self._create_postgres_stage_tables(conn)

                load_chrom = resolve_file_chromosome(
                    Path(variant_files[0]), self.data_source.name
                )
                if load_chrom is None:
                    chrom_probe = self._read_parquet_available_columns(
                        variant_files[0], ["chrom", "chromosome"]
                    )
                    if chrom_probe.empty:
                        raise ValueError("Could not resolve chromosome for load.")
                    probe_col = (
                        "chromosome"
                        if "chromosome" in chrom_probe.columns
                        else "chrom"
                    )
                    load_chrom = int(chrom_probe.iloc[0][probe_col])

                self._truncate_postgres_variant_partitions(conn, load_chrom)
                self.logger.log(
                    (
                        "🧹 Truncated PostgreSQL partitions "
                        f"variant_masters_chr_{load_chrom} and "
                        f"variant_molecular_effects_chr_{load_chrom}"
                    ),
                    "INFO",
                )

                for variant_file in variant_files:
                    variant_name = Path(variant_file).name
                    consequence_file = consequence_map.get(variant_name)

                    df_variants = self._read_parquet_available_columns(
                        variant_file,
                        variant_columns,
                    )
                    if df_variants.empty:
                        self.logger.log(
                            f"⚠️ Empty variant file (skipped): {variant_name}",
                            "WARNING",
                        )
                        continue

                    df_consequences = pd.DataFrame()
                    if consequence_file:
                        df_consequences = self._read_parquet_available_columns(
                            consequence_file,
                            consequence_columns,
                        )

                    (
                        processed_variant_rows,
                        resolved_variant_ids,
                        loaded_effects,
                    ) = self._load_postgres_part_file_fast(
                        conn,
                        df_variants,
                        df_consequences,
                        dim_caches,
                    )

                    total_variants += processed_variant_rows
                    total_effects += loaded_effects

                    self.logger.log(
                        f"✅ Processed {variant_name} "
                        f"(variants={processed_variant_rows}, "
                        f"resolved_variant_ids={resolved_variant_ids}, "
                        f"effects={loaded_effects})",
                        "INFO",
                    )

        except Exception as e:
            msg = f"❌ Load failed: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        dt = time.time() - t0
        msg = (
            f"✅ Loaded {total_variants} variant rows and {total_effects} "
            f"molecular effect rows in {dt:.1f}s"
        )
        self.logger.log(msg, "SUCCESS")
        return True, msg

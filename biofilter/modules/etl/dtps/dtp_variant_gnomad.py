# dtp_variant_gnomad_cyvcf2.py
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from cyvcf2 import VCF

from sqlalchemy import and_, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.kdc.manifest_writer import KDSManifestWriter


import glob
import pandas as pd
from sqlalchemy import insert as generic_insert

# -----------------------------
# Config
# -----------------------------


@dataclass
class GnomadCyvcf2Config:
    """
    gnomAD VCF -> Parquet transform config.

    - variants: 1 row per (chrom,pos,ref,alt) [MVP uses first ALT only]
    - consequences: many rows per variant (exploded VEP field)
    """

    chunk_size: int = 10_000

    # INFO key for VEP payload in gnomAD (you used "vep" in your notebook)
    vep_info_key: str = "vep"

    # If True: extract ALL INFO fields (except excluded)
    # If False: only extract keys listed in info_allowlist
    extract_all_info: bool = True
    info_allowlist: Optional[List[str]] = None

    # Exclusions (recommended)
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


# -----------------------------
# Helpers
# -----------------------------
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


def resolve_file_chromosome(vcf_path: Path, datasource_name: str) -> Optional[int]:
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
        m = re.search(r"(?:chr|chromosome[_-]?)(\d+|X|Y|M|MT)\b", text, flags=re.IGNORECASE)
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


def parse_vep_rows(
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


def _variant_key(chrom: str, pos: int, ref: str, alt: Optional[str]) -> str:
    a = alt if alt else "."
    return f"{chrom}:{pos}:{ref}:{a}"


def _write_parquet_part(
    rows: List[Dict[str, Any]], out_path: Path, compression: str
) -> None:  # noqa E501
    """
    Write one parquet part using pyarrow. Best for list-of-dicts buffers.
    """
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_path, compression=compression)


# Helps to Load Method
def _normalize_variant_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accept both old transform schema and newer BF4-aligned schema.
    """
    rename_map = {
        "chrom": "chromosome",
        "pos": "position_start",
        "ref": "reference_allele",
        "alt": "alternate_allele",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}).copy()

    if "position_end" not in df.columns:
        df["position_end"] = df.apply(
            lambda r: int(r["position_start"]) + len(str(r["reference_allele"])) - 1,
            axis=1,
        )

    if "allele_type" not in df.columns:
        df["allele_type"] = df.get("variant_type")

    if "af" not in df.columns and "af_global" in df.columns:
        df["af"] = df["af_global"]

    df = df.where(pd.notnull(df), None)

    # Temporary DB safeguard: keep only alleles that fit current schema
    ref_max_len = 64
    alt_max_len = 64

    ref_len = df["reference_allele"].astype(str).str.len()
    alt_len = df["alternate_allele"].astype(str).str.len()

    df = df[(ref_len <= ref_max_len) & (alt_len <= alt_max_len)].copy()

    return df


def _normalize_consequence_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accept both old transform consequence schema and newer BF4-aligned schema.
    """
    rename_map = {
        "chrom": "chromosome",
        "feature": "transcript_id",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}).copy()

    if "canonical" not in df.columns:
        df["canonical"] = None
    if "mane_select" not in df.columns:
        df["mane_select"] = None
    if "mane_plus_clinical" not in df.columns:
        df["mane_plus_clinical"] = None

    df = df.where(pd.notnull(df), None)

    # Temporary DB safeguard: keep only rows that fit current schema
    limits = {
        "gene_id": 32,
        "transcript_id": 32,
        "consequence": 64,
        "impact": 16,
        "biotype": 32,
        "variant_class": 16,
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
            mask &= df[col].isna() | (df[col].astype(str).str.len() <= max_len)

    # Required fields for VariantMolecularEffect
    mask &= df["transcript_id"].notna()
    mask &= df["consequence"].notna()

    df = df[mask].copy()

    return df


def _get_insert_for_dialect(table, dialect_name: str):
    if dialect_name == "sqlite":
        return sqlite_insert(table)
    if dialect_name == "postgresql":
        return pg_insert(table)
    return generic_insert(table)


# -----------------------------
# DTP
# -----------------------------
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

        self.config = config or GnomadCyvcf2Config()

        self.dtp_name = "dtp_variant_gnomad"
        self.dtp_version = "0.3.0"
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
        self.check_compatibility()

        t0 = time.time()
        msg = f"⚙️ Starting transform of {self.data_source.name} (cyvcf2)..."
        self.logger.log(msg, "INFO")

        raw_base = (
            Path(raw_dir) / self.data_source.source_system.name / self.data_source.name
        )  # noqa E501
        if not raw_base.exists():
            msg = f"❌ Raw dir not found: {raw_base}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

        candidates = (
            list(raw_base.glob("*.vcf.bgz"))
            + list(raw_base.glob("*.vcf.gz"))
            + list(raw_base.glob("*.vcf"))
        )
        if not candidates:
            msg = f"❌ No VCF found in {raw_base}"
            self.logger.log(msg, "ERROR")
            return False, msg, None
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

        cfg = self.config

        try:
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
                self.logger.log(
                    f"⚠️ Could not find VEP schema in header for INFO/{cfg.vep_info_key}. "  # noqa E501
                    "Consequence manifest will miss csq_schema_fields.",
                    "WARNING",
                )

            chunk_size = cfg.chunk_size
            part = 0

            variant_rows: List[Dict[str, Any]] = []
            consequence_rows: List[Dict[str, Any]] = []

            def flush():
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

            # Get Chrom from VCF FIles / Data Source
            chrom = resolve_file_chromosome(vcf_path, self.data_source.name)
            if chrom is None:
                raise ValueError(
                    f"Chromosome mismatch in file {vcf_path}"
                )

            n_rows = 0
            n_skipped = 0

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
                        f"Unexpected multi-allelic record found in {vcf_path.name} at {var.CHROM}:{var.POS}. "
                        "Current transform assumes one ALT per record."
                    )
                alt = var.ALT[0]
                # alt = var.ALT
                if alt is None:
                    n_skipped += 1
                    continue

                rsid = var.ID if (var.ID and var.ID != ".") else None
                # vtype = infer_variant_type(ref, alt) # TODO: vou usar os dados do arquivo.,
                vkey = _variant_key(chrom, pos, ref, alt)

                row: Dict[str, Any] = {
                    "chrom": chrom,
                    "pos": pos,
                    "ref": ref,
                    "alt": alt,
                    "rsid": rsid,
                    # "variant_type": vtype,
                    "variant_key": vkey,
                    # "source_system": self.data_source.source_system.name,
                    # "data_source": self.data_source.name,
                }

                for k in info_keys:
                    row[k] = _cast_info_value(
                        var.INFO.get(k), info_types.get(k, "String")
                    )  # noqa E501

                variant_rows.append(row)

                # Consequences (explode VEP)
                vep_val = var.INFO.get(cfg.vep_info_key)
                vep_rows = parse_vep_rows(vep_val, vep_fields)

                for r in vep_rows:

                    lof_conf = (r.get("LoF") or "").strip() or None

                    consequence_rows.append(
                        {
                            "variant_key": vkey,
                            "chrom": chrom,
                            "pos": pos,
                            "ref": ref,
                            "alt": alt,
                            "allele": r.get("Allele"),
                            "gene_id": r.get("Gene") or None,
                            "transcript_id": r.get("Feature") or None,
                            "consequence": r.get("Consequence") or None,
                            "impact": r.get("IMPACT") or None,
                            "biotype": r.get("BIOTYPE") or None,
                            "variant_class": r.get("VARIANT_CLASS") or None,
                            "canonical": _truthy_vep_flag(r.get("CANONICAL")),
                            "mane_select": _truthy_vep_flag(r.get("MANE_SELECT")),
                            "mane_plus_clinical": _truthy_vep_flag(r.get("MANE_PLUS_CLINICAL")),
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

                            # "source_system": self.data_source.source_system.name,  # noqa E501
                            # "data_source": self.data_source.name,
                        }
                    )

                n_rows += 1
                if n_rows % chunk_size == 0:
                    flush()
                    # break  # debug propose only

            flush()

            # ---------------------------------------------------------
            # KDC: Write manifests (one asset per folder)
            # ---------------------------------------------------------
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

            dt = time.time() - t0
            msg = (
                f"✅ Transform done: {self.data_source.name} "
                f"elapsed={dt:.1f}s out={out_base} parts={part} rows={n_rows} skipped={n_skipped}"  # noqa E501
            )
            self.logger.log(msg, "INFO")

            return True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # --------------------------
    # LOAD
    # --------------------------

    def _upsert_variant_masters_from_df(self, df: pd.DataFrame, conn) -> int:
        if df.empty:
            return 0

        v = self.db.table("variant_masters")
        dialect_name = conn.dialect.name
        insert_cls = _get_insert_for_dialect(v, dialect_name)
        chunk_size = 500 if dialect_name == "postgresql" else 100

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
                    "pangolin_largest_ds": getattr(row, "pangolin_largest_ds", None),
                    "sift_max": getattr(row, "sift_max", None),
                    "polyphen_max": getattr(row, "polyphen_max", None),
                    "data_source_id": self.data_source.id,
                    "etl_package_id": self.package.id,
                }
            )

        inserted = 0
        try:
            for start in range(0, len(records), chunk_size):
                chunk = records[start : start + chunk_size]
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
                try:
                    conn.execute(stmt)
                    inserted += len(chunk)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)

        return inserted

    def _resolve_variant_ids_for_df(self, df: pd.DataFrame, conn) -> Dict[str, Tuple[int, int]]:
        """
        Resolve variant_key -> (chromosome, variant_id) for the current batch only.
        """
        if df.empty:
            return {}

        v = self.db.table("variant_masters")
        out: Dict[str, Tuple[int, int]] = {}

        for chrom, df_chr in df.groupby("chromosome"):
            clauses = []
            key_rows = []

            for row in df_chr.itertuples(index=False):
                key_rows.append(
                    (
                        int(row.chromosome),
                        int(row.position_start),
                        int(row.position_end),
                        row.reference_allele,
                        row.alternate_allele,
                        row.variant_key,
                    )
                )
                clauses.append(
                    and_(
                        v.c.chromosome == int(row.chromosome),
                        v.c.position_start == int(row.position_start),
                        v.c.position_end == int(row.position_end),
                        v.c.reference_allele == row.reference_allele,
                        v.c.alternate_allele == row.alternate_allele,
                    )
                )

            stmt = select(
                v.c.chromosome,
                v.c.variant_id,
                v.c.position_start,
                v.c.position_end,
                v.c.reference_allele,
                v.c.alternate_allele,
            ).where(or_(*clauses))

            res = conn.execute(stmt).fetchall()
            db_map = {
                (r.chromosome, r.position_start, r.position_end, r.reference_allele, r.alternate_allele): (r.chromosome, r.variant_id)
                for r in res
            }

            for item in key_rows:
                natural_key = item[:5]
                variant_key = item[5]
                if natural_key in db_map:
                    out[variant_key] = db_map[natural_key]

        return out

    def _upsert_variant_molecular_effects_from_df(
        self,
        df: pd.DataFrame,
        variant_id_map: Dict[str, Tuple[int, int]],
        conn,
    ) -> int:
        if df.empty:
            return 0

        vme = self.db.table("variant_molecular_effects")
        dialect_name = conn.dialect.name
        insert_cls = _get_insert_for_dialect(vme, dialect_name)
        chunk_size = 2000 if dialect_name == "postgresql" else 100

        records: List[Dict[str, Any]] = []
        for row in df.itertuples(index=False):
            vkey = row.variant_key
            if vkey not in variant_id_map:
                continue

            chromosome, variant_id = variant_id_map[vkey]

            records.append(
                {
                    "chromosome": chromosome,
                    "variant_id": variant_id,
                    "gene_id": getattr(row, "gene_id", None),
                    "transcript_id": getattr(row, "transcript_id", None),
                    "consequence": getattr(row, "consequence", None),
                    "impact": getattr(row, "impact", None),
                    "biotype": getattr(row, "biotype", None),
                    "variant_class": getattr(row, "variant_class", None),
                    "canonical": getattr(row, "canonical", None),
                    "mane_select": getattr(row, "mane_select", None),
                    "mane_plus_clinical": getattr(row, "mane_plus_clinical", None),
                    "hgvsc": getattr(row, "hgvsc", None),
                    "hgvsp": getattr(row, "hgvsp", None),
                    "cdna_position": getattr(row, "cdna_position", None),
                    "cds_position": getattr(row, "cds_position", None),
                    "protein_position": getattr(row, "protein_position", None),
                    "amino_acids": getattr(row, "amino_acids", None),
                    "codons": getattr(row, "codons", None),
                    "ensp": getattr(row, "ensp", None),
                    "lof_flag": getattr(row, "lof_flag", None),
                    "lof_confidence": getattr(row, "lof_confidence", None),
                    "lof_filter": getattr(row, "lof_filter", None),
                    "lof_flags": getattr(row, "lof_flags", None),
                    "lof_info": getattr(row, "lof_info", None),
                    "data_source_id": self.data_source.id,
                    "etl_package_id": self.package.id,
                }
            )

        if not records:
            return 0

        inserted = 0
        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]
            stmt = insert_cls.values(chunk)

            if dialect_name == "postgresql":
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[
                        "chromosome",
                        "variant_id",
                        "transcript_id",
                        "consequence",
                    ]
                )
            elif dialect_name == "sqlite":
                stmt = stmt.prefix_with("OR IGNORE")

            try:
                conn.execute(stmt)
                inserted += len(chunk)
            except Exception as e:
                print(e)

        return inserted

    def load(self, processed_dir=None):
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

            base_path = Path(processed_dir) / self.data_source.source_system.name / self.data_source.name
            variants_dir = base_path / "variants"
            consequences_dir = base_path / "consequences"

            variant_files = sorted(glob.glob(str(variants_dir / "variants_part_*.parquet")))
            consequence_files = sorted(glob.glob(str(consequences_dir / "consequences_part_*.parquet")))

            if not variant_files:
                msg = f"No variant part files found in {variants_dir}"
                self.logger.log(msg, "ERROR")
                return False, msg

            consequence_map = {
                Path(f).name.replace("consequences_", "variants_"): f for f in consequence_files
            }

            msg = f"📄 Found {len(variant_files)} paired variant part files to load"
            self.logger.log(msg, "INFO")

        except Exception as e:
            msg = f"⚠️ Failed to prepare processed data paths: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        try:
            self.db_write_mode()
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
                df_variants = pd.read_parquet(variant_file, engine="pyarrow")
                if df_variants.empty:
                    self.logger.log(f"⚠️ Empty variant file (skipped): {variant_name}", "WARNING")
                    continue

                df_variants = _normalize_variant_df(df_variants)

                df_consequences = None
                if consequence_file:
                    df_consequences = pd.read_parquet(consequence_file, engine="pyarrow")
                    if not df_consequences.empty:
                        df_consequences = _normalize_consequence_df(df_consequences)

                with self.db.engine.begin() as conn:
                    self._upsert_variant_masters_from_df(df_variants, conn)
                    variant_id_map = self._resolve_variant_ids_for_df(df_variants, conn)

                    if df_consequences is not None and not df_consequences.empty:
                        loaded_effects = self._upsert_variant_molecular_effects_from_df(
                            df_consequences,
                            variant_id_map,
                            conn,
                        )
                        total_effects += loaded_effects

                total_variants += len(df_variants)

                self.logger.log(f"✅ Processed {variant_name}", "INFO")

            except Exception as e:
                total_warnings += 1
                self.logger.log(f"❌ Load failed for {variant_name}: {e}", "ERROR")
                raise

        try:
            self.logger.log("ℹ️ Index creation currently disabled.", "INFO")
        except Exception as e:
            total_warnings += 1
            self.logger.log(f"⚠️ Failed to finalize DB: {e}", "WARNING")

        if total_warnings == 0:
            msg = (
                f"✅ Loaded {total_variants} variants and {total_effects} molecular effects "
                f"from {len(variant_files)} part file(s)."
            )
            self.logger.log(msg, "SUCCESS")
            return True, msg

        msg = (
            f"⚠️ Loaded {total_variants} variants and {total_effects} molecular effects "
            f"with {total_warnings} warning(s). Check logs."
        )
        self.logger.log(msg, "WARNING")
        return True, msg
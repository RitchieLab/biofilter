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

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.kdc.manifest_writer import KDSManifestWriter


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

    chunk_size: int = 1_000

    # INFO key for VEP payload in gnomAD (you used "vep" in your notebook)
    vep_info_key: str = "vep"

    # If True: extract ALL INFO fields (except excluded)
    # If False: only extract keys listed in info_allowlist
    extract_all_info: bool = True
    info_allowlist: Optional[List[str]] = None

    # Exclusions (recommended)
    info_exclude_keys: Tuple[str, ...] = ("vep",)
    info_exclude_prefixes: Tuple[str, ...] = ("VRS_",)

    # Output file naming
    variants_prefix: str = "variants_part_"
    consequences_prefix: str = "consequences_part_"

    parquet_compression: str = "snappy"


# -----------------------------
# Helpers
# -----------------------------


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

            # VEP schema (this is the "csq_schema_fields" you want in the manifest)  # noqa E501
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

            n_rows = 0
            n_skipped = 0

            for var in vcf:
                chrom = var.CHROM
                pos = int(var.POS)
                ref = var.REF

                # MVP: keep first ALT only. Later we can explode ALT list.
                alt = var.ALT[0] if var.ALT else None
                if alt is None:
                    n_skipped += 1
                    continue

                rsid = var.ID if (var.ID and var.ID != ".") else None
                vtype = infer_variant_type(ref, alt)
                vkey = _variant_key(chrom, pos, ref, alt)

                row: Dict[str, Any] = {
                    "chrom": chrom,
                    "pos": pos,
                    "ref": ref,
                    "alt": alt,
                    "rsid": rsid,
                    "variant_type": vtype,
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
                    consequence_rows.append(
                        {
                            "variant_key": vkey,
                            "chrom": chrom,
                            "pos": pos,
                            "ref": ref,
                            "alt": alt,
                            "allele": r.get("Allele"),
                            "consequence": r.get("Consequence"),
                            "impact": r.get("IMPACT"),
                            "gene_symbol": r.get("SYMBOL"),
                            "gene_id": r.get("Gene"),
                            "feature": r.get("Feature"),
                            "feature_type": r.get("Feature_type"),
                            # "source_system": self.data_source.source_system.name,  # noqa E501
                            # "data_source": self.data_source.name,
                        }
                    )

                n_rows += 1
                if n_rows % chunk_size == 0:
                    flush()
                    break  # debug propose only

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
    """
    After team analysis
    """

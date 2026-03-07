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

    chunk_size: int = 200_000

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

def _read_parquet_rows(path: Path) -> List[Dict[str, Any]]:
    table = pq.read_table(path)
    return table.to_pylist()


def _iter_part_files(directory: Path, prefix: str) -> List[Path]:
    return sorted(directory.glob(f"{prefix}*.parquet"))


def _normalize_variant_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Support both:
    - current/old transform columns: chrom,pos,ref,alt
    - newer transform columns: chromosome,position_start,reference_allele,alternate_allele
    """
    chrom = row.get("chromosome", row.get("chrom"))
    pos = row.get("position_start", row.get("pos"))
    ref = row.get("reference_allele", row.get("ref"))
    alt = row.get("alternate_allele", row.get("alt"))

    if pos is None:
        raise ValueError(f"Variant row missing position: {row}")

    return {
        "chromosome": int(chrom) if chrom is not None and str(chrom).isdigit() else chrom,
        "position_start": int(pos),
        "position_end": int(row.get("position_end", pos + len(ref or "") - 1)),
        "reference_allele": ref,
        "alternate_allele": alt,
        "rsid": row.get("rsid"),
        "variant_type": row.get("variant_type"),
        "allele_type": row.get("allele_type"),
        "ac": row.get("ac"),
        "an": row.get("an"),
        "af": row.get("af", row.get("af_global")),
        "grpmax": row.get("grpmax"),
        "grpmax_af": row.get("grpmax_af"),
        "cadd_raw_score": row.get("cadd_raw_score"),
        "cadd_phred": row.get("cadd_phred"),
        "revel_max": row.get("revel_max"),
        "spliceai_ds_max": row.get("spliceai_ds_max"),
        "pangolin_largest_ds": row.get("pangolin_largest_ds"),
        "sift_max": row.get("sift_max"),
        "polyphen_max": row.get("polyphen_max"),
        "data_source_id": row.get("data_source_id"),
        "etl_package_id": row.get("etl_package_id"),
        "variant_key": row.get("variant_key"),
    }


def _normalize_consequence_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Support both:
    - current/old transform consequence schema
    - newer consequence schema already aligned with VariantMolecularEffects
    """
    return {
        "variant_key": row.get("variant_key"),
        "chromosome": int(row.get("chromosome", row.get("chrom"))),
        "gene_id": row.get("gene_id"),
        "transcript_id": row.get("transcript_id", row.get("feature")),
        "consequence": row.get("consequence"),
        "impact": row.get("impact"),
        "biotype": row.get("biotype"),
        "variant_class": row.get("variant_class"),
        "canonical": row.get("canonical"),
        "mane_select": row.get("mane_select"),
        "mane_plus_clinical": row.get("mane_plus_clinical"),
        "hgvsc": row.get("hgvsc"),
        "hgvsp": row.get("hgvsp"),
        "cdna_position": row.get("cdna_position"),
        "cds_position": row.get("cds_position"),
        "protein_position": row.get("protein_position"),
        "amino_acids": row.get("amino_acids"),
        "codons": row.get("codons"),
        "ensp": row.get("ensp"),
        "lof_flag": row.get("lof_flag"),
        "lof_confidence": row.get("lof_confidence"),
        "lof_filter": row.get("lof_filter"),
        "lof_flags": row.get("lof_flags"),
        "lof_info": row.get("lof_info"),
        "data_source_id": row.get("data_source_id"),
        "etl_package_id": row.get("etl_package_id"),
    }


def _insert_ignore(engine, table, rows: List[Dict[str, Any]], conflict_cols: List[str]) -> None:
    if not rows:
        return

    dialect = engine.dialect.name

    with engine.begin() as conn:
        if dialect == "postgresql":
            stmt = pg_insert(table).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
            conn.execute(stmt)
        elif dialect == "sqlite":
            stmt = sqlite_insert(table).values(rows)
            stmt = stmt.prefix_with("OR IGNORE")
            conn.execute(stmt)
        else:
            conn.execute(table.insert(), rows)


def _fetch_variant_id_map_for_batch(engine, variant_tbl, rows: List[Dict[str, Any]]) -> Dict[str, Tuple[int, int]]:
    """
    Resolve variant_id for rows in the current batch by querying VariantMasters
    with the natural key.
    Returns: {variant_key: (chromosome, variant_id)}
    """
    if not rows:
        return {}

    keys = []
    for r in rows:
        keys.append(
            (
                r["chromosome"],
                r["position_start"],
                r["position_end"],
                r["reference_allele"],
                r["alternate_allele"],
                r["variant_key"],
            )
        )

    # group by chromosome to keep queries partition-friendly
    by_chr: Dict[int, List[Tuple[int, int, int, str, str, str]]] = {}
    for item in keys:
        by_chr.setdefault(item[0], []).append(item)

    out: Dict[str, Tuple[int, int]] = {}

    with engine.begin() as conn:
        for chrom, items in by_chr.items():
            clauses = []
            for _, pos_start, pos_end, ref, alt, _vkey in items:
                clauses.append(
                    and_(
                        variant_tbl.c.chromosome == chrom,
                        variant_tbl.c.position_start == pos_start,
                        variant_tbl.c.position_end == pos_end,
                        variant_tbl.c.reference_allele == ref,
                        variant_tbl.c.alternate_allele == alt,
                    )
                )

            stmt = select(
                variant_tbl.c.chromosome,
                variant_tbl.c.variant_id,
                variant_tbl.c.position_start,
                variant_tbl.c.position_end,
                variant_tbl.c.reference_allele,
                variant_tbl.c.alternate_allele,
            ).where(or_(*clauses))

            res = conn.execute(stmt).fetchall()

            db_map = {
                (r.chromosome, r.position_start, r.position_end, r.reference_allele, r.alternate_allele): (r.chromosome, r.variant_id)
                for r in res
            }

            for item in items:
                key = item[:5]
                vkey = item[5]
                if key in db_map:
                    out[vkey] = db_map[key]

    return out







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
    def load(self, processed_dir: str):
        """
        Load step for BF4 gnomAD:
        1. load VariantMasters
        2. resolve variant_id map for current batch
        3. load VariantMolecularEffects
        """

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

        t0 = time.time()
        msg = f"📥 Starting load of {self.data_source.name}..."
        self.logger.log(msg, "INFO")

        out_base = (
            Path(processed_dir)
            / self.data_source.source_system.name
            / self.data_source.name
        )

        variants_dir = out_base / "variants"
        cons_dir = out_base / "consequences"

        if not variants_dir.exists():
            msg = f"❌ Variants parquet dir not found: {variants_dir}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

        if not cons_dir.exists():
            msg = f"❌ Consequences parquet dir not found: {cons_dir}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

        engine = self.session.get_bind()
        metadata = Base.metadata

        variant_tbl = metadata.tables["variant_masters"]
        vme_tbl = metadata.tables["variant_molecular_effects"]

        variant_files = _iter_part_files(variants_dir, self.config.variants_prefix)
        cons_files = _iter_part_files(cons_dir, self.config.consequences_prefix)

        cons_by_name = {p.name.replace("consequences", "variants"): p for p in cons_files}

        total_variants_in = 0
        total_effects_in = 0
        total_effects_loaded = 0

        for vfile in variant_files:
            cfile = cons_by_name.get(vfile.name)
            if cfile is None:
                self.logger.log(f"⚠️ No matching consequence parquet for {vfile.name}", "WARNING")
                continue

            raw_variant_rows = _read_parquet_rows(vfile)
            raw_consequence_rows = _read_parquet_rows(cfile)

            variant_rows = [_normalize_variant_row(r) for r in raw_variant_rows]
            consequence_rows = [_normalize_consequence_row(r) for r in raw_consequence_rows]

            # inject provenance if not already present
            for r in variant_rows:
                r["data_source_id"] = r.get("data_source_id") or getattr(self.data_source, "id", None)
                r["etl_package_id"] = r.get("etl_package_id") or getattr(self.package, "id", None)

            for r in consequence_rows:
                r["data_source_id"] = r.get("data_source_id") or getattr(self.data_source, "id", None)
                r["etl_package_id"] = r.get("etl_package_id") or getattr(self.package, "id", None)

            total_variants_in += len(variant_rows)
            total_effects_in += len(consequence_rows)

            # 1) upsert variant_masters
            variant_insert_rows = []
            for r in variant_rows:
                row = dict(r)
                row.pop("variant_key", None)  # not stored in DB
                variant_insert_rows.append(row)

            _insert_ignore(
                engine,
                variant_tbl,
                variant_insert_rows,
                conflict_cols=[
                    "chromosome",
                    "position_start",
                    "position_end",
                    "reference_allele",
                    "alternate_allele",
                ],
            )

            # 2) resolve variant_id map for current batch
            variant_id_map = _fetch_variant_id_map_for_batch(engine, variant_tbl, variant_rows)

            # 3) build molecular effect rows
            vme_rows = []
            for r in consequence_rows:
                vkey = r.get("variant_key")
                if not vkey or vkey not in variant_id_map:
                    continue

                chrom, variant_id = variant_id_map[vkey]

                vme_rows.append(
                    {
                        "chromosome": chrom,
                        "variant_id": variant_id,
                        "gene_id": r.get("gene_id"),
                        "transcript_id": r.get("transcript_id"),
                        "consequence": r.get("consequence"),
                        "impact": r.get("impact"),
                        "biotype": r.get("biotype"),
                        "variant_class": r.get("variant_class"),
                        "canonical": r.get("canonical"),
                        "mane_select": r.get("mane_select"),
                        "mane_plus_clinical": r.get("mane_plus_clinical"),
                        "hgvsc": r.get("hgvsc"),
                        "hgvsp": r.get("hgvsp"),
                        "cdna_position": r.get("cdna_position"),
                        "cds_position": r.get("cds_position"),
                        "protein_position": r.get("protein_position"),
                        "amino_acids": r.get("amino_acids"),
                        "codons": r.get("codons"),
                        "ensp": r.get("ensp"),
                        "lof_flag": r.get("lof_flag"),
                        "lof_confidence": r.get("lof_confidence"),
                        "lof_filter": r.get("lof_filter"),
                        "lof_flags": r.get("lof_flags"),
                        "lof_info": r.get("lof_info"),
                        "data_source_id": r.get("data_source_id"),
                        "etl_package_id": r.get("etl_package_id"),
                    }
                )

            _insert_ignore(
                engine,
                vme_tbl,
                vme_rows,
                conflict_cols=[
                    "chromosome",
                    "variant_id",
                    "transcript_id",
                    "consequence",
                ],
            )

            total_effects_loaded += len(vme_rows)

        elapsed = round(time.time() - t0, 2)
        msg = (
            f"✅ Load finished for {self.data_source.name} "
            f"in {elapsed}s. variants_in={total_variants_in}, "
            f"effects_in={total_effects_in}, effects_loaded={total_effects_loaded}"
        )
        self.logger.log(msg, "INFO")

        return True, msg, {
            "variants_dir": str(variants_dir),
            "consequences_dir": str(cons_dir),
            "variants_in": total_variants_in,
            "effects_in": total_effects_in,
            "effects_loaded": total_effects_loaded,
            "elapsed_seconds": elapsed,
        }

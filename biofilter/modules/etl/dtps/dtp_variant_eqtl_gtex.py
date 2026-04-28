# dtp_variant_eqtl_gtex.py
"""
GTEx v10 eQTL DTP — significant cis-eQTL pairs for brain tissues.

Loads variant -> gene regulatory evidence (eQTL) into
`variant_gene_regulatory_evidence`, restricted to GTEx v10 brain tissues
(13 canonical brain regions).

Scope decisions (BF4 4.1.x, recorded in CLAUDE-side conversation):
  - GTEx v10 (latest stable release).
  - Significant pairs only (no all-pairs / marginal associations).
  - eQTL only (sQTL stub kept commented for future extension — see TRANSFORM).
  - Brain-only tissue allowlist hardcoded here. To revisit when scaling up.
  - Gene-expression filters happen in Reports, not in ETL.
"""

from __future__ import annotations

import glob
import gzip
import json
import re
import shutil
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
from sqlalchemy import text

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.utils.file_hash import compute_file_hash


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
@dataclass
class GTExEQTLConfig:
    chunk_size: int = 250_000
    parquet_compression: str = "snappy"
    qtl_type: str = "eQTL"
    study_label: str = "GTEx_v10"


# Canonical GTEx v10 brain tissue labels (also used in v8 — names stable).
# These match the per-tissue file prefixes inside the eQTL tarball.
BRAIN_TISSUES_V10: frozenset[str] = frozenset({
    "Brain_Amygdala",
    "Brain_Anterior_cingulate_cortex_BA24",
    "Brain_Caudate_basal_ganglia",
    "Brain_Cerebellar_Hemisphere",
    "Brain_Cerebellum",
    "Brain_Cortex",
    "Brain_Frontal_Cortex_BA9",
    "Brain_Hippocampus",
    "Brain_Hypothalamus",
    "Brain_Nucleus_accumbens_basal_ganglia",
    "Brain_Putamen_basal_ganglia",
    "Brain_Spinal_cord_cervical_c-1",
    "Brain_Substantia_nigra",
})


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _normalize_col_name(name: str) -> str:
    s = str(name or "").strip().lower()
    if s.startswith("#"):
        s = s[1:]
    return s


def _find_col_name(frame: pd.DataFrame, aliases: Iterable[str]) -> Optional[str]:
    lookup = {_normalize_col_name(c): c for c in frame.columns}
    for alias in aliases:
        match = lookup.get(_normalize_col_name(alias))
        if match is not None:
            return match
    return None


def _parse_chromosome(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.lower().replace("chromosome", "").replace("chrom", "").replace("chr", "")
    s = s.strip()
    if s == "x":
        return 23
    if s == "y":
        return 24
    if s in {"m", "mt"}:
        return 25
    try:
        chrom = int(s)
    except Exception:
        return None
    if 1 <= chrom <= 25:
        return chrom
    return None


_GTEX_VARIANT_ID_RE = re.compile(
    r"^chr(?P<chrom>[0-9XYMTm]+)_(?P<pos>\d+)_(?P<ref>[ACGTN\-]+)_(?P<alt>[ACGTN\-]+)(?:_b\d+)?$",
    re.IGNORECASE,
)


def _parse_gtex_variant_id(variant_id: Any) -> Optional[tuple[int, int, str, str]]:
    """Parse a GTEx variant_id like 'chr1_12345_A_G_b38' into a tuple.

    Returns (chromosome_int, position_start, ref, alt) or None when the value
    cannot be parsed.
    """
    if variant_id is None:
        return None
    s = str(variant_id).strip()
    if not s:
        return None
    m = _GTEX_VARIANT_ID_RE.match(s)
    if not m:
        return None
    chrom = _parse_chromosome(m.group("chrom"))
    if chrom is None:
        return None
    try:
        pos = int(m.group("pos"))
    except Exception:
        return None
    ref = m.group("ref").upper()
    alt = m.group("alt").upper()
    return chrom, pos, ref, alt


_ENSG_VERSION_RE = re.compile(r"^(ENSG\d+)\.\d+$", re.IGNORECASE)


def _strip_ensg_version(gene_id: Any) -> Optional[str]:
    if gene_id is None:
        return None
    s = str(gene_id).strip()
    if not s:
        return None
    m = _ENSG_VERSION_RE.match(s)
    return m.group(1).upper() if m else s.upper()


def _safe_trim(value: Any, max_len: int) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) > max_len:
        return s[:max_len]
    return s


def _build_evidence_key(
    gene_id: str,
    qtl_type: str,
    bio_context: Optional[str],
) -> str:
    ctx = bio_context or "-"
    return f"{gene_id}:{qtl_type}:{ctx}"


# -----------------------------------------------------------------------------
# DTP
# -----------------------------------------------------------------------------
class DTP(DTPBase):
    def __init__(
        self,
        logger=None,
        debug_mode: bool = False,
        datasource=None,
        package=None,
        session=None,
        db=None,
        config: Optional[GTExEQTLConfig] = None,
    ):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db
        self.config = config or GTExEQTLConfig()

        self.dtp_name = "dtp_variant_eqtl_gtex"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "4.1.0"
        self.compatible_schema_max = "5.0.0"

    # ------------------------------------------------------------------
    # EXTRACT
    # ------------------------------------------------------------------
    def _resolve_local_source(self, source_url: str) -> Optional[Path]:
        if not source_url:
            return None
        if source_url.startswith("file://"):
            return Path(source_url[7:])
        if source_url.startswith("/"):
            return Path(source_url)
        return None

    def extract(self, raw_dir: str):
        self.check_compatibility()

        msg = f"📦 Starting extraction of {self.data_source.name} (GTEx v10 eQTL)..."
        self.logger.log(msg, "INFO")

        source_url = (self.data_source.source_url or "").strip()
        if not source_url:
            msg = "❌ datasource.source_url is empty"
            self.logger.log(msg, "ERROR")
            return False, msg, None

        landing_path = (
            Path(raw_dir)
            / self.data_source.source_system.name
            / self.data_source.name
        )
        landing_path.mkdir(parents=True, exist_ok=True)

        try:
            current_hash = None
            try:
                current_hash = self.get_md5_from_url_file(f"{source_url}.md5")
            except Exception:
                current_hash = None

            local_path = self._resolve_local_source(source_url)
            if local_path is not None:
                if not local_path.exists():
                    msg = f"❌ Local source file not found: {local_path}"
                    self.logger.log(msg, "ERROR")
                    return False, msg, None

                target = landing_path / local_path.name
                if target.resolve() != local_path.resolve():
                    shutil.copy2(local_path, target)

                if current_hash is None:
                    current_hash = compute_file_hash(target)

                self._unpack_brain_tissues(target, landing_path)
                msg = f"✅ Local GTEx tarball staged to {target}"
                self.logger.log(msg, "INFO")
                return True, msg, current_hash

            status, dl_msg = self.http_download(source_url, str(landing_path))
            if not status:
                self.logger.log(dl_msg, "ERROR")
                return False, dl_msg, current_hash

            downloaded = landing_path / Path(source_url).name
            if current_hash is None and downloaded.exists():
                current_hash = compute_file_hash(downloaded)

            self._unpack_brain_tissues(downloaded, landing_path)
            msg = f"✅ {self.data_source.name} downloaded to {landing_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as exc:
            msg = f"❌ ETL extract failed: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    def _unpack_brain_tissues(self, archive: Path, landing_path: Path) -> None:
        """Extract only brain-tissue significant-pairs files from the GTEx tarball.

        Other tissues are skipped to keep disk footprint bounded.
        """
        if not archive.exists():
            return
        if not tarfile.is_tarfile(archive):
            # Single-tissue or pre-extracted layout — nothing to do.
            return

        out_dir = landing_path / "tissues"
        out_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(archive, "r:*") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                base = Path(member.name).name
                if "signif_pairs" not in base.lower():
                    continue
                tissue = self._tissue_from_filename(base)
                if tissue is None or tissue not in BRAIN_TISSUES_V10:
                    continue
                target = out_dir / base
                if target.exists():
                    continue
                with tar.extractfile(member) as src, open(target, "wb") as dst:
                    if src is None:
                        continue
                    shutil.copyfileobj(src, dst)
                self.logger.log(f"  ↳ extracted {base}", "INFO")

    @staticmethod
    def _tissue_from_filename(name: str) -> Optional[str]:
        """Recover the canonical tissue label from a GTEx per-tissue filename.

        Examples accepted:
          Brain_Cortex.v10.signif_pairs.parquet
          Brain_Cortex.v10.signif_pairs.txt.gz
          Brain_Cortex.v10.eQTLs.signif_pairs.parquet
        """
        stem = Path(name).name
        # Strip everything from the first ".v" version separator onwards.
        m = re.match(r"^(?P<tissue>[A-Za-z0-9_\-]+)\.v\d+", stem)
        if not m:
            return None
        return m.group("tissue")

    # ------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------
    def _iter_tissue_files(self, raw_base: Path) -> list[tuple[str, Path]]:
        out: list[tuple[str, Path]] = []
        search_dirs = [raw_base / "tissues", raw_base]
        seen: set[Path] = set()
        for d in search_dirs:
            if not d.exists():
                continue
            patterns = [
                "*.signif_pairs.parquet",
                "*.signif_pairs.txt.gz",
                "*.signif_pairs.tsv.gz",
                "*.signif_pairs.txt",
            ]
            for pattern in patterns:
                for f in sorted(d.glob(pattern)):
                    if f in seen:
                        continue
                    tissue = self._tissue_from_filename(f.name)
                    if tissue is None or tissue not in BRAIN_TISSUES_V10:
                        continue
                    out.append((tissue, f))
                    seen.add(f)
        return out

    def _read_tissue_file(self, path: Path) -> Iterable[pd.DataFrame]:
        suffix = "".join(path.suffixes).lower()
        if suffix.endswith(".parquet"):
            yield pd.read_parquet(path, engine="pyarrow")
            return

        compression: Optional[str] = "gzip" if suffix.endswith(".gz") else None
        reader = pd.read_csv(
            path,
            sep="\t",
            dtype=str,
            compression=compression,
            chunksize=self.config.chunk_size,
            low_memory=False,
        )
        for chunk in reader:
            yield chunk

    def _normalize_chunk(self, df: pd.DataFrame, tissue: str) -> pd.DataFrame:
        cfg = self.config
        out = pd.DataFrame(index=df.index)

        col_variant = _find_col_name(df, ["variant_id"])
        col_gene = _find_col_name(df, ["gene_id", "phenotype_id"])
        col_pval = _find_col_name(df, ["pval_nominal", "pvalue", "p_value"])
        col_slope = _find_col_name(df, ["slope", "beta"])
        col_slope_se = _find_col_name(df, ["slope_se", "se", "beta_se"])
        col_af = _find_col_name(df, ["af", "maf"])
        col_ma_count = _find_col_name(df, ["ma_count"])
        col_ma_samples = _find_col_name(df, ["ma_samples"])
        col_tss = _find_col_name(df, ["tss_distance"])
        col_qbeta = _find_col_name(df, ["pval_beta"])
        col_pval_thresh = _find_col_name(df, ["pval_nominal_threshold"])

        if col_variant is None or col_gene is None:
            raise ValueError(
                "GTEx file is missing required columns: need 'variant_id' and 'gene_id' "
                "(or 'phenotype_id' as fallback)."
            )

        parsed = df[col_variant].map(_parse_gtex_variant_id)
        out["chromosome"] = parsed.map(lambda t: t[0] if t is not None else None)
        out["position_start"] = parsed.map(lambda t: t[1] if t is not None else None)
        out["reference_allele"] = parsed.map(lambda t: t[2] if t is not None else None)
        out["alternate_allele"] = parsed.map(lambda t: t[3] if t is not None else None)
        out["position_end"] = (
            pd.to_numeric(out["position_start"], errors="coerce").astype("Int64")
            + out["reference_allele"].astype("string").str.len()
            - 1
        ).astype("Int64")
        out["chromosome"] = pd.to_numeric(out["chromosome"], errors="coerce").astype("Int64")
        out["position_start"] = pd.to_numeric(out["position_start"], errors="coerce").astype(
            "Int64"
        )

        gene_versioned = (
            df[col_gene].astype("string").str.strip().where(lambda s: s.ne(""), pd.NA)
        )
        out["gene_id_versioned"] = gene_versioned
        out["gene_id"] = gene_versioned.map(_strip_ensg_version)

        out["bio_context"] = tissue
        out["qtl_type"] = cfg.qtl_type
        # `effect_allele` is varchar(64) in the target schema; long indels in
        # `alternate_allele` (allowed up to 256 to keep the JOIN with
        # variant_masters) are nulled out here to avoid string truncation.
        out["effect_allele"] = out["alternate_allele"].where(
            out["alternate_allele"].astype("string").str.len() <= 64, pd.NA
        )

        out["beta"] = pd.to_numeric(
            df[col_slope] if col_slope else pd.Series(None, index=df.index),
            errors="coerce",
        )
        out["se"] = pd.to_numeric(
            df[col_slope_se] if col_slope_se else pd.Series(None, index=df.index),
            errors="coerce",
        )
        out["p_value"] = pd.to_numeric(
            df[col_pval] if col_pval else pd.Series(None, index=df.index),
            errors="coerce",
        )
        # GTEx v10 signif_pairs does not expose total sample size per pair;
        # `ma_samples` is the count of samples carrying the minor allele, not N.
        # We leave `n` NULL and preserve `ma_samples` in `details` for transparency.
        out["n"] = pd.NA

        # Build details JSON with auxiliary fields useful for downstream reports.
        def _row_details(row) -> Optional[str]:
            payload: dict[str, Any] = {"study": cfg.study_label}
            if pd.notna(row.get("gene_id_versioned")):
                payload["gene_id_versioned"] = str(row["gene_id_versioned"])
            for key, src_col in (
                ("af", col_af),
                ("ma_samples", col_ma_samples),
                ("ma_count", col_ma_count),
                ("tss_distance", col_tss),
                ("pval_beta", col_qbeta),
                ("pval_nominal_threshold", col_pval_thresh),
            ):
                if src_col is None:
                    continue
                val = row.get(src_col)
                if pd.notna(val):
                    payload[key] = str(val)
            try:
                return json.dumps(payload, separators=(",", ":"))
            except Exception:
                return None

        details_source = df.copy()
        details_source["gene_id_versioned"] = out["gene_id_versioned"]
        out["details"] = details_source.apply(_row_details, axis=1)

        out["evidence_key"] = [
            _safe_trim(
                _build_evidence_key(
                    gene_id=str(g) if pd.notna(g) else "",
                    qtl_type=cfg.qtl_type,
                    bio_context=tissue,
                ),
                256,
            )
            for g in out["gene_id"]
        ]

        mask = (
            out["chromosome"].notna()
            & out["position_start"].notna()
            & out["reference_allele"].notna()
            & out["alternate_allele"].notna()
            & out["gene_id"].notna()
            & out["evidence_key"].notna()
            & (out["reference_allele"].str.len() <= 64)
            & (out["alternate_allele"].str.len() <= 256)
        )
        out = out.loc[mask].copy()

        # Keep most-significant p-value when the same (variant, gene, tissue)
        # appears more than once (rare but defensive).
        out = out.sort_values(by=["p_value"], ascending=True, na_position="last")
        out = out.drop_duplicates(
            subset=[
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                "evidence_key",
            ],
            keep="first",
        )

        return out[
            [
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                "evidence_key",
                "gene_id",
                "bio_context",
                "qtl_type",
                "beta",
                "se",
                "p_value",
                "n",
                "effect_allele",
                "details",
            ]
        ]

    def transform(self, raw_dir: str, processed_dir: str):
        t0 = time.time()
        msg = f"⚙️ Starting transform of {self.data_source.name} (GTEx v10 eQTL)..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

        try:
            raw_base = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )
            if not raw_base.exists():
                msg = f"❌ Raw dir not found: {raw_base}"
                self.logger.log(msg, "ERROR")
                return False, msg

            tissue_files = self._iter_tissue_files(raw_base)
            if not tissue_files:
                msg = (
                    f"❌ No brain-tissue significant-pairs files found under {raw_base}. "
                    f"Expected one of {sorted(BRAIN_TISSUES_V10)}."
                )
                self.logger.log(msg, "ERROR")
                return False, msg

            out_base = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )
            evid_dir = out_base / "evidence"
            evid_dir.mkdir(parents=True, exist_ok=True)

            for old in evid_dir.glob("evidence_part_*.parquet"):
                old.unlink()

        except Exception as exc:
            msg = f"❌ Error preparing transform paths: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg

        part = 0
        rows_in = 0
        rows_out = 0

        try:
            for tissue, tissue_file in tissue_files:
                self.logger.log(f"  ↳ transforming {tissue} ({tissue_file.name})", "INFO")
                for chunk in self._read_tissue_file(tissue_file):
                    rows_in += len(chunk.index)
                    norm = self._normalize_chunk(chunk, tissue)
                    if norm.empty:
                        continue

                    out_file = evid_dir / f"evidence_part_{part:04d}.parquet"
                    norm.to_parquet(
                        out_file,
                        index=False,
                        compression=self.config.parquet_compression,
                    )
                    rows_out += len(norm.index)
                    part += 1

            # ------------------------------------------------------------------
            # FUTURE: sQTL extension
            # ------------------------------------------------------------------
            # GTEx v10 also ships sQTL significant-pairs files with the same
            # schema (phenotype_id is an intron cluster instead of a gene).
            # To extend, locate "*.sqtl_signif_pairs.*" files, derive gene_id
            # from the cluster -> gene map shipped alongside, and set
            # qtl_type = "sQTL". Evidence_key already includes qtl_type so
            # eQTL and sQTL coexist on the same (variant, gene) pair.
            # ------------------------------------------------------------------

        except Exception as exc:
            msg = f"❌ ETL transform failed: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg

        dt = time.time() - t0
        msg = (
            f"✅ Transform done for {self.data_source.name}: "
            f"tissues={len(tissue_files)} parts={part} "
            f"rows_in={rows_in} rows_out={rows_out} elapsed={dt:.1f}s"
        )
        self.logger.log(msg, "INFO")
        return True, msg

    # ------------------------------------------------------------------
    # LOAD
    # ------------------------------------------------------------------
    def _prepare_load_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        out = df.copy()
        out["chromosome"] = pd.to_numeric(out["chromosome"], errors="coerce").astype("Int64")
        out["position_start"] = pd.to_numeric(out["position_start"], errors="coerce").astype(
            "Int64"
        )
        out["position_end"] = pd.to_numeric(out["position_end"], errors="coerce").astype("Int64")
        for num_col in ("beta", "se", "p_value"):
            out[num_col] = pd.to_numeric(out.get(num_col), errors="coerce")
        out["n"] = pd.to_numeric(out.get("n"), errors="coerce").astype("Int64")

        text_cols = [
            "reference_allele",
            "alternate_allele",
            "evidence_key",
            "gene_id",
            "bio_context",
            "qtl_type",
            "effect_allele",
            "details",
        ]
        for col in text_cols:
            if col not in out.columns:
                out[col] = None
                continue
            out[col] = out[col].astype("string").str.strip()
            out[col] = out[col].where(out[col].ne(""), pd.NA)

        mask = (
            out["chromosome"].notna()
            & out["position_start"].notna()
            & out["position_end"].notna()
            & out["reference_allele"].notna()
            & out["alternate_allele"].notna()
            & out["evidence_key"].notna()
            & out["gene_id"].notna()
            & out["qtl_type"].notna()
        )
        out = out.loc[mask].copy()

        out = out.sort_values(by=["p_value"], ascending=True, na_position="last")
        out = out.drop_duplicates(
            subset=[
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                "evidence_key",
            ],
            keep="first",
        )
        return out

    def _load_part_via_stage(
        self, conn, df: pd.DataFrame, stage_table: str
    ) -> tuple[int, int]:
        if df.empty:
            return 0, 0

        conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
        df.to_sql(
            stage_table,
            con=conn,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=10_000,
        )

        join_sql = f"""
            FROM {stage_table} s
            JOIN variant_masters vm
              ON vm.chromosome = s.chromosome
             AND vm.position_start = s.position_start
             AND vm.position_end = s.position_end
             AND vm.reference_allele = s.reference_allele
             AND vm.alternate_allele = s.alternate_allele
        """

        unmatched_sql = f"""
            SELECT COUNT(*)
            FROM {stage_table} s
            LEFT JOIN variant_masters vm
              ON vm.chromosome = s.chromosome
             AND vm.position_start = s.position_start
             AND vm.position_end = s.position_end
             AND vm.reference_allele = s.reference_allele
             AND vm.alternate_allele = s.alternate_allele
            WHERE vm.variant_id IS NULL
        """

        matched_count = int(conn.execute(text(f"SELECT COUNT(*) {join_sql}")).scalar() or 0)
        unmatched_count = int(conn.execute(text(unmatched_sql)).scalar() or 0)

        dialect = conn.dialect.name
        excluded = "EXCLUDED" if dialect == "postgresql" else "excluded"
        insert_sql = f"""
            INSERT INTO variant_gene_regulatory_evidence (
                chromosome,
                variant_id,
                evidence_key,
                gene_id,
                bio_context,
                qtl_type,
                beta,
                se,
                p_value,
                n,
                effect_allele,
                details,
                data_source_id,
                etl_package_id
            )
            SELECT
                s.chromosome,
                vm.variant_id,
                s.evidence_key,
                s.gene_id,
                s.bio_context,
                s.qtl_type,
                s.beta,
                s.se,
                s.p_value,
                s.n,
                s.effect_allele,
                s.details,
                :data_source_id,
                :etl_package_id
            {join_sql}
            ON CONFLICT (chromosome, variant_id, evidence_key)
            DO UPDATE SET
                gene_id = {excluded}.gene_id,
                bio_context = {excluded}.bio_context,
                qtl_type = {excluded}.qtl_type,
                beta = {excluded}.beta,
                se = {excluded}.se,
                p_value = {excluded}.p_value,
                n = {excluded}.n,
                effect_allele = {excluded}.effect_allele,
                details = {excluded}.details,
                data_source_id = {excluded}.data_source_id,
                etl_package_id = {excluded}.etl_package_id
        """

        conn.execute(
            text(insert_sql),
            {
                "data_source_id": self.data_source.id,
                "etl_package_id": self.package.id,
            },
        )

        conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))
        return matched_count, unmatched_count

    def load(self, processed_dir=None):
        t0 = time.time()
        msg = f"📥 Loading {self.data_source.name} GTEx v10 eQTL evidence..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

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
            evid_dir = base_path / "evidence"
            part_files = sorted(glob.glob(str(evid_dir / "evidence_part_*.parquet")))
            if not part_files:
                msg = f"❌ No GTEx evidence part files found in {evid_dir}"
                self.logger.log(msg, "ERROR")
                return False, msg
        except Exception as exc:
            msg = f"⚠️ Failed to prepare processed data paths: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg

        total_matched = 0
        total_unmatched = 0
        stage_table = "tmp_gtex_eqtl_stage"

        try:
            self.db_write_mode()
        except Exception as exc:
            msg = f"⚠️ Failed to switch DB to write mode: {exc}"
            self.logger.log(msg, "WARNING")
            return False, msg

        try:
            with self.db.engine.begin() as conn:
                conn.execute(
                    text(
                        "DELETE FROM variant_gene_regulatory_evidence "
                        "WHERE data_source_id = :data_source_id"
                    ),
                    {"data_source_id": self.data_source.id},
                )

                for part_file in part_files:
                    df = pd.read_parquet(part_file, engine="pyarrow")
                    df = self._prepare_load_df(df)
                    matched, unmatched = self._load_part_via_stage(conn, df, stage_table)
                    total_matched += matched
                    total_unmatched += unmatched
                    self.logger.log(
                        f"✅ Processed {Path(part_file).name} "
                        f"(matched={matched}, unmatched={unmatched})",
                        "INFO",
                    )

        except Exception as exc:
            msg = f"❌ Load failed: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg

        dt = time.time() - t0
        msg = (
            f"✅ Loaded GTEx v10 eQTL evidence: matched={total_matched}, "
            f"unmatched={total_unmatched}, elapsed={dt:.1f}s"
        )
        self.logger.log(msg, "SUCCESS")
        return True, msg

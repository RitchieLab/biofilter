from __future__ import annotations

import glob
import gzip
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
from sqlalchemy import text

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.utils.file_hash import compute_file_hash


@dataclass
class AlphaMissenseConfig:
    chunk_size: int = 250_000
    parquet_compression: str = "snappy"
    predictor_name: str = "alphamissense"
    predictor_version: Optional[str] = None


def _normalize_col_name(name: str) -> str:
    s = str(name or "").strip().lower()
    if s.startswith("#"):
        s = s[1:]
    return s


def _find_col_name(
    frame: pd.DataFrame,
    aliases: Iterable[str],
) -> Optional[str]:
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
    if not s:
        return None

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


def _infer_chromosome_from_text(text: str) -> Optional[int]:
    m = re.search(
        r"(?:chr|chromosome[_-]?)(\d+|x|y|m|mt)\b",
        str(text or ""),
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    return _parse_chromosome(m.group(1))


def _build_predictor_key(
    predictor_name: str,
    predictor_version: Optional[str],
    transcript_id: Optional[str],
) -> str:
    version = predictor_version or "na"
    transcript = transcript_id or "-"
    return f"{predictor_name}:{version}:{transcript}"


def _safe_trim(value: Any, max_len: int) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) > max_len:
        return s[:max_len]
    return s


class DTP(DTPBase):
    def __init__(
        self,
        logger=None,
        debug_mode: bool = False,
        datasource=None,
        package=None,
        session=None,
        db=None,
        config: Optional[AlphaMissenseConfig] = None,
    ):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db
        self.config = config or AlphaMissenseConfig()

        self.dtp_name = "dtp_variant_alphamissense"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "4.0.0"

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

        msg = f"📦 Starting extraction of {self.data_source.name} (AlphaMissense)..."
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

                msg = f"✅ Local AlphaMissense file staged to {target}"
                self.logger.log(msg, "INFO")
                return True, msg, current_hash

            status, dl_msg = self.http_download(source_url, str(landing_path))
            if not status:
                self.logger.log(dl_msg, "ERROR")
                return False, dl_msg, current_hash

            downloaded = landing_path / Path(source_url).name
            if current_hash is None and downloaded.exists():
                current_hash = compute_file_hash(downloaded)

            msg = f"✅ {self.data_source.name} downloaded to {landing_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as exc:
            msg = f"❌ ETL extract failed: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # ------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------
    def _locate_raw_file(self, raw_base: Path) -> Optional[Path]:
        patterns = [
            "*.tsv.gz",
            "*.tsv.bgz",
            "*.bgz",
            "*.gz",
            "*.tsv",
            "*.txt",
        ]
        for pattern in patterns:
            files = sorted(raw_base.glob(pattern))
            if files:
                return files[0]
        return None

    def _detect_header_skiprows(self, input_file: Path, compression: str) -> int:
        """
        Detect how many initial lines must be skipped until the TSV header.

        AlphaMissense files include license/comment lines before the header:
        e.g. '# Copyright ...', '# Licensed ...', '#CHROM ...'
        """
        open_fn = gzip.open if compression == "gzip" else open
        with open_fn(input_file, "rt", encoding="utf-8", errors="replace") as handle:
            for line_idx, line in enumerate(handle):
                head = (line or "").strip()
                if not head:
                    continue
                if head.startswith("#CHROM\t") or head.startswith("CHROM\t"):
                    return line_idx
                # Safeguard for malformed files
                if line_idx > 5000:
                    break
        raise ValueError(
            f"Could not detect TSV header (#CHROM) in AlphaMissense file: {input_file}"
        )

    def _normalize_chunk(
        self,
        df: pd.DataFrame,
        fallback_chromosome: Optional[int],
    ) -> pd.DataFrame:
        cfg = self.config
        out = pd.DataFrame(index=df.index)

        col_chrom = _find_col_name(df, ["chromosome", "chrom", "chr", "#chrom"])
        col_pos = _find_col_name(df, ["position_start", "position", "pos", "bp"])
        col_end = _find_col_name(df, ["position_end", "end", "stop"])
        col_ref = _find_col_name(df, ["reference_allele", "ref", "reference"])
        col_alt = _find_col_name(df, ["alternate_allele", "alt", "alternate"])
        col_tx = _find_col_name(df, ["transcript_id", "transcript", "feature", "enst"])
        col_score = _find_col_name(
            df,
            [
                "am_pathogenicity",
                "pathogenicity",
                "alphamissense_score",
                "score",
            ],
        )
        col_class = _find_col_name(
            df,
            [
                "am_class",
                "classification",
                "alphamissense_class",
                "class",
            ],
        )
        col_version = _find_col_name(df, ["predictor_version", "version", "model_version"])
        col_protein_variant = _find_col_name(
            df,
            ["protein_variant", "protein_change", "hgvsp"],
        )

        if col_pos is None or col_ref is None or col_alt is None:
            raise ValueError(
                "AlphaMissense file is missing required columns. "
                "Need position/ref/alt columns."
            )

        if col_chrom is not None:
            out["chromosome"] = df[col_chrom].map(_parse_chromosome)
        else:
            out["chromosome"] = fallback_chromosome

        out["position_start"] = pd.to_numeric(df[col_pos], errors="coerce").astype("Int64")
        out["reference_allele"] = (
            df[col_ref]
            .astype("string")
            .str.strip()
            .str.upper()
            .where(lambda s: s.ne(""), pd.NA)
        )
        out["alternate_allele"] = (
            df[col_alt]
            .astype("string")
            .str.strip()
            .str.upper()
            .where(lambda s: s.ne(""), pd.NA)
        )

        if col_end is not None:
            out["position_end"] = pd.to_numeric(df[col_end], errors="coerce").astype("Int64")
        else:
            out["position_end"] = (
                out["position_start"] + out["reference_allele"].str.len() - 1
            ).astype("Int64")

        if col_tx is not None:
            tx = (
                df[col_tx]
                .astype("string")
                .str.strip()
                .where(lambda s: s.ne(""), pd.NA)
            )
            out["transcript_id"] = tx.str.slice(0, 32)
        else:
            out["transcript_id"] = None

        out["score"] = pd.to_numeric(
            df[col_score] if col_score is not None else pd.Series(None, index=df.index),
            errors="coerce",
        )
        out["classification"] = (
            df[col_class]
            .astype("string")
            .str.strip()
            .where(lambda s: s.ne(""), pd.NA)
            .str.slice(0, 64)
            if col_class is not None
            else None
        )

        version_series = None
        if col_version is not None:
            version_series = (
                df[col_version]
                .astype("string")
                .str.strip()
                .where(lambda s: s.ne(""), pd.NA)
                .str.slice(0, 32)
            )
        else:
            version_series = pd.Series(cfg.predictor_version, index=df.index, dtype="string")

        out["predictor_name"] = cfg.predictor_name
        out["predictor_version"] = version_series

        if col_protein_variant is not None:
            protein_variant = (
                df[col_protein_variant]
                .astype("string")
                .str.strip()
                .where(lambda s: s.ne(""), pd.NA)
            )
        else:
            protein_variant = pd.Series(pd.NA, index=df.index, dtype="string")

        out["details"] = protein_variant.apply(
            lambda x: f'{{"protein_variant":"{x}"}}' if pd.notna(x) else None
        )

        out["predictor_key"] = [
            _build_predictor_key(
                predictor_name=cfg.predictor_name,
                predictor_version=(
                    None
                    if pd.isna(ver)
                    else _safe_trim(ver, 32)
                ),
                transcript_id=(
                    None
                    if pd.isna(tx)
                    else _safe_trim(tx, 32)
                ),
            )
            for ver, tx in zip(out["predictor_version"], out["transcript_id"])
        ]
        out["predictor_key"] = out["predictor_key"].map(lambda x: _safe_trim(x, 128))

        mask = (
            out["chromosome"].notna()
            & out["position_start"].notna()
            & out["position_end"].notna()
            & out["reference_allele"].notna()
            & out["alternate_allele"].notna()
            & (out["reference_allele"].str.len() <= 64)
            & (out["alternate_allele"].str.len() <= 256)
            & out["predictor_key"].notna()
            & (out["predictor_key"].str.len() <= 128)
        )

        out = out.loc[mask].copy()
        out["chromosome"] = pd.to_numeric(out["chromosome"], errors="coerce").astype("Int64")

        out = out.sort_values(by=["score"], ascending=False, na_position="last")
        out = out.drop_duplicates(
            subset=[
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                "predictor_key",
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
                "predictor_key",
                "transcript_id",
                "predictor_name",
                "predictor_version",
                "score",
                "classification",
                "details",
            ]
        ]

    def transform(self, raw_dir: str, processed_dir: str):
        t0 = time.time()
        msg = f"⚙️ Starting transform of {self.data_source.name} (AlphaMissense)..."
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

            input_file = self._locate_raw_file(raw_base)
            if input_file is None:
                msg = f"❌ No AlphaMissense input file found in {raw_base}"
                self.logger.log(msg, "ERROR")
                return False, msg

            out_base = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )
            pred_dir = out_base / "predictions"
            pred_dir.mkdir(parents=True, exist_ok=True)

            for old in pred_dir.glob("predictions_part_*.parquet"):
                old.unlink()

        except Exception as exc:
            msg = f"❌ Error preparing transform paths: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg

        fallback_chrom = _infer_chromosome_from_text(self.data_source.name)
        if fallback_chrom is None:
            fallback_chrom = _infer_chromosome_from_text(input_file.name)

        compression = "gzip" if input_file.suffix in {".gz", ".bgz"} else "infer"
        part = 0
        rows_in = 0
        rows_out = 0

        try:
            skiprows = self._detect_header_skiprows(input_file, compression)
            reader = pd.read_csv(
                input_file,
                sep="\t",
                dtype=str,
                compression=compression,
                skiprows=skiprows,
                chunksize=self.config.chunk_size,
                low_memory=False,
            )

            for chunk in reader:
                rows_in += len(chunk.index)
                norm = self._normalize_chunk(chunk, fallback_chrom)
                if norm.empty:
                    continue

                out_file = pred_dir / f"predictions_part_{part:04d}.parquet"
                norm.to_parquet(
                    out_file,
                    index=False,
                    compression=self.config.parquet_compression,
                )
                rows_out += len(norm.index)
                part += 1

        except Exception as exc:
            msg = f"❌ ETL transform failed: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg

        dt = time.time() - t0
        msg = (
            f"✅ Transform done for {self.data_source.name}: "
            f"parts={part} rows_in={rows_in} rows_out={rows_out} elapsed={dt:.1f}s"
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
        out["score"] = pd.to_numeric(out.get("score"), errors="coerce")

        text_cols = [
            "reference_allele",
            "alternate_allele",
            "predictor_key",
            "transcript_id",
            "predictor_name",
            "predictor_version",
            "classification",
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
            & out["predictor_key"].notna()
        )
        out = out.loc[mask].copy()

        out = out.sort_values(by=["score"], ascending=False, na_position="last")
        out = out.drop_duplicates(
            subset=[
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                "predictor_key",
            ],
            keep="first",
        )
        return out

    def _load_part_via_stage(self, conn, df: pd.DataFrame, stage_table: str) -> tuple[int, int]:
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
        if dialect == "postgresql":
            insert_sql = f"""
                INSERT INTO variant_effect_predictions (
                    chromosome,
                    variant_id,
                    predictor_key,
                    transcript_id,
                    predictor_name,
                    predictor_version,
                    score,
                    classification,
                    details,
                    data_source_id,
                    etl_package_id
                )
                SELECT
                    s.chromosome,
                    vm.variant_id,
                    s.predictor_key,
                    s.transcript_id,
                    s.predictor_name,
                    s.predictor_version,
                    s.score,
                    s.classification,
                    s.details,
                    :data_source_id,
                    :etl_package_id
                {join_sql}
                ON CONFLICT (chromosome, variant_id, predictor_key)
                DO UPDATE SET
                    transcript_id = EXCLUDED.transcript_id,
                    predictor_name = EXCLUDED.predictor_name,
                    predictor_version = EXCLUDED.predictor_version,
                    score = EXCLUDED.score,
                    classification = EXCLUDED.classification,
                    details = EXCLUDED.details,
                    data_source_id = EXCLUDED.data_source_id,
                    etl_package_id = EXCLUDED.etl_package_id
            """
        else:
            insert_sql = f"""
                INSERT INTO variant_effect_predictions (
                    chromosome,
                    variant_id,
                    predictor_key,
                    transcript_id,
                    predictor_name,
                    predictor_version,
                    score,
                    classification,
                    details,
                    data_source_id,
                    etl_package_id
                )
                SELECT
                    s.chromosome,
                    vm.variant_id,
                    s.predictor_key,
                    s.transcript_id,
                    s.predictor_name,
                    s.predictor_version,
                    s.score,
                    s.classification,
                    s.details,
                    :data_source_id,
                    :etl_package_id
                {join_sql}
                ON CONFLICT (chromosome, variant_id, predictor_key)
                DO UPDATE SET
                    transcript_id = excluded.transcript_id,
                    predictor_name = excluded.predictor_name,
                    predictor_version = excluded.predictor_version,
                    score = excluded.score,
                    classification = excluded.classification,
                    details = excluded.details,
                    data_source_id = excluded.data_source_id,
                    etl_package_id = excluded.etl_package_id
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
        msg = f"📥 Loading {self.data_source.name} AlphaMissense predictions..."
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
            pred_dir = base_path / "predictions"
            part_files = sorted(glob.glob(str(pred_dir / "predictions_part_*.parquet")))
            if not part_files:
                msg = f"❌ No AlphaMissense part files found in {pred_dir}"
                self.logger.log(msg, "ERROR")
                return False, msg
        except Exception as exc:
            msg = f"⚠️ Failed to prepare processed data paths: {exc}"
            self.logger.log(msg, "ERROR")
            return False, msg

        total_matched = 0
        total_unmatched = 0
        stage_table = "tmp_alphamissense_stage"

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
                        "DELETE FROM variant_effect_predictions "
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
            f"✅ Loaded AlphaMissense predictions: matched={total_matched}, "
            f"unmatched={total_unmatched}, elapsed={dt:.1f}s"
        )
        self.logger.log(msg, "SUCCESS")
        return True, msg

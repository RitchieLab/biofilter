import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from requests.exceptions import RequestException
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models import (
    EntityGroup,
    EntityRelationshipType,
    EntityRelationship,
    EntityAlias,
)  # noqa E501
from biofilter.etl.mixins.base_dtp import DTPBase


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.use_conflict_csv = use_conflict_csv

        # DTP versioning
        self.dtp_name = "dtp_clingen"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Extract phase for the ClinGen DTP.

        - Downloads selected public flat files/exports from ClinGen.
        - Normalizes date-suffixed filenames (e.g., '...-YYYY-MM-DD.csv' → fixed name).
        - Computes a content hash on the canonical Gene-Disease Summary file.
        - Returns (success: bool, message: str, current_hash: Optional[str])
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(msg, "INFO")

        try:
            # --- Compatibility check ---
            self.check_compatibility()

            HEADERS = {
                "User-Agent": "Biofilter3R-ETL/1.0 (+https://your.domain)",
                "Accept": "text/csv, text/plain, application/json;q=0.9, */*;q=0.8",
            }

            # 1) Endpoint → canonical filename mapping
            CANONICAL = {
                "reports/curation-activity-summary-report": "ClinGen-Curation-Activity-Summary.csv",
                "gene-validity/download": "ClinGen-Gene-Disease-Summary.csv",
                "gene-dosage/download": "ClinGen-Gene-Dosage.csv",
            }

            def _infer_extension_from_headers(headers: dict) -> str:
                ctype = headers.get("Content-Type", "").lower()
                if "text/csv" in ctype: return ".csv"
                if "tsv" in ctype: return ".tsv"
                if "json" in ctype: return ".json"
                if "text/plain" in ctype: return ".txt"
                return ""

            def _filename_from_content_disposition(headers: dict) -> str | None:
                cd = headers.get("Content-Disposition", "")
                m = re.search(r'filename\s*=\s*"([^"]+)"', cd)
                return m.group(1) if m else None

            def _safe_download(url: str, out_dir: str, max_retries: int = 3, backoff_sec: float = 1.5) -> str:
                last_exc = None
                for attempt in range(1, max_retries + 1):
                    try:
                        r = requests.get(url, stream=True, timeout=90, headers=HEADERS, allow_redirects=True)
                        r.raise_for_status()

                        # Prefer server-provided filename; fallback = last path segment + inferred ext
                        fname = _filename_from_content_disposition(r.headers)
                        if not fname:
                            fname = url.rstrip("/").split("/")[-1] or "download"
                            if "." not in fname:
                                fname += _infer_extension_from_headers(r.headers) or ""

                        out_path = os.path.join(out_dir, fname)
                        with open(out_path, "wb") as handle:
                            for chunk in r.iter_content(chunk_size=1024 * 1024):
                                if chunk:
                                    handle.write(chunk)
                        return out_path
                    except RequestException as e:
                        last_exc = e
                        time.sleep(backoff_sec * attempt)
                raise last_exc if last_exc else RuntimeError("Unknown download error")

            def _maybe_follow_url_file(path: str, out_dir: str) -> str:
                """
                If 'path' is a small text file holding a single URL, fetch that URL and overwrite 'path' with the real content.
                Returns final path.
                """
                try:
                    if os.path.getsize(path) > 1_000_000:  # >1MB => not a tiny URL stub
                        return path
                    with open(path, "rb") as fh:
                        blob = fh.read(4096)  # small peek
                    try:
                        text = blob.decode("utf-8", errors="ignore").strip()
                    except Exception:
                        return path
                    # Single-line URL?
                    if re.fullmatch(r"https?://\S+", text):
                        # Download the real file to a temp, then replace
                        real_tmp = _safe_download(text, out_dir)
                        os.replace(real_tmp, path)
                        return path
                    return path
                except Exception:
                    return path

            def _looks_like_delimited_csv(first_bytes: bytes) -> tuple[bool, str]:
                """
                Heuristic: check for commas or tabs in first lines to guess CSV/TSV.
                Returns (is_table, suggested_ext)
                """
                try:
                    text = first_bytes.decode("utf-8", errors="ignore")
                except Exception:
                    return (False, "")
                head = "\n".join(text.splitlines()[:3])
                if "," in head and "\n" in head:
                    return (True, ".csv")
                if "\t" in head and "\n" in head:
                    return (True, ".tsv")
                return (False, "")

            def _normalize_to_canonical(path: str, canonical_name: str) -> str:
                """
                Rename any downloaded file to the canonical name.
                - Strips date suffixes like '-YYYY-MM-DD.csv'
                - If extension is .txt but content is CSV/TSV, fix extension before renaming
                """
                dirname, fname = os.path.split(path)
                with open(path, "rb") as fh:
                    first = fh.read(8192)

                # If it looks like CSV/TSV but extension is .txt or no ext, fix ext
                base, ext = os.path.splitext(fname)
                is_table, suggest = _looks_like_delimited_csv(first)
                if is_table and (ext.lower() in ("", ".txt")) and suggest:
                    new_path_guess = os.path.join(dirname, base + suggest)
                    try:
                        os.replace(path, new_path_guess)
                        path = new_path_guess
                        fname = os.path.basename(path)
                        ext = suggest
                    except Exception:
                        pass

                # Strip date suffix if present (keep original extension)
                if re.search(r"-\d{4}-\d{2}-\d{2}(\.[A-Za-z0-9]+)?$", fname):
                    _, ext2 = os.path.splitext(fname)
                    path_final = os.path.join(dirname, os.path.splitext(canonical_name)[0] + (ext2 or ext))
                else:
                    path_final = os.path.join(dirname, canonical_name)

                if os.path.abspath(path_final) != os.path.abspath(path):
                    try:
                        os.replace(path, path_final)
                        return path_final
                    except Exception:
                        return path
                return path

            # ---- In your loop over endpoints, use the mapping + fixups ----
            base_url = "https://search.clinicalgenome.org/kb/"
            landing_path = os.path.join(raw_dir, self.data_source.source_system.name, self.data_source.name)
            os.makedirs(landing_path, exist_ok=True)

            downloaded_files = []
            for ep, canonical_name in CANONICAL.items():
                url = base_url + ep
                try:
                    tmp_path = _safe_download(url, landing_path)
                    # If we got a tiny text file with a presigned URL, follow it:
                    tmp_path = _maybe_follow_url_file(tmp_path, landing_path)
                    # Normalize/rename to canonical final name:
                    final_path = _normalize_to_canonical(tmp_path, canonical_name)
                    downloaded_files.append(final_path)
                    self.logger.log(f"✅ Fetched {ep} → {os.path.basename(final_path)}", "INFO")
                except Exception as e:
                    msg = f"❌ Failed to download '{ep}': {e}"
                    self.logger.log(msg, "ERROR")
                    return False, msg, None

            # Pick the canonical gene-validity file for hashing:
            summary_path = os.path.join(landing_path, CANONICAL["gene-validity/download"])
            if not os.path.exists(summary_path):
                msg = "❌ Expected ClinGen Gene-Disease Summary file was not found after normalization."
                self.logger.log(msg, "ERROR")
                return False, msg, None

            current_hash = compute_file_hash(summary_path)
            msg = f"✅ ClinGen files downloaded and normalized to {landing_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None


    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):
        """
        Transforms ClinGen flat files into normalized Parquet outputs for downstream load.

        Inputs (expected at raw_dir/<SourceSystem>/<DataSource>/):
        - ClinGen-Gene-Disease-Summary.csv
        - ClinGen-Curation-Activity-Summary.csv    (optional enrichment)
        - ClinGen-Gene-Dosage.csv                  (optional enrichment)

        Outputs (written to processed_dir/<SourceSystem>/<DataSource>/):
        - gene_disease_validity.parquet
        - curation_activity_summary.parquet        (if available)
        - gene_dosage.parquet                      (if available)
        """

        msg = f"🔧 Transforming the {self.data_source.name} data ..."
        self.logger.log(msg, "INFO")  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        # Check if raw_dir and processed_dir are provided
        try:
            # Define input/output base paths
            input_path = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            output_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501

            # Ensure output directory exists
            output_path.mkdir(parents=True, exist_ok=True)

            # Input file path
            # List downloaded files for verification
            if not input_path.exists():
                msg = f"❌ Input directory not found: {input_path}"
                self.logger.log(msg, "ERROR")
                return False, msg

            self.logger.log("📂 Downloaded files:", "INFO")
            for p in sorted(input_path.glob("*")):
                if p.is_file():
                    self.logger.log(f"   - {p.name} ({p.stat().st_size} bytes)", "INFO")

            # Canonical expected filenames (as produced by the extract step)
            F_GDV = input_path / "ClinGen-Gene-Disease-Summary.csv"
            F_SUM = input_path / "ClinGen-Curation-Activity-Summary.csv"
            F_DOS = input_path / "ClinGen-Gene-Dosage.csv"

            # Output files (without extension; we write .parquet and, if debug, .csv)
            OUT_GDV = output_path / "gene_disease_validity"
            OUT_SUM = output_path / "curation_activity_summary"
            OUT_DOS = output_path / "gene_dosage"

            # Remove prior outputs (both .csv and .parquet)
            for f in (OUT_GDV, OUT_SUM, OUT_DOS):
                for ext in (".csv", ".parquet"):
                    target = f.with_suffix(ext)
                    if target.exists():
                        target.unlink()
                        self.logger.log(f"🗑️  Removed existing file: {target}", "INFO")

        except Exception as e:
            msg = f"❌ Error preparing paths: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # --- Helpers ---
        CLASS_ORDER = {
            "Definitive": 6,
            "Strong": 5,
            "Moderate": 4,
            "Limited": 3,
            "Disputed": 2,
            "Refuted": 1,
            "No Known Disease Relationship": 0,
            "No Known Disease": 0,
        }

        def _parse_isoish_date(x: str) -> str | None:
            x = (x or "").strip()
            if not x:
                return None
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d", "%m/%d/%Y"):
                try:
                    return datetime.strptime(x, fmt).date().isoformat()
                except Exception:
                    continue
            return None

        def _write_output(df: pd.DataFrame, base_path: Path, ok_msg: str):
            df.to_parquet(base_path.with_suffix(".parquet"), index=False)
            if getattr(self, "debug_mode", False):
                df.to_csv(base_path.with_suffix(".csv"), index=False)
            self.logger.log(ok_msg, "INFO")

        # --- Loaders for the two header styles you showed ---

        def load_gene_disease_validity_curations(csv_path: Path) -> pd.DataFrame:
            """
            Handles the 'CLINGEN GENE DISEASE VALIDITY CURATIONS' file which contains decorative
            rows (+ signs) and the real header somewhere below.
            """
            import pandas as pd

            # Read raw with no header first
            raw = pd.read_csv(csv_path, header=None, dtype=str, keep_default_na=False)
            # Identify the line where the true header appears (contains 'GENE SYMBOL')
            header_row_idx = raw.index[raw.apply(lambda r: (r == "GENE SYMBOL").any(), axis=1)]
            if len(header_row_idx) == 0:
                raise ValueError("Header row with 'GENE SYMBOL' not found in validity curations file.")
            header_idx = int(header_row_idx[0])
            header = raw.iloc[header_idx].tolist()

            # Re-read using the discovered header line
            df = pd.read_csv(csv_path, skiprows=header_idx + 1, names=header, dtype=str, keep_default_na=False)

            # Select and rename columns
            colmap = {
                "GENE SYMBOL": "gene_symbol",
                "GENE ID (HGNC)": "hgnc_id",
                "DISEASE LABEL": "disease_label",
                "DISEASE ID (MONDO)": "mondo_id",
                "MOI": "moi",
                "SOP": "sop",
                "CLASSIFICATION": "classification",
                "ONLINE REPORT": "report_url",
                "CLASSIFICATION DATE": "assertion_date",
                "GCEP": "gcep",
            }
            missing = [c for c in colmap.keys() if c not in df.columns]
            if missing:
                raise ValueError(f"Missing expected columns in validity curations: {missing}")

            df = df[list(colmap.keys())].rename(columns=colmap)

            # Normalize identifiers
            df["hgnc_id"] = df["hgnc_id"].str.strip()
            df["mondo_id"] = df["mondo_id"].str.strip()
            df = df[df["hgnc_id"].str.startswith("HGNC:") & df["mondo_id"].str.startswith("MONDO:")]

            # Normalize MOI / SOP
            df["moi"] = df["moi"].str.strip()
            df["sop_version"] = df["sop"].str.extract(r"(\d+)").fillna("")
            df.drop(columns=["sop"], inplace=True)

            # Dates and class rank
            df["assertion_date"] = df["assertion_date"].apply(_parse_isoish_date)
            df["class_rank"] = df["classification"].map(CLASS_ORDER).fillna(-1)

            # Deduplicate by (HGNC, MONDO) keeping the most recent, then strongest class
            df.sort_values(
                by=["hgnc_id", "mondo_id", "assertion_date", "class_rank"],
                ascending=[True, True, False, False],
                inplace=True,
            )
            df = df.drop_duplicates(subset=["hgnc_id", "mondo_id"], keep="first")

            # Final shape
            return df[
                [
                    "hgnc_id",
                    "gene_symbol",
                    "mondo_id",
                    "disease_label",
                    "moi",
                    "sop_version",
                    "classification",
                    "assertion_date",
                    "gcep",
                    "report_url",
                ]
            ]

        def load_curation_activity_summary(csv_path: Path) -> pd.DataFrame:
            """
            Handles 'ClinGen Curation Activity Summary Report' which starts with README lines
            and then presents a lowercase header (gene_symbol, hgnc_id, ...).
            """
            import pandas as pd

            raw = pd.read_csv(csv_path, header=None, dtype=str, keep_default_na=False)
            header_row_idx = raw.index[raw.apply(lambda r: (r == "gene_symbol").any(), axis=1)]
            if len(header_row_idx) == 0:
                raise ValueError("Header row with 'gene_symbol' not found in summary report.")
            header_idx = int(header_row_idx[0])
            header = raw.iloc[header_idx].tolist()

            df = pd.read_csv(csv_path, skiprows=header_idx + 1, names=header, dtype=str, keep_default_na=False)

            # Keep a curated subset that mirrors validity relationships, plus extra fields
            cols = [
                "gene_symbol",
                "hgnc_id",
                "disease_label",
                "mondo_id",
                "mode_of_inheritance",
                "gene_disease_validity_assertion_classifications",
                "gene_disease_validity_assertion_reports",
                "gene_disease_validity_gceps",
            ]
            missing = [c for c in cols if c not in df.columns]
            if missing:
                raise ValueError(f"Missing expected columns in summary report: {missing}")

            df = df[cols].copy()
            # Normalize identifiers
            df["hgnc_id"] = df["hgnc_id"].str.strip()
            df["mondo_id"] = df["mondo_id"].str.strip()
            df = df[df["hgnc_id"].str.startswith("HGNC:") & df["mondo_id"].str.startswith("MONDO:")]

            # Optional: explode compound fields separated by '|'
            # For a flat summary parquet, we keep as-is (pipe-separated strings)
            return df

        # --- Transform pipeline ---
        try:
            total_outputs = 0

            # 1) Gene–Disease Validity (preferred input)
            if F_GDV.exists():
                df_validity = load_gene_disease_validity_curations(F_GDV)
                _write_output(df_validity, OUT_GDV, f"✅ Wrote gene_disease_validity to {OUT_GDV.with_suffix('.parquet')}")
                total_outputs += 1
            else:
                self.logger.log(f"⚠️  Missing file: {F_GDV.name} (validity curations). Skipping.", "WARNING")

            # 2) Curation Activity Summary (optional enrichment)
            if F_SUM.exists():
                df_summary = load_curation_activity_summary(F_SUM)
                _write_output(df_summary, OUT_SUM, f"✅ Wrote curation_activity_summary to {OUT_SUM.with_suffix('.parquet')}")
                total_outputs += 1
            else:
                self.logger.log(f"ℹ️  Optional file not found: {F_SUM.name}.", "INFO")

            # 3) Gene Dosage (optional; passthrough -> parquet)
            if F_DOS.exists():
                # Many times dosage CSV already has a clean header on the first row.
                # If needed, add a header-detection routine similar to the ones above.
                df_dos = pd.read_csv(F_DOS, dtype=str, keep_default_na=False)
                _write_output(df_dos, OUT_DOS, f"✅ Wrote gene_dosage to {OUT_DOS.with_suffix('.parquet')}")
                total_outputs += 1
            else:
                self.logger.log(f"ℹ️  Optional file not found: {F_DOS.name}.", "INFO")

            if total_outputs == 0:
                msg = "❌ No outputs were produced. Check input files and extract step."
                self.logger.log(msg, "ERROR")
                return False, msg

            msg = f"✅ Transform completed. Outputs: {total_outputs} parquet file(s)."
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg


    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        """
        Load ClinGen Gene→Disease validity edges from gene_disease_validity.parquet.

        Expected columns:
        - hgnc_id, gene_symbol, mondo_id, disease_label
        - moi, sop_version, classification, assertion_date, gcep, report_url
        """
        

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",  # noqa E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_relationships = 0
        total_warnings = 0

        # READ PROCESSED DATA TO LOAD
        try:
            # Check if processed dir was set
            if not processed_dir:
                msg = "⚠️  processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR

            processed_path = os.path.join(
                processed_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            # processed_file_name = processed_path + "/master_data.parquet"
            processed_file_name = processed_path + "/gene_disease_validity.parquet"  # NOTE: We can change it!

            if not os.path.exists(processed_file_name):
                msg = f"⚠️  File not found: {processed_file_name}"
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR

            df = pd.read_parquet(processed_file_name, engine="pyarrow")

            if df.empty:
                msg = "DataFrame is empty."
                self.logger.log(msg, "ERROR")
                return False, msg

            df.fillna("", inplace=True)

        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # Set DB and drop indexes
        try:
            # self.db_write_mode()
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # GET ENTITY GROUP ID AND OMICS STATUS
        # --- Prepare maps ---
        group_map = {
            g.name.lower(): g.id
            for g in self.session.query(EntityGroup).all()
        }
        gene_group_id = group_map.get("genes")
        disease_group_id = group_map.get("diseases")

        rel_type_map = {
            rt.code.lower(): rt.id
            for rt in self.session.query(EntityRelationshipType).all()
        }

        rel_type_id = rel_type_map.get("part_of") # TODO: Mudar aqui

        # --- Clean old relationships ---
        deleted = (
            self.session.query(EntityRelationship)
            .filter_by(data_source_id=self.data_source.id)
            .delete(synchronize_session=False)
        )
        self.logger.log(f"🧹 Deleted {deleted} old ClinGen relationships", "INFO")
        self.session.commit()

        not_loaded = []

        # RUN LOAD BY ROW
        for _, row in df.iterrows():
            try:
                gene_id = str(row["hgnc_id"])
                disease_id = str(row["mondo_id"])

                # Find Gene entity
                gene_entity = (
                    self.session.query(EntityAlias)
                    .filter(EntityAlias.alias_value == gene_id)
                    .filter(EntityAlias.is_active.is_(True))
                    .filter(EntityAlias.group_id == gene_group_id)
                    .first()
                )

                # Find Disease entity
                disease_entity = (
                    self.session.query(EntityAlias)
                    .filter(EntityAlias.alias_value == disease_id)
                    .filter(EntityAlias.is_active.is_(True))
                    .filter(EntityAlias.group_id == disease_group_id)
                    .first()
                )

                if not gene_entity or not disease_entity:
                    not_loaded.append(row)
                    continue

                rel = EntityRelationship(
                    entity_1_id=gene_entity.entity_id,
                    entity_2_id=disease_entity.entity_id,
                    entity_1_group_id=gene_group_id,
                    entity_2_group_id=disease_group_id,
                    relationship_type_id=rel_type_id,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(rel)
                total_relationships += 1

            except Exception as e:
                not_loaded.append(row)
                self.logger.log(f"⚠️ Failed row: {e}", "WARNING")

        # Commit batch
        try:
            self.session.commit()
            msg = f"✅ {total_relationships} ClinGen relationships loaded successfully"
            self.logger.log(msg, "INFO")
        except Exception as e:
            self.session.rollback()
            msg = f"❌ Error committing relationships: {e}"
            self.logger.log(msg, "ERROR")

        # Save not loaded if exists
        if not_loaded:
            df_not_loaded = pd.DataFrame(not_loaded)
            not_loaded_path = processed_path + "/relationship_data_not_loaded.csv"
            df_not_loaded.to_csv(not_loaded_path, index=False)
            self.logger.log(
                f"⚠️ {len(df_not_loaded)} ClinGen relationships skipped. See {not_loaded_path}",
                "WARNING",
            )

        # Restore DB to read mode
        try:
            self.create_indexes(self.get_entity_index_specs)
            # self.db_read_mode()
        except Exception as e:
            self.logger.log(f"⚠️ Failed to restore DB indexes: {e}", "WARNING")

        return True, f"📥 Total ClinGen Relationships: {total_relationships}"
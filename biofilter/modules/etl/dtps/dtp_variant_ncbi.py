import bz2
import glob
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import insert as generic_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection

from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.etl.mixins.entity_query_mixin import EntityQueryMixin


def _map_seq_id_to_chrom(seq_id: str) -> int | None:
    """
    Map RefSeq chromosome accessions to our integer chromosome encoding.

    - NC_000001.* -> 1
    - ...
    - NC_000022.* -> 22
    - NC_000023.* -> 23 (X)
    - NC_000024.* -> 24 (Y)
    - NC_012920.* -> 25 (MT, human mitochondrial)
    - Anything else -> None (we skip non-primary chromosomes/contigs here)
    """
    if not seq_id:
        return None

    s = seq_id.strip().upper()
    # Chromosomes 1..24 (X=23, Y=24)
    m = re.match(r"^NC_0*([0-9]{1,2})\.", s)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 22:
            return n
        if n == 23:
            return 23  # X
        if n == 24:
            return 24  # Y
        # ignorer other NC_00xxx.* that is not chromosomes
        return None

    # Mitochondrial sequence (human)
    # RefSeq canonical: NC_012920.1
    if s.startswith("NC_012920"):
        return 25  # MT
    # Alt contigs, scaffolds, etc. (NT_, NW_, etc.) -> will not use here
    return None


def _extract_merge_log(record: dict) -> list[str]:
    """Return list of merged rsIDs from a dbSNP record."""
    return [
        f"{m['merged_rsid']}"
        for m in record.get("dbsnp1_merges", []) or []
        if m.get("merged_rsid")
    ]


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        db=None,
    ):  # noqa: E501
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db

        # DTP versioning
        self.dtp_name = "dtp_variant_ncbi"
        self.dtp_version = "1.2.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "3.2.0"

    # -------------------------------------------------------------------------
    #                            EXTRACT METHOD
    # -------------------------------------------------------------------------
    def extract(self, raw_dir: str):
        """
        Downloads the file from the dbSNP JSON release and stores it locally
        only if it doesn't exist or if the MD5 has changed.
        """
        msg = f"📦 Starting extraction of {self.data_source.name} data..."

        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url

        try:
            # Landing path
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )

            # Get hash from current md5 file
            url_md5 = f"{source_url}.md5"
            current_hash = self.get_md5_from_url_file(url_md5)

            # Download the file
            status, msg = self.http_download(source_url, landing_path)

            if not status:
                self.logger.log(msg, "ERROR")
                return False, msg, current_hash

            # Finish block
            msg = f"✅ {self.data_source.name} file downloaded to {landing_path}"  # noqa: E501
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

        msg = f"🔧 Transforming the {self.data_source.name} data ..."

        self.logger.log(msg, "INFO")  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        try:
            input_file = self.get_raw_file(raw_dir)
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg

            output_dir = self.get_path(processed_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            for f in output_dir.iterdir():
                if f.name.endswith(".parquet"):
                    f.unlink()

        except Exception as e:
            msg = f"❌ Error constructing paths: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # parameters
        batch_size = 200_000

        def already_done(pid: int) -> bool:
            return os.path.exists(
                os.path.join(output_dir, f"processed_part_{pid}.parquet")
            )  # noqa E501

        try:
            batch, batch_id = [], 0

            with bz2.open(input_file, "rt", encoding="utf-8") as f:

                for line in f:
                    batch.append(line)
                    if len(batch) >= batch_size:
                        if not already_done(batch_id):
                            self._process_batch(
                                batch, batch_id, str(output_dir)
                            )  # noqa E501
                        else:
                            self.logger.log(
                                f"⏭️  Skipping existing part {batch_id}",
                                "DEBUG",  # noqa E501
                            )  # noqa E501
                        batch_id += 1
                        batch = []

                # Tail
                if batch:
                    if not already_done(batch_id):
                        self._process_batch(batch, batch_id, str(output_dir))
                    else:
                        self.logger.log(
                            f"⏭️  Skipping existing part {batch_id}", "DEBUG"
                        )  # noqa E501
                    batch_id += 1
                    batch = []

            # msg = f"✅ Processing completed with {len(futures)} batches."
            msg = f"✅ Processing completed with {batch_id} batches (serial)."
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

    #  Support functions to TRANSFORM FASE  #
    # --------------------------------------#

    def _extract_snp_positions(self, rec):
        primary = rec.get("primary_snapshot_data") or {}
        placements = primary.get("placements_with_allele") or []

        position_37 = None
        position_38 = None
        ref = None
        alt = None
        alt_new = None
        chrom = None  # int 1..25

        try:
            for p in placements:
                pan = p.get("placement_annot") or {}
                seq_traits = pan.get("seq_id_traits_by_assembly") or []
                if not seq_traits:
                    continue

                assembly_name = (
                    seq_traits[0].get("assembly_name") or ""
                ).upper()  # noqa E501
                seq_type = pan.get("seq_type", "")

                # only refseq_chromosome
                if seq_type != "refseq_chromosome":
                    continue

                # Retrieve the seq_id to map it to the chromosome later
                # ex: "NC_000008.11" -> 8
                alleles = p.get("alleles") or []

                # Extract ref/alt from this placement
                local_ref = None
                local_alt = []
                local_pos = None

                for al in alleles:
                    spdi = (al.get("allele") or {}).get("spdi") or {}
                    hgvs = al.get("hgvs", "") or ""
                    pos0 = spdi.get("position")
                    if pos0 is None:
                        continue
                    pos1 = pos0 + 1  # 0-based -> 1-based

                    # sufix HGVS to know if is ref ou alt
                    # ex: "NC_000008.11:g.19956018="   -> ref
                    #     "NC_000008.11:g.19956018A>G" -> alt
                    #     "NC_000008.11:g.19956018A>T" -> alt
                    if hgvs.endswith("="):
                        local_ref = spdi.get("deleted_sequence") or spdi.get(
                            "inserted_sequence"
                        )  # noqa E501
                        local_pos = pos1
                    elif ">" in hgvs:
                        local_alt.append(spdi.get("inserted_sequence"))
                        local_pos = pos1

                # if not get ref/alt, ignore this placement
                if local_pos is None or local_ref is None or local_alt is None:
                    continue

                # map chromossome from seq_id (or by GenomeAssembly table)
                seq_id = p.get("seq_id") or spdi.get("seq_id")
                chrom = _map_seq_id_to_chrom(seq_id)
                if chrom is None:
                    continue

                # keep by build
                if "GRCH38" in assembly_name:
                    position_38 = local_pos
                    ref = local_ref
                    alt = local_alt
                elif "GRCH37" in assembly_name:
                    position_37 = local_pos
                    # ref/al must be same, but set it if not yet
                    if ref is None:
                        ref = local_ref
                    if alt is None:
                        alt = local_alt

                # stop if both build were figure out
                if position_37 is not None and position_38 is not None:
                    break

            if ref is not None or alt is not None:
                alt_new = "/".join(sorted(set(alt)))

            return chrom, position_37, position_38, ref, alt_new

        except Exception as e:
            print(e)

    def _process_batch(self, batch, batch_id: int, output_dir: str) -> None:
        """
        Process a batch of dbSNP JSON lines and write a Parquet part.

        This replaces the external worker_dbsnp() function and keeps the logic
        contained inside the DTP class.
        """
        pid = os.getpid()
        self.logger.log(
            f"[PID {pid}] Processing batch {batch_id} with {len(batch)} lines...",  # noqa E501
            "DEBUG",
        )

        rows = []

        for line in batch:
            try:
                rec = json.loads(line)

                # Normalize rs_id as numeric (BigInteger)
                raw_refsnp = rec.get("refsnp_id", None)
                if raw_refsnp is None:
                    continue
                try:
                    rs_numeric = int(raw_refsnp)
                except (TypeError, ValueError):
                    continue

                # Keep only SNVs
                primary = rec.get("primary_snapshot_data") or {}
                variant_type = primary.get("variant_type", "")
                if variant_type != "snv":
                    continue

                chrom, pos37, pos38, ref, alt = self._extract_snp_positions(
                    rec
                )  # noqa E501
                if chrom is None or (pos37 is None and pos38 is None):
                    # jump if do not have coordenates
                    continue

                rows.append(
                    {
                        "rs_id": rs_numeric,
                        "chromosome": chrom,
                        "position_37": pos37,
                        "position_38": pos38,
                        "reference_allele": ref,
                        "alternate_allele": alt,
                        "merge_log": _extract_merge_log(rec),
                    }
                )

            except Exception as e:
                self.logger.log(
                    f"[PID {pid}] ⚠️ Error in batch {batch_id}: {e}",
                    "WARNING",
                )
                continue

        if not rows:
            self.logger.log(
                f"[PID {pid}] ⚠️ No rows produced for batch {batch_id}",
                "WARNING",
            )
            return

        df = pd.DataFrame(rows)

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        out_path = output_dir / f"processed_part_{batch_id}.parquet"
        out_path_csv = output_dir / f"processed_part_{batch_id}.csv"

        df.to_parquet(out_path, index=False)
        if self.debug_mode:
            # Save in CSV format to debug
            df.to_csv(out_path_csv, index=False)

        self.logger.log(
            f"[PID {pid}] ✅ Finished batch {batch_id}, "
            f"saved {len(df)} rows → {out_path}",
            "INFO",
        )

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------
    def load(self, processed_dir=None):

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        # Setting variables to loader
        total_variants = 0
        total_warnings = 0
        # total_snps = 0
        self.LOAD_CHUNK_SIZE = 50_000
        self.dropped_variants = []

        if self.debug_mode:
            start_total = time.time()

        # ----= READ PROCESSED DATA =----
        # NOTE: # List all generated Parquet files.
        try:
            if not processed_dir:
                msg = "⚠️  processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR
            processed_path = self.get_path(processed_dir)
            files_list = sorted(
                glob.glob(str(processed_path / "processed_part_*.parquet"))
            )
            if not files_list:
                msg = f"No part files found in {processed_path}"
                self.logger.log(msg, "ERROR")
                return False, msg
            msg = f"📄 Found {len(files_list)} part files to load"
            self.logger.log(msg, "INFO")
        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # Set DB and drop indexes
        try:
            self.db_write_mode()
            self.drop_indexes(self.get_snp_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # ===== PROCESS PER FILE =====
        # ============================
        for data_file in files_list:
            self.logger.log(f"📂 Processing {data_file}", "INFO")

            try:
                df_data = pd.read_parquet(data_file, engine="pyarrow")

                if df_data.empty:
                    self.logger.log(f"⚠️ Empty file (skipped): {data_file}", "WARNING")
                    continue

                # ✅ Transaction per file (commit/rollback automático)
                with self.db.engine.begin() as conn:
                    # SNP UPSERT
                    self._upsert_snps_from_df(df=df_data, conn=conn)

                    # SNPMerge UPSERT/INSERT
                    self._upsert_snpmerge_from_df(df=df_data, conn=conn)

                self.logger.log(f"✅ Processed {data_file}", "INFO")

            except Exception as e:
                total_warnings += 1
                self.logger.log(f"❌ SNP load failed for {data_file}: {e}", "ERROR")
                raise

        # Set DB to Read Mode and Create Index
        try:
            # self.create_indexes(self.get_snp_index_specs)
            # self.db_read_mode()
            self.logger.log("ℹ️  Index creation currently disabled.", "INFO")
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to finalize DB (create indexes / read mode): {e}"
            self.logger.log(msg, "WARNING")

        if self.debug_mode:
            msg = f"Load process ran in {time.time() - start_total:.2f}s"
            self.logger.log(msg, "DEBUG")

        if total_warnings == 0:
            msg = f"✅ Loaded {total_variants} variants from {len(files_list)} file(s)."
            self.logger.log(msg, "SUCCESS")
            return True, msg

        msg = f"⚠️ Loaded {total_variants} variants with {total_warnings} warning(s). Check logs."
        self.logger.log(msg, "WARNING")
        return True, msg

    # -------------------------------------------------------------------------
    # Get Dialect Cnnector
    # -------------------------------------------------------------------------

    def _get_insert_for_dialect(self, table, dialect_name: str):
        if dialect_name == "sqlite":
            return sqlite_insert(table)
        if dialect_name == "postgresql":
            return pg_insert(table)
        return generic_insert(table)

    # -------------------------------------------------------------------------
    # INSERT Variants
    # -------------------------------------------------------------------------

    def _upsert_snps_from_df(self, df: pd.DataFrame, conn: Connection) -> int:
        if df.empty:
            return 0

        v = self.db.table("variant_snps")
        dialect_name = conn.dialect.name

        # SQLite param-limit safety
        chunk_size = 80 if dialect_name == "sqlite" else 2000

        records: List[Dict[str, Any]] = []
        for row in df.itertuples(index=False):
            try:
                rs_num = int(row.rs_id)
                chrom = int(row.chromosome)
            except Exception:
                continue

            records.append(
                {
                    "source_id": rs_num,
                    "source_type": "rs",
                    "chromosome": chrom,
                    "position_37": (
                        int(row.position_37) if pd.notna(row.position_37) else None
                    ),
                    "position_38": (
                        int(row.position_38) if pd.notna(row.position_38) else None
                    ),
                    "position_other": None,
                    "reference_allele": row.reference_allele,
                    "alternate_allele": row.alternate_allele,
                    "data_source_id": self.data_source.id,
                    "etl_package_id": self.package.id,
                }
            )

        if not records:
            return 0

        insert_cls = self._get_insert_for_dialect(v, dialect_name)

        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]
            stmt = insert_cls.values(chunk)

            if dialect_name in ("sqlite", "postgresql"):
                stmt = stmt.on_conflict_do_update(
                    index_elements=["chromosome", "source_type", "source_id"],
                    set_={
                        "position_37": stmt.excluded.position_37,
                        "position_38": stmt.excluded.position_38,
                        "position_other": stmt.excluded.position_other,
                        "reference_allele": stmt.excluded.reference_allele,
                        "alternate_allele": stmt.excluded.alternate_allele,
                        "data_source_id": stmt.excluded.data_source_id,
                        "etl_package_id": stmt.excluded.etl_package_id,
                    },
                )

            conn.execute(stmt)

        return len(records)

    # -------------------------------------------------------------------------
    # INSERT Variants Merged
    # -------------------------------------------------------------------------

    def _upsert_snpmerge_from_df(self, df: pd.DataFrame, conn: Connection) -> None:
        if df.empty:
            return

        m = self.db.table("variant_snp_merges")  # ajuste para o nome real da tabela
        dialect_name = conn.dialect.name

        chunk_size = 100 if dialect_name == "sqlite" else 5000

        insert_cls = self._get_insert_for_dialect(m, dialect_name)

        records: List[Dict[str, Any]] = []

        # se merge_log no parquet já vem como list, perfeito
        for row in df.itertuples(index=False):
            try:
                canonical = int(getattr(row, "rs_id"))
            except Exception:
                continue

            merge_list = getattr(row, "merge_log", None)

            if merge_list is None:
                continue

            if len(merge_list) == 0:
                continue

            for obsolete in merge_list:
                try:
                    obsolete_int = int(obsolete)
                except Exception:
                    continue

                records.append(
                    {
                        "rs_obsolete_id": obsolete_int,
                        "rs_canonical_id": canonical,
                        "data_source_id": self.data_source.id,
                        "etl_package_id": self.package.id,
                    }
                )

        if not records:
            return

        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]
            stmt = insert_cls.values(chunk)

            if dialect_name in ("postgresql", "sqlite"):
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["rs_obsolete_id", "rs_canonical_id"]
                )

            conn.execute(stmt)

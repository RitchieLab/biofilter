# dtp_variant_ncbi_v2.py
import os
import bz2
import glob
import sys
import __main__
import time
import pandas as pd
import numpy as np
from typing import Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
from sqlalchemy.exc import IntegrityError
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models import (
    GenomeAssembly,
    VariantMaster,
    VariantLocus,
    EntityGroup,
    EntityAlias,
    EntityRelationship,
    EntityRelationshipType,
)
from biofilter.etl.dtps.worker_dbsnp import worker_dbsnp
import sqlalchemy as sa


# Staging tables (permanent, truncated each file; works across SQLite/Postgres)
STG_RS_NORM       = "tmp_rs_norm"
STG_NEW_RS        = "tmp_new_rs"
STG_RS_MAP        = "tmp_rs_map"
STG_NEW_ENTITIES  = "tmp_new_entities"
STG_LOCUS         = "tmp_locus"
STG_VMASTER       = "tmp_vmaster"
STG_VAR_GENE      = "tmp_var_gene"
STG_RS_MERGE      = "tmp_rs_merge"
STG_VAR_GENE_IDS  = "tmp_var_gene_ids"


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
        self.conflict_mgr = ConflictManager(session, logger)

        # DTP versioning
        self.dtp_name = "dtp_variant_ncbi"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Downloads the file from the dbSNP JSON release and stores it locally
        only if it doesn't exist or if the MD5 has changed.
        """
        msg = f"Starting extraction of {self.data_source.name} data..."

        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url
        # if force_steps:
        #     last_hash = ""
        #     msg = "Ignoring hash check."
        #     self.logger.log(msg, "WARNING")
        # else:
        #     last_hash = self.etl_process.raw_data_hash

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

            # if not current_hash:
            #     msg = f"Failed to retrieve MD5 from {url_md5}"
            #     self.logger.log(msg, "WARNING")
            #     return False, msg, None

            # # Compare current hash and last processed hash
            # if current_hash == last_hash:
            #     msg = f"No change detected in {source_url}"
            #     self.logger.log(msg, "INFO")
            #     return False, msg, current_hash

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

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
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
        max_workers = 6

        try:
            futures, batch, batch_id = [], [], 0
            with bz2.open(
                input_file, "rt", encoding="utf-8"
            ) as f, ProcessPoolExecutor(  # noqa E501
                max_workers=max_workers
            ) as ex:
                if __name__ == "__main__" or (
                    hasattr(__main__, "__file__") and not hasattr(sys, "ps1")
                ):
                    for line in f:
                        batch.append(line)
                        if len(batch) >= batch_size:
                            futures.append(
                                ex.submit(
                                    worker_dbsnp,
                                    batch.copy(),
                                    batch_id,
                                    output_dir,
                                )
                            )
                            batch.clear()
                            batch_id += 1
                    if batch:
                        futures.append(
                            ex.submit(
                                worker_dbsnp,
                                batch.copy(),
                                batch_id,
                                output_dir,
                            )
                        )

                    for fut in as_completed(futures):
                        fut.result()
                else:
                    self.logger.log(
                        "⚠️ Skipping multiprocessing: not in __main__ context.",  # noqa E501
                        "WARNING",
                    )

            msg = f"✅ Processing completed with {len(futures)} batches."
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # --- Support methods ---

    def _to_py(self, x):
        """Converte strings que representam listas/dicts para objeto Python."""
        if isinstance(x, np.ndarray):
            x = x.tolist()
        if isinstance(x, (list, dict)) or x is None:
            return x

    def _load_input_frame(self, path: str) -> pd.DataFrame:
        if path.endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path, sep=",")
        expected = [
            "rs_id",
            "variant_type",
            "build_id",
            "seq_id",
            "assembly",
            "start_pos",
            "end_pos",
            "ref",
            "alt",
            "placements",
            "merge_log",
            "gene_links",
            "quality",
        ]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            raise ValueError(f"Input file {path} missing columns: {missing}")

        for c in ["start_pos", "end_pos", "build_id"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        df["placements"] = df["placements"].apply(self._to_py)
        df["merge_log"] = df["merge_log"].apply(self._to_py)
        df["gene_links"] = df["gene_links"].apply(self._to_py)

        # alt can come as a list of strings; normalize to string "A/T"
        def _alt_str(x):
            if isinstance(x, list):
                return "/".join(
                    sorted(
                        {str(a) for a in x if a is not None and str(a) != ""}
                    )  # noqa E501
                )
            if x is None:
                return ""
            return str(x)

        df["alt"] = df["alt"].apply(_alt_str)
        df["ref"] = df["ref"].fillna("").astype(str)

        # In the absence of placement or empty lists, use []
        for c in ["placements", "merge_log", "gene_links"]:
            df[c] = df[c].apply(lambda v: v if isinstance(v, list) else [])

        return df

    def _norm_rs(self, x: str) -> str | None:
        if not x:
            return None
        s = str(x).strip()
        # Accept "RS123", "rs123", "  rs123  "
        if s.lower().startswith("rs") and s[2:].isdigit():
            return f"rs{int(s[2:])}"
        # Some dumps come with just the number
        if s.isdigit():
            return f"rs{int(s)}"
        return None

    def _norm_chr(s: str | None) -> str | None:
        if not s:
            return None
        x = str(s).strip().upper()
        if x.startswith("CHR"):
            x = x[3:]
        if x in {"23", "X"}:
            return "X"
        if x in {"24", "Y"}:
            return "Y"
        if x in {"M", "MT", "MITO", "MITOCHONDRIAL"}:
            return "MT"
        return x  # "1".."22", "X","Y","MT"

    def _ensure_list(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return []
        if isinstance(x, (list, tuple, set)):
            return list(x)
        return [x]


    # -------------------------------------------
    # Helpers for a DB-agnostic, set-based LOAD
    # -------------------------------------------

    def _exec(self, sql: str, params: dict | None = None):
        """Simple wrapper to execute SQL text with named params."""
        return self.session.execute(sa.text(sql), params or {})

    # def _ensure_staging_tables(self):
    #     """Create staging tables if not exist (portable DDL)."""
    #     ddl = f"""
    #     CREATE TABLE IF NOT EXISTS {STG_RS_NORM} (rs_norm TEXT PRIMARY KEY);
    #     CREATE TABLE IF NOT EXISTS {STG_NEW_RS} (rs_norm TEXT PRIMARY KEY);
    #     CREATE TABLE IF NOT EXISTS {STG_RS_MAP} (rs_norm TEXT PRIMARY KEY, entity_id INTEGER);

    #     CREATE TABLE IF NOT EXISTS {STG_NEW_ENTITIES} (entity_id INTEGER, rs_norm TEXT);

    #     CREATE TABLE IF NOT EXISTS {STG_LOCUS} (
    #     rs_norm TEXT,
    #     assembly_id INTEGER,
    #     chromosome TEXT,
    #     start_pos BIGINT,
    #     end_pos BIGINT,
    #     reference_allele TEXT,
    #     alternate_allele TEXT,
    #     data_source_id INTEGER
    #     );

    #     CREATE TABLE IF NOT EXISTS {STG_VMASTER} (
    #     rs_norm TEXT PRIMARY KEY,
    #     variant_type TEXT,
    #     chromosome TEXT,
    #     quality TEXT
    #     );

    #     CREATE TABLE IF NOT EXISTS {STG_VAR_GENE} (
    #     rs_norm TEXT,
    #     gene_alias_norm TEXT,
    #     data_source_id INTEGER
    #     );

    #     CREATE TABLE IF NOT EXISTS {STG_RS_MERGE} (
    #     primary_rs_norm TEXT,
    #     merged_rs_norm  TEXT
    #     );

    #     CREATE TABLE IF NOT EXISTS {STG_VAR_GENE_IDS} (
    #     src_entity_id INTEGER,
    #     dst_entity_id INTEGER,
    #     data_source_id INTEGER
    #     );
    #     """
    #     for stmt in ddl.strip().split(";\n"):
    #         if stmt.strip():
    #             self._exec(stmt)
    def _ensure_staging_tables(self):
        """Create staging tables (TEMP on SQLite, UNLOGGED on Postgres) + helpful indexes."""
        dialect = self.session.bind.dialect.name
        if dialect == "sqlite":
            tprefix = "TEMP "
            unlogged = ""            # not supported
        elif dialect == "postgresql":
            tprefix = ""             # UNLOGGED can’t be TEMP at same time
            unlogged = "UNLOGGED "   # speeds up writes; good for volatile staging
        else:
            tprefix = ""
            unlogged = ""

        # Tables
        ddls = [
            # rs keys
            f"CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_RS_NORM} (rs_norm TEXT PRIMARY KEY)",
            f"CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_NEW_RS} (rs_norm TEXT PRIMARY KEY)",
            f"CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_RS_MAP} (rs_norm TEXT PRIMARY KEY, entity_id INTEGER NOT NULL)",

            # new entities mapping (one row per new rs)
            f"CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_NEW_ENTITIES} (entity_id INTEGER PRIMARY KEY, rs_norm TEXT UNIQUE)",

            # locus rows (may have duplicates before dedupe on insert-select)
            f"""CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_LOCUS} (
                rs_norm TEXT,
                assembly_id INTEGER,
                chromosome TEXT,
                start_pos BIGINT,
                end_pos BIGINT,
                reference_allele TEXT,
                alternate_allele TEXT,
                data_source_id INTEGER
            )""",

            # per-rs attributes for VariantMaster
            f"""CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_VMASTER} (
                rs_norm TEXT PRIMARY KEY,
                variant_type TEXT,
                chromosome TEXT,
                quality TEXT
            )""",

            # variant->gene links from file
            f"""CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_VAR_GENE} (
                rs_norm TEXT,
                gene_alias_norm TEXT,
                data_source_id INTEGER
            )""",

            # merge map (old rs -> primary rs)
            f"""CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_RS_MERGE} (
                primary_rs_norm TEXT,
                merged_rs_norm  TEXT
            )""",

            # resolved ids for relationships
            f"""CREATE {tprefix}{unlogged}TABLE IF NOT EXISTS {STG_VAR_GENE_IDS} (
                src_entity_id INTEGER,
                dst_entity_id INTEGER,
                data_source_id INTEGER
            )""",
        ]

        # Indexes that speed up joins (cheap to keep; create once)
        idx = [
            f"CREATE INDEX IF NOT EXISTS ix_{STG_RS_NORM}_rs ON {STG_RS_NORM} (rs_norm)",
            f"CREATE INDEX IF NOT EXISTS ix_{STG_RS_MAP}_rs ON {STG_RS_MAP} (rs_norm)",
            f"CREATE INDEX IF NOT EXISTS ix_{STG_RS_MAP}_eid ON {STG_RS_MAP} (entity_id)",

            f"CREATE INDEX IF NOT EXISTS ix_{STG_LOCUS}_rs ON {STG_LOCUS} (rs_norm)",
            f"CREATE INDEX IF NOT EXISTS ix_{STG_LOCUS}_asm_chr_pos ON {STG_LOCUS} (assembly_id, chromosome, start_pos, end_pos)",

            f"CREATE INDEX IF NOT EXISTS ix_{STG_VAR_GENE}_rs ON {STG_VAR_GENE} (rs_norm)",
            f"CREATE INDEX IF NOT EXISTS ix_{STG_VAR_GENE}_gene ON {STG_VAR_GENE} (gene_alias_norm)",

            f"CREATE INDEX IF NOT EXISTS ix_{STG_RS_MERGE}_primary ON {STG_RS_MERGE} (primary_rs_norm)",
            f"CREATE INDEX IF NOT EXISTS ix_{STG_RS_MERGE}_merged  ON {STG_RS_MERGE} (merged_rs_norm)",
        ]

        # Execute one by one (SQLite can’t batch text() with multiple statements)
        for stmt in ddls + idx:
            self._exec(stmt)

    def _truncate_staging(self):
        """Clean staging tables before each file."""
        for t in [
            STG_RS_NORM, STG_NEW_RS, STG_RS_MAP, STG_NEW_ENTITIES,
            STG_LOCUS, STG_VMASTER, STG_VAR_GENE, STG_RS_MERGE,
            STG_VAR_GENE_IDS
        ]:
            self._exec(f"DELETE FROM {t}")

    # def _to_sql(self, df: pd.DataFrame, table: str):
    #     """Portable bulk upload using pandas.to_sql (works for SQLite & Postgres)."""
    #     if df is None or df.empty:
    #         return
    #     df.to_sql(table, con=self.session.bind, if_exists="append", index=False, method="multi", chunksize=100_000)
    def _to_sql(self, df: pd.DataFrame, table: str, chunksize: int = 20_000):
        """Portable bulk upload using the SAME Session connection to avoid SQLite writer lock."""
        if df is None or df.empty:
            return

        # import time
        # import sqlalchemy as sa

        # Use session-bound Connection (same transaction/connection as previous DELETEs)
        conn = self.session.connection()  # <-- chave para evitar "database is locked"

        # Small exponential backoff on SQLite 'database is locked'
        max_attempts = 6
        for attempt in range(max_attempts):
            try:
                # Note: method='multi' gera executemany de INSERTs
                df.to_sql(
                    table,
                    con=conn,                # <-- mesma conexão
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=chunksize,
                )
                return
            except Exception as e:
                msg = str(e).lower()
                is_sqlite = (self.session.bind.dialect.name == "sqlite")
                if is_sqlite and ("database is locked" in msg or "database is locked" in repr(e).lower()):
                    sleep = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s, 4s, 8s, 16s
                    self.logger.log(f"⏳ SQLite locked inserting into {table}. Retrying in {sleep:.1f}s...", "WARNING")
                    time.sleep(sleep)
                    continue
                raise  # não era lock do SQLite — propaga

    def _get_next_entity_id(self) -> int:
        """Pre-allocate IDs (portable across SQLite/Postgres). Single-writer ETL assumed."""
        return int(self._exec("SELECT COALESCE(MAX(id),0)+1 FROM entities").scalar() or 1)

    def _db_write_mode_portable(self):
        """Boost session for heavy write."""
        try:
            self.db_write_mode()  # keep your existing tuning
        except Exception:
            pass
        # Extra pragmas for SQLite
        if self.session.bind.dialect.name == "sqlite":
            self._exec("PRAGMA journal_mode=WAL")
            self._exec("PRAGMA synchronous=OFF")
            self._exec("PRAGMA temp_store=MEMORY")
            self._exec("PRAGMA cache_size=-500000")
            self._exec("PRAGMA busy_timeout=60000")  # <— espera até 60s se estiver lockado

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

        total_warnings = 0

        if self.debug_mode:
            start_total = time.time()

        # ----= READ PART FILES =----
        # ---------------------------
        try:
            if not processed_dir:
                msg = "⚠️ processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg
            processed_path = self.get_path(processed_dir)
            files_list = sorted(glob.glob(str(processed_path / "processed_part_*.parquet")))  # noqa E501
            if not files_list:
                msg = f"No part files found in {processed_path}"
                self.logger.log(msg, "ERROR")
                return False, msg
            self.logger.log(f"📄 Found {len(files_list)} part files to load", "INFO")  # noqa E501
        except Exception as e:
            msg = f"⚠️ Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # ----= DB WRITE MODE + drop heavy indexes =----
        # ----------------------------------------------
        try:
            self._db_write_mode_portable()
            self.drop_indexes(self.get_variant_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️ Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg

        # ----= GROUPS & RELATION TYPE & ASSEMBLIES MAPS =----
        # ----------------------------------------------------
        try:
            self.get_entity_group("Variants")  # sets self.entity_group
            gene_group = self.session.query(EntityGroup).filter_by(name="Genes").first()  # noqa E501
            if not gene_group:
                raise ValueError("EntityGroup 'Genes' not found in database.")
        except Exception as e:
            msg = f"Error on DTP to get Entity Group: {e}"
            return False, msg

        relationship_type = (
            self.session.query(EntityRelationshipType)
            .filter(EntityRelationshipType.code == "associated_with")
            .one_or_none()
        )
        if not relationship_type:
            relationship_type = EntityRelationshipType(
                code="associated_with",
                description="Auto-created by variant DTP"
            )
            self.session.add(relationship_type)
            self.session.commit()

        assemblies = self.session.query(GenomeAssembly).all()
        acc2asm_id: Dict[str, int] = {a.accession: a.id for a in assemblies}
        acc2chrom: Dict[str, str] = {a.accession: (a.chromosome or "") for a in assemblies}  # noqa E501

        # ----= Ensure staging exists =----
        # ---------------------------------
        self._ensure_staging_tables()

        # ========= FILE LOOP =========
        # =============================
        for part_path in files_list:


            try:
                self.logger.log(f"📂 Processing {part_path}", "INFO")
                start_file = time.time()
                if self.debug_mode:
                    start_file = time.time()

                df = self._load_input_frame(part_path)
                if df.empty:
                    self.logger.log("DataFrame is empty.", "WARNING")
                    continue

                # Normalize rs and build per-file DataFrames for staging
                # 1) rs_norm (primary)
                df["rs_norm"] = df["rs_id"].apply(self._norm_rs)
                df_rs = df[["rs_norm"]].dropna().drop_duplicates()

                # 2) VariantMaster attributes (one per rs_norm)
                def _norm_vtype(x):
                    return str(x).upper() if pd.notna(x) and str(x).strip() else "SNP"
                
                def _canon_chr(seq_id):
                    return acc2chrom.get(str(seq_id), None)

                df_vmaster = (
                    df[["rs_norm", "variant_type", "seq_id", "quality"]]
                    .dropna(subset=["rs_norm"])
                    .assign(
                        variant_type=lambda d: d["variant_type"].apply(_norm_vtype),
                        chromosome=lambda d: d["seq_id"].apply(_canon_chr),
                    )[["rs_norm", "variant_type", "chromosome", "quality"]]
                    .drop_duplicates(subset=["rs_norm"])
                )

                # 3) Canonical locus + placements → tmp_locus
                # Canonical
                can_locus = df[["rs_norm", "seq_id", "start_pos", "end_pos", "ref", "alt"]].dropna(subset=["rs_norm"]).copy()
                can_locus["assembly_id"] = can_locus["seq_id"].astype(str).map(acc2asm_id).astype("Int64")
                can_locus["chromosome"]  = can_locus["seq_id"].astype(str).map(acc2chrom)
                can_locus = can_locus.dropna(subset=["assembly_id", "start_pos", "end_pos"])
                can_locus = can_locus.assign(
                    reference_allele=lambda d: d["ref"].astype(str),
                    alternate_allele=lambda d: d["alt"].astype(str),
                    data_source_id=self.data_source.id,
                )[["rs_norm", "assembly_id", "chromosome", "start_pos", "end_pos", "reference_allele", "alternate_allele", "data_source_id"]]

                # Placements (list of dicts)
                # Expand safely and filter
                if "placements" in df.columns:
                    exp = (
                        df[["rs_norm", "placements"]]
                        .dropna(subset=["rs_norm"])
                        .explode("placements", ignore_index=True)
                    )
                    exp = exp[exp["placements"].notna()]
                    if not exp.empty:
                        pl = pd.json_normalize(exp["placements"]).rename(columns={
                            "seq_id":"p_seq","start_pos":"p_start","end_pos":"p_end","ref":"p_ref","alt":"p_alt","assembly":"p_asm"
                        })
                        pl["rs_norm"] = exp["rs_norm"].values
                        pl["assembly_id"] = pl["p_seq"].astype(str).map(acc2asm_id).astype("Int64")
                        pl["chromosome"]  = pl["p_seq"].astype(str).map(acc2chrom)
                        pl = pl.dropna(subset=["assembly_id","p_start","p_end"])
                        # discard ref==alt (no variation)
                        pl["p_ref"] = pl["p_ref"].astype(str)
                        pl["p_alt"] = pl["p_alt"].astype(str)
                        pl = pl[pl["p_ref"] != pl["p_alt"]]
                        pl = pl.assign(
                            reference_allele=lambda d: d["p_ref"],
                            alternate_allele=lambda d: d["p_alt"],
                            start_pos=lambda d: d["p_start"].astype("Int64"),
                            end_pos=lambda d: d["p_end"].astype("Int64"),
                            data_source_id=self.data_source.id,
                        )
                        pl = pl[["rs_norm","assembly_id","chromosome","start_pos","end_pos","reference_allele","alternate_allele","data_source_id"]]
                        df_locus = pd.concat([can_locus, pl], ignore_index=True)
                    else:
                        df_locus = can_locus
                else:
                    df_locus = can_locus

                # Deduplicate locus rows
                if not df_locus.empty:
                    df_locus = df_locus.drop_duplicates(
                        subset=["rs_norm","assembly_id","chromosome","start_pos","end_pos","reference_allele","alternate_allele","data_source_id"]
                    )

                # 4) Variant → Gene links (rs_norm, entrez as alias_norm)
                # 'gene_links' is a list; assume integers or strings convertible to int
                if "gene_links" in df.columns:
                    gl = (
                        df[["rs_norm","gene_links"]]
                        .dropna(subset=["rs_norm"])
                        .explode("gene_links", ignore_index=True)
                    )
                    gl = gl[gl["gene_links"].notna()]
                    if not gl.empty:
                        gl["gene_alias_norm"] = gl["gene_links"].astype(str).str.replace(r"\.0$","",regex=True)
                        gl["data_source_id"]  = self.data_source.id
                        df_var_gene = gl[["rs_norm","gene_alias_norm","data_source_id"]].drop_duplicates()
                    else:
                        df_var_gene = pd.DataFrame(columns=["rs_norm","gene_alias_norm","data_source_id"])
                else:
                    df_var_gene = pd.DataFrame(columns=["rs_norm","gene_alias_norm","data_source_id"])

                # 5) Merge aliases (primary_rs_norm → merged_rs_norm)
                if "merge_log" in df.columns:
                    mg = (
                        df[["rs_norm","merge_log"]]
                        .dropna(subset=["rs_norm"])
                        .explode("merge_log", ignore_index=True)
                    )
                    mg = mg[mg["merge_log"].notna()]
                    if not mg.empty:
                        mg["primary_rs_norm"] = mg["rs_norm"]
                        mg["merged_rs_norm"]  = mg["merge_log"].apply(self._norm_rs)
                        mg = mg.dropna(subset=["merged_rs_norm"])
                        mg = mg[mg["primary_rs_norm"] != mg["merged_rs_norm"]]
                        df_merge = mg[["primary_rs_norm","merged_rs_norm"]].drop_duplicates()
                    else:
                        df_merge = pd.DataFrame(columns=["primary_rs_norm","merged_rs_norm"])
                else:
                    df_merge = pd.DataFrame(columns=["primary_rs_norm","merged_rs_norm"])

                # --------------------------
                # Upload to staging (per file)
                # --------------------------
                self._truncate_staging()
                self._to_sql(df_rs, STG_RS_NORM)
                self._to_sql(df_vmaster, STG_VMASTER)
                self._to_sql(df_locus, STG_LOCUS)
                self._to_sql(df_var_gene, STG_VAR_GENE)
                self._to_sql(df_merge, STG_RS_MERGE)

                # --------------------------
                # DB-side set-based steps
                # --------------------------

                # A) Discover new rs_norm
                self._exec("DROP TABLE IF EXISTS tmp_new_rs")
                self._exec(f"""
                    CREATE TEMP TABLE tmp_new_rs AS
                    SELECT r.rs_norm
                    FROM {STG_RS_NORM} r
                    LEFT JOIN entity_aliases ea
                    ON ea.alias_norm = r.rs_norm
                    AND ea.group_id = :vg
                    WHERE ea.id IS NULL
                """, {"vg": self.entity_group})

                # """
                # stg_total = self._exec("SELECT COUNT(*) AS stg_total FROM tmp_rs_norm").scalar()
                # print("stg_total =", stg_total)

                # vg_id = self._exec("SELECT id FROM entity_groups WHERE name='Variants'").scalar()
                # print("vg_id =", vg_id)

                # matches = self._exec("""
                #     SELECT COUNT(*)
                #     FROM tmp_rs_norm r
                #     JOIN entity_aliases ea
                #     ON ea.alias_norm = r.rs_norm
                #     AND ea.group_id   = :vg
                # """, {"vg": vg_id}).scalar()
                # print("matches =", matches)

                # novos = self._exec("""
                #     SELECT r.rs_norm
                #     FROM tmp_rs_norm r
                #     LEFT JOIN entity_aliases ea
                #     ON (ea.alias_norm = r.rs_norm AND ea.group_id = :vg)
                #     WHERE ea.id IS NULL
                #     LIMIT 20
                # """, {"vg": vg_id}).all()
                # print("novos (sample) =", [r[0] for r in novos])

                # """


                vg_id = self.entity_group if isinstance(self.entity_group, int) else self.entity_group.id

                # B) Pre-allocate IDs and insert Entities + primary aliases (only if new exist)
                new_count = int(self._exec(f"SELECT COUNT(*) FROM {STG_NEW_RS}").scalar() or 0)
                if new_count > 0:
                    start_id = self._get_next_entity_id()

                    # Build tmp_new_entities(entity_id, rs_norm)
                    new_rs = pd.read_sql(f"SELECT rs_norm FROM {STG_NEW_RS}", self.session.connection())
                    new_rs["entity_id"] = [start_id + i for i in range(len(new_rs))]
                    self._to_sql(new_rs[["entity_id", "rs_norm"]], STG_NEW_ENTITIES)

                    # Entities (💡 agora com data_source_id e etl_package_id)
                    self._exec(f"""
                        INSERT INTO entities (id, group_id, has_conflict, is_active, data_source_id, etl_package_id)
                        SELECT ne.entity_id, :vg, 0, 1, :ds, :pkg
                        FROM {STG_NEW_ENTITIES} ne
                    """, {"vg": vg_id, "ds": self.data_source.id, "pkg": self.package.id})

                    # # Insert primary aliases for rs_norm (type 'rsID', xref 'dbSNP')
                    # Primary alias for rs_norm (type 'rsID', xref 'dbSNP') + package stamp
                    self._exec(f"""
                        INSERT INTO entity_aliases (
                            entity_id, alias_value, alias_norm, alias_type, xref_source,
                            group_id, data_source_id, etl_package_id,
                            is_primary, is_active
                        )
                        SELECT ne.entity_id, ne.rs_norm, ne.rs_norm, 'rsID', 'dbSNP',
                            :vg, :ds, :pkg,
                            1, 1
                        FROM {STG_NEW_ENTITIES} ne
                        LEFT JOIN entity_aliases ea
                        ON ea.entity_id = ne.entity_id
                        AND ea.group_id  = :vg
                        AND ea.alias_norm = ne.rs_norm
                        AND ea.alias_type = 'rsID'
                        AND ea.xref_source = 'dbSNP'
                        WHERE ea.id IS NULL
                    """, {"vg": vg_id, "ds": self.data_source.id, "pkg": self.package.id})

                # C) Build full rs → entity_id map (for all rs in file)
                # 1) limpar a staging
                self._exec(f"DELETE FROM {STG_RS_MAP}")

                # 2) popular a staging (JOIN robusto, aceita alias_norm ou alias_value)
                self._exec(f"""
                    INSERT INTO {STG_RS_MAP} (rs_norm, entity_id)
                    SELECT r.rs_norm, ea.entity_id
                    FROM {STG_RS_NORM} r
                    JOIN entity_aliases ea
                    ON (
                        (LOWER(COALESCE(ea.alias_norm, ''))  = LOWER(r.rs_norm)
                        OR  LOWER(COALESCE(ea.alias_value,'')) = LOWER(r.rs_norm))
                    AND ea.group_id = :vg
                    )
                """, {"vg": vg_id})


                # D) VariantMaster (idempotent by variant_id)
                # We store rs_norm (string) in variant_masters.variant_id and link entity_id
                self._exec(f"""
                    INSERT INTO variant_masters (
                        variant_id, variant_type, omic_status_id, chromosome, quality,
                        entity_id, data_source_id, etl_package_id
                    )
                    SELECT DISTINCT
                        vm.rs_norm                      AS variant_id,
                        vm.variant_type,
                        1                                AS omic_status_id,
                        vm.chromosome,
                        vm.quality,
                        m.entity_id,
                        :ds                              AS data_source_id,
                        :pkg                             AS etl_package_id
                    FROM {STG_RS_MAP} m
                    JOIN {STG_VMASTER} vm
                    ON vm.rs_norm = m.rs_norm
                    LEFT JOIN variant_masters ex
                    ON ex.variant_id = vm.rs_norm      -- <— chave de idempotência correta (UNIQUE)
                    WHERE ex.id IS NULL
                """, {"ds": self.data_source.id, "pkg": self.package.id})

                # E) VariantLocus (idempotent; correct table name and FK to variant_masters.id)
                self._exec(f"""
                    INSERT INTO variant_loci (
                        variant_id, assembly_id, chromosome, start_pos, end_pos,
                        reference_allele, alternate_allele, data_source_id, etl_package_id
                    )
                    SELECT DISTINCT
                        vmdb.id                         AS variant_id,   -- <— FK para variant_masters.id
                        l.assembly_id,
                        l.chromosome,
                        l.start_pos,
                        l.end_pos,
                        l.reference_allele,
                        l.alternate_allele,
                        l.data_source_id,
                        :pkg
                    FROM {STG_LOCUS} l
                    JOIN {STG_RS_MAP} m
                    ON m.rs_norm = l.rs_norm
                    JOIN variant_masters vmdb
                    ON vmdb.variant_id = m.rs_norm     -- pega o ID interno do VM
                    LEFT JOIN variant_loci vl
                    ON vl.variant_id   = vmdb.id
                    AND vl.assembly_id  = l.assembly_id
                    AND vl.chromosome   = l.chromosome
                    AND vl.start_pos    = l.start_pos
                    AND vl.end_pos      = l.end_pos
                    AND ( (vl.reference_allele IS NULL AND l.reference_allele IS NULL)
                        OR vl.reference_allele = l.reference_allele )
                    AND ( (vl.alternate_allele IS NULL AND l.alternate_allele IS NULL)
                        OR vl.alternate_allele = l.alternate_allele )
                    WHERE vl.id IS NULL
                """, {"pkg": self.package.id})

                # F) Insert merged aliases (skip existing alias_norm)
                self._exec(f"""
                    INSERT INTO entity_aliases (
                        entity_id, alias_value, alias_norm, alias_type, xref_source,
                        group_id, data_source_id, etl_package_id,
                        is_primary, is_active
                    )
                    SELECT DISTINCT m.entity_id, mr.merged_rs_norm, mr.merged_rs_norm, 'merged', 'dbSNP',
                        :vg, :ds, :pkg,
                        0, 0
                    FROM {STG_RS_MERGE} mr
                    JOIN {STG_RS_MAP}   m  ON m.rs_norm = mr.primary_rs_norm
                    LEFT JOIN entity_aliases ea
                    ON ea.alias_norm = mr.merged_rs_norm
                    AND ea.group_id   = :vg
                    WHERE ea.id IS NULL
                """, {"vg": vg_id, "ds": self.data_source.id, "pkg": self.package.id})


                # G) Variant → Gene links
                # Resolve gene aliases 'ENTREZ' (alias_type='code', xref_source='ENTREZ')
                self._exec(f"DELETE FROM {STG_VAR_GENE_IDS}")
                self._exec(f"""
                    INSERT INTO {STG_VAR_GENE_IDS} (src_entity_id, dst_entity_id, data_source_id)
                    SELECT DISTINCT rm.entity_id AS src, gea.entity_id AS dst, vg.data_source_id
                    FROM {STG_VAR_GENE} vg
                    JOIN {STG_RS_MAP} rm
                    ON rm.rs_norm = vg.rs_norm
                    JOIN entity_aliases gea
                    ON gea.alias_norm = vg.gene_alias_norm
                    AND gea.group_id = :gg
                    AND gea.alias_type = 'code'
                    AND gea.xref_source = 'ENTREZ'
                """, {"gg": gene_group.id})

                self._exec(f"""
                    INSERT INTO entity_relationships (entity_1_id, entity_1_group_id, entity_2_id, entity_2_group_id, relationship_type_id, data_source_id, etl_package_id)
                    SELECT t.src_entity_id, :vg, t.dst_entity_id, :gg, :rt, t.data_source_id, :pkg
                    FROM {STG_VAR_GENE_IDS} t
                    LEFT JOIN entity_relationships er
                    ON er.entity_1_id = t.src_entity_id
                    AND er.entity_2_id = t.dst_entity_id
                    AND er.relationship_type_id = :rt
                    AND er.data_source_id = t.data_source_id
                    WHERE er.entity_1_id IS NULL
                """, {
                    "vg": self.entity_group,
                    "gg": gene_group.id,
                    "rt": relationship_type.id,
                    "pkg": self.package.id
                })

                # if self.debug_mode:
                self.logger.log(f"File {part_path} ingested in {time.time() - start_file:.2f}s", "INFO")

            except Exception as e:
                self.session.rollback()
                total_warnings += 1
                self.logger.log(f"❌ Error while loading {os.path.basename(part_path)}: {e}", "WARNING")
                # continue to next file

        # ----= Recreate indexes + read mode =----
        try:
            self.create_indexes(self.get_variant_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            # termine QUALQUER transação pendente antes de mudar PRAGMAs
            try:
                self.session.commit()
            except:
                self.session.rollback()
            # self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            self.logger.log(f"⚠️ Failed to switch DB to read mode or recreate indexes: {e}", "WARNING")

        if self.debug_mode:
            self.logger.log(f"Load process ran in {time.time() - start_total:.2f}s", "DEBUG")

        if total_warnings == 0:
            msg = f"✅ Loaded variants from {len(files_list)} file(s) (set-based path)."
            self.logger.log(msg, "SUCCESS")
            return True, msg
        else:
            msg = f"Loaded with {total_warnings} warning(s). Check logs."
            self.logger.log(msg, "WARNING")
            return True, msg

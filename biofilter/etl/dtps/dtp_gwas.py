import os
import time  # DEBUG MODE
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import text
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.db.models import (
    VariantGWAS
)  # noqa E501


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
        self.dtp_name = "dtp_gwas"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Download flat_files from EBI FTP.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(msg, "INFO")

        try:
            # Check compatibility
            self.check_compatibility()

            # source_url = self.data_source.source_url
            # Donwload more files to extract data
            base_url = "https://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/"
            files = [
                "gwas-catalog-associations.tsv",
                "gwas-efo-trait-mappings.tsv",
                # There are other files to use in future.
                ]  # noqa E501

            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)

            downloaded_files = []
            for f in files:
                url = base_url + f
                out_file = landing_path + "/" + f
                try:
                    self.logger.log(f"⬇️ Downloading {url} ...", "INFO")
                    r = requests.get(url, stream=True)
                    r.raise_for_status()
                    with open(out_file, "wb") as handle:
                        for chunk in r.iter_content(chunk_size=8192):
                            handle.write(chunk)
                    downloaded_files.append(str(out_file))
                except Exception as e:
                    msg = f"❌ Failed to download {f}: {e}"
                    self.logger.log(msg, "ERROR")
                    return False, msg, None

            # Compute file hash to Compound File
            file_path = landing_path + "/gwas-catalog-associations.tsv"
            current_hash = compute_file_hash(file_path)

            msg = f"✅ CheBI files downloaded to {landing_path}"
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
        """

        msg = f"⚙️ Starting transform of {self.data_source.name}..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        if self.debug_mode:
            start_total = time.time()

        try:
            input_path = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            output_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )
            output_path.mkdir(parents=True, exist_ok=True)

            # --- GWAS Catalog File ---
            input_file = input_path / "gwas-catalog-associations.tsv"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            dtype_map = {
                "CHR_ID": str,
                "CHR_POS": str,  # pode ser único valor ou lista separada por ";"
                "SNPS": str,
                "SNP_ID_CURRENT": str,  # idem, pode ter múltiplos
                "RISK ALLELE FREQUENCY": str,  # pode ser número, range ou "NR"
                "P-VALUE": str,  # pode ser "NR" ou notação científica
                "OR or BETA": str,
            }

            gwas_catalog = pd.read_csv(input_file, sep="\t", dtype=dtype_map, low_memory=False)

            # --- Normalize numeric fields ---
            gwas_catalog["CHR_POS"] = (
                gwas_catalog["CHR_POS"]
                .str.split(";")
                .str[0]  # pega o primeiro se múltiplo
                .pipe(pd.to_numeric, errors="coerce")
            )

            gwas_catalog["SNP_ID_CURRENT"] = (
                gwas_catalog["SNP_ID_CURRENT"]
                .str.split(";")
                .str[0]
                .pipe(pd.to_numeric, errors="coerce")
            )

            # P-VALUE em float (quando possível)
            gwas_catalog["P-VALUE"] = pd.to_numeric(gwas_catalog["P-VALUE"], errors="coerce")
            # gwas_catalog = gwas_catalog.rename(columns={"..": "..", "..": ".."})

            # --- Mapping Traits ---
            input_file = input_path / "gwas-efo-trait-mappings.tsv"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            gwas_trait_mapping = pd.read_csv(input_file, sep="\t")

            # Padronizar URIs -> IDs curtos (EFO, MONDO, HP)
            def normalize_uri(uri: str) -> str:
                # if pd.isna(uri):
                #     return None
                # if "EFO_" in uri:
                #     return "EFO:" + uri.split("EFO_")[-1]
                # if "MONDO_" in uri:
                #     return "MONDO:" + uri.split("MONDO_")[-1]
                # if "HP_" in uri:
                #     return "HP:" + uri.split("HP_")[-1]
                # return uri.split("/")[-1]  # fallback
                if pd.isna(uri) or not isinstance(uri, str):
                    return ""
                return uri.split("/")[-1].replace("_", ":").upper()

            gwas_trait_mapping["efo_id"] = gwas_trait_mapping["EFO URI"].map(normalize_uri)
            gwas_trait_mapping["parent_id"] = gwas_trait_mapping["Parent URI"].map(normalize_uri)

            gwas_trait_mapping = gwas_trait_mapping[
                ["Disease trait", "EFO term", "Parent term", "efo_id", "parent_id"]
            ]

            # --- Aggregate mappings per Disease trait ---
            agg_cols = {
                "EFO term": lambda x: list(set(x.dropna().astype(str))),
                # "EFO URI": lambda x: list(set(x.dropna().astype(str))),
                "Parent term": lambda x: list(set(x.dropna().astype(str))),
                # "Parent URI": lambda x: list(set(x.dropna().astype(str))),
                "efo_id": lambda x: list(set(x.dropna().astype(str))),
                "parent_id": lambda x: list(set(x.dropna().astype(str))),
            }
            gwas_trait_mapping_grouped = (
                gwas_trait_mapping.groupby("Disease trait", as_index=False)
                .agg(agg_cols)
            )

            # --- Merge with GWAS Catalog ---
            merged = gwas_catalog.merge(
                gwas_trait_mapping_grouped,
                left_on="DISEASE/TRAIT",
                right_on="Disease trait",
                how="left"   # keep all GWAS Catalog rows
            )

            # Garantir que colunas ausentes virem listas vazias
            for col in ["EFO term", "Parent term", "efo_id", "parent_id"]:
                merged[col] = merged[col].apply(lambda x: x if isinstance(x, list) else [])

            # # Keep only Columns to load
            merged = merged[
                [
                    # "DATE ADDED TO CATALOG",
                    "PUBMEDID",  # pubmed_id
                    # "FIRST AUTHOR",
                    # "DATE",
                    # "JOURNAL",
                    # "LINK ",
                    # "STUDY",
                    # "DISEASE/TRAIT",
                    "INITIAL SAMPLE SIZE",  # initial_sample_size
                    "REPLICATION SAMPLE SIZE",  # replication_sample_size
                    # "REGION",
                    "CHR_ID",  # chr_id
                    "CHR_POS",  # chr_pos
                    "REPORTED GENE(S)",  # reported_gene
                    "MAPPED_GENE",  # mapped_gene
                    # "UPSTREAM_GENE_ID",
                    # "DOWNSTREAM_GENE_ID",
                    # "SNP_GENE_IDS",
                    # "UPSTREAM_GENE_DISTANCE",
                    # "DOWNSTREAM_GENE_DISTANCE",
                    "STRONGEST SNP-RISK ALLELE",  # snp_risk_allele
                    "SNPS",  # snp_id
                    # "MERGED",
                    # "SNP_ID_CURRENT",
                    "CONTEXT",  # context
                    "INTERGENIC",  # intergenic
                    "RISK ALLELE FREQUENCY",  # risk_allele_frequency
                    "P-VALUE",  # p_value
                    "PVALUE_MLOG",  # pvalue_mlog
                    # "P-VALUE (TEXT)",
                    "OR or BETA",  # odds_ratio_beta
                    "95% CI (TEXT)",  # ci_text
                    # "PLATFORM [SNPS PASSING QC]",  # platform
                    # "CNV",  # cnv
                    "Disease trait",  # raw_trait
                    "EFO term",  # mapped_trait
                    "Parent term",  # parent_trait
                    "efo_id",  # mapped_trait_id
                    "parent_id",  # parent_trait_id
                ]
            ]

            column_map = {
                "PUBMEDID": "pubmed_id",
                "Disease trait": "raw_trait",
                "EFO term": "mapped_trait",
                "efo_id": "mapped_trait_id",
                "Parent term": "parent_trait",
                "parent_id": "parent_trait_id",
                "CHR_ID":  "chr_id",
                "CHR_POS": "chr_pos",
                "REPORTED GENE(S)": "reported_gene",
                "MAPPED_GENE":  "mapped_gene",
                "SNPS": "snp_id",
                "STRONGEST SNP-RISK ALLELE":  "snp_risk_allele",
                "RISK ALLELE FREQUENCY": "risk_allele_frequency",
                "CONTEXT": "context",
                "INTERGENIC": "intergenic",
                "P-VALUE": "p_value",
                "PVALUE_MLOG": "pvalue_mlog",
                "OR or BETA": "odds_ratio_beta",
                "95% CI (TEXT)": "ci_text",
                "INITIAL SAMPLE SIZE": "initial_sample_size",
                "REPLICATION SAMPLE SIZE": "replication_sample_size",
                "PLATFORM [SNPS PASSING QC]": "platform",
                "CNV": "cnv",
            }
            merged.rename(columns=column_map, inplace=True)

            # Save one master file
            merged.to_parquet(output_path / "master_data.parquet", index=False)

            if self.debug_mode:
                merged.to_csv(output_path / "master_data.csv", index=False)
                end_time = time.time() - start_total
                msg = str(
                    f"processed {len(merged)} records / Time Total: {end_time:.2f}s |"  # noqa E501
                )  # noqa E501
                self.logger.log(msg, "DEBUG")

            msg = f"✅ GWAS transformed into at {output_path}"  # noqa E501
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        """
        Load transformed GWAS Catalog into Biofilter3R schema.

        In this version we overwrite all records in `variant_gwas`.
        Entities not handled yet.
        """
        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

        if self.debug_mode:
            start_total = time.time()

        total_records = 0
        total_warnings = 0

        try:
            if not processed_dir:
                msg = "⚠️ processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg

            processed_path = os.path.join(
                processed_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            processed_file_name = os.path.join(processed_path, "master_data.parquet")

            if not os.path.exists(processed_file_name):
                msg = f"⚠️ File not found: {processed_file_name}"
                self.logger.log(msg, "ERROR")
                return False, msg

            df = pd.read_parquet(processed_file_name, engine="pyarrow")
            if df.empty:
                msg = "⚠️ DataFrame is empty."
                self.logger.log(msg, "ERROR")
                return False, msg

            df.fillna("", inplace=True)

        except Exception as e:
            msg = f"⚠️ Failed to read processed data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg


        # def safe_float(val):
        #     """
        #     Convert value to float if possible, else None.
        #     Handles NR, NA, empty strings, and invalid entries.
        #     """
        #     if pd.isna(val) or str(val).strip() in ["", "NR", "NA", "N/A", "nan"]:
        #         return None
        #     try:
        #         return float(val)
        #     except ValueError:
        #         return None

        # # Campos que devem virar float
        # float_fields = ["risk_allele_frequency", "odds_ratio_beta",
        #                 "p_value", "pvalue_mlog"]

        # for field in float_fields:
        #     if field in df.columns:
        #         df[field] = df[field].apply(safe_float).astype("float64")

        # # def flatten_list(val):
        # #     if isinstance(val, list):
        # #         return ";".join([str(v) for v in val if v])
        # #     return str(val) if pd.notna(val) else None

        # def flatten_list(val):
        #     if isinstance(val, (list, np.ndarray)):
        #         return ";".join([str(v) for v in val if v is not None and str(v) != ""])
        #     return str(val) if pd.notna(val) and str(val) != "" else None

        # # for col in ["mapped_trait", "mapped_trait_id", "parent_trait", "parent_trait_id"]:
        # #     if col in df.columns:
        # #         df[col] = df[col].apply(flatten_list)
        # for col in ["mapped_trait", "mapped_trait_id", "parent_trait", "parent_trait_id"]:
        #     if col in df.columns:
        #         df[col] = df[col].apply(flatten_list)

        # df["data_source_id"] = self.data_source.id
        # df["etl_package_id"] = self.package.id

        # Helpers
        SENTINELS = {"", "NA", "N/A", "na", "null", "None", "Nan", "nan"}

        def flatten_list(val):
            if isinstance(val, (list, np.ndarray)):
                vals = [str(v) for v in val if v is not None and str(v) != ""]
                return ";".join(vals) if vals else None
            s = None if pd.isna(val) else str(val)
            return s if s and s not in SENTINELS else None

        # Campos multivalorados -> string única (se existirem)
        for col in ["mapped_trait", "mapped_trait_id", "parent_trait", "parent_trait_id"]:
            if col in df.columns:
                df[col] = df[col].apply(flatten_list)

        # Numéricos: converta com coercion (''/NA -> NaN)
        if "risk_allele_frequency" in df.columns:
            df["risk_allele_frequency"] = pd.to_numeric(df["risk_allele_frequency"], errors="coerce")

        if "p_value" in df.columns:
            df["p_value"] = pd.to_numeric(df["p_value"], errors="coerce")

        if "pvalue_mlog" in df.columns:
            df["pvalue_mlog"] = pd.to_numeric(df["pvalue_mlog"], errors="coerce")

        # chr_pos é INTEGER no modelo
        if "chr_pos" in df.columns:
            df["chr_pos"] = pd.to_numeric(df["chr_pos"], errors="coerce").astype("Int64")  # pandas nullable int

        # chr_id é TEXT; normalize vazios para None
        if "chr_id" in df.columns:
            df["chr_id"] = df["chr_id"].astype(str)
            df.loc[df["chr_id"].isin(SENTINELS) | df["chr_id"].isna(), "chr_id"] = None

        # odds_ratio_beta é String(50) no modelo -> não converter para float
        # (se quiser, padronize para string curta)
        if "odds_ratio_beta" in df.columns:
            df["odds_ratio_beta"] = df["odds_ratio_beta"].astype(str)
            df.loc[df["odds_ratio_beta"].isin(SENTINELS), "odds_ratio_beta"] = None
            df["odds_ratio_beta"] = df["odds_ratio_beta"].str.slice(0, 50)

        # Campos textuais comuns: normalize vazios para None (sem quebrar numéricos)
        text_cols = [
            "pubmed_id","raw_trait","mapped_trait","mapped_trait_id","parent_trait",
            "parent_trait_id","reported_gene","mapped_gene","snp_id","snp_risk_allele",
            "context","intergenic","ci_text","initial_sample_size","replication_sample_size",
            "platform","cnv","notes"
        ]
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df.loc[df[col].isin(SENTINELS) | df[col].isna(), col] = None

        # IDs de sistema
        df["data_source_id"] = self.data_source.id
        df["etl_package_id"] = self.package.id

        # Converta todos os NaN/NaT restantes para None (p/ psycopg2)
        df = df.where(pd.notna(df), None)

        # # --- DB operations ---
        try:
            # 1. Clear old data
            self.session.execute(text("DELETE FROM variant_gwas"))
            self.session.commit()

        #     # 2. Prepare bulk objects
        #     records = df.to_dict(orient="records")
        #     objs = [VariantGWAS(**r) for r in records]

        #     # 3. Bulk insert
        #     self.session.bulk_save_objects(objs)
        #     self.session.commit()
        #     total_records = len(objs)

            records = df.to_dict(orient="records")
            
            # NOTE: Keep only 255 per record (Rethink next versions)
            for r in records:
                for k, v in r.items():
                    if isinstance(v, str) and len(v) > 255:
                        r[k] = v[:255]  # corta para 255 caracteres

            self.session.execute(VariantGWAS.__table__.insert(), records)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            msg = f"❌ Error inserting records: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        finally:
            # Recreate indexes
            try:
                self.create_indexes(self.get_variant_gwas_index_specs)
            except Exception as e:
                total_warnings += 1
                msg = f"⚠️ Failed to recreate indexes: {e}"
                self.logger.log(msg, "WARNING")

        # --- Wrap up ---
        end_time = time.time() - start_total if self.debug_mode else None
        msg = f"✅ Loaded {total_records} GWAS associations into variant_gwas"
        if end_time:
            msg += f" in {end_time:.2f}s"

        self.logger.log(msg, "INFO")
        return True, msg

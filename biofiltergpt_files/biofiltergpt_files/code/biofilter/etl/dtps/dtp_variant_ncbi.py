import os
import ast
import bz2
import glob
import pandas as pd
import __main__
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from sqlalchemy.exc import IntegrityError

# from sqlalchemy import text

from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.etl.mixins.variant_query_mixin import VariantQueryMixin

from biofilter.db.models.variants_models import (
    GenomeAssembly,
    Variant,
    VariantGeneRelationship,
)

# Worker function to suport transform in parallel
from biofilter.etl.dtps.worker_dbsnp import worker_dbsnp


class DTP(DTPBase, VariantQueryMixin):
    def __init__(
        self,
        logger=None,
        datasource=None,
        etl_process=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.data_source = datasource
        self.etl_process = etl_process
        self.session = session
        self.use_conflict_csv = use_conflict_csv
        self.conflict_mgr = ConflictManager(session, logger)

        # DTP versioning
        self.dtp_name = "dtp_variant_ncbi"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "3.0.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Downloads the file from the dbSNP JSON release and stores it locally
        only if it doesn't exist or if the MD5 has changed.
        """
        msg = f"Starting extraction of {self.data_source.name} data..."

        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url
        if force_steps:
            last_hash = ""
            msg = "Ignoring hash check."
            self.logger.log(msg, "WARNING")
        else:
            last_hash = self.etl_process.raw_data_hash

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

            if not current_hash:
                msg = f"Failed to retrieve MD5 from {url_md5}"
                self.logger.log(msg, "WARNING")
                return False, msg, None

            # Compare current hash and last processed hash
            if current_hash == last_hash:
                msg = f"No change detected in {source_url}"
                self.logger.log(msg, "INFO")
                return False, msg, current_hash

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
    def transform(self, raw_path, processed_path):

        self.logger.log(
            f"🔧 Transforming the {self.data_source.name} data ...", "INFO"
        )  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        # INPUT DATA
        input_file = self.get_raw_file(raw_path)
        if not input_file.exists():
            msg = f"❌ Input file not found: {input_file}."
            msg += " Consider running the extract() step or checking the source URL."  # noqa: E501
            self.logger.log(msg, "ERROR")
            return None, False, msg

        # OUTPUT DATA
        output_dir = self.get_path(processed_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        # Clean only batch CSVs that follow the expected naming
        for f in output_dir.iterdir():
            if f.name.startswith("processed_part_") and f.name.endswith(
                ".parquet"
            ):  # noqa: E501
                f.unlink()

        # VARIABLES
        # Transfer to interface this parameters
        batch_size: int = 200_000
        max_workers: int = 10

        futures = []
        batch = []
        batch_id = 0

        try:
            # Get GenomeAssembly IDs List
            assembly_map = {
                a.accession: str(a.id)
                for a in self.session.query(GenomeAssembly)  # noqa: E501
            }

            with bz2.open(
                input_file, "rt", encoding="utf-8"
            ) as f, ProcessPoolExecutor(  # noqa: E501
                max_workers=max_workers
            ) as executor:  # noqa: E501
                if __name__ == "__main__" or (
                    hasattr(__main__, "__file__") and not hasattr(sys, "ps1")
                ):

                    for line in f:
                        batch.append(line)
                        if len(batch) >= batch_size:
                            futures.append(
                                executor.submit(
                                    worker_dbsnp,
                                    batch.copy(),
                                    batch_id,
                                    output_dir,
                                    assembly_map,
                                )
                            )  # noqa: E501
                            batch.clear()
                            batch_id += 1

                    if batch:
                        futures.append(
                            executor.submit(
                                worker_dbsnp,
                                batch.copy(),
                                batch_id,
                                output_dir,
                                assembly_map,
                            )
                        )  # noqa: E501

                    for future in as_completed(futures):
                        future.result()
                else:
                    msg = "⚠️ Skipping multiprocessing: not in __main__ context."  # noqa: E501
                    self.logger.log(msg, "WARNING")

            msg = f"✅ Processing completed with {len(futures)} batches."
            self.logger.log(msg, "INFO")
            return None, True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_path=None, chunk_size=100_000):

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        try:
            index_specs = [
                ("variants", ["data_source_id"]),
                ("variants", ["variant_id"]),
                ("variants", ["chromosome"]),
                ("variant_gene_relationships", ["data_source_id"]),
                ("variant_gene_relationships", ["variant_id"]),
                ("variant_gene_relationships", ["gene_id"]),
                ("variant_gene_relationships", ["variant_id", "gene_id"]),
            ]
            self.db_write_mode()
            self.drop_indexes(index_specs)
        except Exception as e:
            msg = f"⚠️ Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        # Variables
        total_variants = 0
        total_warnings = 0
        load_status = False

        # 🚨 Garante que self.data_source é válido na sessão atual
        # self.data_source = self.session.merge(self.data_source)
        data_source_id = self.data_source.id

        if df is None:
            if not processed_path:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_variants, load_status, msg

            processed_path = self.get_path(processed_path)
            csv_files = sorted(
                glob.glob(str(processed_path / "processed_part_*.parquet"))
            )

            if not csv_files:
                msg = f"No part files found in {processed_path}"
                self.logger.log(msg, "ERROR")
                return total_variants, load_status, msg

            self.logger.log(
                f"📄 Found {len(csv_files)} part files to load", "INFO"
            )  # noqa: E501

        # Drop all variants x Genes from Data Source
        # NOTE: If we change the schema, review it!
        try:
            self.session.query(VariantGeneRelationship).filter_by(
                data_source_id=data_source_id
            ).delete()
            self.session.query(Variant).filter_by(
                data_source_id=data_source_id
            ).delete()

            self.session.commit()
            msg = "🗑️ Previous records deleted for this data source"
            self.logger.log(msg, "INFO")

        except Exception as e:
            self.session.rollback()
            msg = f"❌ Failed to delete previous records: {e}"
            self.logger.log(msg, "ERROR")
            return total_variants, load_status, msg

        # Process and ingest variants and gene links from file
        for csv_file in csv_files:
            try:
                self.logger.log(f"📂 Processing {csv_file}", "INFO")

                # Read file
                df = pd.read_parquet(
                    csv_file,
                    columns=[
                        "rs_id",
                        "position_base_1",
                        "assembly_id",
                        "allele",
                        "allele_type",
                        "gene_ids",
                    ],
                )

                df["ref"] = ""
                df["alt"] = ""

                # Prepare Variants DataFrame
                df_ref = df[df["allele_type"] == "ref"].copy()
                df_ref = df_ref[
                    ["rs_id", "position_base_1", "assembly_id", "allele"]
                ].drop_duplicates("rs_id")

                df_alt = (
                    df[df["allele_type"] == "sub"]
                    .groupby("rs_id")["allele"]
                    .agg(lambda alleles: "/".join(sorted(set(alleles))))
                    .reset_index()
                    .rename(columns={"allele": "alt"})
                )
                df_ref["rs_id"] = df_ref["rs_id"].astype(str)
                df_alt["rs_id"] = df_alt["rs_id"].astype(str)
                df_variants = df_ref.merge(df_alt, on="rs_id", how="left")
                df_variants["alt"] = df_variants["alt"].fillna("")
                df_variants = df_variants.dropna(
                    subset=["assembly_id", "position_base_1"]
                )  # noqa: E501
                df_variants["assembly_id"] = df_variants["assembly_id"].astype(
                    int
                )  # noqa: E501
                df_variants["position_base_1"] = df_variants["position_base_1"].astype(
                    int
                )  # noqa: E501

                variants_to_insert = [
                    Variant(
                        variant_id=row["rs_id"],
                        position=row["position_base_1"],
                        assembly_id=row["assembly_id"],
                        chromosome=row["assembly_id"],
                        ref=row["allele"],
                        alt=row["alt"],
                        data_source_id=data_source_id,
                    )
                    for _, row in df_variants.iterrows()
                ]

                try:
                    self.session.bulk_save_objects(variants_to_insert)
                    self.session.commit()
                    total_variants += len(variants_to_insert)
                except IntegrityError as e:
                    self.session.rollback()
                    total_warnings += 1
                    msg = f"Integrity error in {csv_file}: {str(e)}"
                    self.logger.log(msg, "ERROR")
                    # Go to next file
                    continue

                # Variants x Gene links
                df_links = df[
                    df["gene_ids"].apply(
                        lambda x: hasattr(x, "__len__") and len(x) > 0
                    )  # noqa: E501
                ].copy()  # noqa: E501
                df_links = df_links[["rs_id", "gene_ids"]].drop_duplicates(
                    "rs_id"
                )  # noqa: E501
                df_links["gene_ids"] = df_links["gene_ids"].apply(
                    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
                )
                df_links = df_links.explode("gene_ids")
                df_links["gene_ids"] = df_links["gene_ids"].astype(int)
                df_links["rs_id"] = df_links["rs_id"].astype(str)

                links_to_insert = [
                    VariantGeneRelationship(
                        gene_id=row["gene_ids"],
                        variant_id=row["rs_id"],
                        data_source_id=data_source_id,
                    )
                    for _, row in df_links.iterrows()
                ]

                try:
                    self.session.bulk_save_objects(links_to_insert)
                    self.session.commit()
                except IntegrityError as e:
                    self.session.rollback()
                    total_warnings += 1
                    msg = f"Integrity error in {csv_file} for gene-variant links: {str(e)}"  # noqa E501
                    self.logger.log(msg, "ERROR")
                    # Go to next file
                    continue

            except Exception as e:
                total_warnings += 1
                msg = f"Unexpected error processing {csv_file}: {e}"
                self.logger.log(msg, "ERROR")
                # Go to next file
                continue

        try:
            # Stating Indexs
            self.create_indexes(index_specs)
            self.db_read_mode()
            load_status = True
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            load_status = True  # TODO: Think \ keep as success!?!

        if total_warnings == 0:
            msg = f"✅ Loaded {total_variants} variants from {len(csv_files)} CSV chunks."  # noqa: E501
            self.logger.log(msg, "SUCCESS")
        else:
            msg = f"Loaded {total_variants} with {total_warnings} warning to analysis in log file"  # noqa: E501
            self.logger.log(msg, "WARNING")

        return total_variants, load_status, msg

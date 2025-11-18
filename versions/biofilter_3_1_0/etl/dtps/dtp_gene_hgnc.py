import os
import json
import time  # DEBUG MODE
import requests
import pandas as pd
from pathlib import Path
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.db.models import (
    CurationConflict,
    ConflictStatus,
    OmicStatus,
)  # noqa E501


class DTP(DTPBase, EntityQueryMixin, GeneQueryMixin):
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
        self.dtp_name = "dtp_gene_hgnc"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Download data from the HGNC API and stores it locally.
        Also computes a file hash to track content versioning.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(
            msg,
            "INFO",  # noqa: E501
        )  # noqa: E501

        try:

            # Check Compartibility
            self.check_compatibility()

            source_url = self.data_source.source_url
            # if force_steps:
            #     last_hash = ""
            #     msg = "Ignoring hash check, forcing download"
            #     self.logger.log(msg, "WARNING")
            # else:
            #     last_hash = self.package.raw_data_hash

            # Landing directory
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            file_path = os.path.join(landing_path, "hgnc_data.json")

            # Download the file
            msg = f"⬇️  Fetching JSON from API: {source_url} ..."
            self.logger.log(msg, "INFO")

            headers = {"Accept": "application/json"}
            response = requests.get(source_url, headers=headers)

            if response.status_code != 200:
                msg = f"Failed to fetch data from HGNC: {response.status_code}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            with open(file_path, "w") as f:
                f.write(response.text)

            # Compute hash and compare
            current_hash = compute_file_hash(file_path)
            # if current_hash == last_hash:
            #     msg = f"No change detected in {file_path}"
            #     self.logger.log(msg, "INFO")
            #     return False, msg, current_hash

            # Finish block
            msg = f"✅ HGNC file downloaded to {file_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    # def transform(self, raw_path, processed_path):
    def transform(self, raw_dir: str, processed_dir: str):

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
            input_file = input_path / "hgnc_data.json"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg

            # Output files paths
            output_file_master = output_path / "master_data"

            # Delete existing files if they exist (both .csv and .parquet)
            for f in [output_file_master]:
                for ext in [".csv", ".parquet"]:
                    target_file = f.with_suffix(ext)
                    if target_file.exists():
                        target_file.unlink()
                        self.logger.log(
                            f"🗑️  Removed existing file: {target_file}", "INFO"
                        )  # noqa E501

        except Exception as e:
            msg = f"❌ Error constructing paths: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        try:
            # LOAD JSON
            with open(input_file, "r") as f:
                data = json.load(f)

            df = pd.DataFrame(data["response"]["docs"])

            if self.debug_mode:
                df.to_csv(
                    output_file_master.with_suffix(".csv"), index=False
                )  # noqa: E501

            df.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )  # noqa: E501

            msg = f"✅ HGNC data transformed and saved at {output_file_master}"  # noqa: E501
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            return False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        """
        TODO: CREATE DOCSTRING
        """

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",
        )

        # CHECK COMPARTIBILITY
        self.check_compatibility()

        # VARIABLES TO LOAD PROCESS
        if self.debug_mode:
            start_total = time.time()
            prev_time = start_total

        total_gene = 0  # not considered conflict genes
        total_warnings = 0
        # Gene List with resolved conflicts to be processed later
        genes_with_solved_conflict = []
        # Gene List with pending conflicts (to be processed later)
        genes_with_pending_conflict = []

        # ALIASES MAP FROM PROCESS DATA FIELDS
        self.alias_schema = {
            "symbol": ("symbol", "HGNC", True),
            "hgnc_id": ("code", "HGNC", None),
            "ensembl_gene_id": ("code", "ENSEMBL", None),
            "entrez_id": ("code", "ENTREZ", None),
            "ucsc_id": ("code", "UCSC", None),
            "name": ("synonym", "HGNC", None),
            "prev_symbol": ("prev_symbol", "HGNC", None),
            "prev_name": ("synonym", "HGNC", None),
            "alias_symbol": ("symbol", "HGNC", None),
            "alias_name": ("synonym", "HGNC", None),
        }

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

            # Setting files names
            conflict_file_name = processed_path + "/master_data_conflict.csv"
            processed_file_name = processed_path + "/master_data.parquet"

            # Process Pre Load Genes with Conflict?
            if self.use_conflict_csv:
                if not os.path.exists(conflict_file_name):
                    msg = f"⚠️  File not found: {conflict_file_name}"
                    self.logger.log(msg, "ERROR")
                    return False, msg  # ⧮ Leaving with ERROR

                df = pd.read_csv(processed_path, dtype=str)

            # Read Processed Gene Master Data
            else:
                if not os.path.exists(processed_file_name):
                    msg = f"⚠️  File not found: {processed_file_name}"
                    self.logger.log(msg, "ERROR")
                    return False, msg  # ⧮ Leaving with ERROR
                df = pd.read_parquet(processed_file_name, engine="pyarrow")

        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # GET ENTITY GROUP ID AND OMICS STATUS
        try:
            self.get_entity_group("Genes")
        except Exception as e:
            msg = f"Error on DTP to get Entity Group: {e}"
            return False, msg  # ⧮ Leaving with ERROR

        # TODO: Better Manage Control to Omics Status / Now all will be Active
        try:
            gene_status = (
                self.session.query(OmicStatus).filter_by(name="active").first()
            )
        except Exception as e:
            msg = f"Error on DTP to get the Omics Status: {e}"
            return False, msg  # ⧮ Leaving with ERROR
        if not gene_status:
            msg = "⚠️  OmicStatus Active not found."
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # PRELOAD THE HGNC IDS WITH RESOLVED CONFLICTS
        # TODO: 🚧 Conflict was desable after schame changes in 3.0.1 🚧
        resolved_genes = {
            c.identifier
            for c in self.session.query(CurationConflict).filter_by(
                entity_type="gene", status=ConflictStatus.resolved
            )
        }

        # SET DB AND DROP INDEXES
        try:
            self.db_write_mode()
            self.drop_indexes(self.get_gene_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # NTERACTION WITH EACH MASTER DATA ROW
        # Row = HGNC Gene
        for _, row in df.iterrows():

            # Define the Gene Master
            gene_master = row.get("symbol")  # v3.0.1
            # gene_master = row.get("hgnc_id")  # v3.0.0
            if row.get("status") == "Approved":
                # is_deactive = False
                is_active = True
            else:
                # is_deactive = True
                is_active = False

            # NOTE: Use to debugging
            if gene_master == "FACL1":
                pass

            if not gene_master:
                msg = f"⚠️  Gene Master not found in row: {row}"
                self.logger.log(msg, "WARNING")
                total_warnings += 1
                # TODO: Add in ETLLOG Model
                continue

            # If in debug mode, show times
            if self.debug_mode:
                current_time = time.time()
                elapsed_total = current_time - start_total
                elapsed_since_last = (current_time - prev_time) * 1000
                prev_time = current_time
                msg = str(
                    f"{row.name} - {gene_master} | Total: {elapsed_total:.2f}s | Δ: {elapsed_since_last:.0f}ms"  # noqa E501
                )  # noqa E501
                self.logger.log(msg, "DEBUG")

            # Skip genes with resolved conflicts in lote
            # TODO: 🚧 Conflict was desable after schame changes in 3.0.1 🚧
            if gene_master in resolved_genes:
                self.logger.log(
                    f"Gene '{gene_master}' skipped, conflict already resolved",
                    "INFO",  # noqa E501
                )
                genes_with_solved_conflict.append(row)
                continue

            # --- ALIASES STRUCTURE ---

            # Create a dict of Aliases
            alias_dict = self.build_alias(row)

            # Only Primary Name
            is_primary_alias = next(
                (a for a in alias_dict if a.get("is_primary")), None
            )
            # Only Aliases Names
            not_primary_alias = [
                a for a in alias_dict if a != is_primary_alias
            ]  # noqa E501

            # --- CREATE THE ENTITY RECORDS ---

            # Add or Get Entity
            entity_id, _ = self.get_or_create_entity(
                name=is_primary_alias["alias_value"],
                group_id=self.entity_group,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
                alias_type=is_primary_alias["alias_type"],
                xref_source=is_primary_alias["xref_source"],
                alias_norm=is_primary_alias["alias_norm"],
                is_active=is_active,
            )

            # Add or Get EntityName
            self.get_or_create_entity_name(
                group_id=self.entity_group,
                entity_id=entity_id,
                aliases=not_primary_alias,
                is_active=is_active,
                data_source_id=self.data_source.id,  # noqa E501
                package_id=self.package.id,
            )

            # -- CREATE THE GENES RECORDS ---

            # Define data values
            chromosome = self.extract_chromosome(row.get("location"))
            locus_group_name = row.get("locus_group")
            locus_type_name = row.get("locus_type")
            region_label = row.get("location")
            # TODO: How to get Start and End information in HGNC Source System?
            start = row.get("start")
            end = row.get("end")

            # --> Locus Groups
            locus_group_instance, status = self.get_or_create_locus_group(
                name=locus_group_name,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
            )  # noqa: E501
            if not status:
                msg = f"⚠️  Error on Locus Group to: {gene_master}"
                self.logger.log(msg, "WARNING")
                total_warnings += 1
                continue  # TODO: Add in ETLLOG Model

            # --> Locus Types
            locus_type_instance, status = self.get_or_create_locus_type(
                name=locus_type_name,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
            )  # noqa: E501
            if not status:
                msg = f"⚠️  Error on Locus Type to: {gene_master}"
                self.logger.log(msg, "WARNING")
                total_warnings += 1
                continue  # TODO: Add in ETLLOG Model

            # --> Regions
            region_instance, status = self.get_or_create_genomic_region(
                label=region_label,
                chromosome=chromosome,
                start=start,
                end=end,
                data_source_id=self.data_source.id,
                package_id=self.package.id,
            )  # noqa: E501
            if not status:
                msg = f"⚠️  Error on Region to: {gene_master}"
                self.logger.log(msg, "WARNING")
                total_warnings += 1
                continue  # TODO: Add in ETLLOG Model

            group_names_list = self.parse_gene_groups(row.get("gene_group"))

            gene, conflict_flag, status = self.get_or_create_gene(
                status_id=int(gene_status.id),
                symbol=gene_master,
                hgnc_status=row.get("status"),
                entity_id=entity_id,
                chromosome=chromosome,
                data_source_id=self.data_source.id,
                locus_group=locus_group_instance,
                locus_type=locus_type_instance,
                gene_group_names=group_names_list,
                package_id=self.package.id,
            )

            # TODO: 🚧 Conflict was desable after schame changes in 3.0.1 🚧
            if conflict_flag:
                msg = f"Gene '{gene_master}' has conflicts"
                self.logger.log(msg, "WARNING")
                # Add to the list of genes with resolved conflicts
                genes_with_pending_conflict.append(row)

            if gene is not None:
                total_gene += 1

                location, status = self.get_or_create_gene_location(
                    gene=gene,
                    chromosome=chromosome,
                    start=row.get("start"),
                    end=row.get("end"),
                    strand=row.get("strand"),
                    region=region_instance,
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                )
                if not status:
                    msg = f"⚠️  Error on Gene Location insert to: {gene_master}"  # noqa E501
                    self.logger.log(msg, "WARNING")
                    msg = f"⮐  Applied rollback to {gene_master} gene"
                    self.logger.log(msg, "WARNING")
                    total_warnings += 1
                    continue  # TODO: Add in ETLLOG Model

            # Check if location was created successfully
            if not location:
                msg = f"⚠️  Failed to create Location for gene {gene_master}"
                self.logger.log(msg, "WARNING")

        #  ---> PROCESSED ALL PROCESS DATA ROWS

        # POST INGESTION TASK

        # TODO: 🚧 Conflict was desable after schame changes in 3.0.1 🚧
        # -- PROCESS THE PENDING CONFLICTS ---

        if genes_with_pending_conflict:
            conflict_df = pd.DataFrame(genes_with_pending_conflict)

            if os.path.exists(conflict_file_name):
                msg = f"⚠️ Overwriting existing conflict file: {conflict_file_name}"  # noqa: E501
                self.logger.log(msg, "WARNING")

            conflict_df.to_csv(conflict_file_name, index=False)
            msg = f"✅ Saved {len(conflict_df)} gene conflicts to {conflict_file_name}"  # noqa: E501
            self.logger.log(msg, "INFO")

            # TODO: 🧠 Additional suggestion (optional)
            # Generalize this behavior into a helper like
            # save_pending_conflicts(entity_type: str, rows: List[Dict], path: str)  # noqa: E501
            # to make it reusable for SNPs, Proteins, etc.

        # post-processing the resolved conflicts
        for row in genes_with_solved_conflict:
            msg = f"Check and apply conflict rules to  {row.get('hgnc_id')}"
            self.logger.log(msg, "INFO")

            # Apply conflict resolution
            self.conflict_mgr.apply_resolution(row)

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(self.get_gene_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to read mode or create indexes: {e}"  # noqa E501
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        #  ---> LOAD FINISHED WITH SUCCESS
        if total_warnings != 0:
            msg = f"⚠️  {total_warnings} Warning(s) to analysis in the LOG FILE"  # noqa E501
            self.logger.log(msg, "WARNING")

        msg = f"🧬 Loaded {total_gene} genes into database"
        self.logger.log(msg, "INFO")

        return True, msg

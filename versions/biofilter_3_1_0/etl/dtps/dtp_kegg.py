import os
import pandas as pd
from pathlib import Path
import requests
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models import (
    PathwayMaster,
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
        self.dtp_name = "dtp_kegg"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Downloads KEGG Pathway data. Uses the hash of 'KEGGPathways.txt' as
        reference. Only proceeds with full extraction if the hash has changed.
        """

        msg = f"⬇️ Starting extraction of {self.data_source.name} data..."

        self.logger.log(
            msg,
            "INFO",  # noqa: E501
        )  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url

        try:
            # Prepare download path
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            file_path = os.path.join(landing_path, "kegg_pathways.txt")

            # Download the OBO file
            msg = f"⬇️  Fetching txt from URL: {source_url} ..."
            self.logger.log(msg, "INFO")

            headers = {
                "Accept": "text/plain"
            }  # Optional, KEGG responds with TXT anyway
            response = requests.get(source_url, headers=headers)
            # Case if the file grows too large, we can use streaming
            # response = requests.get(source_url, headers=headers, stream=True)

            if response.status_code != 200:
                msg = f"Failed to fetch data from KEGG: {response.status_code}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(response.text)

            # Compute hash
            current_hash = compute_file_hash(file_path)

            msg = f"✅ GO file downloaded to {file_path}"
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
        Transforms the KEGG raw_pathways.txt file into a structured CSV.
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
            input_file = input_path / "kegg_pathways.txt"
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

        # Process source file to Biofilter format
        try:
            rows = []
            with open(input_file, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) != 2:
                        continue
                    pid = parts[0].replace("path:", "")
                    desc = parts[1]
                    rows.append((pid, desc))

            df = pd.DataFrame(rows, columns=["pathway_id", "description"])
            df.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )  # noqa: E501

            if self.debug_mode:
                df.to_csv(output_file_master.with_suffix(".csv"), index=False)  # noqa: E501

            self.logger.log(
                f"✅ KEGG pathways transformed to CSV at {output_path}", "INFO"
            )
            return True, f"{len(df)} pathways processed"

        except Exception as e:
            msg = f"❌ Transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",  # noqa E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_pathways = 0
        total_warnings = 0

        # ALIASES MAP FROM PROCESS DATA FIELDS
        self.alias_schema = {
            "pathway_id": ("code", "KEGG", True),
            "description": ("name", "KEGG", None),
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
            processed_file_name = processed_path + "/master_data.parquet"

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
            self.db_write_mode()
            self.drop_indexes(self.get_pathway_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # GET ENTITY GROUP ID AND OMICS STATUS
        try:
            self.get_entity_group("Pathways")
        except Exception as e:
            msg = f"Error on DTP to get Entity Group: {e}"
            return False, msg  # ⧮ Leaving with ERROR

        # RUN LOAD BY ROW
        try:
            for _, row in df.iterrows():

                pathway_master = row["pathway_id"]
                pathway_name = row["description"]

                if not pathway_master:
                    msg = f"Pathway Master not found in row: {row}"
                    self.logger.log(msg, "WARNING")
                    continue

                # --- ALIASES STRUCTURE ---
                # Create a dict of Aliases
                alias_dict = self.build_alias(row)
                # Only primary Name
                is_primary_alias = next(
                    (a for a in alias_dict if a.get("is_primary")), None
                )
                # Only Aliases Names
                not_primary_alias = [
                    a for a in alias_dict if a != is_primary_alias
                ]  # noqa E501

                # Add or Get Entity
                entity_id, _ = self.get_or_create_entity(
                    name=is_primary_alias["alias_value"],
                    group_id=self.entity_group,
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    alias_type=is_primary_alias["alias_type"],
                    xref_source=is_primary_alias["xref_source"],
                    alias_norm=is_primary_alias["alias_norm"],
                    is_active=True,
                )

                self.get_or_create_entity_name(
                    group_id=self.entity_group,
                    entity_id=entity_id,
                    aliases=not_primary_alias,
                    is_active=True,
                    data_source_id=self.data_source.id,  # noqa E501
                    package_id=self.package.id,
                )

                # Check if the pathway already exists
                existing_pathway = (
                    self.session.query(PathwayMaster)
                    .filter_by(
                        pathway_id=pathway_master,
                    )
                    .first()
                )

                # Create new if it does not exist
                if not existing_pathway:
                    pathway = PathwayMaster(
                        entity_id=entity_id,
                        pathway_id=pathway_master,
                        description=pathway_name,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    )

                    self.session.add(pathway)
                    self.session.commit()

                    total_pathways += 1

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(self.get_pathway_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"📥 Total Pathways: {total_pathways}"  # noqa E501  # noqa E501
        self.logger.log(msg, "INFO")

        return True, msg

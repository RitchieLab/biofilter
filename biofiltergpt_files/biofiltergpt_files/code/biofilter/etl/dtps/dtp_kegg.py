import os
import pandas as pd
from pathlib import Path
import requests
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models.entity_models import (
    EntityGroup,
)  # noqa E501
from biofilter.db.models.pathway_models import Pathway
from biofilter.etl.mixins.base_dtp import DTPBase


class DTP(DTPBase, EntityQueryMixin):
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

        # DTP versioning
        self.dtp_name = "dtp_kegg"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "3.0.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
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
        if force_steps:
            last_hash = ""
            msg = "Ignoring hash check, forcing download"
            self.logger.log(msg, "WARNING")
        else:
            last_hash = self.etl_process.raw_data_hash

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
            if current_hash == last_hash:
                msg = f"No change detected in {file_path}"
                self.logger.log(msg, "INFO")
                return False, msg, current_hash

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
                return None, False, msg

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
            return None, False, msg

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
            df.to_csv(output_file_master.with_suffix(".csv"), index=False)  # noqa: E501
            df.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )  # noqa: E501

            self.logger.log(
                f"✅ KEGG pathways transformed to CSV at {output_path}", "INFO"
            )
            return df, True, f"{len(df)} pathways processed"

        except Exception as e:
            msg = f"❌ Transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):

        msg = f"📥 Loading {self.data_source.name} data into the database..."

        self.logger.log(
            msg,
            "INFO",  # noqa E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_pathways = 0
        total_warnings = 0
        load_status = False

        data_source_id = self.data_source.id

        # Set DB and drop indexes
        try:
            index_specs = [
                ("pathways", ["entity_id"]),
                # Já possui index=True + unique=True, mas bom explicitar
                ("pathways", ["pathway_id"]),
                ("pathways", ["data_source_id"]),
            ]

            index_specs_entity = [
                # Entity
                ("entities", ["group_id"]),
                ("entities", ["has_conflict"]),
                ("entities", ["is_deactive"]),
                # EntityName
                ("entity_names", ["entity_id"]),
                ("entity_names", ["name"]),
                ("entity_names", ["data_source_id"]),
                ("entity_names", ["data_source_id", "name"]),
                ("entity_names", ["data_source_id", "entity_id"]),
                ("entity_names", ["entity_id", "is_primary"]),
                # EntityRelationship
                ("entity_relationships", ["entity_1_id"]),
                ("entity_relationships", ["entity_2_id"]),
                ("entity_relationships", ["relationship_type_id"]),
                ("entity_relationships", ["data_source_id"]),
                (
                    "entity_relationships",
                    ["entity_1_id", "relationship_type_id"],
                ),  # noqa E501
                (
                    "entity_relationships",
                    ["entity_1_id", "entity_2_id", "relationship_type_id"],
                ),  # noqa E501
                # EntityRelationshipType
                ("entity_relationship_types", ["code"]),
            ]

            self.db_write_mode()
            # self.drop_indexes(index_specs) # Keep indices to improve checks
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️ Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        try:

            if df is None:
                if not processed_dir:
                    msg = "Either 'df' or 'processed_path' must be provided."
                    self.logger.log(msg, "ERROR")
                    return total_pathways, load_status, msg

                processed_path = self.get_path(processed_dir)
                # processed_data = str(processed_path / "master_data.csv")
                processed_data = str(processed_path / "master_data.parquet")

                if not os.path.exists(processed_data):
                    msg = f"File not found: {processed_data}"
                    self.logger.log(msg, "ERROR")
                    return total_pathways, load_status, msg

                self.logger.log(
                    f"📥 Reading data in chunks from {processed_data}", "INFO"
                )  # noqa E501

                # df = pd.read_csv(processed_data, dtype=str)
                df = pd.read_parquet(processed_data, engine="pyarrow")

            # Get Entity Group ID
            if not hasattr(self, "entity_group") or self.entity_group is None:
                group = (
                    self.session.query(EntityGroup)
                    .filter_by(name="Pathways")
                    .first()  # noqa: E501
                )  # noqa: E501
                if not group:
                    msg = "EntityGroup 'Pathways' not found in the database."
                    self.logger.log(msg, "ERROR")
                    return total_pathways, load_status
                    # raise ValueError(msg)
                self.entity_group = group.id
                msg = f"EntityGroup ID for 'Pathways' is {self.entity_group}"
                self.logger.log(msg, "DEBUG")

            # IMPORTANT: We will not use conflict manager here
            # Interaction to each Reactome Pathway
            for _, row in df.iterrows():

                pathway_master = row["pathway_id"]
                pathway_name = row["description"]
                # name = pathway_name.split(" - ")[
                #     0
                # ].strip()

                if not pathway_master:
                    msg = f"Pathway Master not found in row: {row}"
                    self.logger.log(msg, "WARNING")
                    continue

                # Add or Get Entity
                entity_id, _ = self.get_or_create_entity(
                    name=pathway_master,
                    group_id=self.entity_group,
                    data_source_id=self.data_source.id,
                )

                # Add or Get Entity Name
                self.get_or_create_entity_name(
                    entity_id, pathway_name, data_source_id=self.data_source.id
                )

                # Check if the pathway already exists
                existing_pathway = (
                    self.session.query(Pathway)
                    .filter_by(
                        pathway_id=pathway_master,
                    )
                    .first()
                )

                # Create new if it does not exist
                if not existing_pathway:
                    pathway = Pathway(
                        entity_id=entity_id,
                        pathway_id=pathway_master,
                        description=pathway_name,
                        data_source_id=data_source_id,
                    )

                    self.session.add(pathway)
                    self.session.commit()

                    total_pathways += 1

            # msg = f"✅ Relations loaded: {total_pathways}"  # noqa: E501
            # self.logger.log(msg, "INFO")
            # return total_pathways, True, msg

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return 0, False, msg

        # Set DB to Read Mode and Create Index
        try:
            # Drop Indexs
            self.drop_indexes(index_specs)
            self.drop_indexes(index_specs_entity)
            # Stating Indexs
            self.create_indexes(index_specs)
            self.create_indexes(index_specs_entity)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        load_status = True

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"📥 Total Pathways: {total_pathways}"  # noqa E501  # noqa E501
        self.logger.log(msg, "INFO")

        return total_pathways, True, msg

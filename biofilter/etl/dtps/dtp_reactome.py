import os
import pandas as pd
from pathlib import Path
import zipfile
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models.entity_models import (
    EntityGroup,
)  # noqa E501
from biofilter.db.models.pathway_models import Pathway
from biofilter.etl.mixins.base_dtp import DTPBase

# TODO: Separa os processos de dados mestres e dados de relacionamentos


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

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Downloads Reactome data. Uses the hash of 'ReactomePathways.txt' as
        reference. Only proceeds with full extraction if the hash has changed.
        """

        msg = f"⬇️ Starting extraction of {self.data_source.name} data..."

        self.logger.log(
            msg,
            "INFO",  # noqa: E501
        )  # noqa: E501

        source_url = self.data_source.source_url
        files_to_download = [
            "ReactomePathways.txt",
            "ReactomePathwaysRelation.txt",
            "ReactomePathways.gmt.zip",
            "Ensembl2Reactome.txt",
            "UniProt2Reactome.txt",
        ]
        if force_steps:
            last_hash = ""
            msg = "Ignoring hash check."
            self.logger.log(msg, "WARNING")
        else:
            last_hash = self.etl_process.raw_data_hash

        try:
            # Landing directory
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)

            # Step 1: Download only the main file
            main_file = "ReactomePathways.txt"
            file_url = f"{source_url}{main_file}"
            file_path = os.path.join(landing_path, main_file)

            status, msg = self.http_download(file_url, landing_path)
            if not status:
                return False, msg, None

            # Step 2: Compute hash and compare
            current_hash = compute_file_hash(file_path)
            if current_hash == last_hash:
                msg = f"No change detected in {main_file}"  # noqa: E501
                self.logger.log(msg, "INFO")
                return False, msg, current_hash  # Skip further downloads

            # Step 3: Download the remaining files
            for file_name in files_to_download:
                if file_name == main_file:
                    continue  # Already downloaded

                file_url = f"{source_url}{file_name}"

                # Download the file
                status, msg = self.http_download(file_url, landing_path)
                if not status:
                    return False, msg, None

            # Finish block
            msg = f"✅ All Reactome files downloaded to {landing_path}"
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
        self.logger.log(msg, "INFO")

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
            input_file = input_path / "ReactomePathways.txt"

            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return None, False, msg

            df_pathways = pd.read_csv(
                input_file,
                sep="\t",
                header=None,
                names=["reactome_id", "pathway_name", "species"],
            )

            # Filter only Homo sapiens
            df_pathways = df_pathways[df_pathways["species"] == "Homo sapiens"]

            # Output files paths
            output_file_master = output_path / "master_data"

            # Save filtered pathways
            df_pathways.to_csv(
                output_file_master.with_suffix(".csv"), index=False
            )  # noqa: E501
            df_pathways.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )

            msg = f"✅ Pathways master data written with {len(df_pathways)} records)"  # noqa E501
            self.logger.log(msg, "INFO")

            # START SECOND FILES
            # Process relations
            records = []
            valid_ids = set(df_pathways["reactome_id"])

            # Pathways relations
            relations_file = input_path / "ReactomePathwaysRelation.txt"

            with open(relations_file, "r") as infile:
                for line in infile:
                    if line.startswith("#"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) < 2:
                        continue
                    parent_id = parts[0]
                    child_id = parts[1]

                    if parent_id in valid_ids and child_id in valid_ids:
                        records.append(
                            {
                                "reactome_id": child_id,
                                "relation_type": "pathway_parent",
                                "relation": parent_id,
                                "evidence": "curated",  # Manually set to IEA
                            }
                        )

            # Process Genes Symbols
            gmt_zip_file = input_path / "ReactomePathways.gmt.zip"

            # Map Pathway Name -> Reactome ID
            pathway_name_to_id = df_pathways.set_index("pathway_name")[
                "reactome_id"
            ].to_dict()

            with zipfile.ZipFile(gmt_zip_file, "r") as zip_ref:
                for info in zip_ref.infolist():
                    if not info.filename.endswith(".gmt"):
                        continue

                    with zip_ref.open(info.filename) as file:
                        for line in file:
                            parts = line.decode("utf-8").strip().split("\t")
                            if len(parts) < 3:
                                continue
                            pathway_name = parts[0]

                            if pathway_name not in pathway_name_to_id:
                                continue

                            reactome_id = pathway_name_to_id[pathway_name]
                            gene_symbols = parts[2:]

                            for gene_symbol in gene_symbols:
                                records.append(
                                    {
                                        "reactome_id": reactome_id,
                                        "relation_type": "gene_symbol",
                                        "relation": gene_symbol,
                                        "evidence": "IEA",  # Manually set IEA
                                    }
                                )

            # Process Ensembl IDs (Genes and Proteins)
            ensembl_file = input_path / "Ensembl2Reactome.txt"

            with open(ensembl_file, "r") as infile:
                for line in infile:
                    if line.startswith("#"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) < 6:
                        continue
                    ensembl_id = parts[0]
                    reactome_id = parts[1]
                    pathway_name = parts[3]
                    evidence = parts[4]
                    species = parts[5]

                    if species != "Homo sapiens":
                        continue
                    if reactome_id not in valid_ids:
                        continue

                    if ensembl_id.startswith("ENSG"):
                        ensembl_type = "ensembl_gene"
                    elif ensembl_id.startswith("ENSP"):
                        ensembl_type = "ensembl_protein"
                    else:
                        continue  # Ignore unexpected entries

                    records.append(
                        {
                            "reactome_id": reactome_id,
                            "relation_type": ensembl_type,
                            "relation": ensembl_id,
                            "evidence": evidence,
                        }
                    )

            # Process Uniprot (Protein)
            uniprot_file = input_path / "UniProt2Reactome.txt"

            with open(uniprot_file, "r") as infile:
                for line in infile:
                    if line.startswith("#"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) < 6:
                        continue
                    uniprot_id = parts[0]
                    reactome_id = parts[1]
                    pathway_name = parts[3]
                    evidence = parts[4]
                    species = parts[5]

                    if species != "Homo sapiens":
                        continue
                    if reactome_id not in valid_ids:
                        continue

                    records.append(
                        {
                            "reactome_id": reactome_id,
                            "relation_type": "uniprot_protein",
                            "relation": uniprot_id,
                            "evidence": evidence,
                        }
                    )

            # Convert to DataFrame
            df_relations = pd.DataFrame(records)

            # Output files paths
            output_file_relationship = output_path / "relationship_data"

            # Save relationship pathways
            df_relations.to_csv(
                output_file_relationship.with_suffix(".csv"), index=False
            )  # noqa: E501
            df_relations.to_parquet(
                output_file_relationship.with_suffix(".parquet"), index=False
            )

            self.logger.log(
                f"✅ Reactome links written with {len(df_relations)} links)",
                "INFO",  # noqa E501
            )  # noqa: E501

            msg = f"✅ Finished transforming {self.data_source.name} data."
            return None, True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",
        )

        total_pathways = 0
        load_status = False

        data_source_id = self.data_source.id

        if df is None:
            if not processed_dir:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_pathways, load_status, msg

            processed_path = self.get_path(processed_dir)
            processed_data = str(processed_path / "master_data.parquet")

            if not os.path.exists(processed_data):
                msg = f"File not found: {processed_data}"
                self.logger.log(msg, "ERROR")
                return total_pathways, load_status, msg

            self.logger.log(
                f"📥 Reading data in chunks from {processed_data}", "INFO"
            )  # noqa E501

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

        try:
            # Interaction to each Reactome Pathway
            for _, row in df.iterrows():

                pathway_master = row.get("reactome_id")
                pathway_name = row.get("pathway_name")

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

            msg = f"📥 Total Pathways: {total_pathways}"  # noqa E501
            self.logger.log(msg, "INFO")
            return total_pathways, True, msg

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return 0, False, msg

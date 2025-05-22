import os
import pandas as pd
import zipfile
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models.entity_models import (
    EntityGroup,
    EntityName,
    EntityRelationshipType,
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

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Downloads Reactome data. Uses the hash of 'ReactomePathways.txt' as
        reference. Only proceeds with full extraction if the hash has changed.
        """

        self.logger.log(
            f"⬇️ Starting extraction of {self.data_source.name} data...",
            "INFO",  # noqa: E501
        )  # noqa: E501

        msg = ""
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

        self.logger.log(
            f"🔧 Transforming the {self.data_source.name} data ...", "INFO"
        )  # noqa: E501

        msg = ""
        try:
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            processed_path = os.path.join(
                processed_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(processed_path, exist_ok=True)

            # START FIRST FILE
            # Process pathways
            pathways_file = os.path.join(landing_path, "ReactomePathways.txt")
            df_pathways = pd.read_csv(
                pathways_file,
                sep="\t",
                header=None,
                names=["reactome_id", "pathway_name", "species"],
            )

            # Filter only Homo sapiens
            df_pathways = df_pathways[df_pathways["species"] == "Homo sapiens"]

            # Save filtered pathways
            pathways_csv = os.path.join(processed_path, "master_data.csv")
            df_pathways.to_csv(pathways_csv, index=False)
            self.logger.log(
                f"✅ Pathways transformed and saved to {pathways_csv}", "INFO"
            )

            # START SECOND FILES
            # Process relations
            records = []
            valid_ids = set(df_pathways["reactome_id"])
            # Pathways relations
            relations_file = os.path.join(
                landing_path, "ReactomePathwaysRelation.txt"
            )  # noqa E501

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
            gmt_zip_file = os.path.join(
                landing_path, "ReactomePathways.gmt.zip"
            )  # noqa E501

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

            # Process Emsembl IDs (Genes and Proteins)
            ensembl_file = os.path.join(landing_path, "Ensembl2Reactome.txt")

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

            # Proces Uniprot (Protein)
            uniprot_file = os.path.join(landing_path, "UniProt2Reactome.txt")

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

            # Save results in process file
            df_relations = pd.DataFrame(records)
            relations_csv = os.path.join(
                processed_path, "pathway_relations.csv"
            )  # noqa E501
            df_relations.to_csv(relations_csv, index=False)

            self.logger.log(
                f"✅ Pathways Relations transformed and saved to {relations_csv}",  # noqa E501
                "INFO",
            )

            msg = "Transformation Completed"

            return df_pathways, True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):

        self.logger.log(
            f"📥 Loading {self.data_source.name} data into the database...",
            "INFO",  # noqa E501
        )

        total_pathways = 0
        load_status = False
        msg = ""

        # NOTE: Garante que self.data_source é válido na sessão atual
        # self.data_source = self.session.merge(self.data_source)
        data_source_id = self.data_source.id

        if df is None:
            if not processed_dir:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_pathways, load_status, msg

            processed_path = self.get_path(processed_dir)
            processed_data = str(processed_path / "master_data.csv")

            if not os.path.exists(processed_data):
                msg = f"File not found: {processed_data}"
                self.logger.log(msg, "ERROR")
                return total_pathways, load_status, msg

            self.logger.log(
                f"📥 Reading data in chunks from {processed_data}", "INFO"
            )  # noqa E501

            df = pd.read_csv(processed_data, dtype=str)

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

        # ----------------------------------------------
        # START TRANSACTION DATA (--> PATHWAY RELATIONS)

        try:
            msg = ""
            load_status = False

            # processed_path = self.get_path(processed_path)
            relations_file = str(processed_path / "pathway_relations.csv")

            if not os.path.exists(relations_file):
                msg = f"File not found: {relations_file}"
                self.logger.log(msg, "ERROR")
                return 0, load_status, msg

            df = pd.read_csv(relations_file, dtype=str)

            # Add columns for IDs and relationship type
            df["entity_1_id"] = None
            df["entity_2_id"] = None
            df["relationship_type_id"] = None

            # Get pathway IDs from EntityName
            pathway_ids = (
                self.session.query(EntityName.name, EntityName.entity_id)
                .filter(EntityName.data_source_id == self.data_source.id)
                .all()
            )
            pathway_id_map = dict(pathway_ids)

            # Map entity_1_id and entity_2_id (pathway_parent)
            df["entity_1_id"] = df["reactome_id"].map(pathway_id_map)
            df["entity_2_id"] = None  # Clean to avoid overwriting

            mask_pathway_parent = df["relation_type"] == "pathway_parent"
            df.loc[mask_pathway_parent, "entity_2_id"] = df.loc[
                mask_pathway_parent, "relation"
            ].map(pathway_id_map)

            # Map entity_2_id (gene_symbol, ensembl_gene, uniprot_protein)
            mask_others = ~mask_pathway_parent
            relation_names_to_lookup = (
                df.loc[mask_others, "relation"].dropna().unique().tolist()
            )

            # Query EntityName in batch
            relation_entities = (
                self.session.query(EntityName.name, EntityName.entity_id)
                .filter(EntityName.name.in_(relation_names_to_lookup))
                .all()
            )
            relation_name_to_entity_id = dict(relation_entities)

            # Apply map on remaining entity_2_id
            df.loc[mask_others, "entity_2_id"] = df.loc[
                mask_others, "relation"
            ].map(  # noqa: E501
                relation_name_to_entity_id
            )

            # Get all relationship types
            relationship_types = self.session.query(
                EntityRelationshipType.code, EntityRelationshipType.id
            ).all()
            relationship_type_map = dict(relationship_types)

            # Map relationship types to REACTOME relation types
            relation_type_to_relationship_code = {
                "pathway_parent": "part_of",
                "gene_symbol": "in_pathway",
                "ensembl_gene": "in_pathway",
                "ensembl_protein": "in_pathway",
                "uniprot_protein": "in_pathway",
            }
            # Map relationship types to IDs
            df["relationship_type_id"] = df["relation_type"].apply(
                lambda x: relationship_type_map[
                    relation_type_to_relationship_code.get(x, "in_pathway")
                ]
            )

            # USE THIS IF YOU WANT TO KEEP THE OLD RELATIONSHIP TYPE
            # df["relationship_type_id"] = df["relation_type"].apply(
            #     lambda x: relationship_type_map["part_of"]
            #     if x == "pathway_parent"
            #     else relationship_type_map["in_pathway"]
            # )

            # Load only valid relationships and save invalid ones in file
            df_valid = df[
                df["entity_1_id"].notnull() & df["entity_2_id"].notnull()
            ]  # noqa: E501
            df_invalid = df[
                df["entity_1_id"].isnull() | df["entity_2_id"].isnull()
            ]  # noqa: E501

            # Load duplicates relationships in valid dataframe
            df_valid.loc[:, "entity_2_id"] = df_valid["entity_2_id"].astype(
                int
            )  # noqa: E501
            df_valid = df_valid.drop_duplicates(
                subset=["entity_1_id", "entity_2_id", "relationship_type_id"]
            )

            # Load valid relationships
            total_pathways_relations_added = 0
            total_pathways_relations_existed = 0
            for _, row in df_valid.iterrows():
                status = self.get_or_create_entity_relationship(
                    entity_1_id=int(row["entity_1_id"]),
                    entity_2_id=int(row["entity_2_id"]),
                    relationship_type_id=int(row["relationship_type_id"]),
                    data_source_id=self.data_source.id,
                )
                if status:
                    total_pathways_relations_added += 1
                else:
                    total_pathways_relations_existed += 1

            # Commit ao final do batch
            try:
                self.session.commit()
                load_status = True
                msg = f"✅ {len(df_valid)} relations loaded successfully"
                self.logger.log(msg, "INFO")
            except Exception as e:
                self.session.rollback()
                load_status = False
                msg = f"❌ Error loading relations: {str(e)}"
                self.logger.log(msg, "ERROR")
                return 0, load_status, msg

            # Export not found to CSV
            not_found_csv = os.path.join(
                processed_path, "pathway_relations_not_found.csv"
            )
            df_invalid.to_csv(not_found_csv, index=False)
            self.logger.log(
                f"⚠️ Relations not found exported to {not_found_csv}",
                "WARNING",  # noqa: E501
            )

            msg = f"✅ Relations loaded: {len(df_valid)} | Not found: {len(df_invalid)}"  # noqa: E501
            self.logger.log(msg, "INFO")
            return len(df_valid), True, msg

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return 0, False, msg

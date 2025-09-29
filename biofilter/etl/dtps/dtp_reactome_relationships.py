import os
import pandas as pd
from pathlib import Path
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models import (
    Entity,
    EntityAlias,
    EntityRelationshipType,
    ETLDataSource,
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

        # DTP versioning
        self.dtp_name = "dtp_reactome_relationships"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        This DTP is specifically for loading relationships from Reactome.
        It does not perform data extraction. To extract Reactome data, use the
        'reactome' data source instead.
        """
        msg = (
            f"🔄 The data source '{self.data_source.name}' is for relationships only. "  # noqa E501
            "Use the 'reactome' data source to extract raw data."
        )
        self.logger.log(msg, "INFO")
        return True, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):
        """
        This DTP is specifically for loading relationships from Reactome.
        It does not perform data transformation. To transform Reactome data,
        use the 'Reactome' data source instead.
        """
        msg = (
            f"⚠️ The data source '{self.data_source.name}' is for relationships only. "  # noqa E501
            "Transformation should be done through the 'Reactome' data source."
        )
        self.logger.log(msg, "INFO")
        return True, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        """
        Load relationships between pathways and other entities from processed file.  # noqa: E501
        """
        msg = f"🔄 Loading relationships for data source '{self.data_source.name}'..."  # noqa E501

        # Check Compartibility
        self.check_compatibility()

        self.logger.log(msg, "INFO")

        total_relationships = 0
        total_warnings = 0
        parent_source = "reactome"

        # READ PROCESSED DATA TO LOAD
        try:
            # Check if processed dir was set
            if not processed_dir:
                msg = "⚠️  processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR

            # The file is hosted in the parent dtp.
            processed_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / parent_source  # noqa E501
            )  # noqa: E501
            processed_file_name = str(processed_path / "relationship_data.parquet")  # noqa E501

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

        # Map entity groups and relationship types to their IDs
        try:
            # Add columns for IDs and relationship type
            df["entity_1_id"] = None
            df["entity_2_id"] = None
            df["entity_1_group_id"] = None
            df["entity_2_group_id"] = None
            df["relationship_type_id"] = None

            # Use the parent source to get the DataSource ID
            # This is necessary because the Entities were loaded from the
            # Reactome data source.
            parent_ds_id = (
                self.session.query(ETLDataSource.id)
                .filter_by(name=parent_source)
                .scalar()  # noqa E501
            )

            # Get pathway IDs from EntityName
            pathway_ids = (
                self.session.query(EntityAlias.alias_value, EntityAlias.entity_id)  # noqa E501
                .filter(EntityAlias.data_source_id == parent_ds_id)
                .filter(EntityAlias.is_primary.is_(True))
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
                self.session.query(EntityAlias.alias_value, EntityAlias.entity_id)  # noqa E501
                .filter(EntityAlias.alias_value.in_(relation_names_to_lookup))
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
            df_not_loaded = df[
                df["entity_1_id"].isnull() | df["entity_2_id"].isnull()
            ]  # noqa: E501

            # Load duplicates relationships in valid dataframe
            df_valid.loc[:, "entity_2_id"] = df_valid["entity_2_id"].astype(
                int
            )  # noqa: E501
            df_valid = df_valid.drop_duplicates(
                subset=["entity_1_id", "entity_2_id", "relationship_type_id"]
            )

            # Fetch group_ids from Entity
            entity_ids_used = pd.concat([
                df_valid["entity_1_id"],
                df_valid["entity_2_id"]
            ]).dropna().unique().tolist()

            entity_groups = (
                self.session.query(Entity.id, Entity.group_id)
                .filter(Entity.id.in_(entity_ids_used))
                .all()
            )
            entity_group_map = dict(entity_groups)

            # Map group IDs
            df_valid["entity_1_group_id"] = df_valid["entity_1_id"].map(entity_group_map)
            df_valid["entity_2_group_id"] = df_valid["entity_2_id"].map(entity_group_map)

            # Load valid relationships
            total_pathways_relations_added = 0
            total_pathways_relations_existed = 0
            for _, row in df_valid.iterrows():
                status = self.get_or_create_entity_relationship(
                    entity_1_id=int(row["entity_1_id"]),
                    entity_2_id=int(row["entity_2_id"]),
                    entity_1_group_id=int(row["entity_1_group_id"]) if row["entity_1_group_id"] else None,
                    entity_2_group_id=int(row["entity_2_group_id"]) if row["entity_2_group_id"] else None,
                    relationship_type_id=int(row["relationship_type_id"]),
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                )
                if status:
                    total_pathways_relations_added += 1
                else:
                    total_pathways_relations_existed += 1

            # Commit batch
            try:
                self.session.commit()
                total_relationships += 1
                msg = f"✅ {len(df_valid)} relations loaded successfully"
                self.logger.log(msg, "INFO")
            except Exception as e:
                self.session.rollback()
                msg = f"❌ Error loading relations: {str(e)}"
                self.logger.log(msg, "ERROR")
                # return 0, load_status, msg

            # Relations not loaded due to inconsistencies
            if not df_not_loaded.empty:
                msg = f"⚠️ {len(df_not_loaded)} relationships not loaded due to some inconsistencies."  # noqa E501
                self.logger.log(msg, "WARNING")

                # Save the not loaded relationships to a CSV file
                not_loaded_path = (
                    processed_path / "relationship_data_not_loaded.csv"
                )  # noqa E501
                df_not_loaded.to_csv(not_loaded_path, index=False)

            # msg = f"✅ Relations loaded: {len(df_valid)} | Not found: {len(df_not_loaded)}"  # noqa: E501
            # self.logger.log(msg, "INFO")
            # return len(df_valid), True, msg

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"📥 Total Pathways: {total_relationships}"  # noqa E501  # noqa E501
        self.logger.log(msg, "INFO")

        return True, msg

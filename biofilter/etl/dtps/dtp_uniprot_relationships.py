import os
from pathlib import Path
import pandas as pd
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models.entity_models import (
    EntityGroup,
    Entity,
    EntityName,
    EntityRelationship,
    EntityRelationshipType,
)  # noqa E501
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
        This DTP is specifically for loading relationships from UniProt.
        It does not perform data extraction. To extract UniProt data, use the
        'uniprot' data source instead.
        """
        msg = (
            f"🔄 The data source '{self.data_source.name}' is for relationships only. "  # noqa E501
            "Use the 'uniprot' data source to extract raw data."
        )
        self.logger.log(msg, "INFO")
        return False, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):
        """
        This DTP is specifically for loading relationships from UniProt.
        It does not perform data transformation. To transform UniProt data,
        use the 'uniprot' data source instead.
        """
        msg = (
            f"⚠️ The data source '{self.data_source.name}' is for relationships only. "  # noqa E501
            "Transformation should be done through the 'uniprot' data source."
        )
        self.logger.log(msg, "INFO")
        return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):
        """
        Load relationships between proteins and other entities from processed file.  # noqa: E501
        """
        msg = f"🔄 Loading relationships for data source '{self.data_source.name}'..."  # noqa E501

        self.logger.log(msg, "INFO")
        load_status = False

        total_relationships = 0

        # data_source_id = self.data_source.id

        # This DTP doesn't transfer any data from previous processes.

        # We cannot use the get_path method here, because:
        #    The file is hosted in the parent dtp.
        processed_path = (
            Path(processed_dir)
            / self.data_source.source_system.name
            / "uniprot"  # noqa E501
        )  # noqa: E501
        processed_data = str(processed_path / "relationship_data.parquet")

        if not os.path.exists(processed_data):
            msg = f"File not found: {processed_data}"
            self.logger.log(msg, "ERROR")
            return total_relationships, load_status, msg

        self.logger.log(
            f"📥 Reading data in chunks from {processed_data}", "INFO"
        )  # noqa E501

        df = pd.read_parquet(processed_data, engine="pyarrow")

        # Check if DataFrame is empty
        if df.empty:
            msg = "DataFrame is empty."
            self.logger.log(msg, "ERROR")
            return total_relationships, load_status, msg

        # Map entity groups and relationship types to their IDs
        try:
            # Load in memory all entity groups to avoid multiple queries
            group_map = {
                g.name.lower(): g.id
                for g in self.session.query(EntityGroup).all()  # noqa E501
            }  # noqa E501
            # Load in memory all relationship types to avoid multiple queries
            rel_type_map = {
                rt.code.lower(): rt.id
                for rt in self.session.query(EntityRelationshipType).all()
            }  # noqa E501
        except Exception as e:
            msg = f"Error loading entity groups or relationship types: {e}"
            self.logger.log(msg, "ERROR")
            return total_relationships, load_status, msg

        try:
            # Transform the mapping to a Series with Int64 type (allows NaN)
            df["source_group_id"] = (
                df["source_type"].str.lower().map(group_map).astype("Int64")
            )  # noqa E501
            df["target_group_id"] = (
                df["target_type"].str.lower().map(group_map).astype("Int64")
            )  # noqa E501
            # Map relationship types to their IDs
            df["relation_type_id"] = (
                df["relation_type"].str.lower().map(rel_type_map).astype("Int64")
            )  # noqa E501
        except KeyError as e:
            msg = f"Error mapping group IDs or relationship types: {e}"
            self.logger.log(msg, "ERROR")
            return total_relationships, load_status, msg

        try:
            # Clean previous relationships from this data source
            deleted = (
                self.session.query(EntityRelationship)
                .filter_by(data_source_id=self.data_source.id)
                .delete(synchronize_session=False)
            )
            self.logger.log(
                f"🧹  Deleted {deleted} existing relationships from this data source",
                "INFO",
            )  # noqa E501
            self.session.commit()

            # Reserve all relationships not loaded
            not_loaded = []

            for _, row in df.iterrows():
                target_ids = str(row["target_id"]).split("|")
                for target_id in target_ids:

                    # Get source entity
                    source_name = row["source_id"]
                    target_name = target_id
                    relation_type = row["relation_type_id"]
                    source_type = row["source_group_id"]
                    target_type = row["target_group_id"]

                    # Check values before processing
                    if (
                        pd.isna(row["relation_type_id"])
                        or pd.isna(row["source_id"])
                        or pd.isna(row["target_id"])
                    ):  # noqa E501
                        not_loaded.append(row)
                        msg = f"⚠️  Skipping: Missing required fields: {source_name} ➝ {target_name} - {relation_type}"  # noqa E501
                        self.logger.log(msg, "WARNING")
                        continue

                    # Get entity_1_id
                    source_entity = (
                        self.session.query(EntityName, Entity)
                        .join(Entity, Entity.id == EntityName.entity_id)
                        .filter(EntityName.name == source_name)
                        .filter(Entity.is_deactive.is_(None))
                        .filter(Entity.group_id == source_type)
                        .first()
                    )
                    # Get entity_2_id
                    target_entity = (
                        self.session.query(EntityName, Entity)
                        .join(Entity, Entity.id == EntityName.entity_id)
                        .filter(EntityName.name == target_name)
                        .filter(Entity.is_deactive.is_(None))
                        .filter(Entity.group_id == target_type)  # helper
                        .first()
                    )

                    if not source_entity or not target_entity:
                        not_loaded.append(row)
                        msg = f"⚠️  Skipping: Entity not found or group mismatch: {source_name} ➝ {target_name}"  # noqa E501
                        self.logger.log(msg, "WARNING")
                        continue

                    # TODO: check if exists a relationship already or add it in DB  # noqa E501
                    rel = EntityRelationship(
                        entity_1_id=source_entity.Entity.id,
                        entity_2_id=target_entity.Entity.id,
                        relationship_type_id=relation_type,
                        data_source_id=self.data_source.id,
                    )
                    self.session.add(rel)
                    total_relationships += 1

            # Commit batch
            try:
                self.session.commit()
                load_status = True
                msg = f"✅ {total_relationships} relations loaded successfully"
                self.logger.log(msg, "INFO")
            except Exception as e:
                self.session.rollback()
                load_status = False
                msg = f"❌ Error loading relations: {str(e)}"
                self.logger.log(msg, "ERROR")
                # return 0, load_status, msg

            # If there are relationships not loaded, save them in a DataFrame
            df_not_loaded = pd.DataFrame(not_loaded)

            if not df_not_loaded.empty:
                msg = f"⚠️ {len(df_not_loaded)} relationships not loaded due to some inconsistencies."  # noqa E501
                self.logger.log(msg, "WARNING")

                # Save the not loaded relationships to a CSV file
                not_loaded_path = (
                    processed_path / "relationship_data_not_loaded.csv"
                )  # noqa E501
                df_not_loaded.to_csv(not_loaded_path, index=False)

            msg = f"✅ Relations loaded: {total_relationships} | Not found: {len(df_not_loaded)}"  # noqa: E501
            self.logger.log(msg, "INFO")
            return total_relationships, True, msg

        except Exception as e:
            msg = f"KeyError during loading relationships: {e}"
            self.logger.log(msg, "ERROR")
            return total_relationships, load_status, msg

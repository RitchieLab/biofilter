import os
from pathlib import Path
import pandas as pd
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models.model_entities import (
    EntityGroup,
    Entity,
    EntityAlias,
    EntityRelationship,
    EntityRelationshipType,
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
        self.dtp_name = "dtp_mondo_relationships"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"


    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        msg = f"🔄 The data source '{self.data_source.name}' is for MONDO relationships only."
        self.logger.log(msg, "INFO")
        return True, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):
        msg = f"⚠️ The data source '{self.data_source.name}' is for relationships only. Transform is handled in 'mondo'."
        self.logger.log(msg, "INFO")
        return True, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        msg = f"🔄 Loading MONDO relationships..."
        self.logger.log(msg, "INFO")

        self.check_compatibility()

        total_relationships = 0
        total_warnings = 0
        parent_source = "mondo"

        # --- Read processed file ---
        processed_path = (
            Path(processed_dir) / self.data_source.source_system.name / parent_source
        )
        processed_file_name = str(processed_path / "relationship_data.parquet")

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

        # --- Prepare maps ---
        group_map = {
            g.name.lower(): g.id
            for g in self.session.query(EntityGroup).all()
        }
        rel_type_map = {
            rt.code.lower(): rt.id
            for rt in self.session.query(EntityRelationshipType).all()
        }

        # --- Clean old relationships ---
        deleted = (
            self.session.query(EntityRelationship)
            .filter_by(data_source_id=self.data_source.id)
            .delete(synchronize_session=False)
        )
        self.logger.log(f"🧹 Deleted {deleted} old MONDO relationships", "INFO")
        self.session.commit()

        not_loaded = []

        # Iterate over rows
        for _, row in df.iterrows():
            try:
                src_code = row["term1_code"]
                tgt_code = row["term2_code"]
                rel_type = row["relation_type"]

                src_group = row["term1_group"].lower()
                tgt_group = row["term2_group"].lower()

                # Ajustar aqui (deixar tanto o map quanto a input como lower)
                src_group_id = group_map.get(src_group)
                tgt_group_id = group_map.get(tgt_group)
                rel_type_id = rel_type_map.get(rel_type.lower())

                if not src_group_id or not tgt_group_id or not rel_type_id:
                    not_loaded.append(row)
                    continue

                # Find source entity
                src_entity = (
                    self.session.query(EntityAlias, Entity)
                    .join(Entity, Entity.id == EntityAlias.entity_id)
                    .filter(EntityAlias.alias_value == src_code)
                    .filter(Entity.is_active.is_(True))
                    .filter(Entity.group_id == src_group_id)
                    .first()
                )

                # Find target entity
                tgt_entity = (
                    self.session.query(EntityAlias, Entity)
                    .join(Entity, Entity.id == EntityAlias.entity_id)
                    .filter(EntityAlias.alias_value == tgt_code)
                    .filter(Entity.is_active.is_(True))
                    .filter(Entity.group_id == tgt_group_id)
                    .first()
                )

                if not src_entity or not tgt_entity:
                    not_loaded.append(row)
                    continue

                rel = EntityRelationship(
                    entity_1_id=src_entity.Entity.id,
                    entity_2_id=tgt_entity.Entity.id,
                    entity_1_group_id=src_group_id,
                    entity_2_group_id=tgt_group_id,
                    relationship_type_id=rel_type_id,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(rel)
                total_relationships += 1

            except Exception as e:
                not_loaded.append(row)
                self.logger.log(f"⚠️ Failed row: {e}", "WARNING")

        # Commit batch
        try:
            self.session.commit()
            msg = f"✅ {total_relationships} MONDO relationships loaded successfully"
            self.logger.log(msg, "INFO")
        except Exception as e:
            self.session.rollback()
            msg = f"❌ Error committing relationships: {e}"
            self.logger.log(msg, "ERROR")

        # Save not loaded if exists
        if not_loaded:
            df_not_loaded = pd.DataFrame(not_loaded)
            not_loaded_path = processed_path / "relationship_data_not_loaded.csv"
            df_not_loaded.to_csv(not_loaded_path, index=False)
            self.logger.log(
                f"⚠️ {len(df_not_loaded)} MONDO relationships skipped. See {not_loaded_path}",
                "WARNING",
            )

        # Restore DB to read mode
        try:
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            self.logger.log(f"⚠️ Failed to restore DB indexes: {e}", "WARNING")

        return True, f"📥 Total MONDO Relationships: {total_relationships}"
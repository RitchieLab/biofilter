import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from biofilter.modules.db.models import (  # noqa E501
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationshipType,
)
from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.etl.mixins.entity_query_mixin import EntityQueryMixin

# Maps Reactome relation_type -> EntityRelationshipType.code
RELATION_TYPE_TO_CODE = {
    "pathway_parent": "part_of",
    "gene_symbol": "in_pathway",
    "ensembl_gene": "in_pathway",
    "ensembl_protein": "in_pathway",
    "uniprot_protein": "in_pathway",
}


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        db=None,
    ):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db

        # DTP versioning
        self.dtp_name = "dtp_reactome_relationships"
        self.dtp_version = "2.0.0"
        self.compatible_schema_min = "4.1.0"
        self.compatible_schema_max = "5.0.0"

    # -------------------------------------------------------------------------
    #                            EXTRACT METHOD
    # -------------------------------------------------------------------------
    def extract(self, raw_dir: str):
        """
        No extraction. Uses the parent 'reactome' DTP processed output.
        """
        msg = (
            f"⚠️  '{self.data_source.name}' is for relationships only. "
            "Use 'reactome' to extract raw data."
        )
        self.logger.log(msg, "INFO")
        return True, msg, None

    # -------------------------------------------------------------------------
    #                            TRANSFORM METHOD
    # -------------------------------------------------------------------------
    def transform(self, raw_dir: str, processed_dir: str):
        """
        No transformation. Uses the parent 'reactome' DTP processed output.
        """
        msg = (
            f"⚠️  '{self.data_source.name}' is for relationships only. "
            "Transformation goes through the 'reactome' data source."
        )
        self.logger.log(msg, "INFO")
        return True, msg

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------
    def load(self, processed_dir=None):
        """
        Load Reactome relationships into entity_relationships using
        a temp table + INSERT...SELECT...NOT EXISTS for performance.

        Strategy mirrors dtp_biogrid:
        - resolve entities with explicit group + alias_type filters and
          priority-based ambiguity resolution
        - stage candidates into a TEMP TABLE
        - dedup against existing rows server-side (directional triple:
          entity_1_id, entity_2_id, relationship_type_id)
        """
        msg = f"🔄 Loading relationships for '{self.data_source.name}'..."
        self.logger.log(msg, "INFO")
        self.check_compatibility()

        parent_source = "reactome"

        # ------------------------------------------------------------------
        # 1) Read processed parquet
        # ------------------------------------------------------------------
        try:
            if not processed_dir:
                msg = "⚠️  processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg

            processed_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / parent_source
            )
            processed_file = processed_path / "relationship_data.parquet"

            if not os.path.exists(processed_file):
                msg = f"⚠️  File not found: {processed_file}"
                self.logger.log(msg, "ERROR")
                return False, msg

            df = pd.read_parquet(processed_file, engine="pyarrow")
            if df.empty:
                msg = "DataFrame is empty."
                self.logger.log(msg, "ERROR")
                return False, msg

            df.fillna("", inplace=True)
        except Exception as e:
            msg = f"⚠️  Failed to read parquet: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # ------------------------------------------------------------------
        # 2) Resolve EntityGroup IDs
        # ------------------------------------------------------------------
        try:
            group_ids = {}
            for name in ("Pathways", "Genes", "Proteins"):
                gid = (
                    self.session.query(EntityGroup.id)
                    .filter_by(name=name)
                    .scalar()
                )
                if gid is None:
                    msg = f"⚠️  EntityGroup '{name}' not found in DB."
                    self.logger.log(msg, "ERROR")
                    return False, msg
                group_ids[name] = gid
        except Exception as e:
            msg = f"⚠️  Failed to resolve entity groups: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # ------------------------------------------------------------------
        # 3) Resolve relationship_type IDs (part_of, in_pathway)
        # ------------------------------------------------------------------
        try:
            needed_codes = set(RELATION_TYPE_TO_CODE.values())
            rel_type_map = dict(
                self.session.query(
                    EntityRelationshipType.code,
                    EntityRelationshipType.id,
                )
                .filter(EntityRelationshipType.code.in_(needed_codes))
                .all()
            )
            missing = needed_codes - set(rel_type_map.keys())
            if missing:
                msg = (
                    f"⚠️  Relationship types not found in DB: {missing}"
                )
                self.logger.log(msg, "ERROR")
                return False, msg
        except Exception as e:
            msg = f"⚠️  Failed to resolve relationship types: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # ------------------------------------------------------------------
        # 4) Resolve entity_1 (always Pathway via reactome_id)
        # ------------------------------------------------------------------
        try:
            reactome_ids = (
                df["reactome_id"].dropna().astype(str).unique().tolist()
            )
            pathway_map = self._resolve_aliases(
                values=reactome_ids,
                group_id=group_ids["Pathways"],
                alias_types=["code"],
                xref_source="Reactome",
                label="pathway",
            )
            df["entity_1_id"] = df["reactome_id"].map(pathway_map)
        except Exception as e:
            msg = f"⚠️  Failed to resolve pathways: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # ------------------------------------------------------------------
        # 5) Resolve entity_2 by relation_type (with proper group scope)
        # ------------------------------------------------------------------
        try:
            df["entity_2_id"] = pd.NA

            # 5.1 pathway_parent → Pathway (reuses pathway_map)
            mask = df["relation_type"] == "pathway_parent"
            if mask.any():
                df.loc[mask, "entity_2_id"] = (
                    df.loc[mask, "relation"].map(pathway_map)
                )

            # 5.2 gene_symbol → Genes (symbol > prev_symbol > is_primary)
            mask = df["relation_type"] == "gene_symbol"
            if mask.any():
                vals = (
                    df.loc[mask, "relation"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
                gene_sym_map = self._resolve_aliases(
                    values=vals,
                    group_id=group_ids["Genes"],
                    alias_types=["symbol", "prev_symbol"],
                    label="gene_symbol",
                )
                df.loc[mask, "entity_2_id"] = (
                    df.loc[mask, "relation"].map(gene_sym_map)
                )

            # 5.3 ensembl_gene → Genes (alias_type='code')
            mask = df["relation_type"] == "ensembl_gene"
            if mask.any():
                vals = (
                    df.loc[mask, "relation"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
                gene_ens_map = self._resolve_aliases(
                    values=vals,
                    group_id=group_ids["Genes"],
                    alias_types=["code"],
                    label="ensembl_gene",
                )
                df.loc[mask, "entity_2_id"] = (
                    df.loc[mask, "relation"].map(gene_ens_map)
                )

            # 5.4 ensembl_protein → Proteins (alias_type='code')
            mask = df["relation_type"] == "ensembl_protein"
            if mask.any():
                vals = (
                    df.loc[mask, "relation"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
                prot_ens_map = self._resolve_aliases(
                    values=vals,
                    group_id=group_ids["Proteins"],
                    alias_types=["code"],
                    label="ensembl_protein",
                )
                df.loc[mask, "entity_2_id"] = (
                    df.loc[mask, "relation"].map(prot_ens_map)
                )

            # 5.5 uniprot_protein → Proteins (any alias except 'name')
            mask = df["relation_type"] == "uniprot_protein"
            if mask.any():
                vals = (
                    df.loc[mask, "relation"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
                prot_uni_map = self._resolve_aliases(
                    values=vals,
                    group_id=group_ids["Proteins"],
                    alias_types=None,  # excludes 'name' by default
                    label="uniprot_protein",
                )
                df.loc[mask, "entity_2_id"] = (
                    df.loc[mask, "relation"].map(prot_uni_map)
                )
        except Exception as e:
            msg = f"⚠️  Failed to resolve entity_2: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # ------------------------------------------------------------------
        # 6) Map relationship_type_id (no silent fallback)
        # ------------------------------------------------------------------
        unknown = (
            set(df["relation_type"].unique())
            - set(RELATION_TYPE_TO_CODE.keys())
        )
        if unknown:
            msg = f"⚠️  Unknown relation_types in input: {unknown}"
            self.logger.log(msg, "ERROR")
            return False, msg

        df["relationship_type_id"] = df["relation_type"].map(
            lambda x: rel_type_map[RELATION_TYPE_TO_CODE[x]]
        )

        # ------------------------------------------------------------------
        # 7) Split valid / invalid candidates and persist unresolved CSV
        # ------------------------------------------------------------------
        df_valid = df[
            df["entity_1_id"].notna() & df["entity_2_id"].notna()
        ].copy()
        df_invalid = df[
            df["entity_1_id"].isna() | df["entity_2_id"].isna()
        ].copy()

        if not df_invalid.empty:
            invalid_path = (
                processed_path / "relationship_data_not_loaded.csv"
            )
            df_invalid.to_csv(invalid_path, index=False)
            self.logger.log(
                f"⚠️  {len(df_invalid):,} relationships unresolved "
                f"(saved to {invalid_path})",
                "WARNING",
            )

        if df_valid.empty:
            msg = "No resolved relationships to load."
            self.logger.log(msg, "INFO")
            return True, msg

        df_valid["entity_1_id"] = df_valid["entity_1_id"].astype(int)
        df_valid["entity_2_id"] = df_valid["entity_2_id"].astype(int)
        df_valid["relationship_type_id"] = (
            df_valid["relationship_type_id"].astype(int)
        )

        # Directional dedup within file
        df_valid = (
            df_valid
            .drop_duplicates(
                subset=[
                    "entity_1_id", "entity_2_id", "relationship_type_id"
                ]
            )
            .reset_index(drop=True)
        )

        # ------------------------------------------------------------------
        # 8) Map group_ids in batch (single round-trip)
        # ------------------------------------------------------------------
        try:
            entity_ids = (
                pd.concat(
                    [df_valid["entity_1_id"], df_valid["entity_2_id"]]
                )
                .unique()
                .tolist()
            )
            entity_group_map = dict(
                self.session.query(Entity.id, Entity.group_id)
                .filter(Entity.id.in_(entity_ids))
                .all()
            )
            df_valid["entity_1_group_id"] = (
                df_valid["entity_1_id"].map(entity_group_map)
            )
            df_valid["entity_2_group_id"] = (
                df_valid["entity_2_id"].map(entity_group_map)
            )

            invalid_groups = (
                df_valid["entity_1_group_id"].isna()
                | df_valid["entity_2_group_id"].isna()
            )
            if invalid_groups.any():
                self.logger.log(
                    f"⚠️  Dropping {int(invalid_groups.sum()):,} "
                    "candidates with unresolved group_id",
                    "WARNING",
                )
                df_valid = df_valid[~invalid_groups].copy()

            if df_valid.empty:
                msg = "All candidates dropped after group_id mapping."
                self.logger.log(msg, "WARNING")
                return True, msg

            df_valid["entity_1_group_id"] = (
                df_valid["entity_1_group_id"].astype(int)
            )
            df_valid["entity_2_group_id"] = (
                df_valid["entity_2_group_id"].astype(int)
            )
        except Exception as e:
            msg = f"⚠️  Failed to map entity group_ids: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        self.logger.log(
            f"🧮 {len(df_valid):,} candidate relationships ready",
            "INFO",
        )

        # ------------------------------------------------------------------
        # 9) Bulk insert via temp table + server-side NOT EXISTS
        # ------------------------------------------------------------------
        return self._bulk_insert_relationships(df_valid)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _resolve_aliases(
        self,
        *,
        values,
        group_id,
        alias_types,
        xref_source=None,
        label="alias",
    ):
        """
        Resolve a list of alias strings to entity_ids within a single
        EntityGroup. Applies priority-based ambiguity resolution and logs
        any duplicates.

        Priority rules:
        - When `alias_types` is a list of length > 1, earlier types win
          over later ones (e.g. ['symbol', 'prev_symbol'] → symbol first).
        - Within the same alias_type, is_primary=True wins.
        - When `alias_types` is None, the query excludes alias_type='name'
          (descriptive labels) and ranks only by is_primary.
        """
        if not values:
            return {}

        q = (
            self.session.query(
                EntityAlias.alias_value,
                EntityAlias.entity_id,
                EntityAlias.alias_type,
                EntityAlias.is_primary,
            )
            .filter(EntityAlias.group_id == group_id)
            .filter(EntityAlias.alias_value.in_(values))
        )
        if alias_types is not None:
            q = q.filter(EntityAlias.alias_type.in_(alias_types))
        else:
            q = q.filter(EntityAlias.alias_type != "name")
        if xref_source:
            q = q.filter(EntityAlias.xref_source == xref_source)

        rows = q.all()
        if not rows:
            return {}

        df = pd.DataFrame(
            rows,
            columns=[
                "alias_value", "entity_id", "alias_type", "is_primary"
            ],
        )
        df["is_primary"] = df["is_primary"].fillna(False).astype(bool)

        if alias_types and len(alias_types) > 1:
            # Earlier types in the list get higher priority
            type_priority = {
                a: i for i, a in enumerate(reversed(alias_types))
            }
            df["priority"] = (
                df["alias_type"].map(type_priority).fillna(-1).astype(int)
                * 10
                + df["is_primary"].astype(int)
            )
        else:
            df["priority"] = df["is_primary"].astype(int)

        ambiguous_mask = df["alias_value"].duplicated(keep=False)
        if ambiguous_mask.any():
            sample = (
                df.loc[ambiguous_mask, "alias_value"].unique().tolist()[:20]
            )
            self.logger.log(
                f"⚠️  Ambiguous {label} aliases resolved by priority "
                f"(showing up to 20): {sample}",
                "WARNING",
            )

        df = (
            df.sort_values("priority", ascending=False)
            .drop_duplicates(subset=["alias_value"], keep="first")
        )
        return dict(zip(df["alias_value"], df["entity_id"]))

    def _bulk_insert_relationships(self, df_valid):
        """
        Stage candidates into a TEMP TABLE and run a single
        INSERT...SELECT...WHERE NOT EXISTS to load only new directional
        triples (entity_1_id, entity_2_id, relationship_type_id) for this
        data source.

        Indexes on entity_relationships are intentionally kept in place
        so the NOT EXISTS lookup is fast.
        """
        temp_table = "reactome_load_candidates"
        total_inserted = 0
        insert_error = None
        data_source_id = self.data_source.id
        etl_package_id = self.package.id

        try:
            # Defensive: drop stale temp from prior aborted runs in this
            # session (TEMP tables live for the connection).
            self.session.execute(
                text(f"DROP TABLE IF EXISTS {temp_table}")
            )
            self.session.execute(
                text(
                    f"""
                    CREATE TEMP TABLE {temp_table} (
                        entity_1_id BIGINT NOT NULL,
                        entity_1_group_id INTEGER,
                        entity_2_id BIGINT NOT NULL,
                        entity_2_group_id INTEGER,
                        relationship_type_id INTEGER NOT NULL,
                        data_source_id INTEGER NOT NULL,
                        etl_package_id INTEGER
                    )
                    """
                )
            )
            self.session.commit()

            chunk_size = 50_000
            insert_sql = text(
                f"""
                INSERT INTO {temp_table} (
                    entity_1_id, entity_1_group_id,
                    entity_2_id, entity_2_group_id,
                    relationship_type_id, data_source_id, etl_package_id
                ) VALUES (
                    :entity_1_id, :entity_1_group_id,
                    :entity_2_id, :entity_2_group_id,
                    :relationship_type_id,
                    :data_source_id, :etl_package_id
                )
                """
            )

            total = len(df_valid)
            for chunk_start in range(0, total, chunk_size):
                chunk = df_valid.iloc[
                    chunk_start: chunk_start + chunk_size
                ]
                rows = [
                    {
                        "entity_1_id": int(r.entity_1_id),
                        "entity_1_group_id": int(r.entity_1_group_id),
                        "entity_2_id": int(r.entity_2_id),
                        "entity_2_group_id": int(r.entity_2_group_id),
                        "relationship_type_id": int(
                            r.relationship_type_id
                        ),
                        "data_source_id": data_source_id,
                        "etl_package_id": etl_package_id,
                    }
                    for r in chunk.itertuples(index=False)
                ]
                self.session.execute(insert_sql, rows)
                self.session.commit()
                chunk_number = (chunk_start // chunk_size) + 1
                self.logger.log(
                    f"📥 Staged chunk {chunk_number} "
                    f"({len(rows):,} rows) into temp table",
                    "DEBUG",
                )

            # Server-side directional dedup
            self.logger.log(
                "🔍 Inserting deduplicated relationships server-side...",
                "INFO",
            )
            result = self.session.execute(
                text(
                    f"""
                    INSERT INTO entity_relationships (
                        entity_1_id, entity_1_group_id,
                        entity_2_id, entity_2_group_id,
                        relationship_type_id,
                        data_source_id, etl_package_id
                    )
                    SELECT
                        c.entity_1_id, c.entity_1_group_id,
                        c.entity_2_id, c.entity_2_group_id,
                        c.relationship_type_id,
                        c.data_source_id, c.etl_package_id
                    FROM {temp_table} c
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM entity_relationships er
                        WHERE er.data_source_id = c.data_source_id
                          AND er.entity_1_id = c.entity_1_id
                          AND er.entity_2_id = c.entity_2_id
                          AND er.relationship_type_id
                              = c.relationship_type_id
                    )
                    """
                )
            )
            total_inserted = result.rowcount or 0
            self.session.commit()

            self.logger.log(
                f"💾 Inserted {total_inserted:,} new relationships",
                "INFO",
            )

            self.session.execute(
                text(f"DROP TABLE IF EXISTS {temp_table}")
            )
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            insert_error = f"⚠️  Bulk insert failed: {e}"
            self.logger.log(insert_error, "ERROR")
            try:
                self.session.execute(
                    text(f"DROP TABLE IF EXISTS {temp_table}")
                )
                self.session.commit()
            except Exception:
                self.session.rollback()

        if insert_error:
            return False, insert_error

        msg = (
            f"📥 Total Reactome relationships inserted: "
            f"{total_inserted:,}"
        )
        return True, msg

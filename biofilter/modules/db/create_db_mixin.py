from __future__ import annotations

import json
import os
from datetime import datetime
from importlib import import_module
from typing import Dict, List, Optional

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine.url import make_url

from biofilter.modules.db.base import Base

# from biofilter.modules.db.migrate import alembic_upgrade_head
from biofilter.modules.db.core_ddl import (
    ddl_list_partitions,
    ddl_variant_effect_predictions,
    ddl_variant_gene_regulatory_evidence,
    ddl_variant_masters,
    ddl_variant_molecular_effect,
    ddl_variant_regulatory_elements,
)
from biofilter.modules.db.migrate import get_repo_heads, get_script_location
from biofilter.utils.db_loader import bootstrap_models

CORE_PARTITIONED = {
    "variant_masters",
    "variant_molecular_effects",
    "variant_effect_predictions",
    "variant_regulatory_elements",
    "variant_gene_regulatory_evidence",
}

# ---------------------------------------------------------------------
# Seed unique keys (must match unique constraints / natural keys)
# ---------------------------------------------------------------------
SEED_UNIQUE_KEYS: Dict[str, List[str]] = {
    # --- config ---
    "SystemConfig": ["key"],
    "BiofilterMetadata": ["schema_version"],
    # --- ETL ---
    "ETLSourceSystem": ["name"],
    "ETLDataSource": ["name"],
    # --- entities / curation ---
    "EntityGroup": ["name"],
    "EntityRelationshipType": ["id"],
    "OmicStatus": ["name"],
    # --- variants dims ---
    "VariantConsequenceGroup": ["name"],
    "VariantConsequenceCategory": ["name"],
    "VariantConsequence": ["name"],
    # --- genome ---
    "GenomeAssembly": ["accession"],
}


class CreateDBMixin:
    """
    DB bootstrap:
    - (Postgres) ensures database exists (CREATE DATABASE) using AUTOCOMMIT
    - connects (optionally even if DB doesn't exist yet)
    - registers models/tables into Base.metadata
    - creates tables (with special handling for Postgres partitioned variant_snps)  # noqa E501
    - seeds initial data
    """

    # -----------------------------
    # Postgres helpers
    # -----------------------------
    def ensure_postgres_database(self, db_uri: str) -> bool:
        """
        Ensure the target database exists. Returns True if created, False if
        already exists.

        IMPORTANT:
        - CREATE DATABASE cannot run inside a transaction, so we use AUTOCOMMIT.  # noqa E501
        - The user in db_uri must have CREATEDB privilege (or be superuser).
        """
        url = make_url(db_uri)

        if not url.database:
            raise ValueError(
                "db_uri must include a database name (e.g., .../biofilter_dev)."  # noqa E501
            )

        target_db = url.database
        admin_url = url.set(database="postgres")

        admin_engine = create_engine(admin_url, future=True)

        # AUTOCOMMIT is required for CREATE DATABASE
        with admin_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": target_db},
            ).scalar()

            if exists:
                return False

            conn.execute(text(f'CREATE DATABASE "{target_db}" OWNER "{url.username}"'))  # noqa E501
            return True

    # -----------------------------
    # Postgres helpers
    # -----------------------------
    def create_db(self, overwrite: bool = False, seed_dir: str = "seed") -> bool:  # noqa E501

        # 1) Now we can connect safely
        # If overwrite=False and DB exists -> short-circuit
        if self.exists_db(new_db=True) and not overwrite:
            msn = f"Database already exists at {self.db_uri}"
            self.logger.log(msn, "WARNING")
            return False

        # 2) If Postgres, ensure DB exists BEFORE connecting to it
        if getattr(self, "db_uri", None):
            url = make_url(self._normalize_uri(self.db_uri))
            if url.drivername.startswith("postgresql"):
                try:
                    created = self.ensure_postgres_database(self.db_uri)
                    if created:
                        self.logger.log(
                            f"🆕 Created PostgreSQL database '{url.database}'", "INFO"  # noqa E501
                        )
                except Exception as e:
                    self.logger.log(
                        f"❌ Could not ensure PostgreSQL database exists: {e}", "ERROR"  # noqa E501
                    )
                    raise

        self.connect(check_exists=False)

        try:
            self.logger.log("📦 Bootstrapping models...", "INFO")
            bootstrap_models(self.engine)  # single call is enough

            self.logger.log("🏗️  Creating tables...", "INFO")
            self._create_tables()

            self.logger.log("🌱 Seeding initial data...", "INFO")
            self._seed_all(seed_dir)

            self.logger.log(f"✅ Database created at {self.db_uri}", "SUCCESS")
            return True

        except Exception as e:
            self.logger.log(f"❌ Failed to create database: {e}", "ERROR")
            raise

    # -----------------------------------------------------------------
    # Public entry point: DB upgrade
    # -----------------------------------------------------------------
    def upgrade_db(self, seed_dir: str = "seed") -> None:
        """
        Apply/refresh master data seeds idempotently.
        Assumes schema is already upgraded.
        """
        # ensure connected, bootstrap models if needed, then seed
        self.connect(check_exists=True)
        bootstrap_models(self.engine)
        self._seed_all(seed_dir)

    # ---------------------------------------------------------------------
    # TABLE CREATION
    # ---------------------------------------------------------------------
    def _create_tables(self) -> None:
        """
        Create tables for the connected engine.
        - Postgres: create all tables except variant_snps via metadata,
        then create partitioned variant_snps via DDL + partitions.
        - SQLite: create everything via metadata (variant_snps is Core Table).
        """
        dialect = self.engine.dialect.name

        core_partitioned = CORE_PARTITIONED  # or inline set

        if dialect == "postgresql":
            other_tables = [
                t for t in Base.metadata.sorted_tables if t.name not in core_partitioned  # noqa E501
            ]
            Base.metadata.create_all(self.engine, tables=other_tables)

            self._create_partitioned_parent()

            self._ensure_partitions_by_chromosome(core_partitioned)

            insp = inspect(self.engine)
            existing = set(insp.get_table_names())
            missing = core_partitioned - existing
            if missing:
                raise RuntimeError(
                    f"Core parent tables missing on PostgreSQL: {sorted(missing)}"  # noqa E501
                )

            self.logger.log("✅ Tables created successfully (PostgreSQL).", "INFO")  # noqa E501
            return

        # SQLite / others
        Base.metadata.create_all(self.engine)

        for tbl in core_partitioned:
            if tbl not in Base.metadata.tables:
                raise RuntimeError(
                    f"{tbl} was not registered in Base.metadata (SQLite path)."
                )

        self.logger.log("✅ Tables created successfully (SQLite).", "INFO")

    def _create_partitioned_parent(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(ddl_variant_masters()))
            conn.execute(
                text(ddl_variant_molecular_effect())
            )  # renomeie para plural se quiser
            conn.execute(text(ddl_variant_effect_predictions()))
            conn.execute(
                text(ddl_variant_regulatory_elements())
            )  # corrigir typo no import/func
            conn.execute(text(ddl_variant_gene_regulatory_evidence()))  # idem

    def _ensure_partitions_by_chromosome(self, core_partitioned) -> None:
        with self.engine.begin() as conn:
            for tbl in core_partitioned:
                ddls = ddl_list_partitions(
                    parent_table=tbl,
                    part_prefix=tbl,
                    chrom_min=1,
                    chrom_max=25,
                )
                for ddl in ddls:
                    conn.execute(text(ddl))

                self.logger.log(f"✅ Ensured {tbl} partitions", "INFO")

    # -------------------------------------------------------------------------
    # Load Seeds to new DB
    # -------------------------------------------------------------------------
    def _seed_all(self, seed_dir):
        self._seed_from_json(
            f"{seed_dir}/initial_config.json", "model_config", "SystemConfig"
        )
        self._seed_from_json(
            f"{seed_dir}/initial_metadata.json",
            "model_config",
            "BiofilterMetadata",
            # key="schema_version",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_source_systems.json",
            "model_etl",
            "ETLSourceSystem",
            key="source_systems",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_data_sources.json",
            "model_etl",
            "ETLDataSource",
            key="data_sources",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_entity_group.json",
            "model_entities",
            "EntityGroup",
            key="entity_groups",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_entity_relationship_types.json",
            "model_entities",
            "EntityRelationshipType",
            key="entity_relationship_types",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_omic_status.json",
            "model_curation",
            "OmicStatus",
            key="omic_status",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_variant_consequence_groups.json",
            "model_variants",
            "VariantConsequenceGroup",
            key="variant_consequence_groups",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_variant_consequence_categories.json",
            "model_variants",
            "VariantConsequenceCategory",
            key="variant_consequence_categories",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_variant_consequences.json",
            "model_variants",
            "VariantConsequence",
            key="variant_consequences",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_genome_assemblies.json",
            "model_config",
            "GenomeAssembly",
            key="genome_assemblies",
        )

    def _seed_from_json(
        self, file: str, module_name: str, model_name: str, key: Optional[str] = None  # noqa E501
    ) -> None:
        """
        Seed data using an idempotent UPSERT strategy:
        - If the record exists (by natural key), update fields (non-null only).
        - Else, create it.

        This makes `biofilter db upgrade` safe to run repeatedly.
        """
        model_module = import_module(f"biofilter.modules.db.models.{module_name}")  # noqa E501
        model_class = getattr(model_module, model_name)

        json_path = os.path.join(os.path.dirname(__file__), file)
        if not os.path.exists(json_path):
            self.logger.log(f"JSON not found: {json_path}", "WARNING")
            return

        unique_keys = SEED_UNIQUE_KEYS.get(model_name)
        if not unique_keys:
            raise RuntimeError(
                f"Missing unique key config for seed model: {model_name}"
            )

        with self.get_session() as session:
            with open(json_path, "r") as f:
                data = json.load(f)
            records = data.get(key, data) if key else data

            applied = created = updated = skipped = 0

            for item in records:
                applied += 1

                # --- Special: BiofilterMetadata schema_revision comes from Alembic heads ---  # noqa E501
                if model_name == "BiofilterMetadata":
                    script_location = get_script_location()
                    schema_revision = ",".join(get_repo_heads(script_location))
                    item["schema_revision"] = schema_revision

                # --- Parse datetime-like fields (if your seeds contain them) ---  # noqa E501
                for k, v in list(item.items()):
                    if (k.endswith("_start") or k.endswith("_end")) and isinstance(  # noqa E501
                        v, str
                    ):
                        try:
                            item[k] = datetime.fromisoformat(v)
                        except ValueError:
                            self.logger.log(
                                f"Invalid datetime format in key {k}: {v}", "WARNING"  # noqa E501
                            )

                # --- Resolve FK by name (your existing behavior) ---
                if "source_system" in item:
                    fk_name = item.pop("source_system")
                    ETLSourceSystem = import_module(
                        "biofilter.modules.db.models.model_etl"
                    ).ETLSourceSystem
                    fk_obj = (
                        session.query(ETLSourceSystem).filter_by(name=fk_name).first()  # noqa E501
                    )
                    if not fk_obj:
                        self.logger.log(
                            f"Source System not found for name: {fk_name}", "WARNING"  # noqa E501
                        )
                        skipped += 1
                        continue
                    item["source_system_id"] = fk_obj.id

                if "data_source" in item:
                    fk_name = item.pop("data_source")
                    ETLDataSource = import_module(
                        "biofilter.modules.db.models.model_etl"
                    ).ETLDataSource
                    fk_obj = (
                        session.query(ETLDataSource).filter_by(name=fk_name).first()  # noqa E501
                    )
                    if not fk_obj:
                        self.logger.log(
                            f"Data Source not found for name: {fk_name}", "WARNING"  # noqa E501
                        )
                        skipped += 1
                        continue
                    item["data_source_id"] = fk_obj.id

                if model_name == "VariantConsequence":
                    if "group" in item and "consequence_group" not in item:
                        item["consequence_group"] = item.pop("group")
                    if "category" in item and "consequence_category" not in item:
                        item["consequence_category"] = item.pop("category")

                    if "consequence_group" in item:
                        group_name = item.pop("consequence_group")
                        VariantConsequenceGroup = import_module(
                            "biofilter.modules.db.models.model_variants"
                        ).VariantConsequenceGroup
                        group_obj = (
                            session.query(VariantConsequenceGroup)
                            .filter_by(name=group_name)
                            .first()
                        )
                        if not group_obj:
                            self.logger.log(
                                f"Variant Consequence Group not found for name: {group_name}",  # noqa E501
                                "WARNING",
                            )
                            skipped += 1
                            continue
                        item["consequence_group_id"] = group_obj.id

                    if "consequence_category" in item:
                        category_name = item.pop("consequence_category")
                        VariantConsequenceCategory = import_module(
                            "biofilter.modules.db.models.model_variants"
                        ).VariantConsequenceCategory
                        category_obj = (
                            session.query(VariantConsequenceCategory)
                            .filter_by(name=category_name)
                            .first()
                        )
                        if not category_obj:
                            self.logger.log(
                                f"Variant Consequence Category not found for name: {category_name}",  # noqa E501
                                "WARNING",
                            )
                            skipped += 1
                            continue
                        item["consequence_category_id"] = category_obj.id

                # --- Build lookup from natural key(s) ---
                lookup = {k: item.get(k) for k in unique_keys}
                if any(v is None for v in lookup.values()):
                    self.logger.log(
                        f"Seed item missing unique keys {unique_keys}: {item}",
                        "WARNING",
                    )
                    skipped += 1
                    continue

                existing = session.query(model_class).filter_by(**lookup).one_or_none()  # noqa E501
                if existing is None:
                    session.add(model_class(**item))
                    created += 1
                else:
                    # Update only provided fields (non-null), preventing accidental wipes  # noqa E501
                    for k, v in item.items():
                        if v is not None:
                            setattr(existing, k, v)
                    updated += 1

            session.commit()
            self.logger.log(
                f"Seeded: {model_name} | applied={applied} created={created} updated={updated} skipped={skipped}",  # noqa E501
                "INFO",
            )

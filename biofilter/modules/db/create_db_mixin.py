from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from importlib import import_module

from sqlalchemy import text, inspect, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine.url import make_url

from biofilter.modules.db.base import Base
from biofilter.utils.db_loader import bootstrap_models
from biofilter.modules.db.migrate import get_script_location, get_repo_heads

# from biofilter.modules.db.migrate import alembic_upgrade_head
from biofilter.modules.db.core_ddl import (
    ddl_variant_masters,
    ddl_variant_molecular_effect,
    ddl_variant_effect_predictions,
    ddl_list_partitions,
    ddl_variant_regulatory_elements,
    ddl_variant_gene_regulatory_evidence,
)

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
    # --- genome ---
    "GenomeAssembly": ["accession"],
}


class CreateDBMixin:
    """
    DB bootstrap:
    - (Postgres) ensures database exists (CREATE DATABASE) using AUTOCOMMIT
    - connects (optionally even if DB doesn't exist yet)
    - registers models/tables into Base.metadata
    - creates tables (with special handling for Postgres partitioned variant_snps)
    - seeds initial data
    """

    # -----------------------------
    # Postgres helpers
    # -----------------------------
    def ensure_postgres_database(self, db_uri: str) -> bool:
        """
        Ensure the target database exists. Returns True if created, False if already exists.

        IMPORTANT:
        - CREATE DATABASE cannot run inside a transaction, so we use AUTOCOMMIT.
        - The user in db_uri must have CREATEDB privilege (or be superuser).
        """
        url = make_url(db_uri)

        if not url.database:
            raise ValueError(
                "db_uri must include a database name (e.g., .../biofilter_dev)."
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

            # NOTE: identifier quoting — target_db comes from your config, not user input
            conn.execute(text(f'CREATE DATABASE "{target_db}" OWNER "{url.username}"'))
            return True

    # -----------------------------
    # Postgres helpers
    # -----------------------------
    def create_db(self, overwrite: bool = False, seed_dir: str = "seed") -> bool:

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
                            f"🆕 Created PostgreSQL database '{url.database}'", "INFO"
                        )
                except Exception as e:
                    self.logger.log(
                        f"❌ Could not ensure PostgreSQL database exists: {e}", "ERROR"
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
                t for t in Base.metadata.sorted_tables if t.name not in core_partitioned
            ]
            Base.metadata.create_all(self.engine, tables=other_tables)

            self._create_partitioned_parent()

            self._ensure_partitions_by_chromosome(core_partitioned)

            insp = inspect(self.engine)
            existing = set(insp.get_table_names())
            missing = core_partitioned - existing
            if missing:
                raise RuntimeError(
                    f"Core parent tables missing on PostgreSQL: {sorted(missing)}"
                )

            self.logger.log("✅ Tables created successfully (PostgreSQL).", "INFO")
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
            f"{seed_dir}/initial_genome_assemblies.json",
            "model_config",
            "GenomeAssembly",
            key="genome_assemblies",
        )

    def _seed_from_json(
        self, file: str, module_name: str, model_name: str, key: Optional[str] = None
    ) -> None:
        """
        Seed data using an idempotent UPSERT strategy:
        - If the record exists (by natural key), update fields (non-null only).
        - Else, create it.

        This makes `biofilter db upgrade` safe to run repeatedly.
        """
        model_module = import_module(f"biofilter.modules.db.models.{module_name}")
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

                # --- Special: BiofilterMetadata schema_revision comes from Alembic heads ---
                if model_name == "BiofilterMetadata":
                    script_location = get_script_location()
                    schema_revision = ",".join(get_repo_heads(script_location))
                    item["schema_revision"] = schema_revision

                # --- Parse datetime-like fields (if your seeds contain them) ---
                for k, v in list(item.items()):
                    if (k.endswith("_start") or k.endswith("_end")) and isinstance(
                        v, str
                    ):
                        try:
                            item[k] = datetime.fromisoformat(v)
                        except ValueError:
                            self.logger.log(
                                f"Invalid datetime format in key {k}: {v}", "WARNING"
                            )

                # --- Resolve FK by name (your existing behavior) ---
                if "source_system" in item:
                    fk_name = item.pop("source_system")
                    ETLSourceSystem = import_module(
                        "biofilter.modules.db.models.model_etl"
                    ).ETLSourceSystem
                    fk_obj = (
                        session.query(ETLSourceSystem).filter_by(name=fk_name).first()
                    )
                    if not fk_obj:
                        self.logger.log(
                            f"Source System not found for name: {fk_name}", "WARNING"
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
                        session.query(ETLDataSource).filter_by(name=fk_name).first()
                    )
                    if not fk_obj:
                        self.logger.log(
                            f"Data Source not found for name: {fk_name}", "WARNING"
                        )
                        skipped += 1
                        continue
                    item["data_source_id"] = fk_obj.id

                # --- Build lookup from natural key(s) ---
                lookup = {k: item.get(k) for k in unique_keys}
                if any(v is None for v in lookup.values()):
                    self.logger.log(
                        f"Seed item missing unique keys {unique_keys}: {item}",
                        "WARNING",
                    )
                    skipped += 1
                    continue

                existing = session.query(model_class).filter_by(**lookup).one_or_none()
                if existing is None:
                    session.add(model_class(**item))
                    created += 1
                else:
                    # Update only provided fields (non-null), preventing accidental wipes
                    for k, v in item.items():
                        if v is not None:
                            setattr(existing, k, v)
                    updated += 1

            session.commit()
            self.logger.log(
                f"Seeded: {model_name} | applied={applied} created={created} updated={updated} skipped={skipped}",
                "INFO",
            )

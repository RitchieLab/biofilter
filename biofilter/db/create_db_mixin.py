import os
import json
from sqlalchemy.exc import IntegrityError
from biofilter.db.base import Base
from importlib import import_module
from biofilter.utils.db_loader import load_all_models  # ✅ novo import
from datetime import datetime


class CreateDBMixin:
    def create_db(self, overwrite=False, seed_dir="seed"):
        if self.exists_db() and not overwrite:
            msn = f"Database already exists at {self.db_uri}"
            self.logger.log(msn, "WARNING")
            return False

        self.connect(check_exists=False)

        self.logger.log("Loading models...", "INFO")
        load_all_models()  # ✅ importa dinamicamente todos os modelos

        self.logger.log("Creating tables...", "INFO")
        self._create_tables()

        self.logger.log("Seeding initial data...", "INFO")
        self._seed_all(seed_dir)

        self.logger.log(f"Database created at {self.db_uri}", "INFO")
        return True

    def _create_tables(self):
        load_all_models()
        Base.metadata.create_all(self.engine)

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
            "SourceSystem",
            key="source_systems",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_data_sources.json",
            "model_etl",
            "DataSource",
            key="data_sources",
        )
        self._seed_from_json(
            f"{seed_dir}/initial_etl_processes.json",
            "model_etl",
            "ETLProcess",
            key="etl_processes",
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

    def _seed_from_json(self, file, module_name, model_name, key=None):
        model_module = import_module(f"biofilter.db.models.{module_name}")
        model_class = getattr(model_module, model_name)

        json_path = os.path.join(os.path.dirname(__file__), file)
        if not os.path.exists(json_path):
            self.logger.log(f"JSON not found: {json_path}", "WARNING")
            return

        with self.get_session() as session:
            with open(json_path, "r") as f:
                data = json.load(f)
            records = data.get(key, data) if key else data

            for item in records:
                # Converter campos datetime (se existirem e forem string)
                for key, value in item.items():
                    if key.endswith("_start") or key.endswith("_end"):
                        if isinstance(value, str):
                            try:
                                item[key] = datetime.fromisoformat(value)
                            except ValueError:
                                self.logger.log(
                                    f"Invalid datetime format in key {key}: {value}",  # noqa E501
                                    "WARNING",
                                )  # noqa: E501

                # Search FK ID from Names
                if "source_system" in item:
                    fk_name = item.pop("source_system")
                    fk_qry = (
                        session.query(
                            import_module(
                                "biofilter.db.models.model_etl"
                            ).SourceSystem  # noqa E501
                        )
                        .filter_by(name=fk_name)
                        .first()
                    )

                    if not fk_qry:
                        self.logger.log(
                            f"Source System not found for name: {fk_name}",
                            "WARNING",  # noqa E501
                        )
                        continue
                    item["source_system_id"] = fk_qry.id

                if "data_source" in item:
                    fk_name = item.pop("data_source")
                    fk_qry = (
                        session.query(
                            import_module(
                                "biofilter.db.models.model_etl"
                            ).DataSource  # noqa E501
                        )
                        .filter_by(name=fk_name)
                        .first()
                    )

                    if not fk_qry:
                        self.logger.log(
                            f"Data Source not found for name: {fk_name}",
                            "WARNING",  # noqa E501
                        )
                        continue
                    item["data_source_id"] = fk_qry.id

                try:
                    session.add(model_class(**item))
                except Exception as e:
                    self.logger.log(f"Failed to add {item}: {e}", "ERROR")
            try:
                session.commit()
                self.logger.log(f"Seeded: {model_name}", "INFO")
            except IntegrityError:
                session.rollback()
                msn = f"{model_name} data already exists. Skipping."
                self.logger.log(msn, "WARNING")

import os
import json
from sqlalchemy.exc import IntegrityError
from biofilter.db.base import Base
from importlib import import_module
from biofilter.utils.db_loader import load_all_models  # ✅ novo import


class CreateDBMixin:
    def create_db(self, overwrite=False):
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
        self._seed_all()

        self.logger.log(f"Database created at {self.db_uri}", "INFO")
        return True

    def _create_tables(self):
        Base.metadata.create_all(self.engine)

    def _seed_all(self):
        self._seed_from_json(
            "seed/initial_config.json", "config_models", "SystemConfig"
        )

        self._seed_from_json(
            "seed/initial_sourcesystems.json",
            "etl_models",
            "SourceSystem",
            key="source_systems",
        )
        self._seed_from_json(
            "seed/initial_datasources.json",
            "etl_models",
            "DataSource",
            key="data_sources",
        )
        self._seed_from_json(
            "seed/initial_etlprocesses.json",
            "etl_models",
            "ETLProcess",
            key="etl_processes",
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
                session.add(model_class(**item))
            try:
                session.commit()
                self.logger.log(f"Seeded: {model_name}", "INFO")
            except IntegrityError:
                session.rollback()
                msn = f"{model_name} data already exists. Skipping."
                self.logger.log(msn, "WARNING")

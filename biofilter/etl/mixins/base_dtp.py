import os
import requests
from sqlalchemy import text
from packaging import version
from pathlib import Path
from typing import Optional
from biofilter.utils.file_hash import compute_file_hash
from biofilter.db.models.config_models import BiofilterMetadata


class DTPBase:
    def http_download(self, url: str, landing_dir: str) -> Path:
        filename = os.path.basename(url)
        local_path = Path(landing_dir) / filename
        os.makedirs(landing_dir, exist_ok=True)

        response = requests.get(url, stream=True)
        if response.status_code != 200:
            msg = f"Failed to download {filename}. HTTP Status: {response.status_code}"  # noqa: E501
            return False, msg

        msg = f"⬇️  Downloading {filename} ..."
        self.logger.log(msg, "INFO")

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        msg = f"Downloaded {filename} to {landing_dir}"
        return True, msg

    def get_md5_from_url_file(self, url_md5: str) -> Optional[str]:

        try:
            response = requests.get(url_md5)
            if response.status_code == 200:
                remote_md5 = response.text.strip().split()[0]
            else:
                remote_md5 = None
        except Exception:
            remote_md5 = None

        return remote_md5

    # File System Management Methods
    def get_path(self, path: str) -> Path:
        raw_path_ds = (
            Path(path) / self.data_source.source_system.name / self.data_source.name
        )  # noqa: E501
        raw_path_ds.mkdir(parents=True, exist_ok=True)
        return raw_path_ds

    def get_raw_file(self, raw_path: str) -> Path:
        raw_path_ds = self.get_path(raw_path)
        filename = Path(self.data_source.source_url).name
        return raw_path_ds / filename

    def check_compatibility(self):
        metadata = (
            self.session.query(BiofilterMetadata)
            .order_by(BiofilterMetadata.id.desc())
            .first()
        )
        if not metadata:
            raise Exception("❌ Database metadata not found. Schema may not be initialized.")

        db_version = metadata.schema_version
        db_v = version.parse(db_version)
        min_v = version.parse(self.compatible_schema_min)
        max_v = version.parse(self.compatible_schema_max) if self.compatible_schema_max else None

        if db_v < min_v or (max_v and db_v > max_v):
            msg = (
                f"❌ Incompatible schema version for {self.dtp_name} v{self.dtp_version}.\n"
                f"   Required: >= {self.compatible_schema_min}"
            )
            if self.compatible_schema_max:
                msg += f" and <= {self.compatible_schema_max}"
            msg += f"\n   Current DB version: {db_version}"
            raise Exception(msg)

    def apply_sqlite_write_optimizations(self):
        """
        Apply SQLite PRAGMA settings to optimize for bulk insert.
        Only applies when the current engine is SQLite.
        """
        if self.session.bind.dialect.name != "sqlite":
            return

        self.logger.log("⚙️ Applying SQLite PRAGMA optimizations for bulk insert", "DEBUG")

        # self.session.execute(text("PRAGMA journal_mode = OFF;"))
        # self.session.execute(text("PRAGMA synchronous = OFF;"))
        self.session.execute(text("PRAGMA journal_mode = WAL;"))         # Melhor desempenho e permite leitura paralela
        self.session.execute(text("PRAGMA synchronous = NORMAL;"))       # Mais rápido que FULL, ainda com segurança razoável
        self.session.execute(text("PRAGMA locking_mode = EXCLUSIVE;"))   # Trava exclusivo para este processo
        self.session.execute(text("PRAGMA temp_store = MEMORY;"))        # Operações temporárias em RAM
        self.session.execute(text("PRAGMA cache_size = -100000"))        # ~100MB de cache
        self.session.execute(text("PRAGMA foreign_keys = OFF;"))         # Ignora FK durante carga
        self.session.commit()

    def reset_sqlite_pragmas(self):
        if self.session.bind.dialect.name != "sqlite":
            return

        self.session.execute(text("PRAGMA journal_mode = DELETE;"))
        self.session.execute(text("PRAGMA synchronous = FULL;"))
        self.session.execute(text("PRAGMA locking_mode = NORMAL;"))
        self.session.execute(text("PRAGMA foreign_keys = ON;"))
        self.session.commit()

    # NOTE: ⚠️ Se quiser cobrir outros bancos no futuro (ex: MySQL, Oracle),
    # seria bom extrair para um IndexManagerMixin mais genérico.
    def create_indexes(self, index_specs: list[tuple[str, list[str]]]):
        """
        Create indexes for the current database engine.
        Accepts a list of tuples: (table_name, [columns])
        """
        if self.session.bind.dialect.name not in ("sqlite", "postgresql"):
            self.logger.log("❌ Unsupported database engine for index creation", "WARNING")
            return

        for table, columns in index_specs:
            index_name = f"idx_{table}_{'_'.join(columns)}"
            col_str = ", ".join(columns)
            sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({col_str});"
            self.logger.log(f"📌 Creating index: {index_name}", "DEBUG")
            self.session.execute(text(sql))

        self.session.commit()

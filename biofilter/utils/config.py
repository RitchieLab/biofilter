import toml
from pathlib import Path
import biofilter


class BiofilterConfig:

    def __init__(self, filename=".biofilter.toml"):
        
        project_root = Path(biofilter.__file__).resolve().parent.parent
        self.path = project_root / filename

        if not self.path.exists():
            raise FileNotFoundError(f"Config file not found: {self.path}")

        self.data = toml.load(self.path)

    def get(self, section, key, default=None):
        return self.data.get(section, {}).get(key, default)

    @property
    def db_uri(self):
        return self.get("database", "db_uri")

    @property
    def etl_root(self):
        return self.get("etl", "data_root")

    @property
    def log_level(self):
        return self.get("logging", "level", "INFO")

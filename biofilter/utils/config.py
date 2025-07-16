import tomllib
from pathlib import Path


def get_db_uri_from_config():
    config_file = Path.cwd() / ".biofilter.toml"
    if config_file.exists():
        with config_file.open("rb") as f:
            config = tomllib.load(f)
        return config.get("database", {}).get("db_uri")
    return None

# import toml
# from pathlib import Path
# import biofilter


# class BiofilterConfig:

#     def __init__(self, filename=".biofilter.toml"):

#         project_root = Path(biofilter.__file__).resolve().parent.parent
#         self.path = project_root / filename

#         if not self.path.exists():
#             raise FileNotFoundError(f"Config file not found: {self.path}")

#         self.data = toml.load(self.path)

#     def get(self, section, key, default=None):
#         return self.data.get(section, {}).get(key, default)

#     @property
#     def db_uri(self):
#         return self.get("database", "db_uri")

#     @property
#     def etl_root(self):
#         return self.get("etl", "data_root")

#     @property
#     def log_level(self):
#         return self.get("logging", "level", "INFO")

from __future__ import annotations

from pathlib import Path
import toml


def find_config_file(
    filename: str = ".biofilter.toml", start_dir: Path | None = None
) -> Path | None:
    """
    Find config file in the current working directory or any parent directory.
    """
    start_dir = start_dir or Path.cwd()
    for p in [start_dir, *start_dir.parents]:
        candidate = p / filename
        if candidate.exists():
            return candidate
    return None


class BiofilterConfig:
    """
    Biofilter configuration loader.

    Search order (default):
    1) ./<filename> (cwd)
    2) ../<filename> (parents)
    """

    def __init__(
        self, filename: str = ".biofilter.toml", path: str | Path | None = None
    ):
        if path is not None:
            self.path = Path(path).expanduser().resolve()
        else:
            found = find_config_file(filename=filename)
            if not found:
                raise FileNotFoundError(
                    f"Config file not found: {filename} (searched from {Path.cwd()})"
                )
            self.path = found

        if not self.path.exists():
            raise FileNotFoundError(f"Config file not found: {self.path}")

        self.data = toml.load(str(self.path))

    def get(self, section, key, default=None):
        return self.data.get(section, {}).get(key, default)

    @property
    def db_uri(self):
        v = self.get("database", "db_uri")
        # treat empty string as not set
        return v if v else None

    @property
    def etl_root(self):
        return self.get("etl", "data_root")

    @property
    def log_level(self):
        return self.get("logging", "level", "INFO")


# TODO (Biofilter 4):
# Derive download_path and processed_path from [etl].data_root
# instead of requiring explicit config entries.
#
# Proposed behavior:
#   download_path  = <etl.data_root>/raw
#   processed_path = <etl.data_root>/processed
#
# This will:
# - Keep the TOML config minimal and DRY
# - Ensure consistent ETL paths
# - Improve `biofilter config show` output
#
# Also consider exposing derived paths explicitly in `config show`
# under a "Derived paths" section.

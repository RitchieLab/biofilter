import os
import requests
from packaging import version
from pathlib import Path
from typing import Optional
from typing import Optional, Dict

# from biofilter.utils.file_hash import compute_file_hash
from biofilter.db.models import BiofilterMetadata, EntityGroup
from biofilter.etl.mixins.base_dtp_turning import DBTuningMixin


class DTPBase(DBTuningMixin):

    # ---FIX START

    # --- Hotfix flags (v3.1.x) ----------------------------------------------
    TRUNCATE_MODE_255: bool = True  # set False after 3.2.0 schema TEXT migration

    # Central limits used only when TRUNCATE_MODE_255 == True
    MAXLEN_ALIAS: int = 255  # alias_value / alias_norm / free-text aliases
    MAXLEN_DESCRIPTION: int = 255  # generic descriptions (Pfam, GO, UniProt, etc.)

    def __init__(self, *args, **kwargs):
        # ...
        self.trunc_metrics: Dict[str, int] = {}  # field_name -> count

    # ----------------------------- Text guards -------------------------------

    @staticmethod
    def _normalize_text(s: Optional[str]) -> Optional[str]:
        """
        Lightweight normalization for alias_norm, etc.
        Adjust if you need ASCII folding or more aggressive rules.
        """
        if s is None:
            return None
        return " ".join(str(s).lower().split())

    # def _bump_trunc(self, field: str) -> None:
    #     self.trunc_metrics[field] = self.trunc_metrics.get(field, 0) + 1
    # TODO: Pensar em como implementar o super().__init__(*args, **kwargs) para as classes princiapis
    def _bump_trunc(self, field: str) -> None:
        if not hasattr(self, "trunc_metrics") or self.trunc_metrics is None:
            self.trunc_metrics = {}
        self.trunc_metrics[field] = self.trunc_metrics.get(field, 0) + 1

    def safe_truncate(
        self, val: Optional[str], maxlen: int, field: str
    ) -> Optional[str]:
        """
        Truncates `val` to `maxlen` only when TRUNCATE_MODE_255 is True.
        Counts truncations in self.trunc_metrics.
        """
        if val is None:
            return None
        v = str(val).strip()
        if self.TRUNCATE_MODE_255 and len(v) > maxlen:
            self._bump_trunc(field)
            return v[:maxlen]
        return v

    # Convenience wrappers for common cases
    def guard_alias_value(self, s: Optional[str]) -> Optional[str]:
        return self.safe_truncate(s, self.MAXLEN_ALIAS, "alias_value")

    def guard_alias_norm(self, s: Optional[str]) -> Optional[str]:
        n = self._normalize_text(s)
        return self.safe_truncate(n, self.MAXLEN_ALIAS, "alias_norm")

    def guard_description(self, s: Optional[str]) -> Optional[str]:
        return self.safe_truncate(s, self.MAXLEN_DESCRIPTION, "description")

    # ----------------------------- ETL hooks ---------------------------------

    def _log_truncation_summary(self):
        if not self.trunc_metrics:
            self.logger.info("Truncation summary: none")
            return
        # compact, stable ordering:
        parts = [f"{k}={v}" for k, v in sorted(self.trunc_metrics.items())]
        self.logger.warning(f"Truncation summary: {', '.join(parts)}")

    # Call this at the end of each load()
    def finalize_load(self):
        self._log_truncation_summary()
        # any other common epilogue

    # ---FIX END

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
            Path(path)
            / self.data_source.source_system.name
            / self.data_source.name  # noqa E501
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
            raise Exception(
                "❌ Database metadata not found. Schema may not be initialized."
            )  # noqa E501

        db_version = metadata.schema_version
        db_v = version.parse(db_version)
        min_v = version.parse(self.compatible_schema_min)
        max_v = (
            version.parse(self.compatible_schema_max)
            if self.compatible_schema_max
            else None
        )  # noqa E501

        if db_v < min_v or (max_v and db_v > max_v):
            msg = (
                f"❌ Incompatible schema version for {self.dtp_name} v{self.dtp_version}.\n"  # noqa E501
                f"   Required: >= {self.compatible_schema_min}"
            )
            if self.compatible_schema_max:
                msg += f" and <= {self.compatible_schema_max}"
            msg += f"\n   Current DB version: {db_version}"
            raise Exception(msg)

    def get_entity_group(self, entity_group):
        if not hasattr(self, "entity_group") or self.entity_group is None:
            group = (
                self.session.query(EntityGroup)
                .filter_by(name=entity_group)
                .first()  # noqa: E501
            )  # noqa: E501
            if not group:
                msg = f"EntityGroup {entity_group} not found in the database."
                # self.logger.log(msg, "ERROR")
                raise ValueError(msg)

            self.entity_group = group.id

            msg = f"EntityGroup ID for {entity_group}  is {self.entity_group}"
            self.logger.log(msg, "DEBUG")

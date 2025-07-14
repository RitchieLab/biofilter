import os
import requests
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

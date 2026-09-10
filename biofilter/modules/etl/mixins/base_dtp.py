import base64
import os
import time
from pathlib import Path
from typing import Dict, Optional

import requests
from packaging import version

# from biofilter.utils.file_hash import compute_file_hash
from biofilter.modules.db.models import BiofilterMetadata, EntityGroup
from biofilter.modules.etl.mixins.base_dtp_turning import DBTuningMixin


class DTPBase(DBTuningMixin):
    TRUNCATE_MODE_255: bool = True
    MAXLEN_ALIAS: int = 255  # alias_value / alias_norm / free-text aliases
    MAXLEN_DESCRIPTION: int = 255  # generic descriptions (Pfam, GO, UniProt, etc.)

    def __init__(self, *args, **kwargs):
        self.trunc_metrics: Dict[str, int] = {}  # field_name -> count

    @staticmethod
    def _normalize_text(s: Optional[str]) -> Optional[str]:
        """
        Lightweight normalization for alias_norm, etc.
        Adjust if you need ASCII folding or more aggressive rules.
        """
        if s is None:
            return None
        return " ".join(str(s).lower().split())

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
        # Commit whatever the last batch left pending. get_or_create_*
        # commits every COMMIT_BATCH_SIZE rows rather than every row, so
        # the tail of a run would otherwise stay uncommitted.
        flush = getattr(self, "flush_pending_writes", None)
        if callable(flush):
            flush()
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

    # ------------------------------------------------------------------
    # Resumable download (large files)
    # ------------------------------------------------------------------
    def remote_file_info(self, url: str) -> Dict[str, Optional[str]]:
        """
        Probe a URL with HEAD and return size, server-side md5 and whether
        the origin honours byte ranges.

        Google Cloud Storage exposes the object's md5 through the
        `x-goog-hash` header (base64), which lets us record a content hash
        without reading a multi-gigabyte file back from disk.
        """
        info: Dict[str, Optional[str]] = {
            "size": None,
            "md5": None,
            "accept_ranges": None,
        }
        try:
            resp = requests.head(url, allow_redirects=True, timeout=60)
        except Exception:
            return info

        if resp.status_code != 200:
            return info

        length = resp.headers.get("Content-Length")
        if length and length.isdigit():
            info["size"] = int(length)

        info["accept_ranges"] = resp.headers.get("Accept-Ranges")

        # `x-goog-hash` may carry several comma-separated algorithms.
        raw_hash = resp.headers.get("x-goog-hash", "")
        for part in raw_hash.split(","):
            part = part.strip()
            if part.startswith("md5="):
                try:
                    info["md5"] = base64.b64decode(part[4:]).hex()
                except Exception:
                    info["md5"] = None
                break

        if not info["md5"]:
            etag = (resp.headers.get("ETag") or "").strip('"')
            # A plain 32-hex ETag is the object md5; multipart ETags
            # (which carry a `-N` suffix) are not, so they are ignored.
            if len(etag) == 32 and all(c in "0123456789abcdef" for c in etag):
                info["md5"] = etag

        return info

    def http_download_resumable(
        self,
        url: str,
        landing_dir: str,
        *,
        max_retries: int = 5,
        chunk_size: int = 8 * 1024 * 1024,
        expected_size: Optional[int] = None,
        expected_md5: Optional[str] = None,
    ) -> tuple:
        """
        Download `url` into `landing_dir`, resuming a partial file instead
        of restarting it.

        Written for the gnomAD VCFs, where a single file is 2-11 GB and a
        dropped connection several hours in must not cost the whole
        transfer. Returns `(ok, message, md5)`.

        A partial file is continued with a `Range` request; a file already
        at the expected size is left alone and reported as complete.
        """
        filename = os.path.basename(url)
        os.makedirs(landing_dir, exist_ok=True)
        local_path = Path(landing_dir) / filename

        total = expected_size
        md5 = expected_md5
        supports_range = True

        if total is None or md5 is None:
            info = self.remote_file_info(url)
            if total is None:
                total = info["size"]
            if md5 is None:
                md5 = info["md5"]
            supports_range = (info["accept_ranges"] or "").lower() == "bytes"

        existing = local_path.stat().st_size if local_path.exists() else 0

        if total is not None and existing == total:
            msg = f"✅ {filename} already complete ({existing:,} bytes)"
            self.logger.log(msg, "INFO")
            return True, msg, md5

        if total is not None and existing > total:
            # Longer than the origin: a truncated or corrupt artifact.
            # Restarting is the only safe recovery.
            self.logger.log(
                f"⚠️  {filename} is larger than the remote file. Restarting.",
                "WARNING",
            )
            local_path.unlink()
            existing = 0

        if existing and not supports_range:
            self.logger.log(
                f"⚠️  Origin does not accept ranges. Restarting {filename}.",
                "WARNING",
            )
            local_path.unlink()
            existing = 0

        attempt = 0
        while attempt < max_retries:
            attempt += 1
            headers = {}
            mode = "wb"
            if existing:
                headers["Range"] = f"bytes={existing}-"
                mode = "ab"
                pct = f" ({100 * existing / total:.1f}%)" if total else ""
                self.logger.log(
                    f"⏭️  Resuming {filename} at {existing:,} bytes{pct}",
                    "INFO",
                )
            else:
                size_note = f" ({total:,} bytes)" if total else ""
                self.logger.log(
                    f"⬇️  Downloading {filename}{size_note}", "INFO"
                )

            try:
                resp = requests.get(
                    url, headers=headers, stream=True, timeout=120
                )
                if resp.status_code not in (200, 206):
                    raise IOError(f"HTTP {resp.status_code}")
                if existing and resp.status_code == 200:
                    # Range was ignored; the body is the whole object.
                    mode = "wb"
                    existing = 0

                with open(local_path, mode) as fh:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if chunk:
                            fh.write(chunk)

                existing = local_path.stat().st_size
                if total is None or existing >= total:
                    msg = f"✅ Downloaded {filename} ({existing:,} bytes)"
                    self.logger.log(msg, "INFO")
                    return True, msg, md5

                self.logger.log(
                    f"⚠️  {filename} incomplete "
                    f"({existing:,}/{total:,}). Retrying.",
                    "WARNING",
                )
            except Exception as exc:  # noqa: BLE001
                existing = (
                    local_path.stat().st_size if local_path.exists() else 0
                )
                self.logger.log(
                    f"⚠️  {filename} failed on attempt {attempt}/"
                    f"{max_retries}: {exc}",
                    "WARNING",
                )
                if attempt < max_retries:
                    time.sleep(min(2 ** attempt, 60))

        msg = (
            f"❌ Failed to download {filename} after {max_retries} attempts "
            f"({existing:,}/{total or 0:,} bytes)"
        )
        return False, msg, md5

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

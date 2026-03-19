from __future__ import annotations

from typing import Iterable, Optional, Union

from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.etl.etl_manager import ETLManager


class ETLComponent(BaseComponent):
    """
    ETL component wrapping ETLManager.

    Important: it always uses the shared Database instance from core.
    """

    def _manager(self) -> ETLManager:
        manager = ETLManager(
            debug_mode=self.core.debug_mode,
            db=self.core.require_db(),
            logger=self.core.logger,
        )
        return manager

    def update(
        self,
        source_system: list | None = None,
        data_sources: list | None = None,
        run_steps: list | None = None,
        force_steps: list | None = None,
    ) -> bool:
        # db = self.require_db()
        self.core.logger.log("🚀 Starting ETL update process...", "INFO")

        manager = self._manager()
        manager.start_process(
            source_system=source_system,
            data_sources=data_sources,
            download_path=self.core.settings.get("download_path", "./downloads"),  # noqa E501
            processed_path=self.core.settings.get("processed_path", "./processed"),  # noqa E501
            run_steps=run_steps,
            force_steps=force_steps,
        )

        self.core.logger.log("✅ ETL update process finished.", "INFO")
        return True

    def update_all(
        self,
        source_system: list[str] | None = None,
        data_sources: list[str] | None = None,
        drop_files_on_success: bool = False,
        only_active: bool = True,
        stop_on_error: bool = False,
    ) -> dict:
        self.core.logger.log("🚀 Starting ETL update-all process...", "INFO")
        manager = self._manager()

        out = manager.start_process_all(
            source_system=source_system,
            data_sources=data_sources,
            download_path=self.core.settings.get("download_path", "./downloads"),  # noqa E501
            processed_path=self.core.settings.get("processed_path", "./processed"),  # noqa E501
            drop_files_on_success=drop_files_on_success,
            only_active=only_active,
            stop_on_error=stop_on_error,
        )

        self.core.logger.log("✅ ETL update-all process finished.", "INFO")
        return out

    def restart(
        self,
        data_source: list[str] | None = None,
        source_system: list[str] | None = None,
        delete_files: bool = False,
    ):
        self.core.logger.log("🔄 Restarting ETL processes...", "INFO")
        manager = self._manager()

        return manager.restart_etl_process(
            data_source=data_source,
            source_system=source_system,
            download_path=self.core.settings.get("download_path", "./downloads"),  # noqa E501
            processed_path=self.core.settings.get("processed_path", "./processed"),  # noqa E501
            delete_files=delete_files,
        )

    def rollback(
        self,
        package_ids: list[int] | None = None,
        data_source: list[str] | None = None,
        source_system: list[str] | None = None,
        delete_files: bool = False,
    ):
        self.core.logger.log("↩️ Rolling back ETL data...", "INFO")
        manager = self._manager()

        return manager.rollback_etl_process(
            package_ids=package_ids,
            data_source=data_source,
            source_system=source_system,
            download_path=self.core.settings.get("download_path", "./downloads"),  # noqa E501
            processed_path=self.core.settings.get("processed_path", "./processed"),  # noqa E501
            delete_files=delete_files,
        )

    def index(
        self,
        groups: Optional[Union[str, Iterable[str]]] = None,
        drop_only: bool = False,
        drop_first: bool = True,
        set_write_mode: bool = True,
        set_read_mode: bool = True,
    ) -> tuple[bool, str]:
        if groups is None:
            index_group = None
        elif isinstance(groups, str):
            index_group = [groups]
        else:
            index_group = list(groups)

        self.core.logger.log("🧱 Starting index rebuild...", "INFO")
        manager = self._manager()

        ok, msg = manager.rebuild_indexes(
            index_group=index_group,
            drop_only=drop_only,
            drop_first=drop_first,
            set_write_mode=set_write_mode,
            set_read_mode=set_read_mode,
        )

        self.core.logger.log(msg, "INFO" if ok else "WARNING")
        return ok, msg

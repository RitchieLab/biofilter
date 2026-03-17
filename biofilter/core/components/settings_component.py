from __future__ import annotations

from biofilter.core.components.base_component import BaseComponent
from biofilter.core.settings_manager import SettingsManager


class SettingsComponent(BaseComponent):
    """
    Settings component. Cached SettingsManager bound to the
    current DB session factory.
    """

    def _get_manager(self) -> SettingsManager:
        db = self.require_db()
        # Cache per BiofilterCore instance
        if self.core._settings_manager is None:
            self.core.logger.log("⚙️  Initializing settings manager...", "INFO")  # noqa E501
            with db.get_session() as session:
                self.core._settings_manager = SettingsManager(session)
        return self.core._settings_manager

    def get(self, key: str, default=None):
        return self._get_manager().get(key, default)

    def set(self, key: str, value):
        return self._get_manager().set(key, value)

from biofilter.db.models import SystemConfig
from sqlalchemy.orm import Session


class SettingsManager:
    def __init__(self, session: Session):
        self.session = session
        self._cache = {}

    def _get_raw(self, key):
        if key in self._cache:
            return self._cache[key]

        config = (
            self.session.query(SystemConfig)
            .filter(SystemConfig.key == key)
            .one_or_none()
        )
        if config:
            self._cache[key] = config
        return config

    def get(self, key):
        config = self._get_raw(key)
        if not config:
            return None

        if config.type == "bool":
            return config.value.lower() == "true"
        elif config.type == "int":
            return int(config.value)
        elif config.type == "float":
            return float(config.value)
        elif config.type == "path":
            return str(config.value)
        return config.value

    def set(self, key, value):
        config = self._get_raw(key)
        if not config:
            raise KeyError(f"Config key '{key}' not found")

        config.value = str(value)
        self.session.commit()
        self._cache[key] = config

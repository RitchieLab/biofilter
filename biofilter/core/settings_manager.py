from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, TypeVar, Union, overload

from sqlalchemy.orm import Session

from biofilter.modules.db.models import SystemConfig


T = TypeVar("T")


class SettingsManager:
    """
    Read/write access to SystemConfig with lightweight caching and typed parsing.

    Notes
    -----
    - Values are stored as strings in SystemConfig.value.
    - SystemConfig.type controls parsing: bool/int/float/path/str (default).
    """

    def __init__(self, session: Session):
        self.session = session
        self._cache: dict[str, Optional[SystemConfig]] = {}

    def _get_raw(self, key: str) -> Optional[SystemConfig]:
        if key in self._cache:
            return self._cache[key]

        config = (
            self.session.query(SystemConfig)
            .filter(SystemConfig.key == key)
            .one_or_none()
        )
        # Cache hits AND misses to avoid repeated queries
        self._cache[key] = config
        return config

    def refresh(self, key: Optional[str] = None) -> None:
        """
        Clear cache for one key or all keys.
        Useful if settings may change outside this manager.
        """
        if key is None:
            self._cache.clear()
        else:
            self._cache.pop(key, None)

    def get(
        self, key: str, default: T | None = None, *, as_path: bool = False
    ) -> T | Any | None:
        """
        Get a config value parsed according to SystemConfig.type.

        Parameters
        ----------
        key:
            SystemConfig.key
        default:
            Returned when key does not exist OR value cannot be parsed.
        as_path:
            If True and type is "path", returns pathlib.Path instead of str.

        Returns
        -------
        Parsed value or default.
        """
        config = self._get_raw(key)
        if not config:
            return default

        raw = config.value
        ctype = (config.type or "").lower().strip()

        try:
            if ctype == "bool":
                # Accept a few common truthy/falsey strings
                val = str(raw).strip().lower()
                if val in {"true", "1", "yes", "y", "on"}:
                    return True  # type: ignore[return-value]
                if val in {"false", "0", "no", "n", "off"}:
                    return False  # type: ignore[return-value]
                return default

            if ctype == "int":
                return int(raw)  # type: ignore[return-value]

            if ctype == "float":
                return float(raw)  # type: ignore[return-value]

            if ctype == "path":
                return Path(str(raw)) if as_path else str(raw)  # type: ignore[return-value]

            # Default: string (or whatever was stored)
            return raw  # type: ignore[return-value]

        except Exception:
            return default

    def require(self, key: str, *, as_path: bool = False) -> Any:
        """
        Same as get(), but raises if missing or unparsable.
        Use for mandatory settings.
        """
        sentinel = object()
        val = self.get(key, default=sentinel, as_path=as_path)
        if val is sentinel:
            raise KeyError(f"Missing or invalid config key: '{key}'")
        return val

    def set(self, key: str, value: Any, commit: bool = True) -> None:
        """
        Set (or create) a SystemConfig key.

        Stores value as string.
        If key does not exist, creates it with inferred type "str".
        """
        config = self._get_raw(key)

        if config is None:
            config = SystemConfig(key=key, value=str(value), type="str")
            self.session.add(config)
        else:
            config.value = str(value)

        if commit:
            self.session.commit()

        # Update cache
        self._cache[key] = config

    def set_typed(self, key: str, value: Any, ctype: str, commit: bool = True) -> None:
        """
        Set value and explicit type (bool/int/float/path/str).
        """
        ctype = (ctype or "str").lower().strip()
        config = self._get_raw(key)

        if config is None:
            config = SystemConfig(key=key, value=str(value), type=ctype)
            self.session.add(config)
        else:
            config.value = str(value)
            config.type = ctype

        if commit:
            self.session.commit()

        self._cache[key] = config

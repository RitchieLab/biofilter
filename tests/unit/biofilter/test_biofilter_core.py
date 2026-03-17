from __future__ import annotations

from pathlib import Path

import pytest

import biofilter.biofilter as bmod


class DummyLogger:
    def __init__(self, *args, **kwargs):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


def test_safe_db_uri_redacts_password():
    uri = "postgresql+psycopg2://admin:secret@localhost:5432/biofilter_dev"
    safe = bmod.BiofilterCore._safe_db_uri(uri)

    assert safe == "postgresql+psycopg2://admin:***@localhost:5432/biofilter_dev"


def test_safe_db_uri_keeps_value_when_no_password():
    uri = "sqlite:///tmp/biofilter.db"
    safe = bmod.BiofilterCore._safe_db_uri(uri)
    assert safe == uri


def test_biofilter_core_loads_db_uri_from_config(monkeypatch):
    class FakeConfig:
        path = Path("/tmp/.biofilter.toml")
        db_uri = "sqlite:///from_config.db"

    monkeypatch.setattr(bmod, "Logger", DummyLogger)
    monkeypatch.setattr(bmod, "BiofilterConfig", FakeConfig)

    core = bmod.BiofilterCore(db_uri=None, debug_mode=False)

    assert core.db_uri == "sqlite:///from_config.db"
    assert core.config_path == "/tmp/.biofilter.toml"


def test_biofilter_core_handles_missing_config(monkeypatch):
    def _raise_config_not_found():
        raise FileNotFoundError

    monkeypatch.setattr(bmod, "Logger", DummyLogger)
    monkeypatch.setattr(bmod, "BiofilterConfig", _raise_config_not_found)

    core = bmod.BiofilterCore(db_uri=None, debug_mode=False)

    assert core.config is None
    assert core.config_path is None


def test_biofilter_core_require_db_raises_when_not_connected(monkeypatch):
    monkeypatch.setattr(bmod, "Logger", DummyLogger)

    class FakeConfig:
        path = Path("/tmp/.biofilter.toml")
        db_uri = None

    monkeypatch.setattr(bmod, "BiofilterConfig", FakeConfig)

    core = bmod.BiofilterCore(db_uri=None, debug_mode=False)
    core.db = None

    with pytest.raises(RuntimeError, match="Database not connected"):
        core.require_db()


def test_biofilter_facade_wires_components_and_autoconnect(monkeypatch):
    monkeypatch.setattr(bmod, "Logger", DummyLogger)

    class FakeConfig:
        path = Path("/tmp/.biofilter.toml")
        db_uri = None

    monkeypatch.setattr(bmod, "BiofilterConfig", FakeConfig)

    class FakeDBComponent:
        def __init__(self, core):
            self.core = core
            self.connect_calls = 0

        def connect(self):
            self.connect_calls += 1

    class FakeSettingsComponent:
        def __init__(self, core):
            self.core = core

    class FakeETLComponent:
        def __init__(self, core):
            self.core = core

    class FakeReportComponent:
        def __init__(self, core):
            self.core = core

    monkeypatch.setattr(bmod, "DBComponent", FakeDBComponent)
    monkeypatch.setattr(bmod, "SettingsComponent", FakeSettingsComponent)
    monkeypatch.setattr(bmod, "ETLComponent", FakeETLComponent)
    monkeypatch.setattr(bmod, "ReportComponent", FakeReportComponent)

    bf = bmod.Biofilter(db_uri="sqlite:///local.db", debug_mode=False)

    assert isinstance(bf.db, FakeDBComponent)
    assert isinstance(bf.settings, FakeSettingsComponent)
    assert isinstance(bf.etl, FakeETLComponent)
    assert isinstance(bf.report, FakeReportComponent)
    assert bf.db.connect_calls == 1
    assert bf.core.db_component is bf.db
    assert bf.core.settings is bf.settings
    assert bf.core.etl is bf.etl
    assert bf.core.report is bf.report


def test_biofilter_facade_does_not_autoconnect_when_db_uri_is_none(monkeypatch):
    monkeypatch.setattr(bmod, "Logger", DummyLogger)

    class FakeConfig:
        path = Path("/tmp/.biofilter.toml")
        db_uri = None

    monkeypatch.setattr(bmod, "BiofilterConfig", FakeConfig)

    class FakeDBComponent:
        def __init__(self, core):
            self.core = core
            self.connect_calls = 0

        def connect(self):
            self.connect_calls += 1

    class FakeSettingsComponent:
        def __init__(self, core):
            self.core = core

    class FakeETLComponent:
        def __init__(self, core):
            self.core = core

    class FakeReportComponent:
        def __init__(self, core):
            self.core = core

    monkeypatch.setattr(bmod, "DBComponent", FakeDBComponent)
    monkeypatch.setattr(bmod, "SettingsComponent", FakeSettingsComponent)
    monkeypatch.setattr(bmod, "ETLComponent", FakeETLComponent)
    monkeypatch.setattr(bmod, "ReportComponent", FakeReportComponent)

    bf = bmod.Biofilter(db_uri=None, debug_mode=False)

    assert bf.db.connect_calls == 0
    assert repr(bf) == "<Biofilter(db_uri=None)>"

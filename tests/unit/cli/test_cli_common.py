from __future__ import annotations

import click
import pytest

import biofilter.api.cli.common as cmod


class DummyCtx:
    def __init__(self, obj=None):
        self.obj = obj


def test_clean_db_uri_none_or_blank_or_value():
    assert cmod._clean_db_uri(None) is None
    assert cmod._clean_db_uri("   ") is None
    assert cmod._clean_db_uri(" sqlite:///x.db ") == "sqlite:///x.db"


def test_try_resolve_db_uri_prefers_cli_value():
    assert cmod.try_resolve_db_uri("sqlite:///from_cli.db") == "sqlite:///from_cli.db"


def test_try_resolve_db_uri_from_config(monkeypatch):
    class FakeConfig:
        db_uri = "sqlite:///from_config.db"

    monkeypatch.setattr(cmod, "BiofilterConfig", FakeConfig)

    assert cmod.try_resolve_db_uri(None) == "sqlite:///from_config.db"


def test_try_resolve_db_uri_handles_missing_config(monkeypatch):
    class MissingConfig:
        def __init__(self):
            raise FileNotFoundError

    monkeypatch.setattr(cmod, "BiofilterConfig", MissingConfig)

    assert cmod.try_resolve_db_uri(None) is None


def test_resolve_db_uri_returns_value_or_raises(monkeypatch):
    monkeypatch.setattr(cmod, "try_resolve_db_uri", lambda v: "sqlite:///ok.db")
    assert cmod.resolve_db_uri(None) == "sqlite:///ok.db"

    monkeypatch.setattr(cmod, "try_resolve_db_uri", lambda v: None)
    with pytest.raises(click.UsageError, match="DB not set"):
        cmod.resolve_db_uri(None)


def test_get_ctx_db_uri_and_debug():
    assert cmod.get_ctx_db_uri(DummyCtx({"db_uri": " sqlite:///a.db "})) == "sqlite:///a.db"  # noqa E501
    assert cmod.get_ctx_db_uri(DummyCtx({"db_uri": "   "})) is None
    assert cmod.get_ctx_db_uri(DummyCtx(None)) is None

    assert cmod.get_ctx_debug(DummyCtx({"debug": True})) is True
    assert cmod.get_ctx_debug(DummyCtx({"debug": False})) is False
    assert cmod.get_ctx_debug(DummyCtx(None)) is False


def test_require_db_uri_priority(monkeypatch):
    calls = []

    def fake_resolve_db_uri(v):
        calls.append(v)
        return "sqlite:///resolved.db"

    monkeypatch.setattr(cmod, "resolve_db_uri", fake_resolve_db_uri)

    ctx = DummyCtx({"db_uri": "sqlite:///global.db"})
    out = cmod.require_db_uri(ctx, local_db_uri="sqlite:///local.db")
    assert out == "sqlite:///resolved.db"
    assert calls[-1] == "sqlite:///local.db"

    out = cmod.require_db_uri(ctx, local_db_uri="   ")
    assert out == "sqlite:///resolved.db"
    assert calls[-1] == "sqlite:///global.db"


def test_local_and_global_db_uri_option_decorators():
    @cmod.local_db_uri_option
    def local_cmd(db_uri):
        return db_uri

    @cmod.global_db_uri_option
    def global_cmd(db_uri):
        return db_uri

    local_opt_names = {p.name for p in local_cmd.__click_params__}
    global_opt_names = {p.name for p in global_cmd.__click_params__}

    assert "db_uri" in local_opt_names
    assert "db_uri" in global_opt_names

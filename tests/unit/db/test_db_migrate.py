from __future__ import annotations

from types import SimpleNamespace

import pytest

import biofilter.modules.db.migrate as migmod


def _status(*, current, head="head_rev", is_versioned=True):
    return migmod.MigrationStatus(
        script_location="/tmp/alembic",
        head=head,
        current=current,
        is_versioned=is_versioned,
    )


def test_get_head_revision_returns_single_head(monkeypatch):
    monkeypatch.setattr(
        migmod.ScriptDirectory,
        "from_config",
        lambda cfg: SimpleNamespace(get_heads=lambda: ["rev_123"]),
    )

    assert migmod.get_head_revision("/tmp/alembic") == "rev_123"


def test_get_head_revision_raises_for_multiple_heads(monkeypatch):
    monkeypatch.setattr(
        migmod.ScriptDirectory,
        "from_config",
        lambda cfg: SimpleNamespace(get_heads=lambda: ["rev_a", "rev_b"]),
    )

    with pytest.raises(RuntimeError, match="Expected single Alembic head"):
        migmod.get_head_revision("/tmp/alembic")


def test_run_migration_requires_engine_and_db_uri():
    with pytest.raises(ValueError, match="engine is required"):
        migmod.run_migration(engine=None, db_uri="sqlite:///x")

    with pytest.raises(ValueError, match="db_uri is required"):
        migmod.run_migration(engine=object(), db_uri=None)


def test_run_migration_status_prints_and_returns_true(monkeypatch, capsys):
    monkeypatch.setattr(migmod, "get_status", lambda engine, db_uri: _status(current="r1"))

    ok = migmod.run_migration(engine=object(), db_uri="sqlite:///x", action="status")

    assert ok is True
    out = capsys.readouterr().out
    assert "Alembic status" in out
    assert "Repo head" in out
    assert "DB revision" in out


def test_run_migration_stamp_head_refuses_versioned_without_force(monkeypatch):
    monkeypatch.setattr(
        migmod,
        "get_status",
        lambda engine, db_uri: _status(current="old_rev", is_versioned=True),
    )

    with pytest.raises(RuntimeError, match="Refusing to stamp"):
        migmod.run_migration(
            engine=object(),
            db_uri="sqlite:///x",
            action="stamp-head",
            force=False,
        )


def test_run_migration_stamp_head_executes_stamp_and_mirror(monkeypatch):
    monkeypatch.setattr(
        migmod,
        "get_status",
        lambda engine, db_uri: _status(current=None, head="rev_head", is_versioned=False),
    )
    monkeypatch.setattr(migmod, "_make_alembic_config", lambda script_location, db_uri: "CFG")

    calls = {}
    monkeypatch.setattr(
        migmod.command,
        "stamp",
        lambda cfg, rev: calls.update({"cfg": cfg, "rev": rev}),
    )
    monkeypatch.setattr(
        migmod,
        "_mirror_revision_to_metadata",
        lambda session_factory, rev: calls.update({"mirror_session_factory": session_factory, "mirror_rev": rev}),
    )

    ok = migmod.run_migration(
        session_factory="SESSION_FACTORY",
        engine=object(),
        db_uri="sqlite:///x",
        action="stamp-head",
    )

    assert ok is True
    assert calls["cfg"] == "CFG"
    assert calls["rev"] == "rev_head"
    assert calls["mirror_session_factory"] == "SESSION_FACTORY"
    assert calls["mirror_rev"] == "rev_head"


def test_run_migration_dry_run_calls_alembic_upgrade_with_sql(monkeypatch):
    monkeypatch.setattr(migmod, "get_status", lambda engine, db_uri: _status(current="r1"))
    monkeypatch.setattr(migmod, "_make_alembic_config", lambda script_location, db_uri: "CFG")

    calls = {}
    monkeypatch.setattr(
        migmod.command,
        "upgrade",
        lambda cfg, target, sql=False: calls.update(
            {"cfg": cfg, "target": target, "sql": sql}
        ),
    )

    ok = migmod.run_migration(
        engine=object(),
        db_uri="sqlite:///x",
        action="dry-run",
        target="head",
    )

    assert ok is True
    assert calls == {"cfg": "CFG", "target": "head", "sql": True}


def test_run_migration_upgrade_noop_when_up_to_date(monkeypatch):
    monkeypatch.setattr(
        migmod,
        "get_status",
        lambda engine, db_uri: _status(current="rev_head", head="rev_head", is_versioned=True),
    )

    called = {"upgrade": False}

    def _should_not_be_called(*args, **kwargs):
        called["upgrade"] = True

    monkeypatch.setattr(migmod.command, "upgrade", _should_not_be_called)

    ok = migmod.run_migration(engine=object(), db_uri="sqlite:///x", action="upgrade")

    assert ok is True
    assert called["upgrade"] is False


def test_run_migration_upgrade_raises_for_unversioned_db_without_force(monkeypatch):
    monkeypatch.setattr(
        migmod,
        "get_status",
        lambda engine, db_uri: _status(current=None, head="rev_head", is_versioned=False),
    )

    with pytest.raises(RuntimeError, match="not Alembic-versioned"):
        migmod.run_migration(
            engine=object(),
            db_uri="sqlite:///x",
            action="upgrade",
            force=False,
        )


def test_run_migration_upgrade_executes_and_mirrors_latest_revision(monkeypatch):
    statuses = iter(
        [
            _status(current="rev_old", head="rev_head", is_versioned=True),
            _status(current="rev_new", head="rev_head", is_versioned=True),
        ]
    )
    monkeypatch.setattr(migmod, "get_status", lambda engine, db_uri: next(statuses))
    monkeypatch.setattr(migmod, "_make_alembic_config", lambda script_location, db_uri: "CFG")

    calls = {"upgrade": None, "mirror": None}
    monkeypatch.setattr(
        migmod.command,
        "upgrade",
        lambda cfg, target: calls.__setitem__("upgrade", (cfg, target)),
    )
    monkeypatch.setattr(
        migmod,
        "_mirror_revision_to_metadata",
        lambda session_factory, rev: calls.__setitem__("mirror", (session_factory, rev)),
    )

    ok = migmod.run_migration(
        session_factory="SESSION_FACTORY",
        engine=object(),
        db_uri="sqlite:///x",
        action="upgrade",
        target="head",
    )

    assert ok is True
    assert calls["upgrade"] == ("CFG", "head")
    assert calls["mirror"] == ("SESSION_FACTORY", "rev_new")

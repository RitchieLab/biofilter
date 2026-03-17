from __future__ import annotations

import importlib
import click
from click.testing import CliRunner

mmod = importlib.import_module("biofilter.api.cli.main")


def test_version_flag_prints_version_and_db(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(mmod, "current_version", "9.9.9-test")
    monkeypatch.setattr(mmod, "try_resolve_db_uri", lambda v: "sqlite:///ctx.db")

    result = runner.invoke(mmod.main, ["--version"])

    assert result.exit_code == 0, result.output
    assert "biofilter 9.9.9-test" in result.output
    assert "DB: sqlite:///ctx.db" in result.output


def test_main_without_subcommand_shows_help_and_active_db(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(mmod, "try_resolve_db_uri", lambda v: "sqlite:///active.db")

    result = runner.invoke(mmod.main, [])

    assert result.exit_code == 0, result.output
    assert "Biofilter 4 CLI - Omics Knowledge Platform" in result.output
    assert "Active DB: sqlite:///active.db" in result.output


def test_main_without_subcommand_shows_not_set_when_no_db(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(mmod, "try_resolve_db_uri", lambda v: None)

    result = runner.invoke(mmod.main, [])

    assert result.exit_code == 0, result.output
    assert "Active DB: <not set>" in result.output


def test_main_stores_global_options_in_context(monkeypatch):
    runner = CliRunner()
    seen = {}

    @click.command("dummy")
    @click.pass_context
    def dummy(ctx):
        seen["db_uri"] = (ctx.obj or {}).get("db_uri")
        seen["debug"] = (ctx.obj or {}).get("debug")
        click.echo("dummy ok")

    # temporarily register helper subcommand
    mmod.main.add_command(dummy)
    try:
        result = runner.invoke(
            mmod.main,
            ["--db-uri", "sqlite:///global.db", "--debug", "dummy"],
        )
    finally:
        mmod.main.commands.pop("dummy", None)

    assert result.exit_code == 0, result.output
    assert "dummy ok" in result.output
    assert seen == {"db_uri": "sqlite:///global.db", "debug": True}

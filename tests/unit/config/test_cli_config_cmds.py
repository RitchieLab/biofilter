from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner
import pytest
import toml

import biofilter.api.cli.config_cmds as cmod


def test_parse_key_valid():
    section, name = cmod._parse_key("database.db_uri")
    assert section == "database"
    assert name == "db_uri"


@pytest.mark.parametrize("key", ["database", ".db_uri", "database.", "   "])
def test_parse_key_invalid(key):
    with pytest.raises(Exception):
        cmod._parse_key(key)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("true", True),
        ("false", False),
        ("10", 10),
        ("3.14", 3.14),
        ('"abc"', "abc"),
        ("'xyz'", "xyz"),
        ("hello", "hello"),
    ],
)
def test_infer_value(raw, expected):
    assert cmod._infer_value(raw) == expected


def test_config_init_creates_file_and_prefills_values(tmp_path):
    runner = CliRunner()
    out_dir = tmp_path / "cfg"

    result = runner.invoke(
        cmod.config,
        [
            "init",
            "--path",
            str(out_dir),
            "--db-uri",
            "sqlite:///custom.db",
            "--data-root",
            "./my_data",
        ],
    )

    assert result.exit_code == 0, result.output
    cfg_path = out_dir / ".biofilter.toml"
    assert cfg_path.exists()
    content = cfg_path.read_text(encoding="utf-8")
    assert 'db_uri = "sqlite:///custom.db"' in content
    assert 'data_root = "./my_data"' in content


def test_config_init_fails_if_file_exists_without_force(tmp_path):
    runner = CliRunner()
    cfg_path = tmp_path / ".biofilter.toml"
    cfg_path.write_text("x", encoding="utf-8")

    result = runner.invoke(
        cmod.config,
        ["init", "--path", str(tmp_path)],
    )

    assert result.exit_code != 0
    assert "already exists" in result.output


def test_config_get_returns_value(tmp_path):
    runner = CliRunner()
    cfg = {
        "database": {"db_uri": "sqlite:///x.db"},
        "etl": {"data_root": "./biofilter_data"},
    }
    cfg_path = tmp_path / ".biofilter.toml"
    cfg_path.write_text(toml.dumps(cfg), encoding="utf-8")

    result = runner.invoke(
        cmod.config,
        ["get", "database.db_uri", "--path", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    assert "sqlite:///x.db" in result.output


def test_config_get_fails_when_key_not_set(tmp_path):
    runner = CliRunner()
    cfg_path = tmp_path / ".biofilter.toml"
    cfg_path.write_text(toml.dumps({"database": {}}), encoding="utf-8")

    result = runner.invoke(
        cmod.config,
        ["get", "database.db_uri", "--path", str(tmp_path)],
    )

    assert result.exit_code != 0
    assert "is not set" in result.output


def test_config_set_updates_value_with_inferred_type(tmp_path):
    runner = CliRunner()
    cfg_path = tmp_path / ".biofilter.toml"
    cfg_path.write_text(
        toml.dumps(
            {
                "database": {"db_uri": "sqlite:///old.db"},
                "etl": {"allow_parallel": False},
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        cmod.config,
        ["set", "etl.allow_parallel", "true", "--path", str(tmp_path)],
    )

    assert result.exit_code == 0, result.output
    updated = toml.load(str(cfg_path))
    assert updated["etl"]["allow_parallel"] is True


def test_config_set_fails_when_section_does_not_exist(tmp_path):
    runner = CliRunner()
    cfg_path = tmp_path / ".biofilter.toml"
    cfg_path.write_text(toml.dumps({"database": {}}), encoding="utf-8")

    result = runner.invoke(
        cmod.config,
        ["set", "missing.key", "value", "--path", str(tmp_path)],
    )

    assert result.exit_code != 0
    assert "Section [missing] does not exist" in result.output


def test_find_config_file_raises_with_hint_for_custom_path(tmp_path):
    with pytest.raises(Exception) as exc:
        cmod._find_config_file(str(tmp_path))
    assert "looked in" in str(exc.value)

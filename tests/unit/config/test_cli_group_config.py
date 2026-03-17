from __future__ import annotations

from click.testing import CliRunner

import biofilter.api.cli.groups.config as gmod


def test_config_show_uses_cli_db_override(monkeypatch):
    runner = CliRunner()

    class FakeConfig:
        def __init__(self):
            self.path = "/tmp/.biofilter.toml"
            self.db_uri = "sqlite:///from_config.db"
            self.download_path = "./raw"
            self.processed_path = "./processed"

    monkeypatch.setattr(gmod, "BiofilterConfig", FakeConfig)

    result = runner.invoke(
        gmod.config,
        ["show"],
        obj={"db_uri": "sqlite:///from_cli.db"},
    )

    assert result.exit_code == 0, result.output
    assert "Config file:" in result.output
    assert "/tmp/.biofilter.toml" in result.output
    assert "db_uri: sqlite:///from_cli.db   (from CLI)" in result.output
    assert "download_path: ./raw" in result.output
    assert "processed_path: ./processed" in result.output


def test_config_show_handles_missing_config_file(monkeypatch):
    runner = CliRunner()

    class MissingConfig:
        def __init__(self):
            raise FileNotFoundError

    monkeypatch.setattr(gmod, "BiofilterConfig", MissingConfig)

    result = runner.invoke(gmod.config, ["show"], obj={})

    assert result.exit_code == 0, result.output
    assert "Config file:" in result.output
    assert "<not found>" in result.output
    assert "db_uri: <not set>" in result.output
    assert "download_path: <not set>" in result.output
    assert "processed_path: <not set>" in result.output

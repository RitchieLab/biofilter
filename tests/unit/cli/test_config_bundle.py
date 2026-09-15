"""`[database] bundle` in .biofilter.toml."""

from __future__ import annotations

from pathlib import Path

import pytest

from biofilter.utils.config import BiofilterConfig


def _config(tmp_path: Path, body: str) -> BiofilterConfig:
    path = tmp_path / ".biofilter.toml"
    path.write_text(body)
    return BiofilterConfig(path=path)


class TestBundleKey:
    def test_absolute_path_passes_through(self, tmp_path):
        cfg = _config(tmp_path, '[database]\nbundle = "/data/bundles/20260914"\n')
        assert cfg.bundle == "/data/bundles/20260914"

    def test_relative_path_resolves_against_the_config_not_the_cwd(
        self, tmp_path, monkeypatch
    ):
        """
        The config is found by walking up from wherever you are, so a
        relative path in it has to mean the same thing from the project
        root and from a notebook two levels down.
        """
        (tmp_path / "bundles" / "20260914").mkdir(parents=True)
        cfg = _config(tmp_path, '[database]\nbundle = "./bundles/20260914"\n')

        elsewhere = tmp_path / "notebooks" / "deep"
        elsewhere.mkdir(parents=True)
        monkeypatch.chdir(elsewhere)

        assert cfg.bundle == str(tmp_path / "bundles" / "20260914")

    def test_absent_or_empty_is_none(self, tmp_path):
        assert _config(tmp_path, "[database]\ndb_uri = \"sqlite:///x.db\"\n").bundle is None
        assert _config(tmp_path, '[database]\nbundle = ""\n').bundle is None

    def test_db_uri_still_works_on_its_own(self, tmp_path):
        cfg = _config(tmp_path, '[database]\ndb_uri = "sqlite:///x.db"\n')
        assert cfg.db_uri == "sqlite:///x.db"


class TestPrecedence:
    def test_a_configured_bundle_wins_over_a_configured_db_uri(
        self, tmp_path, monkeypatch
    ):
        """
        Same precedence --bundle has over --db-uri: reports read bundles,
        and a db_uri left over from the PostgreSQL era should not shadow
        one that is right there.
        """
        import biofilter.api.cli.common as cmod

        bundle = tmp_path / "bundles" / "20260914"
        bundle.mkdir(parents=True)
        cfg_path = tmp_path / ".biofilter.toml"
        cfg_path.write_text(
            '[database]\n'
            f'bundle = "{bundle}"\n'
            'db_uri = "postgresql+psycopg2://u:p@localhost/db"\n'
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("BIOFILTER_BUNDLE", raising=False)
        monkeypatch.delenv("BIOFILTER_DB_URI", raising=False)

        assert cmod.try_resolve_db_uri(None) == f"parquet://{bundle}"

    def test_an_explicit_db_uri_still_wins_over_the_config(self, tmp_path, monkeypatch):
        import biofilter.api.cli.common as cmod

        (tmp_path / ".biofilter.toml").write_text(
            f'[database]\nbundle = "{tmp_path}"\n'
        )
        monkeypatch.chdir(tmp_path)

        assert cmod.try_resolve_db_uri("sqlite:///explicit.db") == "sqlite:///explicit.db"

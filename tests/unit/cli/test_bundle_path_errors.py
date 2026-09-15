"""
What you are told when a bundle path is wrong.

The failure used to surface from deep in the engine as "Database not
found at duckdb:///:memory:" — naming the translated URI, which the
caller never typed and cannot act on.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from biofilter.utils.bundle_path import (
    BundlePathError,
    bundle_to_uri,
    check_bundle_path,
)


def _bundle(root: Path) -> Path:
    (root / "tables").mkdir(parents=True)
    (root / "manifest.json").write_text(json.dumps({"manifest_version": 2}))
    return root


class TestAccepts:
    def test_a_real_bundle(self, tmp_path):
        check_bundle_path(bundle_to_uri(_bundle(tmp_path / "20260914")))

    def test_anything_that_is_not_a_bundle_uri(self):
        check_bundle_path("postgresql+psycopg2://u:p@host/db")
        check_bundle_path("sqlite:///nowhere.db")
        check_bundle_path("")


class TestRejects:
    def test_a_missing_directory_names_the_path_you_typed(self, tmp_path):
        with pytest.raises(BundlePathError, match="No such directory"):
            check_bundle_path(bundle_to_uri(tmp_path / "typo"))

    def test_a_missing_directory_lists_the_bundles_that_are_there(self, tmp_path):
        _bundle(tmp_path / "20260910")
        _bundle(tmp_path / "20260914")

        with pytest.raises(BundlePathError) as caught:
            check_bundle_path(bundle_to_uri(tmp_path / "20260915"))

        message = str(caught.value)
        assert "20260910" in message and "20260914" in message

    def test_pointing_at_tables_says_to_go_up_one(self, tmp_path):
        """The usual mistake, and the docs used to teach it."""
        root = _bundle(tmp_path / "20260914")

        with pytest.raises(BundlePathError) as caught:
            check_bundle_path(bundle_to_uri(root / "tables"))

        message = str(caught.value)
        assert "tables/ directory" in message
        assert str(root) in message

    def test_a_directory_without_a_manifest(self, tmp_path):
        (tmp_path / "somewhere").mkdir()
        with pytest.raises(BundlePathError, match="no manifest.json"):
            check_bundle_path(bundle_to_uri(tmp_path / "somewhere"))

    def test_an_interrupted_build_says_so(self, tmp_path):
        """tables/ but no manifest: the build did not finish."""
        (tmp_path / "half" / "tables").mkdir(parents=True)
        with pytest.raises(BundlePathError, match="incomplete or interrupted"):
            check_bundle_path(bundle_to_uri(tmp_path / "half"))

    def test_a_file_is_not_a_directory(self, tmp_path):
        target = tmp_path / "bundle.txt"
        target.write_text("not a bundle")
        with pytest.raises(BundlePathError, match="Not a directory"):
            check_bundle_path(bundle_to_uri(target))


class TestThroughTheFacade:
    def test_biofilter_raises_before_the_engine_is_reached(self, tmp_path):
        from biofilter.biofilter import Biofilter

        with pytest.raises(BundlePathError) as caught:
            Biofilter(bundle=tmp_path / "nope")

        # The path the caller wrote, not duckdb:///:memory:
        assert "nope" in str(caught.value)
        assert "duckdb" not in str(caught.value)


class TestThroughTheCLI:
    def test_the_cli_shows_a_usage_error_not_a_traceback(self, tmp_path):
        from click.testing import CliRunner

        from biofilter.api.cli.main import main

        root = _bundle(tmp_path / "20260914")
        result = CliRunner().invoke(
            main, ["--bundle", str(root / "tables"), "report", "list"]
        )

        assert result.exit_code != 0
        assert "Traceback" not in result.output
        assert "Point at the bundle itself" in result.output

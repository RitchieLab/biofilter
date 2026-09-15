"""`Biofilter(bundle=...)`: the folder a user has, not a URI to memorise."""

from __future__ import annotations

from pathlib import Path

import pytest

from biofilter.biofilter import Biofilter


@pytest.fixture
def bundle_dir(fixture_bundle):
    return fixture_bundle


def test_a_bundle_path_opens_without_a_uri(bundle_dir):
    bf = Biofilter(bundle=bundle_dir)

    assert bf.core.db_uri == f"parquet://{bundle_dir}"
    assert "annotation_master_gene" in [r["name"] for r in bf.report.list()]


def test_a_string_path_works_too(bundle_dir):
    assert Biofilter(bundle=str(bundle_dir)).core.db_uri.startswith("parquet://")


def test_a_relative_path_is_resolved(bundle_dir, monkeypatch):
    monkeypatch.chdir(bundle_dir.parent)
    bf = Biofilter(bundle=bundle_dir.name)

    assert bf.core.db_uri == f"parquet://{bundle_dir}"


def test_bundle_wins_over_db_uri(bundle_dir):
    """
    Same precedence as --bundle on the CLI: the more specific request
    wins, rather than being resolved silently in the other direction.
    """
    bf = Biofilter(bundle=bundle_dir, db_uri="sqlite:///ignored.db")

    assert bf.core.db_uri == f"parquet://{bundle_dir}"


def test_no_bundle_leaves_db_uri_alone(tmp_path):
    """Without a bundle, db_uri passes through untranslated."""
    from biofilter.biofilter import BiofilterCore

    uri = f"sqlite:///{tmp_path / 'x.db'}"
    assert BiofilterCore(db_uri=uri).db_uri == uri


def test_reports_run_through_the_facade(bundle_dir):
    bf = Biofilter(bundle=bundle_dir)
    result = bf.report.run("annotation_master_gene", input_data=["TP53"])

    assert result.num_rows == 1
    assert result.provenance["bundle_id"] == "fixturebundle0001"

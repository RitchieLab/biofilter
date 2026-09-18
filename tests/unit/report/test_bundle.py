"""Bundle.open: what it resolves, what it refuses."""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pyarrow as pa
import pytest

from biofilter.modules.report import (
    Bundle,
    BundleIncomplete,
    BundleNotFound,
    BundleVersionError,
)


class TestResolution:
    def test_partitioned_table_is_one_view_over_every_file(self, fixture_bundle):
        with Bundle.open(fixture_bundle) as bundle:
            table = bundle.tables["variant_masters"]
            assert len(table.files) == 2
            assert table.rows == 5
            assert table.partitioned

    def test_empty_parent_file_beside_the_directory_is_ignored(self, fixture_bundle):
        """
        The regression this module exists to prevent.

        An undeclared `variant_masters.parquet` sits next to the
        partition directory with the 4.2.x schema and no rows. A reader
        that scans the directory serves zero rows; a reader that reads
        the manifest serves five.
        """
        assert (fixture_bundle / "tables" / "variant_masters.parquet").is_file()
        with Bundle.open(fixture_bundle) as bundle:
            rows = bundle.con.execute(
                "SELECT count(*) FROM variant_masters"
            ).fetchone()[0]
            assert rows == 5
            columns = {
                r[0]
                for r in bundle.con.execute("DESCRIBE variant_masters").fetchall()
            }
            assert "position" in columns
            assert "position_start" not in columns

    def test_single_file_table_resolves(self, fixture_bundle):
        with Bundle.open(fixture_bundle) as bundle:
            assert bundle.tables["gene_masters"].rows == 3
            assert not bundle.tables["gene_masters"].partitioned

    def test_identity_comes_from_the_manifest(self, fixture_bundle):
        with Bundle.open(fixture_bundle) as bundle:
            assert bundle.bundle_id == "fixturebundle0001"
            assert bundle.biofilter_version == "4.3.0"
            assert bundle.build_record() is not None


class TestRefusal:
    def test_directory_without_manifest(self, tmp_path):
        (tmp_path / "tables").mkdir()
        with pytest.raises(BundleNotFound, match="manifest.json"):
            Bundle.open(tmp_path)

    def test_future_manifest_version_says_to_update(self, fixture_bundle):
        path = fixture_bundle / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["manifest_version"] = 99
        path.write_text(json.dumps(manifest))

        with pytest.raises(BundleVersionError, match="Update Biofilter"):
            Bundle.open(fixture_bundle)

    def test_missing_declared_file_is_caught_on_open(self, fixture_bundle):
        (fixture_bundle / "tables" / "gene_masters.parquet").unlink()
        with pytest.raises(BundleIncomplete, match="missing"):
            Bundle.open(fixture_bundle)

    def test_truncated_file_is_caught_by_size(self, fixture_bundle):
        target = fixture_bundle / "tables" / "gene_masters.parquet"
        target.write_bytes(target.read_bytes()[:-50])
        with pytest.raises(BundleIncomplete, match="size mismatch"):
            Bundle.open(fixture_bundle)

    def test_require_names_what_is_missing(self, fixture_bundle):
        with Bundle.open(fixture_bundle) as bundle:
            bundle.require("gene_masters", "variant_masters")
            with pytest.raises(BundleIncomplete, match="variant_gwas"):
                bundle.require("gene_masters", "variant_gwas")


class TestIsolation:
    def test_views_are_shared_but_scratch_space_is_not(self, fixture_bundle):
        """
        One connection per report execution, over the same catalog.

        The legacy layer could not do this: a parquet bundle forced
        StaticPool, so every session shared one connection and two
        reports collided on the same temp-table name.
        """
        with Bundle.open(fixture_bundle) as bundle:
            first, second = bundle.cursor(), bundle.cursor()

            assert first.execute("SELECT count(*) FROM gene_masters").fetchone()[0] == 3
            assert second.execute("SELECT count(*) FROM gene_masters").fetchone()[0] == 3

            first.register("scratch", pa.table({"v": [1, 2]}))
            assert first.execute("SELECT count(*) FROM scratch").fetchone()[0] == 2
            with pytest.raises(Exception):
                second.execute("SELECT count(*) FROM scratch").fetchone()


class TestReleaseMismatch:
    """
    A bundle built by another release opens, and says so.

    The hard refusal belongs to `manifest_version` — a layout this
    release cannot read. A *release* difference is different: it usually
    works, and refusing would make an archived bundle unreadable by the
    only install at hand. So it warns and records, and the record is what
    makes a result recognisable later as one produced across a boundary.
    """

    @staticmethod
    def _restamp(bundle_root: Path, version: str) -> None:
        path = bundle_root / "manifest.json"
        manifest = json.loads(path.read_text())
        manifest["biofilter_version"] = version
        path.write_text(json.dumps(manifest))

    def test_same_release_is_silent(self, fixture_bundle):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with Bundle.open(fixture_bundle) as bundle:
                assert bundle.version_mismatch is None
        assert not [
            w for w in caught if "was built by Biofilter" in str(w.message)
        ]

    def test_older_bundle_warns_and_records(self, fixture_bundle):
        self._restamp(fixture_bundle, "4.2.0")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with Bundle.open(fixture_bundle) as bundle:
                assert bundle.version_mismatch is not None
                assert "4.2.0" in bundle.version_mismatch
                # It names the way out, not just the problem.
                assert "--schema" in bundle.version_mismatch
        assert [
            w for w in caught if "was built by Biofilter" in str(w.message)
        ]

    def test_patch_release_is_not_a_mismatch(self, fixture_bundle):
        """Only major.minor is compared; a patch fixes code, not schema."""
        self._restamp(fixture_bundle, "4.3.99")
        with Bundle.open(fixture_bundle) as bundle:
            assert bundle.version_mismatch is None

    def test_unparseable_version_is_not_a_mismatch(self, fixture_bundle):
        """An unreadable stamp is not evidence of disagreement."""
        self._restamp(fixture_bundle, "unknown")
        with Bundle.open(fixture_bundle) as bundle:
            assert bundle.version_mismatch is None

    def test_mismatch_reaches_the_result(self, fixture_bundle):
        from biofilter.modules.report.report_manager import ReportManager

        self._restamp(fixture_bundle, "4.2.0")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with Bundle.open(fixture_bundle) as bundle:
                manager = ReportManager(bundle=bundle)
                result = manager.run("resolve_entity", input_data=["TP53"])

        assert result.provenance["version_mismatch"] is not None
        assert "4.2.0" in result.provenance["version_mismatch"]

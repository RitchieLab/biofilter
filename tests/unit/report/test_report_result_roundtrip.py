"""
Saving a result and getting it back unchanged.

`write()` exports for reading elsewhere and is lossy on purpose — CSV
cannot hold a list column, so `flatten_for_csv` renders it as JSON text.
`save()` / `load()` is the other job: lose nothing, and stay usable
later, when the bundle that produced the rows may be gone.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from biofilter.modules.report import Bundle, ReportManager
from biofilter.modules.report.result import (
    Artifact,
    PRIMARY_TABLE,
    ReportResult,
)


def _result(**provenance) -> ReportResult:
    """A result with a nested column, which is what CSV cannot carry."""
    table = pa.table(
        {
            "gene": pa.array(["TP53", "BRCA1"]),
            "aliases": pa.array([["p53", "LFS1"], ["BRCC1"]]),
            "score": pa.array([1.5, None], pa.float64()),
        }
    )
    base = {"report": "demo", "bundle_id": "b0001", "params": {"input_data": ["TP53"]}}
    base.update(provenance)
    return ReportResult(table=table, provenance=base)


class TestTheRoundTrip:
    def test_the_table_comes_back_identical(self, tmp_path):
        original = _result()
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.table.equals(original.table)

    def test_a_nested_column_survives(self, tmp_path):
        """The thing a CSV export cannot do."""
        original = _result()
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.table.column("aliases").to_pylist() == [["p53", "LFS1"], ["BRCC1"]]

    def test_a_null_stays_null_rather_than_becoming_a_string(self, tmp_path):
        original = _result()
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.table.column("score").to_pylist() == [1.5, None]

    def test_provenance_comes_back_whole(self, tmp_path):
        original = _result(coverage={"missing_tables": ["variant_gtex"]})
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.provenance["report"] == "demo"
        assert back.provenance["params"] == {"input_data": ["TP53"]}
        assert back.provenance["coverage"] == {"missing_tables": ["variant_gtex"]}


class TestMoreThanOneTable:
    def test_every_table_is_saved_and_named(self, tmp_path):
        original = _result()
        original.extra_tables["rejected"] = pa.table({"input": pa.array(["NOPE"])})
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert set(back.tables) == {PRIMARY_TABLE, "rejected"}
        assert back.extra_tables["rejected"].column("input").to_pylist() == ["NOPE"]

    def test_the_main_table_stays_the_main_table(self, tmp_path):
        original = _result()
        original.extra_tables["rejected"] = pa.table({"input": pa.array(["NOPE"])})
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.table.equals(original.table)
        assert "rejected" not in back.table.column_names

    def test_tables_lists_the_main_one_first(self):
        result = _result()
        result.extra_tables["second"] = pa.table({"x": pa.array([1])})

        assert list(result.tables) == [PRIMARY_TABLE, "second"]

    def test_a_manifest_missing_its_main_table_is_refused(self, tmp_path):
        _result().save(tmp_path / "saved")
        manifest_path = tmp_path / "saved" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["primary_table"] = "not_written"
        manifest_path.write_text(json.dumps(manifest))

        with pytest.raises(ValueError, match="names 'not_written'"):
            ReportResult.load(tmp_path / "saved")


class TestWhatASavedResultIsFor:
    def test_it_can_be_opened_as_a_bundle_and_queried(self, tmp_path):
        """
        The point of the directory layout. Reusing a result mostly means
        querying it, and a saved result is shaped like a bundle so the
        reader Biofilter already has can open it.
        """
        original = _result()
        original.extra_tables["rejected"] = pa.table({"input": pa.array(["NOPE"])})
        original.save(tmp_path / "saved")

        with Bundle.open(tmp_path / "saved") as bundle:
            rows = bundle.con.execute(
                f'SELECT gene FROM "{PRIMARY_TABLE}" ORDER BY gene'
            ).fetchall()
            assert [r[0] for r in rows] == ["BRCA1", "TP53"]
            assert bundle.con.execute("SELECT count(*) FROM rejected").fetchone()[0] == 1

    def test_loading_says_whether_the_source_bundle_is_still_there(self, tmp_path):
        original = _result(bundle_root=str(tmp_path / "gone"))
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")
        source = back.provenance["source_bundle"]

        assert source["still_present"] is False
        assert source["bundle_id"] == "b0001"
        assert "not at that path any more" in source["means"]

    def test_a_present_bundle_is_reported_as_present(self, tmp_path, fixture_bundle):
        original = _result(bundle_root=str(fixture_bundle))
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.provenance["source_bundle"]["still_present"] is True

    def test_a_missing_bundle_is_not_an_error(self, tmp_path):
        """
        A result outliving its bundle is the normal case, and the reason
        to save one. The rows are unchanged either way.
        """
        original = _result(bundle_root="/nowhere/at/all")
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.table.equals(original.table)


class TestArtifacts:
    def test_they_come_back_pointing_inside_the_saved_result(self, tmp_path):
        log = tmp_path / "rejected.csv"
        log.write_text("input\nNOPE\n")
        original = _result()
        original.artifacts.append(
            Artifact(name="rejected", path=log, kind="csv", description="dropped rows")
        )
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert [a.name for a in back.artifacts] == ["rejected"]
        assert back.artifacts[0].path.parent == tmp_path / "saved"
        assert back.artifacts[0].description == "dropped rows"


class TestRefusals:
    def test_saving_over_something_is_refused(self, tmp_path):
        target = tmp_path / "saved"
        target.mkdir()
        (target / "something.txt").write_text("mine")

        with pytest.raises(FileExistsError, match="already holds something"):
            _result().save(target)

    def test_overwrite_makes_it_explicit(self, tmp_path):
        target = tmp_path / "saved"
        target.mkdir()
        (target / "something.txt").write_text("mine")

        assert _result().save(target, overwrite=True) == target

    def test_loading_a_directory_that_is_not_a_result(self, tmp_path):
        (tmp_path / "empty").mkdir()

        with pytest.raises(FileNotFoundError, match="not a saved result"):
            ReportResult.load(tmp_path / "empty")

    def test_a_declared_table_that_is_not_there(self, tmp_path):
        _result().save(tmp_path / "saved")
        (tmp_path / "saved" / "tables" / f"{PRIMARY_TABLE}.parquet").unlink()

        with pytest.raises(FileNotFoundError, match="which is not"):
            ReportResult.load(tmp_path / "saved")


class TestAgainstARealReport:
    def test_a_report_result_survives_the_round_trip(self, fixture_bundle, tmp_path):
        with Bundle.open(fixture_bundle) as bundle:
            result = ReportManager(bundle=bundle).run(
                "annotate_gene", input_data=["TP53"]
            )

        result.save(tmp_path / "saved")
        back = ReportResult.load(tmp_path / "saved")

        assert back.table.equals(result.table)
        assert back.provenance["report"] == "annotate_gene"
        assert back.provenance["bundle_id"] == "fixturebundle0001"

    def test_the_parameters_that_produced_it_travel_with_it(
        self, fixture_bundle, tmp_path
    ):
        with Bundle.open(fixture_bundle) as bundle:
            result = ReportManager(bundle=bundle).run(
                "annotate_gene", input_data=["TP53"], match_mode="like"
            )

        result.save(tmp_path / "saved")
        back = ReportResult.load(tmp_path / "saved")

        assert back.provenance["params"]["match_mode"] == "like"
        assert back.provenance["params"]["input_data"] == ["TP53"]


class TestWarnings:
    """
    A report that copes with a problem leaves nothing behind unless it
    says so. The log reaches whoever is watching; the provenance reaches
    whoever opens the result later, who never has the log.
    """

    def test_a_result_always_has_the_key(self, fixture_bundle):
        with Bundle.open(fixture_bundle) as bundle:
            result = ReportManager(bundle=bundle).run(
                "annotate_gene", input_data=["TP53"]
            )

        assert result.provenance["warnings"] == []

    def test_a_warning_carries_its_context(self, fixture_bundle):
        from biofilter.modules.report.reports.base_report import ReportBase

        with Bundle.open(fixture_bundle) as bundle:
            report = ReportBase(bundle=bundle)
            report.warn("something was dropped", rows=7, tables=["a", "b"])
            report.close()

        assert report.run_warnings == [
            {"message": "something was dropped", "rows": 7, "tables": ["a", "b"]}
        ]

    def test_context_is_rendered_so_it_can_be_written_as_json(self, fixture_bundle):
        from pathlib import Path

        from biofilter.modules.report.reports.base_report import ReportBase

        with Bundle.open(fixture_bundle) as bundle:
            report = ReportBase(bundle=bundle)
            report.warn("a path", where=Path("/tmp/x"))
            report.close()

        json.dumps(report.run_warnings)  # would raise if it were not
        assert report.run_warnings[0]["where"] == "/tmp/x"

    def test_warnings_survive_the_round_trip(self, tmp_path):
        original = _result(warnings=[{"message": "watch out", "rows": 3}])
        original.save(tmp_path / "saved")

        back = ReportResult.load(tmp_path / "saved")

        assert back.provenance["warnings"] == [{"message": "watch out", "rows": 3}]


class TestEmit:
    def test_a_report_can_attach_a_second_table(self, fixture_bundle):
        from biofilter.modules.report.reports.base_report import ReportBase

        with Bundle.open(fixture_bundle) as bundle:
            report = ReportBase(bundle=bundle)
            report.emit("rejected", pa.table({"input": pa.array(["NOPE"])}))
            report.close()

        assert set(report.extra_tables) == {"rejected"}

    def test_it_cannot_be_called_result(self, fixture_bundle):
        from biofilter.modules.report.reports.base_report import ReportBase

        with Bundle.open(fixture_bundle) as bundle:
            report = ReportBase(bundle=bundle)
            with pytest.raises(ValueError, match="main table's name"):
                report.emit("result", pa.table({"x": pa.array([1])}))
            report.close()

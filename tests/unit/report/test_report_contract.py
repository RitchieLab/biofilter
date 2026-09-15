"""The contract: input registration, result shape, provenance, discovery."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from biofilter.modules.report import Bundle, ReportManager, ReportResult
from biofilter.modules.report.result import make_provenance


@pytest.fixture
def bundle(fixture_bundle):
    with Bundle.open(fixture_bundle) as b:
        yield b


class TestDiscovery:
    def test_listing_reports_needs_no_bundle(self):
        """
        `report list` answers a question about the installed package.

        The legacy manager opened a database connection to answer it,
        so listing failed wherever no database was reachable.
        """
        names = [r["name"] for r in ReportManager().list_reports()]
        assert "template" in names

    def test_explain_needs_no_bundle(self):
        guide = ReportManager().explain("template")
        assert "example report" in guide.lower()

    def test_running_without_a_bundle_says_so(self):
        with pytest.raises(ValueError, match="needs a bundle"):
            ReportManager().run("template", input_data=["TP53"])

    def test_unknown_report_lists_what_exists(self):
        with pytest.raises(ValueError, match="Report not found"):
            ReportManager().resolve("no_such_report")


class TestInputRegistration:
    def test_input_is_joined_not_interpolated(self, bundle):
        manager = ReportManager(bundle=bundle)
        result = manager.run("template", input_data=["TP53", "brca1", "NOPE"])

        rows = result.table.to_pylist()
        assert [r["input_value"] for r in rows] == ["NOPE", "TP53", "brca1"]
        found = {r["input_value"]: r["found"] for r in rows}
        assert found == {"TP53": True, "brca1": True, "NOPE": False}

    def test_quotes_in_input_cannot_break_the_query(self, bundle):
        """
        Input never reaches the SQL text, so this is data, not syntax.
        """
        hostile = ["'; DROP TABLE gene_masters; --", 'TP53"']
        result = ReportManager(bundle=bundle).run("template", input_data=hostile)

        assert result.num_rows == 2
        assert all(r["found"] is False for r in result.table.to_pylist())
        # The table it tried to drop is still there.
        assert bundle.con.execute("SELECT count(*) FROM gene_masters").fetchone()[0] == 3

    def test_input_file_is_read(self, bundle, tmp_path):
        path = tmp_path / "genes.txt"
        path.write_text("TP53\nEGFR\n\n")
        result = ReportManager(bundle=bundle).run("template", input_data=path)
        assert result.num_rows == 2


class TestResult:
    def test_provenance_records_the_bundle(self, bundle):
        result = ReportManager(bundle=bundle).run("template", input_data=["TP53"])
        assert result.provenance["bundle_id"] == "fixturebundle0001"
        assert result.provenance["report"] == "template"
        assert result.provenance["params"]["input_data"] == ["TP53"]
        assert result.provenance["rows"] == 1

    def test_csv_export_writes_a_provenance_sidecar(self, bundle, tmp_path):
        result = ReportManager(bundle=bundle).run("template", input_data=["TP53"])
        written = result.write(tmp_path / "out.csv")

        assert [p.name for p in written] == ["out.csv", "out.csv.provenance.json"]
        assert "TP53" in written[0].read_text()

        side = json.loads(written[1].read_text())
        assert side["bundle_id"] == "fixturebundle0001"
        assert side["result_file"] == "out.csv"

    def test_parquet_export_carries_provenance_inside_the_file(self, bundle, tmp_path):
        import pyarrow.parquet as pq

        result = ReportManager(bundle=bundle).run("template", input_data=["TP53"])
        result.write(tmp_path / "out.parquet")

        meta = pq.ParquetFile(tmp_path / "out.parquet").schema_arrow.metadata
        carried = json.loads(meta[b"biofilter_provenance"])
        assert carried["bundle_id"] == "fixturebundle0001"

    def test_dataframe_carries_provenance_in_attrs(self, bundle):
        result = ReportManager(bundle=bundle).run("template", input_data=["TP53"])
        assert result.to_pandas().attrs["bundle_id"] == "fixturebundle0001"

    def test_artifacts_default_empty_and_are_listed_when_present(self, tmp_path):
        result = ReportResult(
            table=pa.table({"a": [1]}),
            provenance=make_provenance(report="x", bundle_id="b"),
        )
        assert result.artifacts == []

        log = tmp_path / "rejected.json"
        log.write_text("[]")
        result.artifacts.append(
            __import__(
                "biofilter.modules.report.result", fromlist=["Artifact"]
            ).Artifact(name="rejected", path=log, kind="log")
        )
        side = json.loads(result.write_provenance(tmp_path / "out.csv").read_text())
        assert side["artifacts"][0]["file"] == "rejected.json"


class TestRequires:
    def test_a_report_naming_a_missing_table_fails_before_running(self, bundle):
        from biofilter.modules.report.bundle import BundleIncomplete

        manager = ReportManager(bundle=bundle)
        cls = manager.get_class("template")
        original = cls.requires
        cls.requires = ("gene_masters", "variant_gtex")
        try:
            with pytest.raises(BundleIncomplete, match="variant_gtex"):
                manager.run("template", input_data=["TP53"])
        finally:
            cls.requires = original

"""
The skip check has to see a *file*, not a directory.

`processed/<system>/<source>/` keeps its subdirectories after the bundle
build moves the parquet out of them. Treating any directory entry as
output meant a source whose files had already gone into a previous
bundle still looked complete, its transform was skipped as
not-applicable, and the next bundle was published without the table.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from biofilter.modules.etl.etl_manager import ETLManager


@pytest.fixture
def manager():
    return ETLManager.__new__(ETLManager)


@pytest.fixture
def data_source():
    return SimpleNamespace(
        name="alphamissense",
        source_system=SimpleNamespace(name="AlphaMissense"),
    )


def _source_dir(base, ds):
    return base / ds.source_system.name / ds.name


def test_a_file_at_any_depth_counts_as_output(manager, data_source, tmp_path):
    directory = _source_dir(tmp_path, data_source) / "predictions"
    directory.mkdir(parents=True)
    (directory / "variant_alphamissense_chr21.parquet").write_bytes(b"PAR1")

    assert manager._step_output_exists(str(tmp_path), data_source) is True


def test_an_emptied_subdirectory_does_not(manager, data_source, tmp_path):
    (_source_dir(tmp_path, data_source) / "predictions").mkdir(parents=True)

    assert manager._step_output_exists(str(tmp_path), data_source) is False


def test_a_missing_directory_does_not(manager, data_source, tmp_path):
    assert manager._step_output_exists(str(tmp_path), data_source) is False


def test_an_unknown_base_path_cannot_force_a_rerun(manager, data_source):
    """A missing setting must not make every step look incomplete."""
    assert manager._step_output_exists(None, data_source) is True


# ---------------------------------------------------------------------------
# Source in the file name
# ---------------------------------------------------------------------------


def test_the_file_name_carries_the_source(tmp_path):
    """
    A bundle's tables/ holds files from several callsets side by side,
    and the table name says what the rows are, not where they came from.
    """
    import pyarrow as pa

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    schema = pa.schema([pa.field("chromosome", pa.int32())])
    sink = ChromosomeFileWriter(
        tmp_path, schema, "variant_masters", source="gnomad"
    )

    assert sink.path_for(21).name == "variant_masters_gnomad_chr21.parquet"


def test_a_table_already_named_for_its_source_takes_no_tag(tmp_path):
    import pyarrow as pa

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    schema = pa.schema([pa.field("chromosome", pa.int32())])
    sink = ChromosomeFileWriter(tmp_path, schema, "variant_alphamissense")

    assert sink.path_for(21).name == "variant_alphamissense_chr21.parquet"


def test_the_table_and_source_are_stamped_into_the_footer(tmp_path):
    """
    So the build maps a file back to its table by reading it. No rule
    splits `variant_masters_gnomad_chr21` into table and source.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    schema = pa.schema([pa.field("chromosome", pa.int32())])
    with ChromosomeFileWriter(
        tmp_path, schema, "variant_masters", source="gnomad"
    ) as sink:
        sink.write_rows([{"chromosome": 21}])

    metadata = pq.ParquetFile(sink.path_for(21)).schema_arrow.metadata
    assert metadata[b"biofilter_table"] == b"variant_masters"
    assert metadata[b"biofilter_source"] == b"gnomad"


def test_the_chemical_source_is_off_by_default():
    """
    `chebi` is the only chemical source. With it inactive, `bundle plan`
    writes it with include:false and nothing produces `chemical_masters`,
    which then does not appear in the bundle at all.
    """
    import json
    from pathlib import Path

    seed = json.loads(
        Path("biofilter/modules/db/seed/initial_data_sources.json").read_text()
    )
    chemical = [
        r for r in seed["data_sources"] if r.get("data_type") == "Chemical"
    ]

    assert [r["name"] for r in chemical] == ["chebi"]
    assert all(r["active"] is False for r in chemical)

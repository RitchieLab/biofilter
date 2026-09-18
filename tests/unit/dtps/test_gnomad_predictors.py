"""
The in-silico predictors come out of the INFO block, not the CSQ block.

gnomAD publishes eight of them as INFO fields of the exome and genome
sites VCFs — `Number=1, Type=Float`, so one value per site rather than
one per transcript. The joint callset carries none of them: its 664 INFO
fields are frequency, count and QC only, which is why the 4.2.x model
declared `cadd_phred`, `revel_max` and the rest on `variant_masters` and
nothing ever filled them.

The header lines below are copied from
`gnomad.exomes.v4.1.1.sites.chr21.vcf.bgz`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("cyvcf2")

from cyvcf2 import VCF  # noqa: E402

from biofilter.modules.etl.dtps.dtp_variant_gnomad_vep import DTP  # noqa: E402

PREDICTORS = [
    "cadd_raw_score", "cadd_phred", "revel_max", "sift_max",
    "polyphen_max", "spliceai_ds_max", "pangolin_largest_ds", "phylop",
]

HEADER = """##fileformat=VCFv4.2
##INFO=<ID=AC,Number=A,Type=Integer,Description="Alternate allele count">
##INFO=<ID=AF,Number=A,Type=Float,Description="Alternate allele frequency">
##INFO=<ID=cadd_raw_score,Number=1,Type=Float,Description="Raw CADD score">
##INFO=<ID=cadd_phred,Number=1,Type=Float,Description="CADD Phred-like score">
##INFO=<ID=revel_max,Number=1,Type=Float,Description="Maximum REVEL score">
##INFO=<ID=sift_max,Number=1,Type=Float,Description="SIFT score">
##INFO=<ID=polyphen_max,Number=1,Type=Float,Description="PolyPhen score">
##INFO=<ID=spliceai_ds_max,Number=1,Type=Float,Description="SpliceAI max delta">
##INFO=<ID=pangolin_largest_ds,Number=1,Type=Float,Description="Pangolin largest delta">
##INFO=<ID=phylop,Number=1,Type=Float,Description="phyloP conservation">
##contig=<ID=chr21>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
"""

RECORDS = [
    # fully scored missense
    "chr21\t5030088\trs1\tG\tA\t.\tPASS\t"
    "AC=12;AF=0.0001;cadd_raw_score=3.21;cadd_phred=22.4;revel_max=0.71;"
    "sift_max=0.02;polyphen_max=0.97;spliceai_ds_max=0.01;"
    "pangolin_largest_ds=0.02;phylop=7.65",
    # conservation only — the common case away from coding sequence
    "chr21\t5030099\t.\tC\tT\t.\tPASS\tAC=3;AF=0.00002;phylop=-1.2",
    # no predictor at all
    "chr21\t5030120\t.\tA\tG\t.\tPASS\tAC=1;AF=0.00001",
]


@pytest.fixture
def vcf_path(tmp_path):
    path = tmp_path / "sites.vcf"
    path.write_text(HEADER + "\n".join(RECORDS) + "\n")
    return path


def test_the_schema_declares_every_predictor_as_a_float(tmp_path):
    """
    Declared, not inferred. Parquet types a column from the first batch
    it sees, so a predictor null across that batch would be typed `null`
    and reject every later batch that carried a value — the bug that
    already bit `mane_plus_clinical`.
    """
    import pyarrow as pa

    schema = DTP._predictor_schema(PREDICTORS)

    assert schema.names == [
        "chromosome", "position", "reference_allele", "alternate_allele",
        *PREDICTORS,
    ]
    # No `callset`. Exomes and genomes never disagree on these — the
    # score comes from the reference and the allele — so the column only
    # bought a duplicate row for every variant present in both.
    assert "callset" not in schema.names
    for name in PREDICTORS:
        assert schema.field(name).type == pa.float64()


def test_predictors_are_read_from_the_info_block(vcf_path):
    """Every one of the eight, off one record."""
    record = next(iter(VCF(str(vcf_path))))
    scores = {n: DTP._scalar(record.INFO.get(n)) for n in PREDICTORS}

    assert scores == {
        "cadd_raw_score": pytest.approx(3.21),
        "cadd_phred": pytest.approx(22.4),
        "revel_max": pytest.approx(0.71),
        "sift_max": pytest.approx(0.02),
        "polyphen_max": pytest.approx(0.97),
        "spliceai_ds_max": pytest.approx(0.01),
        "pangolin_largest_ds": pytest.approx(0.02),
        "phylop": pytest.approx(7.65),
    }


def test_a_partly_scored_record_keeps_what_it_has(vcf_path):
    """Most of the genome is not coding; phyloP alone is still a row."""
    records = list(VCF(str(vcf_path)))
    scores = {n: DTP._scalar(records[1].INFO.get(n)) for n in PREDICTORS}

    assert scores["phylop"] == pytest.approx(-1.2)
    assert all(scores[n] is None for n in PREDICTORS if n != "phylop")
    assert any(v is not None for v in scores.values())


def test_a_record_with_no_score_is_not_a_row(vcf_path):
    """
    The transform skips these. A row of nulls keyed by a variant says
    nothing, and there are hundreds of millions of them.
    """
    records = list(VCF(str(vcf_path)))
    scores = {n: DTP._scalar(records[2].INFO.get(n)) for n in PREDICTORS}

    assert all(v is None for v in scores.values())
    assert not any(v is not None for v in scores.values())


def test_the_config_enables_exactly_the_fields_gnomad_publishes():
    cfg = json.loads(
        Path(
            "biofilter/modules/etl/dtps/config/dtp_variant_gnomad_vep.json"
        ).read_text()
    )
    block = cfg["predictors"]

    assert block["enabled"] is True
    assert block["table_name"] == "variant_predictions"
    assert [f["name"] for f in block["fields"] if f["load"]] == PREDICTORS
    # `filters` here only bounds the intermediate; the real filter is
    # the semi-join against variant_masters.
    assert block["filters"]["min_ac"] == 3


# ---------------------------------------------------------------------------
# The rsID dedupe pass
# ---------------------------------------------------------------------------


def test_dedupe_keeps_the_table_stamp_in_the_footer():
    """
    The pass rewrites each chromosome file in place. Written with
    DuckDB's `COPY ... TO ... (FORMAT parquet)` it came back with no
    key-value metadata, so the build could no longer read which table
    the file belonged to and fell back to parsing the name — which turns
    `variant_rsid_gnomad_chr22` into the table `variant_rsid_gnomad`.
    Caught by a real build, not by a unit test, which is why this exists.
    """
    import tempfile
    from pathlib import Path

    import pyarrow.parquet as pq

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    with tempfile.TemporaryDirectory() as tmp:
        with ChromosomeFileWriter(
            Path(tmp), DTP._rsid_schema(), "variant_rsid", source="gnomad"
        ) as sink:
            duplicated = {
                "chromosome": 22, "position": 100,
                "reference_allele": "A", "alternate_allele": "G",
                "rsid": "rs1",
            }
            sink.write_rows([duplicated, dict(duplicated), {
                "chromosome": 22, "position": 200,
                "reference_allele": "C", "alternate_allele": "T",
                "rsid": "rs2",
            }])

        kept = DTP._dedupe_files(sink)
        path = sink.path_for(22)

        # The duplicate really is collapsed.
        assert kept == 2
        assert pq.ParquetFile(path).metadata.num_rows == 2

        # And the stamp survives it.
        metadata = pq.ParquetFile(path).schema_arrow.metadata
        assert metadata[b"biofilter_table"] == b"variant_rsid"
        assert metadata[b"biofilter_source"] == b"gnomad"


def test_a_variant_in_both_callsets_yields_one_row():
    """
    The dedupe serves the predictor table as well as the rsID map. On
    chr22, 901,714 of 15,551,628 variants are in both callsets and all
    of them carry identical scores; undeduped, a join against
    `variant_masters` returned 3,383,651 rows for 2,889,803 variants.
    """
    import tempfile
    from pathlib import Path

    import pyarrow.parquet as pq

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    fields = ["cadd_phred", "revel_max"]
    with tempfile.TemporaryDirectory() as tmp:
        with ChromosomeFileWriter(
            Path(tmp), DTP._predictor_schema(fields),
            "variant_predictions", source="gnomad",
        ) as sink:
            # the same variant, read once from each callset
            both = {
                "chromosome": 22, "position": 100,
                "reference_allele": "A", "alternate_allele": "G",
                "cadd_phred": 22.4, "revel_max": 0.71,
            }
            only_genomes = {
                "chromosome": 22, "position": 200,
                "reference_allele": "C", "alternate_allele": "T",
                "cadd_phred": 3.1, "revel_max": None,
            }
            sink.write_rows([both, dict(both), only_genomes])

        kept = DTP._dedupe_files(sink)

        assert kept == 2
        table = pq.read_table(sink.path_for(22))
        assert table.num_rows == 2
        assert "callset" not in table.column_names


# ---------------------------------------------------------------------------
# Filtering against variant_masters
# ---------------------------------------------------------------------------


def _predictor_sink(tmp, fields):
    from pathlib import Path

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    return ChromosomeFileWriter(
        Path(tmp), DTP._predictor_schema(fields),
        "variant_predictions", source="gnomad",
    )


def _row(pos, cadd=1.0):
    return {
        "chromosome": 22, "position": pos,
        "reference_allele": "A", "alternate_allele": "G",
        "cadd_phred": cadd, "revel_max": None,
    }


def _master(tmp, positions):
    """A stand-in for what the joint DTP wrote for this chromosome."""
    from pathlib import Path

    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(tmp) / "variant_masters_gnomad_chr22.parquet"
    pq.write_table(pa.table({
        "chromosome": [22] * len(positions),
        "position": list(positions),
        "reference_allele": ["A"] * len(positions),
        "alternate_allele": ["G"] * len(positions),
    }), path)
    return path


FIELDS = ["cadd_phred", "revel_max"]


def test_predictors_are_cut_to_the_variants_the_bundle_holds():
    """
    gnomAD scores every variant it publishes — on chr22, five times more
    than the bundle carries. A predictor for a variant nothing can join
    to is dead weight.
    """
    import tempfile

    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmp:
        with _predictor_sink(tmp, FIELDS) as sink:
            sink.write_rows([_row(100), _row(200), _row(300)])
        master = _master(tmp, [100, 300])

        kept = DTP._reduce_predictor_files(sink, FIELDS, master)

        assert kept == 2
        table = pq.read_table(sink.path_for(22))
        assert sorted(table.column("position").to_pylist()) == [100, 300]


def test_a_variant_in_both_callsets_still_collapses_to_one_row():
    import tempfile

    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmp:
        with _predictor_sink(tmp, FIELDS) as sink:
            sink.write_rows([_row(100), _row(100)])
        master = _master(tmp, [100])

        assert DTP._reduce_predictor_files(sink, FIELDS, master) == 1
        assert pq.ParquetFile(sink.path_for(22)).metadata.num_rows == 1


def test_effects_are_cut_to_the_same_variants():
    """
    All rows of a kept variant survive together — the table has ~14 per
    variant — which is why this is a semi-join, not a grouping.
    """
    import tempfile
    from pathlib import Path

    import pyarrow as pa
    import pyarrow.parquet as pq

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    schema = pa.schema([
        pa.field("chromosome", pa.int32()),
        pa.field("position", pa.int64()),
        pa.field("reference_allele", pa.string()),
        pa.field("alternate_allele", pa.string()),
        pa.field("consequence", pa.string()),
    ])

    def effect(pos, consequence):
        return {
            "chromosome": 22, "position": pos,
            "reference_allele": "A", "alternate_allele": "G",
            "consequence": consequence,
        }

    with tempfile.TemporaryDirectory() as tmp:
        with ChromosomeFileWriter(
            Path(tmp), schema, "variant_molecular_effects", source="gnomad",
        ) as writer:
            writer.write_rows([
                effect(100, "missense_variant"),
                effect(100, "intron_variant"),
                effect(100, "splice_region_variant"),
                effect(200, "missense_variant"),
            ])
        master = _master(tmp, [100])

        kept = DTP._filter_effects_to_master(writer, master)

        assert kept == 3
        table = pq.read_table(writer.path_for(22))
        assert set(table.column("position").to_pylist()) == {100}


def test_the_pre_filter_cannot_lose_a_variant_the_joint_kept():
    """
    `AC_joint = AC_exomes + AC_genomes`, so a joint AC of 5 implies at
    least 3 in one callset. 3 is the lowest pre-filter that loses
    nothing; 5 lost 1.6% of the bundle's variants on chr22.
    """
    cfg = json.loads(
        Path(
            "biofilter/modules/etl/dtps/config/dtp_variant_gnomad_vep.json"
        ).read_text()
    )

    for pre in (cfg["filters"]["min_ac"],
                cfg["predictors"]["filters"]["min_ac"]):
        assert pre is None or pre <= 3


def test_the_config_names_the_source_it_filters_against():
    cfg = json.loads(
        Path(
            "biofilter/modules/etl/dtps/config/dtp_variant_gnomad_vep.json"
        ).read_text()
    )

    assert cfg["variant_master"]["source"] == "gnomad_joint_chr{chrom}"
    # The AC-sum proxy is gone: the join is exact.
    assert "min_ac_combined" not in cfg["predictors"]


# ---------------------------------------------------------------------------
# One threshold, one place
# ---------------------------------------------------------------------------


def _guard(joint_min, vep_min, pred_min=None, monkeypatch=None):
    """Run the bound check against a synthetic joint threshold."""
    from biofilter.modules.etl.dtps import dtp_variant_gnomad_vep as mod

    def fake_load(name, override):
        assert name == "dtp_variant_gnomad_joint"
        return {"filters": {"min_ac": joint_min}}

    monkeypatch.setattr(mod, "load_field_config", fake_load)
    cfg = {"predictors": {"filters": {"min_ac": pred_min}}}
    dtp = DTP.__new__(DTP)
    dtp._check_prefilter_bound(cfg, {"min_ac": vep_min})


def test_the_bound_follows_the_joint_threshold(monkeypatch):
    """A joint bar of 5 needs only 3 in one callset, so 3 is allowed."""
    _guard(joint_min=5, vep_min=3, pred_min=3, monkeypatch=monkeypatch)


def test_a_pre_filter_above_the_bound_is_refused(monkeypatch):
    """
    At 5 against the joint's 5 this left 45,974 chr22 variants — 1.6% of
    the bundle — with no VEP annotation at all, and said nothing.
    """
    with pytest.raises(ValueError, match="drop variants the joint callset keeps"):
        _guard(joint_min=5, vep_min=5, monkeypatch=monkeypatch)


def test_lowering_the_joint_threshold_tightens_the_bound(monkeypatch):
    """
    The borderline case the guard exists for: at a joint bar of 3, a
    variant can be 2 + 1 and reach 2 in no callset, so a pre-filter of 3
    would drop it. Nothing about this side changed — the other end of
    the coupling moved.
    """
    with pytest.raises(ValueError):
        _guard(joint_min=3, vep_min=3, monkeypatch=monkeypatch)
    _guard(joint_min=3, vep_min=2, pred_min=2, monkeypatch=monkeypatch)


def test_no_joint_threshold_means_nothing_to_bound(monkeypatch):
    _guard(joint_min=None, vep_min=9, monkeypatch=monkeypatch)


def test_null_keeps_everything_and_is_always_safe(monkeypatch):
    _guard(joint_min=5, vep_min=None, pred_min=None, monkeypatch=monkeypatch)


# ---------------------------------------------------------------------------
# Severity ranking
# ---------------------------------------------------------------------------


def _effects_writer(tmp, consequences):
    from pathlib import Path

    import pyarrow as pa

    from biofilter.modules.etl.parquet_sink import ChromosomeFileWriter

    schema = pa.schema([
        pa.field("chromosome", pa.int32()),
        pa.field("position", pa.int64()),
        pa.field("consequence", pa.string()),
    ])
    writer = ChromosomeFileWriter(
        Path(tmp), schema, "variant_molecular_effects", source="gnomad",
    )
    with writer:
        writer.write_rows([
            {"chromosome": 22, "position": 100 + i, "consequence": c}
            for i, c in enumerate(consequences)
        ])
    return writer


def test_an_unranked_term_is_named(tmp_path, caplog):
    """
    `variant_consequences` is joined by name and is what orders the
    bundle. A term VEP emits that the seed does not list joins to
    nothing, and the variant quietly stops being the most severe thing
    it is.
    """
    logged = []
    dtp = DTP.__new__(DTP)
    dtp.logger = type("L", (), {"log": lambda self, m, lvl="INFO": logged.append((lvl, m))})()

    writer = _effects_writer(
        tmp_path, ["missense_variant", "a_term_the_seed_does_not_know"]
    )
    dtp._warn_unranked_consequences(writer)

    assert any(
        lvl == "WARNING" and "a_term_the_seed_does_not_know" in m
        for lvl, m in logged
    ), logged


def test_terms_the_seed_ranks_are_silent(tmp_path):
    logged = []
    dtp = DTP.__new__(DTP)
    dtp.logger = type("L", (), {"log": lambda self, m, lvl="INFO": logged.append((lvl, m))})()

    writer = _effects_writer(tmp_path, ["missense_variant", "intron_variant"])
    dtp._warn_unranked_consequences(writer)

    assert logged == []


def test_the_seed_ranks_every_term_uniquely():
    """
    Two terms sharing a rank make 'most severe' ambiguous, and the
    ordering silently depends on row order.
    """
    seed = json.loads(
        Path(
            "biofilter/modules/db/seed/initial_variant_consequences.json"
        ).read_text()
    )
    rows = seed if isinstance(seed, list) else seed["variant_consequences"]
    ranks = [r["severity_rank"] for r in rows]

    assert len(set(ranks)) == len(ranks)
    assert all(isinstance(r, int) for r in ranks)


def test_impacts_are_ranked_in_veps_order():
    seed = json.loads(
        Path(
            "biofilter/modules/db/seed/initial_variant_impacts.json"
        ).read_text()
    )
    by_rank = sorted(seed["variant_impacts"], key=lambda r: r["severity_rank"])

    # Alphabetically HIGH sorts after LOW, which is the whole point.
    assert [r["name"] for r in by_rank] == [
        "HIGH", "MODERATE", "LOW", "MODIFIER",
    ]

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import sys
import types

import pandas as pd
import pyarrow.parquet as pq
import pytest

# Keep unit tests runnable even when cyvcf2 is not installed locally.
if "cyvcf2" not in sys.modules:
    cyvcf2_stub = types.ModuleType("cyvcf2")
    cyvcf2_stub.VCF = object
    sys.modules["cyvcf2"] = cyvcf2_stub

import biofilter.modules.etl.dtps.dtp_variant_gnomad as mod  # noqa: F401


# -----------------------------
# Test helpers (fakes/mocks)
# -----------------------------


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, msg: str, level: str = "INFO"):
        self.messages.append((level, msg))


@dataclass
class FakeSourceSystem:
    name: str


@dataclass
class FakeDataSource:
    name: str
    source_system: FakeSourceSystem
    source_url: str = "http://example.com/file.vcf.bgz"
    release_tag: str = "test"
    grch_version: str = "GRCh38"
    id: int = 1


class FakeVariant:
    """
    Minimal cyvcf2 Variant stub with the attributes used by the DTP:
      - CHROM, POS, REF, ALT, ID, INFO.get()
    """

    def __init__(
        self,
        chrom: str,
        pos: int,
        ref: str,
        alt: Optional[str],
        rsid: Optional[str],
        info: Dict[str, Any],
    ):
        self.CHROM = chrom
        self.POS = pos
        self.REF = ref
        self.ALT = [alt] if alt is not None else []
        self.ID = rsid if rsid is not None else "."
        self.INFO = info
        self.FILTER = None
        self.QUAL = None


class FakeVCF:
    """
    Minimal cyvcf2.VCF stub:
      - raw_header
      - iterable over variants
    """

    def __init__(
        self, path: str, raw_header: str, variants: List[FakeVariant]
    ):  # noqa E501
        self.path = path
        self.raw_header = raw_header
        self._variants = variants

    def __iter__(self):
        yield from self._variants


class FakeConn:
    class _Dialect:
        def __init__(self, name: str):
            self.name = name

    class _ScalarResult:
        def __init__(self, value: int):
            self._value = value

        def scalar(self):
            return self._value

    def __init__(self, dialect_name: str = "postgresql", scalar_value: int = 0):
        self.dialect = self._Dialect(dialect_name)
        self.scalar_value = scalar_value
        self.executed = []

    def execute(self, stmt, params=None):
        self.executed.append((stmt, params))
        return self._ScalarResult(self.scalar_value)


# -----------------------------
# Unit tests for helper funcs
# -----------------------------


def test_parse_info_header_types():
    raw = "\n".join(
        [
            "##fileformat=VCFv4.2",
            '##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">',
            '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">',  # noqa E501
            '##INFO=<ID=FLAGX,Number=0,Type=Flag,Description="A flag">',
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
        ]
    )
    info_types = mod._parse_info_header_types(raw)
    assert info_types["AC"] == "Integer"
    assert info_types["AF"] == "Float"
    assert info_types["FLAGX"] == "Flag"


@pytest.mark.parametrize(
    "val, vtype, expected",
    [
        (10, "Integer", 10),
        ("10", "Integer", 10),
        ("0.1", "Float", 0.1),
        (0.2, "Float", 0.2),
        (True, "Flag", True),
        ("abc", "String", "abc"),
        ([1, 2], "Integer", [1, 2]),
        ((1, 2), "Integer", [1, 2]),
        (None, "Integer", None),
    ],
)
def test_cast_info_value(val, vtype, expected):
    out = mod._cast_info_value(val, vtype)
    assert out == expected


def test_parse_vep_header_format():
    raw = "\n".join(
        [
            "##fileformat=VCFv4.2",
            '##INFO=<ID=vep,Number=.,Type=String,Description="VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature">',  # noqa E501
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
        ]
    )
    vcf = FakeVCF("x", raw_header=raw, variants=[])
    fields = mod._parse_vep_header_format(vcf, "vep")
    assert fields[:4] == ["Allele", "Consequence", "IMPACT", "SYMBOL"]
    assert "Feature" in fields


def test_variant_key():
    k = mod._variant_key("1", 123, "A", "G")
    assert k == "1:123:A:G"


# -----------------------------
# Unit test for transform() using fakes
# -----------------------------


def test_transform_writes_parts(monkeypatch, tmp_path):
    """
    Unit-level transform test:
    - No real cyvcf2 parsing
    - No real VCF IO
    - Uses FakeVCF + FakeVariant
    - Verifies:
        - outputs dirs
        - parquet parts are created with dynamic numbering
        - consequences written under consequences/ (not variants/)
    """

    # 1) Create a fake raw_dir structure with a dummy file that matches glob
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"

    ss = FakeSourceSystem(name="gnomad")
    ds = FakeDataSource(name="gnomad_chr22", source_system=ss)

    raw_base = raw_dir / ss.name / ds.name
    raw_base.mkdir(parents=True, exist_ok=True)
    dummy_vcf = raw_base / "gnomad_chr22_tiny.vcf"
    dummy_vcf.write_text(
        "##dummy\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    )  # noqa E501

    # 2) Fake header with INFO + VEP schema
    raw_header = "\n".join(
        [
            "##fileformat=VCFv4.2",
            '##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">',
            '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">',  # noqa E501
            '##INFO=<ID=vep,Number=.,Type=String,Description="VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature">',  # noqa E501
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
        ]
    )

    # 3) Make a few fake variants (3 variants; chunk_size=2 => 2 parts: 0000 and 0001)  # noqa E501
    v1 = FakeVariant(
        chrom="22",
        pos=100,
        ref="A",
        alt="G",
        rsid="rs1",
        info={
            "AC": 1,
            "AF": 0.1,
            "vep": "G|missense_variant|MODERATE|GENE1|ENSG1|Transcript|ENST1",
        },  # noqa E501
    )
    v2 = FakeVariant(
        chrom="22",
        pos=200,
        ref="C",
        alt="T",
        rsid="rs2",
        info={
            "AC": 2,
            "AF": 0.2,
            "vep": "T|synonymous_variant|LOW|GENE2|ENSG2|Transcript|ENST2",
        },  # noqa E501
    )
    v3 = FakeVariant(
        chrom="22",
        pos=300,
        ref="G",
        alt="A",
        rsid="rs3",
        info={
            "AC": 3,
            "AF": 0.3,
            "vep": "A|stop_gained|HIGH|GENE3|ENSG3|Transcript|ENST3",
        },  # noqa E501
    )
    fake_vcf_obj = FakeVCF(
        str(dummy_vcf), raw_header=raw_header, variants=[v1, v2, v3]
    )  # noqa E501

    # 4) Monkeypatch VCF(...) to return our fake object
    monkeypatch.setattr(mod, "VCF", lambda path: fake_vcf_obj)

    # 5) Run transform with small chunk_size for deterministic parts
    logger = DummyLogger()
    cfg = mod.GnomadCyvcf2Config(
        chunk_size=2,
        vep_info_key="vep",
        extract_all_info=False,
        info_allowlist=["AC", "AF"],
        parquet_compression="snappy",
    )
    dtp = mod.DTP(logger=logger, datasource=ds, config=cfg)

    monkeypatch.setattr(mod.DTP, "check_compatibility", lambda self: None)
    ok, msg = dtp.transform(str(raw_dir), str(processed_dir))
    assert ok is True, msg

    out_base = Path(processed_dir) / ss.name / ds.name
    variants_dir = out_base / "variants"
    cons_dir = out_base / "consequences"

    assert variants_dir.exists()
    assert cons_dir.exists()

    # 6) Validate parquet parts
    variant_files = sorted(variants_dir.glob("variants_part_*.parquet"))
    cons_files = sorted(cons_dir.glob("consequences_part_*.parquet"))

    assert len(variant_files) == 2  # parts: 0000 (2 rows) + 0001 (1 row)
    assert len(cons_files) == 2

    assert variant_files[0].name == "variants_part_0000.parquet"
    assert variant_files[1].name == "variants_part_0001.parquet"
    assert cons_files[0].name == "consequences_part_0000.parquet"
    assert cons_files[1].name == "consequences_part_0001.parquet"

    # 7) Validate content quickly
    vtab0 = pq.read_table(variant_files[0])
    assert vtab0.num_rows == 2
    assert "variant_key" in vtab0.column_names
    assert "AC" in vtab0.column_names
    assert "AF" in vtab0.column_names

    ctab0 = pq.read_table(cons_files[0])
    assert ctab0.num_rows == 2
    assert "consequence" in ctab0.column_names
    assert "gene_symbol_raw" in ctab0.column_names


def test_read_parquet_available_columns_skips_missing(tmp_path):
    df = pd.DataFrame({"chrom": [22], "variant_key": ["22:100:A:G"]})
    parquet_path = tmp_path / "sample.parquet"
    df.to_parquet(parquet_path, index=False)

    dtp = mod.DTP(
        logger=DummyLogger(),
        datasource=FakeDataSource(
            name="gnomad_chr22", source_system=FakeSourceSystem(name="gnomad")
        ),
    )

    out = dtp._read_parquet_available_columns(
        str(parquet_path), ["chrom", "missing_col", "variant_key"]
    )
    assert list(out.columns) == ["chrom", "variant_key"]
    assert out.iloc[0]["variant_key"] == "22:100:A:G"


def test_supports_postgres_fast_load_respects_config():
    dtp = mod.DTP(
        logger=DummyLogger(),
        datasource=FakeDataSource(
            name="gnomad_chr22", source_system=FakeSourceSystem(name="gnomad")
        ),
        config=mod.GnomadCyvcf2Config(postgres_fast_load=False),
    )

    assert dtp._supports_postgres_fast_load(FakeConn("postgresql")) is False
    assert dtp._supports_postgres_fast_load(FakeConn("sqlite")) is False


def test_load_postgres_part_file_fast_stages_and_bulk_loads(monkeypatch):
    dtp = mod.DTP(
        logger=DummyLogger(),
        datasource=FakeDataSource(
            name="gnomad_chr22", source_system=FakeSourceSystem(name="gnomad")
        ),
    )
    dtp.package = type("Pkg", (), {"id": 99})()

    calls = {
        "truncated": 0,
        "copied": [],
        "primed": 0,
    }

    monkeypatch.setattr(
        dtp,
        "_truncate_postgres_stage_tables",
        lambda conn: calls.__setitem__("truncated", calls["truncated"] + 1),
    )

    def fake_copy(conn, *, table_name, df, columns):
        calls["copied"].append((table_name, list(columns), list(df.columns)))

    monkeypatch.setattr(dtp, "_copy_dataframe_to_postgres_stage", fake_copy)
    monkeypatch.setattr(
        dtp, "_bulk_insert_variant_masters_from_stage", lambda conn: 2
    )
    monkeypatch.setattr(
        dtp,
        "_prime_dimension_caches_from_df",
        lambda df, conn, dim_caches: calls.__setitem__(
            "primed", calls["primed"] + 1
        ),
    )
    monkeypatch.setattr(
        dtp, "_bulk_insert_variant_molecular_effects_from_stage", lambda conn: 5
    )

    df_variants = pd.DataFrame(
        [
            {
                "chromosome": 22,
                "position_start": 100,
                "position_end": 100,
                "reference_allele": "A",
                "alternate_allele": "G",
                "variant_key": "22:100:A:G",
                "AC": 1,
                "AN": 10,
                "AF": 0.1,
            },
            {
                "chromosome": 22,
                "position_start": 200,
                "position_end": 200,
                "reference_allele": "C",
                "alternate_allele": "T",
                "variant_key": "22:200:C:T",
                "AC": 2,
                "AN": 10,
                "AF": 0.2,
            },
        ]
    )
    df_consequences = pd.DataFrame(
        [
            {
                "chromosome": 22,
                "variant_key": "22:100:A:G",
                "gene_id_raw": "ENSG1",
                "gene_symbol_raw": "GENE1",
                "transcript_id_raw": "ENST1",
                "consequence": "missense_variant",
            }
        ]
    )

    conn = FakeConn("postgresql", scalar_value=2)
    out = dtp._load_postgres_part_file_fast(
        conn,
        df_variants,
        df_consequences,
        dim_caches={
            "group": {},
            "category": {},
            "consequence": {"missense_variant": 101},
            "impact": {},
            "biotype": {},
        },
    )

    assert out == (2, 2, 5)
    assert calls["truncated"] == 1
    assert calls["primed"] == 1
    assert [item[0] for item in calls["copied"]] == [
        "tmp_gnomad_variant_stage",
        "tmp_gnomad_consequence_stage",
    ]

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import MetaData, create_engine, select
from sqlalchemy.orm import sessionmaker

import biofilter.modules.etl.dtps.dtp_variant_alphamissense as mod
from biofilter.modules.db.models.model_variants import map_variant_masters


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
    source_url: str = "http://example.org/alphamissense.tsv.gz"
    dtp_version: str = "1.0.0"
    id: int = 91


@dataclass
class FakePackage:
    id: int = 701


def test_transform_writes_prediction_parts(monkeypatch, tmp_path):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"

    ss = FakeSourceSystem(name="AlphaMissense")
    ds = FakeDataSource(name="alphamissense_chr22", source_system=ss)
    raw_base = raw_dir / ss.name / ds.name
    raw_base.mkdir(parents=True, exist_ok=True)

    input_path = raw_base / "alphamissense_chr22.tsv"
    input_path.write_text(
        "\n".join(
            [
                "# Copyright 2023 DeepMind Technologies Limited",
                "#",
                "# Licensed under CC BY-NC-SA 4.0 license",
                "#CHROM\tPOS\tREF\tALT\ttranscript_id\tam_pathogenicity\tam_class",
                "chr22\t100\tA\tG\tENST000001\t0.97\tlikely_pathogenic",
                "22\t101\tC\tT\tENST000002\t0.12\tlikely_benign",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    dtp = mod.DTP(logger=DummyLogger(), datasource=ds)
    monkeypatch.setattr(mod.DTP, "check_compatibility", lambda self: None)

    ok, msg = dtp.transform(str(raw_dir), str(processed_dir))
    assert ok is True, msg

    pred_dir = processed_dir / ss.name / ds.name / "predictions"
    # ADR-003: one file per chromosome, named after the table it feeds,
    # instead of chunk-sized parts with arbitrary boundaries.
    files = sorted(pred_dir.glob("variant_alphamissense_chr*.parquet"))
    assert len(files) == 1
    assert files[0].name == "variant_alphamissense_chr22.parquet"

    out = pd.read_parquet(files[0])
    assert len(out.index) == 2
    assert set(
        [
            "chromosome",
            "position_start",
            "position_end",
            "reference_allele",
            "alternate_allele",
            "predictor_key",
            "predictor_name",
        ]
    ).issubset(out.columns)
    assert out["predictor_name"].nunique() == 1
    assert out["predictor_name"].iloc[0] == "alphamissense"
    # The natural key survives: nothing resolves it to a variant_id.
    assert "variant_id" not in out.columns
    # Provenance is stamped by transform now that the parquet is final.
    assert {"data_source_id", "etl_package_id"}.issubset(out.columns)


def test_load_is_not_used_under_adr_003(monkeypatch, tmp_path):
    """
    The load step is gone by design, not by omission.

    It used to resolve the natural key against variant_masters to swap it
    for a generated variant_id — a surrogate valid only inside one bundle
    (ADR-003 §2.5) — and dropped every prediction whose variant was not
    already loaded. The transform output is the final artifact now, so
    calling load must fail loudly rather than silently do nothing.
    """
    dtp = mod.DTP(logger=DummyLogger(), datasource=SimpleNamespace(id=1, name="x"))

    with pytest.raises(NotImplementedError) as exc:
        dtp.load(processed_dir=str(tmp_path))

    assert "ADR-003" in str(exc.value)

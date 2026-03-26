from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import MetaData, create_engine, select
from sqlalchemy.orm import sessionmaker

import biofilter.modules.etl.dtps.dtp_variant_alphamissense as mod
from biofilter.modules.db.models.model_variants import (
    map_variant_effect_predictions,
    map_variant_masters,
)


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
    files = sorted(pred_dir.glob("predictions_part_*.parquet"))
    assert len(files) == 1

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


def test_load_resolves_variant_ids_and_inserts_predictions(monkeypatch, tmp_path):
    engine = create_engine("sqlite:///:memory:", future=True)
    metadata = MetaData()
    variant_masters = map_variant_masters(engine, metadata)
    effect_predictions = map_variant_effect_predictions(engine, metadata)
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            variant_masters.insert().values(
                chromosome=22,
                position_start=100,
                position_end=100,
                reference_allele="A",
                alternate_allele="G",
                rsid="rs123",
            )
        )

    session = sessionmaker(bind=engine, future=True, expire_on_commit=False)()
    db = SimpleNamespace(engine=engine)

    ss = FakeSourceSystem(name="AlphaMissense")
    ds = FakeDataSource(name="alphamissense_chr22", source_system=ss, id=99)

    processed_dir = tmp_path / "processed"
    pred_dir = processed_dir / ss.name / ds.name / "predictions"
    pred_dir.mkdir(parents=True, exist_ok=True)

    load_frame = pd.DataFrame(
        [
            {
                "chromosome": 22,
                "position_start": 100,
                "position_end": 100,
                "reference_allele": "A",
                "alternate_allele": "G",
                "predictor_key": "alphamissense:na:ENST000001",
                "transcript_id": "ENST000001",
                "predictor_name": "alphamissense",
                "predictor_version": None,
                "score": 0.91,
                "classification": "likely_pathogenic",
                "details": None,
            },
            {
                # This row does not exist in variant_masters and should be unmatched
                "chromosome": 22,
                "position_start": 101,
                "position_end": 101,
                "reference_allele": "C",
                "alternate_allele": "T",
                "predictor_key": "alphamissense:na:ENST000002",
                "transcript_id": "ENST000002",
                "predictor_name": "alphamissense",
                "predictor_version": None,
                "score": 0.14,
                "classification": "likely_benign",
                "details": None,
            },
        ]
    )
    load_frame.to_parquet(pred_dir / "predictions_part_0000.parquet", index=False)

    dtp = mod.DTP(
        logger=DummyLogger(),
        datasource=ds,
        package=FakePackage(id=7001),
        session=session,
        db=db,
    )
    monkeypatch.setattr(mod.DTP, "check_compatibility", lambda self: None)

    ok, msg = dtp.load(str(processed_dir))
    assert ok is True, msg

    with engine.connect() as conn:
        rows = conn.execute(select(effect_predictions)).fetchall()

    assert len(rows) == 1
    row = rows[0]
    assert row.chromosome == 22
    assert row.predictor_name == "alphamissense"
    assert row.transcript_id == "ENST000001"
    assert float(row.score) == 0.91

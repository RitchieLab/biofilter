from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine
from sqlalchemy.orm import sessionmaker

from biofilter.modules.report.reports.report_variant_molecular_effects import (
    VariantMolecularEffectsReport,
    _parse_bool,
)


class DummyLogger:
    def log(self, message, level="INFO"):
        pass


def _report(session=None, **kwargs):
    return VariantMolecularEffectsReport(
        session=session,
        db=SimpleNamespace(engine=getattr(session, "bind", None)),
        logger=DummyLogger(),
        **kwargs,
    )


def test_parse_region_dict_rejects_non_positive_positions():
    report = _report()

    parsed = report._parse_region_dict({"chromosome": "1", "start": 0, "end": 10})
    assert parsed.status == "invalid_input"
    assert "positive integers" in (parsed.note or "")

    parsed = report._parse_region_dict({"chromosome": "1", "start": 10, "end": 0})
    assert parsed.status == "invalid_input"
    assert "positive integers" in (parsed.note or "")


def test_parse_region_dict_does_not_fallback_when_zero_is_present():
    report = _report()
    parsed = report._parse_region_dict(
        {"chromosome": "1", "start": 0, "pos_start": 100, "end": 200}
    )

    assert parsed.status == "invalid_input"
    assert parsed.start == 0


def test_parse_bool_handles_common_string_values():
    assert _parse_bool("true", default=False) is True
    assert _parse_bool("yes", default=False) is True
    assert _parse_bool("false", default=True) is False
    assert _parse_bool("0", default=True) is False
    assert _parse_bool("n", default=True) is False
    assert _parse_bool("unexpected", default=True) is True


def test_query_effects_for_chromosome_uses_chunking_for_large_variant_lists():
    engine = create_engine("sqlite:///:memory:", future=True)
    metadata = MetaData()
    vme = Table(
        "variant_molecular_effects",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("variant_id", Integer, nullable=False),
        Column("gene_symbol", String(50)),
    )
    metadata.create_all(engine)

    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.execute(
            vme.insert(),
            [
                {"chromosome": 1, "variant_id": i, "gene_symbol": f"GENE{i}"}
                for i in range(1, 1201)
            ],
        )
        session.commit()

        report = _report(session=session, effect_query_chunk_size=200)

        with patch.object(session, "execute", wraps=session.execute) as exec_mock:
            rows = report._query_effects_for_chromosome(
                vme=vme,
                chromosome=1,
                variant_ids=list(range(1, 1201)),
            )

    assert len(rows) == 1200
    assert exec_mock.call_count == 6

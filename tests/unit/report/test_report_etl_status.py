from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from biofilter.modules.db.models import ETLDataSource, ETLPackage, ETLSourceSystem
from biofilter.modules.report.reports.report_etl_status import ETLStatusReport


class DummyLogger:
    def log(self, message, level="INFO"):
        pass


def _report(session, **kwargs):
    return ETLStatusReport(
        session=session,
        db=SimpleNamespace(engine=getattr(session, "bind", None)),
        logger=DummyLogger(),
        **kwargs,
    )


def _make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    ETLSourceSystem.__table__.create(engine)
    ETLDataSource.__table__.create(engine)
    ETLPackage.__table__.create(engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def test_etl_status_includes_data_sources_without_packages():
    session = _make_session()
    with session:
        source = ETLSourceSystem(name="NCBI", active=True)
        session.add(source)
        session.flush()

        ds_with_package = ETLDataSource(
            name="hgnc",
            source_system_id=source.id,
            data_type="gene",
            format="tsv",
            dtp_script="dtp_gene_hgnc",
            active=True,
        )
        ds_without_package = ETLDataSource(
            name="mondo",
            source_system_id=source.id,
            data_type="disease",
            format="obo",
            dtp_script="dtp_disease_mondo",
            active=True,
        )
        session.add_all([ds_with_package, ds_without_package])
        session.flush()

        session.add(
            ETLPackage(
                data_source_id=ds_with_package.id,
                created_at=datetime(2026, 1, 1, 10, 0, 0),
                status="completed",
                operation_type="extract",
                extract_status="completed",
                extract_end=datetime(2026, 1, 1, 10, 5, 0),
                extract_hash="hash_hgnc_1",
            )
        )
        session.commit()

        df = _report(session).run()

    assert set(df["data_source"].tolist()) == {"hgnc", "mondo"}

    row_hgnc = df[df["data_source"] == "hgnc"].iloc[0]
    assert row_hgnc["extract_status"] == "completed"
    assert int(row_hgnc["extract_package_id"]) > 0

    row_mondo = df[df["data_source"] == "mondo"].iloc[0]
    assert bool(row_mondo["pipeline_ok"]) is False
    assert pd.isna(row_mondo["extract_package_id"])
    assert row_mondo["extract_status"] is None


def test_etl_status_respects_only_active_for_data_sources_without_packages():
    session = _make_session()
    with session:
        source = ETLSourceSystem(name="NCBI", active=True)
        session.add(source)
        session.flush()

        session.add_all(
            [
                ETLDataSource(
                    name="active_ds",
                    source_system_id=source.id,
                    data_type="gene",
                    format="tsv",
                    dtp_script="dtp_gene_active",
                    active=True,
                ),
                ETLDataSource(
                    name="inactive_ds",
                    source_system_id=source.id,
                    data_type="gene",
                    format="tsv",
                    dtp_script="dtp_gene_inactive",
                    active=False,
                ),
            ]
        )
        session.commit()

        df_active = _report(session, only_active=True).run()
        df_all = _report(session, only_active=False).run()

    assert set(df_active["data_source"].tolist()) == {"active_ds"}
    assert set(df_all["data_source"].tolist()) == {"active_ds", "inactive_ds"}

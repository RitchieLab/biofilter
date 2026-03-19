from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.orm import sessionmaker

import biofilter.modules.etl.etl_manager as etl_mgr_mod
from biofilter.modules.db.models import ETLDataSource, ETLPackage, ETLSourceSystem


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class _Ctx:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyDB:
    def __init__(self):
        self.sessions = []

    def get_session(self):
        session = object()
        self.sessions.append(session)
        return _Ctx(session)


def test_select_index_groups_without_filter_returns_catalog_copy():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    catalog = {"gene": "g", "variant": "v"}
    aliases = {}
    selected = manager._select_index_groups(None, catalog, aliases)

    assert selected == catalog
    assert selected is not catalog


def test_select_index_groups_uses_aliases_and_logs_invalid_groups():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    catalog = {"gene": "g", "variant": "v"}
    aliases = {"genes": "gene", "variants": "variant"}

    selected = manager._select_index_groups(
        ["genes", "unknown_group"], catalog, aliases
    )

    assert selected == {"gene": "g"}
    assert any(
        level == "WARNING" and "unknown_group" in message
        for level, message in logger.messages
    )


def test_load_dtp_module_raises_for_empty_script():
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=DummyLogger())
    ds = SimpleNamespace(name="x", dtp_script="")

    try:
        manager._load_dtp_module(ds)
        assert False, "Expected ValueError for empty dtp_script"
    except ValueError as exc:
        assert "empty dtp_script" in str(exc)


def test_load_dtp_module_uses_cache(monkeypatch):
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=DummyLogger())
    ds = SimpleNamespace(name="hgnc", dtp_script="dtp_gene_hgnc")

    calls = {"count": 0}
    fake_module = object()

    def fake_import(module_path):
        calls["count"] += 1
        assert module_path == "biofilter.modules.etl.dtps.dtp_gene_hgnc"
        return fake_module

    monkeypatch.setattr(etl_mgr_mod.importlib, "import_module", fake_import)

    first = manager._load_dtp_module(ds)
    second = manager._load_dtp_module(ds)

    assert first is fake_module
    assert second is fake_module
    assert calls["count"] == 1


def test_start_process_aborts_without_filters():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    manager.start_process(source_system=None, data_sources=None)

    assert any(
        level == "ERROR" and "No source_system or data_sources provided" in message
        for level, message in logger.messages
    )


def test_start_process_warns_when_no_datasource_matches(monkeypatch):
    logger = DummyLogger()
    db = DummyDB()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=db, logger=logger)

    monkeypatch.setattr(manager, "_resolve_datasource_ids", lambda *a, **k: [])

    manager.start_process(source_system=["NCBI"])

    assert any(
        level == "WARNING" and "No matching active DataSources found" in message
        for level, message in logger.messages
    )


def test_start_process_normalizes_inputs_and_runs_each_datasource(monkeypatch):
    logger = DummyLogger()
    db = DummyDB()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=db, logger=logger)

    captured = {"resolve": None, "runs": []}

    def fake_resolve(session, source_system, data_sources):
        captured["resolve"] = (source_system, data_sources)
        return [11, 22]

    def fake_load(session, ds_id):
        return SimpleNamespace(id=ds_id, name=f"ds_{ds_id}", source_system_id=1)

    def fake_run_one_datasource(**kwargs):
        captured["runs"].append(kwargs)

    monkeypatch.setattr(manager, "_resolve_datasource_ids", fake_resolve)
    monkeypatch.setattr(manager, "_load_datasource", fake_load)
    monkeypatch.setattr(manager, "_run_one_datasource", fake_run_one_datasource)

    manager.start_process(
        source_system="NCBI",
        data_sources="hgnc",
        download_path="/raw",
        processed_path="/processed",
    )

    assert captured["resolve"] == (["NCBI"], ["hgnc"])
    assert len(captured["runs"]) == 2
    assert {r["ds"].id for r in captured["runs"]} == {11, 22}
    assert all(r["download_path"] == "/raw" for r in captured["runs"])
    assert all(r["processed_path"] == "/processed" for r in captured["runs"])
    assert all(r["run_steps"] == ["extract", "transform", "load"] for r in captured["runs"])  # noqa E501
    assert all(r["force_steps"] == [] for r in captured["runs"])


def test_start_process_all_skips_completed_runs_pending_and_drops_files(monkeypatch):
    logger = DummyLogger()
    db = DummyDB()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=db, logger=logger)

    captured = {"resolve": None, "runs": [], "deleted": []}
    status_by_ds = {1: "completed", 2: None, 3: "failed"}

    def fake_resolve(session, source_system, data_sources, only_active=True):
        captured["resolve"] = (source_system, data_sources, only_active)
        return [1, 2, 3]

    def fake_load(session, ds_id):
        return SimpleNamespace(
            id=ds_id,
            name=f"ds_{ds_id}",
            source_system=SimpleNamespace(name="NCBI"),
            source_system_id=1,
        )

    def fake_latest(session, ds_id):
        return status_by_ds.get(ds_id)

    def fake_run_one_datasource(**kwargs):
        ds = kwargs["ds"]
        captured["runs"].append(ds.id)
        if ds.id == 2:
            status_by_ds[2] = "completed"
        elif ds.id == 3:
            status_by_ds[3] = "failed"

    def fake_delete(pattern):
        captured["deleted"].append(pattern)

    monkeypatch.setattr(manager, "_resolve_datasource_ids", fake_resolve)
    monkeypatch.setattr(manager, "_load_datasource", fake_load)
    monkeypatch.setattr(manager, "_latest_load_status", fake_latest)
    monkeypatch.setattr(manager, "_run_one_datasource", fake_run_one_datasource)
    monkeypatch.setattr(manager, "_delete_matching_files", fake_delete)

    summary = manager.start_process_all(
        source_system="NCBI",
        data_sources=None,
        download_path="/raw",
        processed_path="/processed",
        drop_files_on_success=True,
        only_active=True,
    )

    assert captured["resolve"] == (["NCBI"], None, True)
    assert captured["runs"] == [2, 3]
    assert captured["deleted"] == ["/raw/NCBI/ds_2*", "/processed/NCBI/ds_2*"]
    assert summary == {
        "selected": 3,
        "skipped": 1,
        "processed": 2,
        "succeeded": 1,
        "failed": 1,
    }


def test_start_process_all_stop_on_error_breaks_after_first_failure(monkeypatch):
    logger = DummyLogger()
    db = DummyDB()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=db, logger=logger)

    processed_ids = []
    status_by_ds = {10: None, 11: None}

    monkeypatch.setattr(
        manager,
        "_resolve_datasource_ids",
        lambda *a, **k: [10, 11],
    )
    monkeypatch.setattr(
        manager,
        "_load_datasource",
        lambda session, ds_id: SimpleNamespace(
            id=ds_id,
            name=f"ds_{ds_id}",
            source_system=SimpleNamespace(name="NCBI"),
            source_system_id=1,
        ),
    )
    monkeypatch.setattr(manager, "_latest_load_status", lambda session, ds_id: status_by_ds.get(ds_id))  # noqa E501

    def fake_run_one_datasource(**kwargs):
        ds = kwargs["ds"]
        processed_ids.append(ds.id)
        status_by_ds[ds.id] = "failed"

    monkeypatch.setattr(manager, "_run_one_datasource", fake_run_one_datasource)

    summary = manager.start_process_all(stop_on_error=True)

    assert processed_ids == [10]
    assert summary["processed"] == 1
    assert summary["failed"] == 1


def test_start_process_all_warns_when_no_datasource_matches(monkeypatch):
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    monkeypatch.setattr(manager, "_resolve_datasource_ids", lambda *a, **k: [])

    summary = manager.start_process_all()

    assert summary == {
        "selected": 0,
        "skipped": 0,
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
    }
    assert any(
        level == "WARNING" and "No matching DataSources found for update-all" in message
        for level, message in logger.messages
    )


def test_resolve_datasource_ids_respects_only_active_and_returns_ordered_ids():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    engine = create_engine("sqlite:///:memory:", future=True)
    ETLSourceSystem.__table__.create(engine)
    ETLDataSource.__table__.create(engine)
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        ss_active = ETLSourceSystem(name="NCBI", active=True)
        ss_inactive = ETLSourceSystem(name="OLD", active=False)
        session.add_all([ss_active, ss_inactive])
        session.flush()

        session.add_all(
            [
                ETLDataSource(
                    name="z_ds",
                    source_system_id=ss_active.id,
                    data_type="gene",
                    format="tsv",
                    dtp_script="dtp_z",
                    active=True,
                ),
                ETLDataSource(
                    name="a_ds",
                    source_system_id=ss_active.id,
                    data_type="gene",
                    format="tsv",
                    dtp_script="dtp_a",
                    active=False,
                ),
                ETLDataSource(
                    name="b_ds",
                    source_system_id=ss_inactive.id,
                    data_type="gene",
                    format="tsv",
                    dtp_script="dtp_b",
                    active=True,
                ),
            ]
        )
        session.commit()

        ids_active = manager._resolve_datasource_ids(
            session, source_system=None, data_sources=None, only_active=True
        )
        ids_all = manager._resolve_datasource_ids(
            session, source_system=None, data_sources=None, only_active=False
        )

    assert len(ids_active) == 1
    assert len(ids_all) == 3
    assert ids_all == sorted(ids_all)


def test_latest_load_status_returns_latest_by_created_at_and_id():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    engine = create_engine("sqlite:///:memory:", future=True)
    ETLSourceSystem.__table__.create(engine)
    ETLDataSource.__table__.create(engine)
    ETLPackage.__table__.create(engine)
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        ss = ETLSourceSystem(name="NCBI", active=True)
        session.add(ss)
        session.flush()
        ds = ETLDataSource(
            name="hgnc",
            source_system_id=ss.id,
            data_type="gene",
            format="tsv",
            dtp_script="dtp_hgnc",
            active=True,
        )
        session.add(ds)
        session.flush()

        session.add_all(
            [
                ETLPackage(
                    data_source_id=ds.id,
                    operation_type="load",
                    status="completed",
                    load_status="completed",
                ),
                ETLPackage(
                    data_source_id=ds.id,
                    operation_type="load",
                    status="failed",
                    load_status="failed",
                ),
            ]
        )
        session.commit()

        status = manager._latest_load_status(session, ds.id)

    assert status == "failed"


def test_restart_etl_process_aborts_without_filters():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    ok = manager.restart_etl_process()

    assert ok is False
    assert any(
        level == "ERROR" and "No source_system or data_source provided" in message
        for level, message in logger.messages
    )


def test_restart_etl_process_warns_when_no_datasource_matches(monkeypatch):
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    monkeypatch.setattr(manager, "_resolve_datasource_ids", lambda *a, **k: [])

    ok = manager.restart_etl_process(source_system=["NCBI"])

    assert ok is False
    assert any(
        level == "WARNING" and "No matching active DataSources found" in message
        for level, message in logger.messages
    )


def test_restart_etl_process_rolls_back_optionally_deletes_and_reruns(monkeypatch):
    logger = DummyLogger()
    db = DummyDB()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=db, logger=logger)

    monkeypatch.setattr(manager, "_resolve_datasource_ids", lambda *a, **k: [7])

    ds = SimpleNamespace(
        id=7,
        name="dbsnp_chr7",
        source_system=SimpleNamespace(name="NCBI"),
        source_system_id=1,
    )
    monkeypatch.setattr(manager, "_load_datasource", lambda *a, **k: ds)

    captured = {"rolled_back": 0, "deleted": [], "runs": []}

    def fake_rollback_data_source(session, ds, note):
        assert ds.id == 7
        assert "rollback before restart" in note
        captured["rolled_back"] += 1
        return True, "ok"

    def fake_delete(pattern):
        captured["deleted"].append(pattern)

    def fake_run(**kwargs):
        captured["runs"].append(kwargs)

    monkeypatch.setattr(manager, "_rollback_data_source", fake_rollback_data_source)
    monkeypatch.setattr(manager, "_delete_matching_files", fake_delete)
    monkeypatch.setattr(manager, "_run_one_datasource", fake_run)

    ok = manager.restart_etl_process(
        data_source=["dbsnp_chr7"],
        delete_files=True,
        download_path="/raw",
        processed_path="/processed",
    )

    assert ok is True
    assert captured["rolled_back"] == 1
    assert captured["deleted"] == ["/raw/NCBI/dbsnp_chr7*", "/processed/NCBI/dbsnp_chr7*"]  # noqa E501
    assert len(captured["runs"]) == 1
    run = captured["runs"][0]
    assert run["ds"] is ds
    assert run["run_steps"] == ["extract", "transform", "load"]
    assert run["force_steps"] == ["extract", "transform", "load"]


def test_rollback_etl_process_aborts_without_filters():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    ok = manager.rollback_etl_process()

    assert ok is False
    assert any(
        level == "ERROR" and "No rollback target provided" in message
        for level, message in logger.messages
    )


def test_rollback_etl_process_rejects_mixed_package_and_datasource_filters():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    ok = manager.rollback_etl_process(
        package_ids=[1],
        data_source=["hgnc"],
    )

    assert ok is False
    assert any(
        level == "ERROR" and "either package_ids OR data_source/source_system" in message  # noqa E501
        for level, message in logger.messages
    )


def test_rollback_etl_process_datasource_mode_uses_rollback_and_optional_delete(monkeypatch):
    logger = DummyLogger()
    db = DummyDB()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=db, logger=logger)

    monkeypatch.setattr(manager, "_resolve_datasource_ids", lambda *a, **k: [7])

    ds = SimpleNamespace(
        id=7,
        name="dbsnp_chr7",
        source_system=SimpleNamespace(name="NCBI"),
        source_system_id=1,
    )
    monkeypatch.setattr(manager, "_load_datasource", lambda *a, **k: ds)

    captured = {"rolled_back": 0, "deleted": []}

    def fake_rollback_data_source(session, ds, note):
        assert ds.id == 7
        assert "manual data-source rollback" in note
        captured["rolled_back"] += 1
        return True, "ok"

    def fake_delete(pattern):
        captured["deleted"].append(pattern)

    monkeypatch.setattr(manager, "_rollback_data_source", fake_rollback_data_source)
    monkeypatch.setattr(manager, "_delete_matching_files", fake_delete)

    ok = manager.rollback_etl_process(
        data_source=["dbsnp_chr7"],
        delete_files=True,
        download_path="/raw",
        processed_path="/processed",
    )

    assert ok is True
    assert captured["rolled_back"] == 1
    assert captured["deleted"] == ["/raw/NCBI/dbsnp_chr7*", "/processed/NCBI/dbsnp_chr7*"]  # noqa E501


def test_rollback_etl_process_package_mode_uses_target_package(monkeypatch):
    logger = DummyLogger()
    db = DummyDB()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=db, logger=logger)

    ds = SimpleNamespace(
        id=9,
        name="hgnc",
        source_system=SimpleNamespace(name="HGNC"),
        source_system_id=2,
    )
    pkg = SimpleNamespace(id=123, data_source_id=9, operation_type="load")
    monkeypatch.setattr(manager, "_load_package", lambda *a, **k: pkg)
    monkeypatch.setattr(manager, "_load_datasource", lambda *a, **k: ds)

    captured = {"called": 0}

    def fake_rollback_package(session, ds, target_package, note):
        assert ds.id == 9
        assert target_package.id == 123
        assert "manual package rollback" in note
        captured["called"] += 1
        return True, "ok"

    monkeypatch.setattr(manager, "_rollback_package", fake_rollback_package)

    ok = manager.rollback_etl_process(package_ids=[123])

    assert ok is True
    assert captured["called"] == 1


def test_simple_purge_by_data_source_deletes_only_non_etl_rows_with_datasource_id():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    engine = create_engine("sqlite:///:memory:", future=True)
    metadata = MetaData()
    table_entity = Table(
        "Entity",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data_source_id", Integer),
        Column("name", String(50)),
    )
    table_etl = Table(
        "etl_status",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data_source_id", Integer),
        Column("status", String(20)),
    )
    table_other = Table(
        "OtherTable",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
    )
    metadata.create_all(engine)

    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.execute(
            table_entity.insert(),
            [
                {"id": 1, "data_source_id": 7, "name": "to_delete"},
                {"id": 2, "data_source_id": 8, "name": "to_keep"},
            ],
        )
        session.execute(
            table_etl.insert(),
            [{"id": 1, "data_source_id": 7, "status": "running"}],
        )
        session.execute(
            table_other.insert(),
            [{"id": 1, "name": "no_data_source_id_column"}],
        )
        session.commit()

        manager._simple_purge_by_data_source(session, 7)

        remaining_entity = session.execute(
            select(table_entity.c.data_source_id, table_entity.c.name)
        ).all()
        remaining_etl = session.execute(
            select(table_etl.c.data_source_id, table_etl.c.status)
        ).all()
        remaining_other = session.execute(select(table_other.c.name)).all()

    assert remaining_entity == [(8, "to_keep")]
    assert remaining_etl == [(7, "running")]
    assert remaining_other == [("no_data_source_id_column",)]
    assert any(
        level == "INFO" and "Simple purge complete" in message
        for level, message in logger.messages
    )


def test_simple_purge_by_package_deletes_only_non_etl_rows_with_package_id():
    logger = DummyLogger()
    manager = etl_mgr_mod.ETLManager(debug_mode=False, db=DummyDB(), logger=logger)

    engine = create_engine("sqlite:///:memory:", future=True)
    metadata = MetaData()
    table_entity = Table(
        "Entity",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("etl_package_id", Integer),
        Column("name", String(50)),
    )
    table_etl = Table(
        "etl_status",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("etl_package_id", Integer),
        Column("status", String(20)),
    )
    metadata.create_all(engine)

    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        session.execute(
            table_entity.insert(),
            [
                {"id": 1, "etl_package_id": 100, "name": "to_delete"},
                {"id": 2, "etl_package_id": 200, "name": "to_keep"},
            ],
        )
        session.execute(
            table_etl.insert(),
            [{"id": 1, "etl_package_id": 100, "status": "running"}],
        )
        session.commit()

        manager._simple_purge_by_package(session, 100)

        remaining_entity = session.execute(
            select(table_entity.c.etl_package_id, table_entity.c.name)
        ).all()
        remaining_etl = session.execute(
            select(table_etl.c.etl_package_id, table_etl.c.status)
        ).all()

    assert remaining_entity == [(200, "to_keep")]
    assert remaining_etl == [(100, "running")]

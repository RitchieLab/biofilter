from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.orm import sessionmaker

import biofilter.modules.etl.etl_manager as etl_mgr_mod


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


def test_restart_etl_process_purges_optionally_deletes_and_reruns(monkeypatch):
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

    captured = {"purged": 0, "deleted": [], "runs": []}

    def fake_purge(session, ds_id):
        assert ds_id == 7
        captured["purged"] += 1

    def fake_delete(pattern):
        captured["deleted"].append(pattern)

    def fake_run(**kwargs):
        captured["runs"].append(kwargs)

    monkeypatch.setattr(manager, "_simple_purge_by_data_source", fake_purge)
    monkeypatch.setattr(manager, "_delete_matching_files", fake_delete)
    monkeypatch.setattr(manager, "_run_one_datasource", fake_run)

    ok = manager.restart_etl_process(
        data_source=["dbsnp_chr7"],
        delete_files=True,
        download_path="/raw",
        processed_path="/processed",
    )

    assert ok is True
    assert captured["purged"] == 1
    assert captured["deleted"] == ["/raw/NCBI/dbsnp_chr7*", "/processed/NCBI/dbsnp_chr7*"]  # noqa E501
    assert len(captured["runs"]) == 1
    run = captured["runs"][0]
    assert run["ds"] is ds
    assert run["run_steps"] == ["extract", "transform", "load"]
    assert run["force_steps"] == ["extract", "transform", "load"]


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

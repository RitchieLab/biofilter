from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import MetaData, Table, create_engine, text

from biofilter.modules.db.transfer import export_full_clone, import_full_clone


class FakeDB:
    def __init__(self, engine):
        self.engine = engine

    def table(self, name: str):
        return Table(name, MetaData(), autoload_with=self.engine)


def _sqlite_engine(db_path: Path):
    return create_engine(f"sqlite:///{db_path}", future=True)


def test_export_full_clone_accepts_include_and_exclude_tables(tmp_path):
    engine = _sqlite_engine(tmp_path / "export.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("CREATE TABLE b (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("CREATE TABLE c (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("INSERT INTO a VALUES (1, 'x')"))
        conn.execute(text("INSERT INTO b VALUES (1, 'y')"))
        conn.execute(text("INSERT INTO c VALUES (1, 'z')"))

    out_dir = tmp_path / "bundle"
    export_full_clone(
        engine,
        out_dir,
        biofilter_version="4.1.1-test",
        schema_version="4.1.1",
        fmt="csv",
        include_tables=["a", "b", "c"],
        exclude_tables=["b"],
    )

    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    exported = [entry["name"] for entry in manifest["tables"]]

    assert exported == ["a", "c"]
    assert (out_dir / "tables" / "a.csv").exists()
    assert not (out_dir / "tables" / "b.csv").exists()
    assert (out_dir / "tables" / "c.csv").exists()


def test_export_full_clone_raises_for_unknown_selected_table(tmp_path):
    engine = _sqlite_engine(tmp_path / "export_unknown.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE only_one (id INTEGER PRIMARY KEY)"))

    with pytest.raises(RuntimeError, match="not found in DB"):
        export_full_clone(
            engine,
            tmp_path / "bundle",
            biofilter_version="4.1.1-test",
            schema_version="4.1.1",
            fmt="csv",
            include_tables=["missing_table"],
        )


def test_export_full_clone_raises_for_unknown_excluded_table(tmp_path):
    engine = _sqlite_engine(tmp_path / "export_unknown_excluded.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE only_one (id INTEGER PRIMARY KEY)"))

    with pytest.raises(RuntimeError, match="excluded table"):
        export_full_clone(
            engine,
            tmp_path / "bundle",
            biofilter_version="4.1.1-test",
            schema_version="4.1.1",
            fmt="csv",
            exclude_tables=["missing_table"],
        )


def test_export_full_clone_raises_when_filters_select_no_tables(tmp_path):
    engine = _sqlite_engine(tmp_path / "export_none.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE a (id INTEGER PRIMARY KEY)"))

    with pytest.raises(RuntimeError, match="No tables selected for export"):
        export_full_clone(
            engine,
            tmp_path / "bundle",
            biofilter_version="4.1.1-test",
            schema_version="4.1.1",
            fmt="csv",
            include_tables=["a"],
            exclude_tables=["a"],
        )


def test_import_full_clone_allow_missing_tables_preserves_unlisted_tables(tmp_path):
    engine = _sqlite_engine(tmp_path / "import.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("CREATE TABLE b (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("INSERT INTO a VALUES (1, 'old_a')"))
        conn.execute(text("INSERT INTO b VALUES (1, 'keep_b')"))

    bundle_dir = tmp_path / "bundle"
    tables_dir = bundle_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"id": 10, "value": "new_a"}]).to_csv(
        tables_dir / "a.csv",
        index=False,
    )
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "biofilter_version": "4.1.1-test",
                "schema_version": "4.1.1",
                "engine": "sqlite",
                "created_at": "2026-03-17T00:00:00+00:00",
                "tables": [{"name": "a", "rows": 1, "file": "tables/a.csv"}],
            }
        ),
        encoding="utf-8",
    )

    db = FakeDB(engine)
    import_full_clone(
        db=db,
        in_dir=str(bundle_dir),
        fmt="csv",
        allow_missing_tables=True,
    )

    with engine.connect() as conn:
        a_rows = conn.execute(text("SELECT id, value FROM a ORDER BY id")).all()
        b_rows = conn.execute(text("SELECT id, value FROM b ORDER BY id")).all()

    assert a_rows == [(10, "new_a")]
    assert b_rows == [(1, "keep_b")]


def test_import_full_clone_raises_when_bundle_missing_tables_by_default(tmp_path):
    engine = _sqlite_engine(tmp_path / "import_strict.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE a (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(text("CREATE TABLE b (id INTEGER PRIMARY KEY, value TEXT)"))

    bundle_dir = tmp_path / "bundle"
    tables_dir = bundle_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"id": 1, "value": "only_a"}]).to_csv(tables_dir / "a.csv", index=False)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "biofilter_version": "4.1.1-test",
                "schema_version": "4.1.1",
                "engine": "sqlite",
                "created_at": "2026-03-17T00:00:00+00:00",
                "tables": [{"name": "a", "rows": 1, "file": "tables/a.csv"}],
            }
        ),
        encoding="utf-8",
    )

    db = FakeDB(engine)
    with pytest.raises(RuntimeError, match="missing tables required by current schema"):
        import_full_clone(
            db=db,
            in_dir=str(bundle_dir),
            fmt="csv",
            allow_missing_tables=False,
        )


def test_import_full_clone_allow_missing_raises_when_no_common_tables(tmp_path):
    engine = _sqlite_engine(tmp_path / "import_no_common.db")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE b (id INTEGER PRIMARY KEY, value TEXT)"))

    bundle_dir = tmp_path / "bundle"
    tables_dir = bundle_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"id": 1, "value": "only_a"}]).to_csv(tables_dir / "a.csv", index=False)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "biofilter_version": "4.1.1-test",
                "schema_version": "4.1.1",
                "engine": "sqlite",
                "created_at": "2026-03-17T00:00:00+00:00",
                "tables": [{"name": "a", "rows": 1, "file": "tables/a.csv"}],
            }
        ),
        encoding="utf-8",
    )

    db = FakeDB(engine)
    with pytest.raises(RuntimeError, match="No common tables"):
        import_full_clone(
            db=db,
            in_dir=str(bundle_dir),
            fmt="csv",
            allow_missing_tables=True,
        )


def test_import_full_clone_csv_preserves_empty_string_in_non_nullable_text(tmp_path):
    source_engine = _sqlite_engine(tmp_path / "source_empty_string.db")
    with source_engine.begin() as conn:
        conn.execute(text("CREATE TABLE a (id INTEGER PRIMARY KEY, format TEXT NOT NULL)"))
        conn.execute(text("INSERT INTO a (id, format) VALUES (1, '')"))

    bundle_dir = tmp_path / "bundle_empty_string"
    export_full_clone(
        source_engine,
        bundle_dir,
        biofilter_version="4.1.1-test",
        schema_version="4.1.1",
        fmt="csv",
    )

    target_engine = _sqlite_engine(tmp_path / "target_empty_string.db")
    with target_engine.begin() as conn:
        conn.execute(text("CREATE TABLE a (id INTEGER PRIMARY KEY, format TEXT NOT NULL)"))

    db = FakeDB(target_engine)
    import_full_clone(
        db=db,
        in_dir=str(bundle_dir),
        fmt="csv",
        allow_missing_tables=False,
    )

    with target_engine.connect() as conn:
        rows = conn.execute(text("SELECT id, format FROM a ORDER BY id")).all()

    assert rows == [(1, "")]

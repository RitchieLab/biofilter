from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from biofilter.modules.db.transfer import _export_table_parquet


def test_export_table_parquet_streams_chunks_to_single_file(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'streaming.db'}", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)"))
        conn.execute(
            text(
                "INSERT INTO sample (id, value) VALUES "
                "(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"
            )
        )

    out_path = tmp_path / "sample.parquet"
    with engine.connect() as conn:
        _export_table_parquet(
            conn,
            engine,
            "sample",
            out_path,
            chunksize=2,
        )

    df = pd.read_parquet(out_path).sort_values("id").reset_index(drop=True)
    assert df.to_dict(orient="records") == [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
        {"id": 3, "value": "c"},
        {"id": 4, "value": "d"},
        {"id": 5, "value": "e"},
    ]


def test_export_table_parquet_writes_empty_file_for_empty_table(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'empty.db'}", future=True)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE empty_table (id INTEGER PRIMARY KEY, value TEXT)"))

    out_path = tmp_path / "empty_table.parquet"
    with engine.connect() as conn:
        _export_table_parquet(
            conn,
            engine,
            "empty_table",
            out_path,
            chunksize=2,
        )

    df = pd.read_parquet(out_path)
    assert len(df) == 0

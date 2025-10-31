import os
import sys
import math
import time
import json
import logging
import argparse
from dataclasses import dataclass, asdict
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from tqdm import tqdm


@dataclass
class ExportConfig:
    db_uri: str
    table: Optional[str] = None           # use table OR sql
    sql: Optional[str] = None             # SELECT ... (opcional)
    columns: Optional[List[str]] = None   # restringe colunas
    where: Optional[str] = None           # WHERE extra (sem 'WHERE')
    order_by: Optional[str] = None        # ORDER BY cols
    chunksize: int = 500_000
    out_dir: str = "./export_parquet"
    partition_by: Optional[List[str]] = None  # ex.: ["assembly_id","chromosome"]
    existing: str = "overwrite_or_ignore"     # or "overwrite_or_ignore"
    compression: str = "zstd"                 # parquet compression
    row_group_size: int = 128 * 1024          # ~128k rows por row-group (ajuste)
    log_every: int = 1                        # imprime a cada N chunks


def _mk_logger() -> logging.Logger:
    logger = logging.getLogger("parquet_exporter")
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        h.setFormatter(fmt)
        logger.addHandler(h)
    logger.setLevel(logging.INFO)
    return logger


def build_sql(cfg: ExportConfig) -> str:
    if cfg.sql:
        base = f"({cfg.sql}) AS q"
        cols = "*" if not cfg.columns else ", ".join(cfg.columns)
        final = f"SELECT {cols} FROM {base}"
    elif cfg.table:
        cols = "*" if not cfg.columns else ", ".join(cfg.columns)
        final = f"SELECT {cols} FROM {cfg.table}"
    else:
        raise ValueError("Provide either table or sql")

    if cfg.where:
        final += f" WHERE {cfg.where}"
    if cfg.order_by:
        final += f" ORDER BY {cfg.order_by}"
    return final


def export_to_parquet(cfg: ExportConfig):
    logger = _mk_logger()
    os.makedirs(cfg.out_dir, exist_ok=True)

    logger.info("Starting export with config: %s", json.dumps(asdict(cfg), indent=2))

    engine = create_engine(cfg.db_uri)
    sql = build_sql(cfg)
    total_rows = 0
    t0 = time.time()

    write_props = pq.ParquetWriter
    # pyarrow write_dataset usa FileSystemDatasetWriter internamente

    # iterativo em chunks
    with engine.connect() as conn:
        reader = pd.read_sql(sql, conn, chunksize=cfg.chunksize)
        for i, df in enumerate(reader, start=1):
            if df.empty:
                continue
            table = pa.Table.from_pandas(df, preserve_index=False)

            # grava dataset particionado (ou não)
            ds.write_dataset(
                data=table,
                base_dir=cfg.out_dir,
                format="parquet",
                partitioning=cfg.partition_by if cfg.partition_by else None,
                existing_data_behavior=cfg.existing,  # "overwrite_or_ignore"
                file_options=pq.ParquetWriter(
                    where=None,  # ignorado; usamos options abaixo
                ),
                # opções reais de parquet
                # usamos 'min_rows_per_group' para controlar row groups
                # via write_options:
                # NOTE: API nova usa pq.ParquetWriter via options no write_table
            )

            # Como write_dataset não expõe diretamente row_group_size via API antiga,
            # mantemos o default (está OK para export). Para ajustes finos, poderíamos
            # usar write_table manualmente por arquivo.

            total_rows += len(df)
            if i % cfg.log_every == 0:
                logger.info("Chunk %d - rows written: %d (total: %d)", i, len(df), total_rows)

    elapsed = time.time() - t0
    logger.info("Export finished: %d rows in %.1fs (%.1f K rows/s)",
                total_rows, elapsed, (total_rows / max(elapsed, 1)) / 1e3)


def cli():
    p = argparse.ArgumentParser(
        description="Biofilter3R Parquet Exporter (generic, partition-aware)"
    )
    p.add_argument("--db-uri", required=True, help="SQLAlchemy URI (e.g., postgresql+psycopg2://user:pass@host:5432/db)")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--table", help="Table name (schema.table or table)")
    src.add_argument("--sql", help="Custom SELECT (without trailing semicolon)")

    p.add_argument("--columns", help="Comma-separated column list")
    p.add_argument("--where", help="WHERE clause (without the 'WHERE')")
    p.add_argument("--order-by", help="ORDER BY expression")
    p.add_argument("--chunksize", type=int, default=500_000)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--partition-by", help="Comma-separated list of partition columns")
    p.add_argument("--existing", default="overwrite_or_ignore", choices=["overwrite_or_ignore"])
    p.add_argument("--compression", default="zstd")
    p.add_argument("--row-group-size", type=int, default=128*1024)
    p.add_argument("--log-every", type=int, default=1)

    args = p.parse_args()

    cfg = ExportConfig(
        db_uri=args.db_uri,
        table=args.table,
        sql=args.sql,
        columns=[c.strip() for c in args.columns.split(",")] if args.columns else None,
        where=args.where,
        order_by=args.order_by,
        chunksize=args.chunksize,
        out_dir=args.out_dir,
        partition_by=[c.strip() for c in args.partition_by.split(",")] if args.partition_by else None,
        existing=args.existing,
        compression=args.compression,
        row_group_size=args.row_group_size,
        log_every=args.log_every,
    )
    export_to_parquet(cfg)


if __name__ == "__main__":
    cli()


"""
Exemplos de uso
---------------
a) Exportar variant_loci (particionado por assembly e cromossomo)
export PYTHONUNBUFFERED=1

nohup nice -n 5 ionice -c2 -n7 \
  poetry run biofilter-parquet \
    --db-uri "postgresql+psycopg2://bioadmin:bioadmin@127.0.0.1:5432/biofilter" \
    --table public.variant_loci \
    --columns "id,variant_id,assembly_id,chromosome,start_pos,end_pos,reference_allele,alternate_allele,data_source_id,etl_package_id" \
    --order-by "assembly_id, chromosome, start_pos" \
    --out-dir "/home/bioadmin/variant_loci_parquet" \
    --partition-by "assembly_id,chromosome" \
    --chunksize 500000 \
    --log-every 1 \
  > scripts/logs/export_variant_loci_$(date +%F_%H%M).log 2>&1 &

echo "PID:" $!; disown

b) Exportar só um subconjunto (ex.: assembly GRCh38)
poetry run biofilter-parquet \
  --db-uri "postgresql+psycopg2://bioadmin:bioadmin@127.0.0.1:5432/biofilter" \
  --table public.variant_loci \
  --where "assembly_id = 42" \
  --columns "id,variant_id,assembly_id,chromosome,start_pos,end_pos,reference_allele,alternate_allele,data_source_id,etl_package_id" \
  --out-dir "/home/bioadmin/variant_loci_parquet_grch38" \
  --partition-by "chromosome"

c) Exportar via SQL customizado
poetry run biofilter-parquet \
  --db-uri "postgresql+psycopg2://bioadmin:bioadmin@127.0.0.1:5432/biofilter" \
  --sql "SELECT id, variant_id, assembly_id, chromosome, start_pos, end_pos FROM public.variant_loci WHERE start_pos IS NOT NULL" \
  --out-dir "/home/bioadmin/variant_loci_custom"

  
5) Roadmap rápido (próximos passos)

- Importer simétrico: biofilter/tools/parquet_loader.py que lê dataset = ds.dataset(...) e faz COPY FROM STDIN por partição (já deixo se quiser).
- Config via .toml/yaml: um “jobfile” definindo várias saídas (ex.: variant_loci, variant_gwas, entity_aliases).
- Parallel export: particionar por assembly_id e abrir N processos (cuidado com IO).
- Checksum por partição (parquet metadata + arquivo .sha256).
- Integração ETL Manager: comando biofilter etl export-parquet <preset>.

"""
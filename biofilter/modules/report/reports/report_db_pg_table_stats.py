import pandas as pd
from sqlalchemy import text

from biofilter.modules.report.reports.base_report import ReportBase


class DBPgTableStatsReport(ReportBase):
    name = "db_pg_table_stats"
    description = (
        "PostgreSQL-only. Shows per-table row estimates and storage breakdown (table/index/toast/total). "
        "Partitioned tables are expanded with one row per partition plus an aggregated parent row."
    )

    # -----------------------------
    # Helpers / Schema contract
    # -----------------------------
    @classmethod
    def available_columns(cls) -> list[str]:
        """
        Internal column keys that can be requested via output_columns=[...].
        """
        return [
            "schema_name",
            "table_name",
            "is_partitioned_parent",
            "is_partition",
            "parent_schema",
            "parent_table",
            "rows_est",
            "table_bytes",
            "index_bytes",
            "toast_bytes",
            "total_bytes",
            "n_indexes",
            "table_size",
            "index_size",
            "total_size",
        ]

    @classmethod
    def explain(cls) -> str:
        return """\
📊 PostgreSQL Table Stats

This report is PostgreSQL-only and returns:
- One row per physical table (including each partition)
- One aggregated row per partitioned parent table (summing all partitions)

Metrics:
- rows_est: estimated row count (fast, based on catalog statistics)
- table_bytes: heap size
- index_bytes: total size of all indexes on the table
- toast_bytes: TOAST size (computed as total - heap - indexes)
- total_bytes: total relation size (heap + indexes + toast)
- n_indexes: number of indexes

Notes:
- rows_est is an estimate and depends on ANALYZE/autovacuum.
- This is designed for observability, not exact counting.
"""

    def run(self) -> pd.DataFrame:
        # Optional filters
        schema = self.params.get("schema")  # str or list[str], e.g. "public" or ["public","biofilter"]
        table = self.params.get("table")    # str or list[str], table name filter

        # Optional output column selection
        output_columns = self.params.get("output_columns")  # list[str] or None

        # Postgres-only guard
        self._require_postgres()

        sql = text(
            """
            WITH RECURSIVE rels AS (
                SELECT
                    c.oid,
                    n.nspname AS schema_name,
                    c.relname AS table_name,
                    c.relkind,
                    c.relispartition,
                    NULL::oid AS parent_oid
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r','p')
                  AND n.nspname NOT IN ('pg_catalog','information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%'
            ),
            parents AS (
                SELECT
                    child.oid AS child_oid,
                    inh.inhparent AS parent_oid
                FROM pg_inherits inh
                JOIN pg_class child ON child.oid = inh.inhrelid
            ),
            rels_with_parent AS (
                SELECT
                    r.oid,
                    r.schema_name,
                    r.table_name,
                    r.relkind,
                    r.relispartition,
                    p.parent_oid
                FROM rels r
                LEFT JOIN parents p ON p.child_oid = r.oid
            ),
            descendants AS (
                SELECT
                    p.oid AS root_parent_oid,
                    c.oid AS child_oid
                FROM rels_with_parent p
                JOIN pg_inherits i ON i.inhparent = p.oid
                JOIN pg_class c ON c.oid = i.inhrelid
                WHERE p.relkind = 'p'

                UNION ALL

                SELECT
                    d.root_parent_oid,
                    c2.oid AS child_oid
                FROM descendants d
                JOIN pg_inherits i2 ON i2.inhparent = d.child_oid
                JOIN pg_class c2 ON c2.oid = i2.inhrelid
            ),
            parent_agg AS (
                SELECT
                    r.oid AS oid,
                    r.schema_name,
                    r.table_name,
                    TRUE  AS is_partitioned_parent,
                    FALSE AS is_partition,
                    NULL::text AS parent_table,
                    NULL::text AS parent_schema,
                    COALESCE(SUM(pc.reltuples)::bigint, 0) AS rows_est,
                    COALESCE(SUM(pg_relation_size(d.child_oid))::bigint, 0) AS table_bytes,
                    COALESCE(SUM(pg_indexes_size(d.child_oid))::bigint, 0) AS index_bytes,
                    COALESCE(SUM(pg_total_relation_size(d.child_oid) - pg_relation_size(d.child_oid) - pg_indexes_size(d.child_oid))::bigint, 0) AS toast_bytes,
                    COALESCE(SUM(pg_total_relation_size(d.child_oid))::bigint, 0) AS total_bytes,
                    COALESCE(SUM((
                        SELECT count(*)
                        FROM pg_index ix
                        WHERE ix.indrelid = d.child_oid
                    ))::bigint, 0) AS n_indexes
                FROM rels_with_parent r
                LEFT JOIN descendants d ON d.root_parent_oid = r.oid
                LEFT JOIN pg_class pc ON pc.oid = d.child_oid
                WHERE r.relkind = 'p'
                GROUP BY r.oid, r.schema_name, r.table_name
            ),
            leaf_rows AS (
                SELECT
                    r.oid AS oid,
                    r.schema_name,
                    r.table_name,
                    FALSE AS is_partitioned_parent,
                    r.relispartition AS is_partition,
                    p.schema_name AS parent_schema,
                    p.table_name  AS parent_table,
                    c.reltuples::bigint AS rows_est,
                    pg_relation_size(r.oid)::bigint AS table_bytes,
                    pg_indexes_size(r.oid)::bigint  AS index_bytes,
                    (pg_total_relation_size(r.oid) - pg_relation_size(r.oid) - pg_indexes_size(r.oid))::bigint AS toast_bytes,
                    pg_total_relation_size(r.oid)::bigint AS total_bytes,
                    (
                        SELECT count(*)
                        FROM pg_index ix
                        WHERE ix.indrelid = r.oid
                    )::bigint AS n_indexes
                FROM rels_with_parent r
                JOIN pg_class c ON c.oid = r.oid
                LEFT JOIN rels_with_parent p ON p.oid = r.parent_oid
                WHERE r.relkind = 'r'
            )
            SELECT
                schema_name,
                table_name,
                is_partitioned_parent,
                is_partition,
                parent_schema,
                parent_table,
                rows_est,
                table_bytes,
                index_bytes,
                toast_bytes,
                total_bytes,
                n_indexes,
                pg_size_pretty(table_bytes) AS table_size,
                pg_size_pretty(index_bytes) AS index_size,
                pg_size_pretty(total_bytes) AS total_size
            FROM parent_agg

            UNION ALL

            SELECT
                schema_name,
                table_name,
                is_partitioned_parent,
                is_partition,
                parent_schema,
                parent_table,
                rows_est,
                table_bytes,
                index_bytes,
                toast_bytes,
                total_bytes,
                n_indexes,
                pg_size_pretty(table_bytes) AS table_size,
                pg_size_pretty(index_bytes) AS index_size,
                pg_size_pretty(total_bytes) AS total_size
            FROM leaf_rows

            ORDER BY
                total_bytes DESC,
                schema_name,
                table_name;
            """
        )

        df = pd.read_sql(sql, self.session.bind)

        # ----------------------------
        # Optional filters (CI like ETLStatusReport)
        # ----------------------------
        if schema:
            df = self._filter_ci_df(df, "schema_name", schema)

        if table:
            df = self._filter_ci_df(df, "table_name", table)

        # ----------------------------
        # Column selection
        # ----------------------------
        if output_columns:
            allowed = set(self.available_columns())
            cols = [c for c in output_columns if c in allowed and c in df.columns]
            if cols:
                df = df[cols]

        return df

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _require_postgres(self) -> None:
        """
        Ensure the active SQLAlchemy dialect is PostgreSQL.
        """
        dialect = getattr(self.session.bind, "dialect", None)
        name = getattr(dialect, "name", None)
        if name != "postgresql":
            raise RuntimeError(
                f"{self.name} is PostgreSQL-only. Current dialect={name!r}."
            )

    def _filter_ci_df(self, df: pd.DataFrame, col: str, value: str | list[str]) -> pd.DataFrame:
        """
        Case-insensitive filter for DataFrame columns (similar spirit to _filter_ci for SQLAlchemy).
        Accepts str or list[str]. Uses substring match by default.
        """
        if value is None:
            return df

        if isinstance(value, str):
            vals = [value]
        else:
            vals = list(value)

        # substring match, case-insensitive
        mask = False
        s = df[col].astype(str)
        for v in vals:
            mask = mask | s.str.contains(str(v), case=False, na=False)
        return df[mask].copy()
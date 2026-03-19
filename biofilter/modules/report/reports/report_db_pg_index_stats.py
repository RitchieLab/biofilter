import pandas as pd
from sqlalchemy import text

from biofilter.modules.report.reports.base_report import ReportBase


class DBPgIndexStatsReport(ReportBase):
    name = "db_pg_index_stats"
    description = (
        "PostgreSQL-only. Shows per-index storage size and definition. "
        "Optionally includes usage stats from pg_stat_all_indexes."
    )

    # -----------------------------
    # Helpers / Schema contract
    # -----------------------------
    @classmethod
    def available_columns(cls) -> list[str]:
        return [
            "schema_name",
            "table_name",
            "index_name",
            "index_method",
            "is_unique",
            "is_primary",
            "is_valid",
            "is_ready",
            "index_bytes",
            "index_size",
            "index_def",
            # optional usage stats
            "idx_scan",
            "idx_tup_read",
            "idx_tup_fetch",
        ]

    @classmethod
    def explain(cls) -> str:
        return str("DOC IN MD FILE")
#         return """\
# 📚 PostgreSQL Index Stats

# This report is PostgreSQL-only and returns one row per index, including:
# - index size (bytes + pretty)
# - access method (btree/gin/gist/brin/...)
# - properties: unique, primary, valid, ready
# - index definition (pg_get_indexdef)

# Optional:
# - usage counters from pg_stat_all_indexes: idx_scan, idx_tup_read, idx_tup_fetch

# Notes:
# - Usage stats reset when PostgreSQL restarts or when stats are reset.
# - Index size is from pg_relation_size(index_oid).
# """

    def run(self) -> pd.DataFrame:
        # Optional filters
        schema = self.params.get("schema")  # str or list[str]
        table = self.params.get("table")  # str or list[str]
        index = self.params.get("index")  # str or list[str]

        # Options
        include_index_def = bool(self.params.get("include_index_def", True))
        include_usage = bool(self.params.get("include_usage", True))

        # Optional output column selection
        output_columns = self.params.get("output_columns")  # list[str] or None

        # Postgres-only guard
        self._require_postgres()

        # Build SQL dynamically (keep simple: choose columns by include_* flags)
        # We always compute index size; index_def/usage are optional.
        select_parts = [
            "ns.nspname AS schema_name",
            "tbl.relname AS table_name",
            "idx.relname AS index_name",
            "am.amname AS index_method",
            "i.indisunique AS is_unique",
            "i.indisprimary AS is_primary",
            "i.indisvalid AS is_valid",
            "i.indisready AS is_ready",
            "pg_relation_size(idx.oid)::bigint AS index_bytes",
            "pg_size_pretty(pg_relation_size(idx.oid)) AS index_size",
        ]

        if include_index_def:
            select_parts.append("pg_get_indexdef(idx.oid) AS index_def")
        else:
            select_parts.append("NULL::text AS index_def")

        if include_usage:
            select_parts.extend(
                [
                    "COALESCE(st.idx_scan, 0)::bigint AS idx_scan",
                    "COALESCE(st.idx_tup_read, 0)::bigint AS idx_tup_read",
                    "COALESCE(st.idx_tup_fetch, 0)::bigint AS idx_tup_fetch",
                ]
            )
            usage_join = """
                LEFT JOIN pg_stat_all_indexes st
                    ON st.indexrelid = idx.oid
            """
        else:
            select_parts.extend(
                [
                    "NULL::bigint AS idx_scan",
                    "NULL::bigint AS idx_tup_read",
                    "NULL::bigint AS idx_tup_fetch",
                ]
            )
            usage_join = ""

        sql = text(
            f"""
            SELECT
                {", ".join(select_parts)}
            FROM pg_index i
            JOIN pg_class tbl ON tbl.oid = i.indrelid
            JOIN pg_class idx ON idx.oid = i.indexrelid
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            JOIN pg_am am ON am.oid = idx.relam
            {usage_join}
            WHERE ns.nspname NOT IN ('pg_catalog','information_schema')
              AND ns.nspname NOT LIKE 'pg_toast%'
            ORDER BY
                index_bytes DESC,
                schema_name,
                table_name,
                index_name;
            """
        )

        df = pd.read_sql(sql, self.session.bind)

        # ----------------------------
        # Optional filters (DF-level, CI)
        # ----------------------------
        if schema:
            df = self._filter_ci_df(df, "schema_name", schema)

        if table:
            df = self._filter_ci_df(df, "table_name", table)

        if index:
            df = self._filter_ci_df(df, "index_name", index)

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
        dialect = getattr(self.session.bind, "dialect", None)
        name = getattr(dialect, "name", None)
        if name != "postgresql":
            raise RuntimeError(
                f"{self.name} is PostgreSQL-only. Current dialect={name!r}."
            )

    def _filter_ci_df(
        self, df: pd.DataFrame, col: str, value: str | list[str]
    ) -> pd.DataFrame:
        if value is None:
            return df

        if isinstance(value, str):
            vals = [value]
        else:
            vals = list(value)

        s = df[col].astype(str)
        mask = False
        for v in vals:
            mask = mask | s.str.contains(str(v), case=False, na=False)
        return df[mask].copy()


"""
biofilter report run pg_index_stats
biofilter report run pg_index_stats --params '{"schema":"public"}'
biofilter report run pg_index_stats --params '{"table":["variant","entity"]}'
biofilter report run pg_index_stats --params '{"index":"trgm"}'
biofilter report run pg_index_stats --params '{"include_usage":false}'
biofilter report run pg_index_stats --params '{"output_columns":["schema_name","table_name","index_name","index_size","idx_scan"]}'

"""
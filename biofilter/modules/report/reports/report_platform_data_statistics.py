"""
What is in this bundle: how much, of what, and how big.

Migrated from `platform_data_statistics` (ADR-004 §6, §2.12). A platform
report — it describes the bundle, not the biology in it, and takes no
input beyond which sections to compute.

The long shape of the original is kept: one row per measurement, with
`section` and `metric` naming it and two free dimensions. Heterogeneous
statistics do not fit a wide table, and a wide one would have to change
shape every time a section is added.

Storage comes from `manifest.json`, so file sizes cost nothing to report.
Counts come from the data, because the manifest counts rows per *file*
and a question like "how many variants per chromosome" should not depend
on reading a chromosome out of a filename.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

import pyarrow as pa

from biofilter.modules.report.reports.base_report import ReportBase

SECTIONS = (
    "bundle",
    "storage",
    "entities",
    "variants",
    "relationships",
    "sources",
)

#: Variant tables worth breaking down by chromosome, when present.
VARIANT_TABLES = (
    "variant_masters",
    "variant_molecular_effects",
    "variant_rsid",
    "variant_predictions",
    "variant_alphamissense",
    "variant_gtex",
)


class PlatformDataStatisticsReport(ReportBase):
    name = "platform_data_statistics"
    description = (
        "What this bundle holds: identity, table sizes on disk, entity counts by "
        "domain, variant counts by chromosome, relationship counts by group pair, "
        "and what each data source contributed."
    )

    requires = ("entities", "entity_groups")

    #: Each one is a section that simply does not appear without it.
    optional = (
        "entity_relationships",
        "entity_relationship_types",
        "etl_data_sources",
        "etl_packages",
        "etl_source_systems",
    ) + VARIANT_TABLES

    COLUMNS = (
        "section",
        "metric",
        "dimension_1",
        "dimension_2",
        "value_number",
        "value_text",
        "as_of",
        "note",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {"sections": list(SECTIONS)}

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_sections(value: Any) -> list[str]:
        if value is None:
            return list(SECTIONS)
        values = value if isinstance(value, (list, tuple, set)) else [value]
        chosen = [str(v).strip().lower() for v in values if str(v).strip()]
        unknown = [s for s in chosen if s not in SECTIONS]
        if unknown:
            raise ValueError(
                f"Unknown section(s): {unknown}. Available: {list(SECTIONS)}"
            )
        return chosen or list(SECTIONS)

    @staticmethod
    def _rows_to_table(rows: Iterable[dict[str, Any]]) -> pa.Table:
        rows = list(rows)
        return pa.table(
            {
                "section": pa.array([r["section"] for r in rows], pa.string()),
                "metric": pa.array([r["metric"] for r in rows], pa.string()),
                "dimension_1": pa.array(
                    [r.get("dimension_1") for r in rows], pa.string()
                ),
                "dimension_2": pa.array(
                    [r.get("dimension_2") for r in rows], pa.string()
                ),
                "value_number": pa.array(
                    [r.get("value_number") for r in rows], pa.float64()
                ),
                "value_text": pa.array([r.get("value_text") for r in rows], pa.string()),
                "as_of": pa.array([r.get("as_of") for r in rows], pa.string()),
                "note": pa.array([r.get("note") for r in rows], pa.string()),
            }
        )

    def _sql_rows(self, section: str, query: str) -> list[dict[str, Any]]:
        """Run a section's query and shape it into measurement rows."""
        table = self.sql(query)
        return [
            {
                "section": section,
                "metric": r["metric"],
                "dimension_1": r.get("dimension_1"),
                "dimension_2": r.get("dimension_2"),
                "value_number": (
                    None if r.get("value_number") is None else float(r["value_number"])
                ),
                "value_text": r.get("value_text"),
                "as_of": r.get("as_of"),
                "note": r.get("note"),
            }
            for r in table.to_pylist()
        ]

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        sections = self._parse_sections(self.param("sections"))
        rows: list[dict[str, Any]] = []

        if "bundle" in sections:
            rows += self._bundle_section()
        if "storage" in sections:
            rows += self._storage_section()
            self._emit_storage()
        if "entities" in sections:
            rows += self._entities_section()
        if "variants" in sections:
            rows += self._variants_section()
            self._emit_variants()
        if "relationships" in sections:
            rows += self._relationships_section()
        if "sources" in sections:
            rows += self._sources_section()

        if not rows:
            rows = [
                {
                    "section": "bundle",
                    "metric": "no_sections",
                    "value_text": "nothing selected",
                }
            ]
        return self._rows_to_table(rows)

    # ------------------------------------------------------------------
    # The two sections the long shape cannot hold faithfully
    # ------------------------------------------------------------------
    #
    # The long shape is right for the rest: heterogeneous measurements
    # share one set of columns, and `relationships` alone carries two
    # metrics of different shapes, so "one table per section" is not even
    # well defined. These two are different — the long form loses
    # something in each, and what it loses is the part you would sort by.

    def _emit_storage(self) -> None:
        """
        Per table: rows, bytes, files — with bytes as a number.

        In the long shape a table's size survives twice and neither is
        usable: `value_text` rounds it to "3.4 MB" and `note` buries the
        exact figure in "1 file(s), 3416028 bytes". Ordering a bundle's
        tables by size means parsing a sentence.
        """
        by_file: dict[str, tuple[int, int]] = {}
        for entry in self.bundle.manifest.get("tables") or []:
            logical = entry.get("table") or entry.get("name")
            if not logical:
                continue
            size, count = by_file.get(logical, (0, 0))
            by_file[logical] = (size + int(entry.get("bytes") or 0), count + 1)

        names, branches, rows_n, bytes_n, files_n = [], [], [], [], []
        for name, table in sorted(self.bundle.tables.items()):
            size, files = by_file.get(name, (0, len(table.files)))
            names.append(name)
            branches.append(table.branch)
            rows_n.append(int(table.rows))
            bytes_n.append(int(size))
            files_n.append(int(files))

        self.emit(
            "storage",
            pa.table(
                {
                    "table": pa.array(names, pa.string()),
                    "branch": pa.array(branches, pa.string()),
                    "rows": pa.array(rows_n, pa.int64()),
                    "bytes": pa.array(bytes_n, pa.int64()),
                    "files": pa.array(files_n, pa.int32()),
                }
            ),
        )

    def _emit_variants(self) -> None:
        """
        Rows per chromosome, with the chromosome as a number.

        `dimension_2` is a string, so a reader sorting it gets 1, 10, 11,
        2 — and the bundle now carries enough chromosomes for that to be
        the usual outcome rather than a curiosity.
        """
        present = [name for name in VARIANT_TABLES if self.bundle.has(name)]
        if not present:
            return

        union = "\nUNION ALL\n".join(
            f"""SELECT '{name}' AS "table", chromosome, count(*) AS rows
                FROM "{name}" GROUP BY chromosome"""
            for name in present
        )
        self.emit(
            "variants",
            self.sql(f'SELECT * FROM ({union}) ORDER BY "table", chromosome'),
        )

    # ------------------------------------------------------------------
    # Sections read from the manifest — no scan
    # ------------------------------------------------------------------
    def _bundle_section(self) -> list[dict[str, Any]]:
        manifest = self.bundle.manifest
        tables = self.bundle.tables
        total_bytes = sum(
            int(entry.get("bytes") or 0) for entry in manifest.get("tables") or []
        )
        created = manifest.get("created_at")

        def row(metric, number=None, text=None, note=None):
            return {
                "section": "bundle",
                "metric": metric,
                "value_number": number,
                "value_text": text,
                "as_of": created,
                "note": note,
            }

        return [
            row("bundle_id", text=self.bundle.bundle_id),
            row("biofilter_version", text=manifest.get("biofilter_version")),
            row("schema_version", text=manifest.get("schema_version")),
            row("created_at", text=created),
            row("tables", number=float(len(tables))),
            row("files", number=float(sum(len(t.files) for t in tables.values()))),
            row("rows", number=float(sum(t.rows for t in tables.values()))),
            row(
                "bytes",
                number=float(total_bytes),
                note=f"{total_bytes / 1e9:.2f} GB as declared by the manifest",
            ),
            row(
                "tables_without_rows",
                number=float(sum(1 for t in tables.values() if t.rows == 0)),
                note="declared, and empty — a source that was planned and did not land",
            ),
        ]

    def _storage_section(self) -> list[dict[str, Any]]:
        """
        Per table: rows, bytes on disk, and how many files it spans.

        All three come from the manifest, so this costs no I/O at all —
        the sizes of a 21 GB bundle are read from a few hundred lines of
        JSON.
        """
        by_file = {}
        for entry in self.bundle.manifest.get("tables") or []:
            logical = entry.get("table") or entry.get("name")
            if not logical:
                continue
            size, count = by_file.get(logical, (0, 0))
            by_file[logical] = (size + int(entry.get("bytes") or 0), count + 1)

        rows = []
        for name, table in sorted(self.bundle.tables.items()):
            size, files = by_file.get(name, (0, len(table.files)))
            rows.append(
                {
                    "section": "storage",
                    "metric": "table",
                    "dimension_1": name,
                    "dimension_2": table.branch,
                    "value_number": float(table.rows),
                    "value_text": f"{size / 1e6:.1f} MB",
                    "note": f"{files} file(s), {size} bytes",
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Sections read from the data
    # ------------------------------------------------------------------
    def _entities_section(self) -> list[dict[str, Any]]:
        return self._sql_rows(
            "entities",
            """
            SELECT
                'entities_by_group' AS metric,
                g.name              AS dimension_1,
                CAST(NULL AS VARCHAR) AS dimension_2,
                count(*)            AS value_number,
                CAST(NULL AS VARCHAR) AS value_text,
                CAST(NULL AS VARCHAR) AS as_of,
                CAST(NULL AS VARCHAR) AS note
            FROM entities e
            JOIN entity_groups g ON g.id = e.group_id
            GROUP BY g.name
            ORDER BY value_number DESC
            """,
        )

    def _variants_section(self) -> list[dict[str, Any]]:
        """
        Counts per chromosome, for each variant table the bundle carries.

        Grouped from the data rather than derived from filenames. It is
        affordable because `chromosome` is a real column with row-group
        statistics: 2.2 billion rows group in about a second.
        """
        rows: list[dict[str, Any]] = []
        for name in VARIANT_TABLES:
            if not self.bundle.has(name):
                continue
            rows += self._sql_rows(
                "variants",
                f"""
                SELECT
                    'variants_by_chromosome' AS metric,
                    '{name}'                 AS dimension_1,
                    CAST(chromosome AS VARCHAR) AS dimension_2,
                    count(*)                 AS value_number,
                    CAST(NULL AS VARCHAR)    AS value_text,
                    CAST(NULL AS VARCHAR)    AS as_of,
                    CAST(NULL AS VARCHAR)    AS note
                FROM "{name}"
                GROUP BY chromosome
                ORDER BY chromosome
                """,
            )
        return rows

    def _relationships_section(self) -> list[dict[str, Any]]:
        if not self.bundle.has("entity_relationships"):
            return []

        rows = self._sql_rows(
            "relationships",
            """
            SELECT
                'relationships_by_group_pair' AS metric,
                coalesce(g1.name, 'Unknown')  AS dimension_1,
                coalesce(g2.name, 'Unknown')  AS dimension_2,
                count(*)                      AS value_number,
                CAST(NULL AS VARCHAR)         AS value_text,
                CAST(NULL AS VARCHAR)         AS as_of,
                CAST(NULL AS VARCHAR)         AS note
            FROM entity_relationships r
            LEFT JOIN entity_groups g1 ON g1.id = r.entity_1_group_id
            LEFT JOIN entity_groups g2 ON g2.id = r.entity_2_group_id
            GROUP BY 2, 3
            ORDER BY value_number DESC
            """,
        )
        if self.bundle.has("entity_relationship_types"):
            rows += self._sql_rows(
                "relationships",
                """
                SELECT
                    'relationships_by_type'      AS metric,
                    coalesce(t.code, 'Unknown')  AS dimension_1,
                    CAST(NULL AS VARCHAR)        AS dimension_2,
                    count(*)                     AS value_number,
                    t.description                AS value_text,
                    CAST(NULL AS VARCHAR)        AS as_of,
                    CAST(NULL AS VARCHAR)        AS note
                FROM entity_relationships r
                LEFT JOIN entity_relationship_types t
                       ON t.id = r.relationship_type_id
                GROUP BY t.code, t.description
                ORDER BY value_number DESC
                """,
            )
        return rows

    def _sources_section(self) -> list[dict[str, Any]]:
        if not (
            self.bundle.has("etl_data_sources") and self.bundle.has("etl_packages")
        ):
            return []
        return self._sql_rows(
            "sources",
            """
            WITH latest AS (
                SELECT
                    p.data_source_id,
                    p.operation_type,
                    p.load_end,
                    p.transform_end,
                    coalesce(p.load_rows, p.transform_rows, p.extract_rows) AS rows
                FROM etl_packages p
                WHERE lower(coalesce(p.status, '')) NOT LIKE '%fail%'
                QUALIFY row_number() OVER (
                    PARTITION BY p.data_source_id ORDER BY p.id DESC
                ) = 1
            )
            SELECT
                'latest_run'                   AS metric,
                ds.name                        AS dimension_1,
                ss.name                        AS dimension_2,
                l.rows                         AS value_number,
                l.operation_type               AS value_text,
                CAST(coalesce(l.load_end, l.transform_end) AS VARCHAR) AS as_of,
                ds.data_type                   AS note
            FROM etl_data_sources ds
            LEFT JOIN etl_source_systems ss ON ss.id = ds.source_system_id
            LEFT JOIN latest l ON l.data_source_id = ds.id
            ORDER BY ds.name
            """,
        )

"""
Genes to variants: which variants belong to these genes, and how.

Replaces `gene_to_variant_filtering`, and retires
`variant_annotation_expanded` with it (ADR-004 §2.14) — that report was
this one with no filters and a gene list scraped out of another report's
CSV by column name.

Two mechanisms decide what "belongs to" means, and they are not the same
question, so the caller picks one.

**position** — the variant's coordinate falls inside the gene's build-38
range, optionally widened by `window_bp`. Answers "what sits in this
stretch of sequence".

**annotation** — VEP linked the variant to the gene. Answers "what did
the annotator associate with this gene", which includes up to 5 kb
outside its body and excludes variants inside it that VEP attributed
elsewhere.

Neither contains the other. Measured over all 958 chr22 genes that have
build-38 coordinates and annotated variants: 2,045,943 gene-variant pairs
by position, 2,651,135 by annotation, of which **144,488 exist only by
position** (139 genes have at least one) and some 749,680 only by
annotation. The overlap is large enough that a single gene often shows
one mapping as a clean subset of the other — which is exactly why the
choice must be recorded rather than inferred from the rows.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import pyarrow as pa

from biofilter.modules.report.reports._resolution import ALIAS_KEY
from biofilter.modules.report.reports.base_report import ReportBase

MAPPINGS = ("position", "annotation")
DEFAULT_BUILD = 38
DEFAULT_MAX_PER_GENE = 5000


class ExpandGeneToVariantReport(ReportBase):
    name = "expand_gene_to_variant"
    description = (
        "Variants belonging to a list of genes, by positional overlap or by VEP "
        "annotation. Filters on impact, consequence, frequency and the in-silico "
        "predictors."
    )

    requires = (
        "entities",
        "entity_aliases",
        "entity_groups",
        "gene_masters",
        "variant_masters",
        "variant_molecular_effects",
    )

    optional = (
        "entity_locations",
        "variant_predictions",
        "variant_alphamissense",
        "variant_consequences",
        "variant_rsid",
    )

    COLUMNS = (
        "input_gene",
        "gene_symbol",
        "gene_entity_id",
        "mapping",
        "status",
        "note",
        "variant_key",
        "rsid",
        "chromosome",
        "position",
        "reference_allele",
        "alternate_allele",
        "af_joint",
        "ac_joint",
        "transcript_id",
        "consequence",
        "consequence_group",
        "severity_rank",
        "impact",
        "canonical",
        "mane_select",
        "lof",
        "cadd_phred",
        "revel_max",
        "sift_max",
        "polyphen_max",
        "alphamissense_score",
        "alphamissense_classification",
        "variants_available",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["CHEK2", "SMARCB1"],
            "mapping": "position",
            "window_bp": 0,
            "most_severe_only": True,
            "af_max": 0.01,
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default

    def _int_param(self, name: str, default: int) -> int:
        """
        An explicit zero is an answer, not an absence.

        `int(param or default)` reads well and is wrong: it turns
        `max_variants_per_gene=0` — the way a caller asks for no cap —
        back into the cap.
        """
        value = self.param(name, default)
        if value is None or (isinstance(value, str) and not value.strip()):
            return default
        return int(value)

    @staticmethod
    def _as_list(value: Any) -> Optional[list[str]]:
        if value is None:
            return None
        values = value if isinstance(value, (list, tuple, set)) else [value]
        cleaned = [str(v).strip() for v in values if str(v).strip()]
        return cleaned or None

    @staticmethod
    def _sql_list(values: list[str]) -> str:
        return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

    def _mapping(self) -> str:
        value = str(self.param("mapping", "position") or "position").strip().lower()
        if value not in MAPPINGS:
            raise ValueError(
                f"mapping must be one of {MAPPINGS}. Got: {value!r}. "
                f"'position' overlaps coordinates; 'annotation' follows VEP's "
                f"gene assignment. They return different variants."
            )
        return value

    def _narrow_to_gene_chromosomes(self, build: int) -> str:
        """
        The chromosomes the input genes sit on, as a WHERE clause.

        Empty unless *every* resolved gene has coordinates. Almost half
        the bundle's genes have none, and under `annotation` mapping VEP
        associates variants by symbol rather than by position — so
        narrowing on a partial set would drop a gene's variants for no
        reason the caller could see. All or nothing is the only version
        of this that cannot lose rows.
        """
        row = self.sql(
            f"""
            WITH genes AS (
                SELECT DISTINCT gm.entity_id
                FROM gene_input i
                JOIN entity_aliases a ON {ALIAS_KEY} = i.input_gene_norm
                JOIN gene_masters gm ON gm.entity_id = a.entity_id
            )
            SELECT
                count(*) AS genes,
                count(l.entity_id) AS placed,
                list_sort(list_distinct(list(l.chromosome))) AS chromosomes
            FROM genes g
            LEFT JOIN (
                SELECT DISTINCT entity_id, chromosome
                FROM entity_locations WHERE build = {build}
            ) l ON l.entity_id = g.entity_id
            """
        ).to_pylist()
        stats = row[0] if row else {}
        chromosomes = stats.get("chromosomes") or []
        if not chromosomes or stats.get("placed") != stats.get("genes"):
            return ""
        return f"WHERE chromosome IN ({', '.join(str(int(c)) for c in chromosomes)})"

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        genes = self.resolve_input_list(
            self.param("input_data", required=True), param_name="input_data"
        )
        if not genes:
            raise ValueError("input_data must contain at least one gene.")

        mapping = self._mapping()
        build = self._int_param("build", DEFAULT_BUILD)
        window = self._int_param("window_bp", 0)
        most_severe_only = self._parse_bool(self.param("most_severe_only"), True)
        max_per_gene = self._int_param("max_variants_per_gene", DEFAULT_MAX_PER_GENE)
        if max_per_gene < 0:
            raise ValueError(
                "max_variants_per_gene must be 0 (no cap) or a positive count. "
                f"Got: {max_per_gene}."
            )
        if window < 0:
            raise ValueError(
                "window_bp must be 0 or positive; it widens the gene's range "
                f"in both directions. Got: {window}."
            )
        emit_not_found = self._parse_bool(self.param("emit_not_found_rows"), True)

        if mapping == "annotation" and window:
            raise ValueError(
                "window_bp applies to mapping='position' only. VEP's own "
                "association already reaches about 5 kb beyond a gene, and "
                "widening it here would not change what VEP recorded."
            )

        # The one thing the rows cannot tell a reader.
        self.note_provenance(
            "mapping",
            {
                "mechanism": mapping,
                "build": build if mapping == "position" else None,
                "window_bp": window if mapping == "position" else None,
                "means": (
                    "variant coordinate inside the gene's range"
                    if mapping == "position"
                    else "VEP associated the variant with the gene"
                ),
            },
        )

        self.register_input(genes, name="gene_input", column="input_gene")

        has_predictions = self.bundle.has("variant_predictions")
        has_alphamissense = self.bundle.has("variant_alphamissense")
        has_consequences = self.bundle.has("variant_consequences")
        has_rsid = self.bundle.has("variant_rsid")
        narrow = self._narrow_to_gene_chromosomes(build)

        if mapping == "position" and not self.bundle.has("entity_locations"):
            raise ValueError(
                "mapping='position' needs entity_locations, which this bundle "
                "does not carry. Use mapping='annotation'."
            )

        filters = self._variant_filters()
        effect_filters = self._effect_filters()

        predictions = (
            f"SELECT * FROM variant_predictions {narrow}"
            if has_predictions
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                   CAST(NULL AS VARCHAR) AS reference_allele,
                   CAST(NULL AS VARCHAR) AS alternate_allele,
                   CAST(NULL AS DOUBLE) AS cadd_phred, CAST(NULL AS DOUBLE) AS revel_max,
                   CAST(NULL AS DOUBLE) AS sift_max, CAST(NULL AS DOUBLE) AS polyphen_max
            WHERE false
            """
        )
        # AlphaMissense scores a variant *on a transcript*, and the
        # transcript it picked is rarely the one VEP calls most severe.
        # Requiring both to agree throws away ~97% of the scores the
        # bundle actually holds — silently, as a null. So the join key
        # follows the grain of the row: a row that is one variant gets
        # the variant's score; a row that is one transcript gets that
        # transcript's.
        am_per_transcript = not most_severe_only
        am_partition = (
            "chromosome, position, reference_allele, alternate_allele, "
            "split_part(transcript_id, '.', 1)"
            if am_per_transcript
            else "chromosome, position, reference_allele, alternate_allele"
        )
        alphamissense = (
            f"""
            SELECT chromosome, position, reference_allele, alternate_allele,
                   split_part(transcript_id, '.', 1) AS transcript_id,
                   score, classification
            FROM variant_alphamissense {narrow}
            QUALIFY row_number() OVER (
                PARTITION BY {am_partition}
                ORDER BY score DESC, transcript_id
            ) = 1
            """
            if has_alphamissense
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                   CAST(NULL AS VARCHAR) AS reference_allele,
                   CAST(NULL AS VARCHAR) AS alternate_allele,
                   CAST(NULL AS VARCHAR) AS transcript_id,
                   CAST(NULL AS DOUBLE) AS score, CAST(NULL AS VARCHAR) AS classification
            WHERE false
            """
        )
        am_transcript_join = (
            "\n                      AND am.transcript_id = p.transcript_id"
            if am_per_transcript
            else ""
        )
        if has_alphamissense:
            self.note_provenance(
                "alphamissense",
                {
                    "joined_on": (
                        "variant and transcript"
                        if am_per_transcript
                        else "variant"
                    ),
                    "means": (
                        "Each row is one transcript, so the score is that "
                        "transcript's."
                        if am_per_transcript
                        else "Each row is one variant, so the score is the "
                        "highest AlphaMissense recorded for it across "
                        "transcripts. Set most_severe_only=false for "
                        "per-transcript scores."
                    ),
                },
            )
        consequences = (
            "SELECT name, severity_rank, consequence_group FROM variant_consequences"
            if has_consequences
            else """
            SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS INTEGER) AS severity_rank,
                   CAST(NULL AS VARCHAR) AS consequence_group
            WHERE false
            """
        )
        rsids = (
            "SELECT chromosome, position, reference_allele, alternate_allele, rsid "
            f"FROM variant_rsid {narrow}"
            if has_rsid
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                   CAST(NULL AS VARCHAR) AS reference_allele,
                   CAST(NULL AS VARCHAR) AS alternate_allele, CAST(NULL AS VARCHAR) AS rsid
            WHERE false
            """
        )

        # The two mechanisms differ only here: which variants a gene owns.
        if mapping == "position":
            pairs = f"""
            SELECT
                g.input_gene, g.gene_symbol, g.gene_entity_id,
                e.chromosome, e.position, e.reference_allele, e.alternate_allele,
                e.variant_key, e.feature AS transcript_id, e.consequence, e.impact,
                e.canonical, e.mane_select, e.lof
            FROM gene_ranges g
            JOIN effects e
              ON e.chromosome = g.chromosome
             AND e.position BETWEEN g.start_pos - {window} AND g.end_pos + {window}
            """
        else:
            pairs = """
            SELECT
                g.input_gene, g.gene_symbol, g.gene_entity_id,
                e.chromosome, e.position, e.reference_allele, e.alternate_allele,
                e.variant_key, e.feature AS transcript_id, e.consequence, e.impact,
                e.canonical, e.mane_select, e.lof
            FROM gene_ids g
            JOIN effects e
              ON e.symbol = g.gene_symbol
            """

        severe_clause = (
            """
            QUALIFY row_number() OVER (
                PARTITION BY input_gene, variant_key
                ORDER BY coalesce(severity_rank, 9999), transcript_id
            ) = 1
            """
            if most_severe_only
            else ""
        )
        cap_clause = (
            f"""
            QUALIFY row_number() OVER (
                PARTITION BY input_gene
                ORDER BY coalesce(severity_rank, 9999), af_joint DESC NULLS LAST,
                         variant_key
            ) <= {max_per_gene}
            """
            if max_per_gene > 0
            else ""
        )
        not_found = "" if emit_not_found else "AND false"

        # A gene the bundle cannot place is not a gene without variants.
        # Under position mapping the two are indistinguishable in the
        # output unless the report says which happened.
        if mapping == "position":
            unplaced_join = (
                "LEFT JOIN gene_ranges r ON r.input_gene = i.input_gene"
            )
            status_case = """
                CASE WHEN g.gene_symbol IS NULL THEN 'not_found'
                     WHEN r.input_gene IS NULL THEN 'no_location'
                     ELSE 'no_variants' END
            """
            note_case = f"""
                CASE WHEN g.gene_symbol IS NULL
                     THEN 'Input did not resolve to a gene in this bundle.'
                     WHEN r.input_gene IS NULL
                     THEN 'Gene resolved, but this bundle carries no build-{build} '
                          || 'coordinates for it, so position mapping cannot place '
                          || 'it. Try mapping=''annotation''.'
                     ELSE 'Gene resolved and placed; no variant in its range met '
                          || 'the criteria.' END
            """
        else:
            unplaced_join = ""
            status_case = (
                "CASE WHEN g.gene_symbol IS NULL THEN 'not_found' "
                "ELSE 'no_variants' END"
            )
            note_case = """
                CASE WHEN g.gene_symbol IS NULL
                     THEN 'Input did not resolve to a gene in this bundle.'
                     ELSE 'Gene resolved; VEP associated no variant meeting the '
                          || 'criteria with it.' END
            """

        table = self.sql(
            f"""
            WITH gene_ids AS (
                SELECT DISTINCT
                    i.input_gene,
                    gm.symbol    AS gene_symbol,
                    gm.entity_id AS gene_entity_id
                FROM gene_input i
                JOIN entity_aliases a ON {ALIAS_KEY} = i.input_gene_norm
                JOIN gene_masters gm ON gm.entity_id = a.entity_id
            ),
            gene_ranges AS (
                SELECT
                    g.input_gene, g.gene_symbol, g.gene_entity_id,
                    l.chromosome, l.start_pos, l.end_pos
                FROM gene_ids g
                JOIN entity_locations l
                  ON l.entity_id = g.gene_entity_id AND l.build = {build}
            ),
            predictions AS ({predictions}),
            alphamissense AS ({alphamissense}),
            consequences AS ({consequences}),
            rsids AS ({rsids}),
            effects AS (SELECT * FROM variant_molecular_effects {narrow}),
            masters AS (SELECT * FROM variant_masters {narrow}),
            pairs AS ({pairs}),
            enriched AS (
                SELECT
                    p.*,
                    v.af_joint, v.ac_joint,
                    rs.rsid,
                    c.severity_rank, c.consequence_group,
                    pr.cadd_phred, pr.revel_max, pr.sift_max, pr.polyphen_max,
                    am.score AS alphamissense_score,
                    am.classification AS alphamissense_classification
                FROM pairs p
                JOIN masters v
                  ON v.chromosome = p.chromosome AND v.position = p.position
                 AND v.reference_allele = p.reference_allele
                 AND v.alternate_allele = p.alternate_allele
                LEFT JOIN rsids rs
                       ON rs.chromosome = p.chromosome AND rs.position = p.position
                      AND rs.reference_allele = p.reference_allele
                      AND rs.alternate_allele = p.alternate_allele
                LEFT JOIN consequences c ON c.name = p.consequence
                LEFT JOIN predictions pr
                       ON pr.chromosome = p.chromosome AND pr.position = p.position
                      AND pr.reference_allele = p.reference_allele
                      AND pr.alternate_allele = p.alternate_allele
                LEFT JOIN alphamissense am
                       ON am.chromosome = p.chromosome AND am.position = p.position
                      AND am.reference_allele = p.reference_allele
                      AND am.alternate_allele = p.alternate_allele{am_transcript_join}
                {filters}
            ),
            narrowed AS (
                SELECT * FROM enriched
                {effect_filters}
                {severe_clause}
            ),
            capped AS (
                -- The window is evaluated before QUALIFY filters, so this
                -- counts what the gene had *before* the cap took hold.
                SELECT *, count(*) OVER (PARTITION BY input_gene) AS available
                FROM narrowed
                {cap_clause}
            )
            SELECT
                input_gene, gene_symbol, gene_entity_id,
                '{mapping}' AS mapping,
                'ok' AS status, CAST(NULL AS VARCHAR) AS note,
                variant_key, rsid, chromosome, position,
                reference_allele, alternate_allele,
                af_joint, ac_joint,
                transcript_id, consequence, consequence_group, severity_rank,
                impact, canonical, mane_select, lof,
                cadd_phred, revel_max, sift_max, polyphen_max,
                alphamissense_score, alphamissense_classification,
                available AS variants_available
            FROM capped

            UNION ALL

            SELECT
                i.input_gene,
                g.gene_symbol, g.gene_entity_id,
                '{mapping}',
                {status_case},
                {note_case},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, 0
            FROM gene_input i
            LEFT JOIN gene_ids g ON g.input_gene = i.input_gene
            {unplaced_join}
            WHERE i.input_gene NOT IN (SELECT input_gene FROM capped)
              {not_found}

            ORDER BY input_gene, severity_rank NULLS LAST, variant_key
            """
        )
        self._note_truncation(table, max_per_gene)
        return table

    # ------------------------------------------------------------------
    def _note_truncation(self, table: pa.Table, max_per_gene: int) -> None:
        """
        Say so when the cap hid rows.

        A capped gene comes back looking like a complete answer: the count
        is a round number and nothing in the rows admits that more existed.
        `variants_available` carries the pre-cap total, so the difference
        is knowable — and belongs in the provenance, where a reader
        checking whether to trust the numbers will look.
        """
        cut: dict[str, dict[str, int]] = {}
        if max_per_gene > 0 and table.num_rows:
            genes = table.column("input_gene").to_pylist()
            available = table.column("variants_available").to_pylist()
            returned: dict[str, int] = {}
            totals: dict[str, int] = {}
            for gene, total in zip(genes, available):
                if not total:  # the not-found / no-variants rows
                    continue
                returned[gene] = returned.get(gene, 0) + 1
                totals[gene] = total
            cut = {
                gene: {"returned": count, "available": totals[gene]}
                for gene, count in returned.items()
                if totals[gene] > count
            }

        self.note_provenance(
            "truncation",
            {
                "max_variants_per_gene": max_per_gene or None,
                "applied": bool(cut),
                "genes": cut or None,
                "means": (
                    "These genes had more variants than the cap allowed; the "
                    "rows kept are the most severe, then the most common. "
                    "Raise max_variants_per_gene or narrow the filters to see "
                    "the rest."
                    if cut
                    else "Every gene returned in full."
                ),
            },
        )

    # ------------------------------------------------------------------
    def _variant_filters(self) -> str:
        """Filters on the variant itself, applied in the enrich join."""
        clauses = []
        af_max = self.param("af_max")
        af_min = self.param("af_min")
        if af_max is not None:
            clauses.append(f"v.af_joint <= {float(af_max)}")
        if af_min is not None:
            clauses.append(f"v.af_joint >= {float(af_min)}")
        return ("WHERE " + " AND ".join(clauses)) if clauses else ""

    def _effect_filters(self) -> str:
        """Filters on the annotation, applied after enrichment."""
        clauses = []
        impact = self._as_list(self.param("impact_filter"))
        consequence = self._as_list(self.param("consequence_type_filter"))
        lof = self._as_list(self.param("lof_confidence_filter"))
        am_class = self._as_list(self.param("alphamissense_classification"))

        if impact:
            clauses.append(f"lower(impact) IN ({self._sql_list([i.lower() for i in impact])})")
        if consequence:
            clauses.append(
                f"lower(consequence) IN ({self._sql_list([c.lower() for c in consequence])})"
            )
        if lof:
            clauses.append(f"lower(lof) IN ({self._sql_list([l.lower() for l in lof])})")
        if am_class:
            clauses.append(
                "lower(alphamissense_classification) IN "
                f"({self._sql_list([a.lower() for a in am_class])})"
            )
        for param, column, op in [
            ("cadd_phred_min", "cadd_phred", ">="),
            ("sift_score_max", "sift_max", "<="),
            ("polyphen_score_min", "polyphen_max", ">="),
            ("alphamissense_score_min", "alphamissense_score", ">="),
        ]:
            value = self.param(param)
            if value is not None:
                clauses.append(f"{column} {op} {float(value)}")
        return ("WHERE " + " AND ".join(clauses)) if clauses else ""

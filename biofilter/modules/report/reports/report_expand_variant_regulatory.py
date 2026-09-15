"""
Regulatory evidence: which genes a variant regulates, and where.

Migrated from `annotation_variant_regulatory_evidence` (ADR-004 §6,
§2.12), renamed because one row is a link from the input to something
reached — expansion, not annotation.

It answers a question `annotate_variant` cannot. VEP says which gene a
variant sits *in*; an eQTL says which gene it *regulates*, and on chr22
those disagree for 88.5% of the pairs the bundle carries. 31,080 variants
with regulatory evidence there are classed by VEP as intergenic,
upstream or downstream — outside any gene at all.

The relational version read `variant_gene_regulatory_evidence`, which is
empty in every 4.3.0 bundle: the eQTL branch writes `variant_gtex`
instead.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import pyarrow as pa

from biofilter.modules.report.reports._resolution import ALIAS_KEY
from biofilter.modules.report.reports.base_report import ReportBase
from biofilter.modules.report.reports.report_annotate_variant import (
    _CHR_POS,
    _CHR_POS_ALLELE,
    _RSID,
    _parse_chromosome,
)

DEFAULT_FLANKING_BP = 0


class ExpandVariantRegulatoryReport(ReportBase):
    name = "expand_variant_regulatory"
    description = (
        "Which genes a variant regulates: one row per variant x tissue x "
        "regulated gene, with effect size and p-value. Takes gene symbols, "
        "rsIDs or positions."
    )

    requires = ("variant_gtex", "entity_aliases", "entities", "entity_groups")

    optional = (
        "variant_rsid",
        "entity_locations",
        "gene_masters",
        "variant_masters",
    )

    COLUMNS = (
        "input_value",
        "input_kind",
        "status",
        "note",
        "variant_key",
        "rsid",
        "chromosome",
        "position",
        "reference_allele",
        "alternate_allele",
        "position_gene_symbol",
        "position_gene_id",
        "regulated_gene_id",
        "regulated_gene_symbol",
        "bio_context",
        "qtl_type",
        "beta",
        "se",
        "p_value",
        "n",
        "effect_allele",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["APOE", "rs429358", "22:20052518"],
            "tissues": None,
            "qtl_type": None,
            "p_value_max": None,
            "flanking_bp": DEFAULT_FLANKING_BP,
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

    @classmethod
    def _classify(cls, value: Any) -> dict[str, Any]:
        """
        Read one input as a variant, or fall back to a gene symbol.

        Anything that is not an rsID or a position is treated as a gene
        name — that is what lets the three modes share a list rather than
        needing a parameter to say which one you meant.
        """
        text = str(value).strip()
        row = {
            "input_value": text,
            "input_kind": "gene",
            "rsid": None,
            "chromosome": None,
            "position": None,
            "reference_allele": None,
            "alternate_allele": None,
            "term": text.lower(),
        }
        if not text:
            row["input_kind"] = "invalid"
            return row

        if _RSID.match(text):
            row.update(input_kind="rsid", rsid=text.lower())
            return row

        match = _CHR_POS_ALLELE.match(text)
        if match:
            chromosome = _parse_chromosome(match.group(1))
            if chromosome:
                row.update(
                    input_kind="chr_pos_allele",
                    chromosome=chromosome,
                    position=int(match.group(2)),
                    reference_allele=match.group(3).upper(),
                    alternate_allele=match.group(4).upper(),
                )
                return row

        match = _CHR_POS.match(text)
        if match:
            chromosome = _parse_chromosome(match.group(1))
            if chromosome:
                row.update(
                    input_kind="chr_pos",
                    chromosome=chromosome,
                    position=int(match.group(2)),
                )
                return row

        return row

    def _register_input(self, values: list[Any]) -> None:
        parsed = [self._classify(v) for v in values]
        self.con.register(
            "reg_input",
            pa.table(
                {
                    "input_value": pa.array([r["input_value"] for r in parsed], pa.string()),
                    "input_kind": pa.array([r["input_kind"] for r in parsed], pa.string()),
                    "rsid": pa.array([r["rsid"] for r in parsed], pa.string()),
                    "chromosome": pa.array([r["chromosome"] for r in parsed], pa.int32()),
                    "position": pa.array([r["position"] for r in parsed], pa.int64()),
                    "reference_allele": pa.array(
                        [r["reference_allele"] for r in parsed], pa.string()
                    ),
                    "alternate_allele": pa.array(
                        [r["alternate_allele"] for r in parsed], pa.string()
                    ),
                    "term": pa.array([r["term"] for r in parsed], pa.string()),
                }
            ),
        )

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        values = self.resolve_input_list(
            self.param("input_data", required=True), param_name="input_data"
        )
        if not values:
            raise ValueError("input_data must contain at least one value.")

        tissues = self._as_list(self.param("tissues"))
        qtl_type = self.param("qtl_type")
        p_value_max = self.param("p_value_max")
        flanking = int(self.param("flanking_bp", DEFAULT_FLANKING_BP) or 0)
        emit_not_found = self._parse_bool(self.param("emit_not_found_rows"), True)

        self._register_input(values)

        has_rsid = self.bundle.has("variant_rsid")
        has_locations = self.bundle.has("entity_locations")

        evidence_filters = []
        if tissues:
            # Read from the data, never hardcoded. The GTEx DTP ships a
            # catalogue of 50 tissues with a load flag each; which ones a
            # bundle carries is a build decision, and the current one
            # happens to be brain only.
            evidence_filters.append(f"lower(g.bio_context) IN ({self._sql_list([t.lower() for t in tissues])})")
        if qtl_type:
            evidence_filters.append(f"lower(g.qtl_type) = lower('{str(qtl_type)}')")
        if p_value_max is not None:
            evidence_filters.append(f"g.p_value <= {float(p_value_max)}")
        evidence_where = (
            ("WHERE " + " AND ".join(evidence_filters)) if evidence_filters else ""
        )

        rsid_cte = (
            "SELECT chromosome, position, reference_allele, alternate_allele, rsid "
            "FROM variant_rsid"
            if has_rsid
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                   CAST(NULL AS VARCHAR) AS reference_allele,
                   CAST(NULL AS VARCHAR) AS alternate_allele,
                   CAST(NULL AS VARCHAR) AS rsid
            WHERE false
            """
        )
        # The gene whose body contains the variant, which is not the gene
        # the eQTL names — on chr22 they differ for 88.5% of pairs.
        locus_cte = (
            f"""
            SELECT
                l.chromosome,
                l.start_pos - {flanking} AS start_pos,
                l.end_pos + {flanking}   AS end_pos,
                gm.symbol AS gene_symbol,
                gm.entity_id
            FROM entity_locations l
            JOIN gene_masters gm ON gm.entity_id = l.entity_id
            WHERE l.build = 38
            """
            if has_locations and self.bundle.has("gene_masters")
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS start_pos,
                   CAST(NULL AS BIGINT) AS end_pos, CAST(NULL AS VARCHAR) AS gene_symbol,
                   CAST(NULL AS BIGINT) AS entity_id
            WHERE false
            """
        )

        not_found = "" if emit_not_found else "AND false"

        return self.sql(
            f"""
            WITH rsids AS ({rsid_cte}),
            loci AS ({locus_cte}),
            -- A gene input becomes the variants inside that gene's range;
            -- a variant input is itself. Both end up as the same shape.
            gene_targets AS (
                SELECT DISTINCT i.input_value, i.input_kind, l.chromosome,
                       l.start_pos, l.end_pos
                FROM reg_input i
                JOIN entity_aliases a ON {ALIAS_KEY} = i.term
                JOIN loci l ON l.entity_id = a.entity_id
                WHERE i.input_kind = 'gene'
            ),
            from_gene AS (
                SELECT t.input_value, t.input_kind, g.*
                FROM gene_targets t
                JOIN variant_gtex g
                  ON g.chromosome = t.chromosome
                 AND g.position BETWEEN t.start_pos AND t.end_pos
            ),
            from_rsid AS (
                SELECT i.input_value, i.input_kind, g.*
                FROM reg_input i
                JOIN rsids r ON lower(r.rsid) = i.rsid
                JOIN variant_gtex g
                  ON g.chromosome = r.chromosome AND g.position = r.position
                 AND g.reference_allele = r.reference_allele
                 AND g.alternate_allele = r.alternate_allele
                WHERE i.input_kind = 'rsid'
            ),
            from_position AS (
                SELECT i.input_value, i.input_kind, g.*
                FROM reg_input i
                JOIN variant_gtex g
                  ON g.chromosome = i.chromosome AND g.position = i.position
                WHERE i.input_kind = 'chr_pos'
            ),
            from_allele AS (
                SELECT i.input_value, i.input_kind, g.*
                FROM reg_input i
                JOIN variant_gtex g
                  ON g.chromosome = i.chromosome AND g.position = i.position
                 AND g.reference_allele = i.reference_allele
                 AND g.alternate_allele = i.alternate_allele
                WHERE i.input_kind = 'chr_pos_allele'
            ),
            evidence AS (
                SELECT * FROM from_gene
                UNION ALL SELECT * FROM from_rsid
                UNION ALL SELECT * FROM from_position
                UNION ALL SELECT * FROM from_allele
            ),
            filtered AS (
                SELECT g.* FROM evidence g
                {evidence_where}
            ),
            -- The eQTL names its target by Ensembl gene id; the symbol is
            -- what a reader recognises.
            regulated AS (
                SELECT DISTINCT a.alias_value AS gene_id, gm.symbol
                FROM entity_aliases a
                JOIN gene_masters gm ON gm.entity_id = a.entity_id
                WHERE a.alias_value IN (SELECT DISTINCT gene_id FROM filtered)
            )
            SELECT
                f.input_value,
                f.input_kind,
                'ok' AS status,
                CAST(NULL AS VARCHAR) AS note,
                f.chromosome || ':' || CAST(f.position AS VARCHAR) || ':'
                    || f.reference_allele || ':' || f.alternate_allele AS variant_key,
                rs.rsid,
                f.chromosome,
                f.position,
                f.reference_allele,
                f.alternate_allele,
                body.gene_symbol      AS position_gene_symbol,
                body.entity_id        AS position_gene_id,
                f.gene_id             AS regulated_gene_id,
                reg.symbol            AS regulated_gene_symbol,
                f.bio_context,
                f.qtl_type,
                f.beta,
                f.se,
                f.p_value,
                f.n,
                f.effect_allele
            FROM filtered f
            LEFT JOIN rsids rs
                   ON rs.chromosome = f.chromosome AND rs.position = f.position
                  AND rs.reference_allele = f.reference_allele
                  AND rs.alternate_allele = f.alternate_allele
            LEFT JOIN loci body
                   ON body.chromosome = f.chromosome
                  AND f.position BETWEEN body.start_pos AND body.end_pos
            LEFT JOIN regulated reg ON reg.gene_id = f.gene_id

            UNION ALL

            SELECT
                i.input_value, i.input_kind,
                CASE WHEN i.input_kind = 'invalid' THEN 'invalid' ELSE 'not_found' END,
                CASE WHEN i.input_kind = 'invalid' THEN 'Empty input.'
                     WHEN i.input_kind = 'gene'
                     THEN 'No regulatory evidence for variants in this gene.'
                     ELSE 'No regulatory evidence for this variant in this bundle.'
                END,
                NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL
            FROM reg_input i
            WHERE i.input_value NOT IN (SELECT input_value FROM filtered)
              {not_found}

            ORDER BY input_value, p_value, bio_context, regulated_gene_symbol
            """
        )

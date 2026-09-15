"""
Variant annotation: what the bundle knows about a list of variants.

Migrated from `annotation_master_variant` (ADR-004 §6, §2.12). The
relational version could not run against a 4.3.0 bundle at all — it
selected `variant_masters.variant_id`, `position_start` and the
predictor columns, none of which the current schema has (§1.4).

One row per (input, transcript). A variant annotated against eleven
transcripts gives eleven rows, unless `most_severe_only` or
`canonical_only` narrows it.
"""

from __future__ import annotations

import re
from typing import Any, Optional, Sequence

import pyarrow as pa

from biofilter.modules.report.reports import _variants as _v
from biofilter.modules.report.reports.base_report import ReportBase

# Parsing a variant the user typed lives in `_variants`, shared with
# every other report that accepts the same shapes. Re-exported here
# because callers already import these names from this module.
_RSID = _v._RSID
_CHR_POS_ALLELE = _v._CHR_POS_ALLELE
_CHR_POS = _v._CHR_POS
_CHROMOSOME_CODES = _v._CHROMOSOME_CODES
_parse_chromosome = _v.parse_chromosome


class AnnotateVariantReport(ReportBase):
    name = "annotate_variant"
    description = (
        "Variant annotation for rsIDs, chr:pos, or chr:pos:ref:alt: identity, "
        "gnomAD joint frequencies, in-silico predictions, and one row per "
        "transcript the variant was annotated against."
    )

    requires = (
        "variant_masters",
        "variant_molecular_effects",
        "variant_rsid",
    )

    #: Used when present. Their absence leaves whole column groups null,
    #: which is why the provenance records which ones were missing.
    optional = (
        "variant_predictions",
        "variant_alphamissense",
        "variant_consequences",
        "variant_impacts",
    )

    COLUMNS = (
        # what was asked
        "input_value",
        "input_kind",
        "status",
        "note",
        # identity
        "variant_key",
        "rsid",
        "chromosome",
        "position",
        "reference_allele",
        "alternate_allele",
        "quality_filter",
        # gnomAD joint frequencies
        "ac_joint",
        "an_joint",
        "af_joint",
        "nhomalt_joint",
        "grpmax_joint",
        "af_grpmax_joint",
        # in-silico predictions
        "cadd_phred",
        "cadd_raw_score",
        "revel_max",
        "sift_max",
        "polyphen_max",
        "spliceai_ds_max",
        "pangolin_largest_ds",
        "phylop",
        # molecular effect, one row per transcript
        "gene_symbol",
        "gene_id",
        "transcript_id",
        "feature_type",
        "biotype",
        "consequence",
        "consequence_group",
        "consequence_category",
        "severity_rank",
        "is_most_severe_for_variant",
        "impact",
        "impact_rank",
        "canonical",
        "mane_select",
        "hgvsc",
        "hgvsp",
        "amino_acids",
        "codons",
        "variant_class",
        "lof",
        "lof_filter",
        # AlphaMissense, when the bundle carries it
        "alphamissense_score",
        "alphamissense_classification",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["rs1225039379", "22:15238761", "22:15238872:G:A"],
            "most_severe_only": False,
            "canonical_only": False,
            "emit_not_found_rows": True,
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

    @classmethod
    def _parse_item(cls, item: Any) -> dict[str, Any]:
        """
        Read one input into the three shapes the report accepts.

        A `chr:pos` without alleles matches every variant at that
        position, which is usually more than one — that fan-out is the
        answer, not a problem.
        """
        row = {
            "input_value": str(item).strip(),
            "input_kind": "invalid",
            "rsid": None,
            "chromosome": None,
            "position": None,
            "reference_allele": None,
            "alternate_allele": None,
            "note": None,
        }
        if isinstance(item, dict):
            chromosome = _parse_chromosome(item.get("chromosome") or item.get("chr"))
            try:
                position = int(item.get("position") or item.get("pos"))
            except (TypeError, ValueError):
                position = None
            ref = item.get("reference_allele") or item.get("ref")
            alt = item.get("alternate_allele") or item.get("alt")
            if chromosome and position and position > 0:
                row.update(chromosome=chromosome, position=position)
                if ref and alt:
                    row.update(
                        input_kind="chr_pos_allele",
                        reference_allele=str(ref).strip().upper(),
                        alternate_allele=str(alt).strip().upper(),
                        input_value=f"{chromosome}:{position}:{ref}:{alt}".upper(),
                    )
                else:
                    row.update(
                        input_kind="chr_pos",
                        input_value=f"{chromosome}:{position}",
                    )
            else:
                row["note"] = "Expected chromosome and position keys."
            return row

        text = row["input_value"]
        if not text:
            row["note"] = "Empty input."
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

        row["note"] = (
            "Expected an rsID (rs12345), chr:pos (22:15238761), "
            "or chr:pos:ref:alt (22:15238872:G:A)."
        )
        return row

    def _register_parsed_input(self, values: list[Any]) -> None:
        parsed = [self._parse_item(v) for v in values]
        self.con.register(
            "variant_input",
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
                    "parse_note": pa.array([r["note"] for r in parsed], pa.string()),
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

        most_severe_only = self._parse_bool(self.param("most_severe_only"), False)
        canonical_only = self._parse_bool(self.param("canonical_only"), False)
        emit_not_found = self._parse_bool(self.param("emit_not_found_rows"), True)

        self._register_parsed_input(values)

        has_predictions = self.bundle.has("variant_predictions")
        has_alphamissense = self.bundle.has("variant_alphamissense")
        has_consequences = self.bundle.has("variant_consequences")
        has_impacts = self.bundle.has("variant_impacts")

        # Every optional table becomes a CTE either way, so the SELECT
        # below does not branch — a bundle without AlphaMissense returns
        # the columns as null rather than a different shape.
        predictions = (
            "SELECT * FROM variant_predictions"
            if has_predictions
            else """
            SELECT
                CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                CAST(NULL AS VARCHAR) AS reference_allele,
                CAST(NULL AS VARCHAR) AS alternate_allele,
                CAST(NULL AS DOUBLE) AS cadd_raw_score, CAST(NULL AS DOUBLE) AS cadd_phred,
                CAST(NULL AS DOUBLE) AS revel_max, CAST(NULL AS DOUBLE) AS sift_max,
                CAST(NULL AS DOUBLE) AS polyphen_max, CAST(NULL AS DOUBLE) AS spliceai_ds_max,
                CAST(NULL AS DOUBLE) AS pangolin_largest_ds, CAST(NULL AS DOUBLE) AS phylop
            WHERE false
            """
        )
        alphamissense = (
            """
            -- AlphaMissense names transcripts with their version
            -- (ENST00000327374.9); VEP does not (ENST00000327374).
            -- Joining them raw matches nothing, silently.
            SELECT chromosome, position, reference_allele, alternate_allele,
                   split_part(transcript_id, '.', 1) AS transcript_id,
                   score, classification
            FROM variant_alphamissense
            QUALIFY row_number() OVER (
                PARTITION BY chromosome, position, reference_allele,
                             alternate_allele, split_part(transcript_id, '.', 1)
                ORDER BY score DESC
            ) = 1
            """
            if has_alphamissense
            else """
            SELECT
                CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                CAST(NULL AS VARCHAR) AS reference_allele,
                CAST(NULL AS VARCHAR) AS alternate_allele,
                CAST(NULL AS VARCHAR) AS transcript_id,
                CAST(NULL AS DOUBLE) AS score, CAST(NULL AS VARCHAR) AS classification
            WHERE false
            """
        )
        consequences = (
            "SELECT name, severity_rank, consequence_group, consequence_category "
            "FROM variant_consequences"
            if has_consequences
            else """
            SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS INTEGER) AS severity_rank,
                   CAST(NULL AS VARCHAR) AS consequence_group,
                   CAST(NULL AS VARCHAR) AS consequence_category
            WHERE false
            """
        )
        impacts = (
            "SELECT name, severity_rank FROM variant_impacts"
            if has_impacts
            else """
            SELECT CAST(NULL AS VARCHAR) AS name, CAST(NULL AS INTEGER) AS severity_rank
            WHERE false
            """
        )

        filters = []
        if canonical_only:
            filters.append("canonical IS NOT NULL")
        if most_severe_only:
            # Ranked by the consequence vocabulary rather than read from a
            # stored flag: 4.3.0 dropped `is_most_severe_for_variant`, and
            # deriving it keeps the answer consistent with whatever
            # severity ordering the bundle carries.
            filters.append("is_most_severe_for_variant")
        having = ("WHERE " + " AND ".join(filters)) if filters else ""

        not_found = "" if emit_not_found else "AND false"

        return self.sql(
            f"""
            WITH predictions AS ({predictions}),
            alphamissense AS ({alphamissense}),
            consequences AS ({consequences}),
            impacts AS ({impacts}),
            -- rsID lookup goes through variant_rsid: variant_masters
            -- carries an `rsid` column and it is entirely null in 4.3.0
            -- bundles, so joining on it would silently match nothing.
            by_rsid AS (
                SELECT i.input_value, i.input_kind, m.*
                FROM variant_input i
                JOIN variant_rsid r ON lower(r.rsid) = i.rsid
                JOIN variant_masters m
                  ON m.chromosome = r.chromosome AND m.position = r.position
                 AND m.reference_allele = r.reference_allele
                 AND m.alternate_allele = r.alternate_allele
                WHERE i.input_kind = 'rsid'
            ),
            by_position AS (
                SELECT i.input_value, i.input_kind, m.*
                FROM variant_input i
                JOIN variant_masters m
                  ON m.chromosome = i.chromosome AND m.position = i.position
                WHERE i.input_kind = 'chr_pos'
            ),
            by_allele AS (
                SELECT i.input_value, i.input_kind, m.*
                FROM variant_input i
                JOIN variant_masters m
                  ON m.chromosome = i.chromosome AND m.position = i.position
                 AND m.reference_allele = i.reference_allele
                 AND m.alternate_allele = i.alternate_allele
                WHERE i.input_kind = 'chr_pos_allele'
            ),
            matched AS (
                SELECT * FROM by_rsid
                UNION ALL SELECT * FROM by_position
                UNION ALL SELECT * FROM by_allele
            ),
            -- One row per transcript the variant was annotated against.
            annotated AS (
                SELECT
                    v.input_value,
                    v.input_kind,
                    v.variant_key,
                    rs.rsid,
                    v.chromosome,
                    v.position,
                    v.reference_allele,
                    v.alternate_allele,
                    v.quality_filter,
                    v.ac_joint, v.an_joint, v.af_joint, v.nhomalt_joint,
                    v.grpmax_joint, v.af_grpmax_joint,
                    p.cadd_phred, p.cadd_raw_score, p.revel_max, p.sift_max,
                    p.polyphen_max, p.spliceai_ds_max, p.pangolin_largest_ds, p.phylop,
                    e.symbol        AS gene_symbol,
                    e.gene          AS gene_id,
                    e.feature       AS transcript_id,
                    e.feature_type,
                    e.biotype,
                    e.consequence,
                    c.consequence_group,
                    c.consequence_category,
                    c.severity_rank,
                    e.impact,
                    im.severity_rank AS impact_rank,
                    e.canonical,
                    e.mane_select,
                    e.hgvsc, e.hgvsp, e.amino_acids, e.codons, e.variant_class,
                    e.lof, e.lof_filter,
                    am.score          AS alphamissense_score,
                    am.classification AS alphamissense_classification
                FROM matched v
                LEFT JOIN variant_molecular_effects e
                       ON e.chromosome = v.chromosome AND e.position = v.position
                      AND e.reference_allele = v.reference_allele
                      AND e.alternate_allele = v.alternate_allele
                LEFT JOIN variant_rsid rs
                       ON rs.chromosome = v.chromosome AND rs.position = v.position
                      AND rs.reference_allele = v.reference_allele
                      AND rs.alternate_allele = v.alternate_allele
                LEFT JOIN predictions p
                       ON p.chromosome = v.chromosome AND p.position = v.position
                      AND p.reference_allele = v.reference_allele
                      AND p.alternate_allele = v.alternate_allele
                LEFT JOIN consequences c ON c.name = e.consequence
                LEFT JOIN impacts im ON im.name = e.impact
                LEFT JOIN alphamissense am
                       ON am.chromosome = v.chromosome AND am.position = v.position
                      AND am.reference_allele = v.reference_allele
                      AND am.alternate_allele = v.alternate_allele
                      AND am.transcript_id = e.feature
            ),
            ranked AS (
                SELECT
                    *,
                    coalesce(
                        severity_rank = min(severity_rank) OVER (
                            PARTITION BY input_value, variant_key
                        ),
                        false
                    ) AS is_most_severe_for_variant
                FROM annotated
            )
            SELECT
                input_value, input_kind,
                'ok' AS status, CAST(NULL AS VARCHAR) AS note,
                variant_key, rsid, chromosome, position,
                reference_allele, alternate_allele, quality_filter,
                ac_joint, an_joint, af_joint, nhomalt_joint,
                grpmax_joint, af_grpmax_joint,
                cadd_phred, cadd_raw_score, revel_max, sift_max, polyphen_max,
                spliceai_ds_max, pangolin_largest_ds, phylop,
                gene_symbol, gene_id, transcript_id, feature_type, biotype,
                consequence, consequence_group, consequence_category, severity_rank,
                is_most_severe_for_variant, impact, impact_rank,
                canonical, mane_select, hgvsc, hgvsp, amino_acids, codons,
                variant_class, lof, lof_filter,
                alphamissense_score, alphamissense_classification
            FROM ranked
            {having}

            UNION ALL

            SELECT
                i.input_value, i.input_kind,
                CASE WHEN i.input_kind = 'invalid' THEN 'invalid' ELSE 'not_found' END,
                coalesce(i.parse_note,
                         'No variant in this bundle for the given ' || i.input_kind || '.'),
                NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL,
                NULL, NULL
            FROM variant_input i
            WHERE i.input_value NOT IN (SELECT input_value FROM matched)
              {not_found}

            ORDER BY input_value, severity_rank, transcript_id
            """
        )

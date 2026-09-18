"""
A cohort's own variants, matched to the bundle and aggregated into bins.

Replaces `variant_list_intersect` and `variant_binning`, which were the
same pipeline stopped at different points:

1. **Read** the cohort's file — a VCF with genotypes, a PLINK `.bim`, or
   a plain list of rsIDs and positions.
2. **Match** each variant against the bundle and place it on the genes
   whose build-38 range contains it.
3. **Aggregate** — keep the rare variants, assign each to a bin (gene,
   gene group, locus type or pathway), and count what every sample
   carries in every bin.

`variant_list_intersect` stopped after step 2 and emitted the matched
list with a PLINK `--extract` id. That is `output_grain="variants"`.
`variant_binning` ran all three. That is `output_grain="bins"`, and it
needs genotypes, so it needs a VCF.

**The guard that matters most.** A cohort file spans the genome; a bundle
need not. Binning a whole-genome VCF against a bundle carrying one
chromosome does not fail — it returns bins covering that chromosome and
says nothing about the rest. Every run reports which chromosomes the
cohort brought, which the bundle can place, and how many variants fell
outside; `require_full_coverage=true` turns that into a refusal.

Only 39,306 of the bundle's 72,660 genes carry build-38 coordinates, and
step 2 is positional, so a little over half the gene catalogue is
reachable at all. `group_by="pathway"` narrows it further: 13,632 genes
sit in a pathway. The coverage block reports both.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional, Sequence

import pyarrow as pa

from biofilter.modules.report.reports import _cohort
from biofilter.modules.report.reports.base_report import ReportBase
from biofilter.modules.report.result import ReportResult

GRAINS = ("variants", "bins")
GROUP_BYS = ("gene", "gene_group", "locus_type", "pathway")

DEFAULT_BUILD = 38
DEFAULT_MAF_CUTOFF = 0.01

#: Chromosome integers, as PLINK spells them back.
_PLINK_CHROMOSOMES = {23: "X", 24: "Y", 25: "MT"}


class AggregateCohortVariantsReport(ReportBase):
    name = "aggregate_cohort_variants"
    description = (
        "A cohort's variants, matched against the bundle and optionally "
        "aggregated into biological bins: which of your variants Biofilter "
        "knows, where they sit, and what each sample carries per bin."
    )

    requires = (
        "entities",
        "entity_groups",
        "entity_locations",
        "gene_masters",
        "variant_masters",
    )

    optional = (
        "variant_rsid",
        "gene_groups",
        "gene_group_memberships",
        "gene_locus_types",
        "entity_relationships",
    )

    VARIANT_COLUMNS = (
        "cohort_variant_id",
        "chromosome",
        "position",
        "reference_allele",
        "alternate_allele",
        "variant_key",
        "rsid",
        "plink_id",
        "match_status",
        "note",
        "gene_entity_ids",
        "gene_symbols",
        "maf_overall",
        "maf_case",
        "maf_control",
        "ac_overall",
        "an_overall",
        "is_rare",
    )

    BIN_COLUMNS = (
        "bin_name",
        "bin_type",
        "sample",
        "sample_class",
        "variant_count",
        "alt_count",
        "bin_variant_count",
        "bin_gene_count",
        "group_by",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.VARIANT_COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "cohort_file": "./cohort.vcf.gz",
            "output_grain": "bins",
            "group_by": "gene",
            "phenotype_file": "./phenotype.csv",
            "phenotype_control_value": 0,
            "maf_cutoff": 0.01,
        }

    # ------------------------------------------------------------------
    # Parameters
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

    def _int_param(self, name: str, default: Optional[int]) -> Optional[int]:
        value = self.param(name, default)
        if value is None or (isinstance(value, str) and not value.strip()):
            return default
        return int(value)

    def _choice(self, name: str, allowed: tuple[str, ...], default: str) -> str:
        value = str(self.param(name, default) or default).strip().lower()
        if value not in allowed:
            raise ValueError(f"{name} must be one of {allowed}. Got: {value!r}")
        return value

    @staticmethod
    def _as_list(value: Any) -> list[str]:
        if value is None:
            return []
        values = value if isinstance(value, (list, tuple, set)) else [value]
        return [str(v).strip() for v in values if str(v).strip()]

    # ------------------------------------------------------------------
    def run(self) -> ReportResult:
        cohort_file = self.param("cohort_file", required=True)
        grain = self._choice("output_grain", GRAINS, "variants")
        group_by = self._choice("group_by", GROUP_BYS, "gene")
        build = self._int_param("build", DEFAULT_BUILD)
        window = self._int_param("window_bp", 0)
        max_variants = self._int_param("max_variants", None)
        maf_cutoff = float(self.param("maf_cutoff", DEFAULT_MAF_CUTOFF))
        rare_case_control = self._parse_bool(self.param("rare_case_control"), True)
        overall_major_allele = self._parse_bool(
            self.param("overall_major_allele"), True
        )
        require_full_coverage = self._parse_bool(
            self.param("require_full_coverage"), False
        )

        if window < 0:
            raise ValueError(f"window_bp must be 0 or positive. Got: {window}.")
        if not 0 < maf_cutoff <= 0.5:
            raise ValueError(
                f"maf_cutoff is a minor allele frequency, so it lies in (0, 0.5]. "
                f"Got: {maf_cutoff}."
            )

        reader = _cohort.CohortReader(cohort_file, self.param("cohort_format"))
        if grain == "bins" and not reader.has_genotypes:
            raise ValueError(
                f"output_grain='bins' needs genotypes, and a "
                f"{reader.source_format!r} file carries none. Binning counts "
                f"what each sample carries; pass a VCF, or use "
                f"output_grain='variants' to match the list against the bundle."
            )

        classes = self._sample_classes(reader.samples)
        case_positions = [
            i for i, s in enumerate(reader.samples) if classes.get(s) == "case"
        ]
        control_positions = [
            i for i, s in enumerate(reader.samples) if classes.get(s) == "control"
        ]
        cohort = reader.read(
            case_positions=case_positions,
            control_positions=control_positions,
            max_variants=max_variants,
        )

        self.con.register("cohort_variants", cohort.variants)
        if cohort.carriers is not None:
            self.con.register("cohort_carriers", cohort.carriers)
        self.con.register(
            "cohort_samples",
            pa.table(
                {
                    "sample_index": pa.array(
                        list(range(len(reader.samples))), pa.int64()
                    ),
                    "sample": pa.array(reader.samples, pa.string()),
                    "sample_class": pa.array(
                        [classes.get(s, "unknown") for s in reader.samples],
                        pa.string(),
                    ),
                }
            ),
        )

        self._note_cohort(cohort, reader, classes, grain, group_by, maf_cutoff)
        self._note_chromosome_coverage(cohort, require_full_coverage)

        if grain == "variants":
            table = self.sql(self._variants_query(build, window, maf_cutoff,
                                                  rare_case_control,
                                                  overall_major_allele))
            self._write_plink_extract(table)
        else:
            self._note_bin_coverage(group_by, build)
            self._note_rare_variants(
                maf_cutoff, rare_case_control, overall_major_allele,
                len(reader.samples),
            )
            table = self.sql(
                self._bins_query(
                    build, window, maf_cutoff, rare_case_control,
                    overall_major_allele, group_by,
                )
            )
            self._emit_variant_to_bin(build, window, maf_cutoff,
                                      rare_case_control, overall_major_allele,
                                      group_by)

        return ReportResult(table=table, provenance={}, artifacts=list(self.artifacts))

    # ------------------------------------------------------------------
    def _sample_classes(self, samples: list[str]) -> dict[str, str]:
        """
        Match the phenotype file to the VCF's sample names.

        A name in one and not the other is the commonest way a case /
        control split comes out silently wrong, so it is counted.
        """
        path = self.param("phenotype_file")
        if not path:
            return {}

        classes = _cohort.read_phenotype(
            path,
            sample_column=str(self.param("phenotype_sample_column", "SampleID")),
            value_column=str(self.param("phenotype_value_column", "Phenotype")),
            control_values=self._as_list(self.param("phenotype_control_value", ["0"]))
            or ["0"],
            case_values=self._as_list(self.param("phenotype_case_values")),
        )
        in_file = set(samples)
        in_phenotype = set(classes)
        unmatched = len(in_file - in_phenotype)
        if unmatched:
            self.warn(
                f"{unmatched} of {len(in_file)} cohort samples have no "
                f"phenotype and take no part in the case/control counts.",
                samples_without_phenotype=unmatched,
            )

        self.note_provenance(
            "phenotype",
            {
                "file": str(path),
                "samples_in_cohort": len(in_file),
                "samples_in_phenotype": len(in_phenotype),
                "matched": len(in_file & in_phenotype),
                "cohort_only": sorted(in_file - in_phenotype)[:20],
                "phenotype_only": sorted(in_phenotype - in_file)[:20],
                "cases": sum(1 for s in samples if classes.get(s) == "case"),
                "controls": sum(1 for s in samples if classes.get(s) == "control"),
                "means": (
                    "Samples present in one file and not the other take no "
                    "part in the case/control counts."
                ),
            },
        )
        return classes

    def _note_cohort(self, cohort, reader, classes, grain, group_by, maf_cutoff) -> None:
        self.note_provenance(
            "cohort",
            {
                "file": str(reader.path),
                "format": cohort.source_format,
                "variants_read": cohort.variants.num_rows,
                "samples": len(reader.samples),
                "carries_genotypes": reader.has_genotypes,
                "rows_skipped": {k: v for k, v in cohort.skipped.items() if v},
                "output_grain": grain,
                "group_by": group_by if grain == "bins" else None,
                "maf_cutoff": maf_cutoff if grain == "bins" else None,
            },
        )

    def _note_chromosome_coverage(self, cohort, require_full: bool) -> None:
        """
        Whether the bundle can place what the cohort brought.

        This is the failure this report exists to prevent. A whole-genome
        VCF against a single-chromosome bundle produces a result that
        looks complete and covers a twenty-third of the data.
        """
        in_cohort = cohort.chromosomes
        in_bundle = self.bundle.chromosomes("variant_masters") or []
        missing = sorted(set(in_cohort) - set(in_bundle))

        counts = {
            int(row[0]): int(row[1])
            for row in self.con.execute(
                "SELECT chromosome, count(*) FROM cohort_variants "
                "WHERE chromosome IS NOT NULL GROUP BY 1"
            ).fetchall()
        }
        unplaceable = sum(counts.get(c, 0) for c in missing)
        total = sum(counts.values()) or 1

        coverage = {
            "chromosomes_in_cohort": in_cohort,
            "chromosomes_in_bundle": in_bundle,
            "chromosomes_missing_from_bundle": missing,
            "variants_the_bundle_cannot_place": unplaceable,
            "share_unplaceable": round(unplaceable / total, 4),
            "means": (
                f"{unplaceable:,} of {total:,} cohort variants sit on "
                f"chromosomes this bundle does not carry. They are reported "
                f"as 'chromosome_not_in_bundle' and take no part in any bin."
                if missing
                else "Every chromosome the cohort brought is in this bundle."
            ),
        }
        self.note_provenance("chromosome_coverage", coverage)

        if missing and not require_full:
            self.warn(
                f"{unplaceable:,} of {total:,} cohort variants "
                f"({unplaceable / total:.1%}) sit on chromosomes this bundle "
                f"does not carry and take no part in the result.",
                chromosomes=missing,
                variants=unplaceable,
            )

        if missing and require_full:
            raise ValueError(
                f"The cohort has variants on chromosome(s) {missing}, which "
                f"this bundle does not carry — {unplaceable:,} of "
                f"{total:,} variants ({unplaceable / total:.1%}) cannot be "
                f"placed. Build a bundle covering them, or pass "
                f"require_full_coverage=false to proceed on what is here."
            )

    def _note_bin_coverage(self, group_by: str, build: int) -> None:
        """How much of the gene catalogue this bin type can even reach."""
        reachable = {
            "gene": "SELECT count(DISTINCT entity_id) FROM entity_locations "
                    f"WHERE build = {build}",
            "gene_group": "SELECT count(DISTINCT m.gene_id) "
                          "FROM gene_group_memberships m "
                          "JOIN gene_masters gm ON gm.id = m.gene_id "
                          "JOIN entity_locations l ON l.entity_id = gm.entity_id "
                          f"AND l.build = {build}",
            "locus_type": "SELECT count(DISTINCT gm.entity_id) FROM gene_masters gm "
                          "JOIN entity_locations l ON l.entity_id = gm.entity_id "
                          f"AND l.build = {build} WHERE gm.locus_type_id IS NOT NULL",
            "pathway": "SELECT count(DISTINCT e2.id) FROM entity_relationships r "
                       "JOIN entities e1 ON e1.id = r.entity_1_id "
                       "JOIN entity_groups g1 ON g1.id = e1.group_id "
                       "JOIN entities e2 ON e2.id = r.entity_2_id "
                       "JOIN entity_groups g2 ON g2.id = e2.group_id "
                       "JOIN entity_locations l ON l.entity_id = e2.id "
                       f"AND l.build = {build} "
                       "WHERE g1.name = 'Pathways' AND g2.name = 'Genes'",
        }[group_by]

        genes = int(self.con.execute("SELECT count(*) FROM gene_masters").fetchone()[0])
        placed = int(self.con.execute(reachable).fetchone()[0])
        self.note_provenance(
            "bin_coverage",
            {
                "group_by": group_by,
                "genes_in_bundle": genes,
                "genes_this_bin_type_can_reach": placed,
                "share": round(placed / genes, 4) if genes else None,
                "means": (
                    "A variant can only be binned into a gene the bundle can "
                    "place and that carries this kind of grouping. A variant "
                    "in any other gene is reported as unbinned, not dropped."
                ),
            },
        )

    def _note_rare_variants(
        self, maf_cutoff: float, rare_case_control: bool,
        overall_major_allele: bool, sample_count: int,
    ) -> None:
        """
        Whether a rare variant could be observed at all in this cohort.

        With N samples the smallest non-zero minor allele frequency is
        1/(2N): one copy in one person. Ask for variants below that and
        the only ones passing the filter are the ones nobody carries, so
        every bin comes back empty — correctly, and for a reason no
        column reveals. A 20-sample cohort cannot observe anything rarer
        than 0.025, which is well above the usual 0.01 cutoff.
        """
        row = self.sql(f"""
            WITH {self._frequencies_cte(maf_cutoff, rare_case_control,
                                        overall_major_allele)}
            SELECT
                count(*) AS variants,
                count(*) FILTER (WHERE is_rare) AS rare,
                count(*) FILTER (WHERE is_rare AND ac_overall > 0)
                    AS rare_with_carriers
            FROM classified
        """).to_pylist()
        stats = row[0] if row else {}
        floor = 1.0 / (2 * sample_count) if sample_count else None
        rare = stats.get("rare") or 0
        carried = stats.get("rare_with_carriers") or 0

        if floor is not None and maf_cutoff < floor:
            means = (
                f"This cohort has {sample_count} samples, so the smallest "
                f"frequency it can observe is {floor:.4f} — one allele copy in "
                f"one person. A maf_cutoff of {maf_cutoff} is below that, so "
                f"every variant passing the filter is one nobody carries and "
                f"every bin will be empty. Raise maf_cutoff above {floor:.4f}, "
                f"or use a larger cohort."
            )
        elif rare and not carried:
            means = (
                f"All {rare:,} rare variants have zero observed copies, so "
                f"there is nothing to aggregate. Nothing is wrong with the "
                f"bundle; the cohort simply carries none of them."
            )
        else:
            means = (
                f"{carried:,} of {rare:,} rare variants are carried by at "
                f"least one sample, and only those can reach a bin."
            )

        if floor is not None and maf_cutoff < floor:
            self.warn(
                f"maf_cutoff {maf_cutoff} is below {floor:.4f}, the smallest "
                f"frequency {sample_count} samples can observe. Every bin "
                f"will be empty.",
                samples=sample_count,
                smallest_observable_maf=round(floor, 6),
            )
        elif rare and not carried:
            self.warn(
                f"None of the {rare:,} rare variants is carried by any "
                f"sample, so there is nothing to aggregate.",
                rare=rare,
            )

        self.note_provenance(
            "rare_variants",
            {
                "maf_cutoff": maf_cutoff,
                "samples": sample_count,
                "smallest_observable_maf": round(floor, 6) if floor else None,
                "variants": stats.get("variants"),
                "rare": rare,
                "rare_with_carriers": carried,
                "means": means,
            },
        )

    # ------------------------------------------------------------------
    def _maf_expression(
        self, rare_case_control: bool, overall_major_allele: bool
    ) -> str:
        """
        Which frequency the rare-variant filter is applied to.

        BioBin's rule: when both arms are present, a variant is rare only
        if it is rare in *both*, so the filter uses the larger of the two
        minor allele frequencies. Keeping the relational version's
        behaviour exactly — changing it silently would reclassify
        somebody's variants.
        """
        overall = "least(af_overall, 1.0 - af_overall)"
        case = "least(af_case, 1.0 - af_case)"
        control = "least(af_control, 1.0 - af_control)"

        if rare_case_control:
            return (
                f"CASE WHEN af_case IS NOT NULL AND af_control IS NOT NULL "
                f"THEN greatest({case}, {control}) ELSE {overall} END"
            )
        if not overall_major_allele:
            return f"CASE WHEN af_control IS NOT NULL THEN {control} ELSE {overall} END"
        return overall

    def _frequencies_cte(
        self, maf_cutoff: float, rare_case_control: bool, overall_major_allele: bool
    ) -> str:
        return f"""
            frequencies AS (
                SELECT
                    c.*,
                    CASE WHEN an_overall > 0
                         THEN ac_overall::DOUBLE / an_overall END AS af_overall,
                    CASE WHEN an_case > 0
                         THEN ac_case::DOUBLE / an_case END AS af_case,
                    CASE WHEN an_control > 0
                         THEN ac_control::DOUBLE / an_control END AS af_control
                FROM cohort_variants c
            ),
            scored AS (
                SELECT
                    f.*,
                    least(af_overall, 1.0 - af_overall) AS maf_overall,
                    least(af_case, 1.0 - af_case) AS maf_case,
                    least(af_control, 1.0 - af_control) AS maf_control,
                    {self._maf_expression(rare_case_control, overall_major_allele)}
                        AS maf_filter
                FROM frequencies f
            ),
            classified AS (
                SELECT *, (maf_filter IS NOT NULL AND maf_filter <= {maf_cutoff})
                       AS is_rare
                FROM scored
            )
        """

    def _cohort_chromosome_filter(self, column: str = "chromosome") -> str:
        """
        Restrict a bundle table to the chromosomes the cohort touches.

        The variant tables are partitioned by chromosome, so naming them
        lets the scan skip whole files. Without it a three-rsID cohort
        reads every chromosome in the bundle to annotate three rows, and
        that cost grows with the bundle rather than with the question.
        """
        rows = self.con.execute(
            "SELECT DISTINCT chromosome FROM cohort_variants "
            "WHERE chromosome IS NOT NULL ORDER BY 1"
        ).fetchall()
        present = [int(r[0]) for r in rows]
        if not present:
            return ""
        return f"WHERE {column} IN ({', '.join(str(c) for c in present)})"

    def _placement_cte(self, build: int, window: int, needs_rsid: bool) -> str:
        """Cohort variant to the genes whose range holds it, and to the bundle."""
        has_rsid = self.bundle.has("variant_rsid")
        rsids = (
            "SELECT chromosome, position, reference_allele, alternate_allele, rsid "
            "FROM variant_rsid"
            if has_rsid
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                   CAST(NULL AS VARCHAR) AS reference_allele,
                   CAST(NULL AS VARCHAR) AS alternate_allele, CAST(NULL AS VARCHAR) AS rsid
            WHERE false
            """
        )
        located = (
            """
                SELECT
                    c.row_id,
                    coalesce(c.chromosome, u.chromosome) AS chromosome,
                    coalesce(c.position, u.position) AS position,
                    coalesce(c.reference_allele, u.reference_allele)
                        AS reference_allele,
                    coalesce(c.alternate_allele, u.alternate_allele)
                        AS alternate_allele,
                    c.variant_id, c.is_rare,
                    c.maf_overall, c.maf_case, c.maf_control,
                    c.ac_overall, c.an_overall
                FROM classified c
                LEFT JOIN looked_up u ON u.row_id = c.row_id
            """
            if needs_rsid
            else """
                SELECT
                    c.row_id, c.chromosome, c.position,
                    c.reference_allele, c.alternate_allele,
                    c.variant_id, c.is_rare,
                    c.maf_overall, c.maf_case, c.maf_control,
                    c.ac_overall, c.an_overall
                FROM classified c
            """
        )
        # The lookup needs every chromosome — finding one is the point.
        # Everything after it only ever touches the cohort's own, and
        # says so, because the variant tables are partitioned by
        # chromosome and a named set lets the scan skip whole files.
        #
        # Which chromosomes those are is known up front for a cohort that
        # gave coordinates, and only after the lookup for one that gave
        # rsIDs — so the filter is a literal list when we have one and a
        # semi-join against the resolved rows when we do not.
        narrow = self._cohort_chromosome_filter()
        if not narrow:
            narrow = (
                "WHERE chromosome IN (SELECT DISTINCT chromosome FROM looked_up)"
            )
        return f"""
            rsids_all AS ({rsids}),
            -- An rsID-only entry has no coordinates of its own; the
            -- bundle supplies them.
            --
            -- Only when one is actually present. `c.chromosome IS NULL`
            -- beside an equality stops DuckDB using a hash join, so this
            -- becomes a nested loop over `variant_rsid` — 264 million
            -- rows on a whole-genome bundle, spilling tens of gigabytes
            -- to do nothing at all for a cohort that gave coordinates,
            -- which every VCF and every .bim does.
            -- Which inputs need a lookup is a filter on the cohort, and
            -- it belongs here rather than inside the join condition.
            -- `ON c.chromosome IS NULL AND <equality>` reads the same and
            -- is not: mixing a non-equality into the condition costs the
            -- hash join, and DuckDB falls back to a nested loop over all
            -- 264 million rsIDs. Separated, the same lookup is a hash
            -- join that finishes in about a second.
            --
            -- `lower()` goes on the cohort's side only. The bundle's
            -- rsids are already lowercase, and wrapping a column in a
            -- function is what throws away its row-group statistics.
            needs_lookup AS (
                SELECT row_id, lower(variant_id) AS rsid
                FROM classified WHERE chromosome IS NULL AND variant_id IS NOT NULL
            ),
            looked_up AS (
                SELECT
                    n.row_id, r.chromosome, r.position,
                    r.reference_allele, r.alternate_allele
                FROM needs_lookup n
                JOIN rsids_all r ON r.rsid = n.rsid
                QUALIFY row_number() OVER (
                    PARTITION BY n.row_id ORDER BY r.chromosome, r.position
                ) = 1
            ),
            masters AS (SELECT * FROM variant_masters {narrow}),
            rsids AS (SELECT * FROM rsids_all {narrow}),
            located AS ({located}),
            matched AS (
                SELECT
                    l.*,
                    v.variant_key,
                    rs.rsid AS bundle_rsid
                FROM located l
                LEFT JOIN masters v
                       ON v.chromosome = l.chromosome AND v.position = l.position
                      AND (l.reference_allele IS NULL
                           OR v.reference_allele = l.reference_allele)
                      AND (l.alternate_allele IS NULL
                           OR v.alternate_allele = l.alternate_allele)
                LEFT JOIN rsids rs
                       ON rs.chromosome = l.chromosome AND rs.position = l.position
                      AND rs.reference_allele = l.reference_allele
                      AND rs.alternate_allele = l.alternate_allele
                QUALIFY row_number() OVER (
                    PARTITION BY l.row_id ORDER BY v.variant_key
                ) = 1
            ),
            placed AS (
                SELECT
                    m.row_id, l.entity_id AS gene_entity_id, gm.symbol AS gene_symbol
                FROM matched m
                JOIN entity_locations l
                  ON l.build = {build} AND l.chromosome = m.chromosome
                 AND m.position BETWEEN l.start_pos - {window} AND l.end_pos + {window}
                JOIN gene_masters gm ON gm.entity_id = l.entity_id
            )
        """

    def _needs_rsid_lookup(self) -> bool:
        """Whether any input arrived without coordinates of its own."""
        return bool(
            self.con.execute(
                "SELECT count(*) FROM cohort_variants WHERE chromosome IS NULL"
            ).fetchone()[0]
        )

    # ------------------------------------------------------------------
    def _variants_query(
        self, build: int, window: int, maf_cutoff: float,
        rare_case_control: bool, overall_major_allele: bool,
    ) -> str:
        bundle_chromosomes = self.bundle.chromosomes("variant_masters") or []
        in_bundle = (
            ", ".join(str(c) for c in bundle_chromosomes) if bundle_chromosomes else "NULL"
        )
        return f"""
            WITH {self._frequencies_cte(maf_cutoff, rare_case_control, overall_major_allele)},
            {self._placement_cte(build, window, self._needs_rsid_lookup())},
            genes AS (
                SELECT
                    row_id,
                    list_sort(list_distinct(list(gene_entity_id))) AS gene_entity_ids,
                    list_sort(list_distinct(list(gene_symbol))) AS gene_symbols
                FROM placed GROUP BY 1
            )
            SELECT
                coalesce(m.variant_id,
                         m.chromosome || ':' || m.position || ':' ||
                         coalesce(m.reference_allele, '?') || ':' ||
                         coalesce(m.alternate_allele, '?')) AS cohort_variant_id,
                m.chromosome, m.position, m.reference_allele, m.alternate_allele,
                m.variant_key,
                coalesce(m.bundle_rsid, m.variant_id) AS rsid,
                -- What PLINK will accept in an --extract file: the id
                -- from the cohort's own file when it has one, since that
                -- is what PLINK matches on, and chr:pos otherwise.
                coalesce(
                    m.variant_id,
                    CASE WHEN m.chromosome IS NULL THEN NULL
                         ELSE (CASE m.chromosome
                                 WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'MT'
                                 ELSE m.chromosome::VARCHAR END)
                              || ':' || m.position END
                ) AS plink_id,
                CASE
                    WHEN m.chromosome IS NULL THEN 'unresolved'
                    WHEN m.chromosome NOT IN ({in_bundle})
                        THEN 'chromosome_not_in_bundle'
                    WHEN m.variant_key IS NULL THEN 'not_in_bundle'
                    WHEN g.gene_entity_ids IS NULL THEN 'in_bundle_no_gene'
                    ELSE 'matched'
                END AS match_status,
                CASE
                    WHEN m.chromosome IS NULL
                        THEN 'Could not be resolved to a position; an rsID the '
                             || 'bundle does not carry has no coordinates.'
                    WHEN m.chromosome NOT IN ({in_bundle})
                        THEN 'This bundle carries no variants for that chromosome, '
                             || 'so nothing can be said about it.'
                    WHEN m.variant_key IS NULL
                        THEN 'The bundle covers this chromosome but does not have '
                             || 'this variant.'
                    WHEN g.gene_entity_ids IS NULL
                        THEN 'Known to the bundle, but no gene with build-'
                             || '{build} coordinates contains it.'
                    ELSE NULL
                END AS note,
                g.gene_entity_ids, g.gene_symbols,
                m.maf_overall, m.maf_case, m.maf_control,
                m.ac_overall, m.an_overall, m.is_rare
            FROM matched m
            LEFT JOIN genes g ON g.row_id = m.row_id
            ORDER BY m.chromosome NULLS LAST, m.position, m.reference_allele
        """

    def _bins_query(
        self, build: int, window: int, maf_cutoff: float,
        rare_case_control: bool, overall_major_allele: bool, group_by: str,
    ) -> str:
        return f"""
            WITH {self._frequencies_cte(maf_cutoff, rare_case_control, overall_major_allele)},
            {self._placement_cte(build, window, self._needs_rsid_lookup())},
            {self._bins_cte(group_by, build)},
            rare_bins AS (
                SELECT DISTINCT b.row_id, b.bin_name, b.bin_type, b.gene_entity_id
                FROM binned b
                JOIN matched m ON m.row_id = b.row_id
                WHERE m.is_rare
            ),
            bin_totals AS (
                SELECT
                    bin_name, bin_type,
                    count(DISTINCT row_id) AS bin_variant_count,
                    count(DISTINCT gene_entity_id) AS bin_gene_count
                FROM rare_bins GROUP BY 1, 2
            )
            SELECT
                rb.bin_name, rb.bin_type,
                s.sample, s.sample_class,
                count(DISTINCT rb.row_id) AS variant_count,
                sum(cc.alt_count)::BIGINT AS alt_count,
                any_value(t.bin_variant_count) AS bin_variant_count,
                any_value(t.bin_gene_count) AS bin_gene_count,
                '{group_by}' AS group_by
            FROM rare_bins rb
            JOIN cohort_carriers cc ON cc.row_id = rb.row_id
            JOIN cohort_samples s ON s.sample_index = cc.sample_index
            JOIN bin_totals t ON t.bin_name = rb.bin_name AND t.bin_type = rb.bin_type
            GROUP BY 1, 2, 3, 4
            ORDER BY alt_count DESC, rb.bin_name, s.sample
        """

    def _bins_cte(self, group_by: str, build: int) -> str:
        """One row per (cohort variant, bin) for the chosen grouping."""
        if group_by == "gene":
            body = """
                SELECT p.row_id, p.gene_entity_id,
                       p.gene_symbol AS bin_name, 'gene' AS bin_type
                FROM placed p WHERE p.gene_symbol IS NOT NULL
            """
        elif group_by == "gene_group":
            body = """
                SELECT p.row_id, p.gene_entity_id, gg.name AS bin_name,
                       'gene_group' AS bin_type
                FROM placed p
                JOIN gene_masters gm ON gm.entity_id = p.gene_entity_id
                JOIN gene_group_memberships m ON m.gene_id = gm.id
                JOIN gene_groups gg ON gg.id = m.group_id
            """
        elif group_by == "locus_type":
            body = """
                SELECT p.row_id, p.gene_entity_id, lt.name AS bin_name,
                       'locus_type' AS bin_type
                FROM placed p
                JOIN gene_masters gm ON gm.entity_id = p.gene_entity_id
                JOIN gene_locus_types lt ON lt.id = gm.locus_type_id
            """
        else:
            body = """
                SELECT p.row_id, p.gene_entity_id, a.alias_value AS bin_name,
                       'pathway' AS bin_type
                FROM placed p
                JOIN entity_relationships r
                  ON r.entity_2_id = p.gene_entity_id
                JOIN entities e ON e.id = r.entity_1_id
                JOIN entity_groups g ON g.id = e.group_id AND g.name = 'Pathways'
                JOIN entity_aliases a ON a.entity_id = e.id AND a.is_primary
            """
        return f"binned AS ({body})"

    # ------------------------------------------------------------------
    def _write_plink_extract(self, table: pa.Table) -> None:
        """
        The `--extract` list, when asked for.

        PLINK matches on the id in the `.bim`, not on coordinates, so a
        file of `chr:pos` strings silently extracts nothing from a
        dataset whose ids are rsIDs. The id the cohort's own file used is
        preferred for exactly that reason.
        """
        path = self.param("plink_extract_path")
        if not path:
            return
        target = Path(str(path)).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)

        # Everything the bundle actually has, whether or not a gene
        # contains it. Requiring a gene would quietly drop intergenic
        # variants the bundle knows perfectly well, and an --extract file
        # is about what to keep from the genotypes, not about genes.
        rows = [
            value
            for value, key, status in zip(
                table.column("plink_id").to_pylist(),
                table.column("variant_key").to_pylist(),
                table.column("match_status").to_pylist(),
            )
            if value and key and status in ("matched", "in_bundle_no_gene")
        ]
        with target.open("w", encoding="utf-8") as handle:
            for value in rows:
                handle.write(f"{value}\n")

        self.add_artifact(
            "plink_extract",
            target,
            kind="plink",
            description=(
                f"{len(rows):,} ids for `plink --extract`, one per line — "
                f"every cohort variant this bundle carries."
            ),
        )

    def _emit_variant_to_bin(
        self, build: int, window: int, maf_cutoff: float,
        rare_case_control: bool, overall_major_allele: bool, group_by: str,
    ) -> None:
        """
        Which variants went into which bin.

        A second table, not a file. A bin count is not auditable without
        it — two runs differing only in `maf_cutoff` produce different
        bins and the result table does not say which variants moved — so
        it travels *with* the result rather than beside it, where it
        could be lost or go stale without anything noticing.

        `variant_to_bin_path` still writes the CSV, for feeding something
        that reads files.
        """
        mapping = self.sql(f"""
            WITH {self._frequencies_cte(maf_cutoff, rare_case_control,
                                        overall_major_allele)},
            {self._placement_cte(build, window, self._needs_rsid_lookup())},
            {self._bins_cte(group_by, build)}
            SELECT
                m.chromosome, m.position, m.reference_allele, m.alternate_allele,
                m.variant_key, m.variant_id AS cohort_variant_id,
                m.maf_overall, m.maf_case, m.maf_control,
                b.bin_name, b.bin_type, b.gene_entity_id
            FROM binned b
            JOIN matched m ON m.row_id = b.row_id
            WHERE m.is_rare
            ORDER BY b.bin_name, m.chromosome, m.position
        """)
        self.emit("variant_to_bin", mapping)

        path = self.param("variant_to_bin_path")
        if not path:
            return
        target = Path(str(path)).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)

        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(mapping.column_names)
            for row in mapping.to_pylist():
                writer.writerow([row[name] for name in mapping.column_names])

        self.add_artifact(
            "variant_to_bin",
            target,
            kind="csv",
            description=(
                f"{mapping.num_rows:,} (variant, bin) rows — what each bin "
                f"count is made of."
            ),
        )

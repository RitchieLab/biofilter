"""
aggregate_cohort_variants: a cohort's own variants, against the bundle.

The fixture bundle holds TP53 at 17:100-400 with variants at 150, 200 and
300; BRCA1 at 17:5000-5400; and 17:9000, which lies in no gene. The
cohort files here are written to sit on top of that.

Two arithmetic facts drive most of these tests. With N samples the
smallest observable minor allele frequency is 1/(2N) — ten samples cannot
see anything rarer than 0.05 — and a variant nobody carries cannot reach
a bin however rare it is.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from biofilter.modules.report import Bundle, ReportManager

SAMPLES = [f"S{i + 1}" for i in range(10)]


def write_vcf(path: Path, records, samples=SAMPLES) -> Path:
    """`records` are (chrom, pos, id, ref, alt, [genotype per sample])."""
    header = ["##fileformat=VCFv4.2", "##contig=<ID=chr17>", "##contig=<ID=chr22>"]
    header.append(
        "\t".join(
            ["#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"]
            + list(samples)
        )
    )
    for chrom, pos, name, ref, alt, genotypes in records:
        header.append(
            "\t".join(
                [f"chr{chrom}", str(pos), name, ref, alt, ".", "PASS", ".", "GT"]
                + list(genotypes)
            )
        )
    path.write_text("\n".join(header) + "\n")
    return path


def carriers(n: int, total: int = len(SAMPLES)) -> list[str]:
    """`n` heterozygous carriers, the rest reference."""
    return ["0/1"] * n + ["0/0"] * (total - n)


@pytest.fixture
def cohort(tmp_path):
    """Three variants in TP53, one in BRCA1, one in no gene, one off-bundle."""
    return write_vcf(
        tmp_path / "cohort.vcf",
        [
            (17, 150, "v150", "A", "G", carriers(1)),
            (17, 200, "v200", "A", "G", carriers(2)),
            (17, 300, "v300", "A", "G", carriers(8)),
            (17, 5100, "v5100", "A", "G", carriers(1)),
            (17, 9000, "v9000", "A", "G", carriers(1)),
            (22, 777, "v777", "C", "T", carriers(1)),
        ],
    )


@pytest.fixture
def phenotype(tmp_path):
    rows = ["SampleID,Phenotype"]
    rows += [f"{s},{1 if i < 5 else 0}" for i, s in enumerate(SAMPLES)]
    path = tmp_path / "phenotype.csv"
    path.write_text("\n".join(rows) + "\n")
    return path


@pytest.fixture
def run(pairing_bundle):
    with Bundle.open(pairing_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("aggregate_cohort_variants", **params)
            return result.table.to_pylist(), result

        yield _run


def _by_id(rows):
    return {r["cohort_variant_id"]: r for r in rows}


class TestMatchingAgainstTheBundle:
    """`output_grain='variants'` — what `variant_list_intersect` did."""

    def test_a_variant_the_bundle_has_and_can_place(self, run, cohort):
        rows, _ = run(cohort_file=str(cohort))
        row = _by_id(rows)["v150"]

        assert row["match_status"] == "matched"
        assert row["variant_key"] == "17:150:A:G"
        assert row["gene_symbols"] == ["TP53"]

    def test_a_variant_in_no_gene_is_distinguished_from_one_the_bundle_lacks(
        self, run, cohort
    ):
        """
        17:9000 is in the bundle and in no gene's range. Reporting it the
        same way as a variant the bundle never heard of would hide which
        of the two happened.
        """
        rows, _ = run(cohort_file=str(cohort))
        row = _by_id(rows)["v9000"]

        assert row["match_status"] == "in_bundle_no_gene"
        assert row["variant_key"] == "17:9000:A:G"
        assert "no gene" in row["note"]

    def test_a_chromosome_the_bundle_does_not_carry_says_so(self, run, tmp_path):
        rows, _ = run(
            cohort_file=str(
                write_vcf(tmp_path / "off.vcf", [(9, 500, "x", "A", "G", carriers(1))])
            )
        )

        assert rows[0]["match_status"] == "chromosome_not_in_bundle"
        assert "carries no variants for that chromosome" in rows[0]["note"]

    def test_a_variant_on_a_covered_chromosome_that_is_simply_absent(
        self, run, tmp_path
    ):
        rows, _ = run(
            cohort_file=str(
                write_vcf(tmp_path / "gap.vcf", [(17, 424242, "g", "A", "G", carriers(1))])
            )
        )

        assert rows[0]["match_status"] == "not_in_bundle"
        assert "does not have this variant" in rows[0]["note"]

    def test_every_cohort_variant_comes_back(self, run, cohort):
        """A variant the bundle cannot explain is still the user's data."""
        rows, _ = run(cohort_file=str(cohort))

        assert len(rows) == 6


class TestOtherCohortFormats:
    def test_a_plink_bim_reads_allele_two_as_the_reference(self, tmp_path, run):
        """
        PLINK names the minor allele first. Taking allele 1 as the
        reference reverses every variant and matches nothing.
        """
        bim = tmp_path / "cohort.bim"
        bim.write_text("17\trs150\t0\t150\tG\tA\n")
        rows, _ = run(cohort_file=str(bim))

        assert rows[0]["reference_allele"] == "A"
        assert rows[0]["alternate_allele"] == "G"
        assert rows[0]["match_status"] == "matched"

    def test_a_plain_position_list(self, tmp_path, run):
        listing = tmp_path / "cohort.txt"
        listing.write_text("# a comment\n17:150:A:G\n17:300:A:G\n")
        rows, _ = run(cohort_file=str(listing))

        assert {r["variant_key"] for r in rows} == {"17:150:A:G", "17:300:A:G"}

    def test_an_unreadable_line_is_counted_rather_than_ignored(self, tmp_path, run):
        listing = tmp_path / "cohort.txt"
        listing.write_text("17:150:A:G\nnot a variant\n")
        _, result = run(cohort_file=str(listing))

        assert result.provenance["cohort"]["rows_skipped"] == {"unrecognised": 1}

    def test_a_missing_file_says_which(self, run):
        with pytest.raises(FileNotFoundError, match="nope.vcf"):
            run(cohort_file="/tmp/nope.vcf")


class TestTheChromosomeGuard:
    """
    The failure this report exists to prevent: a cohort spanning the
    genome, a bundle spanning one chromosome, and a result that looks
    complete.
    """

    def test_it_reports_what_the_bundle_cannot_place(self, run, cohort):
        _, result = run(cohort_file=str(cohort))
        coverage = result.provenance["chromosome_coverage"]

        assert coverage["chromosomes_in_cohort"] == [17, 22]
        assert coverage["chromosomes_missing_from_bundle"] == []

    def test_it_counts_the_variants_that_fall_outside(self, run, tmp_path):
        mixed = write_vcf(
            tmp_path / "mixed.vcf",
            [
                (17, 150, "in", "A", "G", carriers(1)),
                (9, 500, "out1", "A", "G", carriers(1)),
                (9, 600, "out2", "A", "G", carriers(1)),
            ],
        )
        _, result = run(cohort_file=str(mixed))
        coverage = result.provenance["chromosome_coverage"]

        assert coverage["chromosomes_missing_from_bundle"] == [9]
        assert coverage["variants_the_bundle_cannot_place"] == 2
        assert coverage["share_unplaceable"] == pytest.approx(2 / 3, abs=1e-4)

    def test_it_can_be_made_a_refusal(self, run, tmp_path):
        mixed = write_vcf(
            tmp_path / "mixed.vcf",
            [
                (17, 150, "in", "A", "G", carriers(1)),
                (9, 500, "out", "A", "G", carriers(1)),
            ],
        )

        with pytest.raises(ValueError, match=r"chromosome\(s\) \[9\]"):
            run(cohort_file=str(mixed), require_full_coverage=True)

    def test_the_refusal_says_how_much_was_at_stake(self, run, tmp_path):
        mixed = write_vcf(
            tmp_path / "mixed.vcf",
            [(9, 500, "out", "A", "G", carriers(1))],
        )

        with pytest.raises(ValueError, match="100.0%"):
            run(cohort_file=str(mixed), require_full_coverage=True)


class TestAlleleFrequencies:
    def test_counts_come_from_the_genotypes(self, run, cohort):
        """Two heterozygous carriers out of ten samples: 2 copies of 20."""
        rows, _ = run(cohort_file=str(cohort))
        row = _by_id(rows)["v200"]

        assert row["ac_overall"] == 2
        assert row["an_overall"] == 20
        assert row["maf_overall"] == pytest.approx(0.1)

    def test_a_no_call_leaves_the_denominator(self, run, tmp_path):
        """`./.` is not a reference call, and counting it as one deflates the MAF."""
        vcf = write_vcf(
            tmp_path / "nocall.vcf",
            [(17, 150, "v", "A", "G", ["0/1"] + ["./."] * 9)],
        )
        rows, _ = run(cohort_file=str(vcf))

        assert rows[0]["an_overall"] == 2
        assert rows[0]["maf_overall"] == pytest.approx(0.5)

    def test_the_minor_allele_is_the_rarer_one(self, run, tmp_path):
        """A variant carried by nearly everyone has a low MAF, not a high one."""
        vcf = write_vcf(
            tmp_path / "common.vcf",
            [(17, 150, "v", "A", "G", ["1/1"] * 9 + ["0/1"])],
        )
        rows, _ = run(cohort_file=str(vcf))

        assert rows[0]["ac_overall"] == 19
        assert rows[0]["maf_overall"] == pytest.approx(0.05)

    def test_case_and_control_are_counted_apart(self, run, cohort, phenotype):
        """S1-S5 are cases. v300 has eight carriers, so five of them are cases."""
        rows, _ = run(cohort_file=str(cohort), phenotype_file=str(phenotype))
        row = _by_id(rows)["v300"]

        assert row["maf_case"] == pytest.approx(0.5)
        assert row["maf_control"] == pytest.approx(0.3)


class TestThePhenotypeFile:
    def test_samples_in_one_file_and_not_the_other_are_reported(
        self, run, cohort, tmp_path
    ):
        odd = tmp_path / "odd.csv"
        odd.write_text("SampleID,Phenotype\nS1,1\nS2,0\nGHOST,1\n")
        _, result = run(cohort_file=str(cohort), phenotype_file=str(odd))
        phenotype = result.provenance["phenotype"]

        assert phenotype["matched"] == 2
        assert phenotype["phenotype_only"] == ["GHOST"]
        assert len(phenotype["cohort_only"]) == 8

    def test_a_missing_column_says_what_the_file_has(self, run, cohort, tmp_path):
        wrong = tmp_path / "wrong.csv"
        wrong.write_text("id,value\nS1,1\n")

        with pytest.raises(ValueError, match="has no column"):
            run(
                cohort_file=str(cohort),
                phenotype_file=str(wrong),
                phenotype_sample_column="SampleID",
            )

    def test_the_counts_are_recorded(self, run, cohort, phenotype):
        _, result = run(cohort_file=str(cohort), phenotype_file=str(phenotype))

        assert result.provenance["phenotype"]["cases"] == 5
        assert result.provenance["phenotype"]["controls"] == 5


class TestBinning:
    """`output_grain='bins'` — what `variant_binning` did."""

    def test_rare_variants_in_a_gene_become_a_bin(self, run, cohort):
        rows, _ = run(
            cohort_file=str(cohort), output_grain="bins", group_by="gene",
            maf_cutoff=0.15,
        )

        assert {r["bin_name"] for r in rows} == {"TP53", "BRCA1"}
        assert {r["bin_type"] for r in rows} == {"gene"}

    def test_a_row_is_one_sample_in_one_bin(self, run, cohort):
        rows, _ = run(
            cohort_file=str(cohort), output_grain="bins", group_by="gene",
            maf_cutoff=0.15,
        )
        keys = {(r["bin_name"], r["sample"]) for r in rows}

        assert len(keys) == len(rows)

    def test_the_counts_are_what_the_sample_carries(self, run, tmp_path):
        """S1 carries both TP53 variants; S2 carries one."""
        vcf = write_vcf(
            tmp_path / "c.vcf",
            [
                (17, 150, "a", "A", "G", ["0/1"] + ["0/0"] * 9),
                (17, 200, "b", "A", "G", ["1/1", "0/1"] + ["0/0"] * 8),
            ],
        )
        rows, _ = run(cohort_file=str(vcf), output_grain="bins", group_by="gene",
                      maf_cutoff=0.2)
        by_sample = {r["sample"]: r for r in rows}

        assert by_sample["S1"]["variant_count"] == 2
        assert by_sample["S1"]["alt_count"] == 3
        assert by_sample["S2"]["variant_count"] == 1
        assert by_sample["S2"]["alt_count"] == 1

    def test_only_carriers_appear(self, run, tmp_path):
        """A sample carrying nothing in a bin is not a row of zeros."""
        vcf = write_vcf(
            tmp_path / "c.vcf", [(17, 150, "a", "A", "G", ["0/1"] + ["0/0"] * 9)]
        )
        rows, _ = run(cohort_file=str(vcf), output_grain="bins", maf_cutoff=0.2)

        assert [r["sample"] for r in rows] == ["S1"]

    def test_a_common_variant_is_left_out(self, run, cohort):
        """v300 has eight carriers of ten: common, so not in a rare-variant bin."""
        _, result = run(
            cohort_file=str(cohort), output_grain="bins", maf_cutoff=0.15,
        )

        assert result.provenance["rare_variants"]["rare"] == 5

    def test_the_bin_totals_travel_with_each_row(self, run, cohort):
        rows, _ = run(
            cohort_file=str(cohort), output_grain="bins", group_by="gene",
            maf_cutoff=0.15,
        )
        tp53 = [r for r in rows if r["bin_name"] == "TP53"]

        assert {r["bin_variant_count"] for r in tp53} == {2}
        assert {r["bin_gene_count"] for r in tp53} == {1}

    def test_the_sample_class_comes_through(self, run, cohort, phenotype):
        rows, _ = run(
            cohort_file=str(cohort), output_grain="bins", phenotype_file=str(phenotype),
            maf_cutoff=0.15,
        )

        assert {r["sample_class"] for r in rows} <= {"case", "control"}


class TestTheOtherBinTypes:
    def test_gene_group(self, run, cohort):
        rows, _ = run(
            cohort_file=str(cohort), output_grain="bins", group_by="gene_group",
            maf_cutoff=0.15,
        )

        assert {r["bin_name"] for r in rows} == {"p53 family", "BRCA1 A complex"}

    def test_locus_type(self, run, cohort):
        rows, _ = run(
            cohort_file=str(cohort), output_grain="bins", group_by="locus_type",
            maf_cutoff=0.15,
        )

        assert {r["bin_type"] for r in rows} == {"locus_type"}

    def test_pathway(self, run, cohort):
        """The fixture's PATHWAY reaches TP53 and BRCA1."""
        rows, _ = run(
            cohort_file=str(cohort), output_grain="bins", group_by="pathway",
            maf_cutoff=0.15,
        )

        assert rows
        assert {r["bin_type"] for r in rows} == {"pathway"}

    def test_an_unknown_grouping_is_refused(self, run, cohort):
        with pytest.raises(ValueError, match="group_by must be one of"):
            run(cohort_file=str(cohort), output_grain="bins", group_by="chromosome")

    def test_the_reach_of_each_grouping_is_recorded(self, run, cohort):
        _, result = run(
            cohort_file=str(cohort), output_grain="bins", group_by="pathway",
            maf_cutoff=0.15,
        )
        coverage = result.provenance["bin_coverage"]

        assert coverage["group_by"] == "pathway"
        assert coverage["genes_this_bin_type_can_reach"] >= 1


class TestWhenBinningCannotWork:
    def test_a_file_without_genotypes_is_refused_with_a_reason(self, run, tmp_path):
        bim = tmp_path / "c.bim"
        bim.write_text("17\trs1\t0\t150\tG\tA\n")

        with pytest.raises(ValueError, match="needs genotypes"):
            run(cohort_file=str(bim), output_grain="bins")

    def test_a_cutoff_below_what_the_cohort_can_observe_says_so(self, run, cohort):
        """
        Ten samples cannot see a frequency under 0.05. Asking for 0.01
        keeps only variants nobody carries, and every bin comes back
        empty for a reason no column shows.
        """
        rows, result = run(
            cohort_file=str(cohort), output_grain="bins", maf_cutoff=0.01
        )
        rare = result.provenance["rare_variants"]

        assert rows == []
        assert rare["smallest_observable_maf"] == pytest.approx(0.05)
        assert "below that" in rare["means"]

    def test_an_impossible_cutoff_is_refused(self, run, cohort):
        with pytest.raises(ValueError, match=r"lies in \(0, 0.5\]"):
            run(cohort_file=str(cohort), output_grain="bins", maf_cutoff=0.8)

    def test_how_many_rare_variants_had_a_carrier(self, run, cohort):
        _, result = run(
            cohort_file=str(cohort), output_grain="bins", maf_cutoff=0.15
        )
        rare = result.provenance["rare_variants"]

        assert rare["rare"] == 5
        assert rare["rare_with_carriers"] == 5


class TestArtifacts:
    def test_the_plink_extract_file_holds_every_id_the_bundle_carries(
        self, run, cohort, tmp_path
    ):
        """
        Including 17:9000, which no gene contains. Gene placement is a
        separate question from whether the bundle knows the variant, and
        an --extract file answers the second one.
        """
        target = tmp_path / "keep.txt"
        _, result = run(cohort_file=str(cohort), plink_extract_path=str(target))

        assert target.is_file()
        assert set(target.read_text().split()) == {
            "v150", "v200", "v300", "v5100", "v9000", "v777",
        }
        assert [a.name for a in result.artifacts] == ["plink_extract"]

    def test_it_leaves_out_what_the_bundle_could_not_match(self, run, tmp_path):
        """
        A `--extract` list carrying ids the report said nothing about
        keeps variants the analysis never covered.
        """
        vcf = write_vcf(
            tmp_path / "c.vcf",
            [
                (17, 150, "known", "A", "G", carriers(1)),
                (17, 424242, "unknown", "A", "G", carriers(1)),
                (9, 500, "offchrom", "A", "G", carriers(1)),
            ],
        )
        target = tmp_path / "keep.txt"
        run(cohort_file=str(vcf), plink_extract_path=str(target))

        assert target.read_text().split() == ["known"]

    def test_the_variant_to_bin_map_is_written_when_asked(self, run, cohort, tmp_path):
        target = tmp_path / "v2b.csv"
        _, result = run(
            cohort_file=str(cohort), output_grain="bins", maf_cutoff=0.15,
            variant_to_bin_path=str(target),
        )

        assert target.is_file()
        assert "bin_name" in target.read_text().splitlines()[0]
        assert [a.name for a in result.artifacts] == ["variant_to_bin"]

    def test_no_artifact_is_written_unasked(self, run, cohort):
        _, result = run(cohort_file=str(cohort))

        assert result.artifacts == []


class TestTheWindow:
    def test_it_widens_which_gene_a_variant_falls_in(self, run, tmp_path):
        """17:9000 is in no gene; a window wide enough reaches BRCA1."""
        vcf = write_vcf(
            tmp_path / "c.vcf", [(17, 9000, "v", "A", "G", carriers(1))]
        )
        narrow, _ = run(cohort_file=str(vcf))
        wide, _ = run(cohort_file=str(vcf), window_bp=4_000)

        assert narrow[0]["gene_symbols"] is None
        assert wide[0]["gene_symbols"] == ["BRCA1"]

    def test_a_negative_window_is_refused(self, run, cohort):
        with pytest.raises(ValueError, match="window_bp must be 0 or positive"):
            run(cohort_file=str(cohort), window_bp=-1)


class TestTheContract:
    def test_declared_columns_match_the_variants_grain(self, run, cohort):
        from biofilter.modules.report.reports.report_aggregate_cohort_variants import (
            AggregateCohortVariantsReport,
        )

        _, result = run(cohort_file=str(cohort))

        assert list(result.table.column_names) == list(
            AggregateCohortVariantsReport.available_columns()
        )

    def test_the_bins_grain_matches_its_own_declaration(self, run, cohort):
        from biofilter.modules.report.reports.report_aggregate_cohort_variants import (
            AggregateCohortVariantsReport,
        )

        _, result = run(
            cohort_file=str(cohort), output_grain="bins", maf_cutoff=0.15
        )

        assert list(result.table.column_names) == list(
            AggregateCohortVariantsReport.BIN_COLUMNS
        )

    def test_provenance_records_the_cohort_and_the_bundle(self, run, cohort):
        _, result = run(cohort_file=str(cohort))

        assert result.provenance["bundle_id"] == "fixturebundle0001"
        assert result.provenance["cohort"]["format"] == "vcf"
        assert result.provenance["cohort"]["variants_read"] == 6

    def test_an_unknown_grain_is_refused(self, run, cohort):
        with pytest.raises(ValueError, match="output_grain must be one of"):
            run(cohort_file=str(cohort), output_grain="samples")

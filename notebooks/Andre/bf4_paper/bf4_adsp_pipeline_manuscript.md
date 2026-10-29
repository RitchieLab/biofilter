# Knowledge-driven selection of variant pairs for interaction testing in ADSP

*Draft manuscript section — methods and results of the Biofilter 4 selection
pipeline. Companion to `bf4_platform_manuscript.md`, which describes the
platform itself.*

Draft of 2026-10-21 · run of 2026-09-18 against Biofilter 4.3.0, bundle
`9b8419b48be5004e`

> **How to use this draft.** It covers the pipeline from the ADSP variant list
> to the set of variant-pair models handed to interaction testing. The
> interaction testing itself, the cohort description and the phenotype belong
> to the main paper and are left as placeholders. Items marked **[confirm]**
> need checking; the open decisions from the analysis are collected at the end,
> because some of them change the numbers in this text.

---

## Summary

An exhaustive pairwise search over the 711,836 ADSP variants considered here
would mean about 2.5 × 10¹¹ models. We used Biofilter 4 to cut that down to
pairs that prior biological knowledge supports, in three steps:

1. keep variants annotated to a protein-coding gene;
2. keep those with functional evidence — a protein-altering consequence for
   coding variants, or a brain eQTL and pQTL on the same gene for non-coding
   ones;
3. pair the variants whose genes share a curated pathway.

The result is **817,420 candidate variant-pair models** over 5,420 variants.
A stricter set requiring Reactome and KEGG to agree has **91,135**. Both are
an order of magnitude below the ~10 million models considered feasible for
testing **[confirm budget and its source]**. The whole selection is five calls
to Biofilter reports, run against a single versioned knowledge bundle, and
each output records the bundle and parameters that produced it.

```
711,836   ADSP variants
   ↓      Step 1 — annotated to a protein-coding gene
355,644
   ↓      Step 2 — functional evidence (partition: coding / non-coding)
  7,603
   ↓      Step 3 — gene pairs sharing a pathway (≤500 genes)
817,420   variant-pair models      (91,135 with two curations)
```

---

## Methods

### Input variants

The input was a list of 711,836 autosomal single-nucleotide variants from ADSP
**[confirm: release, sample set, QC and how the list was derived]**,
identified by chromosome, position and alleles on GRCh38.

### Knowledge resource

All annotation came from one Biofilter 4.3.0 bundle (`bundle_id`
`9b8419b48be5004e`), built from public sources in September 2026. The parts
this analysis used were:

| Used for | Source in the bundle |
| --- | --- |
| Variant catalogue and gene consequences | gnomAD v4.1 joint callset; VEP annotations shipped with gnomAD |
| Gene type (protein-coding or not) | HGNC locus group and locus type |
| Gene identifiers and aliases | HGNC, NCBI Gene, Ensembl |
| Brain eQTL | GTEx v10, 13 brain tissues, eQTL only (no sQTL) |
| Pathway membership | Reactome, KEGG |

Brain pQTL came from outside Biofilter, from the ADSP Functional Genomics
Consortium (FunGen) xQTL brain proteomics resource
(`brain_pQTL_hmt_variant_gene_lookup.tsv.gz`) **[confirm citation and
cohorts]**.

Of the 711,836 input variants, 711,651 (99.97%) were found in the bundle's
variant catalogue.

### Step 1 — variants annotated to a protein-coding gene

Each variant was annotated with the genes VEP associates it with
(`annotate_variant`). This includes genes the variant lies within and genes
within VEP's default 5 kb upstream or downstream window. Each of those genes
was then classified by its HGNC locus group (`annotate_gene`). A variant was
kept if at least one of its genes has HGNC locus group `protein-coding gene`.
Molecular consequence was not used at this step.

Genes that Biofilter knows only from NCBI or Ensembl, with no HGNC locus
type, were excluded by default (562 `LOC*`/`ENSG*` genes), so the gene
classification has a single provenance. Including them is a parameter.

### Step 2 — functional evidence, as a partition

Coding and non-coding variants have different notions of "functional", so
step 2 is a **partition**, not two filters applied to every variant. Each
variant was assigned to exactly one queue by whether it has a consequence in
VEP's `coding` category, and was judged only by that queue's criterion.

**Queue A — coding variants must alter the protein.** A variant was kept if
its most severe consequence has a VEP severity rank of 14 or lower. That cut
corresponds exactly to the ten consequence types specified for this analysis:
missense, stop gained, stop lost, start lost, splice acceptor, splice donor,
frameshift, in-frame insertion, in-frame deletion and protein-altering
variant. The variant is paired on the gene whose protein it alters.

**Queue B — non-coding variants must regulate a gene at both the RNA and
protein level.** A variant was kept if it is a GTEx brain eQTL
(`expand_variant_regulatory`) and a FunGen brain pQTL **for the same Ensembl
gene**. The bundle carries GTEx's significant cis-eQTL pairs only, and no
further p-value threshold was applied (see sensitivity analyses). The variant is paired on that **QTL target
gene**, not on the gene it lies in. The target genes were then mapped to
Biofilter gene records (`annotate_gene`).

### Step 3 — pairing variants through shared pathways

Two variants form a candidate model when the genes they were selected for are
both members of a curated Reactome or KEGG pathway (`pair_genes`). The
variant–gene assignment from step 2 was supplied to the report directly, not
re-derived from genomic position. Re-deriving it would assign a queue B variant
to the gene it lies in rather than to its QTL target.

Two parameters control the result:

- **Pathway size cap (`max_group_size = 500`).** A pathway naming thousands of
  genes connects all of them while asserting little about any particular
  pair. Of the 2,526 pathways that contain at least one selected gene, the 35
  with more than 500 members (521 to 2,615 genes) were excluded.
- **Curation support (`min_group_sources`).** The count of independent
  curations (Reactome, KEGG) asserting that a gene pair shares a pathway. The
  primary set requires one; the strict set requires both.

Models are counted as **distinct unordered variant pairs**. A pair of variants
can be linked by more than one gene pair, for example when a variant is
assigned to two genes, and it is counted once.

### Software and reproducibility

All steps ran with Biofilter 4.3.0 against bundle `9b8419b48be5004e`, reading
the bundle in-process with DuckDB and with no database server. The five report
calls were `annotate_variant`, `annotate_gene` (step 1),
`expand_variant_regulatory`, `annotate_gene` (step 2) and `pair_genes`
(step 3). Each step is available as a notebook and as an equivalent
command-line script. Every output file is accompanied by a `.provenance.json`
recording the bundle identifier, the reports and parameters, and the
filtering decisions made. Step 2 refuses to proceed if its input was produced
from a different bundle. Genes are carried between steps by their Ensembl
identifier, which is stable across bundle builds. Biofilter's internal entity
identifiers are not stable across builds.

---

## Results

### Step 1

| | variants |
| --- | ---: |
| Input | 711,836 |
| Found in the bundle | 711,651 |
| Annotated to a protein-coding gene | 356,268 |
| Less those whose only such gene lacks an HGNC locus type | −624 |
| **Retained** | **355,644** (49.97%) |

The 356,007 variants removed (including the 185 not in the bundle) fall into
three mutually exclusive groups:

| Reason | variants |
| --- | ---: |
| No gene annotated by VEP (intergenic) | 179,630 |
| Annotated only to non-coding genes (lncRNA, pseudogenes) | 104,998 |
| Annotated to a gene with no Biofilter record | 71,379 |

Retained variants produced 380,775 variant–gene rows. Intronic variants made
up 79.6% of them, and 93.5% of retained variants map to exactly one gene.

### Step 2

| Queue | Criterion | variants |
| --- | --- | ---: |
| A — coding | protein-altering consequence (severity rank ≤ 14) | 2,800 |
| B — non-coding | brain eQTL and pQTL for the same gene | 4,803 |
| **Total** | queues are disjoint | **7,603** |

**Queue A.** Of 6,601 variants with a coding consequence, 3,801 did not meet
the protein-altering criterion: 2,732 synonymous, 1,064 splice-region
variants (`splice_region_variant` 475, `splice_polypyrimidine_tract_variant`
458, `splice_donor_region_variant` 96, `splice_donor_5th_base_variant` 35),
and 5 others. Four of the ten specified consequence types
(frameshift, in-frame insertion, in-frame deletion, protein-altering) are
indel consequences, and none occur in an SNV-only list.

**Queue B.** The number of variants in queue B depends on how much of the
ADSP list the pQTL resource covers, not on the eQTL criterion:

| | variants |
| --- | ---: |
| In the pQTL resource | 283,029 |
| … also in the ADSP list | 12,331 (4.4%) |
| … retained by step 1 | 9,451 |
| … with a brain eQTL for the same gene | 4,803 |

Pairing on the QTL target mattered. Among eQTL links where both genes are
known, **74.3% named a target gene different from the gene the variant lies
in.** Positional assignment would have paired most of queue B on the wrong
gene. The 4,803 queue B variants regulate 1,023 target genes.

### Step 3

The 7,603 variants map to 3,094 genes. Shared pathway membership among those
genes gives:

| Result set | gene pairs | variants in ≥1 model | models |
| --- | ---: | ---: | ---: |
| One curation (primary) | 100,245 | 5,420 | **817,420** |
| Two curations (strict) | 11,727 | 3,133 | **91,135** |

The two sets make different claims. The primary set asks whether two genes
share a pathway. The strict set asks whether Reactome and KEGG independently
agree that they do. The strict set has to be produced by its own query. It
cannot be recovered by filtering the primary set's output on its support
column, which returns 91,093 (0.05% fewer). This is because deduplication
keeps one row per variant pair, and that row may come from a gene pair with
less support than another pair linking the same two variants.

### Sensitivity of the model count

**Pathway size cap and curation support** (models):

| `max_group_size` | one curation | two curations |
| --- | ---: | ---: |
| no limit | 3,825,038 | 797,453 |
| **500** | **817,420** | **91,135** |
| 300 | 592,407 | 66,924 |
| 200 | 402,114 | 48,585 |

Removing the cap multiplies the primary set nearly fivefold, and the whole
increase comes from 35 very large pathways.

**eQTL significance threshold** (queue B variants):

| eQTL p-value cutoff | queue B |
| --- | ---: |
| none | 4,803 |
| ≤ 1 × 10⁻⁴ | 4,704 |
| ≤ 1 × 10⁻⁶ | 3,328 |
| ≤ 1 × 10⁻⁸ | 2,378 |
| ≤ 1 × 10⁻¹⁰ | 1,755 |

A loose cutoff has almost no effect, because the pQTL requirement already does
the filtering.

**Replacing the pQTL requirement with eQTL replication across tissues**
(queue B variants): eQTL in ≥ 1 brain tissue, 82,219; in ≥ 5 tissues, 25,179;
eQTL and pQTL for the same gene (as used), 4,803.

### Consistency checks

- Step 1 was first implemented on the earlier Biofilter 4.2 release. Run
  against the current bundle it returns 355,644 variants against 355,042, a
  difference of 0.17%.
- An earlier estimate of the strict set (99,321 models) summed
  `n₁ × n₂` over gene pairs and so counted a variant pair once per gene pair
  linking it. Counting distinct variant pairs gives 91,135.
- Eight Ensembl genes in the selected set correspond to two Biofilter gene
  records each (a named gene and an NCBI `LOC*` duplicate). Pairing on
  Biofilter entity identifiers instead removes the duplication and gives
  817,108 models instead of 817,420 (0.04%).
- Five selected variants are assigned to a gene with no Biofilter record and
  cannot be paired. The report lists them instead of dropping them silently.

---

## Discussion points

*For the main paper's discussion; trim as needed.*

- **Knowledge selection, not random reduction.** Each step asks a biological
  question — could this variant affect a protein, does it measurably affect
  one, do the two affected genes act together — instead of thinning the
  search space arbitrarily.
- **Different evidence for different variant classes.** Treating step 2 as a
  partition keeps coding and regulatory variants under criteria appropriate
  to each. Pairing regulatory variants on their QTL target puts them in the
  pathway of the gene they act on, which in three of four cases is not the
  gene they lie in.
- **Headroom.** The primary set leaves an order of magnitude of headroom
  against the feasible testing budget. The sensitivity tables show which
  choices would use it and by how much.
- **Reproducibility.** The selection depends on the bundle, the parameters
  and nothing else, and both are recorded beside every output. Because
  public sources keep releasing new versions, a later rerun is reproducible
  only against the same bundle, not against "the current sources".

### Limitations

- Brain eQTL is limited to the 13 GTEx brain tissues in the bundle, and to
  eQTL. sQTL is not included, so splicing-mediated regulation is not captured.
- Queue B is bounded by pQTL coverage: only 4.4% of the pQTL resource overlaps
  the ADSP list. This is partly because 19% of the pQTL variants are indels
  and partly because FunGen xQTL used a different variant set.
- The 1,064 splice-region variants are excluded by construction. They are in
  the coding category, so they cannot enter queue B, and they fall below the
  queue A severity cut (see open decisions).
- Pairing uses pathway co-membership only. Protein interaction, shared disease
  association and gene families are available in Biofilter but were not used
  here.
- Gene annotation inherits the choices of VEP and HGNC. 71,379 variants map to
  genes with no Biofilter record and were removed at step 1.

---

## Suggested figures and tables

- **Figure — Selection funnel.** 711,836 → 355,644 → 7,603 (split
  2,800 / 4,803) → 817,420 / 91,135, with the Biofilter report used at each
  step.
- **Figure — Pairing schematic.** A coding variant paired on the gene it
  alters, a regulatory variant paired on its QTL target, and the two joined
  through a shared pathway.
- **Table — Step criteria and counts** (Steps 1–3 above, condensed).
- **Supplementary tables** — removal reasons in step 1, queue A consequence
  breakdown, and the three sensitivity analyses.

---

## Open decisions for Diane

These came out of the analysis and are not settled yet. The text above
describes the configuration as run.

1. **Splice-region variants (1,064).** Keep them out, or move the queue A cut
   from rank 14 to 18 to include them.
2. **Which result set goes to interaction testing:** 817,420 (one curation)
   or 91,135 (two curations).
3. **Whether to use the headroom**, and on what: lifting the pathway size cap
   (→ 3.8 M models), replacing the pQTL requirement with eQTL in ≥ 5 brain
   tissues (queue B → 25,179), adding other group types (not yet measured), or
   relaxing the queue A cut (+1,064 variants).
4. **The eQTL p-value cutoff**, currently off.

Any change to 1–3 changes the numbers in the Results.

### Where the numbers come from

All in `notebooks/Andre/adsp/`: `README.md` (counts and sensitivity),
`burrao/00_summary.md` (technical appendix and consistency checks),
`burrao/step_2.md` (consequence ranks), the step scripts
(`step_0{1,2,3}_*.py`) for the criteria as implemented, and
`outputs/*.provenance.json` for the parameters of each output.

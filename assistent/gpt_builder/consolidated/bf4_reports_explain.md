# BF4 Report Reference (per report)



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_aggregate_cohort_variants.md ===== -->

# aggregate_cohort_variants

Your cohort's variants, matched against the bundle and aggregated into
biological bins.

```bash
biofilter report run --report-name aggregate_cohort_variants \
    --param cohort_file=./cohort.vcf.gz \
    --param phenotype_file=./phenotype.csv \
    --param output_grain=bins \
    --param group_by=gene \
    --param maf_cutoff=0.01 \
    --output bins.csv
```

> Replaces `variant_list_intersect` and `variant_binning`, which were the
> same pipeline stopped at different points.

## Three stages, two places to stop

1. **Read** the cohort file — a VCF with genotypes, a PLINK `.bim`, or a
   plain list of rsIDs and positions.
2. **Match** each variant against the bundle and place it on the genes
   whose build-38 range contains it.
3. **Aggregate** — keep the rare variants, put each in a bin, and count
   what every sample carries in every bin.

| `output_grain` | one row is | needs genotypes | replaces |
| --- | --- | --- | --- |
| `variants` (default) | one cohort variant | no | `variant_list_intersect` |
| `bins` | one sample in one bin | **yes** | `variant_binning` |

Asking for `bins` with a `.bim` is an error, not an empty result: a
`.bim` has no genotypes, and binning counts what people carry.

## The guard that matters most

A cohort file spans the genome. A bundle need not. Binning a whole-genome
VCF against a bundle carrying one chromosome **does not fail** — it
returns bins covering that chromosome and says nothing about the rest.

Every run reports the overlap:

```json
"chromosome_coverage": {
  "chromosomes_in_cohort": [1, 7, 22],
  "chromosomes_in_bundle": [22],
  "chromosomes_missing_from_bundle": [1, 7],
  "variants_the_bundle_cannot_place": 2,
  "share_unplaceable": 0.005
}
```

Pass `require_full_coverage=true` to turn that into a refusal. Do that
whenever the result is going anywhere but your own screen.

## The other way an empty result happens

With **N samples the smallest observable minor allele frequency is
1/(2N)** — one allele copy in one person. Ten samples cannot see anything
rarer than 0.05. Ask for `maf_cutoff=0.01` on a ten-sample cohort and
every variant passing the filter is one nobody carries, so every bin
comes back empty, correctly, for a reason no column shows.

So the report says it outright:

```json
"rare_variants": {
  "maf_cutoff": 0.01,
  "samples": 20,
  "smallest_observable_maf": 0.025,
  "rare": 368,
  "rare_with_carriers": 0,
  "means": "This cohort has 20 samples, so the smallest frequency it can
            observe is 0.0250 … Raise maf_cutoff above 0.0250, or use a
            larger cohort."
}
```

## Which frequency the rare filter uses

| setting | filters on |
| --- | --- |
| `rare_case_control=true` (default), both arms present | the **larger** of the case and control MAFs |
| `rare_case_control=false`, `overall_major_allele=false` | the control MAF |
| otherwise | the overall MAF |

The first is BioBin's rule: a variant counts as rare only if it is rare in
both arms, so a variant common in cases and absent in controls is not
swept into a rare-variant bin. This is the relational version's behaviour,
kept exactly — changing it silently would reclassify somebody's variants.

A no-call (`./.`) leaves the denominator rather than counting as a
reference call, which would deflate every frequency.

## Bin types, and how much of the catalogue each can reach

| `group_by` | bin is | genes it can reach |
| --- | --- | --- |
| `gene` (default) | the gene | 39,306 — 54.1% |
| `gene_group` | an HGNC gene family | 25,059 — 34.5% |
| `locus_type` | protein-coding, lncRNA, pseudogene… | 39,306 — 54.1% |
| `pathway` | a Reactome or KEGG pathway | 12,521 — 17.2% |

Step 2 is positional, and only 39,306 of the bundle's 72,660 genes carry
build-38 coordinates — so a little over half the catalogue is reachable at
all, whatever the grouping. The `bin_coverage` provenance block reports
the number for the grouping you chose. A variant in a gene the grouping
cannot reach is reported as unbinned, not dropped.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `cohort_file` | required | VCF / `.vcf.gz`, PLINK `.bim`, or a plain list |
| `cohort_format` | by extension | `vcf`, `bim` or `list`, to override |
| `output_grain` | `variants` | or `bins` |
| `group_by` | `gene` | `gene`, `gene_group`, `locus_type`, `pathway` |
| `phenotype_file` | none | CSV with a sample column and a phenotype column |
| `phenotype_sample_column` | `SampleID` | |
| `phenotype_value_column` | `Phenotype` | |
| `phenotype_control_value` | `0` | values meaning control |
| `phenotype_case_values` | none | when absent, anything not a control is a case |
| `maf_cutoff` | `0.01` | a minor allele frequency, so in (0, 0.5] |
| `rare_case_control` | `true` | see above |
| `overall_major_allele` | `true` | see above |
| `window_bp` | `0` | widen a gene's range when placing a variant |
| `build` | `38` | |
| `max_variants` | none | stop after this many, for a quick look |
| `require_full_coverage` | `false` | refuse if the bundle cannot place a chromosome |
| `plink_extract_path` | none | write a `--extract` list |
| `variant_to_bin_path` | none | also write the (variant, bin) mapping as a CSV |

## The PLINK `--extract` file

PLINK matches on the id in your `.bim`, not on coordinates, so a file of
`chr:pos` strings extracts nothing from a dataset whose ids are rsIDs.
The id your own file used is therefore preferred, falling back to
`chr:pos`.

It lists **every cohort variant the bundle carries**, including ones no
gene contains. Gene placement is a separate question from whether the
bundle knows the variant, and `--extract` answers the second one.

## Columns

### `output_grain="variants"`

| column | meaning |
| --- | --- |
| `cohort_variant_id` | the id in your file, or `chr:pos:ref:alt` |
| `chromosome`, `position`, alleles | as your file spelled them |
| `variant_key` | the bundle's key, when it has the variant |
| `rsid`, `plink_id` | for joining and for `--extract` |
| `match_status`, `note` | see below |
| `gene_entity_ids`, `gene_symbols` | the genes containing it |
| `maf_overall`, `maf_case`, `maf_control` | from your genotypes |
| `ac_overall`, `an_overall` | allele count and called alleles |
| `is_rare` | whether it passes `maf_cutoff` |

| `match_status` | means |
| --- | --- |
| `matched` | the bundle has it and a gene contains it |
| `in_bundle_no_gene` | the bundle has it; no gene with coordinates contains it |
| `not_in_bundle` | the bundle covers the chromosome and lacks this variant |
| `chromosome_not_in_bundle` | the bundle carries nothing for that chromosome |
| `unresolved` | an rsID the bundle does not carry, so it has no coordinates |

### `output_grain="bins"`

| column | meaning |
| --- | --- |
| `bin_name`, `bin_type` | the bin, and what kind it is |
| `sample`, `sample_class` | the person, and `case` / `control` / `unknown` |
| `variant_count` | rare variants in this bin the sample carries |
| `alt_count` | alternate allele copies — the burden |
| `bin_variant_count`, `bin_gene_count` | the bin's totals across the cohort |

Only carriers appear. A sample carrying nothing in a bin is absent rather
than a row of zeros, which matters when a cohort has thousands of samples
and a bin has three carriers.

## Reading the result

**`alt_count` is what a burden test consumes**, not evidence of anything
by itself. Compare it across `sample_class`, with a test that belongs
outside this report.

**Rebinning changes the bins.** Two runs differing only in `maf_cutoff`
produce different bins and the result table does not say which variants
moved. So the mapping is a **second table** on the result, not a file
you have to remember to ask for:

```python
bins = bf.report.run("aggregate_cohort_variants", cohort_file="...",
                     output_grain="bins")

bins.table                            # one row per (bin, sample)
bins.extra_tables["variant_to_bin"]   # what each bin is made of
bins.save("runs/2026-09-17")          # both, plus the provenance
```

`variant_to_bin_path` still writes it as a CSV, for feeding something
that reads files.

**Check `provenance["warnings"]`.** This report proceeds through three
situations that can make its answer misleading — chromosomes the bundle
cannot place, a `maf_cutoff` below what the cohort can observe, and
samples with no phenotype. Each is logged when it happens and recorded
there, because whoever opens the result later does not have the log.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotate_disease.md ===== -->

# annotate_disease

What the bundle knows about a list of diseases, one row per input.

```bash
biofilter report run --report-name annotate_disease \
    --input MONDO:0007254 --input "breast cancer" \
    --output diseases.csv
```

## Input

MONDO ids, labels, synonyms, or cross-reference codes from any source the
bundle carries (`DOID:`, `EFO:`, `ICD10CM:`, …). Matching is
case-insensitive. `--input __ALL__` annotates every disease entity.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | diseases, or `__ALL__` |
| `include_relationships` | `true` | count relationships by related entity group |
| `include_xref_summary` | `true` | group cross-reference codes by source |
| `include_clingen_summary` | `true` | count ClinGen's gene assertions |
| `emit_not_found_rows` | `true` | keep inputs that resolved to nothing |

## Columns

| column | meaning |
| --- | --- |
| `input_value`, `input_matched_alias` | the input, and the alias it matched |
| `entity_id` | BF4 entity id — **scoped to this bundle** |
| `disease_id`, `disease_label`, `disease_description` | the MONDO record |
| `omic_status` | curation status |
| `disease_groups` | groups the disease belongs to, as a list |
| `disease_source_system`, `disease_data_source`, `disease_etl_package_id` | which build step produced the row |
| `xref_ids_by_source` | list of `{source, ids}` — every code, grouped by who issued it |
| `clingen_gene_count` | **distinct genes** ClinGen links to this disease |
| `clingen_relationship_count` | ClinGen assertions, which may exceed the gene count |
| `entity_relationships_by_group` | list of `{group_name, count}`, all sources, commonest first |
| `total_entity_relationships` | their sum |
| `other_aliases` | every other alias, minus the id and label above |
| `status`, `note` | `ok`, `partial` or `not_found`, and why |

## Reading the result

**The two ClinGen numbers differ on purpose.** `clingen_gene_count` counts
distinct genes; `clingen_relationship_count` counts assertions. One gene
supported by three lines of evidence is one gene and three assertions.
When the counts diverge, the disease has genes with more than one
assertion behind them.

**ClinGen's share is not the total.** `total_entity_relationships`
includes every source — MONDO's own hierarchy, Reactome, BioGRID. A
disease can have thousands of relationships and no ClinGen genes at all;
that means nobody has curated a gene–disease assertion for it, not that
it has no genetic basis.

**Null vs zero.** `clingen_gene_count` is null when the summary was
switched off and `0` when ClinGen has nothing for this disease.

In CSV the list columns are written as JSON. Parquet keeps them as lists.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotate_gene.md ===== -->

# annotate_gene

Everything the bundle knows about a list of genes, one row per input.

```bash
biofilter --bundle /path/to/bundle report run \
    --report-name annotate_gene \
    --input TP53 --input BRCA1 \
    --output genes.csv
```

The result is written as CSV with a `genes.csv.provenance.json` beside
it, naming the bundle the rows came from. That matters here because
`entity_id` is scoped to one build: the same integer means a different
gene in the next bundle.

## Input

Gene symbols, aliases, synonyms, or cross-reference codes (`HGNC:11998`,
`ENSG00000141510`, `7157`). Matching is case-insensitive.

`--input __ALL__` annotates every gene entity in the bundle instead.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | genes, via `--input`/`--input-file`, or `__ALL__` |
| `include_relationships` | `true` | count relationships by related entity group |
| `include_variant_summary` | `true` | count variants inside the gene's range |
| `emit_not_found_rows` | `true` | keep inputs that resolved to nothing |

```bash
--param include_variant_summary=false
```

## Columns

| column | meaning |
| --- | --- |
| `input_value` | the value as given |
| `input_matched_alias` | the alias it matched, which may differ in case or form |
| `entity_id` | BF4 entity id — **scoped to this bundle** |
| `gene_symbol` | HGNC symbol |
| `hgnc_id`, `ensembl_id`, `entrez_id` | cross-reference codes |
| `hgnc_status`, `omic_status` | curation status |
| `gene_locus_group`, `gene_locus_type` | HGNC classification |
| `gene_groups` | HGNC gene families, as a list |
| `build`, `chromosome`, `start_position`, `end_position` | build 38 coordinates |
| `entity_relationships_by_group` | list of `{group_name, count}`, commonest first |
| `total_entity_relationships` | their sum |
| `variant_count_in_gene_range` | variants between start and end |
| `other_aliases` | every other alias, minus the canonical ids above |
| `status` | `ok`, `partial`, or `not_found` |
| `note` | why, when it is not `ok` |

In CSV the list columns are written as JSON, so `gene_groups` reads as
`["p53 family"]`. Parquet keeps them as real lists.

## Reading the result

**`status`** is the first thing to look at.

- `ok` — resolved, with a gene record and build 38 coordinates.
- `partial` — resolved, but something is missing; `note` says what.
  Usually no build 38 location, which also makes
  `variant_count_in_gene_range` null.
- `not_found` — the bundle has no gene entity for this input. The row is
  kept on purpose: dropping it would leave no way to tell "absent from
  the bundle" from "never asked for".

**`variant_count_in_gene_range` null vs 0.** Null means the count was
not made — either the gene has no build 38 range, or
`include_variant_summary=false`. Zero means the range was searched and
held no variants. A bundle built for a subset of chromosomes returns 0
for every gene outside them, which is true of that bundle and not of the
genome.

**Relationships are counted in both directions.** A gene appearing as
either side of a relationship counts it, grouped by what is on the other
side. `Unknown` appears when the other entity's group is not recorded.

**Where an id is ambiguous**, the alias with the best rank wins —
primary, then symbol/preferred, then code, then synonym/name — and ties
break on the alias text, so the choice does not depend on row order.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotate_go.md ===== -->

# annotate_go

What the bundle knows about a list of Gene Ontology terms, one row per
input.

```bash
biofilter report run --report-name annotate_go \
    --input GO:0006915 --input GO:0008150 \
    --output go_terms.csv
```

## Input

GO ids (`GO:0006915`). `--input __ALL__` annotates every GO entity.

⚠️ **Terms resolve by id, not by name.** The bundle carries GO codes as
aliases but not term names, so `apoptotic process` does not resolve while
`GO:0006915` does. This matches the relational report it replaces; if
name lookup is wanted, it is an ETL change, not a report one.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | GO ids, or `__ALL__` |
| `include_relationships` | `true` | count relationships by related entity group |
| `include_go_relation_details` | `true` | list the neighbouring GO ids, not just count them |
| `max_go_terms_per_side` | `25` | cap on that list, per side |
| `emit_not_found_rows` | `true` | keep inputs that resolved to nothing |

## Columns

| column | meaning |
| --- | --- |
| `input_value`, `input_matched_alias` | the input, and the alias it matched |
| `entity_id` | BF4 entity id — **scoped to this bundle** |
| `go_id`, `go_name`, `go_namespace` | the term record |
| `go_source_system`, `go_data_source`, `go_etl_package_id` | which build step produced the row |
| `go_parent_count`, `go_child_count` | edges up and down the ontology |
| `go_parent_relation_types`, `go_child_relation_types` | `is_a`, `part_of`, … on each side |
| `go_parent_ids`, `go_child_ids` | the neighbouring terms, capped |
| `entity_relationships_by_group` | list of `{group_name, count}`, commonest first |
| `total_entity_relationships` | their sum |
| `other_aliases` | every other alias, minus the id and name above |
| `status`, `note` | `ok`, `partial` or `not_found`, and why |

## Reading the result

**Parents and children are ontology edges; relationships are not.**
`go_parent_count` and `go_child_count` describe where the term sits in
the GO hierarchy. `entity_relationships_by_group` describes what else in
the bundle is linked to it — genes annotated with the term, mostly. A
term can be deep in the ontology and annotate nothing.

**A namespace is a different ontology.** `biological_process`,
`molecular_function` and `cellular_component` do not share a root;
comparing counts across them compares different trees.

**The id lists are capped.** `max_go_terms_per_side` defaults to 25, so a
term near the root shows the first 25 children by id, not all of them.
The counts are never capped — trust those.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotate_pathway.md ===== -->

# annotate_pathway

What the bundle knows about a list of pathways, one row per input.

```bash
biofilter report run --report-name annotate_pathway \
    --input R-HSA-109581 --input hsa04210 \
    --output pathways.csv
```

## Input

Pathway ids — Reactome (`R-HSA-…`), KEGG (`hsa…`) — or any alias the
bundle carries. Matching is case-insensitive. `--input __ALL__` annotates
every pathway entity.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | pathways, or `__ALL__` |
| `include_relationships` | `true` | count relationships by related entity group |
| `emit_not_found_rows` | `true` | keep inputs that resolved to nothing |

## Columns

| column | meaning |
| --- | --- |
| `input_value`, `input_matched_alias` | the input, and the alias it matched |
| `entity_id` | BF4 entity id — **scoped to this bundle** |
| `pathway_id`, `pathway_description` | the pathway record |
| `pathway_source_system`, `pathway_data_source`, `pathway_etl_package_id` | which build step produced the row |
| `entity_relationships_by_group` | list of `{group_name, count}`, commonest first |
| `total_entity_relationships` | their sum |
| `other_aliases` | every other alias, minus the id and description above |
| `status`, `note` | `ok`, `partial` or `not_found`, and why |

## Reading the result

**`pathway_source_system` matters more here than elsewhere.** Reactome
and KEGG describe overlapping biology with different granularity and
different ids, and the bundle carries both. Two rows can be the same
pathway under two curations; nothing in this report merges them.

**Relationships are what make a pathway useful.** A pathway with a large
`Genes` count is one the bundle can expand into a gene set; one with none
is present as a label only.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotate_protein.md ===== -->

# annotate_protein

What the bundle knows about a list of proteins, one row per input.

```bash
biofilter report run --report-name annotate_protein \
    --input P04637 --input Q09472 \
    --output proteins.csv
```

## Input

UniProt accessions (`P04637`), isoform accessions (`P04637-2`), entry
names (`TP53_HUMAN`), or any alias the bundle carries. Matching is
case-insensitive. `--input __ALL__` annotates every protein entity.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | proteins, or `__ALL__` |
| `include_relationships` | `true` | count relationships by related entity group |
| `include_pfam_summary` | `true` | count and list Pfam domains by type |
| `emit_not_found_rows` | `true` | keep inputs that resolved to nothing |

## Columns

| column | meaning |
| --- | --- |
| `input_value`, `input_matched_alias` | the input, and the alias it matched |
| `entity_id` | the entity the input matched — **may be an isoform** |
| `canonical_entity_id` | the entity the annotation describes |
| `protein_id` | UniProt accession of the canonical protein |
| `input_is_isoform`, `input_isoform_accession` | whether the input named an isoform, and which |
| `isoform_count` | how many isoforms the protein has |
| `function`, `location`, `tissue_expression`, `pseudogene_note` | the UniProt record |
| `protein_source_system`, `protein_data_source`, `protein_etl_package_id` | which build step produced the row |
| `pfam_total_count` | distinct Pfam accessions on the protein |
| `pfam_count_by_type` | list of `{type, count}` — Domain, Family, Repeat, … |
| `pfam_ids_by_type` | list of `{type, ids}` — the accessions themselves |
| `entity_relationships_by_group` | list of `{group_name, count}`, commonest first |
| `total_entity_relationships` | their sum |
| `other_aliases` | every other alias, minus the accession above |
| `status`, `note` | `ok`, `partial` or `not_found`, and why |

## Reading the result

**Two entity ids, and they can differ.** A protein with isoforms has an
entity per isoform as well as one for the canonical sequence. An input
naming an isoform resolves to the isoform's entity — that is `entity_id`
— but the function, domains and relationships being reported belong to
the protein, whose entity is `canonical_entity_id`. The `note` says so
when they differ.

This is deliberate: an isoform entity carries almost nothing on its own,
so reporting its zero relationships would be technically true and
practically useless.

**`isoform_count` is isoforms, not entities.** A protein with
`isoform_count = 3` has four entities: three isoforms and the canonical.

**Pfam counts are distinct accessions.** A domain appearing twice in a
sequence is one accession. `pfam_total_count` is the sum across types, so
it matches the total of `pfam_count_by_type`.

In CSV the list columns are written as JSON. Parquet keeps them as lists.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotate_variant.md ===== -->

# annotate_variant

What the bundle knows about a list of variants.

```bash
biofilter report run --report-name annotate_variant \
    --input rs1225039379 --input 22:20052518:C:T \
    --param most_severe_only=true \
    --output variants.csv
```

> Called `annotation_master_variant` before 4.3.0. The version it
> replaces could not run against a 4.3.0 bundle at all — it selected
> `variant_masters.variant_id`, `position_start` and the predictor
> columns, none of which the current schema has.

## Input

Three shapes, mixed freely:

| shape | example | matches |
| --- | --- | --- |
| rsID | `rs1225039379` | the variant that rsID maps to |
| chr:pos | `22:15238761` | **every** variant at that position |
| chr:pos:ref:alt | `22:20052518:C:T` | exactly that variant |

`chr`, `CHR`, `chromosome` prefixes and `:`, `-`, `_`, space separators
are all accepted. `X`, `Y` and `MT` map to 23, 24 and 25.

## One row per transcript

A variant is annotated against every transcript it overlaps — often
dozens. `22:20052518:C:T` returns **89 rows**, one per transcript, with
the variant-level facts (frequencies, CADD, REVEL) repeated on each.

Two ways to narrow it:

| parameter | keeps |
| --- | --- |
| `most_severe_only` | the transcript with the worst consequence |
| `canonical_only` | transcripts VEP marked canonical |

They are different questions and can disagree — the most severe
consequence is not always on the canonical transcript.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | rsIDs, positions, or full variant ids |
| `most_severe_only` | `false` | one row per variant, worst consequence |
| `canonical_only` | `false` | canonical transcripts only |
| `emit_not_found_rows` | `true` | keep inputs that matched nothing |

## Reading the result

**`status` tells you what happened to the input.**

| value | meaning |
| --- | --- |
| `ok` | matched a variant |
| `not_found` | parsed fine, no such variant in this bundle |
| `invalid` | could not be parsed; `note` says what was expected |

**`is_most_severe_for_variant` is derived, not stored.** 4.3.0 dropped
that flag from the schema, so it is computed from
`variant_consequences.severity_rank` — which keeps it consistent with
whatever severity ordering the bundle actually carries, rather than with
whatever the ETL believed when it wrote the row.

**A bundle built for a subset of chromosomes returns `not_found` for
everything outside it.** That is true of the bundle, not of the genome.

You do not have to remember to check: the `.provenance.json` written
beside every result records it.

```json
"coverage": {
  "optional_tables_absent": ["variant_predictions", "variant_alphamissense"],
  "chromosomes": [22]
}
```

An empty `optional_tables_absent` means every source this report can use
was present. A non-empty one names the column groups that came back null
for want of data rather than for want of an answer.

**rsIDs come from `variant_rsid`, not from `variant_masters.rsid`.** That
column exists and is entirely null in 4.3.0 bundles — 0 of 2,889,803 rows
in the chr22 build. Roughly 97% of variants have an rsID in the mapping
table.

**AlphaMissense scores one transcript per variant.** Of 89 transcripts
for `22:20052518:C:T`, one carries a score. That is AlphaMissense's own
scope, not a join failure — it predicts on the canonical protein
sequence.

## Column groups

| group | columns |
| --- | --- |
| input | `input_value`, `input_kind`, `status`, `note` |
| identity | `variant_key`, `rsid`, `chromosome`, `position`, alleles, `quality_filter` |
| frequency | `ac_joint`, `an_joint`, `af_joint`, `nhomalt_joint`, `grpmax_joint`, `af_grpmax_joint` |
| prediction | `cadd_phred`, `cadd_raw_score`, `revel_max`, `sift_max`, `polyphen_max`, `spliceai_ds_max`, `pangolin_largest_ds`, `phylop` |
| molecular effect | gene, transcript, `consequence` and its group/category/rank, `impact`, `canonical`, `mane_select`, HGVS, `lof` |
| AlphaMissense | `alphamissense_score`, `alphamissense_classification` |

Frequencies are the gnomAD **joint** callset. The exomes and genomes
columns exist in `variant_masters` and are not surfaced here; ask for
them directly if you need the split.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_expand_entity_neighborhood.md ===== -->

# expand_entity_neighborhood

What sits one hop from each of these entities.

```bash
biofilter report run --report-name expand_entity_neighborhood \
    --input gene:BRCA1 --input "disease:breast cancer" --input APOE \
    --output neighbourhood.csv
```

> Called `entity_neighborhood_summary` before 4.3.0.

## Input

A heterogeneous list. Genes, diseases, proteins, pathways and GO terms
can be mixed freely, and each item may carry a type hint:

```
gene:BRCA1              restrict the search to Genes
disease:breast cancer   restrict it to Diseases
APOE                    search every group
GO:0006915              a GO identifier, not a hint — see below
```

A prefix is a hint **only when it names an entity group**. The group
names come from the bundle itself, plus convenient spellings
(`gene`, `protein`, `go_terms`, …). Anything else stays part of the
term, which is what keeps `MONDO:0007254`, `HGNC:11998` and `DOID:1612`
intact.

`go:` is deliberately **not** a hint, because it is the prefix of every
GO identifier. Use `go_terms:` if you need to restrict to that group.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | the list, with or without hints |
| `match_mode` | `exact` | `exact`, `like`, or `fuzzy` |
| `similarity_threshold` | `80` | fuzzy only, 0–100 |
| `neighbors_top_n_per_type` | `50` | cap the named neighbours per kind |
| `aliases_top_n` | `20` | cap the alias sample |
| `emit_not_found_rows` | `true` | keep inputs that resolved to nothing |

## Columns

| column | meaning |
| --- | --- |
| `input_value` | the item as you wrote it, hint included |
| `input_type_hint` | the hint, if the prefix was one |
| `match_mode` | how it was matched |
| `entity_id` | BF4 entity id — **scoped to this bundle** |
| `entity_group` | which kind it turned out to be |
| `matched_alias`, `primary_name` | what matched, and what it is called |
| `similarity_score` | fuzzy only; null otherwise |
| `alias_count`, `aliases_top` | how many names it has, and a sample |
| `degree_total` | neighbours in one hop, both directions |
| `neighbors_by_type` | list of `{group_name, count, names}`, commonest first |
| `status`, `note` | `ok` or `not_found`, and why |

## Reading the result

**`degree_total = 0` with `status = 'ok'` is a real answer.** The entity
exists and nothing in this bundle is linked to it. GO terms are the usual
case: they carry no entity relationships at all here, so a GO input
resolves cleanly and comes back with an empty neighbourhood. The `note`
says so rather than leaving you to wonder.

**`neighbors_by_type` is capped; `degree_total` and `count` are not.** A
gene linked to 2,511 other genes reports that count, and the first 50
names.

**Names, not ids.** A neighbour is named by its primary alias, falling
back to `ENTITY:<id>` when it has none.

## What changed in the migration

**Identifier prefixes are no longer eaten.** The relational version
treated *any* prefix before a colon as a type hint, so `GO:0006915`
became the term `0006915` — which resolved to a **disease**. Every CURIE
in the bundle was affected, and none of it failed loudly.

**The schema no longer depends on the bundle.** That version added one
column per entity group present in the data: 29 columns against a real
bundle, 14 of them impossible to know before running it, each holding a
JSON string. It is one nested column now, and `available_columns()` tells
the whole truth.

**Group names are read from the bundle.** The old map pointed `go` at a
group called `GO Terms`, which no 4.3.0 bundle has.

**Column names are snake_case**, like every other report;
`"Degree Total (1-hop)"` is now `degree_total`.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_expand_entity_relationship.md ===== -->

# expand_entity_relationship

Which links the bundle holds for these entities.

```bash
biofilter report run --report-name expand_entity_relationship \
    --input TP53 --input BRCA1 \
    --param output_entity_groups=Pathways \
    --output relationships.csv
```

> Called `entity_relationship_model` before 4.3.0. It does not model
> anything — it expands entities into the relationship rows they take
> part in.

## One row per (input, relationship)

A relationship with an input on **both** sides is reached from each, so
it produces two rows differing in `match_side` and `direction`. That is
usually what you want in `input_to_any` and rarely what you want in
`between_inputs`, which is why `deduplicate_pairs` defaults differently
per scope.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | names, symbols or codes |
| `relationship_scope` | `input_to_any` | `input_to_any`, or `between_inputs` |
| `input_entity_groups` | none | restrict which groups an input may resolve to |
| `output_entity_groups` | none | keep only links whose far side is in these groups |
| `relationship_types` | none | keep only these relationship type codes |
| `deduplicate_pairs` | per scope | collapse the two anchors of one relationship |
| `emit_not_found_rows` | `true` | keep inputs that produced nothing |

### Scope

| scope | keeps |
| --- | --- |
| `input_to_any` | every link where an input appears on either side |
| `between_inputs` | only links where **both** sides are inputs |

`between_inputs` is how you ask "how are these things connected to each
other", as opposed to "what is each of them connected to".

## Columns

| column | meaning |
| --- | --- |
| `input_original`, `input_matched_alias` | the input, and the alias it matched |
| `input_entity_id`, `input_primary_name`, `input_group_name` | the entity it resolved to |
| `match_side` | `entity_1` or `entity_2` — which side the input sits on |
| `direction` | `input->related` or `related->input` |
| `relationship_id`, `relationship_type`, `relationship_description` | the link |
| `related_entity_id`, `related_primary_name`, `related_group_name` | the far side |
| `entity_1_id`, `entity_1_primary_name` | the link's own left side |
| `entity_2_id`, `entity_2_primary_name` | and its right side |
| `data_source_id`, `etl_package_id` | which build step produced the link |
| `observation` | empty, `not found`, or `no relationships in scope` |

## Reading the result

**Three kinds of row, told apart by `observation`.**

| value | meaning |
| --- | --- |
| *(empty)* | a real relationship |
| `not found` | the bundle has no entity for this input |
| `no relationships in scope` | the input resolved, and nothing came back for it |

The third is new. The relational version emitted rows only for inputs it
could not resolve, so a gene with five thousand relationships and none to
`Chemicals` simply vanished from a chemicals-filtered result —
indistinguishable from one that was never asked about.

**`match_side` and `direction` say the same thing twice, on purpose.**
`match_side` is structural — which column of `entity_relationships` your
input occupies. `direction` is how to read the row. Relationships in BF4
are not all symmetric, and which side an entity sits on can carry
meaning.

**`entity_1_*` and `entity_2_*` are the link's own ends**, not "input"
and "related". When the input is `entity_2`, the related entity is
`entity_1`. The `related_*` columns save you working that out.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_expand_gene_to_variant.md ===== -->

# expand_gene_to_variant

The variants belonging to a list of genes, with their annotation.

```bash
biofilter report run --report-name expand_gene_to_variant \
    --input CHEK2 --input SMARCB1 \
    --param mapping=position \
    --param window_bp=5000 \
    --param af_max=0.01 \
    --output gene_variants.csv
```

> Replaces `gene_to_variant_filtering`, and retires
> `variant_annotation_expanded` with it. That second report was this one
> with no filters and a gene list scraped out of another report's CSV by
> column name; chaining reports is now the caller's job, done with files
> they can see.

## You have to choose what "belongs to" means

This is the one parameter with no safe default, so the report asks for a
decision rather than making one quietly.

| `mapping` | a variant belongs to a gene when… |
| --- | --- |
| `position` | its coordinate falls inside the gene's build-38 range |
| `annotation` | VEP associated it with that gene |

They are not the same question and they do not return the same variants.
Measured across all **958 chr22 genes** in the current bundle that have
build-38 coordinates and annotated variants:

- **2,045,943** gene-variant pairs by position
- **2,651,135** by annotation
- **144,488** pairs exist *only* by position — 139 genes have at least one
- roughly **749,680** exist *only* by annotation

Position finds variants that VEP attributed to a neighbouring gene, or to
no gene at all. Annotation finds variants outside the gene body — VEP
reaches about 5 kb beyond it — and follows VEP's gene model rather than
the coordinates BF4 stores.

Because a single gene often shows one mapping as a clean subset of the
other, you cannot tell from the rows which question was asked. So the
report records it in two places: a `mapping` column on every row, and the
`mapping` block of the provenance JSON.

```json
"mapping": {
  "mechanism": "position",
  "build": 38,
  "window_bp": 5000,
  "means": "variant coordinate inside the gene's range"
}
```

`window_bp` widens the range in both directions and applies to
`position` only. Passing it with `mapping=annotation` is an error rather
than a no-op: VEP's association already reaches beyond the gene body, and
silently ignoring the window would let you believe it had applied.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | gene symbols, HGNC or Ensembl ids — anything `resolve_entity` accepts |
| `mapping` | `position` | `position` or `annotation`; see above |
| `build` | `38` | genome build, for `position` |
| `window_bp` | `0` | widen the gene's range both ways, for `position` |
| `most_severe_only` | `true` | one row per gene and variant, keeping the worst consequence |
| `max_variants_per_gene` | `5000` | cap per gene; `0` means no cap |
| `emit_not_found_rows` | `true` | keep inputs that produced nothing |
| `af_min`, `af_max` | none | joint allele frequency bounds |
| `impact_filter` | none | `HIGH`, `MODERATE`, `LOW`, `MODIFIER` |
| `consequence_type_filter` | none | VEP consequence names |
| `lof_confidence_filter` | none | LOFTEE `HC`, `LC` |
| `cadd_phred_min` | none | |
| `sift_score_max` | none | |
| `polyphen_score_min` | none | |
| `alphamissense_score_min` | none | |
| `alphamissense_classification` | none | `likely_pathogenic`, `likely_benign`, `ambiguous` |

## The cap is real, and it says so

`max_variants_per_gene` defaults to 5000 because a single gene on a whole
-genome bundle can carry hundreds of thousands of variants. A capped gene
looks exactly like a complete answer — a round number of rows, and
nothing in them admitting more existed.

So the provenance says what was hidden, per gene:

```json
"truncation": {
  "max_variants_per_gene": 200,
  "applied": true,
  "genes": {
    "CHEK2":   {"returned": 200, "available": 3796},
    "SMARCB1": {"returned": 200, "available": 4260}
  },
  "means": "These genes had more variants than the cap allowed; …"
}
```

The rows kept are the most severe first, then the most common. Pass
`max_variants_per_gene=0` for no cap at all.

## Columns

| column | meaning |
| --- | --- |
| `input_gene` | what you asked for, verbatim |
| `gene_symbol`, `gene_entity_id` | what it resolved to |
| `mapping` | `position` or `annotation` — which question this row answers |
| `status`, `note` | `ok`, `not_found`, `no_location`, `no_variants` |
| `variant_key`, `rsid`, `chromosome`, `position`, alleles | the variant |
| `af_joint`, `ac_joint` | gnomAD joint frequency and count |
| `transcript_id`, `consequence`, `consequence_group`, `severity_rank` | the annotation |
| `impact`, `canonical`, `mane_select`, `lof` | VEP flags |
| `cadd_phred`, `revel_max`, `sift_max`, `polyphen_max` | in-silico predictors |
| `alphamissense_score`, `alphamissense_classification` | AlphaMissense |
| `variants_available` | the gene's pre-cap total, so a capped row admits it |

## Reading the result

**Four statuses, and the difference between two of them matters.**

| status | means |
| --- | --- |
| `ok` | a variant |
| `not_found` | the input did not resolve to a gene in this bundle |
| `no_location` | the gene resolved, but the bundle has no build-38 coordinates for it, so `position` cannot place it — try `annotation` |
| `no_variants` | the gene resolved and, under the mapping you chose, nothing met the criteria |

`no_location` exists because a gene the bundle cannot place is not a gene
without variants, and under `position` mapping the two are otherwise
indistinguishable. This is not a rare corner: **33,354 of the 72,660
genes in the current bundle — 45.9% — carry no build-38 coordinates**, so
`position` mapping cannot place almost half the gene catalogue. For those
genes `annotation` is the only mapping that answers anything.

**`most_severe_only` changes what a row counts.** With it on (the
default) one row is a gene-variant pair, and the consequence shown is the
worst across transcripts. With it off one row is a gene-variant-transcript
triple, and counting rows counts transcripts.

**A missing predictor is a missing column, not a zero.** If the bundle
carries no `variant_predictions` or `variant_alphamissense` table, those
columns come back null for every row and the omission is recorded in the
provenance `coverage` block. Check it before concluding that nothing
scored.

**AlphaMissense needs two corrections to join at all, and both failures
are silent nulls.**

*The version.* AlphaMissense writes `ENST00000327374.9`; VEP writes
`ENST00000327374`. Joining the raw strings matches nothing. The report
strips the version on both sides.

*The transcript.* AlphaMissense scores a variant **on a transcript**, and
the transcript it picked is rarely the one VEP calls most severe.
Requiring the two to agree threw away 97% of the scores this bundle
holds: of 5,917 CHEK2 missense annotations, only 222 matched on
transcript, and after collapsing to one row per variant just 6 survived —
while 249 of the gene's 308 missense variants have a score somewhere.

So the join key follows the grain of the row:

| `most_severe_only` | a row is | `alphamissense_score` is |
| --- | --- | --- |
| `true` (default) | one variant | the highest score recorded for that variant |
| `false` | one transcript | that transcript's score, or null |

The provenance `alphamissense` block says which applied. Under the
default, all 174 rare damaging missense variants in CHEK2 come back
scored instead of 6.

## Chaining

There is none, by design. To feed these variants into another report,
write the CSV and read the column you want:

```python
genes = bf.report.run("expand_gene_to_variant",
                      input_data=["CHEK2"], mapping="position")
variants = genes.to_pandas().query("status == 'ok'").variant_key.tolist()

bf.report.run("expand_variant_regulatory", input_data=variants)
```

The intermediate list is a thing you can look at, which is the point.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_expand_variant_regulatory.md ===== -->

# expand_variant_regulatory

Which genes a variant regulates, in which tissue, with what effect.

```bash
biofilter report run --report-name expand_variant_regulatory \
    --input APOE --input rs429358 \
    --param p_value_max=1e-8 \
    --output regulatory.csv
```

> Called `annotation_variant_regulatory_evidence` before 4.3.0. Renamed
> because one row is a link from the input to something reached —
> expansion, not annotation.

## Why this is not `annotate_variant`

`annotate_variant` reports the gene a variant sits **in**, from VEP. This
reports the gene it **regulates**, from eQTL. They are usually different
genes, and the difference is the point.

Measured on chr22 of the current bundle:

- 6,919,646 variant × gene pairs carry both kinds of evidence
- **88.5% name a different gene** — the variant is in one gene and
  regulates another
- **31,080 variants with regulatory evidence** are classed by VEP as
  `intergenic`, `upstream` or `downstream` — outside any gene at all

For those last ones `annotate_variant` says "not in a gene" while the
eQTL says "regulates this gene, in this tissue, with this effect".

## Input

Three shapes, mixed freely in one list:

| shape | example | means |
| --- | --- | --- |
| gene symbol | `APOE` | every variant inside that gene's range |
| rsID | `rs429358` | that variant |
| position | `19:44908684` or `19:44908684:T:C` | that position, or that exact variant |

Anything that is not an rsID or a position is read as a gene name.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | genes, rsIDs, positions |
| `tissues` | none | keep only these `bio_context` values |
| `qtl_type` | none | `eQTL`, `sQTL`, … |
| `p_value_max` | none | keep only evidence at least this significant |
| `flanking_bp` | `0` | widen a gene's range, for gene inputs |
| `emit_not_found_rows` | `true` | keep inputs with no evidence |

## Columns

| column | meaning |
| --- | --- |
| `input_value`, `input_kind`, `status`, `note` | what was asked, and what happened |
| `variant_key`, `rsid`, `chromosome`, `position`, alleles | the variant |
| `position_gene_symbol`, `position_gene_id` | the gene whose **body contains** it |
| `regulated_gene_id`, `regulated_gene_symbol` | the gene it **regulates** |
| `bio_context` | the tissue |
| `qtl_type` | `eQTL`, `sQTL` |
| `beta`, `se`, `p_value`, `n`, `effect_allele` | the association |

## Reading the result

**Which tissues a bundle has is a build decision, not a property of this
report.** GTEx ships 50; the DTP config carries a load flag per tissue,
and the current build loads 13 — all brain. Nothing here is hardcoded to
that: ask for the tissues you want, or look at what came back. The result
covers whatever the bundle was built with.

⚠️ So absence of evidence in this report is absence **in the tissues this
bundle carries**. Check `bio_context` before reading a null as biology.

**`regulated_gene_symbol` can be null while the evidence is real.** GTEx
names its target by Ensembl gene id, and roughly 17% of those on chr22
have no BF4 entity — lncRNAs and pseudogenes without HGNC symbols.
`regulated_gene_id` is always there; only the symbol is missing.

**`position_gene_symbol` can be null too**, when the variant falls
outside every gene body. That is common and expected — it is exactly the
case this report exists for.

**Gene input means "variants inside this gene's range"**, resolved from
build 38 coordinates in `entity_locations`. `flanking_bp` widens it for
promoter and downstream regions.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_pair_genes.md ===== -->

# pair_genes

Which of these genes are related, and by what — and optionally, what that
implies about a list of your own.

```bash
biofilter report run --report-name pair_genes \
    --input CHEK2 --input SMARCB1 --input NF2 \
    --param group_types=Pathways \
    --param max_group_size=300 \
    --output gene_pairs.csv
```

## Two stages

1. **Connect.** Seed genes reach partner genes through a shared entity: a
   pathway, a disease, a protein. That entity is the **group**, and how
   many distinct groups link a pair is the pair's support.
2. **Expand**, only when you ask. Given a gene → item mapping, every gene
   pair becomes the item pairs it implies.

Stage 2 is optional. Without a mapping this report answers a question
that stands on its own: which of my genes are related, and by what.

## It never touches variants

No overlap, no window, none of the machinery `pair_variants` uses to go
from a variant to a gene and back. This report takes **genes**, and for
the expansion it takes the link **you** supplied.

That is the point, not a limitation. `pair_variants` derives "this
variant belongs to this gene" from coordinates, which is right when the
variant is coding and wrong when it is regulatory — a variant sits in one
gene and acts on another.

Measured on the current bundle across all 11,532,453 variant × gene links
that carry both kinds of evidence, **91.5% name a gene other than the one
the variant sits in**. On the narrower set of one analysis's non-coding
inputs it was 72.5% (ADR-005 §1.1). Either way the exception is the rule.

When your evidence for the attachment comes from outside Biofilter — a
colocalization, a fine-mapping, a curated list — this is the report that
will use it instead of re-deriving it.

A variant passed as `input_data` resolves to nothing, because a variant
is not a gene. Variants enter only through the mapping.

## The mapping: two columns, gene then item

As a file:

```
APOE	111
APOE	222
APOE	333
APOA	444
APOA	555
```

Or inline, for something small:

```python
bf.report.run("pair_genes", input_data=["APOE", "APOA"],
              mapping={"APOE": ["111", "222", "333"], "APOA": ["444", "555"]})
```

Many-to-many in both directions: a gene may carry any number of items,
and an item may belong to any number of genes.

With the gene pair APOE-APOA found through a pathway, the expansion is
the cross product of the two sides — 3 × 2 = **six** item pairs:

```
111-444   111-555
222-444   222-555
333-444   333-555
```

Use `mapping_file` for anything a shell will not carry through `--param`.

## The item is opaque, and that is the design

The report **never reads the item**. It is a label carried from one side
of a pair to the other.

That is what makes gene→position, gene→rsID, gene→probe and
gene→exposure one feature rather than four. **Biofilter is build 38
throughout, and managing build is yours** — but because nothing here
interprets a coordinate, a caller pairing build-37 positions gets
build-37 positions out, correctly, and Biofilter never has to know what a
build-37 position is.

The price is stated rather than hidden:

- no filtering by allele frequency, no resolving an rsID, no validating
  anything about the item;
- **two spellings of one thing are two things.** `22:100:A:G` and
  `chr22:100:A:G` are different items and nothing here can tell
  otherwise, which bounds what the deduplication below can promise.

Correctness of the items is yours. Reports that *do* interpret variants
already exist — `annotate_variant`, `expand_gene_to_variant` — and this
is not one of them.

## Three rules the simple example does not show

The cross product is not the work. These are, and they are the same for
every caller, which is why they live here rather than in each analysis:

| rule | why |
| --- | --- |
| a pair is unordered | testing (X, Y) is testing (Y, X) |
| deduplicate **globally** | the same item pair arrives through every gene pair that links it |
| drop self-pairs | an item on both genes of a pair would pair with itself |

The second is the one that bites. Deduplication is across the whole
answer, not per gene pair: if APOE-APOA and APOC-APOA are both pairs and
`111` is on both APOE and APOC, then `111-444` arrives twice. On one real
run that was **4.3%** of the answer — 72,554 pairs against the 75,794 a
naive `sum(n1 × n2)` would report, from 83 items attached to two genes
each. A count inflated by 4.3% survives review because it looks
plausible.

**Both genes must carry at least one item.** Not "both were named" — a
gene can be in your input and carry nothing, and then there is nothing on
its side to pair.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | gene names, symbols or ids — anything `resolve_entity` accepts |
| `membership` | `both` | `both`: both genes from your input. `either`: one from input, the other any gene it reaches |
| `group_types` | `["Pathways"]` | `Pathways`, `Diseases`, `Proteins`, `Genes` |
| `max_group_size` | `300` | drop groups reaching more genes than this; `0` for no limit |
| `min_group_support` | `1` | require this many distinct groups behind a pair |
| `min_group_sources` | `1` | require this many distinct **curations** |
| `max_pairs` | `1000000` | cap on rows returned |
| `mapping` | none | inline gene → item mapping |
| `mapping_file` | none | the same as a two-column file |

`membership="either"` is **refused** with a mapping. The partner gene
came from the bundle, not from your list, so there is nothing on that
side to pair — half a pair is not a pair, and silently returning one
would be worse than the error.

## `max_group_size` decides the size *and* the meaning

A pathway naming 2,615 genes links its members while saying almost
nothing about any of them. Measured on one real run:

| `max_group_size` | gene pairs | item pairs |
| --- | ---: | ---: |
| no limit | 56,611 | 375,720 |
| 500 | 12,823 | 72,554 |
| **300 (default)** | 9,665 | 53,413 |
| 200 | 6,020 | 37,059 |

Removing the cap multiplies the answer fivefold with groups that assert
almost nothing. When a result is empty or thin, this is usually why, and
the provenance says so:

```json
"group_filter": {
  "max_group_size": 300,
  "groups_touching_input": 67,
  "groups_kept": 50,
  "groups_excluded_by_size": 17,
  "smallest_excluded": 344
}
```

## Support, and why the source is separate

`group_support_count` counts the **groups** linking two genes.
`group_support_source_count` counts the **curations** that asserted them,
read from the bundle rather than guessed from an accession prefix.

Two curations agreeing is not the same as one curation saying it twice,
so `min_group_sources` is a stronger claim than `min_group_support`.

## Columns

Without a mapping, one row is a gene pair:

| column | meaning |
| --- | --- |
| `gene_1_id`, `gene_1_symbol`, `gene_1_from_input` | one side |
| `gene_2_id`, `gene_2_symbol`, `gene_2_from_input` | the other |
| `group_support_count` | how many distinct groups link them |
| `group_support_source_count` | how many distinct curations |
| `group_support_types` | `Pathways`, `Diseases`, … |
| `group_support_names` | the groups themselves |
| `group_support_sources` | the curations: `reactome_relationships`, `kegg_relationships`, `biogrid` … |
| `membership` | which mode produced this row |

With a mapping, one row is an **item pair**, and that becomes the primary
table:

| column | meaning |
| --- | --- |
| `item_1`, `item_2` | your items, unordered |
| `gene_1_*`, `gene_2_*` | the gene pair that implied them |
| `group_support_*` | that pair's support |

The gene pairs are still there, as `extra_tables["gene_pairs"]`:

```python
result = bf.report.run("pair_genes", input_data=[...], mapping_file="map.tsv")

result.table                            # item pairs — what you asked for
result.extra_tables["gene_pairs"]       # the pairs they came from
result.save("runs/2026-09-17")          # both, plus the provenance
```

The item pairs are primary because `write()` exports only the primary
table. Making gene pairs primary would hand `result.write("pairs.csv")`
the wrong grain with no error at all.

`available_columns()` reports the gene-pair columns, since that is what
the report returns when asked nothing special. It is a classmethod and
cannot see your parameters, so it cannot describe both shapes.

## Reading the result

**`group_support_count` is a weight for ranking, not a p-value.** It
counts groups under the size limit you chose; change `max_group_size` and
every count changes with it.

**A pair is always across two distinct genes**, and never appears in both
orientations.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_pair_variants.md ===== -->

# pair_variants

Candidate variant × variant pairs whose genes share biology.

```bash
biofilter report run --report-name pair_variants \
    --input CHEK2 --input SMARCB1 --input NF2 \
    --param group_types=Pathways \
    --param max_group_size=300 \
    --param membership=both \
    --output pairs.csv
```

> Replaces six reports that were one pipeline written in three eras:
> `variant_gene_location_model` (stage 1 alone),
> `variant_single_gene_annotation` (stages 1-2), `snp_snp_model` and
> `variant_modeling` (all three, differing only in whether both sides had
> to come from the input). The remaining two, `variant_list_intersect`
> and `snp_snp_pair_generator`, never read a bundle at all and are
> handled separately.

## The three stages

1. **Place.** Each input is read as a gene name or as a variant (rsID,
   `chr:pos`, `chr:pos:ref:alt`) — mixed freely in one list. A variant is
   placed on the genes whose build-38 range contains it, widened by
   `window_bp` if you pass one.
2. **Connect.** Seed genes reach partner genes through a shared entity: a
   pathway, a disease, a protein. That entity is the **group**, and how
   many distinct groups link a pair is the pair's `group_support_count`.
3. **Pair.** Gene pairs become variant pairs.

Gene pairs alone are a question of their own, and have a report of their
own: **`pair_genes`**. It also expands a gene pair by a list you supply,
which is what to use when the variant to gene attachment comes from
outside the bundle — a colocalization, a fine-mapping, a curated list.

`output_grain` is gone from here. Two ways to reach one answer is what
the six-report consolidation removed, and keeping it would repeat the
mistake at a smaller scale.

## `max_group_size` is the parameter that matters

It is not a performance knob with a quality side effect — it is the other
way round. A pathway naming 2,615 genes, or a protein interacting with
5,338, links its members to each other while saying almost nothing about
any of them.

Measured on the current bundle, groups generate this many gene pairs:

| `max_group_size` | Pathways | Proteins | Diseases |
| --- | --- | --- | --- |
| 100 | 1.6 M (86% of groups kept) | 11.6 M (72%) | 14.9 K (100%) |
| **300 (default)** | **6.9 M (98%)** | 76.9 M (93%) | 56.2 K (100%) |
| no limit | 37.1 M | 469.8 M | 56.2 K |

300 keeps 98% of pathways while cutting the pairs they generate more than
fivefold, and never touches diseases — the largest disease in the bundle
names 232 genes. It is also the difference between 0.27 s and 30 s on a
300-variant input against protein groups.

**When the result is empty, this is usually why**, so the provenance says
what the filter removed:

```json
"group_filter": {
  "max_group_size": 300,
  "groups_touching_input": 67,
  "groups_kept": 50,
  "groups_excluded_by_size": 17,
  "smallest_excluded": 344,
  "means": "17 of the 67 groups that reach these genes name more than 300 …"
}
```

Asking for `CHEK2`, `SMARCB1` and `NF2` through pathways returns nothing
at the default. That is correct: the only pathways linking them have
1,231, 1,321 and 1,543 genes. Without the provenance block it would read
as "these genes share no biology".

## `membership`: one side from the input, or both

| value | means |
| --- | --- |
| `both` (default) | both variants come from the input |
| `either` | one comes from the input; the other is any variant in a gene the input reaches |

This is the only difference there ever was between `variant_modeling`
(`both`) and `snp_snp_model` (`either`). It is not a cosmetic filter:
`either` is unbounded in a way `both` is not. Five seed genes reach
19,393 partner genes through proteins.

**Naming a gene and naming a variant are different requests.** Naming
`TP53` asks for its variants. Naming `rs1042522` asks for that variant,
not for the other 4,000 in the gene that contains it. `variant_1_from_input`
and `variant_2_from_input` report which is which.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | gene names and/or variants, mixed |
| `membership` | `both` | see above |
| `group_types` | `["Pathways"]` | `Pathways`, `Diseases`, `Proteins`, `Genes` |
| `max_group_size` | `300` | drop groups reaching more genes than this; `0` for no limit |
| `min_group_support` | `1` | require this many distinct groups behind a pair |
| `max_variants_per_gene` | `100` | how many variants each gene contributes; `0` for all |
| `max_pairs` | `1000000` | cap on rows returned |
| `window_bp` | `0` | widen a gene's range when placing a variant |
| `build` | `38` | genome build |
| `af_min`, `af_max` | none | joint allele frequency bounds |

## Why `max_variants_per_gene` exists

Pairs grow with the **square** of the variants per gene. A gene on chr22
carries about 4,000 variants; three such genes paired in full are 48
million rows, which is not an answer anybody reads — and the query runs
out of memory before it gets there.

The variants kept are the **most common**, because a pairwise interaction
test has no power on a rare variant. A variant you named by hand is never
dropped. Both facts are recorded in the provenance `pairing` block.

## Gene Ontology is not offered

The bundle carries 38,092 GO entities and **zero** GO relationships, so a
GO group cannot link two genes. The legacy reports accepted
`group_entity_type='GO'` and returned nothing, which reads as a finding.
Here a group type the bundle cannot use is refused by name.

## Columns

| column | meaning |
| --- | --- |
| `input_1`, `input_2` | what you typed, when you typed this variant or its gene |
| `variant_1_key`, `variant_1_rsid`, `variant_1_chromosome`, `variant_1_position` | side 1 |
| `gene_1_id`, `gene_1_symbol` | the gene side 1 sits in |
| `variant_1_from_input` | whether side 1 came from your list |
| `variant_2_*`, `gene_2_*` | the same for side 2 |
| `group_support_count` | how many distinct groups link the two genes |
| `group_support_types` | `Pathways`, `Diseases`, … |
| `group_support_names` | the groups themselves |
| `membership` | which mode produced this row |

## Reading the result

**A pair is always across two distinct genes**, and never appears in both
orientations. Two variants in the same gene are not a candidate here.

**`group_support_count` is a weight, not a p-value.** It counts the
groups linking the two genes under the size limit you chose. Change
`max_group_size` and every count changes with it.

**Hitting `max_pairs` is the normal case, not the exception**, because
pair counts grow quadratically. The provenance `truncation` block says
whether it happened; the rows kept are those with the most support.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_platform_data_statistics.md ===== -->

# platform_data_statistics

What this bundle holds: how much, of what, and how big.

```bash
biofilter report run --report-name platform_data_statistics --output stats.csv
```

> Called the same thing before 4.3.0. A platform report — it describes
> the bundle, not the biology in it, and takes no input beyond which
> sections to compute.

## One row per measurement

Heterogeneous statistics do not fit a wide table, so this one is long:

| column | meaning |
| --- | --- |
| `section` | which group of measurements |
| `metric` | what is being measured |
| `dimension_1`, `dimension_2` | what it is measured *by* |
| `value_number` | the number, when there is one |
| `value_text` | the value, when it is not a number |
| `as_of` | when the measured thing happened, not when the report ran |
| `note` | anything that needs saying about the row |

A wide table would have to change shape every time a section is added.

## Sections

| section | what it answers | cost |
| --- | --- | --- |
| `bundle` | which build is this, and how big overall | free |
| `storage` | rows, bytes and file count per table | free |
| `entities` | how many of each kind of thing | a scan |
| `variants` | how many variants per chromosome, per variant table | a scan |
| `relationships` | links by group pair, and by type | a scan |
| `sources` | what each data source contributed, and when | a scan |

```bash
--param sections=bundle --param sections=storage
```

**`bundle` and `storage` cost nothing.** They read `manifest.json`, which
already records rows and bytes per file. The sizes of a 21 GB bundle come
out of a few hundred lines of JSON — measured at 0.00s against the full
one, where the complete report takes 2.8 seconds over 3.2 billion rows.

**`variants` is grouped from the data, not from filenames.** The manifest
counts rows per *file*, and a file happening to be one chromosome is a
convention of the current build rather than a guarantee. Grouping by the
`chromosome` column is affordable because it has row-group statistics —
2.2 billion rows group in about a second.

## Reading the result

**`tables_without_rows` is the one to watch.** A declared table with no
rows is a source that was planned and did not land. It gets a number of
its own rather than being buried in the per-table list, because it is the
measurement most likely to mean something is wrong.

**`storage` sums a partitioned table across its files.** `note` says how
many, so a table spread over 25 files reads as one row.

**`sources` lists every data source, including ones that never ran** —
those have a null `value_text` and no `as_of`. `platform_etl_status` is
where to go for why.

**`as_of` is about the data, not the report.** When a source was last
loaded, for instance. When the *report* ran is in the provenance sidecar.

## The two tables beside the long one

The long shape holds most of this report faithfully. Two sections it
cannot, and in both cases what it loses is the part you would sort by —
so those travel as tables of their own:

```python
stats = bf.report.run("platform_data_statistics")

stats.table                        # the long measurements, unchanged
stats.extra_tables["storage"]      # table, branch, rows, bytes, files
stats.extra_tables["variants"]     # table, chromosome, rows
```

`storage.bytes` is an integer. In the long shape a table's size survives
twice and neither is usable: `value_text` rounds it to `"3.4 MB"` and
`note` buries the figure in `"1 file(s), 3416028 bytes"`.

`variants.chromosome` is an integer. In the long shape it is a string in
`dimension_2`, so sorting gives 1, 10, 11, 2.

The other four sections keep the long shape and lose nothing by it.
`relationships` alone carries two metrics of different shapes, which is
why "one table per section" is not a thing this report could have.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_platform_etl_packages.md ===== -->

# platform_etl_packages

Every ETL package that went into this bundle. One row per package, which
is one stage of one run.

```bash
biofilter report run --report-name platform_etl_packages \
    --param operation_type=load --output packages.csv
```

> Called `etl_packages` before 4.3.0. A platform report — it describes
> the bundle, not the biology in it, and takes no input.

Where `platform_etl_status` summarises one row per data source, this is
the unaggregated record behind it. Use it when the summary says something
surprising and you want to see the runs themselves.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `only_active` | `false` | restrict to active sources and systems |
| `source_system` | none | one or more systems, e.g. `HGNC` |
| `data_sources` | none | one or more sources, e.g. `hgnc` |
| `operation_type` | none | `extract`, `transform`, or `load` |

## Columns

| column | meaning |
| --- | --- |
| `package_id` | the package, and the order things ran in |
| `source_system`, `data_source`, `data_type` | what it was for |
| `operation_type` | which stage this package is |
| `status` | the package's own outcome |
| `extract_*`, `transform_*`, `load_*` | status, timings, rows and hash per stage |
| `note`, `stats` | whatever the DTP recorded |
| `created_at`, `version_tag` | when, and against which release |

## Reading the result

**One stage per row.** A source that ran fully has three packages:
extract, transform, load. Only the columns of that stage are filled — an
extract package has `extract_status` and `extract_hash` and nulls in the
rest.

**The hash travels.** The digest in a extract package's `extract_hash`
reappears as the next package's `transform_hash`, and then as
`load_hash`. That is what `platform_etl_status` calls alignment.

**Failures stay in the record.** A source that failed and was retried
keeps both packages. `platform_etl_status` reports the latest *good*
stage, so it can say `ok` while a failure sits here — its `latest_error`
is how the two connect.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_platform_etl_status.md ===== -->

# platform_etl_status

What ran to produce this bundle, and whether it holds up. One row per
data source.

```bash
biofilter report run --report-name platform_etl_status --output status.csv
```

> Called `etl_status` before 4.3.0. A platform report — it describes the
> bundle, not the biology in it, and takes no input.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `only_active` | `false` | restrict to active sources and systems |
| `source_system` | none | one or more systems, e.g. `HGNC` |
| `data_sources` | none | one or more sources, e.g. `hgnc` |

## The column to read first: `pipeline_state`

| state | meaning | `pipeline_ok` |
| --- | --- | --- |
| `ok` | every required stage ran, and each on the previous one's output | true |
| `unverifiable` | every stage ran, but no hashes to prove they belong together | true |
| `misaligned` | a stage ran on something other than the previous stage's output | false |
| `incomplete` | a required stage is missing | false |
| `never_run` | no packages at all for this source | false |

`pipeline_ok` means **nothing is known to be wrong**, which is weaker
than "everything is proven right". `unverifiable` is the gap between the
two.

## Why this replaced a simple boolean

The relational version reported `pipeline_ok = False` for **51 of a real
bundle's 68 sources**, and not one of them was broken. Three different
situations were collapsed into one word:

**The variant branch has no load stage.** It writes parquet straight from
transform (ADR-003 §2.3), so `load_status` is null by design. That
accounted for all 51. A report that flags a design decision as a failure
teaches people to ignore it.

**Some DTPs produce no hash.** The relationship DTPs read database state
rather than a downloaded file, so there is nothing to carry forward and
alignment cannot be shown either way. That is `unverifiable`, and
`transform_aligned` is **null** rather than false — false reads as "this
is wrong" rather than "this is unproven".

**Two sources had genuinely never run.** `chebi` and `omim`, which is
what the report should have been drawing attention to all along.

## What "aligned" means

Each ETL stage is its own package row, and the digest of the extract's
output is carried forward: it appears as `extract_hash` in the extract's
package, as `transform_hash` in the transform's, and as `load_hash` in
the load's. Alignment means a stage ran on the previous one's output, not
that two unrelated digests happen to match.

`platform_etl_packages` shows the same hash in all three rows.

## `latest_error`

The most recent failure in a source's history, whether or not it was
later retried successfully — so `pipeline_state = 'ok'` and a non-null
`latest_error` together mean "it worked, but not on the first try".

The message is composed rather than read from the package's `note`,
because every failed package in a real bundle has a null note. Reporting
that null hid the failures entirely.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_resolve_entity.md ===== -->

# resolve_entity

Does the bundle know these names, and unambiguously?

> Called `entity_filter` before 4.3.0. The name changed because it
> describes a resolution step, not a filter: filtering reduces a set,
> this expands one — an ambiguous name comes back as several rows.

```bash
biofilter report run --report-name resolve_entity \
    --input TP53 --input BRCA1 --input NOT_A_GENE \
    --output lookup.csv
```

Use it before any other report: it tells you which of your inputs will
resolve, which are ambiguous, and which the bundle has never heard of.

## One row per match, not per input

Unlike the annotation reports, this one fans out. An input matching three
entities gives three rows. That is the answer — the report's job is to
show the ambiguity, not pick a winner.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | names, symbols, or codes |
| `match_mode` | `exact` | `exact`, `like`, or `fuzzy` |
| `group_filter` | none | restrict to one entity group, e.g. `Genes` |
| `similarity_threshold` | `80` | fuzzy only: minimum score, 0–100 |

### Match modes

| mode | matches when | cost |
| --- | --- | --- |
| `exact` | the alias equals the input, case-insensitively | an equality join |
| `like` | the input occurs **inside** the alias | a scan with a substring test |
| `fuzzy` | Jaro-Winkler similarity ≥ the threshold | a scan with a scored test |

`like` is one-directional on purpose: the input inside the alias, not the
reverse. Matching an alias inside an input would make every
one-character alias match every input containing that character.

## Columns

| column | meaning |
| --- | --- |
| `input_original` | the value you gave |
| `input` | the alias it matched, which may differ in case or form |
| `is_primary` | whether that alias is the entity's preferred name |
| `entity_id` | BF4 entity id — **scoped to this bundle** |
| `primary_name` | the entity's preferred name |
| `group_id`, `group_name` | which kind of entity it is |
| `has_conflict` | the entity was built from sources that disagreed |
| `is_active`, `is_deactive` | curation status, and its negation |
| `data_source_id` | which source contributed the alias |
| `similarity_score` | fuzzy only; null in the other modes |
| `observation` | `multiple matches`, `not found`, or empty |

## Reading the result

**`observation = 'multiple matches'` is about the name, not your search.**
It means that alias belongs to more than one entity, so resolving it
requires a decision you have to make. A broad `like` search returning
two hundred rows is not ambiguous — the row count already told you that.

**`not found` rows are kept deliberately.** Dropping them would leave no
way to tell "the bundle does not know this name" from "you did not ask
about it".

**`has_conflict` is worth a look.** It flags an entity whose sources
disagreed during the build. It does not make the entity wrong, but a
result that depends on one is worth a second look.

## What changed in the migration

**Fuzzy matching is Jaro-Winkler, scored by DuckDB.** The relational
version used `rapidfuzz.fuzz.token_sort_ratio`, which meant pulling all
912 thousand aliases into Python to score them, and an ImportError
wherever that optional dependency was missing.

Scores are no longer comparable between the two versions. Both are 0–100
and the default threshold is still 80, but Jaro-Winkler rewards a shared
prefix and does not reorder words, so a multi-word name scores
differently. Check the threshold against your own inputs rather than
assuming the old one transfers.

**`similarity_score` is always present.** The relational version added
the column only in fuzzy mode, so the result's shape depended on a
parameter.

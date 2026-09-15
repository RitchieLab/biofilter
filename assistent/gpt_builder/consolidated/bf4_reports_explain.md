# BF4 Report Reference (per report)



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

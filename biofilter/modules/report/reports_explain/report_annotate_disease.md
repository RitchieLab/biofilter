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

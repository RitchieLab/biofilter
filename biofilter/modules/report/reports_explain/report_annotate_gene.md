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

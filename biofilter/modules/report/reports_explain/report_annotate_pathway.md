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

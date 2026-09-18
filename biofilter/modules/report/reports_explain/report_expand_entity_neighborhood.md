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

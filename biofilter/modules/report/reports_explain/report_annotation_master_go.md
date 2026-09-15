# annotation_master_go

What the bundle knows about a list of Gene Ontology terms, one row per
input.

```bash
biofilter report run --report-name annotation_master_go \
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

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

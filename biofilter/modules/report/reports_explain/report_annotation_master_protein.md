# annotation_master_protein

What the bundle knows about a list of proteins, one row per input.

```bash
biofilter report run --report-name annotation_master_protein \
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

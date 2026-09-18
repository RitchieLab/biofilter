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
| `input_data` | required | gene identifiers, read according to `gene_identifier` |
| `gene_identifier` | `alias` | how the identifiers are read; see below |
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

## How you name a gene

Three mechanisms, and you say which — for both `input_data` and the
mapping, since it is one decision about one thing.

| `gene_identifier` | Looks in | Use when |
| --- | --- | --- |
| `alias` (default) | every alias: symbols, synonyms, HGNC, Ensembl, Entrez, previous symbols | you are typing gene names |
| a code system — `HGNC`, `ENTREZ`, `ENSEMBL`, … | only that system's aliases | your list came from one source |
| `entity_id` (or `biofilter_id`) | `gene_masters.entity_id`, skipping aliases entirely | you already resolved, and want it to stay resolved |

**Why this is a parameter rather than something the report works out.**
Nothing can tell these apart by looking. 174,410 aliases in the bundle
are bare numbers — Entrez ids — and **14,335 of those are also the entity
id of a different gene**. Entrez `2` is A2M; entity `2` is A1BG-AS1.
Entrez `29974` is A1CF; entity `29974` is RPS2P20. A report that guessed
from the shape of the string would return the wrong gene and say nothing.

Naming the code system is also narrower and therefore more correct. Under
`gene_identifier=entrez`, `2` can only be A2M — the entity-id column is
never consulted, and neither is any other source's alias.

`entity_id` is the bundle's own key. It is exact and it is **scoped to
one bundle**: ids are not stable across builds (ADR-003 §2.5), so a list
of them belongs with the `bundle_id` that issued it.

The code systems offered are read from the bundle, so one built without
UCSC will not pretend to accept it. Asking for a system it does not carry
is refused with the list of what it has.

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

# pair_variants

Candidate variant × variant pairs whose genes share biology.

```bash
biofilter report run --report-name pair_variants \
    --input-file my_variants.txt \
    --param group_types=Pathways \
    --param max_group_size=300 \
    --output pairs.csv
```

**It takes variants, and only the ones you name** — an rsID, `chr:pos`,
or `chr:pos:ref:alt`. A gene name is refused rather than expanded.

> Gene pairs on their own — and expanding them by a list you supply —
> are `pair_genes`. Use that one when the link between a variant and a
> gene comes from outside the bundle, since this report derives it from
> coordinates.
>
> Replaces six reports that were one pipeline written in three eras:
> `variant_gene_location_model` (stage 1 alone),
> `variant_single_gene_annotation` (stages 1-2), `snp_snp_model` and
> `variant_modeling` (all three, differing only in whether both sides had
> to come from the input). The remaining two, `variant_list_intersect`
> and `snp_snp_pair_generator`, never read a bundle at all and are
> handled separately.

## The three stages

1. **Place.** Each input variant — an rsID, `chr:pos`, or
   `chr:pos:ref:alt` — is placed on the genes whose build-38 range
   contains it, widened by `window_bp` if you pass one.
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

## Starting from genes

Run `expand_gene_to_variant`, look at what it returns, filter it, and
pair that.

This report used to do it for you, and that is why it no longer does. A
gene on chr22 holds about 4,000 variants; it kept 100 of them ranked by
allele frequency, and you never saw which 100. Running the expansion
yourself costs one step and puts that selection where it belongs — in
front of the person making it.

```python
variants = bf.report.run("expand_gene_to_variant",
                         input_data=["CHEK2", "SMARCB1"],
                         impact_filter=["HIGH"], af_max=0.01)
keys = variants.to_pandas().query("status == 'ok'").variant_key.tolist()

bf.report.run("pair_variants", input_data=keys)
```

Three parameters went with it, and passing one is an error rather than a
silent change of answer:

| gone | instead |
| --- | --- |
| gene names in `input_data` | `expand_gene_to_variant`, then pair its output |
| `max_variants_per_gene` | the same parameter on `expand_gene_to_variant`, where the expansion now happens |
| `membership="either"` | `pair_genes(membership="either")` → `expand_gene_to_variant` on the partner genes → pair |

`membership="either"` was the unbounded mode: five seed genes reach
19,393 partner genes through proteins. Its replacement is three visible
steps instead of one invisible one.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | variants: rsIDs, `chr:pos`, `chr:pos:ref:alt` |
| `group_types` | `["Pathways"]` | `Pathways`, `Diseases`, `Proteins`, `Genes` |
| `max_group_size` | `300` | drop groups reaching more genes than this; `0` for no limit |
| `min_group_support` | `1` | require this many distinct groups behind a pair |
| `min_group_sources` | `1` | require this many distinct curations |
| `max_pairs` | `1000000` | cap on rows returned |
| `window_bp` | `0` | widen a gene's range when placing a variant |
| `build` | `38` | genome build |
| `af_min`, `af_max` | none | joint allele frequency bounds |

## Gene Ontology is not offered

The bundle carries 38,092 GO entities and **zero** GO relationships, so a
GO group cannot link two genes. The legacy reports accepted
`group_entity_type='GO'` and returned nothing, which reads as a finding.
Here a group type the bundle cannot use is refused by name.

## Columns

| column | meaning |
| --- | --- |
| `input_1`, `input_2` | the text you typed for each side |
| `variant_1_key`, `variant_1_rsid`, `variant_1_chromosome`, `variant_1_position` | side 1 |
| `gene_1_id`, `gene_1_symbol` | the gene side 1 sits in |
| `variant_2_*`, `gene_2_*` | the same for side 2 |
| `group_support_count` | how many distinct groups link the two genes |
| `group_support_source_count` | how many distinct curations asserted them |
| `group_support_types` | `Pathways`, `Diseases`, … |
| `group_support_names` | the groups themselves |
| `group_support_sources` | the curations: `reactome_relationships`, `biogrid`, … |

There is no `from_input` column any more. Every variant in every pair
came from your list, so a column saying so would be a constant.

## Reading the result

**A pair is always across two distinct genes**, and never appears in both
orientations. Two variants in the same gene are not a candidate here.

**`group_support_count` is a weight, not a p-value.** It counts the
groups linking the two genes under the size limit you chose. Change
`max_group_size` and every count changes with it.

**Hitting `max_pairs` is the normal case, not the exception**, because
pair counts grow quadratically. The provenance `truncation` block says
whether it happened; the rows kept are those with the most support.

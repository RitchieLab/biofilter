# pair_variants

Candidate variant × variant pairs whose genes share biology.

```bash
biofilter report run --report-name pair_variants \
    --input CHEK2 --input SMARCB1 --input NF2 \
    --param group_types=Pathways \
    --param max_group_size=300 \
    --param membership=both \
    --output pairs.csv
```

> Replaces six reports that were one pipeline written in three eras:
> `variant_gene_location_model` (stage 1 alone),
> `variant_single_gene_annotation` (stages 1-2), `snp_snp_model` and
> `variant_modeling` (all three, differing only in whether both sides had
> to come from the input). The remaining two, `variant_list_intersect`
> and `snp_snp_pair_generator`, never read a bundle at all and are
> handled separately.

## The three stages

1. **Place.** Each input is read as a gene name or as a variant (rsID,
   `chr:pos`, `chr:pos:ref:alt`) — mixed freely in one list. A variant is
   placed on the genes whose build-38 range contains it, widened by
   `window_bp` if you pass one.
2. **Connect.** Seed genes reach partner genes through a shared entity: a
   pathway, a disease, a protein. That entity is the **group**, and how
   many distinct groups link a pair is the pair's `group_support_count`.
3. **Pair.** Gene pairs become variant pairs.

Stop after stage 2 with `output_grain=gene_pairs`.

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

## `membership`: one side from the input, or both

| value | means |
| --- | --- |
| `both` (default) | both variants come from the input |
| `either` | one comes from the input; the other is any variant in a gene the input reaches |

This is the only difference there ever was between `variant_modeling`
(`both`) and `snp_snp_model` (`either`). It is not a cosmetic filter:
`either` is unbounded in a way `both` is not. Five seed genes reach
19,393 partner genes through proteins.

**Naming a gene and naming a variant are different requests.** Naming
`TP53` asks for its variants. Naming `rs1042522` asks for that variant,
not for the other 4,000 in the gene that contains it. `variant_1_from_input`
and `variant_2_from_input` report which is which.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | gene names and/or variants, mixed |
| `membership` | `both` | see above |
| `output_grain` | `variant_pairs` | or `gene_pairs` to stop after stage 2 |
| `group_types` | `["Pathways"]` | `Pathways`, `Diseases`, `Proteins`, `Genes` |
| `max_group_size` | `300` | drop groups reaching more genes than this; `0` for no limit |
| `min_group_support` | `1` | require this many distinct groups behind a pair |
| `max_variants_per_gene` | `100` | how many variants each gene contributes; `0` for all |
| `max_pairs` | `1000000` | cap on rows returned |
| `window_bp` | `0` | widen a gene's range when placing a variant |
| `build` | `38` | genome build |
| `af_min`, `af_max` | none | joint allele frequency bounds |

## Why `max_variants_per_gene` exists

Pairs grow with the **square** of the variants per gene. A gene on chr22
carries about 4,000 variants; three such genes paired in full are 48
million rows, which is not an answer anybody reads — and the query runs
out of memory before it gets there.

The variants kept are the **most common**, because a pairwise interaction
test has no power on a rare variant. A variant you named by hand is never
dropped. Both facts are recorded in the provenance `pairing` block.

## Gene Ontology is not offered

The bundle carries 38,092 GO entities and **zero** GO relationships, so a
GO group cannot link two genes. The legacy reports accepted
`group_entity_type='GO'` and returned nothing, which reads as a finding.
Here a group type the bundle cannot use is refused by name.

## Columns

| column | meaning |
| --- | --- |
| `input_1`, `input_2` | what you typed, when you typed this variant or its gene |
| `variant_1_key`, `variant_1_rsid`, `variant_1_chromosome`, `variant_1_position` | side 1 |
| `gene_1_id`, `gene_1_symbol` | the gene side 1 sits in |
| `variant_1_from_input` | whether side 1 came from your list |
| `variant_2_*`, `gene_2_*` | the same for side 2 |
| `group_support_count` | how many distinct groups link the two genes |
| `group_support_types` | `Pathways`, `Diseases`, … |
| `group_support_names` | the groups themselves |
| `membership` | which mode produced this row |

## Reading the result

**A pair is always across two distinct genes**, and never appears in both
orientations. Two variants in the same gene are not a candidate here.

**`group_support_count` is a weight, not a p-value.** It counts the
groups linking the two genes under the size limit you chose. Change
`max_group_size` and every count changes with it.

**Hitting `max_pairs` is the normal case, not the exception**, because
pair counts grow quadratically. The provenance `truncation` block says
whether it happened; the rows kept are those with the most support.

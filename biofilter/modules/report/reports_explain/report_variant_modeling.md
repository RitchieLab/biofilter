# Report: `variant_modeling`

## Purpose

Maps an input list of genomic variants to biologically connected variant pairs,
where **both variants in every pair come from the input list**.

The workflow mirrors the diagram below:

```
Input variants (rsID or chr:pos)
    ↓  DB lookup + window_bp
Genes overlapping input variants
    ↓  group membership (Pathway, GO, Disease, …)
Groups
    ↓  co-membership → Gene×Gene pairs
Gene×Gene pairs  [weight = # shared groups]
    ↓  cartesian of input variants per gene
Variant×Variant pairs  ← output
```

`group_support_count` is the biological weight of each pair: how many distinct
groups (pathways, GO terms, diseases, …) have both genes as members.

---

## Use case

Input comes from a pre-genotyped file (VCF, PLINK `.bim`, curated list). Because
all variants are already sequenced in the cohort, pairs are restricted to input
variants only — there is no expansion to new DB variants.

This is the **correct design** for interaction studies where you want to test
epistasis between variants you already have genotyped.

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_data` | list \| path | required | rsID or chr:pos variants; file path (one per line) also accepted |
| `build` | int | `38` | Genome build for gene overlap queries |
| `window_bp` | int | `0` | Extend gene boundaries by N bp when assigning variants to genes |
| `group_entity_groups` | list \| str | `"Pathway"` | Which group types define biological connections (Pathway, GO, Disease, …) |
| `group_data_sources` | list \| str | all | Restrict to specific data sources (Reactome, KEGG, GO, …) |
| `gene_entity_groups` | list \| str | `"Gene"` | Entity group label for genes in the DB |
| `relationship_types` | list \| str | all | Restrict to specific relationship type codes |
| `max_pairs` | int | `1_000_000` | Safety cap — aborts before materialising if estimate exceeds this |

---

## Input format

Each item in `input_data` can be:

| Format | Example | Resolution |
|---|---|---|
| rsID | `rs429358` | Exact rsID match in `variant_masters` |
| chr:pos | `chr19:44908684` | Position overlap in `variant_masters` |
| bare | `19:44908684` | Same as chr:pos |
| file path | `./variants.txt` | One rsID or chr:pos per line |

---

## Output columns

| Column | Description |
|---|---|
| `variant_1_id` / `variant_2_id` | Internal DB IDs |
| `variant_1_rsid` / `variant_2_rsid` | rsIDs (if available) |
| `variant_1_chr` / `variant_2_chr` | Chromosome (chrX format) |
| `variant_1_pos` / `variant_2_pos` | Position start |
| `gene_1_name` / `gene_2_name` | Gene symbols |
| `gene_1_id` / `gene_2_id` | Internal gene entity IDs |
| `group_support_count` | **Weight** — # groups linking gene_1 to gene_2 |
| `group_support_names` | Pipe-separated group names |
| `data_source_support_count` | # data sources |
| `data_source_support_names` | Pipe-separated source names |
| `build` | Genome build used |
| `window_bp` | Window applied to gene boundaries |

Output is sorted by `group_support_count DESC`, then gene names.

---

## Safety check

Before materialising pairs the report **estimates** the total count:

```
estimated = sum(len(variants_gene_1) × len(variants_gene_2) for each gene pair)
```

If `estimated > max_pairs` the report returns a single-row error DataFrame:

```python
{
    "resolution_status": "pair_limit_exceeded",
    "estimated_pairs":   3_200_000,
    "max_pairs":         1_000_000,
    "suggestion":        "..."
}
```

To resolve: apply stricter `group_entity_groups` / `group_data_sources` filters,
or increase `max_pairs`.

---

## API examples

### Basic — Pathway connections

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db")
bf.db.connect()

df = bf.report.run(
    "variant_modeling",
    input_data        = ["rs429358", "rs7412", "chr2:21044574", "chr4:186486470"],
    build             = 38,
    window_bp         = 0,
    group_entity_groups = ["Pathway"],
    group_data_sources  = ["Reactome"],
)

print(f"Pairs: {len(df):,}")
df.sort_values("group_support_count", ascending=False).head(20)
```

### From file

```python
df = bf.report.run(
    "variant_modeling",
    input_data          = "./my_variants.txt",   # one rsID or chr:pos per line
    group_entity_groups = ["Pathway", "GO"],
)
```

### Multiple group types

```python
df = bf.report.run(
    "variant_modeling",
    input_data          = ["rs429358", "rs7412"],
    group_entity_groups = ["Pathway", "Disease", "GO"],
    window_bp           = 5000,
)
```

---

## CLI examples

```bash
# Basic
biofilter report run \
  --report-name variant_modeling \
  --input rs429358 --input rs7412 --input chr2:21044574 \
  --param build=38 \
  --param group_entity_groups=Pathway \
  --output variant_pairs.csv

# From file
biofilter report run \
  --report-name variant_modeling \
  --input-file ./variants.txt \
  --param group_entity_groups=Pathway \
  --param group_data_sources=Reactome \
  --output variant_pairs.csv

# Inspect params template
biofilter report run --report-name variant_modeling --params-template
```

---

## Difference from `snp_snp_model` (legacy)

| | `variant_modeling` | `snp_snp_model` (legacy) |
|---|---|---|
| Input | rsID or chr:pos | chr:pos only |
| Variant source | Input list only | Expands from gene loci (up to 2000/gene) |
| Both variants from input? | **Always** | Optional (scope parameter) |
| group_support_count | Yes — built-in weight | No |
| Design intent | Genotyped cohort study | Discovery / annotation |

---

## Expected scale

| Scenario | Input variants | Gene pairs | Output pairs | Runtime |
|---|---|---|---|---|
| 4 APOE-region variants | ~4 | ~10 | ~60 | < 1s |
| 50 curated disease variants | ~50 | ~200 | ~5k | < 5s |
| 500 pathway variants | ~500 | ~1k | ~100k | < 30s |
| 5000 variants, no filter | ~5000 | ~50k | **safety abort** | — |

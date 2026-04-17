# Report: `snp_snp_pair_generator`

## Purpose

**Phase 3** of the SNP×SNP interaction pipeline.

Takes a LD-pruned variant list (**Lista D**, from PLINK) and the biologically
annotated variant source (**Lista A**, from `gene_to_variant_filtering`), generates
all variant pairs according to the chosen strategy, and returns a fully annotated
pair DataFrame ready for statistical interaction testing (PLINK epistasis, SAIGE,
custom logistic regression, etc.).

---

## Pipeline context

```
Phase 1  variant_single_gene_annotation  →  gene list (~8k genes)
Phase 2  gene_to_variant_filtering       →  Lista A (annotated variants, CSV)
Phase 2.5 variant_list_intersect         →  Lista C  (genotyped subset)
[PLINK]  --indep-pairwise                →  Lista D  (LD-independent)
Phase 3  snp_snp_pair_generator          →  this report — annotated pairs
```

Full tutorial: `notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb`

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `variant_list` | str (path) | required | Lista D from PLINK (`.prune.in`, `.txt`, `.list`) — one variant ID per line |
| `annotation_source` | str (path) | required | Lista A CSV from `gene_to_variant_filtering` — provides all annotation columns |
| `pairing_strategy` | str | `"seed_vs_all"` | How to generate pairs — see strategies below |
| `seed_gene` | str \| None | `None` | Gene symbol to use as seed for `seed_vs_all` (resolved from `annotation_source`) |
| `seed_variants` | list \| None | `None` | Explicit list of seed variant IDs for `seed_vs_all` (alternative to `seed_gene`) |
| `max_pairs` | int | `1_000_000` | Safety cap — report aborts if estimated pairs exceed this value |
| `exclude_same_gene` | bool | `True` | Exclude pairs where both variants belong to the same gene |

---

## Pairing strategies

### `seed_vs_all` *(recommended for gene-centric studies)*

Pairs every **seed** variant against every **non-seed** variant.

```
n_seed × n_other
```

Seed is resolved from `annotation_source`:
- `seed_gene="APOE"` → all Lista D variants where `gene_symbol == "APOE"`
- `seed_variants=["rs429358", "rs7412"]` → explicit IDs

Best choice when the study revolves around a specific gene (e.g., APOE in
Alzheimer's) and the goal is to test interactions between the seed locus
and all pathway partners.

**Scale example:** 12 APOE variants × 11,988 partners = **~144k pairs**

---

### `cross_gene`

All unique pairs between variants from **different genes**.
Same-gene pairs are always excluded regardless of `exclude_same_gene`.

```
all_vs_all − same_gene_pairs
```

Use when there is no specific seed gene and the goal is to test all
inter-gene interactions within the pathway.

**Scale example:** 12k variants, avg 5 variants/gene → ~70M pairs (upper bound).
Apply aggressive Phase 2 filters before using this strategy.

---

### `all_vs_all`

All unique ordered pairs, including same-gene pairs.

```
n × (n − 1) / 2
```

Most permissive — typically only practical with small Lista D (< 2k variants).
The `max_pairs` safety check will abort if the estimate exceeds the limit.

---

## Safety check

Before materialising any pairs, the report **estimates** the pair count and
compares it to `max_pairs`:

- If estimate ≤ `max_pairs` → proceeds normally
- If estimate > `max_pairs` → aborts immediately and returns a single-row
  DataFrame with `resolution_status = "pair_limit_exceeded"`

```python
# Example abort response
{
    "resolution_status": "pair_limit_exceeded",
    "estimated_pairs":   72_000_000,
    "max_pairs":         1_000_000,
    "pairing_strategy":  "all_vs_all",
    "suggestion":        "Switch to seed_vs_all or apply stricter Phase 2 filters …"
}
```

The suggestion message guides the user toward a feasible configuration.

---

## Annotation enrichment

The report joins Lista D variant IDs to Lista A using the same dual-key
matching strategy as `variant_list_intersect`:

1. **rsID match** — if the Lista D ID looks like `rs\d+`
2. **chr:pos match** — fallback for IDs like `19:44908684` or `chr19:44908684`

Every column present in `annotation_source` is carried through to the output,
mirrored on both sides of each pair with `_a` / `_b` suffixes.

Variants in Lista D that are **not found** in `annotation_source` are dropped
with a warning log. They were in Lista D but not in Lista A — typically
variants that passed LD pruning but fell outside the gene windows defined in
Phase 2.

---

## Output DataFrame

One row per variant pair. All annotation columns are mirrored:

| Column | Description |
|---|---|
| `rsid_a` | rsID of variant A |
| `gene_symbol_a` | Gene of variant A |
| `consequence_name_a` | Most severe consequence for variant A |
| `impact_name_a` | VEP impact for variant A (`HIGH`, `MODERATE`, …) |
| `af_a` | Allele frequency of variant A |
| `cadd_phred_a` | CADD Phred score for variant A |
| `alphamissense_score_a` | AlphaMissense score for variant A |
| `alphamissense_classification_a` | AlphaMissense class for variant A |
| *(…same columns with `_b` suffix for variant B…)* | |
| `same_gene` | `True` if both variants belong to the same gene |
| `pairing_strategy` | Strategy used to generate this pair |

---

## Resolution status codes

| `resolution_status` | Meaning |
|---|---|
| *(absent — normal output)* | Pairs generated successfully |
| `pair_limit_exceeded` | Estimated pairs exceed `max_pairs`; no pairs generated |
| `no_variants_matched` | No Lista D variants found in `annotation_source` |
| `seed_not_found` | `seed_gene` / `seed_variants` produced no matches in the enriched list |

---

## API examples

### Standard use — seed_vs_all

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db")
bf.db.connect()

df_pairs = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "seed_vs_all",
    seed_gene         = "APOE",
    max_pairs         = 1_000_000,
    exclude_same_gene = True,
)

print(f"Pairs: {len(df_pairs):,}")
df_pairs.head()
```

### Explicit seed variants

```python
df_pairs = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "seed_vs_all",
    seed_variants     = ["rs429358", "rs7412"],   # APOE ε4 and ε2 alleles
)
```

### Cross-gene (no seed)

```python
df_pairs = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "cross_gene",
    max_pairs         = 500_000,
)
```

### Handling safety abort

```python
df = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "all_vs_all",
)

if "resolution_status" in df.columns:
    status = df["resolution_status"].iloc[0]
    if status == "pair_limit_exceeded":
        print(df["suggestion"].iloc[0])
else:
    print(f"{len(df):,} pairs ready")
```

---

## CLI examples

```bash
# seed_vs_all — APOE seed
biofilter report run \
  --report-name snp_snp_pair_generator \
  --param variant_list=pipeline_output/lista_D.prune.in \
  --param annotation_source=pipeline_output/lista_A.csv \
  --param pairing_strategy=seed_vs_all \
  --param seed_gene=APOE \
  --output pipeline_output/phase3_pairs.csv

# cross_gene — no seed
biofilter report run \
  --report-name snp_snp_pair_generator \
  --param variant_list=pipeline_output/lista_D.prune.in \
  --param annotation_source=pipeline_output/lista_A.csv \
  --param pairing_strategy=cross_gene \
  --param max_pairs=500000 \
  --output pipeline_output/phase3_pairs.csv

# Inspect params template
biofilter report run \
  --report-name snp_snp_pair_generator \
  --params-template
```

---

## Expected scale

| Scenario | Lista D | Strategy | Pairs | Runtime |
|---|---|---|---|---|
| Single gene seed, small pathway | 200 | `seed_vs_all` | ~2k | < 1s |
| APOE seed, Reactome pathway | ~12k | `seed_vs_all` | ~144k | < 5s |
| Medium pathway, no seed | ~2k | `cross_gene` | ~2M | ~10s |
| Large pathway, no seed | ~12k | `cross_gene` | ~70M | **safety abort** |

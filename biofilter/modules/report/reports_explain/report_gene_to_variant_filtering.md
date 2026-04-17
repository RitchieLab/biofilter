# Report Tutorial: `gene_to_variant_filtering`

## Purpose

Phase 2 of the single-variant SNP×SNP interaction pipeline.

Given a list of gene symbols, this report collects the variants overlapping each gene's
genomic locus, with all heavy filters pushed to the SQL layer before data reaches Python.

---

## Pipeline Context

```
Phase 1 — Gene Discovery  (variant_single_gene_annotation)
  input : one variant (chr:pos or rsID)
  output: seed gene + partner-gene list with shared-group annotation
  scale : ~8 k rows (tractable)
          ↓ partner gene symbol list

Phase 2 — Filtered Variant Collection  (this report)
  input : list of gene symbols
  output: one row per (gene × variant) with consequence and prediction annotations
  scale : ~15 k–100 k rows, controlled by filters

Phase 3 — Pair Generation  (planned)
  input : Phase 2 variant sets per gene
  output: variant × variant interaction pairs (seed × partner)
  scale : controlled by Phase 2 filtering
```

Separating gene discovery (Phase 1, tractable) from variant enumeration (Phase 2, SQL-filtered)
prevents the combinatorial explosion that occurs when all variants are annotated before filtering.
Without pre-filtering, a gene like APOE with ~1 k variants × 300 partner genes would produce
~260 M rows before any filter is applied.

---

## Report Name

`gene_to_variant_filtering`

---

## Parameters (API)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gene_symbols` | `list[str]` or comma-string | **required** | Gene symbols to query. Also accepted as `input_data` (alias used by `run_example`). |
| `build` | `int` | `38` | Genome assembly build for locus lookup via `entity_locations`. |
| `gene_window_bp` | `int` | `0` | Extend each gene locus by this many bp on each side before querying variants. Useful for capturing regulatory/nearby variants. |
| `most_severe_only` | `bool` | `True` | Keep only the row flagged `is_most_severe_for_variant=TRUE` in `variant_molecular_effects`. See section **Unit of analysis** below. |
| `impact_filter` | `list[str]` | `None` | Impact names to keep, e.g. `["HIGH", "MODERATE"]`. |
| `consequence_type_filter` | `list[str]` | `None` | Consequence group names, category names, or individual consequence names. Pre-resolved to `consequence_id`s before the main query. |
| `lof_confidence_filter` | `list[str]` | `None` | LoF confidence tiers to keep, e.g. `["HC"]` or `["HC", "LC"]`. Filters `lof_confidence` in `variant_molecular_effects`. |
| `af_max` | `float` | `None` | Maximum allele frequency — rare-variant mode (e.g. `0.01`). |
| `af_min` | `float` | `None` | Minimum allele frequency — common-variant mode (e.g. `0.05`). |
| `cadd_phred_min` | `float` | `None` | Minimum CADD Phred score (e.g. `20`). Applied on `variant_masters.cadd_phred`. |
| `sift_score_max` | `float` | `None` | Maximum SIFT score (lower = more deleterious; e.g. `0.05`). Applied on `variant_masters.sift_max`. |
| `polyphen_score_min` | `float` | `None` | Minimum PolyPhen score (higher = more damaging; e.g. `0.85`). Applied on `variant_masters.polyphen_max`. |
| `alphamissense_score_min` | `float` | `None` | Minimum AlphaMissense score. Applied Python-side after LEFT JOIN. |
| `alphamissense_classification` | `list[str]` | `None` | AlphaMissense classifications to keep, e.g. `["likely_pathogenic", "ambiguous"]`. Applied Python-side. |
| `max_variants_per_gene` | `int` | `5000` | Safety cap per gene after all filters. Emits a WARNING if exceeded. |

---

## Unit of Analysis: `most_severe_only`

This is the most important design decision for the pipeline.

### `most_severe_only=True` (default — recommended for SNP×SNP pipeline)

The unit of analysis is the **variant** — not the transcript, not the allele.

```
variant_masters          (1 row per ALT allele)
    ↓ JOIN on (variant_id, chromosome)
variant_molecular_effects  WHERE is_most_severe_for_variant = TRUE
    → exactly 1 row per variant (the worst consequence across all transcripts)
    → impact, consequence_type, and LoF filters apply to this single row
    ↓ LEFT JOIN on (variant_id, chromosome)   ← NO transcript_id in join key
variant_effect_predictions  WHERE predictor_key = 'alphamissense'
    → AlphaMissense is variant-level — attaches cleanly to the 1 row per variant
```

**Consequence**: if a variant passes the molecular-effect filters (e.g. impact=HIGH) AND
passes the prediction filters (e.g. alphamissense=likely_pathogenic), these are evaluated
on the **same object** — the variant's most-severe consequence record. No transcript mixing.

Result: **1 row per (gene × variant)**.

### `most_severe_only=False` (advanced use)

The unit of analysis is the **transcript**.

Each variant appears N times — once per transcript in `variant_molecular_effects`.
AlphaMissense and other prediction values repeat on every transcript row (they are
stored at the variant level in `variant_effect_predictions`, not per-transcript).

This mode is useful for:
- Splice analysis (identifying which specific transcripts are affected)
- Canonical-transcript-only analysis (filter by `canonical=True` or `mane_select=True` in post-processing)
- Studies that require full transcript-level consequence decomposition

Result: **1 row per (gene × variant × transcript)**.

---

## Consequence Filter Resolution

`consequence_type_filter` accepts names at three levels of granularity:

```
Level 1 — Group       : "transcript_variant", "intergenic_variant", ...
Level 2 — Category    : "coding_sequence_variant", "splice_region_variant", ...
Level 3 — Individual  : "missense_variant", "stop_gained", "frameshift_variant", ...
```

Resolution steps (executed before the main query):

```
consequence_type_filter = ["missense_variant", "splice_region_variant"]
  → query variant_consequence_groups WHERE name IN (...)  → group_ids
  → query variant_consequence_categories WHERE name IN (...) → category_ids
  → query variant_consequences WHERE name IN (...) → direct_ids
  → query variant_consequences WHERE group_id IN (...) OR category_id IN (...) → inherited_ids
  → consequence_ids = inherited_ids ∪ direct_ids
  → main query: WHERE vme.consequence_id IN (consequence_ids)
```

This allows mixing levels in a single filter list.

---

## LoF Filter

The `lof_confidence_filter` parameter filters on `variant_molecular_effects.lof_confidence`.

LOFTEE (Loss-of-Function Transcript Effect Estimator) annotates LoF variants with:

| Value | Meaning |
|---|---|
| `HC` | High Confidence — curated, reliable LoF calls |
| `LC` | Low Confidence — LoF call with caveats |

Typical use cases:
- `["HC"]` — strict, pipeline-grade LoF filtering
- `["HC", "LC"]` — inclusive mode, flag LC separately

Note: `lof_confidence` is `NULL` for non-LoF variants. This filter is applied in SQL with
`AND vme.lof_confidence IN (...)`, which naturally excludes NULLs — i.e., it keeps **only**
variants with the specified LoF confidence tier.

---

## Effect Prediction Sources

| Score | Source table | Column | Applied at |
|---|---|---|---|
| CADD Phred | `variant_masters` | `cadd_phred` | SQL (WHERE clause) |
| SIFT | `variant_masters` | `sift_max` | SQL (WHERE clause) |
| PolyPhen | `variant_masters` | `polyphen_max` | SQL (WHERE clause) |
| AlphaMissense score | `variant_effect_predictions` | `score` WHERE `predictor_key='alphamissense'` | Python (post LEFT JOIN) |
| AlphaMissense class | `variant_effect_predictions` | `classification` WHERE `predictor_key='alphamissense'` | Python (post LEFT JOIN) |

CADD, SIFT, and PolyPhen are stored directly on `variant_masters` as pre-aggregated variant-level
summaries (`cadd_phred`, `sift_max`, `polyphen_max`), which allows SQL-level filtering without
any additional join. AlphaMissense requires a LEFT JOIN to `variant_effect_predictions` and is
therefore filtered in Python after the query.

---

## Query Architecture — Temp Table and Partition-Aware Design

### Problem

With 8 k+ genes in the input (typical Phase 2 call), some chromosomes have hundreds of genes.
A standard `OR (position BETWEEN start1 AND end1) OR (position BETWEEN start2 AND end2) OR ...`
clause becomes unmanageable and prevents the query planner from using indexes efficiently.

### Solution: Temp table + range JOIN per chromosome

```sql
-- Step 1: created once per report run
CREATE TEMP TABLE _bf_gene_ranges (
    gene_entity_id BIGINT,
    gene_symbol    TEXT,
    chromosome     INTEGER,
    range_start    BIGINT,
    range_end      BIGINT
)

-- Step 2: populated with all gene loci (including gene_window_bp expansion)
INSERT INTO _bf_gene_ranges VALUES (...)  -- batch inserts, 500 rows at a time

-- Step 3: one query per chromosome
SELECT ...
FROM _bf_gene_ranges gr
JOIN variant_masters vm
    ON  vm.chromosome      = gr.chromosome
    AND vm.position_start >= gr.range_start
    AND vm.position_start <= gr.range_end
JOIN variant_molecular_effects vme
    ON  vme.variant_id = vm.variant_id
    AND vme.chromosome = vm.chromosome
    AND vme.is_most_severe_for_variant = true  -- when most_severe_only=True
    [AND vme.impact_id IN (...)]
    [AND vme.consequence_id IN (...)]
    [AND vme.lof_confidence IN (...)]
LEFT JOIN (
    SELECT chromosome, variant_id,
           MAX(score) AS am_score, MAX(classification) AS am_class
    FROM variant_effect_predictions
    WHERE chromosome = :chromosome AND predictor_key = 'alphamissense'
    GROUP BY chromosome, variant_id
) vep_am ON vep_am.variant_id = vm.variant_id AND vep_am.chromosome = vm.chromosome
WHERE vm.chromosome = :chromosome
  AND gr.chromosome = :chromosome
  [AND vm.af <= :af_max]
  [AND vm.cadd_phred >= :cadd_phred_min]
  ...
```

**Why one query per chromosome?**
- `variant_masters` is partitioned by `chromosome` on PostgreSQL. Filtering `WHERE vm.chromosome = :chromosome` ensures the query planner uses partition pruning, reading only the relevant child partition.
- The BETWEEN condition on `position_start` leverages the B-tree index on `(chromosome, position_start)`.

**Why a temp table instead of a CTE or subquery?**
- A temp table is materialized — the gene ranges are evaluated once and indexed by the planner.
- A CTE may be inlined or re-evaluated per row depending on the PostgreSQL version and query shape.
- For 8 k+ genes, the temp table approach is consistently faster.

### Variant-gene assignment

A variant can overlap multiple gene loci in the input list. In that case, the report produces
one row per overlapping gene — the same variant appears multiple times, once per gene. This is
the correct behavior for Phase 3, where we need to know which gene each variant belongs to in
order to form gene-level pairs.

---

## Output Columns

| Column | Description |
|---|---|
| `resolution_status` | `None` on success; an error code on failure. |
| `gene_input` | The gene symbol used as input (same as `gene_symbol` on success). |
| `gene_entity_id` | Internal entity ID of the gene. |
| `gene_symbol` | HGNC symbol (from `gene_masters.symbol`). |
| `gene_chromosome` | Chromosome of the gene. |
| `gene_start` | Gene locus start (including `gene_window_bp` expansion). |
| `gene_end` | Gene locus end (including `gene_window_bp` expansion). |
| `variant_id` | Internal variant ID (`variant_masters`). |
| `chromosome` | Chromosome of the variant (integer: X=23, Y=24, MT=25). |
| `position_start` | Variant start position (GRCh38). |
| `position_end` | Variant end position. |
| `rsid` | dbSNP rsID (if available). |
| `reference_allele` | Reference allele. |
| `alternate_allele` | Alternate allele. |
| `af` | Allele frequency (gnomAD). |
| `transcript_id` | Transcript ID from VEP annotation. |
| `consequence_id` | Internal consequence ID (FK to `variant_consequences`). |
| `consequence_name` | Consequence name (e.g. `missense_variant`). |
| `consequence_group` | Consequence group name. |
| `consequence_category` | Consequence category name. |
| `impact_id` | Internal impact ID. |
| `impact_name` | Impact name (`HIGH`, `MODERATE`, `LOW`, `MODIFIER`). |
| `is_most_severe_for_variant` | Whether this is the most severe consequence row for the variant. |
| `hgvsc` | HGVS coding notation. |
| `hgvsp` | HGVS protein notation. |
| `lof_flag` | Boolean LoF flag from LOFTEE. |
| `lof_confidence` | LOFTEE tier: `HC`, `LC`, or NULL. |
| `lof_filter` | LOFTEE filter reason (when LC). |
| `lof_flags` | Additional LOFTEE flags. |
| `canonical` | Whether this is the canonical transcript. |
| `mane_select` | Whether this is the MANE Select transcript. |
| `cadd_phred` | CADD Phred score (variant-level, from `variant_masters`). |
| `sift_max` | SIFT max score across transcripts (from `variant_masters`). |
| `polyphen_max` | PolyPhen max score across transcripts (from `variant_masters`). |
| `alphamissense_score` | AlphaMissense pathogenicity score (from `variant_effect_predictions`). |
| `alphamissense_classification` | AlphaMissense classification (`likely_pathogenic`, `ambiguous`, `likely_benign`). |

### Resolution Status Codes

| Code | Meaning |
|---|---|
| `(None)` | Success. |
| `empty_gene_list` | `gene_symbols` was provided but resolved to an empty list. |
| `no_genes_resolved` | None of the provided symbols matched any `entity_aliases` entry for the Genes group. |
| `no_loci_found` | Gene entity_ids were resolved but none had entries in `entity_locations` for the requested build. |
| `no_variants_found` | Genes and loci were resolved but no variants passed all filters. |

---

## Examples

### API

```python
from biofilter import Biofilter

bf = Biofilter()

# ── Basic: one gene, no filters
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE"],
)

# ── Multiple genes, HIGH/MODERATE impact only
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU", "TOMM40", "BIN1"],
    impact_filter=["HIGH", "MODERATE"],
    most_severe_only=True,
)

# ── Rare variants with LoF HC
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["BRCA1", "BRCA2"],
    af_max=0.001,
    lof_confidence_filter=["HC"],
    impact_filter=["HIGH"],
)

# ── AlphaMissense pathogenic missense variants
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU"],
    consequence_type_filter=["missense_variant"],
    alphamissense_classification=["likely_pathogenic"],
    most_severe_only=True,
)

# ── Full pipeline Phase 2: pass gene list from Phase 1 output
df_phase1 = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",
    group_entity_type="Pathways",
    source_system_filter=["Reactome"],
)

partner_genes = df_phase1["partner_gene_symbol"].dropna().unique().tolist()
seed_gene     = df_phase1["seed_gene_symbol"].iloc[0]
all_genes     = [seed_gene] + partner_genes

df_phase2 = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=all_genes,
    impact_filter=["HIGH", "MODERATE"],
    af_max=0.05,
    most_severe_only=True,
)
```

### CLI

```bash
# ── Basic single gene
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols=APOE

# ── Multiple genes, HIGH/MODERATE impact
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="APOE,CLU,TOMM40,BIN1" \
  --param impact_filter="HIGH,MODERATE" \
  --param most_severe_only=true

# ── Rare LoF variants
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="BRCA1,BRCA2" \
  --param af_max=0.001 \
  --param lof_confidence_filter=HC \
  --param impact_filter=HIGH

# ── Save to CSV
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="APOE,CLU" \
  --param impact_filter="HIGH,MODERATE" \
  --output phase2_variants.csv

# ── Inspect params template
biofilter report run \
  --report-name gene_to_variant_filtering \
  --params-template
```

---

## Expected Scale

| Scenario | Genes | Variants (estimated) |
|---|---|---|
| APOE locus, no filter | 1 | ~1 000 |
| APOE locus, `most_severe_only=True` | 1 | ~100 |
| APOE locus, HIGH/MODERATE + AF < 0.01 | 1 | ~20–30 |
| Phase 1 APOE Reactome partners (~300 genes), HIGH/MODERATE + AF < 0.05 | ~300 | ~15 000 |
| Phase 1 APOE all-source partners (~8 k genes), no filter | ~8 000 | potentially millions — use filters |

**Always apply at minimum `most_severe_only=True` (default) when using output for Phase 3.**

---

## Demo Tips

- Start with `gene_symbols=["APOE"]` and `impact_filter=["HIGH", "MODERATE"]` — fast and produces a readable result.
- Check `resolution_status` first; a non-null value explains why no variants were returned.
- Use `alphamissense_classification=["likely_pathogenic"]` to quickly isolate the most interesting missense variants.
- For LoF studies, combine `lof_confidence_filter=["HC"]` + `impact_filter=["HIGH"]` — these two filters are complementary and together capture curated loss-of-function calls.

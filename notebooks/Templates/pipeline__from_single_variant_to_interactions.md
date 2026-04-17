# SNP×SNP Interaction Pipeline: From a Single Variant to Biologically-Informed Interaction Pairs

**Biofilter 4
**Pipeline version:** 1.0  
**Biofilter version:\*\* 4.1.x

---

## Abstract

We describe a computational pipeline for generating biologically-informed variant interaction pairs for SNP×SNP epistasis analysis. Starting from a single seed variant of interest, the pipeline (1) identifies genes sharing biological context with the seed gene via curated relationship databases, (2) collects and annotates all variants within those gene loci using gnomAD functional annotations, (3) intersects the annotated variant set with the study's genotyped variants, (4) applies linkage disequilibrium (LD) pruning to produce a statistically independent variant set, and (5) generates all pairwise interaction candidates with full annotation on both sides. The pipeline is implemented in Biofilter 4 and is designed to dramatically reduce the interaction search space relative to naive all-pairs approaches while preserving biological interpretability.

---

## 1. Motivation

Genome-wide association studies (GWAS) test each variant independently, ignoring epistatic interactions that may contribute substantially to complex trait heritability. Testing all possible SNP pairs in a typical GWAS dataset (500k–10M variants) is computationally intractable and statistically underpowered after multiple testing correction. Biologically-guided pre-selection of variant pairs — restricting analysis to variants in functionally related genes — reduces the search space by orders of magnitude and improves power by concentrating tests on mechanistically plausible interactions.

---

## 2. Pipeline Architecture

The pipeline is organized into five phases, alternating between Biofilter (biological annotation) and external tools (genotyping and LD computation):

```
Phase 1   [Biofilter]  Biological network construction
Phase 2   [Biofilter]  Variant annotation and filtering       → Lista A
Phase 2.5 [Biofilter]  Genotype-annotation integration       → Lista C
          [PLINK 1.9]  LD pruning on Lista C                 → Lista D
Phase 3   [Biofilter]  Interaction pair generation           → Pairs
```

**Scale reduction example** (APOE seed, Reactome pathways):

| Stage                                          | N                          |
| ---------------------------------------------- | -------------------------- |
| All possible variant pairs (gnomAD)            | ~260 M                     |
| After Phase 2 filters (HIGH/MODERATE, AF < 5%) | ~405 k variants            |
| After genotype intersection (Phase 2.5)        | ~350 k variants (estimate) |
| After LD pruning (r² < 0.2)                    | ~50 k variants             |
| Final interaction pairs (seed × partners)      | ~600 k pairs               |

---

## 3. Data Sources

| Source                     | Content                                                       | Version / Build         |
| -------------------------- | ------------------------------------------------------------- | ----------------------- |
| Biofilter 4 knowledge base | Gene loci, pathway membership, disease associations, GO terms | 4.1.2, GRCh38           |
| Reactome                   | Curated biological pathways                                   | Current at DB ingestion |
| gnomAD v4                  | Variant allele frequencies, functional annotations            | GRCh38                  |
| Ensembl VEP                | Consequence annotations, LOFTEE LoF confidence                | GRCh38                  |
| AlphaMissense              | Deep learning pathogenicity scores for missense variants      | v1                      |
| CADD                       | Combined annotation-dependent depletion scores                | v1.7                    |
| NCBI / HGNC                | Gene symbol resolution, canonical loci                        | Current at DB ingestion |

---

## 4. Phase 1 — Biological Network Construction

**Report:** `variant_single_gene_annotation`
[Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_single_gene_annotation.ipynb)

### Input

A single seed variant specified as rsID (e.g., `rs429358`) or genomic coordinate (`chr19:44908684`).

### Method

1. The seed variant is mapped to its host gene via genomic position overlap with gene loci (GRCh38 coordinates from NCBI/HGNC).
2. The host (seed) gene is queried against the Biofilter 4 entity relationship graph to retrieve all partner genes connected through shared biological groups (pathways, diseases, GO terms, protein families).
3. Relationships are filtered by `group_entity_type` (e.g., `Pathways`) and optionally by source system (e.g., `Reactome`).

### Key parameters

| Parameter              | Value used | Rationale                                                                   |
| ---------------------- | ---------- | --------------------------------------------------------------------------- |
| `group_entity_type`    | `Pathways` | Restricts to curated functional pathways; reduces non-specific associations |
| `source_system_filter` | `Reactome` | Reactome provides manually curated, hierarchical pathway annotations        |

### Output

A DataFrame with one row per (seed_gene × partner_gene × shared_pathway) relationship. The partner gene symbol list is extracted and passed to Phase 2.

### Scale

~8,000 partner genes for APOE via Reactome pathways.

---

## 5. Phase 2 — Variant Annotation and Filtering

**Report:** `gene_to_variant_filtering`

### Input

The gene symbol list from Phase 1.

### Method

1. Gene symbols are resolved to internal entity IDs via alias tables (supports HGNC approved symbols, Ensembl IDs, synonyms).
2. Genomic loci (chromosome, start, end) are retrieved for each gene at the specified genome build.
3. A temporary range table is constructed in the database and joined against the variant master table to retrieve all variants within gene loci using partition-aware per-chromosome queries.
4. Variants are joined to functional annotation tables (`variant_molecular_effects`, `variant_effect_predictions`) to retrieve consequence, impact, prediction scores, and LoF confidence.
5. All filters are applied at the SQL level before data transfer to minimize memory footprint.

### Filters applied

| Filter                  | Parameter          | Value            | Rationale                                                                                      |
| ----------------------- | ------------------ | ---------------- | ---------------------------------------------------------------------------------------------- |
| VEP impact              | `impact_filter`    | `HIGH, MODERATE` | Retains coding variants with functional potential; excludes synonymous and intergenic variants |
| Allele frequency        | `af_max`           | `0.05`           | Excludes common variants (MAF > 5%) unlikely to drive rare functional interactions             |
| Most severe consequence | `most_severe_only` | `True`           | One row per variant; avoids transcript-level redundancy in downstream pair generation          |

Additional filters available but not applied in the default run:

| Filter         | Parameter                      | Notes                                                             |
| -------------- | ------------------------------ | ----------------------------------------------------------------- |
| LoF confidence | `lof_confidence_filter`        | `HC` (high-confidence) or `LC` (low-confidence) LOFTEE annotation |
| AlphaMissense  | `alphamissense_classification` | `likely_pathogenic`, `ambiguous`, `likely_benign`                 |
| CADD Phred     | `cadd_phred_min`               | Minimum CADD score threshold                                      |
| SIFT           | `sift_score_max`               | Maximum SIFT score (lower = more damaging)                        |
| PolyPhen-2     | `polyphen_score_min`           | Minimum PolyPhen-2 score                                          |

### Output (Lista A)

A CSV file (`lista_A.csv`) with one row per (gene × variant), carrying all annotation columns. Exported for use in Phase 2.5.

### Scale

~15,000–500,000 rows depending on gene list size and filters applied.

---

## 6. Phase 2.5 — Genotype–Annotation Integration

**Report:** `variant_list_intersect`

### Input

- **Lista A:** Phase 2 output CSV (biologically annotated variants)
- **Lista B:** Variant list from the study's genotype data (PLINK `.bim` file or VCF)

### Method

Lista A variants are matched against Lista B using a dual-key strategy:

1. **rsID match** (primary): variant IDs matching the pattern `rs\d+` are compared directly.
2. **chr:pos match** (fallback): variants not matched by rsID are matched by (chromosome, position) after normalising chromosome encoding across formats (PLINK integers, VCF `chr`-prefixed strings, Biofilter internal integer encoding).

### Output (Lista C)

- **DataFrame:** all Lista A variants with match status (`matched_rsid`, `matched_chr_pos`, `only_in_a`). Variants with `only_in_a` status are not genotyped in the study dataset and are excluded from downstream analysis.
- **Extract file** (`lista_C.txt`): PLINK-format variant ID list for `--extract`, containing only matched variants.

### Considerations

Variants present in Lista A but absent from Lista B (`only_in_a`) may reflect variants in gnomAD that were not genotyped on the study array, failed genotyping QC, or are absent from the imputation reference panel. These variants are logged for review but do not cause pipeline failure.

---

## 7. LD Pruning

**Tool:** PLINK 1.9

### Input

Lista C (`lista_C.txt`) and the study's PLINK binary dataset.

### Method

```bash
plink --bfile <cohort> \
      --extract lista_C.txt \
      --indep-pairwise 50 5 0.2 \
      --out lista_D
```

LD pruning is performed **exclusively on Lista C** — the biologically relevant, genotyped subset. This is intentional: pruning only the pre-filtered set is computationally faster than pruning the full dataset and avoids the risk of retaining LD proxy variants that have no biological annotation in Lista A.

### Parameters

| Parameter    | Value       | Description                                               |
| ------------ | ----------- | --------------------------------------------------------- |
| Window size  | 50 variants | Sliding window for pairwise LD computation                |
| Step size    | 5 variants  | Window advance step                                       |
| r² threshold | 0.2         | Variants with r² > 0.2 to any retained variant are pruned |

### Output (Lista D)

`lista_D.prune.in` — LD-independent subset of Lista C. These are variants that are biologically annotated, present in the study dataset, and statistically independent.

### Considerations

The r² threshold of 0.2 is a commonly used conservative threshold for interaction analyses. Studies focused on rare coding variants may relax this threshold (e.g., r² < 0.5), as rare functional variants may share partial LD with nearby common variants without being captured by a strict pruning step.

---

## 8. Phase 3 — Interaction Pair Generation

**Report:** `snp_snp_pair_generator`

### Input

- **Lista D** (`lista_D.prune.in`): LD-independent, genotyped, annotated variants
- **Annotation source** (`lista_A.csv`): Phase 2 output — provides annotation for enrichment

### Method

Lista D variant IDs are matched back to Lista A annotations using the same dual-key strategy as Phase 2.5. All annotation columns from Lista A are carried to the output, duplicated with `_a` and `_b` suffixes for each side of the pair.

Pairs are generated according to the specified pairing strategy:

| Strategy      | Description                                                 | Formula          |
| ------------- | ----------------------------------------------------------- | ---------------- |
| `seed_vs_all` | Seed gene variants paired against all partner-gene variants | n_seed × n_other |
| `cross_gene`  | All pairs between variants from different genes             | ≤ n × (n−1) / 2  |
| `all_vs_all`  | All unique pairs                                            | n × (n−1) / 2    |

A safety check estimates the pair count before materialisation; if the estimate exceeds `max_pairs` (default: 1,000,000), the report aborts and returns a descriptive error with a suggestion for reducing scope.

### Default configuration (current study)

| Parameter           | Value         |
| ------------------- | ------------- |
| `pairing_strategy`  | `seed_vs_all` |
| `seed_gene`         | APOE          |
| `exclude_same_gene` | `True`        |
| `max_pairs`         | 1,000,000     |

### Output

A CSV file (`phase3_pairs.csv`) with one row per variant pair. Each row contains full annotation from Lista A for both the seed-side variant (`_a` columns) and the partner-side variant (`_b` columns), plus:

- `same_gene` — boolean flag indicating whether both variants belong to the same gene
- `pairing_strategy` — the strategy used to generate the pair

---

## 9. Implementation Notes

### Software versions

| Tool       | Version          | Reference                                                    |
| ---------- | ---------------- | ------------------------------------------------------------ |
| Biofilter  | 4.1.2            | [biofilter.readthedocs.io](https://biofilter.readthedocs.io) |
| Python     | 3.10+            |                                                              |
| SQLAlchemy | 2.x              |                                                              |
| PostgreSQL | 15+ (production) |                                                              |
| PLINK      | 1.9              | Purcell et al., 2007; Chang et al., 2015                     |
| pandas     | ≥ 2.0            |                                                              |
| NumPy      | ≥ 1.24           |                                                              |

### Reproducibility

- All Biofilter report parameters are logged at runtime and recoverable from the output DataFrame column `resolution_status`.
- The exact gene list, variant list, and pair list at each phase are exported as CSV/TXT files, enabling replication of any downstream step independently.
- The Biofilter database version and ETL package provenance are queryable via `bf.report.run("etl_packages")`.

---

## 10. Limitations and Considerations

**Pathway annotation completeness.** The gene-gene relationships used in Phase 1 are limited to the biological databases ingested into Biofilter 4 (Reactome, KEGG, GO, DisGeNET, etc.). Genes with poor pathway annotation coverage may have fewer or no partner genes identified, even if biologically relevant interactions exist.

**Variant annotation coverage.** Functional annotations (consequence, AlphaMissense, CADD) are available for gnomAD variants only. Variants present in the study cohort but absent from gnomAD will not appear in Lista A and therefore cannot be included in interaction pairs.

**LD pruning and rare variants.** LD pruning can remove rare functional variants when a common proxy variant is retained in the same LD block. For rare-variant studies (MAF < 1%), consider relaxing the r² threshold or performing burden-test aggregation before pair generation.

**Genome build consistency.** All coordinates in Biofilter 4 are aligned to GRCh38. Study cohorts aligned to GRCh37 must be lifted over before Phase 2.5.

**Pair generation scale.** The `seed_vs_all` strategy assumes a single biologically meaningful seed gene. For studies without a clear seed, `cross_gene` pairs may number in the hundreds of millions; aggressive Phase 2 filtering is required to keep the analysis tractable.

---

## 11. References

- Purcell S, et al. PLINK: a tool set for whole-genome association and population-based linkage analyses. _Am J Hum Genet._ 2007;81(3):559–575.
- Chang CC, et al. Second-generation PLINK: rising to the challenge of larger and richer datasets. _Gigascience._ 2015;4:7.
- Cheng J, et al. Accurate proteome-wide missense variant effect prediction with AlphaMissense. _Science._ 2023;381(6664):eadg7492.
- Rentzsch P, et al. CADD: predicting the deleteriousness of variants throughout the human genome. _Nucleic Acids Res._ 2019;47(D1):D886–D894.
- Karczewski KJ, et al. The mutational constraint spectrum quantified from variation in 141,456 humans. _Nature._ 2020;581(7809):434–443. _(gnomAD v3)_
- Jassal B, et al. The reactome pathway knowledgebase. _Nucleic Acids Res._ 2020;48(D1):D498–D503.
- McLaren W, et al. The Ensembl Variant Effect Predictor. _Genome Biol._ 2016;17(1):122.

---

_Document generated from pipeline implementation in Biofilter 4._  
_Companion notebook:_ `notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb`

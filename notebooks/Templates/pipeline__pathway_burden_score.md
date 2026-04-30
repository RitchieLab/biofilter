# Pathway Burden Pipeline: From Gene Hit Lists to Cross-Source Convergence Scores

**Biofilter 4**
**Pipeline version:** 1.0
**Biofilter version:** 4.1.x

---

## Abstract

_This document describes the theoretical design and methodological rationale of the pipeline. Each step is demonstrated in practice in the companion notebook:
[`pipeline__pathway_burden_score.ipynb`](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__pathway_burden_score.ipynb)_

We describe a pipeline for prioritising biological pathways given a list of genes flagged as significant by an upstream genetic analysis (e.g., ExWAS, GWAS). Starting from informal pathway names and a list of significant genes, the pipeline (1) resolves user-provided pathway names against the Biofilter 4 knowledge base using fuzzy or substring matching, accommodating legacy and approximate labels; (2) retrieves the full gene membership of each resolved pathway from curated databases (Reactome, KEGG); (3) intersects pathway membership with the analyst's hit gene list to build per-pathway and per-gene burden tables; (4) computes a **convergence score** for each gene, defined as the number (or weighted sum) of independent knowledge bases that record the gene in any biological relationship — BioGrid (PPI), Reactome (pathways), MONDO (disease ontology), ClinGen (clinical curation), UniProt, and others; (5) rolls convergence into the burden tables, producing a per-pathway summary that combines hit count, hit proportion, and average evidence strength of the genes hitting that pathway. The pipeline operates on summary-level inputs (no individual genotypes required) and is engine-agnostic (PostgreSQL or SQLite).

---

## 1. Motivation

After a genetic analysis identifies a set of significant genes, a common next question is: **which biological processes are these genes collectively pointing to?** Standard pathway enrichment tools (DAVID, Enrichr, GSEA) answer this with a hypergeometric or rank-based test against a fixed pathway database. Useful, but with two limitations relevant to small or curated hit lists:

1. **Single-source pathway annotation.** Most enrichment tools query one database at a time. A gene linked to a pathway only in BioGrid (PPI inference) and not in Reactome (manually curated) may be invisible.
2. **No evidence-weighting per gene.** A hit gene mentioned in 5 independent knowledge bases (BioGrid, Reactome, MONDO, ClinGen, UniProt) carries stronger biological priors than a hit gene mentioned in only one. Standard enrichment treats both equally.

This pipeline addresses both:

**Multi-source pathway lookup.** Phase 2 retrieves gene membership from every relationship source loaded into Biofilter 4 (Reactome and KEGG for pathways, plus any future ones), without requiring the analyst to merge them manually.

**Convergence scoring.** Phase 4 is the methodological contribution: for each ExWAS hit, count the distinct knowledge bases that record the gene in any biological relationship. The score is fully tunable via per-source weights, allowing the analyst to bias toward curated sources (ClinGen, MONDO) over inferred ones (BioGrid PPI) when desired.

The two layers combine to produce a pathway burden score that is **size-aware** (hits per pathway gene), **biologically plural** (pulling from all sources), and **evidence-weighted** (per-gene convergence).

---

## 2. Pipeline Architecture

The pipeline is fully internal to Biofilter 4 — no external tooling required:

```
╔══════════════════════════════════════════════════════════════════════╗
║  [Input]  Analyst inputs                                             ║
║    - pathway_list   : informal pathway names                         ║
║    - exwas_genes    : significant gene symbols                       ║
║    - SOURCE_WEIGHTS : per-source weight overrides (optional)         ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 1 — Pathway Resolution                           ║
║  Report: entity_filter (fuzzy / like / exact)                        ║
║    Input : pathway_list                                              ║
║    Output: found_pathways (canonical primary_names)                  ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 2 — Pathway → Gene Membership                    ║
║  Report: entity_relationship_model (Pathways → Genes)                ║
║    Input : found_pathways                                            ║
║    Output: pathway_gene_map (one row per pathway-gene link)          ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Pandas]  Phase 3 — Burden Tables                                   ║
║  Aggregate hits and proportions                                      ║
║    Output: pathway_table  (per pathway: hit_count, hit_proportion)   ║
║            gene_table     (per gene: pathway_count, is_exwas_hit)    ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 4 — Convergence Scoring                          ║
║  Direct query on entity_relationships + etl_data_sources             ║
║    Input : ExWAS entity_ids + SOURCE_WEIGHTS                         ║
║    Output: gene_convergence (per gene: distinct sources, score)      ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Pandas]  Phase 5 — Convergence Roll-up                             ║
║  Merge convergence into pathway_table and gene_table                 ║
║    Output: pathway_table  (+ mean_convergence, total_convergence)    ║
║            gene_table     (+ convergence_score, evidence_sources)    ║
╚══════════════════════════════════════════════════════════════════════╝
```

**Example scale** (9 pathway names + 65 ExWAS genes):

| Stage                          | N                          |
| ------------------------------ | -------------------------- |
| Input pathway names            | 9                          |
| Resolved pathways (Phase 1)    | ~16 (fuzzy expansion)      |
| Total genes in pathways        | ~3,000 unique              |
| ExWAS hits in resolved set     | ~10–30                     |
| Final per-pathway summary rows | 16 (one per pathway)       |

---

## 3. Data Sources

| Source                     | Content                                        | Used in Phase   |
| -------------------------- | ---------------------------------------------- | --------------- |
| Biofilter 4 knowledge base | Entities, aliases, relationships, data sources | All             |
| Reactome                   | Curated pathways, gene-pathway membership      | 1, 2            |
| KEGG                       | Curated pathways                               | 2 (if loaded)   |
| BioGrid                    | Protein-protein interactions                   | 4 (convergence) |
| MONDO                      | Disease ontology, gene-disease links           | 4 (convergence) |
| ClinGen                    | Clinical gene-disease curation                 | 4 (convergence) |
| UniProt                    | Protein-gene encoding, function                | 4 (convergence) |
| Gene Ontology              | Functional annotation                          | 4 (convergence) |

---

## 4. Phase 1 — Pathway Resolution

**Report:** `entity_filter`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_filter.md)
- [Report Example link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__entity_filter.ipynb)

### Input

A list of informal pathway names. These can be partial labels, common names, or formal database names. Examples: `"estrogen signaling"`, `"inflammation"`, `"DNA repair"`.

### Method

The `entity_filter` report performs case-insensitive matching against the alias table for entities in the `Pathways` group. Three modes:

| Mode    | Behavior                                              | When to use                       |
| ------- | ----------------------------------------------------- | --------------------------------- |
| `exact` | Match `alias_norm` literally                          | Inputs are known canonical names  |
| `like`  | Substring match (`%term%` in either direction)        | Inputs are partial labels         |
| `fuzzy` | rapidfuzz `token_sort_ratio` against all aliases     | Inputs are informal/misspelled   |

`group_filter="Pathways"` ensures matches are scoped to pathway entities only, avoiding cross-domain bleed (e.g., a gene whose alias coincidentally contains "signaling").

### Key parameters

| Parameter              | Value used | Rationale                                                                |
| ---------------------- | ---------- | ------------------------------------------------------------------------ |
| `match_mode`           | `fuzzy`    | Tolerant to informal labels common in study writeups                     |
| `group_filter`         | `Pathways` | Restricts to pathway entities, eliminates cross-group collisions         |
| `similarity_threshold` | `75`       | Permissive enough to catch substring-style queries (e.g. "alzheimer")    |

### Output (`found_pathways`)

A list of canonical `primary_name` values (Reactome IDs like `R-HSA-111885`). Pathways with `observation == "not found"` are excluded; they are surfaced separately for manual review.

---

## 5. Phase 2 — Pathway → Gene Membership

**Report:** `entity_relationship_model`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_relationship_model.md)

### Input

`found_pathways` from Phase 1.

### Method

Resolves each pathway entity and traverses one hop in `entity_relationships` filtered by `relationship_type IN ('in_pathway')`. For each pathway, returns every member gene with the supporting `data_source_id`.

### Key parameters

| Parameter              | Value used        | Rationale                                                |
| ---------------------- | ----------------- | -------------------------------------------------------- |
| `input_entity_groups`  | `["Pathway"]`     | Treats inputs as pathways                                |
| `output_entity_groups` | `["Gene"]`        | Returns only gene neighbours                             |
| `relationship_scope`   | `input_to_any`    | Returns every gene linked to any input pathway           |

### Output (`pathway_gene_map`)

One row per `(pathway, gene)` link, with `data_source_id` for provenance. The same gene may appear under multiple pathways — that's intentional; pathway membership is many-to-many.

---

## 6. Phase 3 — Burden Tables

**Engine:** pandas (no Biofilter call)

### Inputs

- `pathway_gene_map` (Phase 2 output)
- `exwas_genes` (analyst-provided list of significant gene symbols)

### Method

Two grouped aggregations:

**Pathway table.** For each pathway:
- `total_genes` = distinct genes in the pathway (full membership)
- `exwas_hit_count` = distinct genes from `exwas_genes` that fall in this pathway
- `hit_proportion` = `exwas_hit_count / total_genes`
- `genes` and `exwas_genes` columns hold the explicit lists

**Gene table.** For each gene:
- `pathway_count` = number of distinct pathways the gene belongs to
- `pathways` = the list
- `is_exwas_hit` = boolean flag

### Why hit_proportion matters

Pathways vary in size by orders of magnitude — a pathway with 5 genes versus one with 500. A raw hit count favours large pathways. The proportion normalises this and acts as a crude size-adjusted enrichment.

### Output

`pathway_table` and `gene_table` — both ready for the convergence enrichment in Phase 5.

---

## 7. Phase 4 — Convergence Scoring

**Engine:** direct ORM query (no report call)

### Input

- ExWAS gene list (resolved to `entity_id` via `entity_filter`)
- `SOURCE_WEIGHTS` dict (per-source float; defaults to 1.0 each)

### Method

For each ExWAS gene, query `entity_relationships` for **all** rows where the gene appears as either `entity_1` or `entity_2`, then count distinct `data_source_id` values. The data source ID is mapped to a human-readable name via `etl_data_sources`. The convergence score is the sum of weights for the distinct sources observed:

```
convergence_score(gene) = Σ_{s ∈ sources(gene)} SOURCE_WEIGHTS[s]
```

With all weights = 1.0, the score reduces to "count of distinct knowledge bases that mention this gene". Tuning weights lets the analyst bias the score toward curated evidence over inferred evidence, or toward disease-specific sources for clinically motivated hypotheses.

### Default weights

| Source                   | Default weight | Type                       |
| ------------------------ | -------------- | -------------------------- |
| `biogrid`                | 1.0            | PPI (inferred + curated)   |
| `reactome` / `_relationships`             | 1.0            | Curated pathways           |
| `mondo` / `_relationships`               | 1.0            | Disease ontology           |
| `clingen`                | 1.0            | Clinical curation (high)   |
| `uniprot_relationships`  | 1.0            | Protein function           |

Sources not in `SOURCE_WEIGHTS` contribute 0 (silently excluded). Add `gene_ontology`, `kegg_pathways`, `gtex_v10_brain_eqtl`, etc. as needed for the analysis.

### Output (`gene_convergence`)

| Column              | Meaning                                                   |
| ------------------- | --------------------------------------------------------- |
| `gene`              | Gene primary symbol                                       |
| `evidence_sources`  | Sorted list of distinct sources mentioning the gene       |
| `convergence_count` | Length of `evidence_sources`                              |
| `convergence_score` | Weighted sum of `SOURCE_WEIGHTS` over `evidence_sources`  |

### Suggested weight calibrations

| Use case                        | Weight bias                                              |
| ------------------------------- | -------------------------------------------------------- |
| High-confidence clinical scope  | `clingen=2.0`, `mondo=1.5`, `biogrid=0.5`                |
| PPI-driven mechanism            | `biogrid=2.0`, `uniprot_relationships=1.5`               |
| Pathway-centric (default)       | All curated sources = 1.0; inferred sources = 0.5        |

---

## 8. Phase 5 — Convergence Roll-up

**Engine:** pandas

### Method

`gene_convergence` is merged into `gene_table` on the gene symbol. Each pathway in `pathway_table` then receives:

- `mean_convergence` = average `convergence_score` over the pathway's ExWAS hits
- `total_convergence` = sum of `convergence_score` over the pathway's ExWAS hits

`pathway_table` is re-sorted by `total_convergence` descending — pathways whose hits are well-characterised across knowledge bases rank higher.

### Output

The final `pathway_table` carries:

| Column              | Source        | Meaning                                                |
| ------------------- | ------------- | ------------------------------------------------------ |
| `pathway_id`        | Phase 2       | Reactome ID                                            |
| `pathway_name`      | Phase 2       | Pathway primary alias                                  |
| `total_genes`       | Phase 3       | Pathway size (gene count)                              |
| `exwas_hit_count`   | Phase 3       | ExWAS genes that hit this pathway                      |
| `exwas_genes`       | Phase 3       | List of those genes                                    |
| `hit_proportion`    | Phase 3       | `exwas_hit_count / total_genes`                        |
| `mean_convergence`  | Phase 5       | Average evidence per ExWAS hit                         |
| `total_convergence` | Phase 5       | Sum of evidence across ExWAS hits                      |

The combination of `hit_proportion` (size-adjusted enrichment) and `total_convergence` (evidence-weighted hit count) gives a richer ranking than either alone.

---

## 9. Implementation Notes

### Software versions

| Tool       | Version          |
| ---------- | ---------------- |
| Biofilter  | 4.1.2            |
| Python     | 3.10+            |
| SQLAlchemy | 2.x              |
| PostgreSQL | 15+ (production) |
| SQLite     | 3.x (local)      |
| pandas     | ≥ 2.0            |
| rapidfuzz  | ≥ 3.0            |

### Reproducibility

- All Biofilter report calls log their parameters and elapsed time.
- The exact `pathway_list`, `exwas_genes`, and `SOURCE_WEIGHTS` are visible in the notebook cells; saving the notebook itself preserves the analysis.
- The Biofilter database state (which sources are loaded) is queryable via `bf.report.run("etl_status")`.

### Engine support

The pipeline is **engine-agnostic**: every report and ORM call is portable across PostgreSQL and SQLite. Fuzzy matching uses `rapidfuzz` client-side rather than `pg_trgm`.

---

## 10. Limitations and Considerations

**Pathway annotation completeness.** Phase 2 retrieves gene membership only from databases ingested into Biofilter 4. Pathways that exist in the source database but not in BF4 (e.g., KEGG variants not loaded) are invisible.

**Convergence ≠ pathogenicity.** A gene with high convergence is well-characterised, not necessarily disease-relevant. The score reflects research attention, not biological causality. Combine with downstream variant-level annotation (gnomAD, AlphaMissense) for clinical interpretation.

**Source weight choices are subjective.** Default weights treat all sources equally, but ClinGen (clinically curated) and BioGrid (high-throughput PPI) carry very different evidence quality. Weight calibration should reflect the analyst's prior on each source. Document the chosen weights when publishing.

**Pathway resolution false positives.** Fuzzy matching with low thresholds (< 70) can pull unrelated pathways. Always inspect the `result_fuzzy` output and prune false matches before proceeding to Phase 2.

**Single-organism scope.** All knowledge sources currently loaded reflect human (Homo sapiens) annotations. The pipeline does not adapt automatically to other species.

**Independence assumption.** The convergence score treats sources as independent evidence, but BioGrid and UniProt share underlying data; MONDO is partly derived from clinical sources. The score is therefore an **upper bound** on truly independent evidence.

---

## 11. References

- Jassal B, et al. [The reactome pathway knowledgebase.](https://pubmed.ncbi.nlm.nih.gov/31691815/) _Nucleic Acids Res._ 2020;48(D1):D498–D503.
- Oughtred R, et al. [The BioGRID interaction database: 2019 update.](https://pubmed.ncbi.nlm.nih.gov/30476227/) _Nucleic Acids Res._ 2019;47(D1):D529–D541.
- Vasilevsky NA, et al. [Mondo: Unifying diseases for the world, by the world.](https://www.medrxiv.org/content/10.1101/2022.04.13.22273750v3) _medRxiv_ 2022.
- Rehm HL, et al. [ClinGen — The Clinical Genome Resource.](https://pubmed.ncbi.nlm.nih.gov/26014595/) _N Engl J Med._ 2015;372(23):2235–2242.
- The UniProt Consortium. [UniProt: the Universal Protein Knowledgebase in 2023.](https://pubmed.ncbi.nlm.nih.gov/36408920/) _Nucleic Acids Res._ 2023;51(D1):D523–D531.

---

_Document generated from pipeline implementation in Biofilter 4._
_Companion notebook:_ `notebooks/Templates/pipeline__pathway_burden_score.ipynb`

# Report Catalog

Complete index of all reports available in Biofilter 4.
Each report has a **name** (used in CLI and Python API), a brief description,
and links to its explain guide and interactive notebook tutorial where available.

For general usage — how to run, list, and introspect reports — see [Reports](reports.md).

---

## Running any report

```bash
# CLI
biofilter report run --report-name <name> [--param KEY=VALUE ...] [--output file.csv]
biofilter report explain --report-name <name>
biofilter report run --report-name <name> --params-template
```

```python
# Python API
df = bf.report.run("<name>", param1=value1, param2=value2)
```

---

## ETL & Platform Monitoring

Reports for inspecting the state of the ETL pipeline and the knowledge base.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `etl_status` | Current status of all ETL packages (active, last run, row counts) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_etl_status.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__etl_status.ipynb) |
| `etl_packages` | Full provenance log of all ETL executions with timestamps and file hashes | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_etl_packages.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__etl_packages.ipynb) |
| `platform_data_statistics` | Row counts and coverage metrics across all master tables | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_platform_data_statistics.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__platform_data_statistics.ipynb) |
| `db_pg_table_stats` | PostgreSQL table sizes, row estimates, and bloat metrics *(PostgreSQL only)* | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_db_pg_table_stats.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__db_pg_table_stats.ipynb) |
| `db_pg_index_stats` | PostgreSQL index usage, size, and scan counts *(PostgreSQL only)* | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_db_pg_index_stats.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__db_pg_index_stats.ipynb) |

---

## Entity & Relationship

Reports for exploring the biological entity graph.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `entity_filter` | Filter and list entities (genes, pathways, diseases, …) by type, source, or name pattern | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_filter.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__entity_filter.ipynb) |
| `entity_relationship_model` | Retrieve all entities related to an input list through shared biological groups (pathways, diseases, GO, PPI) | — | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__entity_relationship_model.ipynb) |

---

## Annotation Masters

Reference tables exposing the full content of each biological domain in the knowledge base.
Useful for exploring available terms before using them as filters in other reports.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `annotation_master_gene` | All genes with HGNC symbol, Ensembl ID, locus, and source provenance | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_gene.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_gene.ipynb) |
| `annotation_master_pathway` | All pathways across all source systems (Reactome, KEGG, …) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_pathway.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_pathway.ipynb) |
| `annotation_master_protein` | All proteins with UniProt IDs and gene mappings | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_protein.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_protein.ipynb) |
| `annotation_master_disease` | All diseases with MONDO/ClinGen IDs and gene associations | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_disease.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_disease.ipynb) |
| `annotation_master_go` | All Gene Ontology terms (BP, MF, CC) with gene memberships | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_go.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_go.ipynb) |
| `annotation_master_chemical` | All chemical compounds (ChEBI) with gene and pathway associations | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_chemical.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_chemical.ipynb) |
| `annotation_master_variant` | Full annotation for input variants: frequencies, pathogenicity scores, VEP consequences per transcript, AlphaMissense | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_variant.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_variant.ipynb) |

---

## Variant Analysis

Reports for annotating and filtering genomic variants.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `variant_binning` | Assign variants to genomic bins; useful for burden-test preparation | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_binning.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_binning.ipynb) |
| `variant_gene_location_model` | Map variants to overlapping gene loci with distance and region annotations | — | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_gene_location_model.ipynb) |
| `variant_annotation_expanded` | Full annotation expansion for a variant list (consequence, AF, predictions) | — | — |
| `variant_single_gene_annotation` | **Phase 1** — Given a seed variant, returns the seed gene and all partner genes sharing biological context | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_single_gene_annotation.ipynb) |
| `gene_to_variant_filtering` | **Phase 2** — Collect and filter variants across a gene list with SQL-level pathogenicity filters | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__gene_to_variant_filtering.ipynb) |

---

## Variant Interaction Modeling

Direct variant-to-variant interaction modeling from a pre-genotyped input list.
Both variants in every pair come from the input — no DB expansion.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `variant_modeling` | Input variants → gene overlap → group co-membership → Variant×Variant pairs with group_support_count weight | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_modeling.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_modeling.ipynb) |

---

## SNP×SNP Interaction Pipeline

Reports implementing the biologically-informed SNP×SNP interaction workflow.
See the full pipeline tutorial and methods document for end-to-end guidance.

| Resource | Link |
|---|---|
| Pipeline notebook | [pipeline__from_single_variant_to_interactions.ipynb](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| Pipeline methods doc | [pipeline__from_single_variant_to_interactions.md](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.md) |

| Report | Phase | Description | Explain | Notebook |
|---|---|---|---|---|
| `variant_single_gene_annotation` | Phase 1 | Seed variant → partner gene list via biological network | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_single_gene_annotation.ipynb) |
| `gene_to_variant_filtering` | Phase 2 | Gene list → filtered, annotated variant set (Lista A) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__gene_to_variant_filtering.ipynb) |
| `variant_list_intersect` | Phase 2.5 | Lista A ∩ Lista B → Lista C (genotyped subset, PLINK-ready) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_list_intersect.md) | [Pipeline notebook](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| `snp_snp_pair_generator` | Phase 3 | Lista D → annotated interaction pairs with configurable pairing strategy | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_snp_snp_pair_generator.md) | [Pipeline notebook](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| `snp_snp_model` | Legacy | Earlier SNP×SNP pair model — expands variants from gene loci (superseded by `variant_modeling`) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_snp_snp_model.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__snp_snp_model.ipynb) |

---

## Utilities

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `template` | Blank report template for development and testing | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_template.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__qry_template.ipynb) |

---

## Coverage summary

| Status | Count |
|---|---|
| Reports with explain guide + notebook | 19 |
| Reports with explain guide only | 2 (`variant_list_intersect`, `snp_snp_pair_generator` — covered by pipeline notebook) |
| Reports with notebook only | 2 (`entity_relationship_model`, `variant_gene_location_model`) |
| Reports with neither | 1 (`variant_annotation_expanded`) |
| **Total** | **24** |

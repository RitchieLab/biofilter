# Test of Annotation Persistence for Removed Genes

Github Issue: https://github.com/RitchieLab/biofilter/issues/15

## <span style="color:orange;">🔍 IN ANALYSIS</span>

## Problem Description
This test aims to validate the consistency of gene annotations across versions 2.4.2 and 2.4.3 of Biofilter. In version 2.4.2, it was observed that, when running pathway annotation on an initial list of genes, some genes that should have been annotated were ignored. Re-running the process on a subsequent version with a subset of previously removed genes resulted in correct annotations for some genes that should have been included in the initial run.

## Context
During the initial run on Biofilter 2.4.2 with a list of 55,889 genes, Biofilter ignored 22,674 unrecognized identifiers and 114 ambiguous ones, generating outputs for 18,482 unique genes. Re-running the list of 37,407 previously removed genes produced additional pathway annotations for 18 genes, indicating that the input changed between iterations.

In Biofilter 2.4.3, the issue was not replicated, suggesting that the update may have addressed the inconsistent behavior. This test ensures that version 2.4.3 maintains consistency in annotations across different runs for similar data sets.

## Input
- **Original gene file**: `ROSMAP_RNAseq_FPKM_gene_ensembl_list_edit.txt`
- **Removed genes file**: `ROSMAP_RNAseq_removedbiofilt.txt`

## Procedures
1. **Run with the Original Gene List**: Run the Biofilter command on version 2.4.3 with the original gene file and check the annotations.
    ```bash
    biofilter.py --knowledge ~/group/datasets/loki/loki-20220926.db --gene-file ROSMAP_RNAseq_FPKM_gene_ensembl_list_edit.txt --gene-identifier-type ensembl_gid --filter gene group source --source kegg reactome go --verbose --report-configuration --prefix ROSMAP_RNAseq_ENSEMBL_gene_pathways_2.4.3 --overwrite
    ```

2. **Run with the Subset of Removed Genes**: Repeat the command using the file with removed genes.
    ```bash
    biofilter.py --knowledge ~/group/datasets/loki/loki-20220926.db --gene-file ROSMAP_RNAseq_removedbiofilt_2.4.3.txt --gene-identifier-type ensembl_gid --filter gene group source --source kegg reactome go --verbose --report-configuration --prefix ROSMAP_RNAseq_removedbiofilt_2.4.3 --overwrite
    ```
3. **Validation**: Compare the outputs of the two runs to ensure that all genes that should have been annotated in the first test are present in the second and that annotations are consistent.

## Success Criteria
The test will be considered successful if:
- All 18 genes identified as annotatable in the second test are correctly annotated in the first run of version 2.4.3.
- No discrepancies exist between the annotations of the same genes when run with different input files, ensuring consistency across runs.

This test documents the improvement in the Biofilter update and ensures that the system maintains predictable and consistent behavior.

---
---


# ANALYSIS


## Run 1: Run all Genes

In the first analysis of Run 1, the input file contained **55,889 genes** based on Ensembl coding. Out of these, only **18,496 genes** returned associated groups, after applying a filter based on three data sources (GO, REACTOME, and KEGG).

After analyzing the genes that did not return group information, two key observations emerged:


#### 1. Missing Genes from LOKI

LOKI was loaded with Ensembl gene data from ENTREZ and only considers the **GRCh38** genome build. As a result, genes like **ENSG00000005955**, which is referenced exclusively in GRCh37 (where it maps to the gene **GGNBP2**), are absent from the LOKI database. The `biopolymer_name` table, which is responsible for mapping different nomenclatures to a single `biopolymer_id`, does not include these GRCh37-specific genes.

A point of uncertainty here is that **GGNBP2** is coded as **ENSG00000278311** in GRCh38. However, these two identifiers (from GRCh37 and GRCh38) have no references to each other in the ensembl.org database, raising questions about their proper mapping.

Additionally, there is an issue with the gene **ENSG00000004866**, which is mapped to **two distinct `biopolymer_id` entries** despite having the same description. My understanding is that there should be a **1-to-N relationship** between the `biopolymer` and `biopolymer_name` tables, based on the source data. However, the current implementation suggests an **N-to-N relationship**, which could lead to inconsistencies. Since the LOKI database does not enforce explicit relationship structures, relying on implicit rules within the code, it is prone to these types of issues. 

At this point, I require assistance from someone familiar with ENTREZ data to verify if this behavior is correct.


#### 2. Genes Without Associated Groups

Another factor reducing the number of genes in the filter output is the presence of genes registered as `biopolymers` but without associated groups for the three sources analyzed (GO, REACTOME, and KEGG). This information is tracked in the `group_biopolymer` table, and the absence of group associations directly impacts the filtering process.

---

### Filter Flow Summary

A summary of the filter flow is available below, with additional details in the file **Analysis_run_1.xlsx**.

Number of Genes
- (=) 55,889 Input Ensembl Genes Codes
- (-) 22,673 Not found in biopolymer_name table
- (-)    114 Ambiguous Genes
- (=) 33,102 Genes found in LOKI biopolymer_name table
- (-) 14,606 No references in group_biopolymer table to sources (3:GO, 5:REACTOME, 7:KEGG)  
- (=) 18,496 Unique Genes in Outcome Result

---

### Recommendations and Next Steps

For this Run 1, my main consideration is whether LOKI should account for Ensembl Gene codes from **GRCh37**. If it is decided that GRCh37 should not be supported, then the process is functioning as designed. However, if GRCh37 support is required, we could consider adding extractors for GRCh37 Ensembl genes in future LOKI versions. These genes could be retained in the `biopolymer_name` table and mapped to a single gene in the `biopolymer` table. 

Further validation would be needed to determine if this approach is feasible.

---

## Run 2: Run Genes Out from the first run

In Run 2, only the genes that did not return data during the first execution (Run 1) were processed. The expected scenario was that no groups would be returned, but the outcome mirrored the issue described earlier: 314 records with 18 unique genes were returned.

Upon examining the LOKI database, these genes are indeed present. However, in the group_biopolymer table, where group associations for these genes are stored, the source_id is recorded as 0. This creates a problem, as it is not possible to trace the original source of these groups due to the lack of relationship keys in LOKI.

To address this, I will:

1. Debug the system to investigate why BIOFILTER includes these records in the query output.
  --> Analysis on Analysis_run_2.doc
  --> in the Analysis_run_1_source_id.docx was report the source_id between the group e group_biopolymer tables to address in next versions.

  --> I identified an issue where, when querying a gene (e.g., **ENSG00000055208**) in the `biopolymer_name` table, other genes (e.g., **ENSG00000228408**) may share the same `biopolymer_id`. In such cases, the system only considers the first gene it locates and disregards the others. This behavior explains why 18 genes were missing from the results during the first execution.
  
After isolating these 18 genes and running the process again, they appeared in the results. There seems to be an option that allows handling genes with duplicate `biopolymer_id`s. However, this option verifies if the last gene has the same `biopolymer_id` and excludes it from the results if the condition is met.
  
I believe the output or log reports could be more detailed to provide clarity to users and prevent confusion.

--> I need to check the biofilter query in my manual test!!

2. Analyze LOKI further to determine the reason for the source_id being set to 0 in these cases.







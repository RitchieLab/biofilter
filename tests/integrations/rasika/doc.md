# Test of Annotation Persistence for Removed Genes

Github Issue: https://github.com/RitchieLab/biofilter/issues/15

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
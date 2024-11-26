# Test of build 37 LOKI

Github Issue: https://github.com/RitchieLab/LOKI/issues/16

## Problem Description
The Biofilter system supports compatibility with both GRCh37 (b37) and GRCh38 (b38) genome assemblies. However, there is a notable discrepancy in the recency of the LOKI database versions available on the LPC cluster. Specifically, the GRCh37 version of LOKI was last updated in 2014, while the GRCh38 version was updated in 2022. This creates a challenge for users relying on GRCh37, as they are limited to an outdated dataset, potentially affecting the accuracy and relevance of their analyses.

Efforts to maintain separate LOKI databases for each genome assembly are labor-intensive and may lead to discrepancies between versions. A more streamlined and synchronized approach is needed to ensure up-to-date and consistent support across both assemblies.

## Context
The most recent updates to the LOKI database have been built exclusively on the GRCh38 genome assembly. However, there is a need to accommodate users who rely on other genome assemblies, such as GRCh37, for their analyses. This includes supporting both filtering and annotation tasks while ensuring compatibility with the assembly version specified by the user.

To address this, Biofilter needs to allow users to input genome assemblies different from the one the LOKI database is built on. For example, a user might provide GRCh37 positions (`--ucsc-build-version 19`) while the LOKI database is constructed on GRCh38. In such cases, Biofilter can dynamically map positions between assemblies using tools like **LiftOver**. 

With this approach:
- Biofilter will compare the assembly version of the user input with the LOKI database version.
- If a mismatch is detected, Biofilter will use a conversion table to map positions from one assembly to the other (e.g., GRCh37 to GRCh38).
- All queries (filters and annotations) will be processed using the converted positions, ensuring compatibility with the LOKI database.
- The output will retain the original input positions (e.g., GRCh37) as labels, while the results will reflect the converted coordinates aligned with the LOKI database's assembly (e.g., GRCh38).

This solution ensures that users working with different genome assemblies can seamlessly perform analyses without requiring separate databases for each assembly version, maintaining flexibility and accuracy in their workflows.

## Input
- **Positions Template**: `input_filename.txt`

## Procedures
1. **Run Issue Test**: 

## TODO
1. **check if the ../data_out/outcomes.tsv is acceptable**

## Success Criteria
Not defined yet
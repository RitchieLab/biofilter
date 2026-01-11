# Data Sources & Knowledge Coverage

Biofilter 4 is built around the concept of a **persistent, local biological knowledge base**.  
Instead of querying external resources in real time, Biofilter ingests, normalizes, and versions biological knowledge from multiple authoritative data sources into a unified, entity-centric schema.

Each external resource is referred to as a **Data Source** and is ingested through one or more **Data Transformation Packages (DTPs)**.  
Once ingested, this knowledge can be reused across analyses, projects, reports, and computational environments without repeated downloads or reprocessing.

This chapter provides an overview of:
- Which data sources are currently supported
- What type of biological knowledge each source contributes
- How sources map to Biofilter entities and relationships
- How users should interpret coverage and limitations

---

## Philosophy: Local, Persistent Knowledge

Biofilter follows a long-standing design principle introduced in earlier versions (via LOKI):

> *Biological knowledge should be downloaded, curated, and stored locally, enabling fast, reproducible, and offline analysis.*

In Biofilter 4, this philosophy is preserved but implemented through a more explicit and extensible architecture:
- Entity-centric identity layer
- Clear separation between master data and relationships
- Provenance and version tracking via ETL Packages
- Incremental updates without rebuilding the entire knowledge base

---

## What Is a Data Source in Biofilter 4?

A **Data Source** represents a specific external biological resource (e.g. dbSNP, HGNC, Reactome).

Each Data Source is associated with:
- A **Source System** (e.g. NCBI, EBI)
- One or more **DTPs**
- A defined role in the knowledge base (master data, relationships, or both)

From a user perspective, Data Sources define:
- Which biological entities exist
- Which relationships can be queried
- Which annotations are available in reports

---

## Types of Data Sources

Biofilter 4 classifies data sources based on how they contribute knowledge.

### Master Data Sources

These sources create **canonical biological entities**.

They:
- Create stable `Entity` and `Aliases` records
- Populate domain-specific master and attributes tables
- Define identity and aliases

Examples:
- Genes
- Variants
- Proteins
- Diseases
- Pathways
- Ontology terms
- Chemicals

Without master data sources, no relationships can exist.

---

### Relationship Data Sources

These sources **connect existing entities** but do not define new identities.

They:
- Create `EntityRelation` records
- Depend on master entities already being loaded
- Encode biological knowledge such as interactions or associations

Examples:
- Gene–Pathway
- Gene–Disease
- Chemical–Disease
- Protein–Protein interactions

---

### Hybrid Data Sources

These sources provide **both master entities and relationships**.

They:
- Define new biological concepts
- Connect them to existing entities

Examples:
- Reactome
- GWAS Catalog
- PharmGKB
- MONDO

---

## High-Level Coverage Summary

| Source System | Data Source | Type | Entities Created | Relationships Created |
|--------------|------------|------|------------------|-----------------------|
| NCBI | dbSNP | Hybrid | Variant | Variant–Gene |
| NCBI | Gene | Master | Gene | — |
| HGNC | Gene | Master | Gene | — |
| Ensembl | Gene | Master | Gene | — |
| UniProt | Protein | Hybrid | Protein | Protein–Gene |
| GO Consortium | Gene Ontology | Master | GO Term | Gene–GO |
| Reactome | Pathways | Hybrid | Pathway | Gene–Pathway |
| MONDO | Disease Ontology | Hybrid | Disease | Gene–Disease |
| Pfam | Protein Families | Master | Pfam | Protein–Pfam |
| NHGRI | GWAS Catalog | Relationship | — | Variant–Trait |
| ChEBI	| Chemical Entities | Master | Chemical | — |
| BioGRID | Protein Interactions | Relationship | — | Protein–Protein | 
| ClinGen | Gene–Disease Validity | Relationship | — | Gene–Disease | 
| KEGG | Pathways | Hybrid | Pathway | Gene–Pathway | 
---

## Data Sources by Domain


### 🧬 Variant Data Sources

#### NCBI dbSNP

**Source system:** NCBI  
**Category:** Hybrid (Master + Relationships)

**Primary entities created**
- Variant (SNP)

**Relationships created**
- Variant → Gene

**DTPs**
- `dtp_variant_ncbi_master.py`
- `dtp_variant_ncbi.py`

**What this source provides**
- Canonical rsIDs
- Genomic coordinates (GRCh37 / GRCh38)
- Reference and alternate alleles
- Variant-to-gene mappings

**How Biofilter uses this source**
- Forms the backbone of variant representation
- Enables gene-centric variant annotation
- Supports positional and region-based queries

---

### 🧬 Gene Data Sources

#### HGNC

**Source system:** HGNC  
**Category:** Master

**Entities created**
- Gene

**DTP**
- `dtp_gene_hgnc.py`

**Role in Biofilter**
- Canonical human gene symbols
- Stable gene identifiers
- Primary alias resolution


#### NCBI Gene

**Source system:** NCBI  
**Category:** Master

**Entities created**
- Gene

**DTP**
- `dtp_gene_ncbi.py`

**Role in Biofilter**
- Entrez Gene identifiers
- Genomic locations
- Cross-references to other gene systems



#### Ensembl Gene

**Source system:** Ensembl  
**Category:** Master

**Entities created**
- Gene

**DTP**
- `dtp_gene_ensembl.py`

**Role in Biofilter**
- Ensembl gene identifiers
- Genomic coordinates
- Cross-database normalization

---

### 🧬 Protein Data Sources

#### UniProt

**Source system:** UniProt  
**Category:** Hybrid

**Entities created**
- Protein

**Relationships**
- Protein → Gene

**DTPs**
- `dtp_uniprot.py`
- `dtp_uniprot_relationships.py`

**Role in Biofilter**
- Canonical protein identifiers
- Isoforms and accessions
- Gene–protein mappings



#### Pfam

**Source system:** Pfam  
**Category:** Master + Relationships

**Entities created**
- Protein Family (Pfam)

**Relationships**
- Protein → Pfam

**DTP**
- `dtp_pfam.py`

---

### 🧬 Pathways & Ontologies

#### Gene Ontology (GO)

**Category:** Master + Relationships

**Entities**
- GO Term

**Relationships**
- Gene → GO Term

**DTP**
- `dtp_go.py`



#### Reactome

**Category:** Hybrid

**Entities**
- Pathway

**Relationships**
- Gene → Pathway

**DTPs**
- `dtp_reactome.py`
- `dtp_reactome_relationships.py`



### 🧬 Disease & Trait Sources

#### MONDO

**Category:** Hybrid

**Entities**
- Disease

**Relationships**
- Gene → Disease

**DTPs**
- `dtp_mondo.py`
- `dtp_mondo_relationships.py`



#### NHGRI GWAS Catalog

**Category:** Relationship

**Relationships**
- Variant → Trait

**DTP**
- `dtp_gwas.py`

---

## How Data Sources Feed Reports and Queries

Data Sources define **what can be queried** in Biofilter.

Reports and queries:
- Only operate over ingested knowledge
- Reflect the current set of loaded Data Sources
- Are fully reproducible based on ETL provenance

Understanding data source coverage is essential for:
- Correct interpretation of reports
- Assessing missing knowledge
- Designing downstream analyses

---

## Key Takeaways

- Biofilter 4 provides **explicit, documented biological coverage**
- Each Data Source has a **clear semantic role**
- Master data and relationships are intentionally separated
- DTPs ensure reproducibility, provenance, and extensibility
- Users can reason about *what is known* and *where it came from*

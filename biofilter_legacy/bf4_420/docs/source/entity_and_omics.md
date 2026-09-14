# Entity Model and Omics Domains

## Why `Entity` Exists

Biofilter 4 uses an entity-centric model so different biological domains can share identity and relationships.

Instead of keeping each source isolated, BF4 stores a common entity layer and links domain records to it. This enables cross-domain queries and reusable knowledge.

## Core Entity Objects

At the center of the schema:

- `EntityGroup`
  - semantic type bucket (for example: Variants, Genes, Proteins, Diseases)
- `Entity`
  - persistent concept record with activity/conflict flags and ETL provenance
- `EntityAlias`
  - names/codes/synonyms from multiple systems (`alias_type`, `xref_source`)
- `EntityRelationshipType`
  - relationship semantics (typed edge meaning)
- `EntityRelationship`
  - directed link between two entities with provenance

Practical effect:

- you can resolve aliases from many sources to one entity identity
- you can traverse relationships across domains without hardcoded paths

## Domain-Specific Master Data

The entity core is complemented by domain tables (master data), such as:

- genes (`GeneMaster` and gene-related tables)
- variants (variant master/effects/GWAS tables)
- proteins (`ProteinMaster`, Pfam links)
- pathways (`PathwayMaster`)
- gene ontology (`GOMaster`, `GORelation`)
- diseases (`DiseaseMaster`)
- chemicals (`ChemicalMaster`)

These domain tables provide rich attributes, while entities/aliases/relationships provide integration.

## Omics Domains in BF4

### Operational Domains (current)

Domains with active schema + ETL/report usage today:

- Variants
- Genes
- Proteins
- Pathways
- Gene Ontology
- Diseases
- Chemicals

These groups define semantic space and allow gradual expansion without redesigning the core model.

## How This Appears in ETL and Reports

- ETL loads source-specific master/relationship data and writes provenance (`ETLPackage`).
- Reports such as `entity_filter` and `entity_relationship_model` operate directly on this entity layer.
- Because identities are persistent, updates can be incremental and still query-consistent across domains.

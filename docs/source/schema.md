# Database Schema

Entity-relationship diagram of the Biofilter 4 database, derived directly from the SQLAlchemy ORM models.

The schema is organized into four logical zones:

- **ETL infrastructure** — source systems, data sources, and execution packages
- **Core entity layer** — canonical entities, aliases, relationships, and genomic locations
- **Domain master tables** — gene, protein, pathway, disease, chemical, GO
- **Variant tables** — partitioned tables for large-scale variant data (gnomAD, VEP, GWAS)

---

## ER Diagram

```{mermaid}
erDiagram

    %% ── ETL INFRASTRUCTURE ──────────────────────────────────────────────────

    etl_source_systems {
        int id PK
        string name
        bool active
    }
    etl_data_sources {
        int id PK
        string name
        int source_system_id FK
        string data_type
        string dtp_script
        bool active
    }
    etl_packages {
        int id PK
        int data_source_id FK
        string status
        string extract_status
        string transform_status
        string load_status
    }

    %% ── CORE ENTITY LAYER ───────────────────────────────────────────────────

    entity_groups {
        int id PK
        string name
    }
    entities {
        bigint id PK
        int group_id FK
        int data_source_id FK
        int etl_package_id FK
        bool is_active
        bool has_conflict
    }
    entity_aliases {
        bigint id PK
        bigint entity_id FK
        int group_id FK
        string alias_value
        string alias_type
        string xref_source
        bool is_primary
    }
    entity_relationship_types {
        int id PK
        string code
        string description
    }
    entity_relationships {
        bigint id PK
        bigint entity_1_id FK
        int entity_1_group_id FK
        bigint entity_2_id FK
        int entity_2_group_id FK
        int relationship_type_id FK
        int data_source_id FK
    }
    entity_locations {
        bigint id PK
        bigint entity_id FK
        int assembly_id FK
        int chromosome
        bigint start_pos
        bigint end_pos
        string strand
    }

    %% ── REFERENCE TABLES ────────────────────────────────────────────────────

    genome_assemblies {
        int id PK
        string accession
        string assembly_name
        string chromosome
    }
    omic_status {
        int id PK
        string name
    }

    %% ── GENE DOMAIN ─────────────────────────────────────────────────────────

    gene_locus_groups {
        int id PK
        string name
    }
    gene_locus_types {
        int id PK
        string name
    }
    gene_masters {
        int id PK
        bigint entity_id FK
        string symbol
        int locus_group_id FK
        int locus_type_id FK
        int omic_status_id FK
    }
    gene_groups {
        int id PK
        string name
    }
    gene_group_memberships {
        int gene_id FK
        int group_id FK
    }

    %% ── PROTEIN DOMAIN ──────────────────────────────────────────────────────

    protein_masters {
        int id PK
        string protein_id
    }
    protein_entities {
        int id PK
        bigint entity_id FK
        int protein_id FK
        bool is_isoform
    }
    protein_pfams {
        int id PK
        string pfam_acc
        string pfam_id
        string type
    }
    protein_pfam_links {
        int protein_id FK
        int pfam_pk_id FK
    }

    %% ── PATHWAY DOMAIN ──────────────────────────────────────────────────────

    pathway_masters {
        int id PK
        bigint entity_id FK
        string pathway_id
        string description
    }

    %% ── DISEASE DOMAIN ──────────────────────────────────────────────────────

    disease_masters {
        int id PK
        bigint entity_id FK
        string disease_id
        int omic_status_id FK
    }
    disease_groups {
        int id PK
        string name
    }
    disease_group_memberships {
        int id PK
        int disease_id FK
        int group_id FK
    }

    %% ── CHEMICAL DOMAIN ─────────────────────────────────────────────────────

    chemical_masters {
        int id PK
        bigint entity_id FK
        string chemical_id
        int omic_status_id FK
        string formula
        float mass
    }

    %% ── GENE ONTOLOGY DOMAIN ────────────────────────────────────────────────

    go_masters {
        int id PK
        bigint entity_id FK
        string go_id
        string name
        string namespace
    }
    go_relations {
        int id PK
        int parent_id FK
        int child_id FK
        string relation_type
    }

    %% ── VARIANT DOMAIN (partitioned by chromosome) ──────────────────────────

    variant_masters {
        bigint variant_id PK
        int chromosome PK
        bigint position_start
        bigint position_end
        string reference_allele
        string alternate_allele
        string rsid
        float af
    }
    variant_molecular_effects {
        bigint variant_id FK
        int chromosome
        string transcript_id
        int consequence_id FK
        int impact_id FK
        bool is_most_severe_for_variant
        string hgvsc
        string hgvsp
    }
    variant_effect_predictions {
        bigint variant_id FK
        int chromosome
        string predictor_key
        string predictor_name
        float score
        string classification
    }
    variant_consequence_groups {
        int id PK
        string name
    }
    variant_consequence_categories {
        int id PK
        string name
    }
    variant_consequences {
        int id PK
        string name
        int consequence_group_id FK
        int consequence_category_id FK
        int severity_rank
    }
    variant_impacts {
        int id PK
        string name
        int severity_rank
    }
    variant_gwas {
        bigint id PK
        string snp_id
        string raw_trait
        string mapped_trait
        float p_value
    }
    variant_gwas_snp {
        bigint id PK
        bigint variant_gwas_id FK
        bigint snp_id
    }
    variant_snp_merges {
        bigint rs_obsolete_id PK
        bigint rs_canonical_id PK
    }

    %% ── RELATIONSHIPS ───────────────────────────────────────────────────────

    %% ETL
    etl_source_systems ||--o{ etl_data_sources : "has"
    etl_data_sources ||--o{ etl_packages : "tracks"

    %% Entity core
    entity_groups ||--o{ entities : "classifies"
    entities ||--o{ entity_aliases : "has"
    entity_groups ||--o{ entity_aliases : "scopes"
    entities ||--o{ entity_relationships : "as entity_1"
    entities ||--o{ entity_relationships : "as entity_2"
    entity_relationship_types ||--o{ entity_relationships : "typed by"
    entities ||--o{ entity_locations : "located at"
    genome_assemblies ||--o{ entity_locations : "assembly"

    %% Gene
    entities ||--o{ gene_masters : "gene"
    gene_locus_groups ||--o{ gene_masters : "locus group"
    gene_locus_types ||--o{ gene_masters : "locus type"
    omic_status ||--o{ gene_masters : "status"
    gene_masters ||--o{ gene_group_memberships : "belongs to"
    gene_groups ||--o{ gene_group_memberships : "has"

    %% Protein
    entities ||--o{ protein_entities : "entity link"
    protein_masters ||--o{ protein_entities : "protein"
    protein_masters ||--o{ protein_pfam_links : "has domain"
    protein_pfams ||--o{ protein_pfam_links : "domain"

    %% Pathway / Disease / Chemical / GO
    entities ||--o{ pathway_masters : "pathway"
    entities ||--o{ disease_masters : "disease"
    omic_status ||--o{ disease_masters : "status"
    disease_masters ||--o{ disease_group_memberships : "belongs to"
    disease_groups ||--o{ disease_group_memberships : "has"
    entities ||--o{ chemical_masters : "chemical"
    omic_status ||--o{ chemical_masters : "status"
    entities ||--o{ go_masters : "GO term"
    go_masters ||--o{ go_relations : "parent"
    go_masters ||--o{ go_relations : "child"

    %% Variants
    variant_masters ||--o{ variant_molecular_effects : "effects"
    variant_consequence_groups ||--o{ variant_consequences : "group"
    variant_consequence_categories ||--o{ variant_consequences : "category"
    variant_masters ||--o{ variant_effect_predictions : "predictions"
    variant_gwas ||--o{ variant_gwas_snp : "indexed SNP"
```

---

## Key design notes

- **Entity as the universal anchor** — every domain master table (`gene_masters`, `disease_masters`, `pathway_masters`, `chemical_masters`, `go_masters`) links back to a single row in `entities`. This enables cross-domain relationship queries through `entity_relationships` without domain-specific join logic.

- **`entity_aliases` as the name registry** — all names, symbols, synonyms, and external codes (HGNC, Ensembl, OMIM, ICD10, MONDO, ChEBI) are stored here. `alias_type` is `preferred`, `synonym`, or `code`; `xref_source` identifies the originating system.

- **`entity_relationships` as the graph surface** — connects any two entities with a typed, directed edge. Supports multi-hop traversal (e.g., gene → pathway → disease) entirely within SQL.

- **Variant tables are partitioned** — `variant_masters`, `variant_molecular_effects`, `variant_effect_predictions`, `variant_regulatory_elements`, and `variant_gene_regulatory_evidence` are partitioned by `chromosome` on PostgreSQL (plain tables on SQLite). No physical FK constraints — integrity is ETL-enforced for performance at scale.

- **Provenance on every row** — `data_source_id` and `etl_package_id` are present on virtually every table, enabling full traceability back to the source system and the exact ETL execution that produced each record.

- **`etl_packages` as the audit log** — each ETL run produces one `ETLPackage` row per DataSource, tracking extract/transform/load status, row counts, file hashes, and timing.
```

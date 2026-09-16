# Database Schema

The schema Biofilter writes through and validates against, derived from the
SQLAlchemy models. It comes in two halves that are shaped very differently,
and the difference is the most important thing on this page.

**The entity graph** is relational and connected. Everything is anchored to
`entities`, joined through surrogate keys and real foreign keys. This is the
half the ER diagram below describes:

- **ETL infrastructure** — source systems, data sources, execution packages
- **Core entity layer** — entities, aliases, relationships, genomic locations
- **Domain masters** — gene, protein, pathway, disease, chemical, GO

**The variant tables** are none of those things. They carry no `entity_id`
and no foreign keys, they are keyed by `chromosome:position:ref:alt`, and
there is one table per source with no cross-source joins. They are declared
imperatively rather than as ORM classes (`map_variant_*` in
`models/model_variants.py`, registered through `utils/db_loader.py`), and
each definition describes the parquet its DTP writes, column for column.

Because they participate in no relationships, an ER diagram says nothing
useful about them — they are listed with their columns
[further down](#variant-tables) instead.

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

    %% ── VARIANT DIMENSIONS (the only variant tables in the entity zone) ────

    variant_consequences {
        string name PK
        int severity_rank
        string consequence_group
        string consequence_category
        string description
        bool is_active
    }
    variant_impacts {
        string name PK
        int severity_rank
        string description
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
```


---

(variant-tables)=
## Variant tables

Keyed by `chromosome`, `position`, `reference_allele`, `alternate_allele` —
the natural key, written as `variant_key` where a single column is needed.
No surrogate ids, no foreign keys, one table per source.

### `variant_masters`

One row per ALT allele of the gnomAD joint callset. Identity and frequency:

`chromosome`, `position`, `reference_allele`, `alternate_allele`,
`variant_key`, `rsid`, `quality_filter`, `exomes_filters`, `genomes_filters`,
`ac_exomes`, `ac_genomes`, `ac_joint`, `af_exomes`, `af_genomes`, `af_joint`,
`an_exomes`, `an_genomes`, `an_joint`, `nhomalt_exomes`, `nhomalt_genomes`,
`nhomalt_joint`, `faf95_joint`, `faf99_joint`, `fafmax_faf95_max_joint`,
`fafmax_faf95_max_gen_anc_joint`, `af_grpmax_joint`, `grpmax_joint`

### `variant_molecular_effects`

One row per variant × transcript, as VEP annotated it. This is the largest
table in a bundle by a wide margin:

`chromosome`, `position`, `reference_allele`, `alternate_allele`,
`variant_key`, `allele`, `consequence`, `impact`, `symbol`, `gene`,
`hgnc_id`, `symbol_source`, `feature_type`, `feature`, `biotype`, `exon`,
`intron`, `hgvsc`, `hgvsp`, `amino_acids`, `codons`, `strand`,
`variant_class`, `canonical`, `mane_select`, `mane_plus_clinical`, `ensp`,
`lof`, `lof_filter`, `lof_flags`, `lof_info`

`symbol` and `gene` are the strings VEP emitted — this is where variants
meet genes, and the join is on the name, never on an entity id.

### `variant_rsid`

The rsID index, separate so a lookup by rsID does not scan the annotation:

`chromosome`, `position`, `reference_allele`, `alternate_allele`, `rsid`

### `variant_predictions`

In-silico predictors carried in the gnomAD VEP release:

`chromosome`, `position`, `reference_allele`, `alternate_allele`,
`cadd_raw_score`, `cadd_phred`, `revel_max`, `sift_max`, `polyphen_max`,
`spliceai_ds_max`, `pangolin_largest_ds`, `phylop`

### `variant_alphamissense`

AlphaMissense pathogenicity, one row per variant × transcript:

`chromosome`, `position`, `reference_allele`, `alternate_allele`,
`predictor_key`, `transcript_id`, `predictor_name`, `predictor_version`,
`score`, `classification`, `details`, `data_source_id`, `etl_package_id`

### `variant_gtex`

GTEx eQTL evidence, one row per variant × tissue × gene:

`chromosome`, `position`, `reference_allele`, `alternate_allele`,
`evidence_key`, `gene_id`, `bio_context`, `qtl_type`, `beta`, `se`,
`p_value`, `n`, `effect_allele`, `details`, `data_source_id`,
`etl_package_id`

### `variant_gwas`

GWAS Catalog associations. Note this one keeps the catalogue's own spelling
(`chr_id`, `chr_pos`) rather than the natural key the others use:

`pubmed_id`, `raw_trait`, `mapped_trait`, `mapped_trait_id`, `parent_trait`,
`parent_trait_id`, `chr_id`, `chr_pos`, `reported_gene`, `mapped_gene`,
`snp_id`, `snp_rank`, `snp_risk_allele`, `risk_allele_frequency`, `context`,
`intergenic`, `p_value`, `pvalue_mlog`, `odds_ratio_beta`, `ci_text`,
`initial_sample_size`, `replication_sample_size`, `platform`,
`data_source_id`, `etl_package_id`

---

## Key design notes

- **Entity as the universal anchor** — every domain master table (`gene_masters`, `disease_masters`, `pathway_masters`, `chemical_masters`, `go_masters`) links back to a single row in `entities`. This enables cross-domain relationship queries through `entity_relationships` without domain-specific join logic.

- **`entity_aliases` as the name registry** — all names, symbols, synonyms, and external codes (HGNC, Ensembl, OMIM, ICD10, MONDO, ChEBI) are stored here. `alias_type` is `preferred`, `synonym`, or `code`; `xref_source` identifies the originating system.

- **`entity_relationships` as the graph surface** — connects any two entities with a typed, directed edge. Supports multi-hop traversal (e.g., gene → pathway → disease) entirely within SQL.

- **Variant tables are split by chromosome** — in a bundle each one is a directory of parquet files, one per chromosome, presented as a single view. They have no physical FK constraints, by design rather than for performance: they link to genes through the symbols and HGNC ids VEP emitted, which is what lets the variant branch be built without the entity graph existing yet.

- **Severity ordering lives in two small tables** — `variant_consequences` and `variant_impacts` are keyed by name, not by a surrogate id, and exist for one reason: `variant_molecular_effects` carries the VEP term and the impact class as strings, and nothing in `missense_variant` or `MODIFIER` says how bad it is. Join on the name to get `severity_rank`.

- **Provenance on every row** — `data_source_id` and `etl_package_id` are present on virtually every table, enabling full traceability back to the source system and the exact ETL execution that produced each record.

- **`etl_packages` as the audit log** — each ETL run produces one `ETLPackage` row per DataSource, tracking extract/transform/load status, row counts, file hashes, and timing.
```

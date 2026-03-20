# DTP Explain: `dtp_reactome_relationships`

## 1. Data source and pipeline role

- `data_source.name`: `reactome_relationships`
- `source_system`: `Reactome`
- source URL in seed: `not_applicable`
- format in seed: `not_applicable`

Pipeline role:
- relationship-only DTP
- consumes relationship staging produced by `dtp_reactome`
- loads rows into `EntityRelationship`

Recommended dependency order:
1. `reactome` transform/load
2. gene/protein sources loaded (to maximize alias resolution)
3. `reactome_relationships` load

## 2. Extract

Behavior:
- no extraction performed
- returns success with informational message

Reason:
- raw files are extracted by `reactome` DTP

## 3. Transform

Behavior:
- no transformation performed
- returns success with informational message

Reason:
- relationship staging (`relationship_data.parquet`) is created by
  `reactome` transform

## 4. Load

Input file:
- `<processed_dir>/<source_system>/reactome/relationship_data.parquet`
- note: reads from parent source folder `reactome` (not its own data source name)

Main goal:
- resolve entity IDs and relationship type IDs, then insert pathway
  relationships into `EntityRelationship`

### 4.1 Entity resolution strategy

Pathway side (`entity_1_id`):
- resolves Reactome pathway IDs through `EntityAlias`
- query is restricted to primary aliases of the parent data source `reactome`

Related side (`entity_2_id`):
- for `pathway_parent`, maps relation value through same pathway ID map
- for other relation types, resolves by alias value in `EntityAlias`
  (batch lookup)

### 4.2 Relationship type mapping

`relation_type` -> `EntityRelationshipType.code`:
- `pathway_parent` -> `part_of`
- `gene_symbol` -> `in_pathway`
- `ensembl_gene` -> `in_pathway`
- `ensembl_protein` -> `in_pathway`
- `uniprot_protein` -> `in_pathway`
- fallback default -> `in_pathway`

### 4.3 Valid rows and insert

Validity filter:
- keeps only rows where both `entity_1_id` and `entity_2_id` were resolved

Dedup:
- casts `entity_2_id` to int
- drops duplicates on:
  - `entity_1_id`
  - `entity_2_id`
  - `relationship_type_id`

Group IDs:
- fetches `Entity.group_id` for all used entity IDs
- fills `entity_1_group_id` and `entity_2_group_id`

Insert operation:
- iterates valid rows and calls `get_or_create_entity_relationship(...)`
- writes `data_source_id = reactome_relationships` and current `etl_package_id`
- commits in batch at end

## 5. Rejections and diagnostics

Rows not loaded:
- rows with unresolved `entity_1_id` or `entity_2_id` are not inserted
- saved to:
  - `<processed_dir>/<source_system>/reactome/relationship_data_not_loaded.csv`

Common causes:
- missing upstream Reactome pathway load
- missing genes/proteins/entities that should match aliases
- alias mismatches across sources

Operational behavior:
- switches DB to write mode
- drops/creates entity index group around load


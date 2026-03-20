# DTP Explain: `dtp_mondo`

## 1. Data source and pipeline role

- `data_source.name`: `mondo`
- `source_system`: `MONDO`
- reference URL (seed): `https://purl.obolibrary.org/obo/mondo.json`
- format: JSON (OBO Graph)

Pipeline role:
- loads MONDO diseases into the Disease domain
- creates disease entities, aliases, `DiseaseMaster`, and subset groups
- stages cross-domain disease links in `relationship_data.parquet` for
  `dtp_mondo_relationships`

Recommended sequence:
1. run `dtp_mondo` (extract + transform + load disease master)
2. run other master DTPs needed by MONDO links (Genes, Chemicals, etc.)
3. run `dtp_mondo_relationships`

## 2. Extract

Source:
- HTTP GET (streaming) to `datasource.source_url`
- expected response: MONDO JSON graph

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- file: `mondo.json`

Hash behavior:
- computes hash from `mondo.json`
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `mondo.json`

### 3.1 Master data output

Node parsing:
- keeps only nodes with normalized ID prefix `MONDO:`
- extracts:
  - `mondo_id`
  - `label`
  - `description` (from `meta.definition.val`)
  - `iri`
  - `is_obsolete` (from `meta.deprecated`)
  - `synonyms` (list)
  - `xrefs` (list)
  - `subsets` (list, normalized from MONDO subset URLs)

Master filter:
- excludes root dummy term `MONDO:0000001`

Master output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- in debug mode: `master_data.csv`

### 3.2 Relationship staging output

Edge parsing:
- normalizes source/target IDs from:
  - compact `PREFIX:CODE`
  - OBO PURLs (`.../obo/PREFIX_CODE`)
  - identifiers.org (`.../prefix/code`)
  - fallback URI fragments

Group mapping (prefix -> Biofilter group):
- `MONDO` -> `diseases`
- `HGNC` / `NCBIGENE` -> `genes`
- `CHEBI` -> `chemicals`
- `UBERON` -> `anatomy`
- `SO` -> `sequenceontology`
- others -> `Unknown`

Predicate mapping:
- `is_a` -> `is_a`
- `RO_0004003` -> `Disease_has_disruption`
- `RO_0004026` -> `located_in`
- `RO_0002162` -> `in_taxon`
- fallback -> original predicate URI

Relationship staging output:
- `<processed_dir>/<source_system>/<data_source>/relationship_data.parquet`
- in debug mode: `relationship_data.csv`

Staging columns:
- `term1_group`, `term1_prefix`, `term1_code`
- `term2_group`, `term2_prefix`, `term2_code`
- `relation_type`

## 4. Load

Main goal:
- load MONDO disease entities and disease master records
- create subset groups and memberships

Target models:
- `Entity` / `EntityAlias` (group `Diseases`)
- `DiseaseMaster`
- `DiseaseGroup`
- `DiseaseGroupMembership`

Alias mapping:
- `mondo_id` -> primary alias (`code`, source `MONDO`)
- `label` -> alias (`label`, source `MONDO`)
- `synonyms` -> alias (`synonyms`, source `MONDO`)

Additional alias enrichment:
- parses `xrefs` (`PREFIX:CODE`) and appends alias:
  - `alias_value = CODE`
  - `alias_type = code`
  - `xref_source = PREFIX`

Load flow:
1. read `master_data.parquet`
2. resolve entity group `Diseases`
3. resolve `OmicStatus` (`active` / `deactive`)
4. create disease groups from unique `subsets`
5. for each disease row:
   - skip missing `mondo_id` or root `MONDO:0000001`
   - create/get entity + aliases
   - create `DiseaseMaster` when missing by `(disease_id, data_source_id)`
   - create `DiseaseGroupMembership` links by subset

Status behavior:
- obsolete terms (`is_obsolete=True`) are inserted as:
  - `Entity.is_active = False`
  - `DiseaseMaster.omic_status_id = deactive`

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts if DataFrame is empty
- requires OmicStatus values `active` and `deactive`

Row-level skips:
- missing/invalid MONDO ID
- root/dummy `MONDO:0000001`

Operational behavior:
- drops disease and entity index groups before load
- recreates entity index group after load

## 6. Practical caveats

- This DTP only stages relationships; final insertion into `EntityRelationship` is done by `dtp_mondo_relationships`.
- Relationship staging may include relation codes not seeded in `EntityRelationshipType`; these rows can be rejected later.
- Prefixes not mapped to known entity groups become `Unknown` and are typically rejected in relationship load.
- Current implementation has helper `_transform_edges()` that writes `entity_relations.csv`, but the active transform path writes `relationship_data.parquet`.

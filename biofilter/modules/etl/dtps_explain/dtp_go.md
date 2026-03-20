# DTP Explain: `dtp_go`

## 1. Data source and pipeline role

- `data_source.name`: `gene_ontology`
- `source_system`: `GO`
- reference URL (seed): `https://current.geneontology.org/ontology/go-basic.obo`
- format: OBO

Pipeline role:
- loads Gene Ontology terms into the GO domain
- creates GO entities/aliases and `GOMaster` records
- loads hierarchical GO relations into `GORelation`

## 2. Extract

Source:
- HTTP GET to `datasource.source_url`
- expected response: GO OBO content (`go-basic.obo`)

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- file: `geneontology.obo`

Hash behavior:
- computes hash from `geneontology.obo`
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `geneontology.obo`

Term parsing (`[Term]` blocks):
- `go_id` from `id:`
- `name` from `name:`
- `namespace` from `namespace:`
- `definition` from `def:`
- `is_obsolete` from `is_obsolete:`
- `replaced_by` from `replaced_by:`
- `consider` (list) from repeated `consider:`
- `alt_ids` (list) from repeated `alt_id:`
- `synonyms` (list) from repeated `synonym:`
- `xrefs` (list) from repeated `xref:`

Relation parsing:
- reads only `is_a:` lines
- emits relation rows with:
  - `parent_id`
  - `child_id`
  - `relation_type` (`is_a`)

Output files:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- `<processed_dir>/<source_system>/<data_source>/relations_data.parquet`
- in debug mode also writes:
  - `master_data.csv`
  - `relations_data.csv`

Transform guards/filters:
- aborts when `geneontology.obo` does not exist
- ignores content after `[Typedef]`
- normalizes list-like columns (`alt_ids`, `consider`, `synonyms`, `xrefs`) as arrays
- fills missing `is_obsolete` as `False`

## 4. Load

Main goal:
- create GO entities and aliases
- load `GOMaster` rows
- load GO hierarchical edges in `GORelation`

Target models:
- `Entity` / `EntityAlias` (group `Gene Ontology`)
- `GOMaster`
- `GORelation`

Alias mapping:
- `go_id` -> primary alias (`code`, source `GO`)
- `alt_ids` -> additional alias (`code`, source `GO`)

Term load flow:
1. read `master_data.parquet`
2. skip rows with missing `go_id` or `name`
3. skip rows where `is_obsolete=True`
4. create/get GO entity + aliases
5. insert `GOMaster` if `go_id` is not present yet

Relation load flow:
1. read `relations_data.parquet`
2. resolve both GO terms via `GOMaster.go_id`
3. skip relation if either endpoint does not exist
4. skip duplicates by `(child_id, parent_id, relation_type)`
5. insert into `GORelation`

Destination highlights:
- `GOMaster.go_id`, `GOMaster.name`, `GOMaster.namespace`, `GOMaster.entity_id`
- `GORelation.parent_id`, `GORelation.child_id`, `GORelation.relation_type`
- provenance via `data_source_id` and `etl_package_id`

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts when DataFrame is empty

Row-level skips (terms):
- missing `go_id`
- missing `name`
- obsolete terms (`is_obsolete=True`)

Row-level skips (relations):
- unresolved GO term on either side
- existing duplicate relation

Operational behavior:
- switches DB to write mode
- drops/creates GO and Entity index groups around load

## 6. Practical caveats

- Current transform captures only `is_a` relations from OBO.
- Existing comments mention other relation types (`part_of`, `regulates`), but they are not parsed in current implementation.
- Relation column names in `relations_data.parquet` are produced by the current code path and should be interpreted according to load behavior (the loader uses those fields exactly as written).

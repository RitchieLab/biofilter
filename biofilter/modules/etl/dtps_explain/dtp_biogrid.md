# DTP Explain: `dtp_biogrid`

## 1. Data source and pipeline role

- `data_source.name`: `biogrid`
- `source_system`: `BioGRID`
- reference URL (seed): `https://downloads.thebiogrid.org/Download/BioGRID/Latest-Release/BIOGRID-ALL-LATEST.mitab.zip`
- format (seed): `mitab` (inside ZIP: `.mitab.txt`)

Pipeline role:
- ingests molecular interactions from BioGRID
- resolves interactions to existing master entities
- loads relationships into `EntityRelationship`

Operational dependency:
- run this after master datasets are loaded for:
  - Genes
  - Proteins
  - Chemicals

## 2. Extract

Source:
- downloads from `data_source.source_url`

Raw output:
- `<raw_dir>/<source_system>/<data_source>/BIOGRID-ALL-LATEST.mitab.zip`

Extract behavior:
- validates schema compatibility
- performs streamed HTTP download (`chunk_size=8192`)
- returns error if HTTP status is not `200`

Hash behavior:
- computes `current_hash` from downloaded ZIP via `compute_file_hash(...)`
- returns `(ok, message, current_hash)`

## 3. Transform

Input file:
- `<raw_dir>/<source_system>/<data_source>/BIOGRID-ALL-LATEST.mitab.zip`

Output file:
- `<processed_dir>/<source_system>/<data_source>/relationship_data.parquet`
- in debug mode also writes:
  - `relationship_data.csv`

### 3.1 MITAB reading strategy

Behavior:
- opens ZIP and finds first inner file ending with `.mitab.txt`
- reads in chunks (`chunksize=100_000`) with selected columns only:
  - `Alt IDs Interactor A`
  - `Alt IDs Interactor B`
  - `Interaction Identifiers`
  - `Interaction Detection Method`
  - `Interaction Types`
  - `Taxid Interactor A`
  - `Taxid Interactor B`

Filter:
- keeps only rows where:
  - `Taxid Interactor A == "taxid:9606"`
  - `Taxid Interactor B == "taxid:9606"`

### 3.2 Identifier extraction and expansion

ID parsing rule:
- from `Alt IDs Interactor A/B`, split by `|`
- keep entries with prefix:
  - `entrez gene/locuslink` -> Genes (`ENTREZ`)
  - `uniprot` -> Proteins (`UNIPROT`)
  - `chebi` or `pubchem` -> Chemicals (`CHEBI`)
- value is taken as suffix after `:` (`p.split(":")[-1]`)

Metadata parsed:
- `interaction_id` from first token of `Interaction Identifiers`
- `interaction_method` from text inside `(...)` in `Interaction Detection Method`
- `interaction_type` from text inside `(...)` in `Interaction Types`

Expanded relationship combinations:
- Gene ↔ Gene
- Gene ↔ Protein
- Protein ↔ Protein
- Protein ↔ Chemical
- Gene ↔ Chemical

Post-processing:
- concatenates all expanded rows
- drops exact duplicates (`drop_duplicates()`)
- writes parquet output

Note:
- `interaction_id`, `interaction_method`, `interaction_type` are preserved in processed parquet, but current load step does not persist these fields into DB.

## 4. Load

Input file:
- `<processed_dir>/<source_system>/<data_source>/relationship_data.parquet`

Main goal:
- map both interaction endpoints to existing entity IDs
- insert `interacts_with` relationships into `EntityRelationship`

### 4.1 Mapping strategy

EntityGroup mapping:
- resolves IDs for group names:
  - `Genes`
  - `Proteins`
  - `Chemicals`

Alias resolution by group:
- Genes:
  - `group_id = Genes`
  - `alias_type = "symbol"`
  - `is_primary = True`
- Proteins:
  - `group_id = Proteins`
  - `alias_type != "name"`
- Chemicals:
  - `group_id = Chemicals`
  - `alias_type != "formula"`

Endpoint mapping key:
- uses tuple key `(group_name, source_name, alias_value)` to reduce ambiguity between groups/sources

Relationship type:
- resolves `EntityRelationshipType` by `code = "interacts_with"`
- aborts load if missing

### 4.2 Cleaning and unresolved aliases

Resolved rows:
- keep only rows with both `entity_1_id` and `entity_2_id`
- cast mapped IDs to `int`

Unresolved rows:
- rows missing either endpoint ID are written to:
  - `<processed_dir>/<source_system>/<data_source>/biogrid_missing_aliases.csv`

### 4.3 Existing relationship check

Behavior:
- loads existing relationships for current `data_source_id`
- normalizes pair order with `(min(entity_1_id, entity_2_id), max(...))`
- removes rows already present for this source

If no new rows:
- returns success with message `No new relationships`

### 4.4 Insert strategy

DB tuning:
- drops relationship indexes before insert
- recreates indexes after insert

Insert mode:
- bulk insert in chunks (`chunk_size=10_000`)
- commits per chunk
- tracks inserted total in `total_relationships`

Failure behavior:
- on chunk insert error: rollback, log error, stop remaining inserts
- if index recreation fails: returns error status

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires processed parquet file
- aborts if input dataframe is empty
- aborts if `interacts_with` relationship type is missing

Transform filters:
- strict human-only filter using exact taxid string `taxid:9606` on both interactors

Load exclusions:
- unresolved endpoint IDs
- rows already present for same data source (pair-normalized match)

## 6. Practical caveats

- Taxonomy filter is strict string equality (`taxid:9606`). If source format varies (for example, includes parenthetical suffix), records may be excluded.
- ID extraction for `entrez gene/locuslink` is prefix-based and keeps the token suffix as-is; depending on source payload, this may return numeric IDs or symbols.
- Current deduplication against DB uses normalized endpoint pairs and does not include interaction metadata (`interaction_id`, method, type).
- Current load persists endpoints + type + provenance, but does not persist BioGRID interaction metadata fields.

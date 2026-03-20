# DTP Explain: `dtp_reactome`

## 1. Data source and pipeline role

- `data_source.name`: `reactome`
- `source_system`: `Reactome`
- reference URL (seed): `https://reactome.org/download/current/`
- format: TXT (plus ZIP side files)

Pipeline role:
- loads Reactome pathways into the Pathway domain
- prepares relationship staging used by `dtp_reactome_relationships`

Typical sequence:
1. run `reactome` (extract + transform + load)
2. run `reactome_relationships` (load relationships from staged file)

## 2. Extract

Downloads these files into raw staging:
- `ReactomePathways.txt` (used as hash reference)
- `ReactomePathwaysRelation.txt`
- `ReactomePathways.gmt.zip`
- `Ensembl2Reactome.txt`
- `UniProt2Reactome.txt`

Raw output path:
- `<raw_dir>/<source_system>/<data_source>/`

Hash behavior:
- computes hash from `ReactomePathways.txt`
- returns `(ok, message, current_hash)`

## 3. Transform

### 3.1 Master pathways output

Input:
- `ReactomePathways.txt` with columns:
  - `reactome_id`
  - `pathway_name`
  - `species`

Filter:
- keeps only `species == "Homo sapiens"`

Output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- in debug mode: `master_data.csv`

### 3.2 Relationship staging output

Additional input files:
- `ReactomePathwaysRelation.txt`
- `ReactomePathways.gmt.zip` (`*.gmt` files)
- `Ensembl2Reactome.txt`
- `UniProt2Reactome.txt`

Common filtering basis:
- `valid_ids = set(reactome_id)` from Homo sapiens master pathways

Generated relation types:
- `pathway_parent`
  - from `ReactomePathwaysRelation.txt`
  - keeps only parent/child where both IDs are in `valid_ids`
- `gene_symbol`
  - from `ReactomePathways.gmt.zip`
  - maps pathway name -> reactome ID, then emits one row per gene symbol
  - evidence set to `IEA`
- `ensembl_gene` / `ensembl_protein`
  - from `Ensembl2Reactome.txt`
  - keeps only `species == "Homo sapiens"` and `reactome_id in valid_ids`
  - `ENSG*` => `ensembl_gene`, `ENSP*` => `ensembl_protein`
- `uniprot_protein`
  - from `UniProt2Reactome.txt`
  - keeps only `species == "Homo sapiens"` and `reactome_id in valid_ids`

Relationship staging output:
- `<processed_dir>/<source_system>/<data_source>/relationship_data.parquet`
- in debug mode: `relationship_data.csv`

## 4. Load

Main goal:
- load Reactome pathway entities + pathway masters (not relationships)

Target models:
- `Entity` / `EntityAlias` for group `Pathways`
- `PathwayMaster`

Alias mapping:
- `reactome_id` -> primary alias (`code`, source `Reactome`)
- `pathway_name` -> additional alias (`name`, source `Reactome`)

Load behavior:
- reads `master_data.parquet`
- creates/gets pathway entity + aliases
- checks existing `PathwayMaster` by `pathway_id = reactome_id`
- inserts only missing pathways

Destination:
- `PathwayMaster.pathway_id = reactome_id`
- `PathwayMaster.description = pathway_name`
- `PathwayMaster.entity_id = resolved entity_id`
- provenance: `data_source_id` is filled

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts if DataFrame is empty

Row-level skip:
- missing empty `reactome_id`

Operational behavior:
- switches DB to write mode
- drops/creates pathway and entity index groups around load

Important dependency:
- relationship records are staged here but loaded by
  `dtp_reactome_relationships`, which reads `relationship_data.parquet`.


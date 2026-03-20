# DTP Explain: `dtp_kegg`

## 1. Data source and pipeline role

- `data_source.name`: `kegg_pathways`
- `source_system`: `KEGG`
- reference URL (seed): `https://rest.kegg.jp/list/pathway/hsa`
- format: TXT

Pipeline role:
- loads KEGG pathways into the Pathway domain
- creates pathway entities and `PathwayMaster` rows
- does not load pathway relationships in this DTP

## 2. Extract

Source:
- HTTP GET to `datasource.source_url`
- expected response: plain text list (`path:<id>\t<description>`)

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- file: `kegg_pathways.txt`

Hash behavior:
- computes hash from `kegg_pathways.txt`
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `kegg_pathways.txt`

Parsing logic:
- reads file line by line
- skips empty lines
- expects exactly 2 tab-separated columns
- removes `path:` prefix from pathway ID

Produced fields:
- `pathway_id`
- `description`

Output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- in debug mode also writes `master_data.csv`

Filters:
- malformed lines (`len(parts) != 2`) are ignored

## 4. Load

Main goal:
- create/update pathway entities and KEGG pathway master records

Target models:
- `Entity` / `EntityAlias` for EntityGroup `Pathways`
- `PathwayMaster`

Alias mapping:
- `pathway_id` -> primary alias (`code`, source `KEGG`)
- `description` -> additional alias (`name`, source `KEGG`)

Load behavior:
- reads `master_data.parquet`
- resolves entity group `Pathways`
- creates/gets entity + aliases
- checks existing pathway by `PathwayMaster.pathway_id`
- inserts only when pathway ID does not exist yet

Destination:
- `PathwayMaster.pathway_id = pathway_id`
- `PathwayMaster.description = description`
- `PathwayMaster.entity_id = resolved entity_id`
- provenance: `data_source_id` and `etl_package_id`

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts if DataFrame is empty

Row-level skip:
- missing empty `pathway_id`

Operational behavior:
- switches DB to write mode
- drops/creates pathway and entity index groups around load


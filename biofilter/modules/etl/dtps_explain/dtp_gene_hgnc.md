# DTP Explain: `dtp_gene_hgnc`

## 1. Data source and pipeline role

- `data_source.name`: `hgnc`
- `source_system`: `HGNC`
- reference URL (seed): `https://rest.genenames.org/fetch/all`
- format: JSON

Pipeline role:
- first step of the gene ingestion flow
- loads the canonical baseline of Genes
- creates core symbols, aliases, and gene metadata used by downstream DTPs

Recommended order:
1. `hgnc`
2. `gene_ncbi`
3. `ensembl`

## 2. Extract

Source:
- performs HTTP GET to `datasource.source_url`
- `Accept: application/json`

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- file: `hgnc_data.json`

Hash behavior:
- computes hash over `hgnc_data.json`
- returns `(ok, message, current_hash)` for ETL package/version control

## 3. Transform

Input:
- `hgnc_data.json`
- reads `data["response"]["docs"]`

Output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- in debug mode also writes `master_data.csv`

Transform behavior:
- no business filter is applied in this step
- payload is normalized into a DataFrame and persisted as-is for load

## 4. Load

Main goal:
- create/update canonical Gene entities and GeneMaster rows from HGNC

Target groups/models:
- EntityGroup: `Genes`
- OmicStatus: `active`
- Entity and entity aliases/names via:
  - `get_or_create_entity(...)`
  - `get_or_create_entity_name(...)`
- Gene metadata via `get_or_create_gene(...)`
- Locus metadata via:
  - `get_or_create_locus_group(...)`
  - `get_or_create_locus_type(...)`
- Gene groups parsed from HGNC `gene_group` field

Primary key behavior (functional):
- primary entity name is HGNC `symbol`
- `is_active = True` when HGNC `status == "Approved"`

Alias/cross-reference mapping used:
- `symbol` -> primary symbol (HGNC)
- `hgnc_id` -> code (HGNC)
- `ensembl_gene_id` -> code (ENSEMBL)
- `entrez_id` -> code (ENTREZ)
- `ucsc_id` -> code (UCSC)
- `name`, `prev_name`, `alias_name` -> synonyms
- `prev_symbol`, `alias_symbol` -> symbols

Notes:
- chromosome is parsed from HGNC `location`
- start/end coordinates are not loaded here
- genomic coordinates are later added by `dtp_gene_ensembl` into `EntityLocation`

## 5. Filters and guards

Row-level skip conditions in load:
- missing/empty `symbol` (gene master key)

Process guards:
- requires `processed_dir`
- requires `master_data.parquet`
- requires OmicStatus `active`
- toggles DB write/read mode and gene/entity indexes around bulk load


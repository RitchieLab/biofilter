# DTP Explain: `dtp_gene_ensembl`

## 1. Data source and pipeline role

- `data_source.name`: `ensembl`
- `source_system`: `Ensembl`
- reference URL (seed):
  `https://ftp.ensembl.org/pub/current_gff3/homo_sapiens/Homo_sapiens.GRCh38.115.chr.gff3.gz`
- format: GFF3 (gzipped)

Pipeline role:
- third step of the gene ingestion flow
- enriches loaded genes with genomic coordinates
- writes coordinates to `EntityLocation` (build 38 context)

Recommended order:
1. `hgnc`
2. `gene_ncbi`
3. `ensembl`

## 2. Extract

Source:
- downloads the Ensembl human GFF3 from `datasource.source_url`

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- file: `Homo_sapiens.GRCh38.115.chr.gff3.gz`

Hash behavior:
- computes hash over downloaded GFF3 file
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `Homo_sapiens.GRCh38.115.chr.gff3.gz`

Parsing logic:
- reads line by line, skipping comments (`#`)
- splits rows into GFF3 columns
- parses the `attributes` column into key/value map
- keeps only rows where `attributes["ID"]` starts with `gene:`

Produced fields:
- `gene_id` (from `ID`)
- `gene_symbol` (from `Name`)
- `biotype`
- `chromosome`
- `start`, `end`
- `strand`
- `source`

Output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- in debug mode also writes `master_data.csv`

## 4. Load

Main goal:
- upsert gene coordinates into `EntityLocation`

Dependencies:
- requires prior gene load (HGNC/NCBI), because matching is done against
  existing `GeneMaster`
- requires `GenomeAssembly` rows for `assembly_name = "GRCh38.p14"`

Matching key:
- builds in-memory index from `GeneMaster` using:
  - `(symbol.upper(), chromosome) -> gene`
- each Ensembl row resolves gene by:
  - `(gene_symbol.upper(), chromosome)`

Chromosome normalization:
- maps chromosome labels with `_map_chrom_to_int(...)`
- supported:
  - `1..22` -> `1..22`
  - `X` -> `23`
  - `Y` -> `24`
  - `MT` or `M` -> `25`
- unmapped chromosomes are skipped

Destination model:
- `EntityLocation` with upsert on `(entity_id, assembly_id)`
- written fields:
  - `entity_id`
  - `entity_group_id` (Genes group, if available)
  - `assembly_id`
  - `build = 38`
  - `chromosome` (normalized int)
  - `start_pos`, `end_pos`
  - `strand`
  - `region_label = None`
  - `data_source_id`
  - `etl_package_id`

Upsert strategy:
- uses dialect-aware insert:
  - PostgreSQL: `ON CONFLICT DO UPDATE`
  - SQLite: `ON CONFLICT DO UPDATE`
  - fallback: generic insert for other dialects

## 5. Filters and skips

Input validation filters:
- drops rows without `gene_symbol` or `chromosome`
- requires columns: `gene_symbol`, `chromosome`, `start`, `end`, `strand`

Skip conditions during load:
- gene not found in `GeneMaster` index
- chromosome cannot be mapped to internal numeric code
- missing assembly mapping for normalized chromosome
- invalid `start`/`end` cast
- duplicate `(entity_id, assembly_id)` inside same batch (deduplicated in-memory)

Operational note:
- if HGNC/NCBI were not loaded first, many Ensembl rows are expected to skip


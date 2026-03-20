# DTP Explain: `dtp_pfam`

## 1. Data source and pipeline role

- `data_source.name`: `pfam`
- `source_system`: `EBI`
- reference URL (seed): `https://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/pfamA.txt.gz`
- format: HMM (raw delivery is a gzipped tabular file)

Pipeline role:
- loads Pfam domain master records into `ProteinPfam`
- prepares domain reference data consumed later by `dtp_uniprot` when creating `ProteinPfamLink`

Recommended sequence:
1. run `dtp_pfam`
2. run `dtp_uniprot`
3. run `dtp_uniprot_relationships` (after master domains are loaded)

## 2. Extract

Source:
- HTTP GET to `datasource.source_url` with stream download
- expected response: `pfamA.txt.gz`

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- files created during extract:
  - `pfamA.txt.gz` (kept)
  - `pfamA.txt` (temporary for hash, removed at end)

Hash behavior:
- unzips `pfamA.txt.gz` to `pfamA.txt`
- computes hash from `pfamA.txt`
- removes `pfamA.txt` after hash computation
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `pfamA.txt.gz` from raw directory

Parsing logic:
- reads gzip TSV with fixed columns
- keeps these fields:
  - `pfam_acc`
  - `pfam_id`
  - `description`
  - `clan_acc`
  - `source_database` (forced to `Pfam`)
  - `type`
  - `long_description`
- drops technical placeholder column (`none_column`)

Output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- in debug mode also writes `master_data.csv`

Transform guards/filters:
- aborts if `pfamA.txt.gz` is missing
- loads only expected initial columns from source file

## 4. Load

Main goal:
- insert new Pfam domain rows into `ProteinPfam`

Target model:
- `ProteinPfam`

Load behavior:
1. read `master_data.parquet`
2. for each row, check existing record by `pfam_acc`
3. insert only when accession does not exist yet
4. commit with bulk insert (`bulk_save_objects`)

Destination highlights:
- `ProteinPfam.pfam_acc`
- `ProteinPfam.pfam_id`
- `ProteinPfam.description` (sanitized by `guard_description`)
- `ProteinPfam.clan_acc`
- `ProteinPfam.source_database`
- `ProteinPfam.type`
- `ProteinPfam.long_description` (sanitized by `guard_description`)
- provenance via `data_source_id` and `etl_package_id`

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts if DataFrame is empty

Row-level behavior:
- no explicit skip by null fields in loop
- duplicate prevention is by existing `pfam_acc` check

Operational behavior:
- does not toggle DB write/read mode in this DTP
- does not drop/create indexes in this DTP

## 6. Practical caveats

- This DTP should precede `dtp_uniprot` in production runs; otherwise, Pfam links from UniProt may be partially missing.
- Current load is insert-only for `ProteinPfam`; existing rows are not updated.
- If source descriptions exceed model limits, they are normalized by `guard_description` during insert.

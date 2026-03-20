# DTP Explain: `dtp_gene_ncbi`

## 1. Data source and pipeline role

- `data_source.name`: `gene_ncbi`
- `source_system`: `NCBI`
- reference URL (seed): `https://ftp.ncbi.nih.gov/gene/DATA/gene_info.gz`
- format: TXT (gzipped)

Pipeline role:
- second step of the gene ingestion flow
- complements HGNC by loading genes not curated by HGNC
- keeps only rows that pass explicit quality filters

Recommended order:
1. `hgnc`
2. `gene_ncbi`
3. `ensembl`

## 2. Extract

Source:
- downloads `gene_info.gz` from `datasource.source_url`

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- downloaded file: `gene_info.gz`
- temporary decompressed file (for hashing): `gene_info`

Hash behavior:
- computes hash from decompressed `gene_info`
- removes temporary `gene_info` after hash
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `gene_info.gz`

Read strategy:
- chunked CSV read (`chunksize=1_000_000`) to reduce memory pressure
- selected columns only:
  - `#tax_id`, `GeneID`, `Symbol`, `Synonyms`, `dbXrefs`
  - `chromosome`, `map_location`, `description`, `type_of_gene`
  - `Full_name_from_nomenclature_authority`, `Other_designations`

Transform filters:
- keeps only human rows: `#tax_id == "9606"`

Derived fields:
- `entrez_id <- GeneID`
- `symbol <- Symbol`
- `synonyms <- Synonyms`
- `hgnc_id` extracted from `dbXrefs` prefix `HGNC:HGNC`, normalized to `HGNC:<id>`
- `ensembl_id` extracted from `dbXrefs` prefix `Ensembl`
- `full_name <- Full_name_from_nomenclature_authority`
- `other_designations <- Other_designations`
- `source = "ncbi"`

Output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- also writes `master_data.csv`

## 4. Load

Main goal:
- load NCBI genes that are not already curated by HGNC

Target groups/models:
- EntityGroup: `Genes`
- OmicStatus: `active`
- Entity and entity aliases/names via:
  - `get_or_create_entity(...)`
  - `get_or_create_entity_name(...)`
- Gene metadata via `get_or_create_gene(...)`
- fallback grouping:
  - GeneGroup `NCBI Gene` (created if missing)
  - LocusType `unknown` (created/get)

Mandatory load filters:
- keep only rows where `hgnc_id` is null
- drop invalid symbol values: `-`, `unknown`, `n/a` (case-insensitive)
- drop rows with `map_location == "-"` (region hint missing)

Additional cleanup:
- removes alias values with empty string or `-`

Field behavior:
- gene symbol key from `symbol`
- chromosome from NCBI `chromosome`
- `hgnc_status` fixed as `Gene from NCBI`
- when `type_of_gene == "protein-coding"`, maps locus group to
  `protein-coding gene`, otherwise uses original `type_of_gene`

## 5. Destination and caveats

Destination:
- creates/updates entities and gene master metadata (same gene domain used by HGNC)
- does not populate genomic start/end coordinates in current implementation

Caveats:
- there is commented code for location/region loading, but it is inactive
- precise coordinates are expected to be completed later by `dtp_gene_ensembl`


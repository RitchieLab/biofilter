# DTP Explain: `dtp_variant_alphamissense`

## 1. Data source and pipeline role

- `data_source.name`: configurable (recommended for this seed: `alphamissense_hg38`)
- `source_system`: expected to be `AlphaMissense` (or equivalent configured seed)
- input format: TSV/TSV.GZ with variant coordinates + AlphaMissense score/class
- canonical GRCh38 source URL:
  - `https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg38.tsv.gz`

Pipeline role:
- ingests AlphaMissense predictor records
- resolves each record against `variant_masters` by natural key:
  - `(chromosome, position_start, position_end, reference_allele, alternate_allele)`
- loads into:
  - `variant_effect_predictions`

## 2. Extract

Source:
- uses `datasource.source_url`
- supports:
  - HTTP/HTTPS download
  - local filesystem path
  - `file://` URL

Behavior:
- validates schema compatibility
- creates raw landing folder:
  - `<raw_dir>/<source_system>/<data_source>/`
- for remote URLs:
  - attempts checksum from `<source_url>.md5`
  - downloads via `http_download(...)`
- for local files:
  - copies source file into landing folder
  - computes SHA256 when remote checksum is unavailable

## 3. Transform

Input discovery (first match in raw folder):
- `*.tsv.gz`
- `*.tsv.bgz`
- `*.bgz`
- `*.gz`
- `*.tsv`
- `*.txt`

Output:
- `<processed_dir>/<source_system>/<data_source>/predictions/`
- part files:
  - `predictions_part_0000.parquet`
  - `predictions_part_0001.parquet`
  - ...

Chunking:
- reads with pandas chunked mode (`chunk_size` in config)
- writes one parquet per transformed chunk

### 3.1 Expected input columns

The DTP resolves aliases for required fields:
- chromosome:
  - `chromosome`, `chrom`, `chr`, `#chrom`
- position:
  - `position_start`, `position`, `pos`, `bp`
- reference allele:
  - `reference_allele`, `ref`, `reference`
- alternate allele:
  - `alternate_allele`, `alt`, `alternate`

Optional aliases:
- transcript:
  - `transcript_id`, `transcript`, `feature`, `enst`
- score:
  - `am_pathogenicity`, `pathogenicity`, `alphamissense_score`, `score`
- class:
  - `am_class`, `classification`, `alphamissense_class`, `class`
- version:
  - `predictor_version`, `version`, `model_version`

## 4. Load

Input:
- parquet parts from processed `predictions/` folder

Load strategy:
1. switch DB to write mode
2. delete previous rows for the same `data_source_id`
3. per part:
   - stage parquet rows in temporary table
   - join staged rows with `variant_masters`
   - upsert into `variant_effect_predictions`
4. log matched vs unmatched variant rows

Upsert key:
- `(chromosome, variant_id, predictor_key)`

`predictor_key` pattern:
- `alphamissense:<predictor_version_or_na>:<transcript_or_->`

## 5. Practical notes

- This DTP requires `variant_masters` to be populated first for the same build.
- Rows that cannot be matched to `variant_masters` are skipped and counted as unmatched.
- Current implementation stores one predictor (`alphamissense`) per row, optionally transcript-level.

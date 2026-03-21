# DTP Explain: `dtp_variant_gnomad`

## 1. Data source and pipeline role

- `data_source.name`: chromosome-specific gnomAD sources (ex: `gnomad_chr22`)
- `source_system`: `GnomAD`
- reference URL (seed example): `https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr22.vcf.bgz`
- format: VCF (`.vcf.bgz`)

Pipeline role:
- ingests gnomAD VCF per chromosome
- transforms into parquet part files:
  - variant master rows
  - variant molecular effect rows (from VEP)
- loads data into:
  - `variant_masters`
  - `variant_molecular_effects`

## 2. Extract

Source:
- uses `datasource.source_url` (remote HTTP or local path/file URL)

Behavior:
- validates compatibility
- attempts to read remote checksum from `<source_url>.md5`
- if `source_url` is local (`/path` or `file://`), returns success with warning (no copy/symlink yet)
- otherwise downloads via `http_download(...)` into raw landing path

Raw output:
- `<raw_dir>/<source_system>/<data_source>/`
- expected downloaded file: VCF bgz/gz/plain

Hash behavior:
- `current_hash` is taken from remote `.md5` when available
- may be `None` if checksum endpoint is missing/unavailable

## 3. Transform

Input discovery:
- scans raw folder for first file matching:
  - `*.vcf.bgz`
  - `*.vcf.gz`
  - `*.vcf`

Output path:
- `<processed_dir>/<source_system>/<data_source>/variants/`
- `<processed_dir>/<source_system>/<data_source>/consequences/`

Output file naming:
- variants: `variants_part_0000.parquet`, `variants_part_0001.parquet`, ...
- consequences: `consequences_part_0000.parquet`, `consequences_part_0001.parquet`, ...

### 3.1 Variant extraction logic

Key config defaults (`GnomadCyvcf2Config`):
- `chunk_size=200000`
- `vep_info_key=vep`
- `extract_all_info=False` (uses allowlist)
- `min_ac=5`
- `min_qual=1`
- `parquet_compression=snappy`

Variant-level filters:
- skip when `AC < min_ac`
- skip when `FILTER` is not one of `None`, `PASS`, `.`, `""`
- skip when `QUAL < min_qual` (if QUAL is numeric)
- skip when ALT is empty
- raises error on multi-ALT record (assumes one ALT per record)

Variant columns produced:
- core:
  - `chrom`, `pos`, `ref`, `alt`, `rsid`, `variant_key`
- plus selected INFO keys:
  - from allowlist when `extract_all_info=False`
  - or all INFO keys except excluded keys/prefixes when `extract_all_info=True`

### 3.2 Consequence extraction logic

VEP parsing:
- reads VEP schema from VCF header (`INFO/<vep_info_key>`, `Format: ...`)
- parses VEP value into annotation rows
- explodes `Consequence` into atomic rows (`&` split)

Consequence fields retained (from config allowlist):
- `Allele`, `Consequence`, `IMPACT`, `SYMBOL`, `Gene`, `Feature_type`, `Feature`, `BIOTYPE`
- `LoF`, `LoF_filter`, `LoF_flags`, `LoF_info`

Derived fields:
- severity/consequence ranks
- per-annotation and per-variant most severe consequence
- boolean flags for most severe rows

Chunk flush:
- writes variant and consequence buffers together by part index
- increments part index each flush

## 4. Load

Input files:
- variant parts: `<processed>/.../variants/variants_part_*.parquet`
- consequence parts: `<processed>/.../consequences/consequences_part_*.parquet`

Pairing rule:
- consequence file is matched by replacing filename prefix:
  - `consequences_part_xxxx` <-> `variants_part_xxxx`

### 4.1 Preconditions

- requires `processed_dir`
- requires at least one variant part file
- switches DB to write mode
- current load path requires PostgreSQL fast-load capability
  - non-PostgreSQL path returns error

### 4.2 PostgreSQL fast path

Steps:
1. create temp stage tables:
   - `tmp_gnomad_variant_stage`
   - `tmp_gnomad_consequence_stage`
2. prepare and sanitize variant DataFrame:
   - rename columns (`chrom->chromosome`, `pos->position_start`, etc.)
   - enforce required fields and max-length masks
   - normalize numeric/text/null behavior
3. COPY variants into stage table
4. insert/upsert into `variant_masters` (`ON CONFLICT DO NOTHING`)
5. resolve `variant_id` by joining staged rows to `variant_masters`
6. prepare consequence DataFrame:
   - normalize gene/transcript/consequence/impact/biotype fields
   - map dimension IDs (`variant_consequences`, `variant_impacts`, `variant_biotypes`)
   - keep rows with valid `variant_id`, `transcript_id`, `consequence_id`
7. COPY consequences into stage table
8. insert/upsert into `variant_molecular_effects` (`ON CONFLICT DO NOTHING`)

Dimension handling:
- loads caches from dimension tables at start
- upserts missing impact/biotype values
- unknown consequence terms are warned and mapped to null `consequence_id`

Partition behavior:
- helper exists to truncate chromosome partitions, but current load keeps truncation disabled in code path (incremental-safe default).

## 5. Filters and guards

Guards:
- raw path must exist and contain VCF
- chromosome must be resolvable from datasource/file naming pattern (`chr1..22`, `X`, `Y`, `M/MT`)
- VEP schema must exist in header
- processed variant parts must exist for load

Transform exclusions:
- low AC / failed FILTER / low QUAL / ALT-empty
- multi-ALT record triggers hard failure

Load exclusions:
- variants failing required schema/masks are dropped before stage copy
- consequences without required IDs (`variant_id`, `transcript_id`, `consequence_id`) are not inserted

## 6. Practical caveats

- Current `load()` is PostgreSQL-oriented; SQLite/other dialects are not supported by this execution path.
- Chromosome inference is based on datasource/file naming conventions; inconsistent naming causes transform/load failure.
- `min_ac` and `min_qual` defaults are strict for unit/debug scenarios; adjust config when needed for test fixtures.
- Transform can be compute-heavy on consequence explosion; `_build_atomic_consequence_rows` is expected hotspot.

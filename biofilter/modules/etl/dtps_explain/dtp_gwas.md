# DTP Explain: `dtp_gwas`

## 1. Data source and pipeline role

- `data_source.name`: `gwas`
- `source_system`: `EBI`
- seed URL (in datasource): `https://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/gwas-catalog-associations.tsv`
- format (seed): `tsv`

Pipeline role:
- ingests GWAS Catalog associations
- enriches disease traits with EFO/parent mappings
- loads association rows into `variant_gwas`
- rebuilds SNP helper links in `variant_gwas_snp`

Version note:
- current DTP is `1.2.0` and supports the newer GWAS ZIP delivery format for associations.

## 2. Extract

Source behavior:
- extract uses GWAS release base path:
  - `https://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/`
- downloads:
  - `gwas-efo-trait-mappings.tsv`
  - `gwas-catalog-associations-full.zip`
- extracts the first TSV inside ZIP and writes as:
  - `gwas-catalog-associations.tsv`
- removes downloaded ZIP after extraction

Raw output path:
- `<raw_dir>/<source_system>/<data_source>/`

Hash behavior:
- computes `current_hash` from extracted:
  - `gwas-catalog-associations.tsv`
- returns `(ok, message, current_hash)`

## 3. Transform

Input files:
- `gwas-catalog-associations.tsv`
- `gwas-efo-trait-mappings.tsv`

Output files:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- debug mode also writes:
  - `master_data.csv`

### 3.1 Association normalization

Reads GWAS catalog with explicit dtypes for key columns:
- `CHR_ID`, `CHR_POS`, `SNPS`, `SNP_ID_CURRENT`
- `RISK ALLELE FREQUENCY`, `P-VALUE`, `OR or BETA`

Normalization rules:
- `CHR_POS`: when multiple values (`;`), keeps first and casts numeric
- `SNP_ID_CURRENT`: when multiple values (`;`), keeps first and casts numeric
- `P-VALUE`: numeric coercion (`errors="coerce"`)

### 3.2 Trait mapping enrichment

Trait mapping input:
- `gwas-efo-trait-mappings.tsv`

Normalization:
- converts `EFO URI` and `Parent URI` to compact IDs:
  - last URI segment
  - `_` replaced by `:`
  - uppercased

Grouped enrichment:
- groups mapping rows by `Disease trait`
- aggregates as unique lists:
  - `EFO term`
  - `Parent term`
  - `efo_id`
  - `parent_id`

Merge:
- left-joins grouped mapping into GWAS catalog using:
  - catalog `DISEASE/TRAIT`
  - mapping `Disease trait`
- ensures list fields are always lists (`[]` when missing)

### 3.3 Final transformed schema

Selected output fields include:
- publication and sample info:
  - `pubmed_id`, `initial_sample_size`, `replication_sample_size`
- genomic/SNP info:
  - `chr_id`, `chr_pos`, `snp_id`, `snp_risk_allele`
  - `reported_gene`, `mapped_gene`, `context`, `intergenic`
- association stats:
  - `risk_allele_frequency`, `p_value`, `pvalue_mlog`, `odds_ratio_beta`, `ci_text`
- trait mapping:
  - `raw_trait`, `mapped_trait`, `mapped_trait_id`, `parent_trait`, `parent_trait_id`

## 4. Load

Input file:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`

Main goal:
- replace all data in `variant_gwas` and `variant_gwas_snp`
- reinsert normalized GWAS rows
- rebuild SNP helper relationships from `snp_id`

### 4.1 Pre-load normalization

String handling:
- fills nulls in object columns as empty string initially
- later normalizes sentinel-like values to `None`:
  - `""`, `NA`, `N/A`, `na`, `null`, `None`, `Nan`, `nan`

List flattening:
- converts list fields to `;`-separated strings:
  - `mapped_trait`
  - `mapped_trait_id`
  - `parent_trait`
  - `parent_trait_id`

Numeric handling:
- converts:
  - `risk_allele_frequency`, `p_value`, `pvalue_mlog` -> numeric
  - `chr_pos` -> nullable integer (`Int64`)

Other normalization:
- `chr_id` normalized to text or `None`
- `odds_ratio_beta` normalized to text, truncated to 50 chars
- all remaining `NaN/NaT` converted to Python `None`
- appends provenance:
  - `data_source_id`
  - `etl_package_id`

### 4.2 Table refresh and insert

DB mode/index handling:
- switches DB to write mode
- drops GWAS indexes before insert
- recreates indexes in `finally`

Refresh behavior:
- PostgreSQL:
  - `TRUNCATE variant_gwas_snp RESTART IDENTITY CASCADE`
  - `TRUNCATE variant_gwas RESTART IDENTITY CASCADE`
- other dialects:
  - `DELETE FROM variant_gwas_snp`
  - `DELETE FROM variant_gwas`

Insert behavior:
- converts dataframe to records dict
- truncates string values to max 255 chars before insert
- bulk inserts into `variant_gwas`

### 4.3 Rebuild of `variant_gwas_snp`

Logic:
- iterates over all `VariantGWAS` rows
- splits `snp_id` tokens using separators:
  - `x`, `X`, `,`, `;`
- keeps only tokens matching `rs` pattern (`rs12345`)
- extracts numeric SNP ID and inserts helper rows:
  - `variant_gwas_id`
  - `snp_id` (numeric)
  - `snp_label`
  - `snp_rank`

Batching:
- bulk saves helper rows with batch size `1000`

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts if processed dataframe is empty

Transform-level guards:
- requires both GWAS association TSV and trait mapping TSV

Load-level guards:
- rebuild helper ignores non-rs tokens in `snp_id`
- helper conversion skips invalid rs numeric parsing

## 6. Practical caveats

- Extract currently ignores the exact seed file URL and always uses the latest-release base endpoints (`.zip` + trait mapping TSV).
- Load does a full replacement of `variant_gwas` and `variant_gwas_snp` on each run (not incremental).
- String truncation to 255 is applied before insert to avoid oversize issues.
- Final success message uses `total_records`, but current code does not increment this counter (message may report `0` even when rows were inserted).

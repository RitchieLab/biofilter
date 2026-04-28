# DTP Explain: `dtp_variant_eqtl_gtex`

## 1. Data source and pipeline role

- `data_source.name`: recommended seed name `gtex_v10_brain_eqtl`
- `source_system`: `GTEx`
- release: GTEx v10 (significant cis-eQTL pairs)
- input format: GTEx v10 single-tissue eQTL distribution (tarball of per-tissue
  significant-pairs files in parquet or `.txt.gz`)

Pipeline role:
- ingests GTEx v10 cis-eQTL significant pairs
- restricted to **13 brain tissues** (hardcoded allowlist — `BRAIN_TISSUES_V10`)
- resolves each record against `variant_masters` by natural key:
  - `(chromosome, position_start, position_end, reference_allele, alternate_allele)`
- loads into:
  - `variant_gene_regulatory_evidence`

### Scope decisions (BF4 4.1.x)

- **GTEx v10** — latest stable release.
- **Significant pairs only** — no all-pairs / marginal associations (volume).
- **eQTL only** — sQTL extension is documented as a stub in `transform()` to be
  enabled later if needed.
- **Brain-only allowlist** is hardcoded in the DTP module. To revisit when
  scaling scope (e.g. cardiac tissues, full atlas).
- **Gene-expression filtering** (e.g. "only genes expressed in brain") is the
  job of Reports, not of this DTP — the DTP loads everything that passes the
  tissue allowlist.

## 2. Extract

Source:
- uses `datasource.source_url` pointing to the GTEx v10 single-tissue eQTL
  tarball.
- supports HTTP/HTTPS download, local filesystem path, and `file://` URLs.

Behavior:
- validates schema compatibility.
- creates raw landing folder:
  - `<raw_dir>/<source_system>/<data_source>/`
- for remote URLs:
  - attempts checksum from `<source_url>.md5`
  - downloads via `http_download(...)`
- for local files: copies source file into landing folder.
- after download, the tarball is **selectively unpacked**: only members whose
  filename matches a brain tissue from `BRAIN_TISSUES_V10` and that contain
  `signif_pairs` in the name are extracted to:
  - `<raw_dir>/<source_system>/<data_source>/tissues/`

This keeps disk footprint bounded — the full v10 tarball contains 49 tissues;
we materialize 13.

## 3. Transform

### Tissue file discovery

Searches in this order:
1. `<raw_base>/tissues/`
2. `<raw_base>/`

Accepted file patterns (per tissue):
- `*.signif_pairs.parquet`
- `*.signif_pairs.txt.gz`
- `*.signif_pairs.tsv.gz`
- `*.signif_pairs.txt`

A file is processed **only if** the tissue label parsed from the filename
(prefix before `.vN`) is in `BRAIN_TISSUES_V10`.

### Expected input columns

Confirmed against `README_eQTL_v10.txt` published in the GTEx v10 cis-QTL bucket
(file extension `*.signif_pairs.parquet`).

Required (alias resolution via `_find_col_name`):
- `variant_id` — GTEx variant id, e.g. `chr1_12345_A_G_b38`
- `gene_id` — Ensembl gene id with version, e.g. `ENSG00000123456.5`
  (falls back to `phenotype_id` if `gene_id` is missing — useful for sQTL files)

Optional:
- `pval_nominal` — mapped to `p_value`
- `slope` — mapped to `beta`
- `slope_se` — mapped to `se`
- `af` (allele frequency of ALT, in-sample) — preserved in `details`
- `ma_samples`, `ma_count`, `tss_distance`, `pval_beta`,
  `pval_nominal_threshold` — preserved in `details` JSON

### `n` is intentionally NULL

The v10 `signif_pairs` files do not expose total sample size per pair — only
`ma_samples` (count of samples carrying the **minor** allele). Mapping
`ma_samples` to `n` would be semantically wrong, so we leave `n=NULL` and
preserve `ma_samples` in `details`. If total per-tissue N is needed in
reports, it can be sourced from GTEx's separate sample-attributes manifest.

### Variant id parsing

GTEx uses a packed string `chr{C}_{pos}_{ref}_{alt}_b38`. Parsed via regex
into `(chromosome_int, position_start, ref, alt)`. `position_end` is derived
as `position_start + len(ref) - 1`.

### Gene id handling

`phenotype_id` like `ENSG00000123456.5` is split:
- `gene_id` ← `ENSG00000123456` (stripped, used in evidence_key)
- the original versioned form is preserved in `details.gene_id_versioned`

### Output

- `<processed_dir>/<source_system>/<data_source>/evidence/`
- part files: `evidence_part_NNNN.parquet`

`evidence_key` pattern (target column of `variant_gene_regulatory_evidence`):
- `<gene_id>:<qtl_type>:<bio_context>` (e.g. `ENSG00000123456:eQTL:Brain_Cortex`)
- truncated to 256 chars to fit the schema.

## 4. Load

Input:
- parquet parts from processed `evidence/` folder.

Load strategy:
1. switch DB to write mode.
2. delete previous rows for the same `data_source_id`.
3. per part:
   - stage parquet rows in temporary table `tmp_gtex_eqtl_stage`
   - join staged rows with `variant_masters`
   - UPSERT into `variant_gene_regulatory_evidence`
4. log matched vs unmatched variant rows.

Upsert key:
- `(chromosome, variant_id, evidence_key)`

## 5. Practical notes

- Requires `variant_masters` to be populated first for the same build (GRCh38).
  GTEx v10 is released on GRCh38; do not mix builds.
- Rows that do not match a row in `variant_masters` are skipped and counted as
  unmatched in the load logs.
- Tissue scope is **module-level constant** (`BRAIN_TISSUES_V10`). To temporarily
  load a different subset, edit the constant and re-run — this is intentional
  for now to keep operational scope explicit and auditable.
- `qtl_type` is fixed to `eQTL` in v1. The transform contains a documented
  stub for adding sQTL with the same loader (uses the same target table).
- `effect_allele` is set to the alternate allele (`alt`) — GTEx convention is
  that `slope` is the effect of the alternate allele relative to the reference.

## 6. Future extensions

Documented in `transform()` as a comment block:

- **sQTL**: add `*.sqtl_signif_pairs.*` discovery, derive `gene_id` from
  GTEx-provided cluster→gene map, and emit rows with `qtl_type='sQTL'`.
- **Configurable tissue scope**: promote `BRAIN_TISSUES_V10` to a config field
  (and read from `data_source.config_json` or similar) when scope expands.
- **Significance filter**: GTEx significant_pairs is already pre-filtered;
  no explicit threshold is applied. Add one to `GTExEQTLConfig` if needed.

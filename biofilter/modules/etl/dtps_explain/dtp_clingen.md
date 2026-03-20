# DTP Explain: `dtp_clingen`

## 1. Data source and pipeline role

- `data_source.name`: `clingen`
- `source_system`: `CLINGEN`
- reference URL (seed): `https://search.clinicalgenome.org/kb/gene-validity/download`
- format (seed): CSV

Pipeline role:
- loads Gene→Disease relationships from ClinGen validity data
- resolves endpoints via existing Gene and Disease entities
- inserts rows into `EntityRelationship`

Operational dependency:
- run this after master datasets are loaded for:
  - Genes (HGNC aliases)
  - Diseases (MONDO aliases)

## 2. Extract

Source:
- downloads from ClinGen KB endpoints under:
  - `https://search.clinicalgenome.org/kb/`

Endpoints fetched:
- `reports/curation-activity-summary-report`
- `gene-validity/download`
- `gene-dosage/download`

Canonical files written to raw path:
- `<raw_dir>/<source_system>/<data_source>/ClinGen-Curation-Activity-Summary.csv`
- `<raw_dir>/<source_system>/<data_source>/ClinGen-Gene-Disease-Summary.csv`
- `<raw_dir>/<source_system>/<data_source>/ClinGen-Gene-Dosage.csv`

Extract behavior:
- retry with backoff on request failures
- follows redirects
- if a downloaded tiny text file contains a URL, fetches the referenced file
- normalizes date-suffixed names to canonical names
- attempts extension correction when a `.txt` file looks like CSV/TSV

Hash behavior:
- computes hash from canonical `ClinGen-Gene-Disease-Summary.csv`
- returns `(ok, message, current_hash)`

## 3. Transform

Input files (raw):
- `ClinGen-Gene-Disease-Summary.csv` (required for relationship load)
- `ClinGen-Curation-Activity-Summary.csv` (optional)
- `ClinGen-Gene-Dosage.csv` (optional)

Outputs (processed):
- `gene_disease_validity.parquet`
- `curation_activity_summary.parquet` (optional)
- `gene_dosage.parquet` (optional)
- in debug mode, CSV copies are also written

### 3.1 Gene-disease validity processing

Header handling:
- scans file to find the real header row containing `GENE SYMBOL`
- reloads with detected header

Selected/renamed columns:
- `GENE SYMBOL` -> `gene_symbol`
- `GENE ID (HGNC)` -> `hgnc_id`
- `DISEASE LABEL` -> `disease_label`
- `DISEASE ID (MONDO)` -> `mondo_id`
- `MOI` -> `moi`
- `SOP` -> `sop` (later reduced to `sop_version`)
- `CLASSIFICATION` -> `classification`
- `ONLINE REPORT` -> `report_url`
- `CLASSIFICATION DATE` -> `assertion_date`
- `GCEP` -> `gcep`

Filters and normalization:
- keeps only rows with:
  - `hgnc_id` starting with `HGNC:`
  - `mondo_id` starting with `MONDO:`
- normalizes `assertion_date` to ISO date when parseable
- derives `sop_version` from numeric component in `SOP`
- maps classification strength to `class_rank`

Dedup rule:
- sorts by `(hgnc_id, mondo_id, assertion_date desc, class_rank desc)`
- keeps first row per `(hgnc_id, mondo_id)`

### 3.2 Curation activity summary processing (optional)

Header handling:
- scans file to find row containing `gene_symbol`

Keeps curated subset:
- `gene_symbol`, `hgnc_id`, `disease_label`, `mondo_id`
- `mode_of_inheritance`
- `gene_disease_validity_assertion_classifications`
- `gene_disease_validity_assertion_reports`
- `gene_disease_validity_gceps`

Filter:
- keeps only `HGNC:` + `MONDO:` identifier rows

### 3.3 Gene dosage processing (optional)

Behavior:
- passthrough CSV -> parquet conversion (string dtype)

## 4. Load

Input file:
- `<processed_dir>/<source_system>/<data_source>/gene_disease_validity.parquet`

Main goal:
- map `hgnc_id` and `mondo_id` to existing entities
- create Gene→Disease relationships in `EntityRelationship`

Preload references:
- `EntityGroup` for `Genes` and `Diseases`
- `EntityRelationshipType` for code `part_of` (current implementation)

Alias resolution:
- genes: `xref_source = HGNC` and `alias_type = code`
- diseases: `xref_source = MONDO` and `alias_type = code`

Load flow:
1. merge relationship rows with gene/disease alias maps
2. add `relationship_type_id`
3. keep only required columns + provenance (`data_source_id`, `etl_package_id`)
4. drop rows with missing mapped IDs
5. drop duplicate relationships on:
   - `entity_1_id`, `entity_1_group_id`
   - `entity_2_id`, `entity_2_group_id`
   - `relationship_type_id`
6. if no valid rows remain, abort load (does not delete existing data)
7. replace old rows atomically:
   - delete current data source rows
   - bulk insert new rows
   - commit once

Operational behavior:
- drops and recreates `EntityRelationship` index set around load

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `gene_disease_validity.parquet`
- aborts if DataFrame is empty
- aborts if `Genes`/`Diseases` groups are missing
- aborts if relationship type `part_of` is missing

Row-level exclusions:
- unresolved gene or disease alias
- missing required relationship IDs
- duplicate relationships after mapping

Safety behavior:
- does not delete old ClinGen relationships when zero valid mapped rows are available

## 6. Practical caveats

- Current relationship code is `part_of` (marked TODO in code). Semantically, `associated_with` may be a better long-term fit depending on ontology policy.
- Transform captures rich metadata (`classification`, `assertion_date`, `gcep`, `report_url`), but current load inserts only relationship endpoints + type/provenance.
- Optional outputs (`curation_activity_summary`, `gene_dosage`) are generated for downstream analysis but are not consumed by this load step.

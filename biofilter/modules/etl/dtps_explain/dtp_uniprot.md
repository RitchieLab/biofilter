# DTP Explain: `dtp_uniprot`

## 1. Data source and pipeline role

- `data_source.name`: `uniprot`
- `source_system`: `UniProt`
- reference URL (seed):
  `https://rest.uniprot.org/uniprotkb/stream?query=organism_id:9606+AND+reviewed:true&format=xml`
- format: XML

Pipeline role:
- loads Protein master entities and protein-domain structures
- also stages cross-domain links in `relationship_data.parquet` for
  `dtp_uniprot_relationships`

Recommended sequence:
1. run `dtp_pfam` first (loads `ProteinPfam` reference)
2. run `dtp_uniprot` (creates proteins and Pfam links)
3. run `dtp_uniprot_relationships` after master-domain loads

## 2. Extract

Source:
- HTTP GET to `datasource.source_url`
- expected response: UniProt XML stream

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- file: `proteins.xml`

Hash behavior:
- computes hash from `proteins.xml`
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `proteins.xml`

Parsing logic (per `<entry>`):
- canonical accession and secondary accessions
- UniProt names and gene symbol
- functional text fields (`function`, subcellular `location`, tissue, caution note)
- db references (GO, KEGG, HGNC, RefSeq)
- sequence length
- isoforms and Pfam IDs

Master output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- in debug mode: `master_data.csv`

Master columns:
- `uniprot_id`, `secondary_ids`, `uniprot_name`, `gene_symbol`
- `full_name`, `ec_number`, `organism`, `tax_id`
- `function`, `location`, `tissue`, `pseudogene_note`
- `protein_length`, `isoforms`, `pfam_ids`

Relationship staging output:
- `<processed_dir>/<source_system>/<data_source>/relationship_data.parquet`
- in debug mode: `relationship_data.csv`

Relationship rows produced:
- GO links:
  - source type `Proteins`, target type `Gene Ontology`, relation `part_of`
- KEGG links:
  - source type `Proteins`, target type `Pathways`, relation `in_pathway`
- HGNC links:
  - source type `Proteins`, target type `Genes`, relation `encodes`
- RefSeq links:
  - source type `Proteins`, target type `Transcriptomics`, relation `has_transcript`

## 4. Load

Main goal:
- create canonical protein entities + isoform entities
- populate `ProteinMaster`, `ProteinEntity`, and Pfam links

Target models:
- `Entity` / `EntityAlias` (group `Proteins`)
- `ProteinMaster`
- `ProteinEntity`
- `ProteinPfamLink` (when Pfam accession exists in `ProteinPfam`)

Alias mapping for canonical entry:
- `uniprot_id` -> primary alias (`code`, source `UniProt`)
- `uniprot_name` -> name alias (`name`, source `UniProt`)
- `secondary_ids` -> synonym alias (`synonym`, source `UniProt`)

Canonical load flow:
1. create/get canonical protein Entity
2. create/get canonical `ProteinMaster` by `(protein_id, data_source_id)`
3. create/get canonical `ProteinEntity` with `is_isoform=False`
4. create `ProteinPfamLink` for each mapped Pfam accession found in `ProteinPfam`

Isoform load flow:
- for each isoform accession:
  - create/get isoform Entity (`alias_type="Isoform"`)
  - create/get `ProteinEntity` with:
    - `is_isoform=True`
    - `isoform_accession=<isoform id>`
    - same `protein_id` as canonical `ProteinMaster`

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts on empty DataFrame

Row-level skip:
- skips entries with missing `uniprot_id`

Operational behavior:
- switches DB to write mode
- drops/creates protein and entity index groups around load

## 6. Practical caveats

- `dtp_pfam` must run before `dtp_uniprot` to maximize `ProteinPfamLink`
  coverage.
- Relationship rows are only staged here; they are inserted into
  `EntityRelationship` by `dtp_uniprot_relationships`.
- Pfam links are only created when accession exists in `ProteinPfam`;
  missing accessions are logged as warnings.
- As currently implemented, `ProteinMaster.location` is filled from the
  transformed `function` field (implementation detail in current code path).

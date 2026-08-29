# DTP Explain: `dtp_kegg`

## 1. Data source and pipeline role

- `data_source.name`: `kegg_pathways`
- `source_system`: `KEGG`
- reference URL (seed): `https://rest.kegg.jp/list/pathway/hsa`
- format: TXT

Pipeline role:
- loads KEGG pathways into the Pathway domain
- creates pathway entities and `PathwayMaster` rows
- stages gene <-> pathway memberships for `dtp_kegg_relationships`,
  which is the DTP that actually writes `EntityRelationship` rows

## 2. Extract

Sources (two endpoints, both public and unauthenticated):
- HTTP GET to `datasource.source_url` — the pathway catalog
  (`https://rest.kegg.jp/list/pathway/hsa`), plain text
  (`path:<id>\t<description>`)
- HTTP GET to the module constant `KEGG_LINK_URL`
  (`https://rest.kegg.jp/link/hsa/pathway`) — the gene memberships,
  plain text (`path:<id>\thsa:<ncbi_gene_id>`)

The catalog endpoint carries no gene data, which is why the second
endpoint is fetched here rather than being derived from `source_url`.

Raw output:
- path pattern: `<raw_dir>/<source_system>/<data_source>/`
- files: `kegg_pathways.txt`, `kegg_gene_pathway.txt`

Hash behavior:
- composite SHA256 over both files (`compute_files_hash`), so the run is
  skipped only when neither file changed
- returns `(ok, message, current_hash)`

## 3. Transform

Input:
- `kegg_pathways.txt`
- `kegg_gene_pathway.txt`

Parsing logic (catalog):
- reads file line by line
- skips empty lines
- expects exactly 2 tab-separated columns
- removes `path:` prefix from pathway ID

Produced fields:
- `pathway_id`
- `description`

Parsing logic (memberships):
- reads file line by line
- expects exactly 2 tab-separated columns
- removes `path:` prefix from the pathway ID and `hsa:` from the gene ID
- the remaining gene number is the NCBI (Entrez) Gene ID
- pathways absent from the catalog are skipped and logged as a warning —
  the catalog is the authority on which pathways exist

Produced fields (memberships):
- `pathway_id`
- `relation_type` (always `ncbi_gene`)
- `relation` (NCBI Gene ID)
- `evidence` (always `curated`)

Output:
- `<processed_dir>/<source_system>/<data_source>/master_data.parquet`
- `<processed_dir>/<source_system>/<data_source>/relationship_data.parquet`
- in debug mode also writes the matching `.csv` files

Filters:
- malformed lines (`len(parts) != 2`) are ignored

## 4. Load

Main goal:
- create/update pathway entities and KEGG pathway master records

Target models:
- `Entity` / `EntityAlias` for EntityGroup `Pathways`
- `PathwayMaster`

Alias mapping:
- `pathway_id` -> primary alias (`code`, source `KEGG`)
- `description` -> additional alias (`name`, source `KEGG`)

Load behavior:
- reads `master_data.parquet`
- resolves entity group `Pathways`
- creates/gets entity + aliases
- checks existing pathway by `PathwayMaster.pathway_id`
- inserts only when pathway ID does not exist yet

Destination:
- `PathwayMaster.pathway_id = pathway_id`
- `PathwayMaster.description = description`
- `PathwayMaster.entity_id = resolved entity_id`
- provenance: `data_source_id` and `etl_package_id`

## 5. Filters and guards

Guards:
- requires `processed_dir`
- requires `master_data.parquet`
- aborts if DataFrame is empty

Row-level skip:
- missing empty `pathway_id`

Operational behavior:
- switches DB to write mode
- drops/creates pathway and entity index groups around load

Note:
- this DTP never writes `EntityRelationship`. Run
  `kegg_relationships` (see `dtp_kegg_relationships.md`) after this one
  to load the gene memberships it stages.


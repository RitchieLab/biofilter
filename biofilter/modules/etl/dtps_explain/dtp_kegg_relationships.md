# DTP Explain: `dtp_kegg_relationships`

## 1. Data source and pipeline role

- `data_source.name`: `kegg_relationships`
- `source_system`: `KEGG`
- source URL in seed: `not_applicable`
- format in seed: `not_applicable`

Pipeline role:
- relationship-only DTP
- consumes the relationship staging produced by `dtp_kegg`
- loads gene -> pathway memberships into `EntityRelationship`

Before this DTP existed, BF4 held the KEGG pathway catalog but **zero**
KEGG gene links, so Reactome was the only source of `in_pathway`
relationships. Loading KEGG makes two-source pathway evidence possible.

Recommended dependency order:
1. gene sources loaded (`gene_ncbi` in particular — provides the
   `ENTREZ` code aliases used for resolution)
2. `kegg_pathways` extract/transform/load
3. `kegg_relationships` load

## 2. Extract

Behavior:
- no extraction performed
- returns success with informational message

Reason:
- raw files are extracted by the `dtp_kegg` DTP, which downloads both
  the pathway catalog (`https://rest.kegg.jp/list/pathway/hsa`) and the
  gene memberships (`https://rest.kegg.jp/link/hsa/pathway`)

## 3. Transform

Behavior:
- no transformation performed
- returns success with informational message

Reason:
- relationship staging (`relationship_data.parquet`) is created by the
  `dtp_kegg` transform

Staging schema written by `dtp_kegg`:

| column | example | meaning |
| --- | --- | --- |
| `pathway_id` | `hsa00010` | KEGG pathway ID (no `path:` prefix) |
| `relation_type` | `ncbi_gene` | only value currently produced |
| `relation` | `10327` | NCBI (Entrez) Gene ID (no `hsa:` prefix) |
| `evidence` | `curated` | KEGG memberships are manually curated |

Pathways present in the membership file but absent from the catalog are
skipped at transform time and logged as a warning — the catalog is the
authority on which pathways exist.

## 4. Load

Input file:
- `<processed_dir>/KEGG/kegg_pathways/relationship_data.parquet`
- note: reads from the parent source folder `kegg_pathways`, not its own
  data source name

Main goal:
- resolve entity IDs and relationship type IDs, then insert pathway
  memberships into `EntityRelationship`

### 4.1 Entity resolution strategy

Pathway side (`entity_1_id`):
- `EntityAlias` lookup restricted to `group = Pathways`,
  `alias_type = 'code'`, `xref_source = 'KEGG'`

Gene side (`entity_2_id`):
- `EntityAlias` lookup restricted to `group = Genes`,
  `alias_type = 'code'`, `xref_source = 'ENTREZ'`

Ambiguity is resolved by priority (`is_primary` wins) and logged.

### 4.2 Relationship type mapping

`relation_type` -> `EntityRelationshipType.code`:
- `ncbi_gene` -> `in_pathway`

There is no silent fallback: an unknown `relation_type` aborts the load.

### 4.3 Valid rows and insert

Validity filter:
- keeps only rows where both `entity_1_id` and `entity_2_id` were resolved

Dedup:
- in-file drop of duplicate directional triples
  (`entity_1_id`, `entity_2_id`, `relationship_type_id`)
- server-side `INSERT ... SELECT ... WHERE NOT EXISTS` against existing
  rows of the same `data_source_id`, so re-running the load is a no-op

Group IDs:
- fetches `Entity.group_id` for all used entity IDs in one round-trip
- fills `entity_1_group_id` and `entity_2_group_id`

Insert operation:
- stages candidates into TEMP TABLE `kegg_load_candidates` in 50k chunks
- single server-side insert with directional dedup
- writes `data_source_id = kegg_relationships` and current
  `etl_package_id`
- indexes on `entity_relationships` are intentionally kept in place so the
  `NOT EXISTS` lookup stays fast

## 5. Rejections and diagnostics

Rows not loaded:
- rows with unresolved `entity_1_id` or `entity_2_id` are not inserted
- saved to:
  - `<processed_dir>/KEGG/kegg_pathways/kegg_rel_not_loaded.csv`

Common causes:
- missing upstream `kegg_pathways` load
- genes absent from BF4, or without an `ENTREZ` code alias
- KEGG gene IDs withdrawn from NCBI

Expected coverage (measured 2026-08-27 against the production bundle):

```
KEGG links                         39,576
KEGG genes                          9,423
  resolve to BF4 Gene entities      9,369   (99.4%)
KEGG pathways                         372
  present in pathway_masters          371
loadable in_pathway relationships  39,382
```

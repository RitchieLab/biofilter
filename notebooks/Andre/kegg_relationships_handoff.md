# Work order — `dtp_kegg_relationships`

Load KEGG gene↔pathway memberships. Today BF4 has 371 KEGG pathway entities
and **zero** gene links, so Reactome is the only source of `in_pathway`
relationships.

## Root cause

Not a bug in `dtp_kegg.py` — it does what it was written to do. The gap is a
**missing second DTP**, exactly the split Reactome already has:

```
dtp_reactome.py                → extract + transform, writes relationship_data.parquet
dtp_reactome_relationships.py  → extract/transform are no-ops; load() reads that parquet
dtp_kegg.py                    → extracts the pathway catalog only
dtp_kegg_relationships.py      → DOES NOT EXIST
```

The configured URL is the catalog endpoint, which carries no gene data:

| endpoint | lines | content |
| --- | ---: | --- |
| `https://rest.kegg.jp/list/pathway/hsa` ← current `source_url` | 372 | `hsa01100 <TAB> Metabolic pathways - Homo sapiens (human)` |
| `https://rest.kegg.jp/link/hsa/pathway` ← what is missing | 39,576 | `path:hsa00010 <TAB> hsa:10327` |

**It is not an access problem.** The membership endpoint was fetched
successfully on 2026-08-21 and needs no key or auth.

## Verified feasibility (measured against the production bundle)

Bundle used: `parquet:///Users/andrerico/Works/Sys/bf_files/tables`
(same snapshot as LPC `/project/hall_shared/datasets/biofilter/20260514/tables`)

The number after `hsa:` is the NCBI Gene ID, already stored in BF4:

```
KEGG genes                          9,423
  resolve to BF4 Gene entities      9,369   (99.4%)
  via entity_aliases.alias_type     'code'
KEGG pathways                         372
  already in pathway_masters          370
```

No new mapping table and no new entity type are required.

## Impact

```
Reactome genes with a pathway      23,263
KEGG genes with a pathway           9,366
  in both                           7,412
  KEGG only (new reach)             1,954
```

On the live ADSP analysis (`notebooks/Andre/adsp/step_03`, 273,344 variant
pairs):

```
pairs with >1 Reactome pathway     122,184
pairs ALSO supported by KEGG        29,912   (10.9%)  ← genuine two-source evidence
both criteria                       15,914
```

This is what unblocks the collaborator's request to "only count pairs that
share more than one source as evidence" — impossible today, since
`data_source_support_count` is 1 for all 273,344 pairs.

## Implementation sketch

1. `dtp_kegg.py`
   - `extract()`: also fetch `https://rest.kegg.jp/link/hsa/pathway`, hash it
     alongside the catalog file
   - `transform()`: write `relationship_data.parquet` in the processed dir,
     shaped like the Reactome one
2. `dtp_kegg_relationships.py` — new, modelled on
   `dtp_reactome_relationships.py` (648 lines, mostly entity-resolution and
   batching boilerplate). `extract()`/`transform()` are no-ops; `load()` reads
   the parquet and creates `in_pathway` relationships.
3. Register the data source in the seed with `dtp_script =
   dtp_kegg_relationships` and `source_url = not_applicable`, mirroring
   `reactome_relationships`.
4. `biofilter/modules/etl/dtps_explain/dtp_kegg_relationships.md`

Estimate: ~1 day, mostly testing. The KEGG-side changes are ~40 lines; the
rest is adapting the Reactome relationships DTP.

## Related, do not lose

`Gene Ontology` has the identical gap — 38,739 GO term entities loaded, zero
gene annotations. Same shape of fix, separate task.

`variant_impacts` is polluted with `INTRON_SIZE:*` and `PERCENTILE:*` values
leaking from LoF_info in `dtp_variant_gnomad`. Unrelated, but noticed while
investigating; also recorded in
`notebooks/Andre/ADR/0002-cohort-coding-gene-overlap-report.md`.

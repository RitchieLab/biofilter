# Report Tutorial: `annotation_variant_regulatory_evidence`

## Purpose

Annotate variants with **gene-regulatory evidence** (eQTL / sQTL) from
`variant_gene_regulatory_evidence` (BF4 4.1.x). Accepts three input modes
so the same report covers the three most common questions:

- **Gene mode** — "what variants in/near gene X have eQTL evidence, and
  which gene do they regulate?"
- **Coord mode** — "for variants near this chromosome:position, what
  regulatory evidence exists?"
- **rsid mode** — "what eQTLs is rs1234567 involved in?"

Output is **gene-centric**: every emitted row carries both the eQTL target
gene (the gene the variant regulates, from the eQTL table) and the gene
whose body contains the variant (resolved via `entity_locations`). These
two genes can differ, since cis-eQTLs in GTEx reach up to ±1 Mb of the TSS
and a variant inside gene A may regulate gene B in the same window.

---

## Report Name

```bash
biofilter report run --report-name annotation_variant_regulatory_evidence \
    --input "APOE,APP,PSEN1" \
    --param input_type=gene \
    --param tissue=Brain_Cortex,Brain_Hippocampus \
    --param max_rows=10000
```

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_data` | list / file | required | Terms to query. Content depends on `input_type`. |
| `input_type` | str | `gene` | One of `gene`, `coord`, `rsid`. |
| `build` | int | `38` | Genome assembly build (`38` or `37`). |
| `flanking_bp` | int | `0` | Window size (bp) around each gene/coord. Ignored for rsid. |
| `tissue` | list / CSV | `None` | Filter by `bio_context` (e.g. `Brain_Cortex,Brain_Hippocampus`). |
| `qtl_type` | str | `eQTL` | Filter by `qtl_type`. Set to `None` to keep all types. |
| `p_value_max` | float | `None` | Keep only rows with `p_value <= p_value_max`. |
| `max_rows` | int | `10000` | Hard cap on returned rows; warns if hit. |

### Input formats per mode

- **`gene`** — gene symbols (`APOE`), Ensembl IDs (`ENSG00000130203`),
  Entrez IDs, or any alias resolvable via `entity_aliases` filtered by
  `EntityGroup.name='Genes'`.
- **`coord`** — `chr1:12345`, `1:12345`, `chr1-12345`, `1,12345`, etc.
  (any of the formats accepted by `ReportBase.resolve_position_list`).
- **`rsid`** — `rs1234567`. The query scans every chromosome partition
  using the `rsid` index — fine for small input lists, expensive for
  huge ones.

---

## Output Columns

Five gene columns make the regulatory relationship explicit:

| Column | When populated | Source |
|---|---|---|
| `input_gene_symbol` | only when `input_type=gene` | input gene resolved label |
| `input_gene_entity_id` | only when `input_type=gene` | `entity_aliases` |
| `eqtl_target_ensembl` | always | `variant_gene_regulatory_evidence.gene_id` (raw ENSG) |
| `eqtl_target_symbol` | when ENSG is registered in `entity_aliases` | resolved primary symbol |
| `position_gene_symbol` | when variant falls in a gene body | `entity_locations` → primary symbol |
| `position_gene_ensembl` | idem | `entity_aliases` (xref_source=ENSEMBL) |

When `position_gene_*` and `eqtl_target_*` differ, the variant is regulating
a neighboring gene in cis (common for distal enhancer SNPs).

Variant identity:

| Column | Source |
|---|---|
| `variant_id`, `chromosome`, `position_start`, `position_end` | `variant_masters` |
| `rsid`, `reference_allele`, `alternate_allele` | `variant_masters` |

eQTL evidence:

| Column | Source |
|---|---|
| `bio_context` (tissue) | `variant_gene_regulatory_evidence.bio_context` |
| `qtl_type` | `eQTL` / `sQTL` / etc. |
| `beta`, `se`, `p_value`, `n` | regression statistics from the source DTP |
| `effect_allele` | the allele whose effect is reported in `beta` |

Provenance + extras:

| Column | Source |
|---|---|
| `details` | JSON blob from the source DTP (`af`, `ma_samples`, `tss_distance`, etc.) |
| `data_source_id`, `etl_package_id` | `etl_data_sources`, `etl_packages` |

---

## How the queries work

### Gene mode

```
input gene terms
    → entity_aliases (lower-cased match within 'Genes' group)
    → entity_locations (filtered by build's assembly_id)
    → temp table: (input_term, entity_id, chromosome, range_start, range_end)
        with [start_pos - flanking_bp, end_pos + flanking_bp]
    → per-chromosome JOIN: variant_masters → variant_gene_regulatory_evidence
```

Per-chromosome iteration lets Postgres prune partitions on `vm.chromosome`.

### Coord mode

Same as gene mode, but the temp table is populated with point ranges
`(pos - flanking_bp, pos + flanking_bp)` — no entity resolution needed.

### rsid mode

Direct lookup — no temp table:

```sql
SELECT ... FROM variant_masters vm
JOIN variant_gene_regulatory_evidence vgre
  ON vgre.chromosome = vm.chromosome AND vgre.variant_id = vm.variant_id
WHERE LOWER(vm.rsid) = ANY(:rsids) AND <evidence filters>
```

Postgres scans each partition's `rsid` index. Fast for small lists; not
recommended for >10K rsids in one call.

---

## Practical Notes

- The report **does not** populate `variant_gene_regulatory_evidence` —
  it only reads from it. Make sure a regulatory-evidence DTP has run
  (e.g. `dtp_variant_eqtl_gtex` for GTEx v10 brain).
- All evidence filters (`tissue`, `qtl_type`, `p_value_max`) are pushed
  to SQL — they don't blow up Python memory.
- The `position_gene_*` lookup runs **after** the main query as one
  query per chromosome against `entity_locations` (UNNEST + BETWEEN).
  When a variant falls inside multiple overlapping gene bodies (rare),
  the first match wins; cross-check `eqtl_target_*` to disambiguate.
- `eqtl_target_symbol` is resolved by looking up the raw ENSG in
  `entity_aliases` (`xref_source='ENSEMBL'`) and following to the
  preferred / `is_primary` alias of the same entity. If the gene isn't
  yet ingested into BF4 (e.g. a new Ensembl release the DB hasn't seen),
  `eqtl_target_symbol` is `NULL` while `eqtl_target_ensembl` still
  contains the raw GTEx value.

---

## Example outputs

### "Variants regulating APOE-region genes in cortex"

```bash
biofilter report run --report-name annotation_variant_regulatory_evidence \
    --input "APOE" \
    --param input_type=gene \
    --param flanking_bp=500000 \
    --param tissue=Brain_Cortex \
    --param p_value_max=1e-6
```

### "Annotation for a single rsID"

```bash
biofilter report run --report-name annotation_variant_regulatory_evidence \
    --input "rs429358" \
    --param input_type=rsid
```

### "Anything within 1 kb of a coordinate"

```bash
biofilter report run --report-name annotation_variant_regulatory_evidence \
    --input "chr19:44908684" \
    --param input_type=coord \
    --param flanking_bp=1000
```

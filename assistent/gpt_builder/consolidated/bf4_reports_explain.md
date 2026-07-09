# BF4 Report Reference (per report)



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_master_chemical.md ===== -->

# Report Tutorial: `annotation_master_chemical`

## Purpose

Compact Chemical annotation report based on `ChemicalMaster`.
For each input chemical alias/ID, returns:

- ChEBI identity (`chemical_id`) and canonical label/definition
- core physical fields (`formula`, `charge`, `mass`, `monoisotopic_mass`, `structure_id`)
- source/provenance fields (`omic_status`, source system, data source, ETL package)
- optional xref summary by source
- optional relationship summary by related entity group

## Report Name

`annotation_master_chemical`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)
- `include_xref_summary`: `bool` (default `True`)
- `include_relationships`: `bool` (default `False`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_chemical",
    input_data=["CHEBI:15377", "CHEBI:17234", "water"],
    include_xref_summary=True,
    include_relationships=True,
)
```

API (`__ALL__`):

```python
df = bf.report.run(
    "annotation_master_chemical",
    input_data="__ALL__",
    include_xref_summary=True,
    include_relationships=False,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_chemical \
  --input CHEBI:15377 --input CHEBI:17234 --input "water" \
  --param include_xref_summary=true \
  --param include_relationships=true
```

CLI (`__ALL__`):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_chemical \
  --param input_data=__ALL__ \
  --param include_xref_summary=true
```

## Notes

- `xref_ids_by_source` summarizes `EntityAlias` entries where `alias_type='code'`.
- `entity_relationships_by_group` and `total_entity_relationships` are optional and disabled by default for performance.
- When `include_aliases=false`, `other_aliases` is returned as null.
- When `input_data="__ALL__"`, the report resolves and returns all chemical entities available in `ChemicalMaster`.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_master_disease.md ===== -->

# Report Tutorial: `annotation_master_disease`

## Purpose

Compact Disease annotation report based on `DiseaseMaster`.
For each input disease alias/ID, returns:

- MONDO identity + label + description
- disease groups/subsets
- source/provenance fields
- optional xref summary by source
- optional ClinGen summary (gene count + relationship count)
- optional relationship summary by related entity group

## Report Name

`annotation_master_disease`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)
- `include_xref_summary`: `bool` (default `True`)
- `include_clingen_summary`: `bool` (default `True`)
- `include_relationships`: `bool` (default `False`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_disease",
    input_data=["MONDO:0019391", "MONDO:0005737", "cystic fibrosis"],
    include_xref_summary=True,
    include_clingen_summary=True,
    include_relationships=True,
)
```

API (`__ALL__`):

```python
df = bf.report.run(
    "annotation_master_disease",
    input_data="__ALL__",
    include_xref_summary=True,
    include_clingen_summary=True,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_disease \
  --input MONDO:0019391 --input MONDO:0005737 --input "cystic fibrosis" \
  --param include_xref_summary=true \
  --param include_clingen_summary=true \
  --param include_relationships=true
```

CLI (`__ALL__`):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_disease \
  --param input_data=__ALL__ \
  --param include_xref_summary=true
```

## Notes

- `clingen_*` fields summarize only relationships loaded from data source `clingen`.
- `entity_relationships_by_group`/`total_entity_relationships` are optional.
- Relationship type semantics from ClinGen may evolve; this report focuses on stable group/source-level summaries.
- When `input_data="__ALL__"`, the report resolves and returns all disease entities available in `DiseaseMaster`.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_master_gene.md ===== -->

# Report Tutorial: `annotation_master_gene`

## Purpose

Compact Gene annotation report focused on performance.
For each input gene/alias, returns:

- resolved `entity_id`
- canonical IDs (`symbol`, `hgnc`, `ensembl`, `entrez`)
- GeneMaster metadata (`hgnc_status`, `omic_status`, `locus_group`, `locus_type`)
- gene groups membership
- build38 coordinates (`chromosome`, `start`, `end`)
- relationship summary by related entity group and total count
- optional variant count in gene range (without variant details)

## Report Name

`annotation_master_gene`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `include_relationships`: `bool` (default `True`)
- `include_variant_summary`: `bool` (default `True`)
- `emit_not_found_rows`: `bool` (default `True`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_gene",
    input_data="__ALL__",
    include_relationships=False,
    include_variant_summary=False,
    emit_not_found_rows=False,
)
```

API (specific genes):

```python
df = bf.report.run(
    "annotation_master_gene",
    input_data=["BRCA1", "TP53", "HGNC:11998"],
    include_relationships=True,
    include_variant_summary=True,
    emit_not_found_rows=True,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_gene \
  --input BRCA1 --input TP53 --input HGNC:11998 \
  --param include_relationships=true \
  --param include_variant_summary=true
```

CLI (all genes):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_gene \
  --param input_data=__ALL__ \
  --param include_relationships=false \
  --param include_variant_summary=false \
  --param emit_not_found_rows=false
```

With input file:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_gene \
  --input-file ./genes.txt \
  --param include_variant_summary=false
```

## Notes

- Relationship summary is reported as a compact list of tuples:
  `[(<RelatedEntityGroup>, <count>), ...]`
- `total_entity_relationships` is the sum of the list above.
- Variant summary only counts overlapping variants in build38 gene interval.
- If gene location is missing in `entity_locations` (build=38), variant count is null.
- `input_data="__ALL__"` returns one row per gene entity found in `GeneMaster`.
- For large databases, prefer `include_relationships=false` and `include_variant_summary=false` for faster runtime.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_master_go.md ===== -->

# Report Tutorial: `annotation_master_go`

## Purpose

Compact GO annotation report based on `GOMaster`.
For each input GO alias/ID, returns:

- GO identity (`go_id`, `name`, `namespace`)
- source/provenance fields
- optional GO DAG summary (`parent/child` counts + relation types)
- optional GO DAG details (parent/child GO IDs)
- optional relationship summary by related entity group

## Report Name

`annotation_master_go`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)
- `include_go_relation_summary`: `bool` (default `True`)
- `include_go_relation_details`: `bool` (default `False`)
- `max_go_terms_per_side`: `int` (default `20`)
- `include_relationships`: `bool` (default `False`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_go",
    input_data=["GO:0006915", "GO:0008150"],
    include_go_relation_summary=True,
    include_go_relation_details=False,
    include_relationships=True,
)
```

API (`__ALL__`):

```python
df = bf.report.run(
    "annotation_master_go",
    input_data="__ALL__",
    include_go_relation_summary=True,
    include_go_relation_details=False,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_go \
  --input GO:0006915 --input GO:0008150 \
  --param include_go_relation_summary=true \
  --param include_go_relation_details=false \
  --param include_relationships=true
```

CLI (`__ALL__`):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_go \
  --param input_data=__ALL__ \
  --param include_go_relation_summary=true
```

## Notes

- `go_parent_count`/`go_child_count` summarize direct edges in `go_relations`.
- `go_parent_ids`/`go_child_ids` are optional and capped by `max_go_terms_per_side`.
- `entity_relationships_by_group`/`total_entity_relationships` summarize graph edges from `entity_relationships`.
- When `input_data="__ALL__"`, the report resolves and returns all GO entities available in `GOMaster`.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_master_pathway.md ===== -->

# Report Tutorial: `annotation_master_pathway`

## Purpose

Compact Pathway annotation report.
For each input pathway alias/ID, returns:

- `entity_id`
- `pathway_id`
- `pathway_description`
- pathway origin (`source_system` and `data_source`)
- optional relationship summary by related entity group

## Report Name

`annotation_master_pathway`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `include_relationships`: `bool` (default `False`)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_pathway",
    input_data=["R-HSA-109581", "hsa00010", "Cell cycle"],
    include_relationships=True,
    emit_not_found_rows=True,
)
```

API (`__ALL__`):

```python
df = bf.report.run(
    "annotation_master_pathway",
    input_data="__ALL__",
    include_relationships=True,
    include_aliases=False,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_pathway \
  --input R-HSA-109581 --input hsa00010 --input "Cell cycle" \
  --param include_relationships=true
```

CLI (`__ALL__`):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_pathway \
  --param input_data=__ALL__ \
  --param include_relationships=true
```

## Notes

- `entity_relationships_by_group` and `total_entity_relationships` are optional.
- When `include_relationships=false`, both columns are returned as null.
- Report does not include variant-level fields by design.
- When `input_data="__ALL__"`, the report resolves and returns all pathways available in `PathwayMaster`.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_master_protein.md ===== -->

# Report Tutorial: `annotation_master_protein`

## Purpose

Compact Protein annotation report using `ProteinMaster` as canonical base.
For each input protein alias/ID (including isoform aliases), returns:

- `entity_id` (input entity)
- canonical protein context (`protein_master_id`, `protein_id`)
- `canonical_entity_id` when available
- ProteinMaster metadata (`function`, `location`, `tissue_expression`, `pseudogene_note`)
- optional Pfam summary (counts by type, optional Pfam IDs by type)
- optional relationship summary by related entity group

## Report Name

`annotation_master_protein`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_pfam_summary`: `bool` (default `True`)
- `include_pfam_details`: `bool` (default `False`)
- `max_pfam_ids_per_type`: `int` (default `20`)
- `include_relationships`: `bool` (default `False`)
- `include_aliases`: `bool` (default `True`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_protein",
    input_data=["P04637", "P04637-2", "TP53_HUMAN"],
    include_pfam_summary=True,
    include_pfam_details=False,
    include_relationships=True,
)
```

API (`__ALL__`):

```python
df = bf.report.run(
    "annotation_master_protein",
    input_data="__ALL__",
    include_pfam_summary=True,
    include_pfam_details=False,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_protein \
  --input P04637 --input P04637-2 --input TP53_HUMAN \
  --param include_pfam_summary=true \
  --param include_pfam_details=false \
  --param include_relationships=true
```

CLI (`__ALL__`):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_protein \
  --param input_data=__ALL__ \
  --param include_pfam_summary=true
```

## Notes

- `ProteinMaster` stores canonical proteins; isoform context is inferred from `protein_entities`.
- `pfam_count_by_type` is compact and stable for V1.
- `pfam_ids_by_type` is optional and capped by `max_pfam_ids_per_type`.
- When `include_relationships=false`, relationship columns are returned as null.
- When `input_data="__ALL__"`, the report resolves and returns all protein entities available in `ProteinEntity`.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_master_variant.md ===== -->

# Report: `annotation_master_variant`

## Purpose

Full annotation expansion for an input list of variants.
Returns **one row per variant × transcript annotation**, joining:

- `variant_masters` — identity, population frequencies, and pre-computed pathogenicity scores
- `variant_molecular_effects` — VEP consequence per transcript
- `variant_effect_predictions` — AlphaMissense score and classification

Complements the other annotation master reports (`annotation_master_gene`,
`annotation_master_pathway`, etc.) with a variant-centric view.

---

## Input

Accepts rsID, chr:pos, or chr:pos:ref:alt formats, mixed in the same list or from a file:

| Format | Example | Behavior |
|---|---|---|
| rsID | `rs429358` | Lookup by dbSNP rsID |
| chr:pos | `chr19:44908684` | Returns **all** alleles at this position (SNVs only) |
| bare chr:pos | `19:44908684` | Same as above |
| chr:pos:ref:alt | `chr19:44908684:T:C` | Returns **only** the exact ref/alt variant (SNV or indel) |
| bare chr:pos:ref:alt | `19:44908684:T:C` | Same as above |
| file path | `./variants.txt` (one per line) | Mixed formats supported |

When ref/alt are provided, the allele filter is exact (uppercased). Use this form to disambiguate
multiallelic sites or to match credible-set variants reported as `chr:pos:ref:alt`.

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_data` | list \| path | required | rsID, chr:pos, or chr:pos:ref:alt list; file path (one per line) also accepted |
| `most_severe_only` | bool | `False` | Keep only the most-severe transcript annotation per variant (see below) |
| `canonical_only` | bool | `False` | Keep only canonical transcript annotations (see below) |

### `most_severe_only`

A single variant typically has **one annotation per transcript** it overlaps — often 5–20 rows
per variant. VEP flags one of those rows as the most biologically severe (the row with the
lowest `consequence_rank`, e.g. `stop_gained` outranks `missense_variant`, which outranks
`synonymous_variant`). The flag is stored as `is_most_severe_for_variant`.

- `False` (default) — returns **all transcript annotations** for every variant.
- `True` — returns **one row per variant**, picking the most-severe transcript. Use when you
  want a compact 1-row-per-variant table for downstream merging or reporting.

If no transcript has the flag set (rare; happens for some intergenic or regulatory hits), the
filter falls back to the full set so the variant is never dropped silently.

### `canonical_only`

Most genes have one **canonical** transcript — the principal isoform used as the reference for
HGVS notation, MANE Select recommendations, and most clinical pipelines. The `canonical`
boolean column marks it.

- `False` (default) — returns annotations from **all transcripts** (canonical + alternative).
- `True` — keeps only annotations on the canonical transcript. Useful when comparing variants
  across genes on equal footing (one isoform per gene).

If a variant has no canonical-transcript annotation in the DB, the filter falls back to the
full set for that variant rather than dropping it.

### Combining both

`most_severe_only=True` + `canonical_only=True` gives **one row per variant on the canonical
transcript**, prioritised by severity — the most concise output. Both filters have the same
fallback rule (never drop a variant entirely), so empty intersections degrade gracefully.

---

## Output columns

### Input tracking
| Column | Description |
|---|---|
| `input_value` | Original input string |
| `status` | `found` / `not_found` / `invalid_input` |
| `note` | Reason when status ≠ found |

### Variant identity
| Column | Description |
|---|---|
| `variant_id` | Internal DB identifier |
| `rsid` | dbSNP rsID (if available) |
| `chromosome` | Chromosome (integer) |
| `position_start` / `position_end` | Genomic coordinates |
| `reference_allele` / `alternate_allele` | Alleles |
| `variant_type` | `SNV`, `MNV`, `INS`, `DEL`, … |
| `allele_type` | Allele-level type |

### Population frequencies *(gnomAD)*
| Column | Description |
|---|---|
| `ac` | Allele count |
| `an` | Allele number |
| `af` | Allele frequency |
| `grpmax` | Ancestry group with highest AF |
| `grpmax_af` | Highest ancestry-group AF |

### Pathogenicity scores *(variant_masters)*
| Column | Description |
|---|---|
| `cadd_phred` | CADD Phred score |
| `cadd_raw_score` | CADD raw score |
| `revel_max` | REVEL max score |
| `spliceai_ds_max` | SpliceAI max delta score |
| `pangolin_largest_ds` | Pangolin largest delta score |
| `sift_max` | SIFT max score (lower = more deleterious) |
| `polyphen_max` | PolyPhen max score |

### Molecular effect *(one row per transcript — variant_molecular_effects)*
| Column | Description |
|---|---|
| `gene_symbol` | HGNC gene symbol |
| `gene_id` | Ensembl gene ID |
| `transcript_id` | Ensembl transcript ID |
| `feature_type` | `Transcript`, `RegulatoryFeature`, … |
| `consequence_raw` | Raw VEP consequence string |
| `consequence_name` | Resolved consequence term |
| `consequence_group` | Consequence group (e.g., `coding`, `splicing`) |
| `consequence_category` | Category (e.g., `loss_of_function`, `missense`) |
| `consequence_rank` | Severity rank (lower = more severe) |
| `impact_name` | VEP impact (`HIGH`, `MODERATE`, `LOW`, `MODIFIER`) |
| `impact_rank` | Impact severity rank |
| `biotype_name` | Transcript biotype |
| `is_most_severe_for_variant` | Boolean — most severe annotation across all transcripts |
| `is_most_severe_for_annotation` | Boolean — most severe within this annotation unit |
| `canonical` | Boolean — canonical transcript |
| `mane_select` | Boolean — MANE Select transcript |
| `mane_plus_clinical` | Boolean — MANE Plus Clinical transcript |
| `hgvsc` | HGVS coding notation |
| `hgvsp` | HGVS protein notation |
| `cdna_position` | cDNA position |
| `cds_position` | CDS position |
| `protein_position` | Protein position |
| `amino_acids` | Amino acid change |
| `codons` | Codon change |
| `variant_class` | SO variant class |
| `lof_confidence` | LoF confidence: `HC` (high), `LC` (low), `Filtered` |
| `lof_filter` | LoF filter flags |

### AlphaMissense predictions *(variant_effect_predictions)*
| Column | Description |
|---|---|
| `alphamissense_score` | AlphaMissense pathogenicity score (0–1) |
| `alphamissense_classification` | `likely_pathogenic`, `ambiguous`, `likely_benign` |

---

## Output sort order

`chromosome ASC → position_start ASC → is_most_severe_for_variant DESC → consequence_rank ASC`

Most severe transcript annotation appears first for each variant.

---

## API examples

### Basic run

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db")
bf.db.connect()

df = bf.report.run(
    "annotation_master_variant",
    input_data=["rs429358", "rs7412", "chr19:44908684"],
)

print(f"Rows: {len(df):,}")
df.head(10)
```

### Most-severe transcript only

```python
df = bf.report.run(
    "annotation_master_variant",
    input_data=["rs429358", "rs7412"],
    most_severe_only=True,
)
```

### Canonical transcript only

```python
df = bf.report.run(
    "annotation_master_variant",
    input_data=["rs429358", "rs7412"],
    canonical_only=True,
)
```

### From file

```python
df = bf.report.run(
    "annotation_master_variant",
    input_data="./my_variants.txt",   # one rsID or chr:pos per line
)
```

---

## CLI examples

```bash
# Basic
biofilter report run \
  --report-name annotation_master_variant \
  --input rs429358 --input rs7412 \
  --output variant_annotations.csv

# From file, most-severe only
biofilter report run \
  --report-name annotation_master_variant \
  --input-file ./variants.txt \
  --param most_severe_only=true \
  --output variant_annotations.csv

# Canonical only
biofilter report run \
  --report-name annotation_master_variant \
  --input rs429358 \
  --param canonical_only=true \
  --output variant_annotations_canonical.csv
```

---

## Expected scale

| Input variants | most_severe_only | Rows | Runtime |
|---|---|---|---|
| 10 | false | ~200–500 | < 5s |
| 10 | true | ~10–20 | < 5s |
| 500 | false | ~10k–50k | < 60s |
| 500 | true | ~500–1k | < 20s |

---

## Annotation master family

| Report | Domain |
|---|---|
| `annotation_master_gene` | Genes |
| `annotation_master_pathway` | Pathways |
| `annotation_master_protein` | Proteins |
| `annotation_master_disease` | Diseases |
| `annotation_master_go` | Gene Ontology terms |
| `annotation_master_chemical` | Chemical compounds |
| `annotation_master_variant` | **Variants** ← this report |

---

## Note — regulatory evidence (eQTL / sQTL) is **not** included here

This report aggregates **coding-level** evidence: VEP molecular effects, LoF (LOFTEE),
AlphaMissense, CADD, REVEL, SpliceAI, Pangolin, SIFT, PolyPhen, and gnomAD frequencies.
It does **not** read from `variant_gene_regulatory_evidence` (eQTL / sQTL from GTEx,
eQTLGen, etc.) — those tables answer a different question ("which gene does this variant
*regulate*?" vs. "what does this variant *do to a transcript*?").

For variant → regulated-gene mappings, use **[`annotation_variant_regulatory_evidence`](report_annotation_variant_regulatory_evidence.md)**.
It accepts gene symbols, coordinates, or rsIDs and supports filters by `tissue`, `qtl_type`,
and `p_value_max`. Run both reports side-by-side when a credible-set variant needs full
coverage (coding effect *and* distal regulatory target).



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_annotation_variant_regulatory_evidence.md ===== -->

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



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_db_pg_index_stats.md ===== -->

# Report Tutorial: `db_pg_index_stats`

## Purpose

PostgreSQL-only report with per-index details:

- size and method
- uniqueness/primary/valid/ready flags
- optional usage stats (`idx_scan`, `idx_tup_read`, `idx_tup_fetch`)

## Report Name

`db_pg_index_stats`

## Parameters (API)

- `schema`: `str | list[str]` (optional)
- `table`: `str | list[str]` (optional)
- `index`: `str | list[str]` (optional)
- `include_index_def`: `bool` (default `True`)
- `include_usage`: `bool` (default `True`)
- `output_columns`: `list[str]` (optional)

## Examples

CLI:

```bash
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_index_stats
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_index_stats --param schema=public --param table=variant_masters --param include_usage=true
```

API:

```python
df = bf.report.run(
    "db_pg_index_stats",
    schema="public",
    table=["variant_masters"],
    include_usage=True,
    output_columns=["schema_name", "table_name", "index_name", "index_size", "idx_scan"],
)
```

## Notes

- Fails on non-Postgres databases by design.
- Usage counters reset on PostgreSQL restart/stat reset.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_db_pg_table_stats.md ===== -->

# Report Tutorial: `db_pg_table_stats`

## Purpose

PostgreSQL-only report for table/storage observability:

- estimated rows
- table/index/toast/total size
- partition-aware output (leaf + parent aggregate)

## Report Name

`db_pg_table_stats`

## Parameters (API)

- `schema`: `str | list[str]` (optional)
- `table`: `str | list[str]` (optional)
- `output_columns`: `list[str]` (optional)

## Examples

CLI:

```bash
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_table_stats
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_table_stats --param schema=public --param 'table=["variant","entity"]'
```

API:

```python
df = bf.report.run(
    "db_pg_table_stats",
    schema="public",
    table=["variant", "entity"],
    output_columns=["schema_name", "table_name", "total_bytes", "n_indexes"],
)
```

## Notes

- Fails on non-Postgres databases by design.
- `rows_est` is catalog-estimated (fast), not exact count.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_entity_filter.md ===== -->

# Report Tutorial: `entity_filter`

## Purpose

Validates a list of entity names and returns matching entities, including conflict/deactivation flags.

## Report Name

`entity_filter`

## Required Parameters (API)

- `input_data`: `list[str]`

## Examples

API (recommended, because this report requires input list parameters):

```python
df = bf.report.run(
    "entity_filter",
    input_data=["BRCA1", "BRCA2", "TP53", "NOT_A_GENE"],
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_filter --input BRCA1 --input BRCA2 --input TP53 --input NOT_A_GENE
```

With input file:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_filter --input-file ./entities.txt
```

## Recommended Demo Columns

- `input_original`
- `primary_name`
- `group_name`
- `has_conflict`
- `is_deactive`
- `observation`



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_entity_neighborhood_summary.md ===== -->

# Report Tutorial: `entity_neighborhood_summary`

## Purpose

Resolves a heterogeneous list of inputs (genes, diseases, pathways, proteins, chemicals, GO terms) into entities and returns a **1-hop neighborhood summary** for each, with neighbor counts and lists grouped by entity type.

Useful as both a validation step (does each input resolve correctly?) and an exploratory view (what does each entity touch in the graph?).

## Report Name

`entity_neighborhood_summary`

## Required Parameters (API)

- `items`: `list[str]` — input terms, optionally with a `type:value` prefix.
  - Examples: `"gene:BRCA1"`, `"disease:Alzheimer disease"`, `"APOE"` (no hint)
  - Plain strings without `type:` are resolved across all groups.
- Alternative: `input_data`: `list[str]` — accepted as alias for `items`.

## Optional Parameters

| Parameter | Default | Description |
|---|---|---|
| `match_mode` | `"exact"` | `"exact"` \| `"like"` \| `"fuzzy"` |
| `similarity_threshold` | `80` | Score cutoff (0–100) for `fuzzy` mode |
| `aliases_top_n` | `20` | Limit for `Aliases Top` list per entity |
| `include_all_aliases` | `False` | When `True`, ignores `aliases_top_n` |
| `neighbors_top_n_per_type` | `50` | Limit for the per-type neighbor list columns |
| `emit_not_found_rows` | `False` | When `True`, emits rows with `Resolve Status="not_found"` for unresolved inputs |

## Match Modes

| Mode | Behavior | Engine support |
|---|---|---|
| `exact` | Exact match against `EntityAlias.alias_norm` (case-insensitive) | PostgreSQL, SQLite |
| `like` | Substring match (`%word%` both directions) | PostgreSQL, SQLite |
| `fuzzy` | rapidfuzz `token_sort_ratio` against all aliases in the (optionally scoped) group | PostgreSQL, SQLite |

The report runs entirely client-side for fuzzy matching — no `pg_trgm` or other database extension required. Works on a local SQLite installation.

## Type Hints

When prefixed (`gene:`, `disease:`, etc.), the resolution is **scoped to the matching `EntityGroup`**, avoiding cross-domain collisions. Without a prefix, the input is resolved across all groups.

| Hint | Resolves into group |
|---|---|
| `gene` / `genes` | Genes |
| `disease` / `diseases` | Diseases |
| `chemical` / `chemicals` | Chemicals |
| `pathway` / `pathways` | Pathways |
| `protein` / `proteins` | Proteins |
| `go` / `go_terms` / `goterms` | GO Terms |

## Output Columns

### Base columns (always present)

| Column | Description |
|---|---|
| `Input Word` | Original input string |
| `Input Type Hint` | The `type:` prefix used (or `None`) |
| `Resolver Mode` | The `match_mode` used for this run |
| `Entity ID` | Resolved entity (or `None` for `not_found`) |
| `Entity Type` | Lowercased singular form (`gene`, `disease`, etc.) |
| `Exact Match` | `True` when `input.lower().strip() == matched_name.lower().strip()` |
| `Matched Name` | The actual `alias_value` that matched the input |
| `Primary Alias` | The entity's canonical/primary alias |
| `Aliases Top` | JSON list of top-N aliases for the entity |
| `Alias Count` | Total alias count for the entity (full count, not truncated) |
| `Degree Total (1-hop)` | Number of distinct 1-hop neighbors |
| `Degree By Type (1-hop)` | JSON object: `{group_name: count}` |
| `Resolve Status` | `resolved` \| `not_found` |
| `Resolve Method` | Echo of the match mode used |
| `Resolve Score` | `1.0` for exact; `None` for like; rapidfuzz score for fuzzy |

### Dynamic per-type columns

One column per `EntityGroup.name` in the database (`Genes`, `Proteins`, `Pathways`, `Diseases`, `Chemicals`, `GO Terms`, …). Each cell is a JSON list of neighbor primary names of that type.

## Examples

### API — mixed input types

```python
df = bf.report.run(
    "entity_neighborhood_summary",
    items=[
        "gene:BRCA1",
        "disease:Alzheimer disease",
        "pathway:DNA repair",
        "APOE",  # no type hint — searches all groups
    ],
    match_mode="exact",
    aliases_top_n=10,
    neighbors_top_n_per_type=20,
    emit_not_found_rows=True,
)
```

### API — fuzzy with custom threshold

```python
df = bf.report.run(
    "entity_neighborhood_summary",
    items=["gene:BRCA1", "disease:alzheimers"],
    match_mode="fuzzy",
    similarity_threshold=70,
)
```

### API — substring search across many pathways

```python
df = bf.report.run(
    "entity_neighborhood_summary",
    items=[f"pathway:{name}" for name in pathway_list],
    match_mode="like",
)
```

### CLI

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name entity_neighborhood_summary \
  --input "gene:BRCA1" \
  --input "disease:Alzheimer disease" \
  --param match_mode=exact \
  --param emit_not_found_rows=true \
  --output neighborhood.csv
```

## Notes and Caveats

- **One row per (input, entity) pair.** When an input resolves to multiple entities (genuine ambiguity), the report emits one row per entity. When the same entity has multiple aliases that match the input (common in `like` mode), they are collapsed into a single row.
- **Type hint scope is recommended** for ambiguous strings. Searching `"BRCA1"` without a hint may match across Genes and any other group where `BRCA1` happens to be a synonym.
- **Fuzzy threshold tuning.** The `token_sort_ratio` scorer penalizes length differences. Searching `"alzheimer"` (single token) against `"Alzheimer disease"` (two tokens) gives ~67%. For substring-style queries, lower the threshold (60–70) or pre-filter with `like` mode first.
- **Engine-agnostic.** No `pg_trgm` or other PostgreSQL-only extensions are used. Works equally on SQLite for local development.
- **Neighborhood counts after truncation.** `Degree Total (1-hop)` reflects the actual distinct neighbor count; `neighbors_top_n_per_type` only truncates the displayed list, not the count.

## Recommended Demo Columns

- `Input Word`
- `Entity ID`
- `Exact Match`
- `Matched Name`
- `Primary Alias`
- `Degree Total (1-hop)`
- `Degree By Type (1-hop)`
- The dynamic per-type columns relevant to the use case



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_etl_packages.md ===== -->

# Report Tutorial: `etl_packages`

## Purpose

Detailed ETL audit report with package-level records and timing/hash fields for extract, transform, and load.

## Report Name

`etl_packages`

## Parameters (API)

- `source_system`: `str | list[str]` (optional)
- `data_sources`: `str | list[str]` (optional)
- `only_active`: `bool` (default `True`)

## Examples

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name etl_packages
```

API:

```python
df = bf.report.run(
    "etl_packages",
    source_system="NCBI",
    data_sources=["dbsnp_chr1", "dbsnp_chr2"],
    only_active=True,
)
```

## Recommended Demo Columns

- `package_id`
- `source_system`
- `data_source`
- `status`
- `operation_type`
- `extract_status`
- `transform_status`
- `load_status`
- `extract_minutes`
- `transform_minutes`
- `load_minutes`



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_etl_status.md ===== -->

# Report Tutorial: `etl_status`

## Purpose

Shows the latest good ETL status per data source (extract/transform/load) and alignment flags.

## Report Name

`etl_status`

## Parameters (API)

- `source_system`: `str | list[str]` (optional)
- `data_sources`: `str | list[str]` (optional)
- `only_active`: `bool` (default `True`)

## Examples

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name etl_status
```

API:

```python
df = bf.report.run(
    "etl_status",
    source_system=["NCBI", "Ensembl"],
    data_sources=["dbsnp_chr1", "hgnc"],
    only_active=True,
)
```

## Recommended Demo Columns

- `source_system`
- `data_source`
- `extract_status`
- `transform_status`
- `load_status`
- `pipeline_ok`
- `latest_error`



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md ===== -->

# Report Tutorial: `gene_to_variant_filtering`

## Purpose

Phase 2 of the single-variant SNP×SNP interaction pipeline.

Given a list of gene symbols, this report collects the variants overlapping each gene's
genomic locus, with all heavy filters pushed to the SQL layer before data reaches Python.

---

## Pipeline Context

```
Phase 1 — Gene Discovery  (variant_single_gene_annotation)
  input : one variant (chr:pos or rsID)
  output: seed gene + partner-gene list with shared-group annotation
  scale : ~8 k rows (tractable)
          ↓ partner gene symbol list

Phase 2 — Filtered Variant Collection  (this report)
  input : list of gene symbols
  output: one row per (gene × variant) with consequence and prediction annotations
  scale : ~15 k–100 k rows, controlled by filters

Phase 3 — Pair Generation  (planned)
  input : Phase 2 variant sets per gene
  output: variant × variant interaction pairs (seed × partner)
  scale : controlled by Phase 2 filtering
```

Separating gene discovery (Phase 1, tractable) from variant enumeration (Phase 2, SQL-filtered)
prevents the combinatorial explosion that occurs when all variants are annotated before filtering.
Without pre-filtering, a gene like APOE with ~1 k variants × 300 partner genes would produce
~260 M rows before any filter is applied.

---

## Report Name

`gene_to_variant_filtering`

---

## Parameters (API)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gene_symbols` | `list[str]` or comma-string | **required** | Gene symbols to query. Also accepted as `input_data` (alias used by `run_example`). |
| `build` | `int` | `38` | Genome assembly build for locus lookup via `entity_locations`. |
| `gene_window_bp` | `int` | `0` | Extend each gene locus by this many bp on each side before querying variants. Useful for capturing regulatory/nearby variants. |
| `most_severe_only` | `bool` | `True` | Keep only the row flagged `is_most_severe_for_variant=TRUE` in `variant_molecular_effects`. See section **Unit of analysis** below. |
| `impact_filter` | `list[str]` | `None` | Impact names to keep, e.g. `["HIGH", "MODERATE"]`. |
| `consequence_type_filter` | `list[str]` | `None` | Consequence group names, category names, or individual consequence names. Pre-resolved to `consequence_id`s before the main query. |
| `lof_confidence_filter` | `list[str]` | `None` | LoF confidence tiers to keep, e.g. `["HC"]` or `["HC", "LC"]`. Filters `lof_confidence` in `variant_molecular_effects`. |
| `af_max` | `float` | `None` | Maximum allele frequency — rare-variant mode (e.g. `0.01`). |
| `af_min` | `float` | `None` | Minimum allele frequency — common-variant mode (e.g. `0.05`). |
| `cadd_phred_min` | `float` | `None` | Minimum CADD Phred score (e.g. `20`). Applied on `variant_masters.cadd_phred`. |
| `sift_score_max` | `float` | `None` | Maximum SIFT score (lower = more deleterious; e.g. `0.05`). Applied on `variant_masters.sift_max`. |
| `polyphen_score_min` | `float` | `None` | Minimum PolyPhen score (higher = more damaging; e.g. `0.85`). Applied on `variant_masters.polyphen_max`. |
| `alphamissense_score_min` | `float` | `None` | Minimum AlphaMissense score. Applied Python-side after LEFT JOIN. |
| `alphamissense_classification` | `list[str]` | `None` | AlphaMissense classifications to keep, e.g. `["likely_pathogenic", "ambiguous"]`. Applied Python-side. |
| `max_variants_per_gene` | `int` | `5000` | Safety cap per gene after all filters. Emits a WARNING if exceeded. |

---

## Unit of Analysis: `most_severe_only`

This is the most important design decision for the pipeline.

### `most_severe_only=True` (default — recommended for SNP×SNP pipeline)

The unit of analysis is the **variant** — not the transcript, not the allele.

```
variant_masters          (1 row per ALT allele)
    ↓ JOIN on (variant_id, chromosome)
variant_molecular_effects  WHERE is_most_severe_for_variant = TRUE
    → exactly 1 row per variant (the worst consequence across all transcripts)
    → impact, consequence_type, and LoF filters apply to this single row
    ↓ LEFT JOIN on (variant_id, chromosome)   ← NO transcript_id in join key
variant_effect_predictions  WHERE predictor_key = 'alphamissense'
    → AlphaMissense is variant-level — attaches cleanly to the 1 row per variant
```

**Consequence**: if a variant passes the molecular-effect filters (e.g. impact=HIGH) AND
passes the prediction filters (e.g. alphamissense=likely_pathogenic), these are evaluated
on the **same object** — the variant's most-severe consequence record. No transcript mixing.

Result: **1 row per (gene × variant)**.

### `most_severe_only=False` (advanced use)

The unit of analysis is the **transcript**.

Each variant appears N times — once per transcript in `variant_molecular_effects`.
AlphaMissense and other prediction values repeat on every transcript row (they are
stored at the variant level in `variant_effect_predictions`, not per-transcript).

This mode is useful for:
- Splice analysis (identifying which specific transcripts are affected)
- Canonical-transcript-only analysis (filter by `canonical=True` or `mane_select=True` in post-processing)
- Studies that require full transcript-level consequence decomposition

Result: **1 row per (gene × variant × transcript)**.

---

## Consequence Filter Resolution

`consequence_type_filter` accepts names at three levels of granularity:

```
Level 1 — Group       : "transcript_variant", "intergenic_variant", ...
Level 2 — Category    : "coding_sequence_variant", "splice_region_variant", ...
Level 3 — Individual  : "missense_variant", "stop_gained", "frameshift_variant", ...
```

Resolution steps (executed before the main query):

```
consequence_type_filter = ["missense_variant", "splice_region_variant"]
  → query variant_consequence_groups WHERE name IN (...)  → group_ids
  → query variant_consequence_categories WHERE name IN (...) → category_ids
  → query variant_consequences WHERE name IN (...) → direct_ids
  → query variant_consequences WHERE group_id IN (...) OR category_id IN (...) → inherited_ids
  → consequence_ids = inherited_ids ∪ direct_ids
  → main query: WHERE vme.consequence_id IN (consequence_ids)
```

This allows mixing levels in a single filter list.

---

## LoF Filter

The `lof_confidence_filter` parameter filters on `variant_molecular_effects.lof_confidence`.

LOFTEE (Loss-of-Function Transcript Effect Estimator) annotates LoF variants with:

| Value | Meaning |
|---|---|
| `HC` | High Confidence — curated, reliable LoF calls |
| `LC` | Low Confidence — LoF call with caveats |

Typical use cases:
- `["HC"]` — strict, pipeline-grade LoF filtering
- `["HC", "LC"]` — inclusive mode, flag LC separately

Note: `lof_confidence` is `NULL` for non-LoF variants. This filter is applied in SQL with
`AND vme.lof_confidence IN (...)`, which naturally excludes NULLs — i.e., it keeps **only**
variants with the specified LoF confidence tier.

---

## Effect Prediction Sources

| Score | Source table | Column | Applied at |
|---|---|---|---|
| CADD Phred | `variant_masters` | `cadd_phred` | SQL (WHERE clause) |
| SIFT | `variant_masters` | `sift_max` | SQL (WHERE clause) |
| PolyPhen | `variant_masters` | `polyphen_max` | SQL (WHERE clause) |
| AlphaMissense score | `variant_effect_predictions` | `score` WHERE `predictor_key='alphamissense'` | Python (post LEFT JOIN) |
| AlphaMissense class | `variant_effect_predictions` | `classification` WHERE `predictor_key='alphamissense'` | Python (post LEFT JOIN) |

CADD, SIFT, and PolyPhen are stored directly on `variant_masters` as pre-aggregated variant-level
summaries (`cadd_phred`, `sift_max`, `polyphen_max`), which allows SQL-level filtering without
any additional join. AlphaMissense requires a LEFT JOIN to `variant_effect_predictions` and is
therefore filtered in Python after the query.

---

## Query Architecture — Temp Table and Partition-Aware Design

### Problem

With 8 k+ genes in the input (typical Phase 2 call), some chromosomes have hundreds of genes.
A standard `OR (position BETWEEN start1 AND end1) OR (position BETWEEN start2 AND end2) OR ...`
clause becomes unmanageable and prevents the query planner from using indexes efficiently.

### Solution: Temp table + range JOIN per chromosome

```sql
-- Step 1: created once per report run
CREATE TEMP TABLE _bf_gene_ranges (
    gene_entity_id BIGINT,
    gene_symbol    TEXT,
    chromosome     INTEGER,
    range_start    BIGINT,
    range_end      BIGINT
)

-- Step 2: populated with all gene loci (including gene_window_bp expansion)
INSERT INTO _bf_gene_ranges VALUES (...)  -- batch inserts, 500 rows at a time

-- Step 3: one query per chromosome
SELECT ...
FROM _bf_gene_ranges gr
JOIN variant_masters vm
    ON  vm.chromosome      = gr.chromosome
    AND vm.position_start >= gr.range_start
    AND vm.position_start <= gr.range_end
JOIN variant_molecular_effects vme
    ON  vme.variant_id = vm.variant_id
    AND vme.chromosome = vm.chromosome
    AND vme.is_most_severe_for_variant = true  -- when most_severe_only=True
    [AND vme.impact_id IN (...)]
    [AND vme.consequence_id IN (...)]
    [AND vme.lof_confidence IN (...)]
LEFT JOIN (
    SELECT chromosome, variant_id,
           MAX(score) AS am_score, MAX(classification) AS am_class
    FROM variant_effect_predictions
    WHERE chromosome = :chromosome AND predictor_key = 'alphamissense'
    GROUP BY chromosome, variant_id
) vep_am ON vep_am.variant_id = vm.variant_id AND vep_am.chromosome = vm.chromosome
WHERE vm.chromosome = :chromosome
  AND gr.chromosome = :chromosome
  [AND vm.af <= :af_max]
  [AND vm.cadd_phred >= :cadd_phred_min]
  ...
```

**Why one query per chromosome?**
- `variant_masters` is partitioned by `chromosome` on PostgreSQL. Filtering `WHERE vm.chromosome = :chromosome` ensures the query planner uses partition pruning, reading only the relevant child partition.
- The BETWEEN condition on `position_start` leverages the B-tree index on `(chromosome, position_start)`.

**Why a temp table instead of a CTE or subquery?**
- A temp table is materialized — the gene ranges are evaluated once and indexed by the planner.
- A CTE may be inlined or re-evaluated per row depending on the PostgreSQL version and query shape.
- For 8 k+ genes, the temp table approach is consistently faster.

### Variant-gene assignment

A variant can overlap multiple gene loci in the input list. In that case, the report produces
one row per overlapping gene — the same variant appears multiple times, once per gene. This is
the correct behavior for Phase 3, where we need to know which gene each variant belongs to in
order to form gene-level pairs.

---

## Output Columns

| Column | Description |
|---|---|
| `resolution_status` | `None` on success; an error code on failure. |
| `gene_input` | The gene symbol used as input (same as `gene_symbol` on success). |
| `gene_entity_id` | Internal entity ID of the gene. |
| `gene_symbol` | HGNC symbol (from `gene_masters.symbol`). |
| `gene_chromosome` | Chromosome of the gene. |
| `gene_start` | Gene locus start (including `gene_window_bp` expansion). |
| `gene_end` | Gene locus end (including `gene_window_bp` expansion). |
| `variant_id` | Internal variant ID (`variant_masters`). |
| `chromosome` | Chromosome of the variant (integer: X=23, Y=24, MT=25). |
| `position_start` | Variant start position (GRCh38). |
| `position_end` | Variant end position. |
| `rsid` | dbSNP rsID (if available). |
| `reference_allele` | Reference allele. |
| `alternate_allele` | Alternate allele. |
| `af` | Allele frequency (gnomAD). |
| `transcript_id` | Transcript ID from VEP annotation. |
| `consequence_id` | Internal consequence ID (FK to `variant_consequences`). |
| `consequence_name` | Consequence name (e.g. `missense_variant`). |
| `consequence_group` | Consequence group name. |
| `consequence_category` | Consequence category name. |
| `impact_id` | Internal impact ID. |
| `impact_name` | Impact name (`HIGH`, `MODERATE`, `LOW`, `MODIFIER`). |
| `is_most_severe_for_variant` | Whether this is the most severe consequence row for the variant. |
| `hgvsc` | HGVS coding notation. |
| `hgvsp` | HGVS protein notation. |
| `lof_flag` | Boolean LoF flag from LOFTEE. |
| `lof_confidence` | LOFTEE tier: `HC`, `LC`, or NULL. |
| `lof_filter` | LOFTEE filter reason (when LC). |
| `lof_flags` | Additional LOFTEE flags. |
| `canonical` | Whether this is the canonical transcript. |
| `mane_select` | Whether this is the MANE Select transcript. |
| `cadd_phred` | CADD Phred score (variant-level, from `variant_masters`). |
| `sift_max` | SIFT max score across transcripts (from `variant_masters`). |
| `polyphen_max` | PolyPhen max score across transcripts (from `variant_masters`). |
| `alphamissense_score` | AlphaMissense pathogenicity score (from `variant_effect_predictions`). |
| `alphamissense_classification` | AlphaMissense classification (`likely_pathogenic`, `ambiguous`, `likely_benign`). |

### Resolution Status Codes

| Code | Meaning |
|---|---|
| `(None)` | Success. |
| `empty_gene_list` | `gene_symbols` was provided but resolved to an empty list. |
| `no_genes_resolved` | None of the provided symbols matched any `entity_aliases` entry for the Genes group. |
| `no_loci_found` | Gene entity_ids were resolved but none had entries in `entity_locations` for the requested build. |
| `no_variants_found` | Genes and loci were resolved but no variants passed all filters. |

---

## Examples

### API

```python
from biofilter import Biofilter

bf = Biofilter()

# ── Basic: one gene, no filters
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE"],
)

# ── Multiple genes, HIGH/MODERATE impact only
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU", "TOMM40", "BIN1"],
    impact_filter=["HIGH", "MODERATE"],
    most_severe_only=True,
)

# ── Rare variants with LoF HC
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["BRCA1", "BRCA2"],
    af_max=0.001,
    lof_confidence_filter=["HC"],
    impact_filter=["HIGH"],
)

# ── AlphaMissense pathogenic missense variants
df = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU"],
    consequence_type_filter=["missense_variant"],
    alphamissense_classification=["likely_pathogenic"],
    most_severe_only=True,
)

# ── Full pipeline Phase 2: pass gene list from Phase 1 output
df_phase1 = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",
    group_entity_type="Pathways",
    source_system_filter=["Reactome"],
)

partner_genes = df_phase1["partner_gene_symbol"].dropna().unique().tolist()
seed_gene     = df_phase1["seed_gene_symbol"].iloc[0]
all_genes     = [seed_gene] + partner_genes

df_phase2 = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=all_genes,
    impact_filter=["HIGH", "MODERATE"],
    af_max=0.05,
    most_severe_only=True,
)
```

### CLI

```bash
# ── Basic single gene
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols=APOE

# ── Multiple genes, HIGH/MODERATE impact
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="APOE,CLU,TOMM40,BIN1" \
  --param impact_filter="HIGH,MODERATE" \
  --param most_severe_only=true

# ── Rare LoF variants
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="BRCA1,BRCA2" \
  --param af_max=0.001 \
  --param lof_confidence_filter=HC \
  --param impact_filter=HIGH

# ── Save to CSV
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="APOE,CLU" \
  --param impact_filter="HIGH,MODERATE" \
  --output phase2_variants.csv

# ── Inspect params template
biofilter report run \
  --report-name gene_to_variant_filtering \
  --params-template
```

---

## Expected Scale

| Scenario | Genes | Variants (estimated) |
|---|---|---|
| APOE locus, no filter | 1 | ~1 000 |
| APOE locus, `most_severe_only=True` | 1 | ~100 |
| APOE locus, HIGH/MODERATE + AF < 0.01 | 1 | ~20–30 |
| Phase 1 APOE Reactome partners (~300 genes), HIGH/MODERATE + AF < 0.05 | ~300 | ~15 000 |
| Phase 1 APOE all-source partners (~8 k genes), no filter | ~8 000 | potentially millions — use filters |

**Always apply at minimum `most_severe_only=True` (default) when using output for Phase 3.**

---

## Demo Tips

- Start with `gene_symbols=["APOE"]` and `impact_filter=["HIGH", "MODERATE"]` — fast and produces a readable result.
- Check `resolution_status` first; a non-null value explains why no variants were returned.
- Use `alphamissense_classification=["likely_pathogenic"]` to quickly isolate the most interesting missense variants.
- For LoF studies, combine `lof_confidence_filter=["HC"]` + `impact_filter=["HIGH"]` — these two filters are complementary and together capture curated loss-of-function calls.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_platform_data_statistics.md ===== -->

# Report Tutorial: `platform_data_statistics`

## Purpose

Platform-level statistics report for operational dashboards.
Returns a compact long-format dataset with:

- entity counts by omic domain (`EntityGroup`)
- variant counts by chromosome (`variant_masters`)
- relationship counts by group pair (`entity_relationships`)
- datasources ingested and latest load execution metadata (`etl_packages`)

## Report Name

`platform_data_statistics`

## Output Shape

Rows are returned in long format using these columns:

- `section`
- `metric`
- `dimension_1`
- `dimension_2`
- `value_number`
- `value_text`
- `as_of`
- `note`

This shape is ideal for pivoting and charting in notebooks.

## Parameters (API)

- `sections`: `list[str]` or comma-separated string (optional)
  - Allowed values:
    - `entity_counts_by_group`
    - `variant_counts_by_chromosome`
    - `relationship_counts_by_group_pair`
    - `datasource_latest_load`
  - Default: all sections.
- `only_active_entities`: `bool` (default `True`)
  - When `True`, entity counts exclude only explicit inactive rows (`is_active=False`).
- `relationship_mode`: `"undirected" | "directed"` (default `"undirected"`)
- `include_totals`: `bool` (default `True`)

## Examples

API (all sections):

```python
df = bf.report.run(
    "platform_data_statistics",
    only_active_entities=True,
    relationship_mode="undirected",
    include_totals=True,
)
```

API (selected sections):

```python
df = bf.report.run(
    "platform_data_statistics",
    sections=["entity_counts_by_group", "datasource_latest_load"],
    include_totals=False,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name platform_data_statistics \
  --param relationship_mode=undirected \
  --param include_totals=true
```

## Notes

- Variant section depends on `variant_masters`; if unavailable, a note row is emitted.
- Relationship counts are aggregated by group pair (domain-domain view), not per entity.
- Datasource section emits multiple metrics per datasource:
  - `latest_load_package_id`
  - `latest_load_status`
  - `latest_load_end`
  - `latest_load_rows`
  - `latest_load_age_days`



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_snp_snp_model.md ===== -->

# Report Tutorial: `snp_snp_model`

## Purpose

Builds BF4 candidate interaction models in two layers:

- `gene_pair`: genes connected by shared biological groups (for example pathways)
- `snp_pair`: variant pairs expanded from those gene pairs
- `Direct Gene` mode is also supported (gene-gene links without intermediate groups)

The report starts from user seed positions (`chr:position`) and maps to variants and genes using `entity_locations`.

## Report Name

`snp_snp_model`

## Core Pipeline

1. Input seed positions (`chr:position`)
2. Resolve seed variants in `variant_masters` using only `allele_type = SNV`
   and collapse multi-allelic rows to one logical variant
3. Map seed variants to seed genes by genomic overlap in `entity_locations`
4. Expand seed genes to biological groups using `entity_relationships`
5. Expand back from those groups to additional genes
6. Build gene-gene pairs by co-membership in the same group
7. Expand variants for genes and generate SNP-SNP pairs

## Required Parameters (API)

- `input_data`: `list[str|dict]` (or a path to a text file)

## Main Parameters

- `build` (default `38`)
- `window_bp` (default `0`)
- `group_entity_groups` (default `['Pathway','Pathways']`)
  - special option: `Direct Gene`
- `group_data_sources` (optional; data source names or IDs used to filter grouping links only)
- `group_entities` (optional explicit group entity names)
- `relationship_types` (optional; default uses all relationship types)
- `gene_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `snp_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `expand_variants_from_expanded_genes` (default `True`)
- `include_gene_pairs` / `include_snp_pairs`
- `limit_variants_per_gene` (default `2000`)
- `max_snp_pairs` (default `200000`)

## Examples

API (inline inputs):

```python
df = bf.report.run(
    "snp_snp_model",
    input_data=["chr17:150", "chr17:280"],
    group_entity_groups=["Pathway"],
    gene_pair_scope="at_least_one_from_seed",
    snp_pair_scope="at_least_one_from_seed",
)
```

CLI (inputs from TXT file):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input-file ./positions.txt \
  --param build=38 \
  --param group_entity_groups='["Pathway"]' \
  --param group_data_sources='["Reactome"]' \
  --output snp_snp_from_txt.csv
```

`positions.txt` format (one position per line):

```text
chr19:44904604
chr1:13259
chr15:63279422
```

CLI (inputs from CSV file):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input-file ./positions.csv \
  --input-column position \
  --param build=38 \
  --param group_entity_groups='["Pathway"]' \
  --output snp_snp_from_csv.csv
```

`positions.csv` example:

```csv
sample_id,position
S1,chr19:44904604
S2,chr1:13259
S3,chr15:63279422
```

## Output Row Types

- `input`: invalid/not-found input traces
- `gene_pair`: gene-gene candidate models
- `snp_pair`: SNP-SNP models expanded from gene pairs
- `summary`: truncation/no-model messages when relevant

## Variant Selection Rules

- Uses only SNV rows when `variant_masters.allele_type` is available
- Collapses alternate-allele duplicates and keeps one deterministic row per
  logical variant (preferably by `rsid`, otherwise by locus/ref)

## Group Selection Notes

- If no `group_entity_groups` is provided, the report defaults to `Pathway/Pathways`.
- You can provide multiple group types (for example `['Pathway', 'GO']`).
- Use `Direct Gene` to model direct gene-gene links without a group intermediary.
- `group_data_sources` filters only the grouping step (gene->group, group->gene, and Direct Gene links);
  variant-to-gene mapping is unchanged.
- If group types are invalid, the report returns a friendly error listing available options.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_snp_snp_pair_generator.md ===== -->

# Report: `snp_snp_pair_generator`

## Purpose

**Phase 3** of the SNP×SNP interaction pipeline.

Takes a LD-pruned variant list (**Lista D**, from PLINK) and the biologically
annotated variant source (**Lista A**, from `gene_to_variant_filtering`), generates
all variant pairs according to the chosen strategy, and returns a fully annotated
pair DataFrame ready for statistical interaction testing (PLINK epistasis, SAIGE,
custom logistic regression, etc.).

---

## Pipeline context

```
Phase 1  variant_single_gene_annotation  →  gene list (~8k genes)
Phase 2  gene_to_variant_filtering       →  Lista A (annotated variants, CSV)
Phase 2.5 variant_list_intersect         →  Lista C  (genotyped subset)
[PLINK]  --indep-pairwise                →  Lista D  (LD-independent)
Phase 3  snp_snp_pair_generator          →  this report — annotated pairs
```

Full tutorial: `notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb`

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `variant_list` | str (path) | required | Lista D from PLINK (`.prune.in`, `.txt`, `.list`) — one variant ID per line |
| `annotation_source` | str (path) | required | Lista A CSV from `gene_to_variant_filtering` — provides all annotation columns |
| `pairing_strategy` | str | `"seed_vs_all"` | How to generate pairs — see strategies below |
| `seed_gene` | str \| None | `None` | Gene symbol to use as seed for `seed_vs_all` (resolved from `annotation_source`) |
| `seed_variants` | list \| None | `None` | Explicit list of seed variant IDs for `seed_vs_all` (alternative to `seed_gene`) |
| `max_pairs` | int | `1_000_000` | Safety cap — report aborts if estimated pairs exceed this value |
| `exclude_same_gene` | bool | `True` | Exclude pairs where both variants belong to the same gene |

---

## Pairing strategies

### `seed_vs_all` *(recommended for gene-centric studies)*

Pairs every **seed** variant against every **non-seed** variant.

```
n_seed × n_other
```

Seed is resolved from `annotation_source`:
- `seed_gene="APOE"` → all Lista D variants where `gene_symbol == "APOE"`
- `seed_variants=["rs429358", "rs7412"]` → explicit IDs

Best choice when the study revolves around a specific gene (e.g., APOE in
Alzheimer's) and the goal is to test interactions between the seed locus
and all pathway partners.

**Scale example:** 12 APOE variants × 11,988 partners = **~144k pairs**

---

### `cross_gene`

All unique pairs between variants from **different genes**.
Same-gene pairs are always excluded regardless of `exclude_same_gene`.

```
all_vs_all − same_gene_pairs
```

Use when there is no specific seed gene and the goal is to test all
inter-gene interactions within the pathway.

**Scale example:** 12k variants, avg 5 variants/gene → ~70M pairs (upper bound).
Apply aggressive Phase 2 filters before using this strategy.

---

### `all_vs_all`

All unique ordered pairs, including same-gene pairs.

```
n × (n − 1) / 2
```

Most permissive — typically only practical with small Lista D (< 2k variants).
The `max_pairs` safety check will abort if the estimate exceeds the limit.

---

## Safety check

Before materialising any pairs, the report **estimates** the pair count and
compares it to `max_pairs`:

- If estimate ≤ `max_pairs` → proceeds normally
- If estimate > `max_pairs` → aborts immediately and returns a single-row
  DataFrame with `resolution_status = "pair_limit_exceeded"`

```python
# Example abort response
{
    "resolution_status": "pair_limit_exceeded",
    "estimated_pairs":   72_000_000,
    "max_pairs":         1_000_000,
    "pairing_strategy":  "all_vs_all",
    "suggestion":        "Switch to seed_vs_all or apply stricter Phase 2 filters …"
}
```

The suggestion message guides the user toward a feasible configuration.

---

## Annotation enrichment

The report joins Lista D variant IDs to Lista A using the same dual-key
matching strategy as `variant_list_intersect`:

1. **rsID match** — if the Lista D ID looks like `rs\d+`
2. **chr:pos match** — fallback for IDs like `19:44908684` or `chr19:44908684`

Every column present in `annotation_source` is carried through to the output,
mirrored on both sides of each pair with `_a` / `_b` suffixes.

Variants in Lista D that are **not found** in `annotation_source` are dropped
with a warning log. They were in Lista D but not in Lista A — typically
variants that passed LD pruning but fell outside the gene windows defined in
Phase 2.

---

## Output DataFrame

One row per variant pair. All annotation columns are mirrored:

| Column | Description |
|---|---|
| `rsid_a` | rsID of variant A |
| `gene_symbol_a` | Gene of variant A |
| `consequence_name_a` | Most severe consequence for variant A |
| `impact_name_a` | VEP impact for variant A (`HIGH`, `MODERATE`, …) |
| `af_a` | Allele frequency of variant A |
| `cadd_phred_a` | CADD Phred score for variant A |
| `alphamissense_score_a` | AlphaMissense score for variant A |
| `alphamissense_classification_a` | AlphaMissense class for variant A |
| *(…same columns with `_b` suffix for variant B…)* | |
| `same_gene` | `True` if both variants belong to the same gene |
| `pairing_strategy` | Strategy used to generate this pair |

---

## Resolution status codes

| `resolution_status` | Meaning |
|---|---|
| *(absent — normal output)* | Pairs generated successfully |
| `pair_limit_exceeded` | Estimated pairs exceed `max_pairs`; no pairs generated |
| `no_variants_matched` | No Lista D variants found in `annotation_source` |
| `seed_not_found` | `seed_gene` / `seed_variants` produced no matches in the enriched list |

---

## API examples

### Standard use — seed_vs_all

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db")
bf.db.connect()

df_pairs = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "seed_vs_all",
    seed_gene         = "APOE",
    max_pairs         = 1_000_000,
    exclude_same_gene = True,
)

print(f"Pairs: {len(df_pairs):,}")
df_pairs.head()
```

### Explicit seed variants

```python
df_pairs = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "seed_vs_all",
    seed_variants     = ["rs429358", "rs7412"],   # APOE ε4 and ε2 alleles
)
```

### Cross-gene (no seed)

```python
df_pairs = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "cross_gene",
    max_pairs         = 500_000,
)
```

### Handling safety abort

```python
df = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = "pipeline_output/lista_D.prune.in",
    annotation_source = "pipeline_output/lista_A.csv",
    pairing_strategy  = "all_vs_all",
)

if "resolution_status" in df.columns:
    status = df["resolution_status"].iloc[0]
    if status == "pair_limit_exceeded":
        print(df["suggestion"].iloc[0])
else:
    print(f"{len(df):,} pairs ready")
```

---

## CLI examples

```bash
# seed_vs_all — APOE seed
biofilter report run \
  --report-name snp_snp_pair_generator \
  --param variant_list=pipeline_output/lista_D.prune.in \
  --param annotation_source=pipeline_output/lista_A.csv \
  --param pairing_strategy=seed_vs_all \
  --param seed_gene=APOE \
  --output pipeline_output/phase3_pairs.csv

# cross_gene — no seed
biofilter report run \
  --report-name snp_snp_pair_generator \
  --param variant_list=pipeline_output/lista_D.prune.in \
  --param annotation_source=pipeline_output/lista_A.csv \
  --param pairing_strategy=cross_gene \
  --param max_pairs=500000 \
  --output pipeline_output/phase3_pairs.csv

# Inspect params template
biofilter report run \
  --report-name snp_snp_pair_generator \
  --params-template
```

---

## Expected scale

| Scenario | Lista D | Strategy | Pairs | Runtime |
|---|---|---|---|---|
| Single gene seed, small pathway | 200 | `seed_vs_all` | ~2k | < 1s |
| APOE seed, Reactome pathway | ~12k | `seed_vs_all` | ~144k | < 5s |
| Medium pathway, no seed | ~2k | `cross_gene` | ~2M | ~10s |
| Large pathway, no seed | ~12k | `cross_gene` | ~70M | **safety abort** |



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_variant_binning.md ===== -->

# Variant Binning

BioBin-style rare-variant aggregation report.

## Purpose

Given a multi-sample VCF (and optional phenotype file), this report:

1. computes internal MAF from VCF genotypes,
2. selects rare variants by `maf_cutoff`,
3. maps variants to genes by genomic overlap (`entity_locations`),
4. expands to bins by `group_by`, and
5. writes output artifacts to `output_dir`.

## Supported `group_by`

- `gene`
- `gene_group`
- `locus_type`
- `pathway`

## Required Params

- `vcf_path`: path to cohort VCF (`.vcf`, `.vcf.gz`, `.vcf.bgz`)
- `output_dir`: directory where CSV/JSON artifacts are written

## Optional Params

- `phenotype_path`: CSV/TSV with sample phenotype labels
- `phenotype_sample_column` (default `SampleID`)
- `phenotype_value_column` (default `Phenotype`)
- `phenotype_control_value` (default `0`)
- `phenotype_case_values` (optional list)
- `group_by` (default `gene`)
- `maf_cutoff` (default `0.01`)
- `rare_case_control` (default `true`)
- `overall_major_allele` (default `true`)
- `build` (default `38`)
- `max_variants` (optional)
- `include_zero_counts` (default `true`)

## Artifacts

- `bin_counts.csv`
- `variant_to_bin.csv`
- `bin_definitions.csv`
- `bin_member_counts.csv`
- `sample_bin_long.csv`
- `summary.json`

The report return value is a 1-row DataFrame summary containing counts and artifact paths.



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_variant_list_intersect.md ===== -->

# Report: `variant_list_intersect`

## Purpose

Intersects a biologically annotated variant list (**Lista A**, from `gene_to_variant_filtering`) with a genotyped variant list (**Lista B**, from a VCF or PLINK dataset) to produce **Lista C** — variants that are biologically relevant AND present in the genotype data.

This is **Phase 2.5** of the SNP×SNP pipeline, sitting between variant annotation (Phase 2) and LD Pruning (external).

---

## Pipeline context

```
[Phase 2]  gene_to_variant_filtering
               → Lista A: biologically annotated variants (CSV)

[Phase 2.5] variant_list_intersect        ← this report
               Lista A ∩ Lista B = Lista C
               + writes lista_C.txt for PLINK --extract

[External]  PLINK LD Pruning on Lista C only
               plink --bfile dataset \
                     --extract lista_C.txt \
                     --indep-pairwise 50 5 0.2 \
                     --out lista_D
               → lista_D.prune.in

[Phase 3]   snp_snp_pair_generator (future)
               Lista D × Lista D → interaction pairs
```

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `variant_list_a` | str (path) | required | CSV/TSV file from `gene_to_variant_filtering` (or any annotated variant list) |
| `a_id_col` | str \| None | `None` | Column name in Lista A to use as variant ID. If `None`, uses the **first column** |
| `variant_list_b` | str (path) | required | Genotype file: `.bim`, `.vcf`, `.vcf.gz`, `.txt`, `.list`, `.snplist`, `.csv`, `.tsv` |
| `b_id_col` | str \| None | `None` | Column name in Lista B (only for `.csv`/`.tsv`). If `None`, uses the **first column** |
| `match_by` | str | `"auto"` | Match strategy: `"rsid"`, `"chr_pos"`, or `"auto"` |
| `plink_extract_path` | str \| None | `None` | If set, writes Lista C to this path in PLINK `--extract` format (one ID per line) |

---

## Match strategies

### `match_by = "auto"` (recommended)

1. Checks if Lista A has rsID values AND Lista B has rsID values → enables rsID matching
2. Checks if Lista A has chr/pos columns AND Lista B has chr/pos → enables chr:pos matching
3. For each variant in Lista A: tries rsID first; falls back to chr:pos if rsID fails
4. Records which method matched via `match_status`

### `match_by = "rsid"`

Only matches by rsID. Variants without rsID or where rsID is absent from Lista B will be `only_in_a`.

### `match_by = "chr_pos"`

Only matches by chromosome + position. Ignores rsID entirely. Useful when Lista B has no rsIDs (e.g., imputed variants).

---

## Supported Lista B file formats

| Extension | Format | rsID source | chr:pos source |
|---|---|---|---|
| `.bim` | PLINK binary map | column 2 (SNP) | column 1 (CHR) + column 4 (BP) |
| `.vcf` | Variant Call Format | column 3 (ID) | column 1 (CHROM) + column 2 (POS) |
| `.vcf.gz` | Gzipped VCF | column 3 (ID) | column 1 (CHROM) + column 2 (POS) |
| `.txt` / `.list` / `.snplist` | One ID per line | if line matches `rs\d+` | if line matches `chr:pos` pattern |
| `.csv` / `.tsv` | Delimited file | `b_id_col` or first column | parsed from ID value |

### Supported chr:pos formats (auto-detected)

```
1:12345        chr1:12345      Chr1:12345
1_12345        chr1_12345
1-12345        chr1-12345
1 12345        chr1 12345
```

---

## Output DataFrame

Every row in Lista A appears in the output. Columns:

| Column | Description |
|---|---|
| `variant_a_id` | Variant ID from Lista A (primary key column) |
| `variant_b_id` | Matched variant ID from Lista B (`None` if not found) |
| `match_status` | One of: `matched_rsid`, `matched_chr_pos`, `only_in_a` |
| `plink_id` | PLINK-ready ID for `--extract`: rsID if matched by rsID; `CHR:POS` if matched by position; `None` if not matched |
| *(all original Lista A columns)* | All annotation columns from `gene_to_variant_filtering` are preserved |

### `match_status` values

| Value | Meaning |
|---|---|
| `matched_rsid` | Found in Lista B by rsID match |
| `matched_chr_pos` | Found in Lista B by chr:pos match (rsID match was not possible or failed) |
| `only_in_a` | Not found in Lista B — variant has no genotype data in this dataset |

---

## PLINK extract file (Lista C)

When `plink_extract_path` is set, the report writes a plain text file containing one `plink_id` per line for all matched variants:

- Matched by rsID → line is the rsID (e.g., `rs429358`)
- Matched by chr:pos → line is `CHR:POS` (e.g., `19:44908684`)

This file is ready for direct use with PLINK:

```bash
plink --bfile my_dataset \
      --extract lista_C.txt \
      --indep-pairwise 50 5 0.2 \
      --out lista_D
```

If no variants matched (e.g., rsID vs chr:pos format mismatch), the file is written empty — no error is raised, but a warning is logged.

---

## API examples

### Basic usage (auto match)

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db")
bf.db.connect()

df = bf.report.run(
    "variant_list_intersect",
    variant_list_a="output/gene_to_variant_filtering.csv",
    variant_list_b="data/my_cohort.bim",
    plink_extract_path="output/lista_C.txt",
)

print(df["match_status"].value_counts())
# matched_rsid      12481
# matched_chr_pos     320
# only_in_a          2199

# Variants ready for LD pruning
lista_c = df[df["plink_id"].notna()]
print(f"Lista C: {len(lista_c):,} variants")
```

### Force chr:pos matching (no rsID in VCF)

```python
df = bf.report.run(
    "variant_list_intersect",
    variant_list_a="output/phase2.csv",
    variant_list_b="data/imputed_cohort.vcf.gz",
    match_by="chr_pos",
    plink_extract_path="output/lista_C.txt",
)
```

### Plain text Lista B (one ID per line)

```python
df = bf.report.run(
    "variant_list_intersect",
    variant_list_a="output/phase2.csv",
    variant_list_b="data/variant_ids.txt",
)
```

### Custom column names

```python
df = bf.report.run(
    "variant_list_intersect",
    variant_list_a="my_variants.csv",
    a_id_col="snp_id",               # column in my_variants.csv
    variant_list_b="genotyped.tsv",
    b_id_col="marker_name",          # column in genotyped.tsv
)
```

---

## CLI examples

```bash
# Basic intersection with PLINK .bim
biofilter report run \
  --report-name variant_list_intersect \
  --param variant_list_a=output/phase2.csv \
  --param variant_list_b=data/cohort.bim \
  --param plink_extract_path=output/lista_C.txt \
  --output output/lista_C_annotated.csv

# Force rsID-only matching
biofilter report run \
  --report-name variant_list_intersect \
  --param variant_list_a=output/phase2.csv \
  --param variant_list_b=data/cohort.bim \
  --param match_by=rsid \
  --output output/result.csv
```

---

## Edge cases

### Lista A has rsIDs but Lista B only has chr:pos

`match_by="auto"` will detect that Lista B has no rsIDs and fall back to chr:pos matching automatically. If Lista A also lacks chr/pos columns, all rows will be `only_in_a` and the extract file will be empty.

### Chromosome encoding differences

The report normalises chromosomes internally:
- PLINK: `"1"–"22"`, `"X"`, `"Y"`, `"MT"` → biofilter integers 1-25
- VCF: `"chr1"–"chr22"`, `"chrX"`, `"chrY"`, `"chrM"` → biofilter integers 1-25
- The PLINK extract file always uses PLINK-style chromosome notation

### Missing rsID in .bim (e.g., `.`)

PLINK often writes `.` for variants with no rsID. The report treats `.` as non-rsID and will only match those variants by chr:pos.

---

## Expected scale

| Scenario | Lista A | Lista B | Lista C | Runtime |
|---|---|---|---|---|
| Single gene | ~300 | 500k | ~250 | < 1s |
| Pathway (~50 genes) | ~5k | 500k | ~4k | < 2s |
| Full pipeline (~8k genes) | ~15k | 500k | ~12k | < 5s |
| Large WGS cohort | ~15k | 10M | ~12k | < 30s |



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_variant_modeling.md ===== -->

# Report: `variant_modeling`

## Purpose

Maps an input list of genomic variants to biologically connected variant pairs,
where **both variants in every pair come from the input list**.

The workflow mirrors the diagram below:

```
Input variants (rsID, chr:pos, or chr:pos:ref:alt)
    ↓  DB lookup + window_bp
Genes overlapping input variants
    ↓  group membership (Pathway, GO, Disease, …)
Groups
    ↓  co-membership → Gene×Gene pairs
Gene×Gene pairs  [weight = # shared groups]
    ↓  cartesian of input variants per gene
Variant×Variant pairs  ← output
```

`group_support_count` is the biological weight of each pair: how many distinct
groups (pathways, GO terms, diseases, …) have both genes as members.

---

## Use case

Input comes from a pre-genotyped file (VCF, PLINK `.bim`, curated list). Because
all variants are already sequenced in the cohort, pairs are restricted to input
variants only — there is no expansion to new DB variants.

This is the **correct design** for interaction studies where you want to test
epistasis between variants you already have genotyped.

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_data` | list \| path | required | rsID, chr:pos, or chr:pos:ref:alt variants; file path (one per line) also accepted |
| `build` | int | `38` | Genome build for gene overlap queries |
| `window_bp` | int | `0` | Extend gene boundaries by N bp when assigning variants to genes |
| `group_entity_groups` | list \| str | `"Pathway"` | Which group types define biological connections (Pathway, GO, Disease, …) |
| `group_data_sources` | list \| str | all | Restrict to specific data sources (Reactome, KEGG, GO, …) |
| `gene_entity_groups` | list \| str | `"Gene"` | Entity group label for genes in the DB |
| `relationship_types` | list \| str | all | Restrict to specific relationship type codes |
| `max_pairs` | int | `1_000_000` | Safety cap — aborts before materialising if estimate exceeds this |

---

## Input format

Each item in `input_data` can be:

| Format | Example | Resolution |
|---|---|---|
| rsID | `rs429358` | Exact rsID match in `variant_masters` |
| chr:pos | `chr19:44908684` | Position overlap (returns **all** alleles at the position; SNVs only) |
| bare chr:pos | `19:44908684` | Same as `chr:pos` |
| chr:pos:ref:alt | `chr19:44908684:T:C` | **Exact** ref/alt match (SNV or indel; no allele_type filter) |
| bare chr:pos:ref:alt | `19:44908684:T:C` | Same as `chr:pos:ref:alt` |
| file path | `./variants.txt` | Mixed formats supported; one entry per line |

Use `chr:pos:ref:alt` for credible-set / fine-mapping inputs to avoid multiallelic ambiguity.
REF/ALT are case-insensitive (normalised to uppercase before the lookup) and must be in `[ACGT]+`.

---

## Output columns

| Column | Description |
|---|---|
| `variant_1_input` / `variant_2_input` | Original input string(s) the user supplied for this variant — comma-joined when more than one input resolved to the same `variant_id`. Use this as a **join key** to merge results back into the caller's source table (credible set, GWAS hits, etc.) |
| `variant_1_id` / `variant_2_id` | Internal DB IDs |
| `variant_1_rsid` / `variant_2_rsid` | rsIDs (if available) |
| `variant_1_chr` / `variant_2_chr` | Chromosome (`chrN` format) |
| `variant_1_pos` / `variant_2_pos` | Position start |
| `variant_1_ref` / `variant_2_ref` | Reference allele from `variant_masters` |
| `variant_1_alt` / `variant_2_alt` | Alternate allele from `variant_masters` |
| `gene_1_name` / `gene_2_name` | Gene symbols |
| `gene_1_id` / `gene_2_id` | Internal gene entity IDs |
| `group_support_count` | **Weight** — # groups linking gene_1 to gene_2 |
| `group_support_names` | Pipe-separated group names |
| `data_source_support_count` | # data sources |
| `data_source_support_names` | Pipe-separated source names |
| `build` | Genome build used |
| `window_bp` | Window applied to gene boundaries |

Output is sorted by `group_support_count DESC`, then gene names.

### Merging results back to the caller's table

Because `variant_*_input` carries the exact string the user supplied, joins back to the source
table are straightforward — no need to reparse `chr:pos:ref:alt` or look up rsID-to-coords:

```python
# credible-set TSV has columns: locus, trait, SNP   (SNP in chr:pos:ref:alt form)
merged = cs.merge(
    pairs_df,
    left_on="SNP",
    right_on="variant_1_input",
    how="inner",
)
```

For chr:pos inputs that hit multiallelic positions (one input → N variants), `variant_*_input`
will repeat the same string across all matching rows. The `variant_*_ref`/`variant_*_alt`
columns disambiguate which allele each row refers to.

---

## Safety check

Before materialising pairs the report **estimates** the total count:

```
estimated = sum(len(variants_gene_1) × len(variants_gene_2) for each gene pair)
```

If `estimated > max_pairs` the report returns a single-row error DataFrame:

```python
{
    "resolution_status": "pair_limit_exceeded",
    "estimated_pairs":   3_200_000,
    "max_pairs":         1_000_000,
    "suggestion":        "..."
}
```

To resolve: apply stricter `group_entity_groups` / `group_data_sources` filters,
or increase `max_pairs`.

---

## API examples

### Basic — Pathway connections

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db")
bf.db.connect()

df = bf.report.run(
    "variant_modeling",
    input_data        = ["rs429358", "rs7412", "chr2:21044574", "chr4:186486470"],
    build             = 38,
    window_bp         = 0,
    group_entity_groups = ["Pathway"],
    group_data_sources  = ["Reactome"],
)

print(f"Pairs: {len(df):,}")
df.sort_values("group_support_count", ascending=False).head(20)
```

### From file

```python
df = bf.report.run(
    "variant_modeling",
    input_data          = "./my_variants.txt",   # one rsID, chr:pos, or chr:pos:ref:alt per line
    group_entity_groups = ["Pathway", "GO"],
)
```

### Multiple group types

```python
df = bf.report.run(
    "variant_modeling",
    input_data          = ["rs429358", "rs7412"],
    group_entity_groups = ["Pathway", "Disease", "GO"],
    window_bp           = 5000,
)
```

---

## CLI examples

```bash
# Basic
biofilter report run \
  --report-name variant_modeling \
  --input rs429358 --input rs7412 --input chr2:21044574 \
  --param build=38 \
  --param group_entity_groups=Pathway \
  --output variant_pairs.csv

# From file
biofilter report run \
  --report-name variant_modeling \
  --input-file ./variants.txt \
  --param group_entity_groups=Pathway \
  --param group_data_sources=Reactome \
  --output variant_pairs.csv

# Inspect params template
biofilter report run --report-name variant_modeling --params-template
```

---

## Difference from `snp_snp_model` (legacy)

| | `variant_modeling` | `snp_snp_model` (legacy) |
|---|---|---|
| Input | rsID, chr:pos, or chr:pos:ref:alt | chr:pos only |
| Variant source | Input list only | Expands from gene loci (up to 2000/gene) |
| Both variants from input? | **Always** | Optional (scope parameter) |
| group_support_count | Yes — built-in weight | No |
| Design intent | Genotyped cohort study | Discovery / annotation |

---

## Expected scale

| Scenario | Input variants | Gene pairs | Output pairs | Runtime |
|---|---|---|---|---|
| 4 APOE-region variants | ~4 | ~10 | ~60 | < 1s |
| 50 curated disease variants | ~50 | ~200 | ~5k | < 5s |
| 500 pathway variants | ~500 | ~1k | ~100k | < 30s |
| 5000 variants, no filter | ~5000 | ~50k | **safety abort** | — |



<!-- ===== SOURCE FILE: biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md ===== -->

# Report Tutorial: `variant_single_gene_annotation`

## Purpose

Phase 1 of the single-variant SNP×SNP interaction pipeline.

Given one input variant (chr:pos or rsID), this report:

1. Resolves the variant to a genomic position (via `variant_masters` when an rsID is supplied).
2. Finds the **seed gene** at that position using `entity_locations` (with an optional base-pair window).
3. Expands through a configurable biological group type (Pathways, Diseases, GO, or direct Gene links) to collect **partner genes**.
4. Enriches every partner gene with genomic coordinates, locus group, functional gene groups, and a variant count estimate.

Output: one row per **(seed gene × partner gene)** pair with shared-group information. Resolution failures return a single diagnostic row with a non-null `resolution_status` field so the caller always receives a usable DataFrame.

## Report Name

`variant_single_gene_annotation`

## Parameters (API)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_variant` | `str` | **required** | Variant to query. Accepts `chr:pos` (e.g. `chr19:44904604`, `19:44904604`) or rsID (e.g. `rs429358`). Separators `:`, `;`, `,`, `-`, and space are all accepted for `chr:pos`. |
| `build` | `int` | `38` | Genome assembly build used to look up `entity_locations`. |
| `window_bp` | `int` | `0` | Base-pair window around the position for gene lookup. Only applies to `chr:pos` input. When multiple genes fall inside the window, the **closest** one is selected (distance = 0 if position is inside the gene body; ties broken by smallest locus span). |
| `group_entity_type` | `str` | `"Pathways"` | `EntityGroup` name used for the expansion step. Controls how partner genes are discovered. Use `"Genes"` for direct gene-gene links (1-hop); use `"Pathways"`, `"Diseases"`, `"GO"`, etc. for 2-hop expansion through an intermediary entity. |
| `source_system_filter` | `list[str]` or `str` or `None` | `None` | Restrict which `entity_relationships` are considered by `ETLSourceSystem` name. Accepts a list (`["Reactome", "KEGG"]`) or a single string. When `None`, all sources are included. |

## Input Formats

| Format | Examples |
|---|---|
| Chromosome + position | `chr19:44904604`, `19:44904604`, `chr19-44904604`, `chr19 44904604` |
| rsID | `rs429358`, `RS429358` (case-insensitive) |

Chromosome aliases: `X` → 23, `Y` → 24, `M` / `MT` → 25.

## Output Columns

| Column | Description |
|---|---|
| `resolution_status` | `None` on success; an error code on failure (see below). |
| `seed_input` | The raw input string as provided. |
| `seed_rsid` | Resolved rsID (populated when input was an rsID). |
| `seed_chromosome` | Chromosome of the input variant (integer). |
| `seed_position` | Position of the input variant. |
| `seed_allele_count` | Number of alleles found in `variant_masters` for this rsID position (rsID input only). |
| `group_entity_type` | The `group_entity_type` parameter value used for this run. |
| `seed_gene_entity_id` | Internal entity ID of the seed gene. |
| `seed_gene_symbol` | HGNC symbol of the seed gene. |
| `seed_gene_chromosome` | Chromosome of the seed gene. |
| `seed_gene_start` | Seed gene start position. |
| `seed_gene_end` | Seed gene end position. |
| `seed_gene_locus_group` | Locus group of the seed gene (e.g. `protein-coding gene`). |
| `seed_gene_groups` | Pipe-separated list of functional gene groups the seed gene belongs to. |
| `seed_gene_total_groups` | Total number of shared groups between seed and all partners (summary). |
| `partner_gene_entity_id` | Internal entity ID of the partner gene. |
| `partner_gene_symbol` | HGNC symbol of the partner gene. |
| `partner_gene_chromosome` | Chromosome of the partner gene. |
| `partner_gene_start` | Partner gene start position. |
| `partner_gene_end` | Partner gene end position. |
| `partner_gene_locus_group` | Locus group of the partner gene. |
| `partner_gene_groups` | Pipe-separated list of functional gene groups of the partner gene. |
| `seed_gene_variant_count` | Approximate number of variants in `variant_masters` overlapping the seed gene locus. |
| `partner_gene_variant_count` | Approximate number of variants in `variant_masters` overlapping the partner gene locus. |
| `shared_group_count` | Number of groups (pathways, diseases, etc.) shared between seed and this partner gene. |
| `shared_group_ids` | Pipe-separated internal entity IDs of the shared groups. |
| `shared_group_names` | Pipe-separated names/descriptions of the shared groups. |
| `shared_group_sources` | Pipe-separated data source names for the shared groups. |

### Resolution Status Codes

| Code | Meaning |
|---|---|
| `(None)` | Success. |
| `invalid_input_format` | The `input_variant` string could not be parsed as chr:pos or rsID. |
| `rsid_not_found` | The rsID was not found in `variant_masters`. |
| `configuration_error` | The `EntityGroup` named `"Genes"` is missing from the database. |
| `group_not_found:<name>` | The requested `group_entity_type` was not found. The error message includes the available groups. |
| `gene_not_found` | No gene was found at the resolved position (with the given window and build). |
| `no_partners_found` | A seed gene was found but has no partner genes via the requested group type. |

## Examples

### API

```python
import biofilter as bf

# Positional input — APOE locus, expand via Pathways
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    build=38,
    window_bp=0,
    group_entity_type="Pathways",
)

# rsID input — same variant by rsID, Reactome only
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",
    build=38,
    group_entity_type="Pathways",
    source_system_filter=["Reactome"],
)

# Direct gene-gene links (1-hop), no source filter
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    group_entity_type="Genes",
)

# With a base-pair window — pick the closest gene within 10 kb
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="19:44904604",
    window_bp=10000,
    group_entity_type="Diseases",
)
```

### CLI

```bash
# Minimal — positional input
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=chr19:44904604

# rsID with Reactome filter
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=rs429358 \
  --param group_entity_type=Pathways \
  --param source_system_filter=Reactome

# Window + Diseases
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=chr7:117548628 \
  --param window_bp=5000 \
  --param group_entity_type=Diseases
```

## Pipeline Context

This report is **Phase 1** of the single-variant SNP×SNP interaction pipeline:

```
Phase 1 — Gene Discovery (this report)
  input: one variant
  output: seed gene + partner-gene list with shared-group annotation

Phase 2 — Filtered Variant Collection  (planned)
  input: Phase 1 partner-gene list
  output: variants per gene, pre-filtered to coding/functional consequences

Phase 3 — Pair Generation  (planned)
  input: Phase 2 variant sets per gene
  output: variant × variant interaction pairs (seed × partner)
```

Separating gene discovery (tractable ~8 k rows) from variant enumeration prevents the combinatorial explosion that occurs when annotating all variants before filtering.

## Demo Tips

- Start with `chr19:44904604` (APOE rs429358 locus) — well-annotated gene with many Reactome pathways.
- Use `source_system_filter=["Reactome"]` to limit output to a manageable size during demos.
- Check `resolution_status` first; a non-null value explains exactly why the report returned no gene rows.
- `shared_group_count` is the primary signal for SNP×SNP prioritization in later pipeline phases.

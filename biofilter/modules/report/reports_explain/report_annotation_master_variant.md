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

Accepts rsID or chr:pos formats, mixed in the same list or from a file:

| Format | Example |
|---|---|
| rsID | `rs429358` |
| chr:pos | `chr19:44908684` |
| bare chr:pos | `19:44908684` |
| file path | `./variants.txt` (one per line) |

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_data` | list \| path | required | rsID or chr:pos list; file path (one per line) also accepted |
| `most_severe_only` | bool | `False` | Keep only the most-severe transcript annotation per variant |
| `canonical_only` | bool | `False` | Keep only canonical transcript annotations |

Both filters can be combined. If a filter produces no rows for a variant, the full set is returned.

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

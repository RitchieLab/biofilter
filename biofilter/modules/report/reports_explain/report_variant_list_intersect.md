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

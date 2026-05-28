# BioBin — Technical Reference

**Date:** 2026-05-13
**Author:** Andre Rico (notes from Phase 5 replication)
**Purpose:** Comprehensive technical documentation of `biobin` as used in the Hui et al. 2023 hearing-loss-genomics replication pipeline. Written specifically to inform replication of biobin functionality in **Biofilter 4 (BF4)** with integrated LOKI.

> All claims in this doc are grounded in either: (a) `biobin --help` output, (b) `biobin --sample-config` defaults, (c) the actual `run_log.txt` from our Phase 5 invocation, or (d) inspection of our output files. Where I'm inferring (e.g., specific internal logic), I flag it as "(inferred)". The user (Andre) should treat anything flagged as "inferred" as worth verifying against biobin source code before implementing in BF4.

---

## 1. What biobin is, in one sentence

biobin aggregates **rare variants** in a VCF into **gene-level bins**, computes a **per-individual burden score** per bin, and runs a **statistical test** (logistic regression by default) of burden vs phenotype, adjusted for covariates — producing a per-bin p-value.

## 2. Conceptual model

```
   VCF (variants × samples)                  Phenotype (cases/controls)
              │                                          │
              ▼                                          ▼
       ┌───────────────────────────────────────────────────────┐
       │  1. Load variants from VCF                            │
       │  2. Filter to "rare" (MAF < cutoff)                   │
       │  3. Map each variant to ≥1 gene/bin (via LOKI or      │
       │     --region-file)                                    │
       │  4. Compute burden score per individual per bin       │
       │     (sum of alt allele dosages; "additive" by default)│
       │  5. For each bin: run logistic regression             │
       │     phenotype ~ burden + covariates                   │
       │     Output beta, SE, p-value                          │
       └───────────────────────────────────────────────────────┘
              │
              ▼
   bins.csv (matrix [variant×individual + summary stats + p-values])
   locus.csv (per-variant → bin(s) mapping)
   run log
```

The key conceptual difference vs single-variant GWAS: **a single rare variant has zero statistical power**. By aggregating many rare variants in the same gene, biobin gets enough cumulative signal to detect an effect at the gene level.

## 3. Architecture & dependencies

biobin is a **C++ program** linked against:

| Library | Purpose |
|---|---|
| `libsqlite3` | LOKI database (gene/region/source metadata) |
| `libboost_*` (regex, filesystem, program_options, iostreams, thread, system) | C++ utilities |
| `libgsl` + `libgslcblas` (GSL — GNU Scientific Library) | Statistics (logistic regression, p-value computation) |
| `libz`, `libbz2`, `liblzma` | Compressed VCF reading |

Practical note for BF4: GSL is the workhorse for the regression. If BF4 uses Python, the equivalent stack is `scipy.optimize` + `scipy.stats` (or `statsmodels` for logit). If BF4 stays in C++, GSL is a natural choice.

## 4. Inputs

### 4.1 VCF file (`-V` / `--vcf-file`)

Standard VCF with per-sample genotypes. From our Phase 5 run, the cohort-wide VCF was 1.5 GB (9,576 variants × 41,748 samples). biobin reads gzipped VCF too.

**Variant requirements (inferred from biobin behavior):**
- Must have GT field (genotype) for each sample
- Multi-allelic sites get expanded into one biobin "locus" per alt allele
- "Star alleles" (`*`): biobin can treat as referent (default `set-star-referent=Y`) or as missing
- Missing genotypes (`./.`): handled per sample; the sample doesn't contribute to that variant's burden but is kept

### 4.2 Phenotype file (`-p` / `--phenotype-filename`)

Tab-separated. From our Phase 5: [`cases_control.txt`](../../analysis/daniel/outputs/phase4/cases_control.txt) — header `PMBB_ID\tSNHL`, then `PMBB_ID\tvalue` rows where value ∈ {0=control, 1=case, NA=missing}.

**Configurable:**
- `--phenotype-control-value` (default `0`): which value counts as control. Cases = anything else (except NA).
- `--drop-missing-phenotype-samples` (default `Y`): drop samples with no phenotype value (NA)
- `--force-all-control` (default `N`): treat all samples as controls (for testing/null distributions)

**Multi-phenotype support:** the file can have multiple phenotype columns — biobin runs the test independently per phenotype (PheWAS-style). With just one column (as in our case), it's single-phenotype.

**Sample alignment behavior (from our run_log):**
- 20,188 samples from phenotype file were NOT in the VCF — those get silently ignored
- biobin only tests samples that appear in BOTH the VCF AND the phenotype file

### 4.3 Covariates file (`--covariates`)

Tab-separated. From our Phase 5: [`covs.txt`](../../analysis/daniel/outputs/phase4/covs.txt) — `PMBB_ID\tSex\tAge\tAgeSq\tPC1\tPC2\tPC3\tPC4`.

biobin's behavior with covariates (from our run_log):
```
WARNING: No header given for multiple traits in covs.txt, assigning sequential names
```
So **biobin doesn't actually parse the header** — it treats every column after the ID as a numeric covariate, regardless of name. The header is informational only.

**For BF4:** trust the column ORDER, not the column names. PMBB_ID first, then numeric covariates.

**Sample alignment behavior:** samples in covs but not in VCF → silently ignored (1,983 in our run).

### 4.4 Region file (`--region-file`)

Tab-separated, columns: `chr\tgene_name\tstart\tstop`. From our Phase 5: [`gene_list_regions.txt`](../../analysis/daniel/outputs/phase4/gene_list_regions.txt) — 176 known HL genes.

**Use case:** when you want biobin to bin variants by YOUR custom gene definitions rather than LOKI's default gene boundaries. We used this for the HL-gene-restricted burden test.

**Important behavior:** when `--region-file` is provided, biobin **still consults LOKI** for additional annotation (the `Gene(s)` column in bins.csv comes from LOKI). LOKI may map variants to additional genes that overlap your custom regions. We observed this as the LOC* artifacts in our top hits — variants in the same physical region got assigned to both the custom gene (ESPN) and LOKI-defined LOCs.

### 4.5 LOKI database (`-D` / `--settings-db`)

The Ritchie Lab's SQLite-backed knowledge integration database. biobin queries it for:
- Gene names + boundaries (when `--region-file` not used, or as supplemental annotation)
- Pathways/groups (when `--bin-pathways=Y`)
- Source metadata (which annotation sources are loaded)

**Sources loaded in `loki-20230816.db` (the one we used):**
```
biogrid, chainfiles, dbsnp, disgenet, entrez, gaad, go, gwas, kegg, mint,
netpath, oreganno, pfam, pharmgkb, reactome, ucsc_ecr
```

biobin excludes some by default (`--exclude-sources dbsnp,oreganno,ucsc_ecr`) — these are noisy or non-gene sources.

**For BF4 (integrated LOKI in Biofilter 4):** the LOKI schema is the contract. biobin doesn't care what's "in" loki as long as the SQLite queries return rows in the expected shape. If BF4 replaces SQLite with another store, the query layer just needs to be ABI-compatible.

## 5. Internal pipeline (what biobin does, step by step)

Based on the run_log and config options observed:

### Step A — Initialization
- Open SQLite LOKI database
- Parse command-line options + config file
- Print warnings about settings/source incompatibilities

### Step B — VCF parsing
- Stream the VCF file line-by-line
- For each variant:
  - Decompose multi-allelics into separate "loci"
  - Compute MAF in the cohort
  - Drop monomorphic if `--keep-monomorphic=N` (default)
  - Drop variants with MAF > `--maf-cutoff` (default 0.05) — i.e., keep only rare
  - Drop variants with MAF ≤ `--maf-threshold` (default 0, so kept by default)
  - With `--rare-case-control=Y` (default), the "rare" check is applied separately to cases AND controls — a variant must be rare in BOTH to be included

### Step C — Phenotype assignment
- For each VCF sample, look up in phenotype file
- Sample with no phenotype value: dropped if `--drop-missing-phenotype-samples=Y`
- Sample with control value (default `0`): control
- Sample with any other value: case
- Sample missing from VCF entirely: ignored

### Step D — Bin construction
- For each "region" (gene), determine the set of variants that overlap
  - With `--region-file`: use the custom regions
  - Otherwise: use LOKI's gene boundaries
- Apply `--bin-minimum-size` (default 5): bins with < 5 variants are dropped
- Apply `--bin-expand-size` (default 50): bins with > 50 variants are split into child bins (by transcript/exon, inferred)
- With `--bin-expand-roles=Y`: split bins by exon vs intron (we used default `N` — no splitting)
- With `--bin-interregion=Y` (default): also create bins for intergenic regions of `--interregion-bin-length` kb (default 50 kb sliding windows)

**Multiple bins per variant (observed in our locus.csv):**
> A single variant typically gets assigned to **multiple bins** because:
> 1. It can overlap multiple gene transcripts
> 2. With LOKI annotation, it can match multiple gene records (real gene + LOCs in same region)
> 3. The custom region file can have overlapping genes
>
> In our Phase 5: variant `1_6425014_C_A` got assigned to **14 bins** (ESPN twice, then 12 LOC*'s).
> Bins per locus distribution (from run_log):
> - 2 bins: 4,152 loci
> - 3 bins: 1,358 loci
> - 4 bins: 977 loci
> - 5-6 bins: 1,146 loci
> - 7-12 bins: 941 loci
> - 13-51 bins: 270 loci

### Step E — Burden score computation
For each (individual, bin) pair:
- `--disease-model=additive` (default): burden = sum of alt allele dosages across all variants in the bin (per individual)
  - 0/0 → 0, 0/1 → 1, 1/1 → 2
- `--disease-model=dominant`: burden = 1 if any variant carrier, else 0
- `--disease-model=recessive`: burden = 1 if any homozygous alt, else 0

**Weighting:** `--weight-loci=N` (default) — no weighting. With `Y`, can use `--weight-model={minimum,maximum,control,overall}` to up-weight rare variants. We used default (no weighting).

### Step F — Statistical test per bin

This is the key for BF4 replication.

`--test logistic` (what Daniel and we used): for each bin, run:

$$\logit\bigl(P(\text{case}_i)\bigr) = \beta_0 + \beta_1 \cdot \text{burden}_i + \sum_j \gamma_j \cdot \text{cov}_{i,j}$$

- Where `case_i` ∈ {0, 1} is the phenotype, `burden_i` is the per-individual burden score for the bin, and `cov_{i,j}` are the covariate columns
- biobin reports the regression's **p-value for β₁** (the burden coefficient) and its standard error (logged as "logistic error margin")
- Implementation: GSL's `gsl_multifit_fdfsolver` or similar — inferred from the GSL dependency

**Other available tests** (`--test` accepts):
- `linear` — linear regression (for continuous phenotypes, e.g., degree-of-HL on 0-4 scale; Daniel uses this in walkthrough Phase 17)
- `wilcoxon` — Wilcoxon rank-sum (non-parametric, no covariates)
- `SKAT-linear`, `SKAT-logistic` — SKAT (Sequence Kernel Association Test) — different statistical framework, uses the SKAT-specific config flags (`--skat-matrix-threshold`, `--skat-eigen-threshold`, `--skat-pvalue-accuracy`)

**Constraint:** `--min-control-frac` (default 0.125) — if controls are < 12.5% of the analyzed cohort (after filtering), biobin warns that allele frequencies may be unreliable. Doesn't abort, just warns. We saw this warning in our Phase 5 run_log (1,098 cases / 39,981 controls = 2.6% cases, but we have plenty of controls, so the warning was about ESRRB-specific bins where many controls fell out).

### Step G — Reporting
Outputs:
- `<prefix>-bins.csv`: the main result (see Section 6)
- `<prefix>-locus.csv`: per-variant info (see Section 6)
- stdout/stderr: warnings and final summary

Configurable with `--report-bins`, `--report-loci`, `--transpose-bins`, `--no-summary`, `-d/--output-delimiter` (default `,`).

## 6. Output structure

### 6.1 bins.csv — the main matrix

**Layout** (rows × columns):

| Row index | Row label | Cells |
|---|---|---|
| 0 | `ID` | column headers — first cell `ID`, then empty, then gene name per bin (e.g., `ESPN`, `ESPN`, `LOC127267194`, ...) |
| 1 | `Total Variants` | total number of variants in each bin |
| 2 | `Total Loci` | total unique loci (= unique chr:pos, may differ from Total Variants if multi-allelic) |
| 3 | `Control Variant Totals` | sum of alt allele counts in controls per bin |
| 4 | `Case Variant Totals` | sum of alt allele counts in cases per bin |
| 5 | `Control Bin Capacity` | max possible alt allele count in controls (= 2 × #controls × #variants in bin) |
| 6 | `Case Bin Capacity` | max possible alt allele count in cases |
| 7 | `Gene(s)` | LOKI-derived gene annotation (can be empty if LOKI has no record) |
| 8 | `logistic p-value` | **the headline result** — p-value from logistic regression of `case ~ burden + covariates` per bin |
| 9 | `logistic error margin` | standard error of β₁ |
| 10+ | `PMBB<id>` | per-individual rows — first cell is sample ID, then burden score per bin |

**For BF4:** the only rows that matter for downstream analysis are 0 (gene names) and 8 (p-values). Row 7 ("Gene(s)") is LOKI-version-dependent and unreliable.

**File size:** our Phase 5 bins.csv was 72 MB (969 bins × ~40k individuals + metadata).

### 6.2 locus.csv — per-variant mapping

Columns: `Chromosome,Location,ID,Gene(s),Bin Name(s)`

Each row is one variant; the `Gene(s)` and `Bin Name(s)` columns are `|`-separated lists of bins this variant contributes to. From our Phase 5:

```
1,6425014,1_6425014_C_A,ESPN|ESPN|LOC127267194|LOC127267195|...,ESPN|ESPN|LOC127267194|LOC127267195|...
```

8,849 loci in our run (after MAF filtering — we put 9,576 in, biobin dropped 727 as too common/monomorphic).

**For BF4:** locus.csv is the documentation of "where did each variant land". Use for debugging or for downstream analyses that need to trace from variant → bin.

### 6.3 run log

Plain text. Useful messages:
- `WARNING: Could not find N samples from X in VCF file.` — soft alignment failure
- `WARNING: All phenotypes are completely missing for sample X. Removing the sample.` — sample dropped
- `WARNING: In phenotype 'P', number of cases is less than 12.5% of the data. Allele frequencies for cases may be unreliable.` — low case count warning
- Final summary:
  ```
  Phenotype: 
     Loci:           8849
   * Rare Loci:      8849
     Total Bins:          967
     * Rare variants are those whose minor allele frequency is below 0.05 and above 0
  Number of Bins per locus
  2  4152
  3  1358
  ...
  ```

## 7. Configuration reference (Phase 5 settings)

From the `biobin -S` defaults vs what Daniel + we used:

| Flag | Default | Our use | Impact |
|---|---|---|---|
| `-D / --settings-db` | `knowledge.bio` | `loki-20230816.db` | Database path; required |
| `-V / --vcf-file` | — | merged VCF (Phase 5.3 output) | Input genotype matrix |
| `-p / --phenotype-filename` | — | `cases_control.txt` | Phenotype labels |
| `--covariates` | — | `covs.txt` | Covariate matrix |
| `--region-file` | — | `gene_list_regions.txt` | Custom 176 HL gene boundaries |
| `--bin-regions` | `Y` | `Y` (default) | Bin by gene regions (vs not) |
| `-G / --genomic-build` | — | `38` | hg38 |
| `--test` | (none) | `logistic` | Logistic regression on bins |
| `-F / --maf-cutoff` | `0.05` | `0.05` (default) | Rare variant cutoff INSIDE biobin (additional to plink's --max-maf .001 already applied) |
| `--maf-threshold` | `0` | `0` (default) | Minimum MAF; 0 = keep everything down to monomorphic |
| `--keep-monomorphic` | `N` | `N` (default) | Drop variants with no variation |
| `-m / --bin-minimum-size` | `5` | `5` (default) | Bins with <5 variants are dropped |
| `-e / --bin-expand-size` | `50` | `50` (default) | Bins with >50 variants split into child bins |
| `--disease-model` | `additive` | `additive` (default) | Genotype coding |
| `--rare-case-control` | `Y` | `Y` (default) | Rare check separately in cases vs controls |
| `--min-control-frac` | `0.125` | `0.125` (default) | Warn if controls < 12.5% |
| `--bin-pathways` | `N` | `N` (default) | Don't include pathway bins |
| `--bin-interregion` | `Y` | `Y` (default) | Include intergenic bins |
| `--exclude-sources` | `dbsnp,oreganno,ucsc_ecr` | (default) | LOKI sources to ignore |
| `--ambiguity` | `resolvable` | (default) | How to handle ambiguous loci |

**Important:** biobin applies its OWN `--maf-cutoff` (0.05) on top of any upstream MAF filtering. In our pipeline we applied `plink --max-maf .001` first (so input was already MAF<0.001), then biobin's MAF<0.05 was a no-op. **For BF4, BF4 should reproduce this layering** — even if upstream filter is tighter, BF4's internal MAF check might still drop some edge cases.

## 8. Concrete numbers from our Phase 5 run

| Quantity | Value |
|---|---|
| VCF input | 9,576 variants × 41,748 samples (1.5 GB) |
| Loci after biobin's MAF filter | 8,849 (dropped 727) |
| All loci flagged as "rare" | 8,849 (= 100%, because plink already filtered MAF<0.001) |
| Total bins generated | 967 |
| Number of bins per locus (distribution) | 2 (43% of loci), 3 (15%), 4 (11%), 5-6 (13%), 7-12 (11%), 13-51 (3%) |
| Wall time | 46.6 min (single-threaded) |
| Max memory | 177 MB |
| Output bins.csv size | 72 MB (969 cols × ~38k rows) |
| Output locus.csv size | 933 KB (8,849 rows) |

## 9. What BF4 needs to replicate — checklist

A minimum-viable BF4 implementation of biobin functionality needs:

### Core (must-have)
- [ ] Parse VCF (with multi-allelic decomposition, half-call handling, missing GT handling)
- [ ] Parse tab-separated phenotype file (PMBB_ID + 1+ phenotype columns, configurable control value)
- [ ] Parse tab-separated covariates file (PMBB_ID + 1+ numeric columns)
- [ ] Parse custom region file (chr/gene/start/stop) OR query LOKI for gene boundaries
- [ ] Cohort MAF computation (per-variant minor allele frequency)
- [ ] MAF filtering (max-maf cutoff, min-maf threshold, rare-case-control mode)
- [ ] Variant-to-bin mapping (one variant → multiple bins, by transcript overlap or gene record overlap)
- [ ] Bin-minimum-size filter (default 5)
- [ ] Bin-expand-size split (default 50)
- [ ] Burden score computation (additive, dominant, recessive disease models)
- [ ] Logistic regression: phenotype ~ burden + covariates → β, SE, p-value
- [ ] Output bins.csv in the documented format
- [ ] Output locus.csv with per-variant bin assignment

### Important (for parity with biobin)
- [ ] Linear regression test (`--test linear` for continuous phenotypes)
- [ ] Wilcoxon test (`--test wilcoxon`)
- [ ] SKAT-linear and SKAT-logistic
- [ ] Bin-interregion (intergenic bins, configurable kb)
- [ ] Bin-expand-roles (split bins by exon vs intron annotation)
- [ ] Bin-pathways (use LOKI groups instead of genes)
- [ ] Sample include/exclude filters
- [ ] Multi-phenotype (PheWAS-style — one regression per phenotype column)
- [ ] Weight-loci (with weight-model = minimum/maximum/control/overall)

### Nice-to-have
- [ ] Bin transpose (`--transpose-bins`)
- [ ] No-summary mode
- [ ] Configurable output delimiter
- [ ] LOKI population-specific gene boundaries (`-P / --population`)
- [ ] Region boundary extension (`-B`)

### Validation targets (from our Phase 5)
- [ ] Same set of bins as biobin (modulo LOKI version drift)
- [ ] Same logistic p-value for ESRRB: **8.6308e-05** with our covariates + phenotype + region file
- [ ] Same per-bin "Total Variants", "Control/Case Variant Totals", "Control/Case Bin Capacity"
- [ ] Same number of bins per locus distribution shape

If BF4 can match these on our Phase 5 inputs, the burden test functionality is correctly replicated.

## 10. Quirks + gotchas encountered

### 10.1 Multi-allelic decomposition expands counts
When a VCF site has multiple ALT alleles, biobin treats each as a separate "locus" but they're at the same chr:pos. This means `Total Variants` ≥ `Total Loci`.

### 10.2 Header in covariates file is ignored
biobin prints `WARNING: No header given for multiple traits` even when our covs.txt has a clear header. It just renames columns sequentially. Column ORDER matters; column NAMES are documentation only.

### 10.3 Genotype `./.` (missing) handling
Inferred: samples with missing GT for a specific variant don't contribute to that variant's burden, but they still contribute to OTHER variants in the bin. The burden score uses available genotypes only. (Worth verifying in BF4 implementation.)

### 10.4 The "Gene(s)" row in bins.csv is LOKI-dependent and unreliable
biobin populates row 7 (`Gene(s)`) by querying LOKI for what gene(s) the bin's variants fall in. When `--region-file` is also used, this row can be EMPTY for many bins because the custom region might not match any LOKI gene record. Use **row 0** (the ID/header row) for gene names — that comes directly from the region file.

### 10.5 LOKI version sensitivity
Different LOKI versions assign variants to different sets of bins (we saw 969 bins with loki-20230816 vs Daniel's 388 bins with loki-20220926). The biology is preserved (same p-values for real genes), but bin counts and rankings differ. **BF4 should document its LOKI version explicitly** in every output.

### 10.6 Sample-to-VCF alignment is silent
samples in phenotype/covs file but missing from VCF are dropped without erroring. Our Phase 5 run silently ignored 20,188 samples this way (cases_control.txt had 59k entries, VCF had 41,748). For BF4, consider an OPTIONAL strict-mode flag that errors on misalignment.

### 10.7 The `--bin-minimum-size 5` default is consequential
Bins with fewer than 5 variants get dropped. This means some genes with very few rare variants are excluded from analysis. For very-targeted analyses (e.g., single-gene deep-dive), set `-m 1` to keep everything.

---

## Reference card — exact Phase 5 invocation

```bash
biobin \
    -D /project/ritchie/datasets/loki/loki-20230816.db \
    -V analysis/daniel/outputs/phase5/merged/merged_maf.001_noRels_keepHLcases.vcf \
    -p analysis/daniel/outputs/phase4/cases_control.txt \
    --covariates analysis/daniel/outputs/phase4/covs.txt \
    --bin-regions Y \
    --region-file analysis/daniel/outputs/phase4/gene_list_regions.txt \
    -G 38 \
    --test logistic \
    --report-prefix analysis/daniel/outputs/phase5/biobin/merged_maf.001_noRels_keepHLcases
```

All other flags use biobin defaults from `biobin -S`. The above produced ESRRB at p=8.6308e-05 in our Phase 5.

---

## Open questions to verify against biobin source code

1. **Exact regression implementation** — is it Wald p-value, score test, or LRT? GSL provides `gsl_multifit_robust` and `gsl_multifit_linear`, but the logistic case uses iterative reweighted least squares (IRLS). Need to confirm convergence criteria and how the p-value is derived.
2. **Burden score quantification** — confirmed additive default = sum of dosages, but does biobin apply weighting beyond `--weight-loci`? E.g., is there an implicit MAF-based weight in SKAT?
3. **Multi-allelic split semantics** — when a variant has 3 alleles (e.g., A/T/C), does biobin create 2 separate loci (A/T and A/C) or one merged locus? Affects burden count calculation.
4. **The `--bin-expand-size` splitting rule** — what exactly determines how a >50-variant bin splits into children? Transcripts? Exons? Equal-sized chunks?
5. **`--ambiguity resolvable`** — what does "resolvable" mean? Likely about which gene a variant gets assigned to when it overlaps multiple genes.

Recommend: dig into biobin source code for these specifics before locking BF4's implementation.

---

## Where biobin source lives (probably)

The Ritchie Lab's biobin source is somewhere on GitHub / internal Bitbucket. Worth asking Alex Frase or Carrie Brown (Ritchie Lab) for the repo URL and a developer-level walkthrough. The binary at `/project/ritchie/env/modules/rlsoftware/latest/bin/biobin` (compiled 16 Jun 2020 or earlier — same era as plink 1.90b6.18) is the canonical reference.

For BF4 specifically: BF4 already integrates LOKI, so the database access layer is shared. The burden-test layer is the new code BF4 needs to add.

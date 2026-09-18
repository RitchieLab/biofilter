# ADSP × Biofilter — selecting variant pairs for interaction testing

Andre Rico · Ritchie Lab · run of 2026-09-18

This is the pipeline that turns the 711,836-variant ADSP list into a set of
variant × variant models worth testing for interaction. Three steps, each a
notebook and an equivalent script, all of it running against a Biofilter 4.3
bundle.

**Results are reported here; the files themselves are shared separately.**

---

## The short version

```
711,836   ADSP variants
   ↓      step 1 — is there a protein-coding gene this variant could affect?
355,644
   ↓      step 2 — does it do something to that gene?
  7,603
   ↓      step 3 — which pairs share biology?
817,420   models     (91,135 if a gene pair must be asserted by two curations)
```

Against the ~10 million models considered feasible, we land an order of
magnitude below it. The open questions at the end are therefore about **what to
spend that headroom on**, not what else to cut.

---

## Step 1 — variants that map to a protein-coding gene

Per the 2026-08-28 discussion: restrict the gene list first, then keep any
variant VEP links to a surviving gene. Consequence is not considered here.

**Criterion.** `gene_masters.locus_group = 'protein-coding gene'` (HGNC), reached
through the VEP annotations gnomAD ships. Genes with no HGNC locus type are
excluded by default so the gene list has a single provenance; they are 562
NCBI-only `LOC*`/`ENSG*` entries, and keeping them is a flag away.

| | variants |
| --- | ---: |
| input | 711,836 |
| matched in the bundle | 711,651 |
| linked to a protein-coding gene | 356,268 |
| less those whose only gene has no HGNC locus type | −624 |
| **delivered** | **355,644** (49.97%), as 380,775 variant × gene rows |

Dropped, and why — the three are mutually exclusive and sum to the total:

| | variants |
| --- | ---: |
| VEP names no gene at all (intergenic) | 179,630 |
| VEP links only to lncRNA / pseudogene | 104,998 |
| VEP names a gene the bundle has no entity for | 71,379 |
| **total dropped** | **356,007** |

`intron_variant` is 79.6% of delivered rows, as expected. 93.5% of variants
touch exactly one gene.

> The equivalent 4.2.0 analysis delivered 355,042. The rewrite reproduces it to
> within 600 variants in 355,000.

---

## Step 2 — the functional-consequence partition

Per 2026-09-03, and confirmed as a **partition**: each variant sits one exam
only, decided by whether it has a coding consequence.

| queue | criterion | result |
| --- | --- | ---: |
| **A** coding | `severity_rank ≤ 14` — exactly the ten consequences listed | **2,800** |
| **B** non-coding | brain eQTL **and** pQTL naming the same gene | **4,803** |
| | **total** (no overlap) | **7,603** |

**Queue A.** 6,601 variants have a coding consequence; 3,801 fail the
protein-altering cut. Of those, 2,732 are synonymous — the criterion working as
intended — and **1,064 are splice-adjacent**: `splice_region_variant` (475),
`splice_polypyrimidine_tract_variant` (458), `splice_donor_region_variant` (96),
`splice_donor_5th_base_variant` (35). See the open questions.

Four of the ten listed consequences produce nothing, all of them indel
consequences (`frameshift`, `inframe_insertion`, `inframe_deletion`,
`protein_altering_variant`), consistent with an SNV-only list.

**Queue B.** eQTL from GTEx in the bundle — 13 brain tissues, eQTL only, no
sQTL. pQTL read directly from `brain_pQTL_hmt_variant_gene_lookup.tsv.gz`, which
is not in Biofilter.

Variants pair on the **QTL target gene**, not the gene they sit in. That was the
right call by a wide margin: of the eQTL links where both genes are named,
**74.3% name a different gene**.

What limits this queue is pQTL coverage, not the criterion:

```
283,029   variants in the pQTL file
 12,331   ... also in the ADSP list          (4.4%)
  9,451   ... surviving step 1
  4,803   ... with an eQTL on the same gene
```

19% of the pQTL file is indels, and FunGen xQTL ran over a different variant
universe. **If 4,803 is too few, the lever is the pQTL requirement.**

---

## Step 3 — variant pairs

Two variants are paired when the genes they were selected for share a pathway.
Reactome and KEGG, gene pairs only from pathways naming ≤500 genes.

3,094 genes → 100,245 gene pairs → **817,420 models**, over 5,420 variants.

Model counts are **distinct unordered variant pairs**. Earlier estimates used
`sum(n1 × n2)` over gene pairs, which counts a pair once per gene pair linking
it; the difference is about 4%.

### Where the count sits

| `max_group_size` | one curation | two curations |
| --- | ---: | ---: |
| no limit | 3,825,038 | 797,453 |
| **500** | **817,420** | **91,135** |
| 300 | 592,407 | 66,924 |
| 200 | 402,114 | 48,585 |

The size cap is not a performance setting. A pathway naming 2,615 genes links
its members while asserting almost nothing about any of them; 35 of the 2,526
groups reaching our genes exceed 500 and are dropped. Removing the cap
multiplies the answer nearly fivefold with exactly those groups.

> For reference, the 4.2.0 estimate for `≤500` with two sources was 99,321
> against 91,135 here; the difference is the deduplication above.

### Two result sets

Both are written, with their own provenance:

| set | models | gene pairs | variants reaching a model |
| --- | ---: | ---: | ---: |
| one curation | 817,420 | 100,245 | 5,420 |
| two curations | 91,135 | 11,727 | 3,133 |

They are different claims, not one being a sample of the other: the first asks
whether the genes share a pathway, the second whether Reactome and KEGG
independently agree that they do.

⚠️ **Filtering the larger file by `group_support_source_count >= 2` does not
reproduce the smaller one** — it returns 91,093, missing 42 pairs (0.05%).
Deduplication keeps one row per variant pair, and when a variant belongs to two
genes the surviving row can be the lower-support one while another gene pair
satisfies the criterion. The criterion has to be applied in the query, not
afterwards.

---

## Open questions

**1. The 1,064 splice-adjacent variants — in or out?** They are `coding` by
category, so the partition sends them to queue A, where they fail the ≤14 cut;
queue B cannot take them back whatever regulatory evidence they carry. This is
the partition working as specified, but it may not be what was intended. Moving
the queue A cutoff from 14 to 18 is the whole fix.

**2. What should the headroom buy?** Measured levers, roughly in order of
effect:

| | effect |
| --- | --- |
| drop the pathway size cap | 817K → 3.8M models |
| swap the pQTL requirement for eQTL in ≥5 brain tissues | queue B 4,803 → 25,179 |
| add Diseases / Proteins / Genes as group types | not yet measured |
| relax the queue A severity cut | +1,064 variants |

**3. Does the eQTL p-value cutoff earn its place?** Currently off. A loose cutoff
does almost nothing, because the pQTL requirement is already the filter:

| cutoff | queue B variants |
| --- | ---: |
| none | 4,803 |
| p ≤ 1e-4 | 4,704 |
| p ≤ 1e-6 | 3,328 |
| p ≤ 1e-8 | 2,378 |
| p ≤ 1e-10 | 1,755 |

**4. Which result set should the interaction testing use** — 817,420 or 91,135?

---

## Running it

Each step is a notebook to read and a script to run; they do the same thing.

```bash
python step_01_adsp_coding_gene_filter.py --input data/adsp_variants.csv \
    --out-summary outputs/step1_summary.csv \
    --out-detail  outputs/step1_per_variant.csv \
    --out-coding  outputs/step1_per_variant_coding.csv

python step_02_adsp_functional_filter.py \
    --input outputs/step1_per_variant_coding.csv \
    --pqtl  data/brain_pQTL_hmt_variant_gene_lookup.tsv.gz --out-dir outputs

python step_03_adsp_variant_pairs.py \
    --input outputs/step2_selected_variants.csv --out-dir outputs \
    --max-group-size 500 --min-group-sources 1
```

The whole analysis is five Biofilter report calls: `annotate_variant` and
`annotate_gene` (step 1), `expand_variant_regulatory` and `annotate_gene`
(step 2), `pair_genes` (step 3). No hand-written SQL, no database server — the
bundle is a directory of Parquet files.

`pair_genes` was written for this analysis. `pair_variants` derives
variant-to-gene membership from coordinates, which is right for a coding variant
and wrong for a regulatory one, and would have silently dropped queue B. The new
report takes the attachment from the caller instead
([ADR-005](../../../adr/0005-pair-genes-and-caller-supplied-expansion.md)).

Every output file carries a `.provenance.json` recording the bundle it came
from, the reports used, and the parameters. This run is bundle
`9b8419b48be5004e`.

---

## Not in this repository

- **`data/`** — the ADSP variant list and the pQTL lookup.
- **`outputs/`** — result files, shared separately.
- Meeting notes and working documents.

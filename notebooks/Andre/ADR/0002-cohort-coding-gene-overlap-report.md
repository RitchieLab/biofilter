# ADR-002: Add a Cohort-Scale "Coding Gene Overlap" Report

| Field      | Value                                                                                                                                        |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Status     | **Proposed** (POC validated 2026-08-20 on 711,836 real ADSP variants)                                                                        |
| Date       | 2026-08-21                                                                                                                                   |
| Author     | Andre Rico                                                                                                                                   |
| Supersedes | (none)                                                                                                                                       |
| Related    | [ADR-001](0001-duckdb-parquet-strategy.md) (parquet/DuckDB read mode), [poc_coding_gene_overlap.py](../poc_coding_gene_overlap.py) (the POC) |

---

## 1. Context

A collaborator asked a question BF4 could not answer:

> "If I share the CSV with the ADSP variants that meet the p-value criteria
> or are unmatched (~700k variants), could you run a Biofilter report that
> tells us how many of those variants map to protein-coding genes?"

The input was 712,000 lines of `chr:pos:ref:alt` (e.g. `1:633963:C:T`),
no header, one column. The question is a **cohort-level aggregate**:
*how many of my N variants*, not *what is known about variant X*.

### Why no existing report answers it

| Report | Blocker |
| --- | --- |
| `annotation_master_variant` | Parses `chr:pos:ref:alt` natively, but `_lookup_variants()` issues **one SQL query per input variant**. 712k inputs = 712k round-trips. It also emits one DataFrame row per variant × transcript, so the result set explodes well past memory before any aggregation happens. |
| `variant_gene_location_model` | `_detect_mode()` / `_parse_region_str()` cannot parse `1:633963:C:T` — it reads the string as a region and fails on the `C:T` tail. Also loops per input. |
| everything else | Wrong grain: entity-centric or gene-centric, not "classify a list of positions". |

The gap is structural, not a missing parameter. Every variant report in
BF4 today is built for the **tens-to-thousands** input range with
**per-variant detail** as the deliverable. Nothing is built for
**hundreds of thousands in, one summary row out**.

### Why the question is ambiguous

"Maps to a protein-coding gene" has (at least) three defensible readings:

- **(A) Gene body, positional** — the variant position falls inside the
  gene's start/end interval, introns and UTRs included.
- **(B) Transcript annotation** — VEP annotated the variant against a
  transcript whose biotype is `protein_coding`. This is what dbSNP shows.
- **(C) Protein-altering** — the consequence actually changes the protein
  (missense, LoF, splice). A much smaller number.

A and B are *not* interchangeable. Measured on the real 712k list they
differ by 5.8 percentage points, in both directions (see §5). The cause
is gene-model divergence: BF4's `entity_locations` carries Ensembl gene
bodies, while the VEP annotations shipped with gnomAD follow RefSeq
transcripts, including extended predicted `XM_` transcripts that reach
past the canonical gene end.

Worked example — `1:702358:G:A` (rs867753267):

| Source | Verdict |
| --- | --- |
| BF4 positional (Ensembl, OR4F16 = 685,716–686,654) | **not** in a protein-coding gene; position falls inside LINC00115, a lncRNA spanning 586,945–827,989 |
| dbSNP / VEP (RefSeq `XM_` transcripts) | **intron variant of OR4F16**, protein-coding |

Both are correct. They use different references. A report that silently
picks one produces a number the user cannot reconcile with dbSNP, and
that is exactly what happened during the POC.

---

## 2. Decision

Add a report `variant_coding_gene_overlap` that answers the cohort
question, computing **both classification axes and reporting them
separately** rather than choosing one.

### D1 — Two independent axes plus their union

The report emits three booleans per input variant:

```
coding_by_position    →  pos BETWEEN gene.start AND gene.end
                         for a gene whose HGNC locus_group is
                         'protein-coding gene', build 38

coding_by_annotation  →  EXISTS (variant_molecular_effects row
                                 whose biotype is 'protein_coding')

coding_by_either      →  coding_by_position OR coding_by_annotation
```

`coding_by_either` is the recommended headline number: it is the most
inclusive and has no systematic blind spot. The other two are kept
because reviewers *will* spot-check against dbSNP, and the report must
be able to explain the discrepancy instead of being contradicted by it.

Axis (C), protein-altering, is **not** in scope for v1 — see §6.

### D2 — Set-based execution, never a per-input loop

The input list is staged once into a temporary table, then classified
with a single range join and a single aggregation. No Python-side loop
over inputs. This is the whole reason the report is feasible:

| Approach | 712k inputs |
| --- | --- |
| Per-variant queries (`annotation_master_variant` shape) | not viable — 712k round-trips |
| Set-based over PostgreSQL | ~2.5 min |
| Set-based over the parquet bundle via DuckDB | **4.6 s** |

### D3 — Summary is the default; per-variant detail is opt-in

`run()` returns the summary DataFrame (one row per metric). A
`detail=True` parameter returns the per-variant frame instead — 712k
rows, so it is written to disk via `--output` rather than rendered.

Detail columns, as validated in the POC:

```
input_variant, chromosome, position,
coding_by_position, coding_by_annotation, coding_by_either,
found_in_db,
position_gene_symbols, annotation_gene_symbols,
n_any_genes, locus_groups_hit
```

`found_in_db` matters: the annotation axis requires an exact
chr/pos/ref/alt match in `variant_masters`. Without that column a user
cannot distinguish "annotated as non-coding" from "we have no annotation
for this variant at all".

### D4 — `window` parameter, defaulting to 0

`window=N` extends each gene interval by N bp on both sides, for
"within N kb of a protein-coding gene" questions. Default 0 = gene body
only, so the headline number is never silently inflated.

### D5 — Reuse the parquet bundle, no new artifacts

Everything needed is already in the bundle exported today:
`gene_masters`, `gene_locus_groups`, `entity_locations` (full tables),
`variant_masters` and `variant_molecular_effects` (partitioned),
`variant_biotypes`. No ETL change, no new export table.

---

## 3. Consequences

### Positive

- **Unblocks a whole class of question.** "How many of my N variants
  are X" is the shape most collaborator requests actually take. This is
  the first report built for it.
- **The discrepancy is documented, not hidden.** A user who checks
  `1:702358:G:A` on dbSNP finds a column that already explains why the
  positional answer differs.
- **Fast enough to be interactive.** 4.6 s on the bundle means it runs
  on an LPC login node, no LSF job needed.
- **Detail CSV makes the user self-sufficient.** They can recount under
  any definition without a round-trip through us.

### Negative

- **Set-based SQL, not ORM.** CLAUDE.md says prefer SQLAlchemy ORM. A
  range join with `count(*) FILTER` and `string_agg` over 712k staged
  rows is not expressible in the ORM without falling back to
  `text()`. This report is an explicit, documented exception.
- **Staging a temp table is a write.** The parquet backend is read-only.
  Needs verification that DuckDB `CREATE TEMP TABLE` works against a
  connection whose catalog is read-only views (it did in the POC, on a
  separate in-memory connection — the report path must be confirmed).
- **New report shape to maintain.** Summary-vs-detail duality is not how
  the other 17 reports work. It needs a clear `reports_explain` doc or
  it will be misused.

### Neutral / mitigations

- **Chromosome encoding.** X/Y/MT map to 23/24/25 in BF4. The parser
  handles `chr` prefixes and `:`/`_`/`-` separators. Anything that does
  not parse lands in an `unparseable` count that is printed before the
  percentages — the user is told when the denominator is not what they
  think it is.
- **Multi-allelic inputs.** Two rows differing only in `alt` are two
  input rows and are counted twice on the positional axis (same
  position). This is correct for "how many of my variants" but should be
  stated in the explain doc; `distinct_positions` is reported alongside.

---

## 4. Alternatives Considered

### Alternative A — Extend `annotation_master_variant` with an aggregate mode

Add a `summarize=True` parameter that skips the per-variant frame.

Rejected. The blocker is not the output shape, it is `_lookup_variants()`
doing one query per input. Fixing that means rewriting the report's core
to be set-based, which changes its behaviour for every existing caller.
A new report carries no such risk, and the two have genuinely different
purposes: one is "tell me everything about these variants", the other is
"count how many of these variants satisfy a predicate".

### Alternative B — Positional axis only (simplest)

Ship only `coding_by_position`.

Rejected on measured evidence: 41,460 variants (5.8%) in the real 712k
list are protein-coding by annotation and missed by position. Users will
spot-check on dbSNP and find the report wrong.

### Alternative C — Annotation axis only (matches dbSNP)

Ship only `coding_by_annotation`.

Rejected for two reasons. First, it inverts the same problem: 7,795
variants (1.1%) are positional-only. Second, it requires the variant to
exist in `variant_masters`. On this particular cohort coverage was 99.7%,
but that is an ADSP-specific result — a cohort enriched for rare or
novel variants would have a much larger blind spot, and the report would
under-report without saying so.

### Alternative D — Ingest RefSeq gene bodies alongside Ensembl

Make the positional axis agree with dbSNP by loading both gene models
into `entity_locations` and letting the user pick.

Not rejected, but **out of scope here**. This is an ETL/data-model
decision affecting every positional report in BF4, not a property of one
report. It deserves its own ADR. The two-axis design in D1 delivers the
correct answer today without waiting on it.

---

## 5. POC results (2026-08-20)

Executed on the LPC against the production parquet bundle, on the real
712k ADSP list. Script: [poc_coding_gene_overlap.py](../poc_coding_gene_overlap.py).

```
python poc_coding_gene_overlap.py \
    --input adsp_variants.csv \
    --with-annotation \
    --out-summary summary.csv \
    --out-detail per_variant.csv
```

| Metric | Variants | % of 711,836 |
| --- | ---: | ---: |
| `coding_by_position` | 314,335 | 44.16% |
| `coding_by_annotation` | 348,000 | 48.89% |
| **`coding_by_either`** | **355,795** | **49.98%** |
| both axes agree — coding | 306,540 | 43.06% |
| annotation only (position missed it) | 41,460 | 5.82% |
| position only (annotation missed it) | 7,795 | 1.10% |
| both axes agree — not coding | 356,041 | 50.02% |
| `found_in_db` | 709,978 | 99.74% |

Distinct protein-coding genes hit: 15,313 positional / 17,407
annotation / 17,644 union.

**Takeaways:**

1. 44% by gene body matches the expected fraction of the genome covered
   by protein-coding gene bodies — the positional axis is behaving.
2. The 5.8% / 1.1% asymmetry is systematic, not noise. A 1-in-100 chr1
   sample run against PostgreSQL independently gave 6.7% / 1.0%, so the
   effect reproduces across backends and subsets.
3. gnomAD coverage of 99.7% on this cohort means the annotation axis was
   near-complete here. Do not generalise that to other cohorts.
4. Wall clock 4.6 s for the positional pass; the annotation pass adds
   the `variant_molecular_effects` join and is the dominant cost.
5. `variants_intergenic` (317,938 in the positional-only run) is a
   **ceiling, not a count**. Whole locus groups have zero build-38
   coordinates in the bundle — 11,974 ncRNA genes and 9,126
   biological-region entries have no `entity_locations` row for build 38,
   so variants landing in them are indistinguishable from true
   intergenic. This must be stated in the explain doc.

---

## 6. Implementation outline

Branch from **`main`** (parquet support is already there —
`PARQUET_URI_SCHEME` is in `biofilter/modules/db/database.py`). The POC
script's logic ports over largely unchanged.

1. **`biofilter/modules/report/reports/report_variant_coding_gene_overlap.py`** (~4 h)
   - `name`, `description`, `available_columns()`, `example_input()`
   - `run(input_data, window=0, with_annotation=True, detail=False)`
   - Stage inputs → `gene_intervals` temp → range join → aggregate
   - Guard: refuse `detail=True` without an `--output` target

2. **`reports_explain/report_variant_coding_gene_overlap.md`** (~2 h)
   - The three definitions, in plain language
   - The `1:702358:G:A` worked example — it is the whole ADR in one row
   - The `variants_intergenic` ceiling caveat
   - Multi-allelic counting behaviour

3. **Backend verification** (~2 h)
   - Confirm `CREATE TEMP TABLE` on a read-only parquet connection
   - Confirm the SQL runs unchanged on PostgreSQL (`count(*) FILTER`,
     `string_agg`, range join all exist in both)
   - Time the PG path so the explain doc can set expectations

4. **Tests** (~2 h)
   - Unit: input parser (chr prefixes, X/Y/MT, malformed, header line)
   - Integration: known small fixture with hand-verified expected counts,
     including one gene-model-divergence case

Total: roughly **1.5 days**.

---

## 7. Open questions

- **ORM exception.** CLAUDE.md mandates ORM over raw SQL. This report
  needs set-based SQL. Confirm the exception is acceptable and note it
  in the report's docstring so a future reader does not "fix" it.
- **Temp tables on a read-only backend.** Verified working in the POC on
  a standalone DuckDB connection. Must be re-verified through BF4's own
  `parquet://` connection path, which registers views over a shared
  catalog.
- **Protein-altering axis (definition C) works, but only via the
  dimension.** `variant_molecular_effects.consequence_group_id` and
  `consequence_category_id` are **100% NULL** —
  `dtp_variant_gnomad.py` never populates them (`grep` for either column
  in that DTP returns nothing), and `consequence_raw` is also NULL
  because the value is normalised into an FK. The working route is
  `variant_molecular_effects.consequence_id` →
  `variant_consequences.consequence_group_id` →
  `variant_consequence_groups`, the way
  `annotation_master_variant._load_consequence_map()` already does.
  Verified 2026-08-21. The denormalised columns should still be
  populated — they exist precisely to avoid that two-hop join on a
  1.79-billion-row table.
- **`variant_impacts` is polluted.** It contains `HIGH`, `LOW`,
  `MODERATE`, `MODIFIER` as expected, but also rows like
  `INTRON_SIZE:32250` and `PERCENTILE:0.474945533769063` — LoF_info
  field values leaking into the impact dimension. Do not filter on
  impact until this is fixed; filter on consequence group instead.
- **Missing build-38 coordinates.** The `variants_intergenic` ceiling is
  a data completeness issue in `entity_locations`, not a report bug.
  Should it be an ETL backlog item?
- **Report naming.** `variant_coding_gene_overlap` vs
  `variant_gene_overlap_summary` — the latter generalises better if we
  later add non-coding or regulatory variants of the same question.

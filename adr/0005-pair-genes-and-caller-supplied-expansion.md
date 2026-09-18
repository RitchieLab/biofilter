# ADR-005: `pair_genes`, and Expansion by a Caller-Supplied List (4.3.0)

| Field   | Value                                                                                                                                  |
| ------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Status  | **Proposed** (measurements taken 2026-09-17 against the 20260914 bundle)                                                                 |
| Date    | 2026-09-17                                                                                                                              |
| Author  | Andre Rico                                                                                                                              |
| Related | [ADR-004](0004-parquet-native-report-module.md) §2.6 (result objects), §2.10b (a result you can put down), §2.12 (report classes); [ADR-002](0002-cohort-coding-gene-overlap-report.md) D5 (reuse the bundle, no new artifacts) |

---

## 1. Context

`pair_variants` consolidated six reports that were one pipeline written in
three eras. It runs three stages:

1. **Place.** Read each input as a gene or a variant; place a variant on the
   genes whose build-38 range contains it.
2. **Connect.** Seed genes reach partner genes through a shared group — a
   pathway, a disease, a protein.
3. **Pair.** Gene pairs become variant pairs.

The consolidation was right and this ADR does not reopen it. What follows is
about a question the pipeline cannot express, found by trying to ask it.

### 1.1 The ADSP analysis attaches variants to genes the bundle cannot

The ADSP interaction study selects variants in two branches. Coding variants
are kept when they alter the protein, and pair on the gene they sit in —
which stage 1 derives correctly. Non-coding variants are kept when a brain
eQTL **and** a pQTL name the same gene, and pair on **that** gene.

A regulatory variant sits inside one gene and acts on another. Measured on the
20260914 bundle across the analysis's non-coding inputs: of the eQTL links where
both genes are named, **72.5% name a gene other than the one the variant sits
in**. The exception is the rule.

So the claim "this variant belongs to this gene" came from evidence the bundle
does not hold — a colocalization of two QTL datasets, one of which
(FunGen xQTL pQTL) is not in Biofilter at all.

### 1.2 Both ends of the pipeline assume positional membership

Supplying the association to stage 1 would not be enough. Stage 3 re-derives it:

```sql
FROM pair_genes pg
JOIN entity_locations l  ON l.entity_id = pg.gene_id
JOIN variant_masters  v  ON v.position BETWEEN l.start_pos - window
                                           AND l.end_pos   + window
LEFT JOIN variant_genes vg ON vg.gene_id = pg.gene_id
                          AND vg.variant_key = v.variant_key
```

The candidate set is the positional range join; `variant_genes` enters only as a
`LEFT JOIN` marking which variants the caller named. A variant attached to a
gene it does not sit inside is absent from that candidate set, so the
`LEFT JOIN` finds nothing and it is dropped.

No error is raised. The result is simply thinner than it should be, in a way
that reads as biology.

### 1.3 Stage 3 also asks the opposite question

Even without the membership problem, stage 3 is the wrong shape for this caller.
It expands each gene into **every variant inside its coordinates** — roughly
4,000 for a gene on chr22 — because its usual caller names genes and wants their
variants. The ADSP analysis spent two steps narrowing 711,836 variants to 2,416
and does not want them expanded back.

### 1.4 Gene pairs are already reachable, as a parameter of another report

`output_grain="gene_pairs"` stops after stage 2 and returns exactly the pairs
with their support. The capability exists; it is spelled as a mode of a report
named for a different question, which is where callers do not look for it.

### 1.5 An MVP was built before this ADR, on purpose

`notebooks/Andre/adsp/adsp__03_variant_pairs.ipynb` uses `pair_variants` for
stage 2 only and does the pairing itself. §5 reports what it measured. The
specification below is written from that use rather than from design.

---

## 2. Decision

### D1 — `pair_genes` is a report, not a grain parameter

A pair of genes is a unit of answer, not a projection of a variant pair. It is
what a caller wants when the question is "which of these genes are related, and
by what", and that question stands on its own.

`output_grain` **leaves `pair_variants`**. Two ways to reach one answer is what
the six-report consolidation removed, and reintroducing it here would repeat the
mistake at a smaller scale.

Both reports share a `_pairing.py`, following the convention `_annotation.py`
established: a module that does not begin with `report_` is not discovered as a
report, and holds what the reports would otherwise retype. Deliberately small —
the group resolution, the `max_group_size` cut, and the `group_filter`
provenance block.

### D2 — An optional expansion stage, driven by a list the caller supplies

`pair_genes` accepts a gene → item mapping and, when given one, returns the item
pairs implied by the gene pairs.

This is the generalisation of what the MVP hand-wrote. The ADSP case supplies
`(variant, gene)`; the mechanism is indifferent to what the item is.

### D3 — The payload is opaque, and that is the whole design

The report **never interprets the item**. It is a label carried from the input
to the output.

This is what makes gene→position, gene→position-in-build-37, gene→rsID and
gene→exposure one feature rather than four. Build 37 is the sharpest case: the
bundle holds no build-37 coordinates and never will, but a report that does not
read the coordinate can pair build-37 positions without the bundle knowing what
a build-37 position is.

The price is stated rather than hidden: with an opaque payload the report cannot
filter by allele frequency, resolve an rsID, or validate anything about the item.
Correctness of the items belongs to the caller. Reports that *do* interpret
variants already exist; this is not one of them.

**Build is the caller's to manage, and the documentation says so.** Biofilter
is build 38 throughout. This report performs no variant → gene mapping at all —
no overlap, no window, none of the machinery `pair_variants` uses to go from a
variant to a gene and back. It uses the link the caller supplied and nothing
else, so a caller pairing build-37 positions gets build-37 positions out,
correctly, and Biofilter never has to know. The report is not being lenient
about build; build never enters it.

It follows that the opaque payload also bounds what deduplication can promise
(D5). Two spellings of the same item — `22:100:A:G` and `chr22:100:A:G` — are
two items, and nothing here can tell otherwise. The platform owns the three
rules; the caller owns writing an item the same way every time.

### D4 — Expansion requires items on both sides

A gene pair produces item pairs only when **both genes carry at least one item**
in the caller's list. Not "both genes are in the input" — a gene can be named and
carry nothing.

Consequently `membership="either"` is **refused** when a mapping is supplied,
rather than silently yielding half-pairs: the partner gene came from the bundle,
not from the caller's list, so there is nothing on that side to pair.

### D5 — The expansion owns the three things that are easy to get wrong

The cross product is not the work. These are, and they are identical for every
caller:

| rule | why |
| --- | --- |
| pairs are unordered | testing (X, Y) is testing (Y, X) |
| deduplicate globally | the same item pair arrives through every gene pair linking it |
| drop self-pairs | an item attached to both genes of a pair would pair with itself |

Measured on the MVP: deduplication alone is **4.3%** (72,554 against 75,794),
caused by 83 items attached to two genes each. Each of these inflates a model
count silently when missed, which is the argument for the platform owning them.

### D6 — One result, two tables; the expansion is the primary one

A result carrying both shapes says so through `extra_tables` (ADR-004 §2.6)
rather than flattening them into one wide table.

**When expansion is requested, the item pairs are the primary table** and the
gene pairs are the extra. `write()` exports only the primary while `save()`
writes every table, so making gene pairs primary would hand
`result.write("pairs.csv")` the wrong grain without any error.

### D7 — The mapping arrives as a file, or inline when small

`aggregate_cohort_variants` already takes a `cohort_file`, and `_cohort.py`
states the principle this follows: *"the bundle is what Biofilter knows; a
cohort file is what the user actually measured."* A gene → item mapping is
another instance of what the user decided rather than what the bundle holds.

Inline stays available for small lists — the ADSP mapping is 2,501 rows — but a
caller with hundreds of thousands cannot pass that through `--param`.

The shape is two columns, gene then item, many-to-many in both directions:

```
APOE  111
APOE  222
APOE  333
APOA  444
APOA  555
```

With a gene pair APOE-APOA found through pathway A, the expansion is the cross
product of the two sides — 3 × 2 = 6 item pairs: 111-444, 222-444, 333-444,
111-555, 222-555, 333-555. The rules in D5 do not show up in an example this
clean, which is the point of stating them separately: they decide what happens
when an item sits on both genes, when a pair is reached through two groups, and
when it is reached through two different gene pairs.

### D8 — The group's source becomes a column

The MVP recovers Reactome versus KEGG from accession prefixes (`R-HSA-`, `hsa`)
because `pair_variants` does not return the source. That is the one genuinely
fragile thing in it: a new pathway source lands in `other` and stops being
counted, and nothing fails.

**The bundle does carry it** — `entity_relationships.data_source_id` joins
`etl_data_sources` and gives `reactome_relationships` (144,094 links) and
`kegg_relationships` (39,451) directly. The report drops it, so this is
returning a column the data already has rather than deriving one it does not.
Cheaper and more certain than parsing accessions, and it stays right when a
third source arrives.

The prefix split is clean today — 100% of Reactome groups start `R-HSA-` and
100% of KEGG groups start `hsa` — which is precisely why its failure would be
silent.

`pair_genes` carries `group_support_sources` beside `group_support_names`, and
takes `min_group_sources`. This is a different and stronger claim than the
existing `min_group_support`, which counts groups: two curations agreeing is not
the same as one curation saying it twice.

### D9 — `input_1` does not survive into a gene-pair result

The gene-pair shape inherited an `input_1` column with no `input_2`. In a
symmetric result that reads as a leftover, and `gene_1_from_input` /
`gene_2_from_input` already carry what it was for.

### D10 — `available_columns()` answers for the primary table, whichever it is

D6 makes the primary table's shape depend on whether a mapping was given, and
`available_columns()` is a classmethod that cannot see the parameters. The same
wart already exists in `aggregate_cohort_variants`, where it declares the
`variants` grain and the `bins` columns live in a second constant the contract
does not mention.

Rather than inherit it quietly: `pair_genes` declares the gene-pair columns,
since that is what it returns when asked nothing special, and names the
expansion columns in a constant of their own that the explain doc and the tests
both reference. The limitation is stated in the doc instead of being discovered.

### D12 — The caller names the column, because nothing else can

*Added 2026-09-18, during implementation.*

`gene_identifier` says how gene identifiers are read, for `input_data`
and the mapping alike: `alias` searches every alias, a named code system
(`HGNC`, `ENTREZ`, `ENSEMBL`, …) searches only that one, and `entity_id`
skips the alias table for the primary key.

The first implementation guessed instead — an entity id took precedence
when the text parsed as an integer. Measured against the bundle, that is
wrong 14,335 ways: 174,410 gene aliases are bare numbers, and 14,335 of
those values are also the entity id of a *different* gene. Entrez 2 is
A2M and entity 2 is A1BG-AS1; Entrez 29974 is A1CF and entity 29974 is
RPS2P20. The heuristic returned the wrong gene silently, which is the
class of defect this project keeps finding.

Naming the code system is not only disambiguation, it is a narrower
search: under `entrez`, `2` can only be A2M. The systems on offer are
read from the bundle rather than hardcoded, so a build without UCSC does
not advertise it.

`entity_id` carries the caveat ADR-003 §2.5 attaches to it: ids are
scoped to the build that issued them, so a list of them means nothing
without its `bundle_id`.

### D13 — `pair_variants` takes variants, and nothing else

*Added 2026-09-18.*

Gene input, the gene-to-variant expansion, `max_variants_per_gene` and
`membership` all leave `pair_variants`. It takes rsIDs and positions, and
pairs the variants it was given.

D11 left open whether `pair_variants` should be re-expressed on top of
`pair_genes`. This is not that — the two reports stay independent — but
it settles the part of the question that mattered: with `pair_genes`
answering "which genes are related", the gene path here was a second way
to reach a neighbouring answer.

Three arguments, in the order they carry weight.

**It hid a choice that is the caller's.** A gene on chr22 holds about
4,000 variants. The report kept 100 of them, ranked by allele frequency,
and the caller never saw which. `expand_gene_to_variant` puts that list
in front of the person choosing, who can filter it before anything is
paired. Deciding for them and not saying so is the defect this project
keeps finding in its own code.

**It was an implicit chain**, and chaining is the caller's job — the rule
set when `expand_gene_to_variant` was built and `variant_annotation_expanded`
was retired for scraping another report's CSV.

**Every parameter now means something in every call.** `max_variants_per_gene`
existed only to bound the expansion; `membership="either"` was the
unbounded mode, the one that needed caps and spilled memory (five seed
genes reach 19,393 partner genes). A parameter that is load-bearing in
one mode and meaningless in another is exactly what Alternative A was
rejected for.

What is lost, and what replaces it:

| gone | instead |
| --- | --- |
| gene input | `expand_gene_to_variant`, then pair its output |
| `membership="either"` | `pair_genes(membership="either")` → `expand_gene_to_variant` on the partners → pair |

Both replacements are longer and visible. That is the trade.

A removed parameter is **refused by name** rather than ignored: a caller
passing `membership="either"` would otherwise get pairs built under a
different rule and no indication of it. The refusal says what replaced
it.

### D11 — Not decided here

- Whether `pair_variants` should eventually be re-expressed on top of
  `pair_genes`. D13 removed the expansion that made the symmetry
  tempting, so the two are now independent reports that share `_pairing`
  and answer different questions. Merging them would be a new argument,
  not a continuation of this one.
- Whether the mapping may carry its own attributes (a p-value, a tissue) that
  travel to the output alongside the item. The MVP kept evidence columns in its
  own files and joined afterwards.
- Whether expansion should cap its output the way `max_variants_per_gene` caps
  stage 3. The caller's list is already a deliberate selection, so a cap may be
  solving a problem this shape does not have.

---

## 3. Consequences

### Positive

- A variant → gene relation that comes from outside the bundle becomes
  expressible without the bundle having to hold it. Colocalization,
  fine-mapping and curated assignments are all the same feature.
- The three pairing rules stop being re-implemented per analysis.
- Gene pairs become findable: `report list` names the question.
- `pair_variants` gets simpler, not more complex — it loses a grain parameter
  and gains nothing.
- Build-37 callers are served without the bundle carrying build 37.

### Negative

- A third report in the pairing family. The mitigation is that they share
  `_pairing.py` and answer visibly different questions, but the count is real.
- Removing `output_grain` is a breaking change for any caller using it. It
  appears in the ADSP step 3 notebook and script, and also in
  `tests/unit/report/test_report_pair_variants.py`, a section of
  `notebooks/templates/reports__pair_variants.ipynb`, and
  `reports_explain/report_pair_variants.md` — all in this repository, but the
  change list is four places wider than the two obvious ones.
- An opaque payload means the report cannot help with item correctness. A
  caller who supplies a malformed variant id gets pairs of malformed ids.
- Two tables in one result is a shape callers have to learn; `write()` giving
  only the primary is a sharp edge even with D6.

### Neutral / mitigations

- Performance is not the motivation and should not be claimed as one: 1,010
  genes to 12,823 gene pairs takes ~5 s today.
- The expansion reads nothing from the bundle. That makes it an odd thing to
  call a report on its own, which is exactly why D2 makes it a stage rather
  than a fourth report — without gene pairs there is nothing to expand.

---

## 4. Alternatives Considered

### Alternative A — `mapping="given"` on `pair_variants`

Add a placement mode: the caller supplies the variant → gene relation, and it
replaces the positional derivation in stages 1 and 3.

Rejected. It makes one report carry two membership models at once, and
`membership="either"` becomes genuinely ambiguous — one side from the caller's
relation, the other from the bundle's. It also leaves `max_variants_per_gene`
and `window_bp` meaningless in one mode and load-bearing in the other.

### Alternative B — A separate `expand_pairs_by_list` report

Keep `pair_genes` about genes; make the expansion its own report.

Rejected. The expansion is not an independent question: with no gene pairs there
is nothing to expand. It would also read nothing from the bundle, which makes it
a strange member of the report layer, and it forces a file round-trip between
two calls that always run together.

### Alternative C — Do nothing; keep `output_grain`

Callers who need caller-supplied expansion write it themselves, as the MVP did.

Rejected on evidence rather than principle: the MVP had to get three
non-obvious rules right (D5), and a 4.3% error in a model count is the kind of
thing that survives review because it looks plausible.

### Alternative E — Expansion as an operation on the result, not a stage

ADR-004 §2.10b made a `ReportResult` a thing you can put down and pick up.
`result.expand_by(mapping)` would be a pure transformation of one: it reads
nothing from the bundle (as §3 notes the expansion does not), it composes with
`save()` / `load()`, and it would serve any report that produces gene pairs.

Rejected, but it is the alternative a reader will propose, so the reason is
worth stating. Two things break. The mapping would not reach the provenance,
so a saved result could not say what produced its pairs — which is most of what
ADR-004 §2.10b is for. And `report explain` would not mention it, leaving the
capability exactly where §1.4 says capabilities go to be missed: reachable, and
not where anyone looks.

### Alternative D — Teach the bundle about QTL-derived gene assignment

Load the pQTL data as a DTP and derive the colocalization inside Biofilter, so
the assignment becomes a bundle fact and stage 1 can use it.

Not rejected, but orthogonal, and it does not generalise. It would serve this
analysis and leave the next caller — fine-mapping, a curated list, build 37 —
exactly where they started. Making pQTL a DTP remains worth doing on its own
merits.

---

## 5. Evidence from the MVP (2026-09-17)

Bundle 20260914, chromosomes 1-5 and 22 at the time of measurement.

### 5.1 The pipeline as built

| stage | how | cost |
| --- | --- | --- |
| reconcile two id spaces | `annotate_gene` on 368 Ensembl ids | 0.2 s |
| gene pairs | `pair_variants`, `output_grain="gene_pairs"` | 5.7 s |
| expansion | hand-written, 2,501-row mapping | < 1 s |

1,010 genes → 12,823 gene pairs → **72,554 item pairs**.

### 5.2 Everything stage 2 returned was used

`gene_1_id` / `gene_2_id` to join, `group_support_count` to rank,
`group_support_names` to recover the source. Nothing was decorative, and nothing
outside it was needed — no `window_bp`, `af_min`/`af_max`,
`max_variants_per_gene` or `membership`. Three of those four would have had no
meaning for this caller.

That is the evidence for D1: gene pairs is a closed, coherent answer.

### 5.3 The cut that decides the size of the answer

| `max_group_size` | gene pairs | item pairs | both sources | item pairs, both sources |
| --- | ---: | ---: | ---: | ---: |
| no limit | 56,611 | 375,720 | 9,295 | 92,021 |
| 500 | 12,823 | 72,554 | 1,655 | 7,073 |
| 300 | 9,665 | 53,413 | 1,037 | 5,439 |
| 200 | 6,020 | 37,059 | 642 | 4,155 |

At 500, 35 of the 1,964 groups reaching these genes were excluded; the smallest
excluded names 521 genes and the largest seen names 2,615. Removing the cap
multiplies the answer fivefold with groups that assert almost nothing.

### 5.4 Deduplication

| definition | pairs |
| --- | ---: |
| `sum(n1 × n2)` over gene pairs | 75,794 |
| distinct unordered item pairs | 72,554 |

**4.3%**, from 83 items attached to two genes.

---

## 6. Implementation outline

1. Extract `_pairing.py` from `report_pair_variants.py`: input resolution for
   genes, the group link CTEs, `max_group_size`, the `group_filter` provenance
   block.
2. `report_pair_genes.py` on top of it, returning the gene-pair table, with
   `group_support_sources` and `min_group_sources` (D8) and without `input_1`
   (D9).
3. `reports_explain/report_pair_genes.md`, stating the opaque payload, the
   both-sides rule, and which table is primary.
4. The expansion stage: mapping parameter (inline or file), the three rules of
   D5, `extra_tables` wiring and primary-table selection per D6.
5. Remove `output_grain` from `pair_variants`, and with it: the gene-pair
   branch of its query, its tests, the "Stopping at the genes" section of its
   template notebook, and its explain doc. The assistant kit regenerates.
6. `notebooks/templates/reports__pair_genes.ipynb` from the template.
7. `tests/unit/report/test_report_pair_genes.py` against the fixture bundle,
   covering: both-sides rule, self-pair removal, global deduplication,
   `membership="either"` refused with a mapping, and the primary table when
   expansion is and is not requested.
8. Port ADSP step 3 onto the new report; the MVP's numbers are the regression
   test.

---

## 7. Questions closed (2026-09-17)

1. **No build column, and no `item_kind`.** Biofilter is build 38 and the
   documentation states it; managing build is the caller's. Since the report
   does no variant → gene mapping whatsoever, a build-37 payload passes through
   untouched and correctly. Naming the payload would be metadata about a thing
   the report already promises not to read, so it is not worth the parameter.
   See D3.
2. **`pair_genes` does not accept variants.** Its query is about genes.
   Variants enter only at expansion, and only as whatever the caller's list
   says they are. Accepting them as input would mean placing them, which is the
   assumption §1.1 exists to escape.
3. **Both a list and a file, two columns: gene, then item.** See D7. No
   ceiling is imposed on the inline form; the practical limit is what a shell
   will carry through `--param`, and the file exists for everything past it.

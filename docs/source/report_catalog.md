# Report Catalog

Every analysis Biofilter can run, organized by the question it answers.
There are 17 reports. Each one takes a list of things you already have —
gene symbols, rsIDs, disease names, a cohort's variants — and returns a
table.

For how reports work in general (parameters, input channels, output
formats), see [Reports](reports.md).

## Find your question

| You have | You want | Report |
|---|---|---|
| Gene symbols or ids | Everything the bundle knows about them | [`annotate_gene`](#annotate-what-you-already-have) |
| Gene symbols | The variants in those genes, filtered by predicted damage | [`expand_gene_to_variant`](#from-genes-to-variants) |
| rsIDs or `chr:pos:ref:alt` | Full annotation, one row per transcript | [`annotate_variant`](#annotate-what-you-already-have) |
| Variants | Which genes they regulate, in which tissue | [`expand_variant_regulatory`](#from-variants-outward) |
| Variants | Plausible interacting pairs, with the biology that links them | [`pair_variants`](#pairs-to-test) |
| Genes | Their variants, then the pairs among those | `expand_gene_to_variant` then [`pair_variants`](#pairs-to-test) |
| Genes | Which of them are related, and by what | [`pair_genes`](#pairs-to-test) |
| Genes, and your own gene-to-anything list | The pairs your list implies | [`pair_genes`](#pairs-to-test) |
| A cohort's variants | Which ones the bundle knows, binned by biology | [`aggregate_cohort_variants`](#a-whole-cohort) |
| Names that are not matching | What they actually resolve to, and where they conflict | [`resolve_entity`](#annotate-what-you-already-have) |
| Any entity list | Its one-hop neighbourhood, or the relationship rows themselves | [`expand_entity_*`](#follow-the-entity-network) |
| Disease, pathway, GO or protein names | Annotation for that domain | [`annotate_*`](#annotate-what-you-already-have) |
| A bundle | What is in it, and how it was built | [`platform_*`](#about-the-bundle-itself) |

## Running a report

```bash
biofilter --bundle /path/to/bundle report run \
  --report-name annotate_gene \
  --input TP53 --input BRCA1 \
  --output genes.csv
```

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundle")
result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])
df = result.to_pandas()
```

Three things worth knowing before you start:

- **`biofilter report explain --report-name <name>`** prints the report's
  full guide — every parameter, every column, and how to read the result.
  It is the authoritative reference; this page is the index.
- **A worked notebook** ships for each report at
  `notebooks/templates/reports__<name>.ipynb`.
- **Options go through `--param KEY=VALUE`**, separately from `--input`.
  Values are coerced: `true`/`false`, numbers, and JSON. A list is written
  as JSON — `--param impact_filter='["HIGH","MODERATE"]'`. Use
  `--params-template` to print every option a report accepts.

## Annotate what you already have

You have identifiers. You want what is known about them.

| Report | Takes | Returns |
|---|---|---|
| `annotate_gene` | Gene symbols, HGNC or Ensembl ids, aliases | Canonical ids, gene metadata, build-38 coordinates, relationship counts by related domain, and optionally how many variants fall in the gene's range |
| `annotate_variant` | rsIDs, `chr:pos`, or `chr:pos:ref:alt` | Identity, gnomAD joint frequencies, in-silico predictions, and one row per transcript the variant was annotated against |
| `annotate_protein` | Accessions, names, aliases | Canonical accession, function, location, tissue expression, isoform resolution, Pfam domains by type |
| `annotate_disease` | Disease names or aliases | Canonical ids, label and description, disease groups, cross-references by source, and the genes ClinGen links to the disease |
| `annotate_pathway` | Pathway names or ids | Canonical id and description, which source contributed it, relationship counts by domain |
| `annotate_go` | GO terms or aliases | GO id, name and namespace, parent and child counts by relation type, relationship counts by domain |
| `resolve_entity` | Any list of names | What each name resolves to, with conflict and status flags |

`resolve_entity` is the one to reach for when another report returns
`not_found` and you want to know why. It supports `match_mode=exact`
(default), `like` for substrings, and `fuzzy` for Jaro-Winkler similarity
above a threshold.

## From genes to variants

**`expand_gene_to_variant`** — the variants that belong to a list of genes.

This is the report for the common screening question: *given these genes,
which variants in them are plausibly damaging and rare enough to matter?*

First you choose what "belongs to" means, because the two answers differ:

| `mapping` | A variant belongs to a gene when… |
|---|---|
| `position` | Its coordinate falls inside the gene's build-38 range |
| `annotation` | VEP associated it with that gene |

Then you filter. Every filter below is optional and they compose:

| Filter | Options |
|---|---|
| `impact_filter` | `HIGH`, `MODERATE`, `LOW`, `MODIFIER` |
| `consequence_type_filter` | VEP consequence names |
| `lof_confidence_filter` | LOFTEE `HC`, `LC` |
| `af_min`, `af_max` | gnomAD joint allele frequency bounds |
| `cadd_phred_min`, `sift_score_max`, `polyphen_score_min` | In-silico predictor thresholds |
| `alphamissense_score_min` | AlphaMissense score |
| `alphamissense_classification` | `likely_pathogenic`, `likely_benign`, `ambiguous` |

A rare, high-impact, likely-pathogenic screen over two genes:

```bash
biofilter --bundle /path/to/bundle report run \
  --report-name expand_gene_to_variant \
  --input BRCA1 --input CHEK2 \
  --param mapping=annotation \
  --param impact_filter=HIGH \
  --param af_max=0.01 \
  --param alphamissense_classification=likely_pathogenic \
  --output candidates.csv
```

Two defaults to be aware of. `most_severe_only` is `true`, so you get one
row per gene and variant keeping the worst consequence — set it to `false`
when you want every transcript. And `max_variants_per_gene` is `5000`; when
a gene is capped, the `variants_available` column reports its pre-cap total,
so a truncated row admits that it is truncated.

## From variants outward

| Report | Answers |
|---|---|
| `expand_variant_regulatory` | Which genes does this variant regulate, and in which tissue? One row per variant × tissue × regulated gene, with effect size and p-value. Takes gene symbols, rsIDs or positions. |

## Pairs to test

Two reports generate candidate pairs, and choosing between them is
choosing **where the link between a variant and a gene comes from**.

| Report | Answers |
|---|---|
| `pair_variants` | Which of these variants plausibly interact? Places each input variant on its genes **by coordinate**, connects those genes through shared pathways, diseases or proteins, and returns the pairs among the variants you named. |
| `pair_genes` | Which of these genes are related, and by what — and, given a gene-to-item list of your own, the item pairs those gene pairs imply. Performs **no** variant-to-gene mapping. |

Use `pair_variants` when the variant belongs to the gene it sits inside.
That is true of a coding variant.

**`pair_variants` takes variants only.** It used to accept gene names and
expand each into the variants inside it, keeping 100 of a gene's ~4,000
by allele frequency without showing you which. Run
`expand_gene_to_variant` first, look at the list, filter it, and pair
that — one visible step instead of one invisible one. `membership` and
`max_variants_per_gene` left with the expansion, and passing either is an
error rather than a silently different answer.

Use `pair_genes` when it does not. A regulatory variant sits in one gene
and acts on another: of the 11,532,453 variant × gene links in this bundle
that carry both kinds of evidence, **91.5% name a gene other than the one
the variant sits in**. If your evidence for the attachment comes from
outside Biofilter — a colocalization, a fine-mapping, a curated
assignment — `pair_variants` cannot use it. It re-derives membership from
coordinates and drops what disagrees, with no error.

`pair_genes` never derives it:

```bash
biofilter --bundle /path/to/bundle report run \
  --report-name pair_genes \
  --input-file my_genes.txt \
  --param gene_identifier=ensembl \
  --param mapping_file=variant_to_gene.tsv \
  --param max_group_size=300 \
  --param min_group_sources=2 \
  --output item_pairs.csv
```

**Say how your genes are identified.** `gene_identifier` is `alias` by
default, which searches every alias — symbols, synonyms, HGNC, Ensembl,
Entrez. Name a code system and the search narrows to it; pass
`entity_id` and it goes to the bundle's own key instead.

This matters more than it looks. 174,410 gene aliases in the bundle are
bare numbers, because that is what an Entrez id is, and **14,335 of those
are also the entity id of a different gene** — Entrez `2` is A2M, entity
`2` is A1BG-AS1. Nothing can tell them apart by looking, so a list from
one source should say which source it came from. It applies to the
mapping as well as the input: one decision about one thing.

The mapping is two columns, gene then item, and the item is **never
read** — which is what lets the same report pair positions, rsIDs, probe
ids or exposures. Biofilter is build 38 and managing build is yours;
because nothing interprets the item, build-37 positions pass through
correctly.

It also owns three rules that are easy to get wrong alone: pairs are
unordered, deduplication is global rather than per gene pair, and an item
attached to both genes does not pair with itself. On one real run the
deduplication alone was 4.3% of the answer.

Both are hypothesis generators, not tests. The pairs are candidates whose
genes share biology, and the supporting columns are there so you can judge
each one — `group_support_count` is a weight for ranking, never a p-value.

## Follow the entity network

| Report | Answers |
|---|---|
| `expand_entity_neighborhood` | What is one hop away from these entities? Takes a mixed list — genes, diseases, proteins — with optional `gene:` style hints, and reports degree overall and by neighbour type. |
| `expand_entity_relationship` | The relationship rows themselves: every link where an input appears on either side, with the related entity named. `scope` controls whether the other side must also be in your input list. |

Use the first to explore, the second to extract.

## A whole cohort

**`aggregate_cohort_variants`** — your cohort's variants, matched against
the bundle and optionally rolled up into biological bins.

It answers three things at once: which of your variants Biofilter knows,
where they sit, and what each sample carries per bin. Binning is optional —
without it you get the match and the placement.

It returns **two tables**: the bins, and the `(variant, bin)` mapping that
says what each bin is made of.

```python
bins = bf.report.run("aggregate_cohort_variants",
                     cohort_file="cohort.vcf.gz", output_grain="bins")

bins.table                            # one row per (bin, sample)
bins.extra_tables["variant_to_bin"]   # what each bin is made of
```

It also writes a `plink --extract` list as an artifact. And it is the report
whose `provenance["warnings"]` most often has something in it — chromosomes
the bundle cannot place, a `maf_cutoff` below what the cohort can observe,
samples with no phenotype. Read them.

## About the bundle itself

| Report | Answers |
|---|---|
| `platform_data_statistics` | What does this bundle hold? Identity, table sizes on disk, entity counts by domain, variant counts by chromosome, relationship counts by group pair, and what each source contributed. |
| `platform_etl_status` | One row per data source: the latest good extract, transform and load, whether each stage ran on the previous one's output, and whether anything is known to be wrong. |
| `platform_etl_packages` | The raw record behind the status: one row per ETL package, with stage, timing, row counts and the hash it carried forward. |

Run `platform_data_statistics` first on any bundle you did not build
yourself. It tells you which chromosomes and which sources are actually in
there, which is what decides whether your question is answerable at all.

It returns **three tables**. The long list of measurements holds most of the
report, but two sections lose the part you would sort by, so they travel as
tables of their own:

```python
stats = bf.report.run("platform_data_statistics")

stats.table                       # the long measurements
stats.extra_tables["storage"]     # table, branch, rows, bytes, files
stats.extra_tables["variants"]    # table, chromosome, rows
```

In the long shape a size is `"3.4 MB"` and a chromosome is a string, so
sorting gives 1, 10, 11, 2. In these two they are integers.

## Reading a result honestly

An empty or partial result has more than one cause, and they are not
interchangeable.

**Per-row status.** Reports that resolve input keep the inputs that
produced nothing, with a status saying why — `not_found` (the name did not
resolve in this bundle), `no_location` (it resolved, but there are no
coordinates for it), `no_variants` (it resolved and nothing met your
criteria). A shorter table is not the same as a negative answer.

**Coverage.** Every result carries a `coverage` block in its provenance
recording which optional tables the bundle did not have and which
chromosomes it spans. If a bundle was built without AlphaMissense, an
AlphaMissense filter silently matches nothing — coverage is where that is
written down.

```python
result.provenance["coverage"]
result.provenance["bundle_id"]
```

**Warnings.** A report that copes with a problem rather than failing records
it, and `provenance["warnings"]` is always present — so an empty list means
nothing went wrong, not that nobody checked.

```python
result.provenance["warnings"]
```

**Bundle identity.** Entity and variant ids are valid only inside the
bundle that produced them. `result.write("out.csv")` saves
`out.csv.provenance.json` beside the file so the result stays traceable to
its build. Pin the bundle, not the id.

**Keeping the whole thing.** `write()` exports one table and flattens what a
spreadsheet cannot hold. For a result you mean to come back to — especially
one of the multi-table reports above — `save()` writes a directory that loses
nothing, and `load()` reads it back:

```python
result.save("./results/cohort_2026_09")
```

See [Reports](reports.md#saving-a-result) for both.

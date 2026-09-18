# Report notebooks (4.3.0)

One notebook per report, next to the report itself in spirit: the code
lives in `biofilter/modules/report/reports/report_<name>.py`, the
reference in `reports_explain/report_<name>.md`, and the worked example
here as `reports__<name>.ipynb`.

The reference answers *what are the parameters*. The notebook answers
*what does this look like when I run it*, which is the question a new
user actually has.

| notebook | for |
| --- | --- |
| `reports__101.ipynb` | start here — how reports work against a bundle |
| `reports__TEMPLATE.ipynb` | copy this when migrating or writing a report |
| `reports__platform_etl.ipynb` | what ran to build this bundle, and whether it holds up |
| `reports__platform_data_statistics.ipynb` | what the bundle holds: sizes, counts, coverage |
| `reports__resolve_entity.ipynb` | **start here for a new input list** — does the bundle know these names |
| `reports__expand_entity_neighborhood.ipynb` | what is one hop from an entity, for a mixed list |
| `reports__expand_entity_relationship.ipynb` | the links themselves, and how a set connects to itself |
| `reports__expand_variant_regulatory.ipynb` | which genes a variant regulates, and in which tissue |
| `reports__expand_gene_to_variant.ipynb` | the variants in a gene — by coordinate or by VEP, and you pick which |
| `reports__pair_variants.ipynb` | candidate variant pairs whose genes share a pathway, disease or protein |
| `reports__pair_genes.ipynb` | which genes are related and by what — and, given your own list, what that implies |
| `reports__aggregate_cohort_variants.ipynb` | your own VCF or .bim against the bundle, and rare-variant bins |
| `reports__annotate_gene.ipynb` | genes |
| `reports__annotate_variant.ipynb` | variants: rsID / position / allele, one row per transcript |
| `reports__annotate_disease.ipynb` | diseases, and the two ClinGen counts |
| `reports__annotate_go.ipynb` | GO terms, and ontology edges vs relationships |
| `reports__annotate_pathway.ipynb` | pathways, and the same biology curated twice |
| `reports__annotate_protein.ipynb` | proteins, and isoform resolution |

## Where results go

Every notebook defines `OUTPUT_DIR` and writes through it:

```
notebooks/templates/outputs/
```

The directory is gitignored. It is resolved from the project root — the
folder holding `.biofilter.toml` — rather than from the working
directory, because VS Code and Jupyter disagree about what a notebook's
working directory is, and a bare filename lands wherever they decided.

## Running them

Every notebook starts with a bundle path to edit:

```python
BUNDLE = "/path/to/biofilter_data/bundles/20260914"
```

A bundle is a directory — point at the directory, not at its `tables/`.

## Writing one

Copy `reports__TEMPLATE.ipynb` and keep its section numbering; people
move between these notebooks and the sections should mean the same thing
in each. Two things earn their place in every one:

**An input that fails to resolve.** How a report reports absence is part
of what a reader needs to know, and it is the part no reference table
conveys.

**Where null differs from zero.** Null usually means "not computed" or
"not applicable"; zero means "computed, and none". Reports that count
things across a bundle built for a subset of chromosomes will return
honest zeros that are easy to misread as biology.

## 4.2.0

The previous set is frozen under
`biofilter_legacy/bf4_420/notebooks/Templates/`. It covers the reports
that still run on the legacy layer, against a PostgreSQL connection.
Those notebooks are replaced, not edited, as each report is migrated.

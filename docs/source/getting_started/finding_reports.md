# Finding a Report

## What a report is

A **report** is a prepared question you can ask the data.

You give it an input — a list of gene symbols, an rsID, a disease name,
sometimes nothing at all — and it gives you back a table. Under the
covers it knows which files to open, how to resolve the names you typed
against the ones the sources use, and how to follow the links between
genes, proteins, pathways, diseases and variants. You do not write
queries and you do not need to know how the data is laid out.

```bash
biofilter --bundle /shared/bundles/bf4_20260912 \
  report run --report-name entity_filter --input APOE,TP53
```

Every report takes the same shape: a name, an input, optional parameters,
and a table out — to your screen, or to a CSV, or straight into a
DataFrame if you are working in Python.

BF4 ships around thirty of them: looking entities up, summarising what is
connected to what, annotating variants, and checking what data the bundle
actually holds. Three ways to find the one you want.

## 1. Browse the catalog

The [Report Catalog](../report_catalog.md) is the full index, grouped by
purpose. Each entry gives you:

- A one-line description of what it does.
- A link to its **Explain Guide** — parameters, output columns, examples.
- A link to a **notebook tutorial** that runs end-to-end.

Use the catalog when you want to see everything available.

## 2. Ask the assistant

For questions in plain language — *"I have a list of genes from a GWAS,
which report shows what pathways they touch?"* — there is a GPT assistant
trained on BF4's reports and terminology:

**[BF4 Assistant](https://chatgpt.com/g/g-6887cf80355c8191ab3f88bbd8955e0d-biofilter-4-assistant)**

Its source — system prompt, FAQ, and a manifest of every report with its
inputs and use cases — lives in the repository's `assistent/` folder.

## 3. Ask Biofilter itself

If you already have it installed:

```bash
biofilter report list
```

And for any one of them:

```bash
biofilter report explain --report-name entity_filter
```

That prints the full guide in your terminal — what it expects, what it
returns, and how to call it.

## Good places to start

Most reports fall into three kinds of work.

**Filtering** — narrowing a list down to what the data recognises or
supports.

| Report | Use it when |
| ------ | ----------- |
| `entity_filter` | You have a list of names and want to know which ones BF4 recognises |
| `gene_to_variant_filtering` | You have genes and want the variants inside them |
| `variant_list_intersect` | You have two variant lists and want what they share |

**Annotation** — attaching what is known to something you already have.

| Report | Use it when |
| ------ | ----------- |
| `annotate_gene` | You want to browse the gene catalog |
| `variant_single_gene_annotation` | You have variants and want their effect on one gene |
| `entity_neighborhood_summary` | You have one entity and want everything connected to it |

**Modeling** — building the sets and pairs an analysis consumes.

| Report | Use it when |
| ------ | ----------- |
| `variant_binning` | You want variants grouped into bins for burden testing |
| `snp_snp_pair_generator` | You need SNP pairs for an interaction scan |
| `entity_relationship_model` | You want the relationship graph around a set of entities |

And one worth running once on any bundle you have just been handed:

| Report | Use it when |
| ------ | ----------- |
| `etl_status` | You want to see which data sources went into this bundle, and when |

It answers "what is actually in here?" — which version of each source,
and whether it loaded.

## Next step

Picked one? [Run your first report](running_reports.md).

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

| Report | Use it when |
| ------ | ----------- |
| `entity_filter` | You have a list of names and want to know which ones BF4 recognises |
| `entity_neighborhood_summary` | You have one entity and want everything connected to it |
| `annotation_master_gene` | You want to browse the gene catalog |
| `etl_status` | You want to see which data sources went into this bundle, and when |

The last one answers "what is actually in here?" — worth running once on
a bundle you have just been given.

## Next step

Picked one? [Run your first report](running_reports.md).

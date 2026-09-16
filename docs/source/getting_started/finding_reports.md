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
  report run --report-name resolve_entity --input APOE --input TP53
```

Repeat `--input` for each value — it is not a comma-separated list.

Every report takes the same shape: a name, an input, optional parameters,
and a table out — to your screen, to a file, or straight into pandas if
you are working in Python.

Biofilter ships 16 of them: resolving names, annotating what you have,
expanding it to what is connected, and checking what the bundle actually
holds. Three ways to find the one you want.

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
biofilter report explain --report-name resolve_entity
```

That prints the full guide in your terminal — what it expects, what it
returns, and how to call it.

## Good places to start

**Start from names you have** — genes, variants, diseases, proteins.

| Report | Use it when |
| ------ | ----------- |
| `resolve_entity` | You have a list of names and want to know which ones Biofilter recognises |
| `annotate_gene` | You want everything known about a set of genes |
| `annotate_variant` | You have rsIDs or positions and want the full annotation |

**Expand to what is connected.**

| Report | Use it when |
| ------ | ----------- |
| `expand_gene_to_variant` | You have genes and want the variants in them, filtered by predicted damage |
| `expand_entity_neighborhood` | You have entities and want everything one hop away |
| `expand_variant_regulatory` | You want to know which genes a variant regulates, and in which tissue |

**Work with a cohort or a set.**

| Report | Use it when |
| ------ | ----------- |
| `aggregate_cohort_variants` | You have a cohort's variants and want them matched and binned |
| `pair_variants` | You need candidate variant pairs whose genes share biology |

And one worth running once on any bundle you have just been handed:

| Report | Use it when |
| ------ | ----------- |
| `platform_data_statistics` | You want to know what is actually in this bundle |

It reports entity counts by domain, variant counts by chromosome, and what
each data source contributed — which is what decides whether your question
is answerable at all before you spend time on it.

## Next step

Picked one? [Run your first report](running_reports.md).

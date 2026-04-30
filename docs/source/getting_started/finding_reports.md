# Finding a Report

BF4 ships with a growing set of reports — entity lookups, neighborhood summaries, variant annotations, ETL status, and more. Three ways to find the one that fits your need.

## 1. Browse the catalog

The [Report Catalog](../report_catalog.md) is the canonical index. It groups reports by purpose (ETL monitoring, entity exploration, variant analysis, modeling) and gives you, for each one:

- A one-line description of what it does.
- A link to its **Explain Guide** (parameters, output columns, examples).
- A link to a **Notebook tutorial** that runs end-to-end.

Use the catalog when you want to scan everything available.

## 2. Ask the GPT assistant

For natural-language questions like _"I have a list of genes from a GWAS — which report should I run to see what pathways they touch?"_, BF4 ships with a GPT assistant kit in the `assistent/` folder of the repository. It contains:

- A system prompt tuned for BF4 terminology.
- A FAQ.
- A manifest of all reports with their inputs, outputs, and use cases.

Link to GPT BF4 Assistent: [BF4 Assistent](https://chatgpt.com/g/g-6887cf80355c8191ab3f88bbd8955e0d-biofilter-4-assistant)

## 3. Use the CLI to introspect

If you already have BF4 installed and just want a quick list:

```bash
biofilter report list
```

For details on a specific report:

```bash
biofilter report explain --report-name entity_filter
```

This prints the full Explain Guide directly in your terminal, including parameters and example invocations.

## Common starting points

If you're new and not sure where to start, these reports are good entry points:

| Report                        | Use it when                                                           |
| ----------------------------- | --------------------------------------------------------------------- |
| `etl_status`                  | You want to see what data is loaded in the database                   |
| `entity_filter`               | You have a list of names and want to check which exist in BF4         |
| `entity_neighborhood_summary` | You have an entity and want to see everything connected to it (1-hop) |
| `entity_relationship_model`   | You want all relationships for a list of entities                     |
| `annotation_master_gene`      | You want to browse the full gene catalog                              |

## Next step

Picked one? [Run your first report](running_reports.md).

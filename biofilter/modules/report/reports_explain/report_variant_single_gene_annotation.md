# Report Tutorial: `variant_single_gene_annotation`

## Purpose

Phase 1 of the single-variant SNP×SNP interaction pipeline.

Given one input variant (chr:pos or rsID), this report:

1. Resolves the variant to a genomic position (via `variant_masters` when an rsID is supplied).
2. Finds the **seed gene** at that position using `entity_locations` (with an optional base-pair window).
3. Expands through a configurable biological group type (Pathways, Diseases, GO, or direct Gene links) to collect **partner genes**.
4. Enriches every partner gene with genomic coordinates, locus group, functional gene groups, and a variant count estimate.

Output: one row per **(seed gene × partner gene)** pair with shared-group information. Resolution failures return a single diagnostic row with a non-null `resolution_status` field so the caller always receives a usable DataFrame.

## Report Name

`variant_single_gene_annotation`

## Parameters (API)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_variant` | `str` | **required** | Variant to query. Accepts `chr:pos` (e.g. `chr19:44904604`, `19:44904604`) or rsID (e.g. `rs429358`). Separators `:`, `;`, `,`, `-`, and space are all accepted for `chr:pos`. |
| `build` | `int` | `38` | Genome assembly build used to look up `entity_locations`. |
| `window_bp` | `int` | `0` | Base-pair window around the position for gene lookup. Only applies to `chr:pos` input. When multiple genes fall inside the window, the **closest** one is selected (distance = 0 if position is inside the gene body; ties broken by smallest locus span). |
| `group_entity_type` | `str` | `"Pathways"` | `EntityGroup` name used for the expansion step. Controls how partner genes are discovered. Use `"Genes"` for direct gene-gene links (1-hop); use `"Pathways"`, `"Diseases"`, `"GO"`, etc. for 2-hop expansion through an intermediary entity. |
| `source_system_filter` | `list[str]` or `str` or `None` | `None` | Restrict which `entity_relationships` are considered by `ETLSourceSystem` name. Accepts a list (`["Reactome", "KEGG"]`) or a single string. When `None`, all sources are included. |

## Input Formats

| Format | Examples |
|---|---|
| Chromosome + position | `chr19:44904604`, `19:44904604`, `chr19-44904604`, `chr19 44904604` |
| rsID | `rs429358`, `RS429358` (case-insensitive) |

Chromosome aliases: `X` → 23, `Y` → 24, `M` / `MT` → 25.

## Output Columns

| Column | Description |
|---|---|
| `resolution_status` | `None` on success; an error code on failure (see below). |
| `seed_input` | The raw input string as provided. |
| `seed_rsid` | Resolved rsID (populated when input was an rsID). |
| `seed_chromosome` | Chromosome of the input variant (integer). |
| `seed_position` | Position of the input variant. |
| `seed_allele_count` | Number of alleles found in `variant_masters` for this rsID position (rsID input only). |
| `group_entity_type` | The `group_entity_type` parameter value used for this run. |
| `seed_gene_entity_id` | Internal entity ID of the seed gene. |
| `seed_gene_symbol` | HGNC symbol of the seed gene. |
| `seed_gene_chromosome` | Chromosome of the seed gene. |
| `seed_gene_start` | Seed gene start position. |
| `seed_gene_end` | Seed gene end position. |
| `seed_gene_locus_group` | Locus group of the seed gene (e.g. `protein-coding gene`). |
| `seed_gene_groups` | Pipe-separated list of functional gene groups the seed gene belongs to. |
| `seed_gene_total_groups` | Total number of shared groups between seed and all partners (summary). |
| `partner_gene_entity_id` | Internal entity ID of the partner gene. |
| `partner_gene_symbol` | HGNC symbol of the partner gene. |
| `partner_gene_chromosome` | Chromosome of the partner gene. |
| `partner_gene_start` | Partner gene start position. |
| `partner_gene_end` | Partner gene end position. |
| `partner_gene_locus_group` | Locus group of the partner gene. |
| `partner_gene_groups` | Pipe-separated list of functional gene groups of the partner gene. |
| `seed_gene_variant_count` | Approximate number of variants in `variant_masters` overlapping the seed gene locus. |
| `partner_gene_variant_count` | Approximate number of variants in `variant_masters` overlapping the partner gene locus. |
| `shared_group_count` | Number of groups (pathways, diseases, etc.) shared between seed and this partner gene. |
| `shared_group_ids` | Pipe-separated internal entity IDs of the shared groups. |
| `shared_group_names` | Pipe-separated names/descriptions of the shared groups. |
| `shared_group_sources` | Pipe-separated data source names for the shared groups. |

### Resolution Status Codes

| Code | Meaning |
|---|---|
| `(None)` | Success. |
| `invalid_input_format` | The `input_variant` string could not be parsed as chr:pos or rsID. |
| `rsid_not_found` | The rsID was not found in `variant_masters`. |
| `configuration_error` | The `EntityGroup` named `"Genes"` is missing from the database. |
| `group_not_found:<name>` | The requested `group_entity_type` was not found. The error message includes the available groups. |
| `gene_not_found` | No gene was found at the resolved position (with the given window and build). |
| `no_partners_found` | A seed gene was found but has no partner genes via the requested group type. |

## Examples

### API

```python
import biofilter as bf

# Positional input — APOE locus, expand via Pathways
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    build=38,
    window_bp=0,
    group_entity_type="Pathways",
)

# rsID input — same variant by rsID, Reactome only
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",
    build=38,
    group_entity_type="Pathways",
    source_system_filter=["Reactome"],
)

# Direct gene-gene links (1-hop), no source filter
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    group_entity_type="Genes",
)

# With a base-pair window — pick the closest gene within 10 kb
df = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="19:44904604",
    window_bp=10000,
    group_entity_type="Diseases",
)
```

### CLI

```bash
# Minimal — positional input
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=chr19:44904604

# rsID with Reactome filter
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=rs429358 \
  --param group_entity_type=Pathways \
  --param source_system_filter=Reactome

# Window + Diseases
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=chr7:117548628 \
  --param window_bp=5000 \
  --param group_entity_type=Diseases
```

## Pipeline Context

This report is **Phase 1** of the single-variant SNP×SNP interaction pipeline:

```
Phase 1 — Gene Discovery (this report)
  input: one variant
  output: seed gene + partner-gene list with shared-group annotation

Phase 2 — Filtered Variant Collection  (planned)
  input: Phase 1 partner-gene list
  output: variants per gene, pre-filtered to coding/functional consequences

Phase 3 — Pair Generation  (planned)
  input: Phase 2 variant sets per gene
  output: variant × variant interaction pairs (seed × partner)
```

Separating gene discovery (tractable ~8 k rows) from variant enumeration prevents the combinatorial explosion that occurs when annotating all variants before filtering.

## Demo Tips

- Start with `chr19:44904604` (APOE rs429358 locus) — well-annotated gene with many Reactome pathways.
- Use `source_system_filter=["Reactome"]` to limit output to a manageable size during demos.
- Check `resolution_status` first; a non-null value explains exactly why the report returned no gene rows.
- `shared_group_count` is the primary signal for SNP×SNP prioritization in later pipeline phases.

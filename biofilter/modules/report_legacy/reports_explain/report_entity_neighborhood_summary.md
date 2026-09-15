# Report Tutorial: `entity_neighborhood_summary`

## Purpose

Resolves a heterogeneous list of inputs (genes, diseases, pathways, proteins, chemicals, GO terms) into entities and returns a **1-hop neighborhood summary** for each, with neighbor counts and lists grouped by entity type.

Useful as both a validation step (does each input resolve correctly?) and an exploratory view (what does each entity touch in the graph?).

## Report Name

`entity_neighborhood_summary`

## Required Parameters (API)

- `items`: `list[str]` — input terms, optionally with a `type:value` prefix.
  - Examples: `"gene:BRCA1"`, `"disease:Alzheimer disease"`, `"APOE"` (no hint)
  - Plain strings without `type:` are resolved across all groups.
- Alternative: `input_data`: `list[str]` — accepted as alias for `items`.

## Optional Parameters

| Parameter | Default | Description |
|---|---|---|
| `match_mode` | `"exact"` | `"exact"` \| `"like"` \| `"fuzzy"` |
| `similarity_threshold` | `80` | Score cutoff (0–100) for `fuzzy` mode |
| `aliases_top_n` | `20` | Limit for `Aliases Top` list per entity |
| `include_all_aliases` | `False` | When `True`, ignores `aliases_top_n` |
| `neighbors_top_n_per_type` | `50` | Limit for the per-type neighbor list columns |
| `emit_not_found_rows` | `False` | When `True`, emits rows with `Resolve Status="not_found"` for unresolved inputs |

## Match Modes

| Mode | Behavior | Engine support |
|---|---|---|
| `exact` | Exact match against `EntityAlias.alias_norm` (case-insensitive) | PostgreSQL, SQLite |
| `like` | Substring match (`%word%` both directions) | PostgreSQL, SQLite |
| `fuzzy` | rapidfuzz `token_sort_ratio` against all aliases in the (optionally scoped) group | PostgreSQL, SQLite |

The report runs entirely client-side for fuzzy matching — no `pg_trgm` or other database extension required. Works on a local SQLite installation.

## Type Hints

When prefixed (`gene:`, `disease:`, etc.), the resolution is **scoped to the matching `EntityGroup`**, avoiding cross-domain collisions. Without a prefix, the input is resolved across all groups.

| Hint | Resolves into group |
|---|---|
| `gene` / `genes` | Genes |
| `disease` / `diseases` | Diseases |
| `chemical` / `chemicals` | Chemicals |
| `pathway` / `pathways` | Pathways |
| `protein` / `proteins` | Proteins |
| `go` / `go_terms` / `goterms` | GO Terms |

## Output Columns

### Base columns (always present)

| Column | Description |
|---|---|
| `Input Word` | Original input string |
| `Input Type Hint` | The `type:` prefix used (or `None`) |
| `Resolver Mode` | The `match_mode` used for this run |
| `Entity ID` | Resolved entity (or `None` for `not_found`) |
| `Entity Type` | Lowercased singular form (`gene`, `disease`, etc.) |
| `Exact Match` | `True` when `input.lower().strip() == matched_name.lower().strip()` |
| `Matched Name` | The actual `alias_value` that matched the input |
| `Primary Alias` | The entity's canonical/primary alias |
| `Aliases Top` | JSON list of top-N aliases for the entity |
| `Alias Count` | Total alias count for the entity (full count, not truncated) |
| `Degree Total (1-hop)` | Number of distinct 1-hop neighbors |
| `Degree By Type (1-hop)` | JSON object: `{group_name: count}` |
| `Resolve Status` | `resolved` \| `not_found` |
| `Resolve Method` | Echo of the match mode used |
| `Resolve Score` | `1.0` for exact; `None` for like; rapidfuzz score for fuzzy |

### Dynamic per-type columns

One column per `EntityGroup.name` in the database (`Genes`, `Proteins`, `Pathways`, `Diseases`, `Chemicals`, `GO Terms`, …). Each cell is a JSON list of neighbor primary names of that type.

## Examples

### API — mixed input types

```python
df = bf.report.run(
    "entity_neighborhood_summary",
    items=[
        "gene:BRCA1",
        "disease:Alzheimer disease",
        "pathway:DNA repair",
        "APOE",  # no type hint — searches all groups
    ],
    match_mode="exact",
    aliases_top_n=10,
    neighbors_top_n_per_type=20,
    emit_not_found_rows=True,
)
```

### API — fuzzy with custom threshold

```python
df = bf.report.run(
    "entity_neighborhood_summary",
    items=["gene:BRCA1", "disease:alzheimers"],
    match_mode="fuzzy",
    similarity_threshold=70,
)
```

### API — substring search across many pathways

```python
df = bf.report.run(
    "entity_neighborhood_summary",
    items=[f"pathway:{name}" for name in pathway_list],
    match_mode="like",
)
```

### CLI

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name entity_neighborhood_summary \
  --input "gene:BRCA1" \
  --input "disease:Alzheimer disease" \
  --param match_mode=exact \
  --param emit_not_found_rows=true \
  --output neighborhood.csv
```

## Notes and Caveats

- **One row per (input, entity) pair.** When an input resolves to multiple entities (genuine ambiguity), the report emits one row per entity. When the same entity has multiple aliases that match the input (common in `like` mode), they are collapsed into a single row.
- **Type hint scope is recommended** for ambiguous strings. Searching `"BRCA1"` without a hint may match across Genes and any other group where `BRCA1` happens to be a synonym.
- **Fuzzy threshold tuning.** The `token_sort_ratio` scorer penalizes length differences. Searching `"alzheimer"` (single token) against `"Alzheimer disease"` (two tokens) gives ~67%. For substring-style queries, lower the threshold (60–70) or pre-filter with `like` mode first.
- **Engine-agnostic.** No `pg_trgm` or other PostgreSQL-only extensions are used. Works equally on SQLite for local development.
- **Neighborhood counts after truncation.** `Degree Total (1-hop)` reflects the actual distinct neighbor count; `neighbors_top_n_per_type` only truncates the displayed list, not the count.

## Recommended Demo Columns

- `Input Word`
- `Entity ID`
- `Exact Match`
- `Matched Name`
- `Primary Alias`
- `Degree Total (1-hop)`
- `Degree By Type (1-hop)`
- The dynamic per-type columns relevant to the use case

# resolve_entity

Does the bundle know these names, and unambiguously?

> Called `entity_filter` before 4.3.0. The name changed because it
> describes a resolution step, not a filter: filtering reduces a set,
> this expands one — an ambiguous name comes back as several rows.

```bash
biofilter report run --report-name resolve_entity \
    --input TP53 --input BRCA1 --input NOT_A_GENE \
    --output lookup.csv
```

Use it before any other report: it tells you which of your inputs will
resolve, which are ambiguous, and which the bundle has never heard of.

## One row per match, not per input

Unlike the annotation reports, this one fans out. An input matching three
entities gives three rows. That is the answer — the report's job is to
show the ambiguity, not pick a winner.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | names, symbols, or codes |
| `match_mode` | `exact` | `exact`, `like`, or `fuzzy` |
| `group_filter` | none | restrict to one entity group, e.g. `Genes` |
| `similarity_threshold` | `80` | fuzzy only: minimum score, 0–100 |

### Match modes

| mode | matches when | cost |
| --- | --- | --- |
| `exact` | the alias equals the input, case-insensitively | an equality join |
| `like` | the input occurs **inside** the alias | a scan with a substring test |
| `fuzzy` | Jaro-Winkler similarity ≥ the threshold | a scan with a scored test |

`like` is one-directional on purpose: the input inside the alias, not the
reverse. Matching an alias inside an input would make every
one-character alias match every input containing that character.

## Columns

| column | meaning |
| --- | --- |
| `input_original` | the value you gave |
| `input` | the alias it matched, which may differ in case or form |
| `is_primary` | whether that alias is the entity's preferred name |
| `entity_id` | BF4 entity id — **scoped to this bundle** |
| `primary_name` | the entity's preferred name |
| `group_id`, `group_name` | which kind of entity it is |
| `has_conflict` | the entity was built from sources that disagreed |
| `is_active`, `is_deactive` | curation status, and its negation |
| `data_source_id` | which source contributed the alias |
| `similarity_score` | fuzzy only; null in the other modes |
| `observation` | `multiple matches`, `not found`, or empty |

## Reading the result

**`observation = 'multiple matches'` is about the name, not your search.**
It means that alias belongs to more than one entity, so resolving it
requires a decision you have to make. A broad `like` search returning
two hundred rows is not ambiguous — the row count already told you that.

**`not found` rows are kept deliberately.** Dropping them would leave no
way to tell "the bundle does not know this name" from "you did not ask
about it".

**`has_conflict` is worth a look.** It flags an entity whose sources
disagreed during the build. It does not make the entity wrong, but a
result that depends on one is worth a second look.

## What changed in the migration

**Fuzzy matching is Jaro-Winkler, scored by DuckDB.** The relational
version used `rapidfuzz.fuzz.token_sort_ratio`, which meant pulling all
912 thousand aliases into Python to score them, and an ImportError
wherever that optional dependency was missing.

Scores are no longer comparable between the two versions. Both are 0–100
and the default threshold is still 80, but Jaro-Winkler rewards a shared
prefix and does not reorder words, so a multi-word name scores
differently. Check the threshold against your own inputs rather than
assuming the old one transfers.

**`similarity_score` is always present.** The relational version added
the column only in fuzzy mode, so the result's shape depended on a
parameter.

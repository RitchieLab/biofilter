# platform_etl_status

What ran to produce this bundle, and whether it holds up. One row per
data source.

```bash
biofilter report run --report-name platform_etl_status --output status.csv
```

> Called `etl_status` before 4.3.0. A platform report — it describes the
> bundle, not the biology in it, and takes no input.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `only_active` | `false` | restrict to active sources and systems |
| `source_system` | none | one or more systems, e.g. `HGNC` |
| `data_sources` | none | one or more sources, e.g. `hgnc` |

## The column to read first: `pipeline_state`

| state | meaning | `pipeline_ok` |
| --- | --- | --- |
| `ok` | every required stage ran, and each on the previous one's output | true |
| `unverifiable` | every stage ran, but no hashes to prove they belong together | true |
| `misaligned` | a stage ran on something other than the previous stage's output | false |
| `incomplete` | a required stage is missing | false |
| `never_run` | no packages at all for this source | false |

`pipeline_ok` means **nothing is known to be wrong**, which is weaker
than "everything is proven right". `unverifiable` is the gap between the
two.

## Why this replaced a simple boolean

The relational version reported `pipeline_ok = False` for **51 of a real
bundle's 68 sources**, and not one of them was broken. Three different
situations were collapsed into one word:

**The variant branch has no load stage.** It writes parquet straight from
transform (ADR-003 §2.3), so `load_status` is null by design. That
accounted for all 51. A report that flags a design decision as a failure
teaches people to ignore it.

**Some DTPs produce no hash.** The relationship DTPs read database state
rather than a downloaded file, so there is nothing to carry forward and
alignment cannot be shown either way. That is `unverifiable`, and
`transform_aligned` is **null** rather than false — false reads as "this
is wrong" rather than "this is unproven".

**Two sources had genuinely never run.** `chebi` and `omim`, which is
what the report should have been drawing attention to all along.

## What "aligned" means

Each ETL stage is its own package row, and the digest of the extract's
output is carried forward: it appears as `extract_hash` in the extract's
package, as `transform_hash` in the transform's, and as `load_hash` in
the load's. Alignment means a stage ran on the previous one's output, not
that two unrelated digests happen to match.

`platform_etl_packages` shows the same hash in all three rows.

## `latest_error`

The most recent failure in a source's history, whether or not it was
later retried successfully — so `pipeline_state = 'ok'` and a non-null
`latest_error` together mean "it worked, but not on the first try".

The message is composed rather than read from the package's `note`,
because every failed package in a real bundle has a null note. Reporting
that null hid the failures entirely.

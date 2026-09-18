# AG Report - Report Operations in Biofilter (CLI/API/Explain Guides)

The full report workflow: find the right one, run it, read what comes back.

Audience: someone with a bundle. If you have not pointed Biofilter at one yet,
start with `ag_start.md`.

---

## 1) Goal

Reports are the read interface. Each takes a list of things you have — gene
symbols, rsIDs, disease names, a cohort's variants — and returns a table.

You do not write queries and you do not need to know how the data is laid out.

---

## 2) How a report works

A report writes SQL against the bundle and returns an Arrow table, which the
manager wraps with a record of how it was produced. Three consequences you
will notice:

- **Input is joined, not interpolated.** Your list is registered as a relation
  and joined, which is why a ten-thousand-value filter is as fast as a
  ten-value one.
- **Each execution is isolated.** Two reports in the same process cannot
  collide.
- **The result carries its origin.** Which bundle, which parameters, and what
  the bundle was missing.

Each report ships three things besides the code:

| Artifact | Where |
|---|---|
| the explain guide | `biofilter/modules/report/reports_explain/report_<name>.md` |
| a worked notebook | `notebooks/templates/reports__<name>.ipynb` |
| its declared needs | `requires` / `optional` on the class — see §9 |

---

## 3) Discover and inspect

```bash
biofilter report list                  # names
biofilter report list --verbose        # names, descriptions, modules

biofilter report explain --report-name annotate_variant
biofilter report example-input --report-name annotate_variant
biofilter report available-columns --report-name annotate_variant
biofilter report run --report-name annotate_variant --params-template
```

`explain` prints the report's full guide and is the authority for that report.
This document describes the workflow; the guide describes the report.

Discovery needs no bundle — it asks about the installed package. Only `run`
reads data.

`report refresh` rebuilds the index after a report is added. You will not need
it otherwise.

---

## 4) Run (CLI)

```bash
biofilter report run \
  --report-name annotate_gene \
  --input TP53 --input BRCA1 \
  --output genes.csv
```

With an explicit bundle:

```bash
biofilter --bundle /path/to/bundles/20260914 \
  report run --report-name annotate_gene --input TP53 --output genes.csv
```

`--output` takes its format from the extension — `.csv`, or `.parquet` to keep
the provenance inside the file. Writing also produces
`<output>.provenance.json` beside it.

`--report-name` also accepts the shorter `--name`.

---

## 5) Inputs vs params (important rule)

They are separate channels, and mixing them is an error rather than a guess.

**Input — the records you are asking about.** One channel at a time:

```bash
--input TP53 --input BRCA1                      # repeat the flag
--input-file genes.txt                          # one value per line
--input-file cohort.csv --input-column symbol   # a CSV column
```

**There is no comma-separated form.** `--input "TP53,BRCA1"` is one value
named `TP53,BRCA1`, and it will not match anything.

**Params — everything else:** filters, modes, thresholds.

```bash
--param mapping=annotation
--param af_max=0.01
```

Do not pass `input_data`, `items` or `input_path` through `--param`.

---

## 6) Parameter parsing

Values are coerced in this order: `true`/`false`, `null`/`none`, then JSON,
then Python literal, else a plain string.

```bash
--param most_severe_only=true          # boolean
--param af_max=0.01                    # number
--param impact_filter='["HIGH","MODERATE"]'   # list, as JSON
--param consequence_type_filter=@./terms.txt  # @ reads from a file
--param note=@@literal_at_sign                # @@ escapes a leading @
```

For anything longer, pass the whole option set at once:

```bash
--params-json '{"mapping":"annotation","af_max":0.01}'
--params-file ./params.yaml            # .json, .yml or .yaml
```

`--params-template` prints what a report accepts, filled with its own example
values — the fastest way to see the surface.

---

## 7) Run via API (notebook / Python)

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundles/20260914")

result = bf.report.run(
    "expand_gene_to_variant",
    input_data=["BRCA1", "CHEK2"],
    mapping="annotation",
    impact_filter="HIGH",
    af_max=0.01,
)

df = result.to_pandas()
result.write("candidates.csv")
```

`run()` returns a result object, not a DataFrame. `.to_pandas()` gives you
one; the wrapper is what carries `.provenance`, `.num_rows` and `.columns`.

---

## 8) The reports

Seventeen, in six families. `biofilter report list --verbose` is always the
authority for what your install has.

| Family | Reports |
|---|---|
| `annotate_*` | `annotate_gene`, `annotate_variant`, `annotate_protein`, `annotate_disease`, `annotate_pathway`, `annotate_go` |
| `expand_*` | `expand_gene_to_variant`, `expand_variant_regulatory`, `expand_entity_neighborhood`, `expand_entity_relationship` |
| `resolve_*` | `resolve_entity` |
| `pair_*` | `pair_genes`, `pair_variants` |
| `aggregate_*` | `aggregate_cohort_variants` |
| `platform_*` | `platform_data_statistics`, `platform_etl_status`, `platform_etl_packages` |

By the question instead:

| You have | Start with |
|---|---|
| names that may not match | `resolve_entity` |
| genes, want what is known | `annotate_gene` |
| rsIDs or positions | `annotate_variant` |
| genes, want the variants in them | `expand_gene_to_variant` |
| variants, want what they regulate | `expand_variant_regulatory` |
| genes, want pairs that share biology | `pair_genes` |
| variants, want candidate pairs | `pair_variants` |
| a cohort | `aggregate_cohort_variants` |
| a bundle you do not know | `platform_data_statistics` |

The full index, organized by question, is in
`docs/source/report_catalog.md`.

---

## 9) Read the result honestly

A short table is not the same as a negative answer.

**Per-row status.** Reports that resolve input keep the inputs that produced
nothing, with a reason:

| Status | Means |
|---|---|
| `not_found` | the name did not resolve in this bundle |
| `no_location` | it resolved, but there are no coordinates for it |
| `no_variants` | it resolved and nothing met your criteria — a real negative |

**Coverage.** Each report declares what it needs:

- `requires` — tables it cannot work without. Checked before the query runs,
  so a bundle built without GTEx says so in one line.
- `optional` — tables it uses when present. Their absence is not an error,
  which is the risk: the columns come back null, and a null because the source
  was never built looks exactly like a null answer.

So every result records which optional tables were missing, and which
chromosomes the bundle spans:

```python
result.provenance["coverage"]
result.provenance["bundle_id"]
result.provenance["version_mismatch"]   # None unless built by another release
```

**Ids are bundle-scoped.** Valid only inside the bundle that produced them,
and a stale id still resolves — to a different gene, with no error. Pin the
bundle, not the id.

---

## 10) Troubleshooting

| Symptom | Do this |
|---|---|
| "report not found" | `biofilter report list --verbose`, use an exact name. 4.2.x names (`entity_filter`, `etl_status`, `annotation_master_*`, `variant_binning`) are gone. |
| "input conflict" | keep records in `--input`/`--input-file`; do not also send `input_data` via `--param` |
| parameter rejected | `--params-template` first; use `--params-json` for anything structured; watch shell quoting |
| `<name> does not carry: <tables>` | the bundle lacks a source this report requires — `platform_data_statistics` shows what it has |
| explain shows nothing | check `reports_explain/report_<name>.md` exists and matches the module name |
| a column is entirely null | check `provenance["coverage"]` before concluding the answer is null |
| everything is slow | first run on a cold cache reads from disk; a variant-scale join over billions of rows is seconds, not minutes — if it is minutes, check you are not on slow network storage |

More detail:

```bash
biofilter --debug report run --report-name <name> --input <value>
```

---

## 11) LLM Assistant Playbook

1. **Discover** — `report list --verbose`. Never recommend a report name
   without confirming it here; the 4.2.x names are gone and several are
   plausible-sounding.
2. **Understand** — `report explain --report-name <name>` and
   `--params-template` before composing a command.
3. **Execute** — start minimal, add `--input` and `--param` progressively,
   `--output` when the user wants a file.
4. **Interpret** — check status values and `provenance["coverage"]` before
   reporting a result as empty. Say which bundle produced it.
5. **Defer** — building a bundle and running the ETL are maintainer tasks.
   Name the guide and the cost; do not improvise a pipeline.

---

## 12) Authoring a new report

Out of scope here — this guide is about running them. See
`docs/source/technical/developer_extensions.md`, which covers the class
contract, the SQL helpers, and the four artifacts that ship together.

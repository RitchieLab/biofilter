# BF4 Example Notebooks



<!-- ===== SOURCE FILE: notebooks/templates/README.md ===== -->

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
| `reports__resolve_entity.ipynb` | **start here for a new input list** — does the bundle know these names |
| `reports__annotate_gene.ipynb` | genes |
| `reports__annotate_disease.ipynb` | diseases, and the two ClinGen counts |
| `reports__annotate_go.ipynb` | GO terms, and ontology edges vs relationships |
| `reports__annotate_pathway.ipynb` | pathways, and the same biology curated twice |
| `reports__annotate_protein.ipynb` | proteins, and isoform resolution |

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



<!-- ===== SOURCE FILE: notebooks/templates/reports__101.ipynb.md ===== -->

<h1>📘 Biofilter — Reports 101 (4.3.0)</h1>

How reports work now that they read a **bundle** instead of a database.

A bundle is a folder: parquet files plus a `manifest.json` that says what
is in them. It is immutable — the build that produced it is the version
of the data — and reports only ever read it.

This notebook covers the whole report API. Each individual report has
its own notebook in this folder.

## What changed from 4.2.0

| | 4.2.0 | 4.3.0 |
| --- | --- | --- |
| data | PostgreSQL, or a parquet bundle through an ORM bridge | the bundle, read natively |
| `bf.report.run()` returns | a DataFrame | a `ReportResult` |
| provenance | lost on export | written beside the file |

There is no relational path any more: a report reads a bundle or it does
not run.

### 1. Open a bundle

```python
from biofilter import Biofilter

# A bundle is a directory. Point at the directory, not at its tables/.
BUNDLE = "/path/to/biofilter_data/bundles/20260914"

bf = Biofilter(bundle=BUNDLE, debug_mode=False)
bf
```

On the command line the same thing is `--bundle`:

```bash
biofilter --bundle /path/to/bundles/20260914 report list
```

Or put the path in `.biofilter.toml` once and drop the argument:

```toml
[database]
bundle = "./biofilter_data/bundles/20260914"
```

A relative path there is relative to the config file, not to where you
are, so it works from a notebook in a subdirectory. With that set,
`Biofilter()` and `biofilter report run ...` both find it, and
`biofilter config show` prints which bundle is in effect.

### 2. What reports exist

```python
import pandas as pd

reports = bf.report.list()
df = pd.DataFrame(reports)

pending = bf.report.pending_migration()
print(f"{len(df)} reports available; {len(pending)} still awaiting rewrite")
df[["name", "description"]]
```

Reports are being rewritten for the bundle one at a time.
`bf.report.pending_migration()` lists the ones that have not moved yet —
they live in `biofilter/modules/report_legacy/reports/` as reference for
whoever rewrites them, and **cannot be run**. Most could not run against
a 4.3.0 bundle anyway: they select columns the bundles stopped carrying.

```python
bf.report.pending_migration()[:5]
```

### 3. Ask a report about itself

```python
report_name = "annotate_gene"

print("columns:")
print(bf.report.available_columns(report_name))

print("\nexample input:")
print(bf.report.example_input(report_name))
```

```python
print(bf.report.explain(report_name))
```

### 4. Run one

```python
result = bf.report.run(report_name, input_data=["TP53", "BRCA1"])

# Native reports return a ReportResult, not a DataFrame.
print(type(result).__name__)
print(f"{result.num_rows} rows")

df = result.to_pandas()
df[["input_value", "gene_symbol", "hgnc_id", "chromosome", "status"]]
```

### 5. Provenance — which bundle produced this

`entity_id` and `variant_id` are **scoped to one bundle**. The same
integer means a different gene in the next build, and a stale id still
resolves — to the wrong row. Carrying the bundle id alongside the data is
what makes that detectable.

```python
result.provenance
```

### 6. Export

```python
# CSV, with a genes.csv.provenance.json written beside it.
written = result.write("genes.csv")
for path in written:
    print(path)
```

```python
# Parquet instead: the provenance travels inside the file's metadata,
# and list columns stay real lists rather than JSON strings.
result.write("genes.parquet")
```

### 7. Reading a result someone else produced

The sidecar is what lets you answer "where did this come from" months
later, and `build_record.json` in the bundle has the per-source detail
behind that id — which DTP, which version, which source URL.

```python
import json

with open("genes.csv.provenance.json") as fh:
    print(json.dumps(json.load(fh), indent=2))
```

---

## Working directly, without the facade

Reports are the packaged questions. For an ad-hoc one, open the bundle
and write SQL — it is the same engine the reports use.

```python
from biofilter.modules.report import Bundle

with Bundle.open(BUNDLE) as bundle:
    print(f"bundle {bundle.bundle_id}, {len(bundle.tables)} tables")

    # Every table is a view; query it as SQL and get Arrow back.
    out = bundle.con.execute("""
        SELECT g.name AS gene_group, count(*) AS genes
        FROM gene_masters gm
        JOIN gene_group_memberships m ON m.gene_id = gm.id
        JOIN gene_groups g ON g.id = m.group_id
        GROUP BY 1 ORDER BY genes DESC LIMIT 10
    """).to_arrow_table()

display(out.to_pandas())
```

### What is in this bundle

`bundle.tables` is the manifest, resolved: one entry per logical table,
with the files behind it. A partitioned table is many files and one view.

```python
with Bundle.open(BUNDLE) as bundle:
    inventory = pd.DataFrame(
        [
            {
                "table": t.name,
                "rows": t.rows,
                "files": len(t.files),
                "branch": t.branch,
            }
            for t in bundle.tables.values()
        ]
    ).sort_values("rows", ascending=False)

inventory.head(15)
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__TEMPLATE.ipynb.md ===== -->

<h1>📄 Biofilter — Report notebook template</h1>

Copy this file to `reports__<report_name>.ipynb` and fill it in. Every
report gets one, and it ships with the report — the code lives in
`biofilter/modules/report/reports/report_<name>.py`, the reference in
`reports_explain/report_<name>.md`, and the worked example here.

Keep the section numbering: people move between these notebooks and the
sections should mean the same thing in each.

Delete this cell when you copy.

<h1>🧬 Biofilter — Report: <code>&lt;report_name&gt;</code></h1>

One paragraph: what question this answers, for whom, and what one row of
the output represents.

### 1. Open a bundle

```python
from biofilter import Biofilter

BUNDLE = "/path/to/biofilter_data/bundles/20260914"
REPORT = "<report_name>"

bf = Biofilter(bundle=BUNDLE, debug_mode=False)
bf
```

### 2. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

```python
print(bf.report.explain(REPORT))
```

### 3. Run it

Show the smallest input that demonstrates the report, and include one
input you expect to **fail** to resolve — how a report reports absence
is part of what a reader needs to know.

```python
result = bf.report.run(REPORT, input_data=[...])

df = result.to_pandas()
print(f"{result.num_rows} rows from bundle {result.provenance['bundle_id']}")
df.head()
```

### 4. Reading the result

Explain the columns a reader could misread. Two that come up in every
report:

- **What `status` values mean**, and whether unresolved inputs are kept.
- **Where null differs from zero.** Null usually means "not computed" or
  "not applicable"; zero means "computed, and none". Say which is which.

If the report has list-valued columns, note that CSV writes them as JSON
while parquet and DataFrames keep them as lists.

```python
df[["...", "status", "note"]]
```

### 5. Parameters worth knowing

One cell per parameter that changes the answer rather than the shape —
especially any that trade completeness for speed.

```python
alternative = bf.report.run(REPORT, input_data=[...], some_param=False)
alternative.to_pandas().head()
```

### 6. At scale

If the report supports `__ALL__` or accepts large inputs, show it with a
timing, and say what the expensive part is.

```python
import time

started = time.perf_counter()
everything = bf.report.run(REPORT, input_data="__ALL__")
print(f"{everything.num_rows:,} rows in {time.perf_counter() - started:.1f}s")
```

### 7. Export

Always show both, and say what the provenance sidecar is for: ids in a
result are scoped to the bundle that produced them.

```python
for path in result.write("<report_name>.csv"):
    print(path)
```

### 8. The same thing on the command line

```bash
biofilter --bundle /path/to/bundles/20260914 report run \
    --report-name <report_name> \
    --input ... \
    --output out.csv
```

### 9. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__annotate_disease.ipynb.md ===== -->

<h1>🩺 Biofilter — Report: <code>annotate_disease</code></h1>

Everything the bundle knows about a list of diseases: MONDO record, groups, cross-references grouped by the source that issued them, and how many genes ClinGen links to the disease.

### 1. Open a bundle

```python
from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_disease"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)
bf
```

### 2. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

```python
print(bf.report.explain(REPORT))
```

### 3. Run it

Diseases resolve by MONDO id, by label, or by a cross-reference code from
any source the bundle carries.

```python
diseases = [
    "MONDO:0007254",     # breast cancer, by id
    "Leigh syndrome",    # by label
    "NOT_A_DISEASE",     # kept, with status='not_found'
]

result = bf.report.run(REPORT, input_data=diseases)
df = result.to_pandas()
df[["input_value", "disease_id", "disease_label", "omic_status", "status"]]
```

### 4. The two ClinGen numbers

`clingen_gene_count` counts **distinct genes**; `clingen_relationship_count`
counts assertions. One gene supported by three lines of evidence is one
gene and three assertions — when they diverge, that is why.

```python
df[[
    "input_value",
    "clingen_gene_count",
    "clingen_relationship_count",
    "total_entity_relationships",
]]
```

ClinGen's share is not the total. A disease can have thousands of
relationships — MONDO's own hierarchy, Reactome, BioGRID — and no ClinGen
genes at all. That means nobody has curated a gene–disease assertion for
it, not that it has no genetic basis.

### 5. Cross-references, grouped by who issued them

```python
for _, row in df[df["status"] == "ok"].iterrows():
    print(row["disease_label"])
    for entry in row["xref_ids_by_source"]:
        print(f"  {entry['source']:<12} {list(entry['ids'])[:4]}")
    print("  groups:", list(row["disease_groups"]))
    print()
```

### 6. Every disease in the bundle

```python
import time

started = time.perf_counter()
everything = bf.report.run(REPORT, input_data="__ALL__", include_clingen_summary=False)
catalog = everything.to_pandas()

print(f"{everything.num_rows:,} diseases in {time.perf_counter() - started:.1f}s")
catalog["status"].value_counts()
```

### 7. Export

CSV by default, with a `.provenance.json` beside it naming the bundle the ids came from.

```python
for path in result.write("annotate_disease.csv"):
    print(path)
```

### 8. The same thing on the command line

```bash
biofilter report run --report-name annotate_disease \\
    --input ... \\
    --output out.csv
```

### 9. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("unresolved inputs:", int((df["status"] == "not_found").sum()))
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__annotate_gene.ipynb.md ===== -->

<h1>🧬 Biofilter — Report: <code>annotate_gene</code></h1>

Everything the bundle knows about a list of genes, one row per input:
canonical IDs, HGNC metadata, build 38 coordinates, relationship counts
by related entity group, and the number of variants inside the gene's
range.

Reads the bundle natively (ADR-004). Accepts symbols, aliases, synonyms
or cross-reference codes, matched case-insensitively.

### 1. Open a bundle

```python
from biofilter import Biofilter

BUNDLE = "/Users/andrerico/Works/Sys/biofilter_430/biofilter_data/bundles/20260914" # change this to the path of your biofilter_data bundle
REPORT = "annotate_gene"

bf = Biofilter(bundle=BUNDLE, debug_mode=False)
bf
```

### 2. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

```python
print(bf.report.explain(REPORT))
```

### 3. Run it

Any of these resolve to the same gene — symbol, synonym, or code:

```
TP53   p53   HGNC:11998   ENSG00000141510   7157
```

```python
input_genes = [
    "TP53",
    "BRCA1",
    "ENSG00000146648",   # EGFR, by Ensembl id
    "NOT_A_GENE",        # kept in the output, with status='not_found'
]

result = bf.report.run(
    REPORT,
    input_data=input_genes,
    include_relationships=True,
    include_variant_summary=True,
    emit_not_found_rows=True,
)

df = result.to_pandas()
print(f"{result.num_rows} rows from bundle {result.provenance['bundle_id']}")
df[["input_value", "input_matched_alias", "gene_symbol", "entity_id", "status"]]
```

### 4. Reading the result

`status` is the first column to look at.

| value | meaning |
| --- | --- |
| `ok` | resolved, with a gene record and build 38 coordinates |
| `partial` | resolved, but something is missing — `note` says what |
| `not_found` | the bundle has no gene entity for this input |

Unresolved inputs are **kept on purpose**. Dropping them would leave no
way to tell "absent from this bundle" from "never asked for".

```python
df[["input_value", "status", "note"]]
```

#### Identity and coordinates

```python
df[[
    "input_value",
    "gene_symbol",
    "hgnc_id",
    "ensembl_id",
    "entrez_id",
    "hgnc_status",
    "omic_status",
    "gene_locus_group",
    "build",
    "chromosome",
    "start_position",
    "end_position",
]]
```

#### Lists: groups, relationships, other aliases

Three columns hold lists rather than scalars. In a DataFrame and in
parquet they are real lists; exported to CSV they are written as JSON so
one cell can hold them.

```python
for _, row in df[df["status"] != "not_found"].iterrows():
    print(row["gene_symbol"])
    print("  gene groups :", list(row["gene_groups"]))
    print("  relationships:", row["total_entity_relationships"], "total")
    for entry in row["entity_relationships_by_group"]:
        print(f"      {entry['group_name']:<12} {entry['count']:>6}")
    print("  other aliases:", list(row["other_aliases"])[:6])
    print()
```

Relationships are counted **in both directions**: a gene appearing on
either side of a relationship counts it, grouped by what is on the other
side.

#### `variant_count_in_gene_range`: null is not zero

| value | meaning |
| --- | --- |
| a number | the range was searched, and held that many variants |
| `0` | the range was searched and held none |
| `NaN` / null | the count was **not made** |

Null happens when the gene has no build 38 range, or when
`include_variant_summary=False`.

⚠️ A bundle built for a subset of chromosomes returns `0` for every gene
outside them. That is true of the bundle, not of the genome — check what
the bundle covers before reading a zero as biology.

```python
df[["input_value", "chromosome", "start_position", "end_position",
    "variant_count_in_gene_range"]]
```

### 5. Lighter modes

`include_relationships` and `include_variant_summary` are the two
expensive sections. Turning them off is what makes whole-catalog mode
comfortable.

```python
fast = bf.report.run(
    REPORT,
    input_data=input_genes,
    include_relationships=False,
    include_variant_summary=False,
)

fast.to_pandas()[["input_value", "gene_symbol", "hgnc_id", "chromosome", "status"]]
```

### 6. Every gene in the bundle

`input_data="__ALL__"` annotates every gene entity instead of a list.

```python
import time

started = time.perf_counter()
everything = bf.report.run(REPORT, input_data="__ALL__")
elapsed = time.perf_counter() - started

catalog = everything.to_pandas()
print(f"{everything.num_rows:,} genes in {elapsed:.1f}s")
print(catalog["status"].value_counts().to_dict())
```

```python
# Genes with no build 38 location are the 'partial' ones, and they are
# also exactly the rows whose variant count is null.
partial = catalog[catalog["status"] == "partial"]
print(f"{len(partial):,} without a build 38 location")
print(f"{catalog['variant_count_in_gene_range'].isna().sum():,} with a null variant count")
```

```python
# What the bundle actually covers, which is what a zero above means.
covered = catalog.dropna(subset=["variant_count_in_gene_range"])
covered.groupby("chromosome")["variant_count_in_gene_range"].agg(
    genes="size", with_variants=lambda s: int((s > 0).sum())
).sort_values("with_variants", ascending=False).head(10)
```

### 7. Export

CSV is the default. The `.provenance.json` written beside it records
which bundle the ids came from — necessary, because `entity_id` means a
different gene in the next build.

```python
for path in everything.write("annotate_gene.csv"):
    print(path)
```

```python
# Parquet keeps the list columns as lists, and carries the provenance in
# the file's own metadata.
everything.write("annotate_gene.parquet")
```

### 8. The same thing on the command line

```bash
biofilter --bundle /path/to/bundles/20260914 report run \
    --report-name annotate_gene \
    --input TP53 --input BRCA1 \
    --param include_variant_summary=false \
    --output genes.csv
```

`--input-file genes.txt` takes one value per line.

### 9. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("unresolved inputs:", int((df["status"] == "not_found").sum()))
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__annotate_go.ipynb.md ===== -->

<h1>🧭 Biofilter — Report: <code>annotate_go</code></h1>

Everything the bundle knows about a list of Gene Ontology terms: id, name and namespace, where the term sits in the ontology, and what else in the bundle is linked to it.

### 1. Open a bundle

```python
from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_go"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)
bf
```

### 2. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

```python
print(bf.report.explain(REPORT))
```

### 3. Run it

⚠️ **Terms resolve by id, not by name.** The bundle carries GO codes as
aliases but not term names, so `GO:0006915` resolves and
`apoptotic process` does not. That matches the report this replaces;
changing it would be an ETL change, not a report one.

```python
terms = [
    "GO:0006915",    # apoptotic process
    "GO:0008150",    # biological_process, near the root
    "GO:9999999",    # kept, with status='not_found'
]

result = bf.report.run(REPORT, input_data=terms)
df = result.to_pandas()
df[["input_value", "go_id", "go_name", "go_namespace", "status"]]
```

### 4. Where the term sits in the ontology

`go_parent_count` and `go_child_count` are ontology edges. A term near the
root has many children and few parents; a leaf is the reverse.

```python
df[[
    "go_id",
    "go_name",
    "go_parent_count",
    "go_child_count",
    "go_parent_relation_types",
    "go_child_relation_types",
]]
```

```python
def as_list(value):
    """Nullable list column to a Python list. `value or []` raises on an array."""
    return [] if value is None else list(value)


for _, row in df[df["status"] == "ok"].iterrows():
    print(f"{row['go_id']}  {row['go_name']}")
    print("  parents:", as_list(row["go_parent_ids"])[:5])
    print("  children:", as_list(row["go_child_ids"])[:5])
    print()
```

The id lists are capped by `max_go_terms_per_side` (25 by default), so a
term near the root shows the first 25 children, not all of them. **The
counts are never capped** — trust those.

### 5. Ontology edges are not relationships

`go_parent_count` says where the term sits. `entity_relationships_by_group`
says what else in the bundle is linked to it — genes annotated with the
term, mostly. A term can be deep in the ontology and annotate nothing.

```python
df[["go_id", "go_child_count", "total_entity_relationships",
    "entity_relationships_by_group"]]
```

### 6. Every term in the bundle

```python
import time

started = time.perf_counter()
everything = bf.report.run(REPORT, input_data="__ALL__",
                           include_go_relation_details=False)
catalog = everything.to_pandas()

print(f"{everything.num_rows:,} terms in {time.perf_counter() - started:.1f}s")
catalog.groupby("go_namespace")[["go_parent_count", "go_child_count"]].agg(
    terms="size", mean_children=("go_child_count", "mean")
) if False else catalog["go_namespace"].value_counts()
```

### 7. Export

CSV by default, with a `.provenance.json` beside it naming the bundle the ids came from.

```python
for path in result.write("annotate_go.csv"):
    print(path)
```

### 8. The same thing on the command line

```bash
biofilter report run --report-name annotate_go \\
    --input ... \\
    --output out.csv
```

### 9. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("unresolved inputs:", int((df["status"] == "not_found").sum()))
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__annotate_pathway.ipynb.md ===== -->

<h1>🔀 Biofilter — Report: <code>annotate_pathway</code></h1>

Everything the bundle knows about a list of pathways: canonical id and description, which source curated it, and what it is linked to.

### 1. Open a bundle

```python
from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_pathway"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)
bf
```

### 2. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

```python
print(bf.report.explain(REPORT))
```

### 3. Run it

Pathways resolve by id — Reactome (`R-HSA-…`) or KEGG (`hsa…`).

```python
pathways = [
    "R-HSA-109581",   # Apoptosis, Reactome
    "hsa04210",       # Apoptosis, KEGG
    "NOT_A_PATHWAY",  # kept, with status='not_found'
]

result = bf.report.run(REPORT, input_data=pathways)
df = result.to_pandas()
df[["input_value", "pathway_id", "pathway_description",
    "pathway_source_system", "status"]]
```

### 4. The same biology, curated twice

Reactome and KEGG describe overlapping biology with different granularity
and different ids, and the bundle carries both. Two rows can be the same
pathway under two curations — **nothing in this report merges them**, and
`pathway_source_system` is how you tell which is which.

```python
df[["pathway_id", "pathway_source_system", "pathway_data_source",
    "total_entity_relationships"]]
```

### 5. What makes a pathway useful

A pathway with a large `Genes` count is one the bundle can expand into a
gene set. One with none is present as a label only.

```python
for _, row in df[df["status"] == "ok"].iterrows():
    print(f"{row['pathway_id']}  {row['pathway_description']}")
    for entry in row["entity_relationships_by_group"]:
        print(f"    {entry['group_name']:<12} {entry['count']:>6}")
    print()
```

### 6. Every pathway in the bundle

```python
import time

started = time.perf_counter()
everything = bf.report.run(REPORT, input_data="__ALL__")
catalog = everything.to_pandas()

print(f"{everything.num_rows:,} pathways in {time.perf_counter() - started:.1f}s")
catalog["pathway_source_system"].value_counts()
```

### 7. Export

CSV by default, with a `.provenance.json` beside it naming the bundle the ids came from.

```python
for path in result.write("annotate_pathway.csv"):
    print(path)
```

### 8. The same thing on the command line

```bash
biofilter report run --report-name annotate_pathway \\
    --input ... \\
    --output out.csv
```

### 9. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("unresolved inputs:", int((df["status"] == "not_found").sum()))
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__annotate_protein.ipynb.md ===== -->

<h1>🧪 Biofilter — Report: <code>annotate_protein</code></h1>

Everything the bundle knows about a list of proteins: UniProt record, isoform resolution, Pfam domains by type, and what the protein is linked to.

### 1. Open a bundle

```python
from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_protein"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)
bf
```

### 2. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

```python
print(bf.report.explain(REPORT))
```

### 3. Run it

Proteins resolve by accession, by entry name, or by an **isoform**
accession — section 4 is about what happens then.

```python
proteins = [
    "P04637",       # TP53, canonical accession
    "TP53_HUMAN",   # the same protein, by entry name
    "P04637-2",     # an isoform of it
    "NOT_A_PROT",   # kept, with status='not_found'
]

result = bf.report.run(REPORT, input_data=proteins)
df = result.to_pandas()
df[["input_value", "protein_id", "entity_id", "canonical_entity_id",
    "input_is_isoform", "status"]]
```

### 4. Two entity ids, and they can differ

A protein with isoforms has an entity per isoform as well as one for the
canonical sequence.

| column | what it is |
| --- | --- |
| `entity_id` | the entity the **input** matched — may be an isoform |
| `canonical_entity_id` | the entity the **annotation** describes |

An isoform entity carries almost nothing on its own, so the report follows
it to the canonical protein before annotating. Reporting the isoform's zero
relationships would be technically true and practically useless. The `note`
says so whenever the two ids differ.

```python
df[["input_value", "entity_id", "canonical_entity_id", "isoform_count", "note"]]
```

`isoform_count` counts isoforms, not entities: a protein with
`isoform_count = 3` has four entities.

### 5. Pfam domains, by type

```python
def as_list(value):
    """Nullable list column to a Python list. `value or []` raises on an array."""
    return [] if value is None else list(value)


for _, row in df[df["status"] == "ok"].iterrows():
    print(f"{row['protein_id']}  ({row['pfam_total_count']} domains)")
    for entry in as_list(row["pfam_ids_by_type"]):
        print(f"    {entry['type']:<10} {list(entry['ids'])[:6]}")
    print("   ", (row["function"] or "")[:90])
    print()
```

Counts are **distinct accessions**: a domain appearing twice in a
sequence is one accession. `pfam_total_count` is the sum across types.

### 6. Every protein in the bundle

```python
import time

started = time.perf_counter()
everything = bf.report.run(REPORT, input_data="__ALL__", include_pfam_summary=False)
catalog = everything.to_pandas()

print(f"{everything.num_rows:,} protein entities in {time.perf_counter() - started:.1f}s")
print("isoform inputs:", int(catalog["input_is_isoform"].fillna(False).sum()))
catalog["status"].value_counts()
```

### 7. Export

CSV by default, with a `.provenance.json` beside it naming the bundle the ids came from.

```python
for path in result.write("annotate_protein.csv"):
    print(path)
```

### 8. The same thing on the command line

```bash
biofilter report run --report-name annotate_protein \\
    --input ... \\
    --output out.csv
```

### 9. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("unresolved inputs:", int((df["status"] == "not_found").sum()))
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__resolve_entity.ipynb.md ===== -->

<h1>🔎 Biofilter — Report: <code>resolve_entity</code></h1>

Does the bundle know these names, and unambiguously?

Run this **before** any other report. It tells you which of your inputs
will resolve, which are ambiguous, and which the bundle has never heard
of — the three things that quietly distort every downstream result.

### 1. Open a bundle

```python
from biofilter import Biofilter

# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "resolve_entity"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)
bf
```

### 2. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

```python
print(bf.report.explain(REPORT))
```

### 3. Run it

One row per **match**, not per input. An input matching three entities
gives three rows — that is the answer, not a problem to hide.

```python
names = ["TP53", "brca1", "NOT_A_GENE"]

result = bf.report.run(REPORT, input_data=names)
df = result.to_pandas()
df[["input_original", "input", "entity_id", "primary_name", "group_name",
    "is_primary", "observation"]]
```

### 4. The three things to look at

| `observation` | what it means |
| --- | --- |
| `not found` | the bundle has no entity answering to this name |
| `multiple matches` | the **name** belongs to more than one entity |
| *(empty)* | one entity, unambiguously |

`not found` rows are kept deliberately: dropping them would leave no way
to tell "the bundle does not know this" from "you did not ask".

```python
df["observation"].value_counts(dropna=False)
```

`multiple matches` is about the name, not your search. It means
resolving that alias requires a decision **you** have to make — which is
worth knowing before a downstream report picks one for you.

```python
ambiguous = df[df["observation"] == "multiple matches"]
ambiguous[["input_original", "input", "entity_id", "primary_name", "group_name"]]
```

### 5. Match modes

| mode | matches when | cost |
| --- | --- | --- |
| `exact` | the alias equals the input, case-insensitively | an equality join |
| `like` | the input occurs **inside** the alias | a scan with a substring test |
| `fuzzy` | Jaro-Winkler similarity ≥ threshold | a scan with a scored test |

```python
import time

for mode in ("exact", "like", "fuzzy"):
    started = time.perf_counter()
    out = bf.report.run(REPORT, input_data=["BRCA1"], match_mode=mode).to_pandas()
    print(f"{mode:6s} {len(out):>5,} rows in {time.perf_counter() - started:.2f}s")
```

`like` is one-directional on purpose: the input inside the alias, not
the reverse. Matching an alias inside an input would make every
one-character alias match every input containing that character.

```python
bf.report.run(REPORT, input_data=["BRCA1"], match_mode="like").to_pandas()[
    ["input_original", "input", "primary_name", "group_name"]
].head(10)
```

### 6. Fuzzy, and a caution

Scoring happens **in the engine**. The relational version pulled all 912
thousand aliases into Python and scored them with `rapidfuzz`, which also
meant an ImportError wherever that optional dependency was missing.

⚠️ **Scores are not comparable to the old ones.** Both scales are 0–100
and the default threshold is still 80, but Jaro-Winkler rewards a shared
prefix and does not reorder words. Check the threshold against your own
inputs rather than assuming the old one transfers.

```python
fuzzy = bf.report.run(
    REPORT, input_data=["TP53"], match_mode="fuzzy", similarity_threshold=90
).to_pandas()

fuzzy.sort_values("similarity_score", ascending=False)[
    ["input_original", "input", "similarity_score", "primary_name", "group_name"]
].head(12)
```

### 7. Narrowing by entity group

```python
for group in ("Genes", "Proteins"):
    out = bf.report.run(
        REPORT, input_data=["TP53"], match_mode="like", group_filter=group
    ).to_pandas()
    print(f"{group:10s} {len(out):>4} matches")

bf.report.run(
    REPORT, input_data=["TP53"], match_mode="like", group_filter="Proteins"
).to_pandas()[["input_original", "input", "primary_name", "group_name"]]
```

### 8. Export

CSV by default, with a `.provenance.json` beside it naming the bundle the
ids came from.

```python
for path in result.write("resolve_entity.csv"):
    print(path)
```

### 9. The same thing on the command line

```bash
biofilter report run --report-name resolve_entity \\
    --input TP53 --input BRCA1 \\
    --param match_mode=fuzzy --param similarity_threshold=90 \\
    --output lookup.csv
```

### 10. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("not found:", int((df["observation"] == "not found").sum()))
print("ambiguous:", int((df["observation"] == "multiple matches").sum()))
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```

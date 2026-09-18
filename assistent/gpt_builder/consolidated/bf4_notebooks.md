# BF4 Example Notebooks



<!-- ===== SOURCE FILE: notebooks/lpc__quickstart.md ===== -->

# Biofilter 4.3 on the LPC — Quickstart

Paste, run, get a CSV.

> **Audience:** LPC users who want to query the Biofilter knowledge base.
> You do not need to know anything about containers, databases or Python.
> If you maintain the Biofilter install on the cluster, see
> [lpc__deploy.md](lpc__deploy.md) instead.

---

## Activate Biofilter

It lives in the lab's shared module tree. Two lines:

```bash
source /project/hall_shared/hall_shared.sh      # makes lab modules visible
module load biofilter/4.3.0                     # CLI on PATH, bundle configured
```

The module puts the `biofilter` CLI on your `PATH` and sets
`BIOFILTER_BUNDLE` to the current bundle, so you never pass a path.

> _Optional:_ add both lines to your `~/.bashrc` so every shell starts
> ready.

Check it took:

```bash
biofilter --version
biofilter config show      # prints which bundle is in effect
```

---

## Run a query

```bash
biofilter report run \
  --report-name annotate_gene \
  --input APOE \
  --output apoe.csv
```

That is the whole thing — no container, no database, no bind mounts. The
result lands in the current directory, typically in about a second.

---

## Change the query

| Flag | Does |
|---|---|
| `--report-name <name>` | which report to run |
| `--input APOE` | one value — **repeat the flag** for more, it is not a comma-separated list |
| `--input-file genes.txt` | one value per line; use this for long lists |
| `--param KEY=VALUE` | options and filters, separate from input |
| `--output <name>.csv` | where to write; `.parquet` also works |

```bash
biofilter report run \
  --report-name annotate_gene \
  --input APOE --input TP53 --input BRCA1 \
  --output genes.csv
```

---

## Which report?

```bash
biofilter report list
biofilter report explain --report-name <name>    # the full guide
```

The ones people reach for first:

| Report | Input | Answers |
|---|---|---|
| `platform_data_statistics` | none | **run this first on a bundle you have not used** — what is actually in it |
| `resolve_entity` | any names | which of my names does Biofilter recognise |
| `annotate_gene` | gene symbols or ids | everything known about these genes |
| `annotate_variant` | rsIDs, `chr:pos`, `chr:pos:ref:alt` | full annotation, one row per transcript |
| `expand_gene_to_variant` | gene symbols | the variants in them, filtered by predicted damage |
| `expand_variant_regulatory` | variants | which genes they regulate, in which tissue |
| `pair_variants` | variants | candidate pairs whose genes share biology |
| `aggregate_cohort_variants` | a cohort's variants | matched, placed, and binned |

The full index is in the
[Report Catalog](https://biofilter.readthedocs.io/en/latest/report_catalog.html).

---

## Before you trust a result

Three things that look like answers but are not:

- **`not_found` in a status column** — the name did not resolve in this
  bundle. Run `resolve_entity` on it to see what it did match.
- **`no_variants`** — it resolved and nothing met your criteria. That one
  is a real negative.
- **A column that is entirely null** — the source may never have been
  built into this bundle. `platform_data_statistics` says what is there.

Writing a result keeps this record. `--output genes.csv` also writes
`genes.csv.provenance.json` beside it, naming the bundle the rows came
from. Keep them together: entity and variant ids are valid only inside
the bundle that produced them.

---

## Using a different bundle

The module points at the current one. For a single command:

```bash
biofilter --bundle /project/hall_shared/datasets/biofilter/<other-date> \
  report run --report-name annotate_gene --input APOE --output out.csv
```

Or for the whole session:

```bash
export BIOFILTER_BUNDLE=/project/hall_shared/datasets/biofilter/<other-date>
biofilter report list
```

Point at the bundle **directory** — the one holding `manifest.json` — not
at its `tables/` subdirectory.

If that bundle was built by a different Biofilter release, opening it
prints a warning saying so. It still works; the warning is there because
a column that changed meaning between releases will not announce itself.

---

## Heavy workloads (LSF)

Biofilter is memory-light — a cohort-scale question over two billion
annotation rows peaked at 1.87 GB — so resource requests can be modest.

```bash
#!/bin/bash
#BSUB -J bf4-batch
#BSUB -o bf4-%J.log
#BSUB -W 1:00
#BSUB -M 8000
#BSUB -n 4

# Do NOT use `#BSUB -L /bin/bash` on this cluster — some compute nodes
# have an /etc/profile guard ("no direct access allowed") that aborts
# login shells silently. Initialise modules manually instead.
if ! type module >/dev/null 2>&1; then
    for init in \
        /etc/profile.d/modules.sh \
        /etc/profile.d/lmod.sh \
        /usr/share/lmod/lmod/init/bash \
        /usr/share/Modules/init/bash; do
        [ -r "$init" ] && source "$init" && type module >/dev/null 2>&1 && break
    done
fi

source /project/hall_shared/hall_shared.sh
module load biofilter/4.3.0

biofilter report run \
  --report-name annotate_variant \
  --input-file my_rsids.txt \
  --output results.csv
```

Check the `.log` afterwards even when the job exits 0 — warnings about a
missing source or a version mismatch are printed, not raised.

---

## Container alternative

If you prefer a container to the module:

```bash
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter:latest

apptainer run \
  --bind /project/hall_shared/datasets/biofilter/<snapshot>:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  bf4.sif \
  report run --report-name annotate_gene --input APOE --output /workspace/apoe.csv
```

`--output` writes inside the container, so it has to point at the bound
`/workspace` or the file goes away with the container.

---

## What is Biofilter?

An entity-centric biological knowledge platform: it lets you query and
annotate genes, variants, proteins, pathways, diseases, GO terms and the
relationships among them, across many curated sources (HGNC, Ensembl,
UniProt, Reactome, KEGG, GO, MONDO, ClinGen, GWAS Catalog, gnomAD,
AlphaMissense, GTEx).

On the LPC it reads a parquet bundle directly through DuckDB — no
database server, no import step, multi-user by design. One copy on shared
storage serves any number of concurrent readers.

- Documentation: <https://biofilter.readthedocs.io/>
- Repository: <https://github.com/RitchieLab/biofilter>

---

## Questions

Andre Rico — <andreluis.rico@pennmedicine.upenn.edu>



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
from pathlib import Path

from biofilter import Biofilter

# A bundle is a directory. Point at the directory, not at its tables/.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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

print(f"{len(df)} reports")
df[["name", "description"]]
```

Every report reads the bundle. The rewrite finished in 4.3.0 and the
frozen relational layer was deleted with it, so this list is the whole
catalogue — there is no second set waiting somewhere else.

The names start with what the report does: `resolve_`, `annotate_`,
`expand_`, `pair_`, `aggregate_`, `platform_`.

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
written = result.write(OUTPUT_DIR / "genes.csv")
for path in written:
    print(path)
```

```python
# Parquet instead: the provenance travels inside the file's metadata,
# and list columns stay real lists rather than JSON strings.
result.write(OUTPUT_DIR / "genes.parquet")
```

### 7. Reading a result someone else produced

The sidecar is what lets you answer "where did this come from" months
later, and `build_record.json` in the bundle has the per-source detail
behind that id — which DTP, which version, which source URL.

```python
import json

with open(OUTPUT_DIR / "genes.csv.provenance.json") as fh:
    print(json.dumps(json.load(fh), indent=2))
```

### 8. Putting a result down and picking it up

`write()` **exports**: CSV to open elsewhere, parquet for size. It
flattens nested columns so a spreadsheet can hold them, which is lossy on
purpose — a list of aliases becomes a JSON string.

`save()` / `load()` is the other job: lose nothing, and stay usable
later. It writes a **directory**, not a file, because a result can carry
more than one table:

```
runs/genes/
├── manifest.json          provenance, and what tables are here
└── tables/
    └── result.parquet
```

```python
from biofilter.modules.report.result import ReportResult

saved = result.save(OUTPUT_DIR / "runs" / "genes", overwrite=True)
back = ReportResult.load(saved)

print("identical:", back.table.equals(result.table))
print("report   :", back.provenance["report"])
print("params   :", back.provenance["params"])
```

A result does not need the bundle that produced it, and does not go
looking. Whether that bundle is still on disk is recorded, because a
result outliving its bundle is the normal case and the reason to save
one — the rows are unchanged either way, and `bundle_id` names the build
whether or not the path still resolves.

```python
back.provenance["source_bundle"]
```

**Some reports return more than one table.** When a report's answer is
genuinely two shapes it says so, rather than flattening them into one or
writing the second out as a file:

```python
bins = bf.report.run("aggregate_cohort_variants", cohort_file="...",
                     output_grain="bins")
bins.table                            # one row per (bin, sample)
bins.extra_tables["variant_to_bin"]   # what each bin is made of
```

And because a saved result is laid out like a bundle, the reader
Biofilter already has opens it — so reusing one usually means querying
it, not loading it back into Python.

```python
from biofilter.modules.report import Bundle

with Bundle.open(saved) as opened:
    print("tables:", sorted(opened.tables))
    display(opened.con.execute(
        "SELECT * FROM result LIMIT 3"
    ).to_arrow_table().to_pandas())
```

**`provenance["warnings"]` is always there.** An empty list means
nothing went wrong, never "nothing was collected" — a report that copes
with a problem in silence leaves nothing behind, so coping gets written
down.

```python
result.provenance["warnings"]
```

---

## Working directly, without the facade

Reports are the packaged questions. For an ad-hoc one, open the bundle
and write SQL — it is the same engine the reports use.

```python
from biofilter.modules.report import Bundle

with Bundle.open(BUNDLE or bf.core.db_uri.removeprefix('parquet://')) as bundle:
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
with Bundle.open(BUNDLE or bf.core.db_uri.removeprefix('parquet://')) as bundle:
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
from pathlib import Path

from biofilter import Biofilter

BUNDLE = "/path/to/biofilter_data/bundles/20260914"
REPORT = "<report_name>"

bf = Biofilter(bundle=BUNDLE, debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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

### 7. If your report needs more than one table

A report returns one table. When its answer is genuinely two shapes —
the rows and what was rejected; the bins and what went into them — call
`self.emit("<name>", table)` in `run()` instead of flattening them
together or writing the second one out as a file. A file is something
the result only names, which is how it stops being checked.

When the report copes with something the reader should know about, call
`self.warn(message, **context)`. It logs at WARNING *and* records the
same thing in `provenance["warnings"]`, which is what reaches whoever
opens the result months later without the log.

```python
def run(self):
    ...
    if dropped:
        self.warn(f"{dropped:,} rows had no coordinates.", rows=dropped)
        self.emit("dropped", dropped_table)
    return main_table
```

```python
# What this run produced, beyond the main table.
print("tables :", {n: t.num_rows for n, t in result.tables.items()})
print("warnings:", result.provenance["warnings"])
print("files  :", [a.name for a in result.artifacts])
```

### 8. Export

Always show both, and say what the provenance sidecar is for: ids in a
result are scoped to the bundle that produced them.

```python
for path in result.write(OUTPUT_DIR / "<report_name>.csv"):
    print(path)
```

### 9. The same thing on the command line

```bash
biofilter --bundle /path/to/bundles/20260914 report run \
    --report-name <report_name> \
    --input ... \
    --output out.csv
```

### 10. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__aggregate_cohort_variants.ipynb.md ===== -->

<h1>🎛️ Biofilter — Report: <code>aggregate_cohort_variants</code></h1>

Your cohort's variants, matched against the bundle and aggregated into
biological bins.

Three stages: **read** your file, **match** it to the bundle, **aggregate**
the rare variants into bins. `output_grain` decides where you stop.

Sections 4 and 6 are the ones to read — they are the two ways this report
can hand you an empty answer that means something.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

BUNDLE = None
REPORT = "aggregate_cohort_variants"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(bf.core.db_uri)
```

### 2. A cohort to work with

This notebook has to be runnable, so it writes a small synthetic cohort
from variants the bundle actually carries. Point `COHORT` at your own VCF
and everything below works unchanged.

Two things are deliberate: 200 samples, and two variants on a chromosome
the bundle does not carry.

```python
import random

from biofilter.modules.report import Bundle

COHORT = OUTPUT_DIR / "demo_cohort.vcf"
PHENOTYPE = OUTPUT_DIR / "demo_phenotype.csv"
N_SAMPLES = 200

with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    rows = bundle.con.execute("""
        SELECT v.chromosome, v.position, v.reference_allele, v.alternate_allele
        FROM variant_masters v
        JOIN entity_locations l ON l.build = 38 AND l.chromosome = v.chromosome
         AND v.position BETWEEN l.start_pos AND l.end_pos
        JOIN gene_masters gm ON gm.entity_id = l.entity_id
        WHERE gm.symbol IN ('CHEK2', 'SMARCB1', 'NF2')
          AND length(v.reference_allele) = 1 AND length(v.alternate_allele) = 1
        ORDER BY v.position LIMIT 600
    """).fetchall()

random.seed(11)
samples = [f"S{i + 1}" for i in range(N_SAMPLES)]
lines = ["##fileformat=VCFv4.2", "##contig=<ID=chr22>"]
lines.append("\t".join(
    ["#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"] + samples))

for i, (chrom, pos, ref, alt) in enumerate(rows):
    genotypes = ["0/0"] * N_SAMPLES
    if i % 3 == 0:                       # rare: one to three carriers
        for j in random.sample(range(N_SAMPLES), random.choice([1, 2, 3])):
            genotypes[j] = "0/1"
    elif i % 3 == 1:                     # common: about 15%
        genotypes = [f"{int(random.random() < 0.15)}/{int(random.random() < 0.15)}"
                     for _ in samples]
    lines.append("\t".join(
        [f"chr{chrom}", str(pos), f"v{i}", ref, alt, ".", "PASS", ".", "GT"] + genotypes))

# On a chromosome this bundle does not carry — section 4 is about these.
lines.append("\t".join(
    ["chr1", "12010", "offtarget", "A", "C", ".", "PASS", ".", "GT"] + ["0/1"] * N_SAMPLES))

COHORT.write_text("\n".join(lines) + "\n")
PHENOTYPE.write_text("\n".join(
    ["SampleID,Phenotype"]
    + [f"{s},{1 if i < N_SAMPLES // 2 else 0}" for i, s in enumerate(samples)]) + "\n")

print(f"{len(rows)} variants, {N_SAMPLES} samples")
print(f"smallest frequency this cohort can observe: {1 / (2 * N_SAMPLES):.4f}")
```

### 3. Stage 2 — which of your variants does Biofilter know?

`output_grain="variants"` is what `variant_list_intersect` did. Five
statuses, and the differences between them are the point.

```python
matched = bf.report.run(REPORT, cohort_file=str(COHORT),
                        phenotype_file=str(PHENOTYPE))
df = matched.to_pandas()

print(f"{len(df):,} cohort variants")
df.groupby("match_status").size().to_frame("variants")
```

```python
df[df.match_status == "matched"].head(5)[
    ["cohort_variant_id", "chromosome", "position", "variant_key", "plink_id",
     "gene_symbols", "maf_overall", "maf_case", "maf_control", "is_rare"]
]
```

`in_bundle_no_gene` and `not_in_bundle` look alike in a spreadsheet
and mean opposite things: the first is a variant Biofilter knows that no
gene contains, the second one it has never heard of. The `note` column
spells out which happened.

### 4. The guard: can the bundle place what you brought?

A cohort file spans the genome. A bundle need not. **Binning a
whole-genome VCF against a single-chromosome bundle does not fail** — it
returns bins for that chromosome and stays silent about the rest.

That is the single most dangerous thing this report could do, so every
run measures it.

```python
matched.provenance["chromosome_coverage"]
```

```python
# When the result leaves your screen, make it a refusal instead.
try:
    bf.report.run(REPORT, cohort_file=str(COHORT), require_full_coverage=True)
except ValueError as exc:
    print("refused:", exc)
```

### 5. Stage 3 — bins

`output_grain="bins"` is what `variant_binning` did. One row is one
sample in one bin, and only carriers appear.

```python
bins = bf.report.run(REPORT, cohort_file=str(COHORT),
                     phenotype_file=str(PHENOTYPE),
                     output_grain="bins", group_by="gene",
                     maf_cutoff=0.01)
bdf = bins.to_pandas()

print(f"{len(bdf):,} (sample, bin) rows across {bdf.bin_name.nunique()} bins")
bdf.head(5)
```

```python
# The burden, which is what a downstream test consumes.
bdf.groupby(["bin_name", "sample_class"]).agg(
    samples=("sample", "nunique"),
    alt_alleles=("alt_count", "sum"),
    variants=("variant_count", "sum"),
)
```

### 6. The other empty result, and it is arithmetic

With **N samples the smallest observable minor allele frequency is
1/(2N)** — one allele copy in one person. A 20-sample cohort cannot see
anything rarer than 0.025, so asking it for `maf_cutoff=0.01` keeps only
variants nobody carries and every bin comes back empty.

Correct, and invisible in the rows. So the report says it.

```python
bins.provenance["rare_variants"]
```

```python
# The same cohort cut down to 10 samples, asking for something it
# cannot observe.
small = OUTPUT_DIR / "demo_small.vcf"
head, *body = COHORT.read_text().splitlines()
cols = body[0].split("\t")
small.write_text("\n".join(
    [head, "\t".join(cols[:9] + cols[9:19])]
    + ["\t".join(r.split("\t")[:9] + r.split("\t")[9:19]) for r in body[1:]]) + "\n")

tiny = bf.report.run(REPORT, cohort_file=str(small), output_grain="bins",
                     maf_cutoff=0.01)
print("rows:", len(tiny.to_pandas()))
print(tiny.provenance["rare_variants"]["means"])
```

### 7. Four kinds of bin, and how far each reaches

Stage 2 is positional, and only 39,306 of the bundle's 72,660 genes carry
build-38 coordinates — so half the catalogue is out of reach whatever the
grouping.

```python
for group_by in ("gene", "gene_group", "locus_type", "pathway"):
    out = bf.report.run(REPORT, cohort_file=str(COHORT),
                        phenotype_file=str(PHENOTYPE),
                        output_grain="bins", group_by=group_by, maf_cutoff=0.01)
    frame = out.to_pandas()
    reach = out.provenance["bin_coverage"]
    print(f"  {group_by:<11} {len(frame):>6,} rows, "
          f"{frame.bin_name.nunique() if len(frame) else 0:>4} bins   "
          f"reaches {reach['genes_this_bin_type_can_reach']:,} genes "
          f"({reach['share']:.1%})")
```

### 8. Which frequency the rare filter uses

With both arms present the default filters on the **larger** of the case
and control MAFs — BioBin's rule, so a variant common in cases and absent
in controls is not swept into a rare bin.

```python
for label, params in [
    ("case/control (default)", {"rare_case_control": True}),
    ("control only", {"rare_case_control": False, "overall_major_allele": False}),
    ("overall", {"rare_case_control": False, "overall_major_allele": True}),
]:
    out = bf.report.run(REPORT, cohort_file=str(COHORT),
                        phenotype_file=str(PHENOTYPE),
                        output_grain="bins", maf_cutoff=0.01, **params)
    rare = out.provenance["rare_variants"]
    print(f"  {label:<24} {rare['rare']:>4} rare, "
          f"{rare['rare_with_carriers']:>4} with carriers")
```

### 9. One table that travels, one file that does not

`variant_to_bin` — what each bin is made of — is a **second table on the
result**, always there when you ask for bins. Two runs differing only in
`maf_cutoff` produce different bins and the main table does not say which
variants moved; this is how you find out. Being a table rather than a
file, it cannot be lost or go stale without anything noticing.

`plink_extract_path` writes a real file, because PLINK reads files. It
matches on the id in your `.bim`, not on coordinates, so the id your own
file used is preferred — a list of `chr:pos` strings extracts nothing
from a dataset keyed by rsIDs.

```python
audited = bf.report.run(
    REPORT, cohort_file=str(COHORT), phenotype_file=str(PHENOTYPE),
    output_grain="bins", maf_cutoff=0.01,
)

print("tables on the result:")
for name, table in audited.tables.items():
    print(f"  {name:<16} {table.num_rows:>6,} rows")

display(audited.extra_tables["variant_to_bin"].to_pandas().head(5))

keep = bf.report.run(
    REPORT, cohort_file=str(COHORT),
    plink_extract_path=str(OUTPUT_DIR / "keep.txt"),
)
for artifact in keep.artifacts:
    print(f"\nfile: {artifact.name} — {artifact.description}")
```

### 10. What the run wants you to know

This report proceeds through three situations that can make its answer
misleading rather than refusing outright. Each is logged when it happens
**and** recorded in the provenance, because whoever opens the result next
month does not have the log.

```python
small = bf.report.run(REPORT, cohort_file=str(COHORT),
                     output_grain="bins", maf_cutoff=0.0001)

for warning in small.provenance["warnings"]:
    print("⚠️ ", warning["message"])

print("\nno warnings on a clean run:",
      bf.report.run(REPORT, cohort_file=str(COHORT),
                    output_grain="variants").provenance["warnings"])
```

### 11. Export

```python
for path in bins.write(OUTPUT_DIR / "aggregate_cohort_variants.csv"):
    print(path)
```

### 12. The same thing on the command line

```bash
biofilter report run --report-name aggregate_cohort_variants \\
    --param cohort_file=./cohort.vcf.gz \\
    --param phenotype_file=./phenotype.csv \\
    --param output_grain=bins \\
    --param group_by=gene \\
    --param maf_cutoff=0.01 \\
    --param require_full_coverage=true \\
    --output bins.csv
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__annotate_disease.ipynb.md ===== -->

<h1>🩺 Biofilter — Report: <code>annotate_disease</code></h1>

Everything the bundle knows about a list of diseases: MONDO record, groups, cross-references grouped by the source that issued them, and how many genes ClinGen links to the disease.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_disease"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
for path in result.write(OUTPUT_DIR / "annotate_disease.csv"):
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
from pathlib import Path

from biofilter import Biofilter

BUNDLE = None  # set a path to override; None reads [database] bundle from .biofilter.toml
REPORT = "annotate_gene"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
for path in everything.write(OUTPUT_DIR / "annotate_gene.csv"):
    print(path)
```

```python
# Parquet keeps the list columns as lists, and carries the provenance in
# the file's own metadata.
everything.write(OUTPUT_DIR / "annotate_gene.parquet")
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
from pathlib import Path

from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_go"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
for path in result.write(OUTPUT_DIR / "annotate_go.csv"):
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
from pathlib import Path

from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_pathway"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
for path in result.write(OUTPUT_DIR / "annotate_pathway.csv"):
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
from pathlib import Path

from biofilter import Biofilter

# A bundle is a directory — the one holding manifest.json.
# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_protein"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
for path in result.write(OUTPUT_DIR / "annotate_protein.csv"):
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



<!-- ===== SOURCE FILE: notebooks/templates/reports__annotate_variant.ipynb.md ===== -->

<h1>🧬 Biofilter — Report: <code>annotate_variant</code></h1>

What the bundle knows about a list of variants: identity, gnomAD joint
frequencies, in-silico predictions, and one row per transcript the
variant was annotated against.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "annotate_variant"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

bf
```

### 2. Which chromosomes does this bundle actually have

Ask first. A bundle built for a subset returns `not_found` for everything
outside it — true of the bundle, not of the genome.

```python
stats = bf.report.run("platform_data_statistics", sections=["variants"]).to_pandas()

present = sorted(stats[stats["dimension_1"] == "variant_masters"]["dimension_2"].unique(),
                 key=int)
print("chromosomes in variant_masters:", present)
```

### 3. What the report offers

```python
print("columns:")
for column in bf.report.available_columns(REPORT):
    print(" ", column)

print("\nexample input:")
print(bf.report.example_input(REPORT))
```

### 4. Three input shapes, mixed freely

| shape | matches |
| --- | --- |
| `rs1225039379` | the variant that rsID maps to |
| `22:15238761` | **every** variant at that position |
| `22:20052518:C:T` | exactly that variant |

`chr`/`CHR`/`chromosome` prefixes and `:`/`-`/`_`/space separators are all
accepted; `X`, `Y`, `MT` map to 23, 24, 25.

```python
result = bf.report.run(REPORT, input_data=[
    "rs1225039379",
    "22:15238761",
    "chr22:20052518:C:T",
    "nonsense",
], most_severe_only=True)

df = result.to_pandas()
df[["input_value", "input_kind", "status", "variant_key", "rsid",
    "gene_symbol", "consequence", "note"]]
```

### 5. One row per transcript

A variant is annotated against every transcript it overlaps — often
dozens. The variant-level facts repeat on each row.

```python
one = bf.report.run(REPORT, input_data=["22:20052518:C:T"]).to_pandas()

print(f"{len(one)} transcripts, {one['gene_symbol'].nunique()} gene(s)")
one[["transcript_id", "consequence", "severity_rank", "impact",
     "canonical", "mane_select", "is_most_severe_for_variant"]].head(8)
```

`is_most_severe_for_variant` is **derived**, not stored. 4.3.0 dropped
that flag from the schema, so it is computed from
`variant_consequences.severity_rank` — consistent with whatever severity
ordering the bundle carries, rather than with what the ETL believed when
it wrote the row.

### 6. Two ways to narrow it, and they disagree

The most severe consequence is not always on the canonical transcript.

```python
for label, params in [
    ("everything", {}),
    ("most_severe_only", {"most_severe_only": True}),
    ("canonical_only", {"canonical_only": True}),
    ("both", {"most_severe_only": True, "canonical_only": True}),
]:
    out = bf.report.run(REPORT, input_data=["22:20052518:C:T"], **params).to_pandas()
    print(f"  {label:18s} {len(out):>3} rows")
```

### 7. Frequencies and predictions

```python
severe = bf.report.run(
    REPORT, input_data=["22:20052518:C:T"], most_severe_only=True
).to_pandas().iloc[0]

print(f"  {severe['variant_key']}  ({severe['rsid']})")
print(f"  af_joint  {severe['af_joint']:.3e}   ac {severe['ac_joint']:,} / an {severe['an_joint']:,}")
print(f"  CADD      {severe['cadd_phred']:.1f} phred")
print(f"  REVEL     {severe['revel_max']}")
print(f"  SIFT      {severe['sift_max']}     PolyPhen {severe['polyphen_max']}")
print(f"  phyloP    {severe['phylop']}")
```

⚠️ Frequencies are the gnomAD **joint** callset. The exomes and genomes
columns exist in `variant_masters` and are not surfaced here.

### 8. AlphaMissense scores one transcript

Of 89 transcripts, one carries a score. That is AlphaMissense's own
scope — it predicts on the canonical protein sequence — not a join that
failed.

(It did fail at first: AlphaMissense writes `ENST00000327374.9` and VEP
writes `ENST00000327374`, so the raw join matched nothing, silently.)

```python
scored = one[one["alphamissense_score"].notna()]

print(f"{len(scored)} of {len(one)} transcripts scored")
scored[["transcript_id", "consequence", "alphamissense_score",
        "alphamissense_classification"]]
```

### 9. Where rsIDs come from

`variant_masters` carries an `rsid` column and it is **entirely null** in
4.3.0 bundles. Lookups go through `variant_rsid`, which covers roughly
97% of variants.

```python
sample = bf.report.run(REPORT, input_data=["22:20052518:C:T"],
                       most_severe_only=True).to_pandas()
print("rsid from variant_rsid:", sample.iloc[0]["rsid"])
```

### 10. Export

```python
for path in result.write(OUTPUT_DIR / "annotate_variant.csv"):
    print(path)
```

### 11. The same thing on the command line

```bash
biofilter report run --report-name annotate_variant \\
    --input-file variants.txt \\
    --param most_severe_only=true \\
    --output variants.csv
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__expand_entity_neighborhood.ipynb.md ===== -->

<h1>🕸️ Biofilter — Report: <code>expand_entity_neighborhood</code></h1>

What sits one hop from each of these entities.

Takes a **heterogeneous** list — genes, diseases, proteins, GO terms, in
any mix — and reports what each one is connected to: how many neighbours,
of which kinds, and which ones.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "expand_entity_neighborhood"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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

Mix whatever you like. A `gene:` style prefix narrows the search to that
group; without one, every group is searched.

```python
items = [
    "gene:TP53",              # restricted to Genes
    "protein:P04637",         # restricted to Proteins
    "MONDO:0007254",          # a CURIE — searched everywhere
    "APOE",                   # a bare name
    "ZZZ_NOT_A_THING",        # kept, with status='not_found'
]

result = bf.report.run(REPORT, input_data=items)
df = result.to_pandas()
df[["input_value", "input_type_hint", "entity_id", "entity_group",
    "degree_total", "status"]]
```

### 4. Why `GO:0006915` is not a hint

A prefix counts as a type hint **only when it names an entity group** —
and the group names are read from the bundle, not hardcoded.

That rule exists because of a defect in the report this replaces. It
treated *any* prefix before a colon as a hint, so `GO:0006915` became the
term `0006915`, which resolved to a **disease**. Every CURIE in the
bundle was affected — `MONDO:`, `HGNC:`, `DOID:`, `EFO:` — and nothing
failed loudly.

`go:` is excluded from the hint vocabulary for the same reason. Use
`go_terms:` to restrict to that group.

```python
bf.report.run(REPORT, input_data=[
    "GO:0006915",              # the GO term itself
    "MONDO:0007254",           # the disease itself
    "go_terms:GO:0006915",     # the same term, restricted explicitly
]).to_pandas()[["input_value", "input_type_hint", "entity_id", "entity_group", "status"]]
```

### 5. A hint narrows, and can rule out

`TP53` is a gene. Asking for it as a disease is a question with the
answer "no".

```python
bf.report.run(REPORT, input_data=["gene:TP53", "disease:TP53"]).to_pandas()[
    ["input_value", "entity_group", "status", "note"]
]
```

### 6. The neighbourhood itself

```python
for _, row in df[df["status"] == "ok"].iterrows():
    print(f"{row['input_value']}  ->  {row['primary_name']}  "
          f"({row['entity_group']}, degree {row['degree_total']:,})")
    for entry in row["neighbors_by_type"]:
        print(f"    {entry['group_name']:<14} {entry['count']:>6}  "
              f"{list(entry['names'])[:3]}")
    print()
```

`neighbors_by_type` is **one nested column**, not one column per entity
group. The report this replaces added a column per group present in the
bundle — 29 columns, 14 of them impossible to know before running it,
each holding a JSON string.

The names are capped by `neighbors_top_n_per_type`; `count` and
`degree_total` never are.

```python
capped = bf.report.run(REPORT, input_data=["gene:TP53"],
                       neighbors_top_n_per_type=3).to_pandas()

entry = capped.iloc[0]["neighbors_by_type"][0]
print(f"{entry['group_name']}: count={entry['count']:,}, names shown={len(entry['names'])}")
print(list(entry["names"]))
```

### 7. Degree zero is an answer

An entity can resolve cleanly and have nothing linked to it. GO terms are
the usual case: this bundle carries no entity relationships for them at
all, so the neighbourhood comes back empty and the `note` says why.

```python
bf.report.run(REPORT, input_data=["GO:0006915"]).to_pandas()[
    ["input_value", "entity_group", "degree_total", "status", "note"]
]
```

### 8. Export

CSV by default, with a `.provenance.json` beside it. The nested column is
written as JSON there; parquet keeps it as a real list.

```python
for path in result.write(OUTPUT_DIR / "expand_entity_neighborhood.csv"):
    print(path)
```

### 9. The same thing on the command line

```bash
biofilter report run --report-name expand_entity_neighborhood \\
    --input gene:BRCA1 --input "disease:breast cancer" \\
    --param neighbors_top_n_per_type=10 \\
    --output neighbourhood.csv
```

### 10. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print("not found:", int((df["status"] == "not_found").sum()))
print("resolved but isolated:",
      int(((df["status"] == "ok") & (df["degree_total"] == 0)).sum()))
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__expand_entity_relationship.ipynb.md ===== -->

<h1>🔗 Biofilter — Report: <code>expand_entity_relationship</code></h1>

Which links the bundle holds for these entities.

One row per (input, relationship). Use it to answer *what is this
connected to*, or — with `relationship_scope="between_inputs"` — *how are
these connected to each other*.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "expand_entity_relationship"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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

### 3. What is this connected to

The default scope, `input_to_any`, returns every link where an input
appears on either side. That is a lot: filter it.

```python
result = bf.report.run(
    REPORT,
    input_data=["TP53"],
    output_entity_groups=["Pathways"],
)
df = result.to_pandas()

print(f"{len(df):,} rows")
df[["input_original", "relationship_type", "related_primary_name",
    "related_group_name", "direction"]].head(10)
```

### 4. How are these connected to each other

`between_inputs` keeps only links whose **both** ends are in your list.
It is a different question, and usually a much smaller answer.

```python
genes = ["TP53", "BRCA1", "EGFR", "MDM2"]

for scope in ("input_to_any", "between_inputs"):
    out = bf.report.run(REPORT, input_data=genes, relationship_scope=scope).to_pandas()
    real = out[out["observation"] == ""]
    print(f"{scope:15s} {len(real):>7,} relationship rows")
```

```python
between = bf.report.run(
    REPORT, input_data=genes, relationship_scope="between_inputs"
).to_pandas()

between[between["observation"] == ""][
    ["input_primary_name", "relationship_type", "related_primary_name", "direction"]
].head(12)
```

### 5. Why a relationship can appear twice

A link with an input at **both** ends is reached from each, producing two
rows that differ in `match_side` and `direction`.

In `between_inputs` that always happens, so `deduplicate_pairs` defaults
to `True` there and `False` in `input_to_any`. Both are overridable.

```python
for dedupe in (True, False):
    out = bf.report.run(
        REPORT, input_data=genes,
        relationship_scope="between_inputs", deduplicate_pairs=dedupe,
    ).to_pandas()
    real = out[out["observation"] == ""]
    print(f"deduplicate_pairs={str(dedupe):5s} {len(real):>5,} rows")
```

### 6. Three kinds of row

| `observation` | meaning |
| --- | --- |
| *(empty)* | a real relationship |
| `not found` | the bundle has no entity for this input |
| `no relationships in scope` | it resolved, and nothing came back |

The third is new in 4.3.0. The report this replaces emitted rows only for
inputs it could not **resolve**, so a gene with five thousand
relationships and none to `Chemicals` simply vanished from a
chemicals-filtered result — indistinguishable from one never asked
about.

```python
mixed = bf.report.run(
    REPORT,
    input_data=["TP53", "GO:0006915", "ZZZ_NOT_A_THING"],
    output_entity_groups=["Chemicals"],
).to_pandas()

mixed[["input_original", "input_entity_id", "input_group_name", "observation"]]
```

### 7. Narrowing further

`input_entity_groups` constrains what an input may resolve to;
`relationship_types` keeps only certain kinds of link.

```python
print("relationship types in this result:")
print(bf.report.run(REPORT, input_data=["TP53"]).to_pandas()["relationship_type"]
      .value_counts().to_string())
```

```python
bf.report.run(
    REPORT,
    input_data=["TP53"],
    relationship_types=["in_pathway"],
    output_entity_groups=["Pathways"],
).to_pandas()[["input_original", "relationship_type", "related_primary_name"]].head(5)
```

### 8. Export

```python
for path in result.write(OUTPUT_DIR / "expand_entity_relationship.csv"):
    print(path)
```

### 9. The same thing on the command line

```bash
biofilter report run --report-name expand_entity_relationship \\
    --input TP53 --input BRCA1 \\
    --param relationship_scope=between_inputs \\
    --output relationships.csv
```

### 10. Quick QA

```python
expected = list(bf.report.available_columns(REPORT))
missing = [c for c in expected if c not in df.columns]

print("missing columns:", missing or "none")
print(df["observation"].value_counts(dropna=False).to_string())
print("bundle:", result.provenance["bundle_id"])
display(df.dtypes.to_frame("dtype"))
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__expand_gene_to_variant.ipynb.md ===== -->

<h1>🎛️ Biofilter — Report: <code>expand_gene_to_variant</code></h1>

The variants belonging to a list of genes, with their annotation.

"Belonging to" is **two different questions**, and this report makes you
pick one. Section 3 is about why that is not a detail.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

BUNDLE = None
REPORT = "expand_gene_to_variant"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(bf.core.db_uri)
```

### 2. Which chromosomes does this bundle carry

Ask first. The current bundle is chr22 only, so a gene anywhere else
resolves fine and then finds nothing — which is a property of the build,
not of the gene.

```python
from biofilter.modules.report import Bundle

with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    chroms = bundle.chromosomes("variant_masters")

print("chromosomes with variants:", chroms)
```

### 3. The choice you have to make

| `mapping` | a variant belongs to a gene when… |
| --- | --- |
| `position` | its coordinate falls inside the gene's build-38 range |
| `annotation` | VEP associated it with that gene |

Run the same gene both ways and compare.

```python
GENE = "CHEK2"

sets = {}
for mapping in ("position", "annotation"):
    out = bf.report.run(REPORT, input_data=[GENE], mapping=mapping,
                        max_variants_per_gene=0).to_pandas()
    sets[mapping] = set(out.query("status == 'ok'").variant_key)
    print(f"  {mapping:<11} {len(sets[mapping]):>6,} variants")

p, a = sets["position"], sets["annotation"]
print(f"\n  both         {len(p & a):>6,}")
print(f"  only position{len(p - a):>7,}")
print(f"  only annotation{len(a - p):>5,}")
```

For `CHEK2` the positional set turns out to be a clean subset of the
annotated one — VEP assigns every in-body variant to the gene *and*
reaches about 5 kb beyond it. That is common but not universal.

Across all **958 chr22 genes** with build-38 coordinates and annotated
variants:

| | pairs |
| --- | --- |
| by position | 2,045,943 |
| by annotation | 2,651,135 |
| **only by position** | **144,488** (139 genes have at least one) |
| only by annotation | ~749,680 |

Only-position variants are the ones VEP attributed to a neighbouring
gene, or to no gene at all. So neither mechanism contains the other, and
nothing in the rows tells you which one you ran.

### 4. So the report records the choice — twice

Once as a column on every row, once in the provenance JSON. A CSV that
gets separated from its provenance file still says which question it
answers.

```python
result = bf.report.run(REPORT, input_data=[GENE], mapping="position")
df = result.to_pandas()

print("column :", df["mapping"].unique().tolist())
print("provenance:")
result.provenance["mapping"]
```

### 5. `window_bp` — widening the gene's range

Applies to `position` only. Passing it with `mapping="annotation"` is an
error rather than a silent no-op: VEP's own association already reaches
past the gene body, so a window there would change nothing while looking
like it had.

```python
for window in (0, 5_000, 50_000):
    out = bf.report.run(REPORT, input_data=[GENE], mapping="position",
                        window_bp=window, max_variants_per_gene=0).to_pandas()
    print(f"  window_bp={window:>6,}  {len(out):>7,} rows")

try:
    bf.report.run(REPORT, input_data=[GENE], mapping="annotation", window_bp=5_000)
except ValueError as exc:
    print("\nrefused:", exc)
```

### 6. The cap, and why it announces itself

`max_variants_per_gene` defaults to 5000. A capped gene looks exactly
like a complete answer — a round number of rows and nothing admitting
more existed — so the provenance says what was hidden, per gene.

```python
capped = bf.report.run(REPORT, input_data=["CHEK2", "SMARCB1"],
                       mapping="position", max_variants_per_gene=200)

print(capped.to_pandas().groupby("input_gene").size().to_dict())
capped.provenance["truncation"]
```

```python
# 0 means no cap — not "fall back to the default".
uncapped = bf.report.run(REPORT, input_data=["CHEK2", "SMARCB1"],
                         mapping="position", max_variants_per_gene=0)

print(uncapped.to_pandas().groupby("input_gene").size().to_dict())
print("applied:", uncapped.provenance["truncation"]["applied"])
```

### 7. Four statuses, and one distinction that matters

| status | means |
| --- | --- |
| `ok` | a variant |
| `not_found` | the input did not resolve to a gene in this bundle |
| `no_location` | the gene resolved, but the bundle has no build-38 coordinates — `position` cannot place it |
| `no_variants` | the gene resolved and nothing met the criteria |

`no_location` is not a rare corner: **33,354 of the 72,660 genes in this
bundle — 45.9% — have no build-38 coordinates**. For those, `annotation`
is the only mapping that can answer anything.

```python
mixed = bf.report.run(REPORT, input_data=["CHEK2", "TP53", "NOT_A_GENE"],
                      mapping="position", max_variants_per_gene=50).to_pandas()

mixed.groupby(["input_gene", "status"]).size().to_frame("rows")
```

```python
# TP53 is on chr17; this bundle carries chr22 only. The report says
# which of the two reasons applies rather than returning an empty frame.
mixed.query("status != 'ok'")[["input_gene", "status", "note"]]
```

### 8. One row per variant, or one per transcript

`most_severe_only=True` (the default) collapses a variant's transcripts
and keeps the worst consequence. Turn it off and a row becomes a
gene-variant-**transcript** triple — counting rows then counts
transcripts, not variants.

```python
for severe in (True, False):
    out = bf.report.run(REPORT, input_data=[GENE], mapping="annotation",
                        most_severe_only=severe, max_variants_per_gene=0).to_pandas()
    print(f"  most_severe_only={str(severe):<5}  "
          f"{len(out):>7,} rows  {out.variant_key.nunique():>6,} variants")
```

### 9. Filtering

Frequency, impact, consequence and the in-silico predictors. They stack.

```python
rare_damaging = bf.report.run(
    REPORT,
    input_data=[GENE],
    mapping="position",
    af_max=0.001,
    impact_filter=["HIGH", "MODERATE"],
    cadd_phred_min=20,
    max_variants_per_gene=0,
).to_pandas()

print(f"{len(rare_damaging):,} rows")
rare_damaging[["variant_key", "rsid", "consequence", "impact",
               "af_joint", "cadd_phred", "alphamissense_classification"]].head(8)
```

### 10. A missing predictor is a missing column, not a zero

If the bundle carries no `variant_predictions` or `variant_alphamissense`
table, those columns come back null for every row. The omission is
recorded in the provenance `coverage` block — check it before concluding
that nothing scored.

```python
result.provenance["coverage"]
```

```python
# AlphaMissense needs two corrections to join, and both failures are
# silent nulls: it versions transcript ids (ENST00000327374.9) where VEP
# does not, and it scores a transcript VEP rarely calls most severe.
#
# So the join follows the grain of the row. One row per variant gets the
# variant's score; one row per transcript gets that transcript's.
for severe in (True, False):
    out = bf.report.run(REPORT, input_data=[GENE], mapping="position",
                        af_max=0.001, impact_filter=["HIGH", "MODERATE"],
                        cadd_phred_min=20, most_severe_only=severe,
                        max_variants_per_gene=0)
    mis = out.to_pandas().query("consequence == 'missense_variant'")
    scored = int(mis["alphamissense_score"].notna().sum())
    print(f"  most_severe_only={str(severe):<5}  {len(mis):>6,} missense, "
          f"{scored:>6,} scored  "
          f"({out.provenance['alphamissense']['joined_on']})")
```

### 11. Export

```python
for path in result.write(OUTPUT_DIR / "expand_gene_to_variant.csv"):
    print(path)
```

### 12. Chaining is your job, on purpose

There is no report-to-report plumbing. Write the list out, look at it,
pass it on.

```python
variants = df.query("status == 'ok'").variant_key.tolist()
bf.report.run("expand_variant_regulatory", input_data=variants)
```

### 13. The same thing on the command line

```bash
biofilter report run --report-name expand_gene_to_variant \\
    --input CHEK2 --input SMARCB1 \\
    --param mapping=position \\
    --param window_bp=5000 \\
    --param af_max=0.01 \\
    --output gene_variants.csv
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__expand_variant_regulatory.ipynb.md ===== -->

<h1>🎛️ Biofilter — Report: <code>expand_variant_regulatory</code></h1>

Which genes a variant **regulates**, in which tissue, with what effect.

Not the same question as `annotate_variant`, which reports the gene a
variant sits *in*. They are usually different genes — see section 3.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

BUNDLE = None
REPORT = "expand_variant_regulatory"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

bf
```

### 2. Which tissues does this bundle carry

Ask first. GTEx ships 50 tissues and the build loads a chosen subset, so
absence of evidence here means absence **in these tissues**.

```python
import duckdb

from biofilter.modules.report import Bundle

with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    tissues = bundle.con.execute(
        "SELECT bio_context, count(*) AS n FROM variant_gtex "
        "GROUP BY 1 ORDER BY n DESC"
    ).to_arrow_table().to_pandas()

print(f"{len(tissues)} tissues in this bundle")
tissues
```

### 3. The gene it is in, and the gene it regulates

This is the whole reason the report exists.

```python
result = bf.report.run(REPORT, input_data=["22:42914646"], p_value_max=1e-10)
df = result.to_pandas()

df[["variant_key", "position_gene_symbol", "regulated_gene_symbol",
    "bio_context", "beta", "p_value"]].head(8)
```

On chr22 of this bundle, of 6,919,646 variant × gene pairs carrying both
kinds of evidence, **88.5% name a different gene**. And 31,080 variants
with regulatory evidence are classed by VEP as `intergenic`, `upstream`
or `downstream` — outside any gene at all. For those, `annotate_variant`
says "not in a gene" while the eQTL says "regulates this one".

### 4. Three input shapes, mixed freely

| shape | means |
| --- | --- |
| `APOE` | every variant inside that gene's range |
| `rs429358` | that variant |
| `22:42914646` | that position |

Anything that is not an rsID or a position is read as a gene name.

```python
mixed = bf.report.run(
    REPORT,
    input_data=["DDT", "22:42914646", "rs4822455"],
    p_value_max=1e-20,
).to_pandas()

mixed.groupby(["input_value", "input_kind"]).size().to_frame("rows")
```

### 5. Gene mode: what do variants in this gene regulate

```python
gene = bf.report.run(REPORT, input_data=["DDT"], p_value_max=1e-50).to_pandas()

print(f"{len(gene):,} rows, "
      f"{gene['regulated_gene_id'].nunique()} regulated genes, "
      f"{gene['bio_context'].nunique()} tissues")

gene.groupby("regulated_gene_symbol", dropna=False).agg(
    tissues=("bio_context", "nunique"),
    best_p=("p_value", "min"),
).sort_values("best_p").head(10)
```

`flanking_bp` widens a gene's range, for promoter and downstream
regions.

```python
for flank in (0, 5000, 50000):
    out = bf.report.run(REPORT, input_data=["DDT"], flanking_bp=flank,
                        p_value_max=1e-50).to_pandas()
    print(f"  flanking_bp={flank:>6,}  {len(out):>6,} rows")
```

### 6. Narrowing by tissue and significance

Tissue names come from the data. Pass whatever the bundle has.

```python
one_tissue = bf.report.run(
    REPORT, input_data=["DDT"],
    tissues=[tissues.iloc[0]["bio_context"]],
    p_value_max=1e-50,
).to_pandas()

print(f"{tissues.iloc[0]['bio_context']}: {len(one_tissue):,} rows")
one_tissue[["regulated_gene_symbol", "beta", "se", "p_value", "n"]].head(6)
```

### 7. Two kinds of null, both meaningful

**`regulated_gene_symbol` null** — GTEx names its target by Ensembl id,
and roughly 17% of those on chr22 have no BF4 entity (lncRNAs and
pseudogenes without HGNC symbols). The evidence is real;
`regulated_gene_id` is always there.

**`position_gene_symbol` null** — the variant falls outside every gene
body. Common, expected, and exactly the case this report is for.

```python
wide = bf.report.run(REPORT, input_data=["22:23913109"]).to_pandas()

print("rows:", len(wide))
print("regulated genes without a BF4 symbol:",
      int(wide["regulated_gene_symbol"].isna().sum()))
print("variant outside any gene body:",
      bool(wide["position_gene_symbol"].isna().all()))
```

### 8. Export

```python
for path in result.write(OUTPUT_DIR / "expand_variant_regulatory.csv"):
    print(path)
```

### 9. The same thing on the command line

```bash
biofilter report run --report-name expand_variant_regulatory \\
    --input APOE \\
    --param p_value_max=1e-8 --param flanking_bp=5000 \\
    --output regulatory.csv
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__pair_genes.ipynb.md ===== -->

<h1>🎛️ Biofilter — Report: <code>pair_genes</code></h1>

Which of these genes are related, and by what — and optionally, what that
implies about a list of your own.

Two stages: **connect** genes through a shared pathway, disease or
protein, then **expand** — only if you ask — by a gene → item mapping you
supply.

Section 4 is the one to read. It is why this report exists rather than
being a mode of `pair_variants`.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

BUNDLE = None
REPORT = "pair_genes"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

GENES = ["CHEK2", "SMARCB1", "NF2"]
print(bf.core.db_uri)
```

### 2. Gene pairs, on their own

No mapping: the answer is which of your genes are related, and by what.
That question stands by itself, which is the argument for this being a
report rather than a parameter of another one.

```python
pairs = bf.report.run(REPORT, input_data=GENES, group_types=["Proteins"])
df = pairs.to_pandas()

print(f"{len(df)} gene pairs, tables: {list(pairs.tables)}")
df[["gene_1_symbol", "gene_2_symbol", "group_support_count",
    "group_support_source_count", "group_support_sources"]]
```

`group_support_sources` names the curation from the bundle rather than
guessing it from an accession prefix. Two curations agreeing is a
different claim from one curation saying it twice, which is what
`min_group_sources` filters on — and what `min_group_support` does not.

### 3. `max_group_size` decides the size *and* the meaning

A pathway naming 2,615 genes links its members while saying almost
nothing about any of them. When a result comes back empty or thin, this
is usually why — so the provenance says what the cut removed.

```python
for size in (200, 300, 1000, 0):
    out = bf.report.run(REPORT, input_data=GENES, group_types=["Proteins"],
                        max_group_size=size)
    label = "no limit" if size == 0 else str(size)
    print(f"  max_group_size={label:<9} {out.num_rows:>3} pairs")

pairs.provenance["group_filter"]
```

### 4. Why this is not a mode of `pair_variants`

`pair_variants` derives "this variant belongs to this gene" from
coordinates. That is right for a coding variant and wrong for a
regulatory one: a variant sits in one gene and acts on another.

Ask the bundle how often those differ.

```python
from biofilter.modules.report import Bundle

with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    disagreement = bundle.con.execute("""
        WITH linked AS (
            SELECT DISTINCT
                g.chromosome, g.position, g.reference_allele, g.alternate_allele,
                g.gene_id AS regulated, e.gene AS sits_in
            FROM variant_gtex g
            JOIN variant_molecular_effects e
              ON e.chromosome = g.chromosome AND e.position = g.position
             AND e.reference_allele = g.reference_allele
             AND e.alternate_allele = g.alternate_allele
            WHERE e.gene IS NOT NULL AND g.gene_id IS NOT NULL
        )
        SELECT count(*) AS pairs,
               count(*) FILTER (WHERE regulated <> sits_in) AS different_gene
        FROM linked
    """).to_arrow_table().to_pandas()

share = disagreement.different_gene[0] / max(disagreement.pairs[0], 1)
print(f"{disagreement.pairs[0]:,} variant x gene links carrying both kinds of evidence")
print(f"{share:.1%} name a gene other than the one the variant sits in")
```

So when your evidence for the attachment comes from outside Biofilter —
a colocalization, a fine-mapping, a curated list — `pair_variants` cannot
use it: its stage 3 re-derives membership from coordinates and drops
anything that disagrees, silently.

`pair_genes` never derives it. It takes the link you supply.

### 5. How you name a gene

Three mechanisms, and you say which — for `input_data` and the mapping
alike, since it is one decision about one thing.

| `gene_identifier` | Looks in |
| --- | --- |
| `alias` (default) | every alias: symbols, synonyms, HGNC, Ensembl, Entrez |
| a code system — `HGNC`, `ENTREZ`, `ENSEMBL`, … | only that system |
| `entity_id` (or `biofilter_id`) | the bundle's key, skipping aliases |

This is a parameter rather than something the report works out, because
nothing can tell them apart by looking.

```python
from biofilter.modules.report import Bundle

with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    collisions = bundle.con.execute("""
        SELECT count(*) AS numeric_aliases,
               count(*) FILTER (WHERE gm.entity_id IS NOT NULL) AS also_an_entity_id
        FROM entity_aliases a
        LEFT JOIN gene_masters gm
               ON gm.entity_id = TRY_CAST(a.alias_value AS BIGINT)
              AND gm.entity_id <> a.entity_id
        WHERE TRY_CAST(a.alias_value AS BIGINT) IS NOT NULL
    """).to_arrow_table().to_pandas()

print(f"{collisions.numeric_aliases[0]:,} aliases are bare numbers (Entrez ids)")
print(f"{collisions.also_an_entity_id[0]:,} of them are the entity id of a *different* gene")
print("\nEntrez 2 is A2M. Entity 2 is A1BG-AS1. A report that guessed would")
print("return the wrong gene and say nothing.")
```

```python
# The same three genes, named three ways.
with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    ids = bundle.con.execute(f"""
        SELECT gm.symbol, gm.entity_id,
               max(CASE WHEN a.xref_source='ENSEMBL' THEN a.alias_value END) AS ensembl
        FROM gene_masters gm
        JOIN entity_aliases a ON a.entity_id = gm.entity_id
        WHERE gm.symbol IN ('CHEK2', 'SMARCB1', 'NF2')
        GROUP BY 1, 2
    """).to_arrow_table().to_pandas()

for label, values, how in [
    ("symbols",     ids.symbol.tolist(),               None),
    ("Ensembl ids", ids.ensembl.tolist(),              "ensembl"),
    ("entity ids",  ids.entity_id.astype(str).tolist(), "entity_id"),
]:
    kw = {"gene_identifier": how} if how else {}
    out = bf.report.run(REPORT, input_data=values, group_types=["Proteins"], **kw)
    print(f"  {label:<12} -> {out.num_rows} gene pairs")
```

Naming the code system is also a **narrower** search, not just a
disambiguation: under `gene_identifier=entrez`, `2` can only be A2M
because no other column is consulted.

`entity_id` is the bundle's own key — exact, and scoped to the build that
issued it (ADR-003 §2.5). A list of entity ids belongs with the
`bundle_id` it came from.

### 6. The mapping: two columns, gene then item

Many-to-many in both directions. The worked example from ADR-005: three
items on one gene, two on the other, one pair between them.

```python
MAPPING = {
    "CHEK2":   ["111", "222", "333"],
    "SMARCB1": ["444", "555"],
}

expanded = bf.report.run(REPORT, input_data=["CHEK2", "SMARCB1"],
                         group_types=["Proteins"], mapping=MAPPING)

print(f"tables: {list(expanded.tables)}")
print(f"  result     {expanded.num_rows} item pairs   <- what write() exports")
print(f"  gene_pairs {expanded.extra_tables['gene_pairs'].num_rows} gene pair")
expanded.to_pandas()[["item_1", "item_2", "gene_1_symbol", "gene_2_symbol"]]
```

```python
# The same thing from a file, which is what a real mapping arrives as.
mapping_file = OUTPUT_DIR / "demo_mapping.tsv"
mapping_file.write_text("\n".join(
    f"{gene}\t{item}" for gene, items in MAPPING.items() for item in items) + "\n")

from_file = bf.report.run(REPORT, input_data=["CHEK2", "SMARCB1"],
                          group_types=["Proteins"], mapping_file=str(mapping_file))
print("same answer:", from_file.num_rows == expanded.num_rows)
```

### 7. The item is never read

Which is what makes gene→position, gene→rsID, gene→probe and
gene→exposure one feature instead of four.

**Biofilter is build 38 and managing build is yours** — but nothing here
interprets a coordinate, so build-37 positions pass through correctly and
Biofilter never has to know what one is.

```python
anything = bf.report.run(
    REPORT, input_data=["CHEK2", "SMARCB1"], group_types=["Proteins"],
    mapping={"CHEK2": ["17:7579472", "exposure:smoking"],
             "SMARCB1": ["probe_0042"]},
).to_pandas()

anything[["item_1", "item_2"]]
```

The price of that is real and worth stating: the report cannot filter by
allele frequency, resolve an rsID, or validate an item — and **two
spellings of one thing are two things**. `22:100:A:G` and
`chr22:100:A:G` are different items, and that bounds what the
deduplication below can promise.

### 8. Three rules the simple example does not show

The cross product is not the work. These are, and they are identical for
every caller — which is the argument for the platform owning them rather
than each analysis re-deriving them.

```python
# An item on both genes would otherwise pair with itself.
self_pair = bf.report.run(
    REPORT, input_data=["CHEK2", "SMARCB1"], group_types=["Proteins"],
    mapping={"CHEK2": ["X", "111"], "SMARCB1": ["X", "444"]},
).to_pandas()

print("pairs:", sorted(zip(self_pair.item_1, self_pair.item_2)))
print("X paired with itself:", bool((self_pair.item_1 == self_pair.item_2).any()))
```

```python
# Deduplication is global, not per gene pair: the same item pair arrives
# through every gene pair linking it. On one real run that was 4.3% of
# the answer — 72,554 against the 75,794 a naive sum(n1 x n2) reports.
three = bf.report.run(
    REPORT, input_data=GENES, group_types=["Proteins"],
    mapping={"CHEK2": ["A"], "SMARCB1": ["B"], "NF2": ["A"]},
)
print(f"{three.extra_tables['gene_pairs'].num_rows} gene pairs "
      f"-> {three.num_rows} item pair(s)")
three.to_pandas()[["item_1", "item_2"]]
```

### 9. Both genes must carry an item

Not "both were named". A gene can be in your input and carry nothing, and
then there is nothing on its side to pair.

For the same reason `membership="either"` is refused with a mapping: the
partner gene came from the bundle, not from your list.

```python
partial = bf.report.run(REPORT, input_data=GENES, group_types=["Proteins"],
                        mapping={"CHEK2": ["A"], "SMARCB1": ["B"]})
print(f"NF2 is in the input and carries nothing -> {partial.num_rows} pair(s)")

try:
    bf.report.run(REPORT, input_data=["CHEK2"], membership="either",
                  mapping={"CHEK2": ["A"]})
except ValueError as exc:
    print("\nrefused:", exc)
```

### 10. Export, and keeping both tables

`write()` exports the primary table, which is the item pairs when you
asked for them. `save()` keeps everything.

```python
for path in expanded.write(OUTPUT_DIR / "pair_genes.csv"):
    print(path)

saved = expanded.save(OUTPUT_DIR / "runs" / "pair_genes", overwrite=True)
print("\nsaved:", saved)

from biofilter.modules.report.result import ReportResult
back = ReportResult.load(saved)
print("tables back:", list(back.tables))
```

### 11. The same thing on the command line

```bash
biofilter report run --report-name pair_genes \\
    --input CHEK2 --input SMARCB1 --input NF2 \\
    --param group_types=Proteins \\
    --param max_group_size=300 \\
    --param mapping_file=./variant_to_gene.tsv \\
    --output item_pairs.csv
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__pair_variants.ipynb.md ===== -->

<h1>🎛️ Biofilter — Report: <code>pair_variants</code></h1>

Candidate variant × variant pairs whose genes share biology.

**It takes variants** — rsIDs, `chr:pos`, `chr:pos:ref:alt` — and pairs
the ones you named. Section 5 is about what it deliberately no longer
does, and how to get it back in one visible step.

### 1. Open a bundle, and get some variants

```python
from pathlib import Path

from biofilter import Biofilter
from biofilter.modules.report import Bundle

BUNDLE = None
REPORT = "pair_variants"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# A realistic input: common variants in three genes that share biology.
with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    VARIANTS = [r[0] for r in bundle.con.execute("""
        SELECT v.variant_key
        FROM variant_masters v
        JOIN entity_locations l ON l.build = 38 AND l.chromosome = v.chromosome
         AND v.position BETWEEN l.start_pos AND l.end_pos
        JOIN gene_masters gm ON gm.entity_id = l.entity_id
        WHERE gm.symbol IN ('CHEK2', 'SMARCB1', 'NF2') AND v.af_joint > 0.05
        ORDER BY v.af_joint DESC LIMIT 40
    """).fetchall()]

print(f"{len(VARIANTS)} variants, e.g. {VARIANTS[:2]}")
```

### 2. What can link two genes in this bundle

A *group* is the entity sitting between two genes: a pathway they share,
a disease both are implicated in, a protein both interact with.

```python
with Bundle.open(bf.core.db_uri.removeprefix("parquet://")) as bundle:
    groups = bundle.con.execute("""
        WITH ge AS (SELECT e.id FROM entities e JOIN entity_groups eg ON eg.id = e.group_id
                    WHERE eg.name = 'Genes'),
        link AS (
            SELECT r.entity_1_id AS grp, r.entity_2_id AS gene FROM entity_relationships r
            WHERE r.entity_2_id IN (SELECT id FROM ge) AND r.entity_1_id NOT IN (SELECT id FROM ge)
            UNION ALL
            SELECT r.entity_2_id, r.entity_1_id FROM entity_relationships r
            WHERE r.entity_1_id IN (SELECT id FROM ge) AND r.entity_2_id NOT IN (SELECT id FROM ge))
        SELECT eg.name AS group_type, count(DISTINCT l.grp) AS groups
        FROM link l JOIN entities e ON e.id = l.grp
        JOIN entity_groups eg ON eg.id = e.group_id
        GROUP BY 1 ORDER BY 2 DESC
    """).to_arrow_table().to_pandas()

groups
```

### 3. Pair them

One row is a pair of your variants whose genes share at least one
group.

```python
pairs = bf.report.run(REPORT, input_data=VARIANTS, group_types=["Proteins"])
df = pairs.to_pandas()

print(f"{len(df):,} pairs from {len(VARIANTS)} variants")
df[["variant_1_key", "gene_1_symbol", "variant_2_key", "gene_2_symbol",
    "group_support_count", "group_support_sources"]].head(6)
```

### 4. `max_group_size`, and why an empty result is not "no biology"

A pathway naming 2,615 genes links its members while saying almost
nothing about any of them. When a result comes back empty or thin, this
is usually why — so the provenance says what the cut removed.

```python
for size in (200, 300, 1000, 0):
    out = bf.report.run(REPORT, input_data=VARIANTS, group_types=["Proteins"],
                        max_group_size=size)
    label = "no limit" if size == 0 else str(size)
    print(f"  max_group_size={label:<9} {out.num_rows:>7,} pairs")

pairs.provenance["group_filter"]
```

### 5. Starting from genes

This report used to accept gene names and expand each one into the
variants inside it. It no longer does, and the reason is not tidiness.

A gene on chr22 holds about 4,000 variants. The report kept **100** of
them, ranked by allele frequency, and you never saw which 100. Running
the expansion yourself costs one step and puts that choice in front of
the person making it.

```python
try:
    bf.report.run(REPORT, input_data=["CHEK2", "SMARCB1"])
except ValueError as exc:
    print(exc)
```

```python
# One visible step instead of one invisible one — and you can look at
# and filter the list before anything is paired.
expanded = bf.report.run("expand_gene_to_variant",
                         input_data=["CHEK2", "SMARCB1"],
                         mapping="position", af_max=0.01,
                         impact_filter=["HIGH", "MODERATE"],
                         max_variants_per_gene=0).to_pandas()

keys = expanded.query("status == 'ok'").variant_key.drop_duplicates().tolist()
print(f"{len(keys):,} rare, damaging variants — yours to filter further")

from_genes = bf.report.run(REPORT, input_data=keys[:200], group_types=["Proteins"])
print(f"{from_genes.num_rows:,} pairs")
```

Three parameters left with it. Passing one is an error rather than a
silent change of answer:

| gone | instead |
| --- | --- |
| gene names in `input_data` | `expand_gene_to_variant`, then pair its output |
| `max_variants_per_gene` | the same parameter on `expand_gene_to_variant` |
| `membership="either"` | `pair_genes(membership="either")` → expand the partners → pair |

```python
for name, value in [("membership", "either"),
                    ("max_variants_per_gene", 10),
                    ("output_grain", "gene_pairs")]:
    try:
        bf.report.run(REPORT, input_data=VARIANTS[:4], **{name: value})
    except ValueError as exc:
        print(f"{name}: {str(exc).splitlines()[-1].strip()}\n")
```

### 6. Support, and what it is not

`group_support_count` counts the groups linking the two genes;
`group_support_source_count` counts the **curations** that asserted them,
read from the bundle rather than guessed from an accession prefix. Two
curations agreeing is not one curation saying it twice.

```python
for support in (1, 10, 30):
    out = bf.report.run(REPORT, input_data=VARIANTS, group_types=["Proteins"],
                        min_group_support=support).to_pandas()
    print(f"  min_group_support={support:>3}  {len(out):>7,} pairs")

df.nlargest(5, "group_support_count")[
    ["gene_1_symbol", "gene_2_symbol", "group_support_count",
     "group_support_source_count", "group_support_sources"]
]
```

It is a weight for ranking candidates — not a p-value, and not evidence
of interaction. Change `max_group_size` and every count changes.

### 7. Gene pairs are a different question

Stage 2 on its own — which genes are related, and by what — is
`pair_genes`. It also expands a gene pair by a list **you** supply, which
is what to reach for when the variant-to-gene attachment comes from
outside the bundle: a colocalization, a fine-mapping, a curated
assignment.

This report derives that attachment from coordinates, which is right for
a coding variant and wrong for a regulatory one.

```python
partners = bf.report.run("pair_genes", input_data=["CHEK2"],
                         group_types=["Proteins"], membership="either").to_pandas()

print(f"{len(partners):,} partner genes for CHEK2")
partners[["gene_1_symbol", "gene_2_symbol", "gene_2_from_input",
          "group_support_count", "group_support_sources"]].head(6)
```

### 8. Export

```python
for path in pairs.write(OUTPUT_DIR / "pair_variants.csv"):
    print(path)
```

### 9. The same thing on the command line

```bash
biofilter report run --report-name pair_variants \\
    --input-file my_variants.txt \\
    --param group_types=Proteins \\
    --param max_group_size=300 \\
    --param min_group_sources=2 \\
    --output pairs.csv
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__platform_data_statistics.ipynb.md ===== -->

<h1>📊 Biofilter — Report: <code>platform_data_statistics</code></h1>

What this bundle holds: how much, of what, and how big.

A platform report — it describes the **bundle**, not the biology in it.
Reach for it when you pick up a bundle you did not build and want to know
what is actually in there.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "platform_data_statistics"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

bf
```

### 2. One row per measurement

Heterogeneous statistics do not fit a wide table, so this one is long:
`section` and `metric` name the measurement, `dimension_1` and
`dimension_2` say what it is measured by.

Two sections cannot be held faithfully that way, and come back as tables
of their own as well — sections 4 and 6 below.

```python
result = bf.report.run(REPORT)
stats = result.to_pandas()

print(f"{len(stats)} measurements")
stats.groupby("section").size()
```

### 3. Which build is this

`bundle_id` is what ties a result back to the data that produced it —
the same id the provenance sidecar carries.

```python
stats[stats["section"] == "bundle"][
    ["metric", "value_number", "value_text", "note"]
]
```

`tables_without_rows` is the number to watch. A declared table with no
rows is a source that was planned and did not land — it gets a
measurement of its own rather than hiding in the per-table list.

### 4. How big, per table

Rows, bytes and file count come from `manifest.json`, so this section
costs **no I/O at all** — the sizes of a multi-gigabyte bundle are read
from a few hundred lines of JSON.

In the long table a size survives twice and neither is usable:
`value_text` rounds it to `"3.4 MB"` and `note` buries the figure in
`"6 file(s), 3249124626 bytes"`. So `storage` also comes back as its own
table, where `bytes` is an integer you can sort by.

```python
storage = result.extra_tables["storage"].to_pandas()

print(f'{len(storage)} tables, {storage["bytes"].sum() / 1e9:.1f} GB in total')
storage.nlargest(12, "bytes")[["table", "branch", "rows", "bytes", "files"]]
```

```python
import time

# The manifest-only sections, timed.
started = time.perf_counter()
bf.report.run(REPORT, sections=["bundle", "storage"])
print(f"bundle + storage: {time.perf_counter() - started:.2f}s")

started = time.perf_counter()
bf.report.run(REPORT)
print(f"everything:       {time.perf_counter() - started:.2f}s")
```

### 5. What kinds of thing are in here

```python
stats[stats["section"] == "entities"][["dimension_1", "value_number"]]
```

### 6. Variants per chromosome

Grouped from the data, not parsed out of filenames: the manifest counts
rows per *file*, and a file happening to be one chromosome is a
convention of the current build rather than a guarantee.

It is affordable because `chromosome` is a real column with row-group
statistics — hundreds of millions of rows group in about a second.

Here too the long shape loses something: `dimension_2` is a string, so
sorting it gives 1, 10, 11, 2. The `variants` table has the chromosome
as an integer.

```python
variants = result.extra_tables.get("variants")

if variants is not None:
    frame = variants.to_pandas()
    wide = frame.pivot_table(
        index="chromosome", columns="table", values="rows", aggfunc="sum"
    ).sort_index()
    display(wide)
else:
    print("this bundle carries no variant tables")
```

⚠️ **A bundle built for a subset of chromosomes shows exactly that.**
The chromosomes listed here are the ones the bundle has; every variant
count elsewhere in Biofilter is about those and silent about the rest.
Worth reading before concluding anything about the genome.

### 7. How things are connected

```python
pairs = stats[
    (stats["section"] == "relationships")
    & (stats["metric"] == "relationships_by_group_pair")
]

pairs[["dimension_1", "dimension_2", "value_number"]].head(10)
```

```python
stats[
    (stats["section"] == "relationships")
    & (stats["metric"] == "relationships_by_type")
][["dimension_1", "value_number", "value_text"]]
```

### 8. What each source contributed

Every data source is listed, including ones that never ran — those have a
null `value_text`. `platform_etl_status` is where to go for why.

```python
sources = stats[stats["section"] == "sources"]

print(f"{len(sources)} sources; {int(sources['value_text'].isna().sum())} never ran")
sources[["dimension_1", "dimension_2", "value_number", "value_text", "as_of"]].head(10)
```

### 9. Export

```python
for path in result.write(OUTPUT_DIR / "platform_data_statistics.csv"):
    print(path)
```

### 10. The same thing on the command line

```bash
biofilter report run --report-name platform_data_statistics --output stats.csv

# Just the free parts:
biofilter report run --report-name platform_data_statistics \\
    --param sections=bundle --param sections=storage
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__platform_etl.ipynb.md ===== -->

<h1>🏗️ Biofilter — Platform: <code>platform_etl_status</code> and <code>platform_etl_packages</code></h1>

What ran to produce this bundle, and whether it holds up.

Platform reports describe the **bundle**, not the biology in it. They
take no input — there is nothing to ask about.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

bf
```

### 2. The summary: one row per data source

```python
result = bf.report.run("platform_etl_status")
status = result.to_pandas()

print(f"{len(status)} data sources in bundle {result.provenance['bundle_id']}")
status.groupby(["branch", "pipeline_state"]).size()
```

### 3. `pipeline_state`, and why it is not a boolean

| state | meaning | `pipeline_ok` |
| --- | --- | --- |
| `ok` | every required stage ran, each on the previous one's output | true |
| `unverifiable` | every stage ran, no hashes to prove they belong together | true |
| `misaligned` | a stage ran on something else | false |
| `incomplete` | a required stage is missing | false |
| `never_run` | no packages at all | false |

`pipeline_ok` means **nothing is known to be wrong** — weaker than
"everything is proven right". `unverifiable` is the gap between them.

```python
status[~status["pipeline_ok"]][
    ["source_system", "data_source", "branch", "pipeline_state", "latest_error"]
]
```

### 4. Why this replaced a simple boolean

The report this replaces returned `pipeline_ok = False` for **51 of this
bundle's 68 sources**, and not one of them was broken.

The variant branch writes parquet straight from transform — there is no
load stage to miss. A report that flags a design decision as a failure
teaches people to ignore it.

```python
variant = status[status["branch"] == "variant"]

print(f"{len(variant)} variant sources")
print("with a load stage:", int(variant["load_package_id"].notna().sum()))
print("reported ok:", int(variant["pipeline_ok"].sum()))
```

### 5. `unverifiable` is not `misaligned`

Some DTPs read database state rather than a downloaded file, so they
produce no hash and alignment cannot be shown either way.
`transform_aligned` is **null** there rather than false — false reads as
"this is wrong" rather than "this is unproven".

```python
status[status["pipeline_state"] == "unverifiable"][
    ["data_source", "transform_aligned", "load_aligned", "pipeline_ok"]
]
```

### 6. A failure in the history, and a source that is fine anyway

`latest_error` is the most recent failure whether or not it was retried
successfully. `pipeline_state = 'ok'` together with a non-null
`latest_error` means "it worked, but not on the first try".

```python
status[status["latest_error"].notna()][
    ["data_source", "pipeline_state", "pipeline_ok", "latest_error"]
]
```

### 7. The packages behind the summary

`platform_etl_packages` is the unaggregated record: one row per package,
which is one stage of one run. Reach for it when the summary says
something surprising.

```python
packages = bf.report.run("platform_etl_packages").to_pandas()

print(f"{len(packages)} packages")
packages.groupby(["operation_type", "status"]).size()
```

### 8. What "aligned" actually means

Each stage is its own package, and the digest of the extract's output is
carried forward — it reappears as the transform's hash, then the load's.
Alignment means a stage ran on the previous one's output, not that two
unrelated digests happen to match.

```python
one = packages[packages["data_source"] == "biogrid"]

one[["package_id", "operation_type", "extract_hash", "transform_hash", "load_hash"]]
```

### 9. Failures stay in the record

`platform_etl_status` reports the latest **good** stage, so a source can
read `ok` while a failed package sits here. That is the pair to look at
together.

```python
failed = packages[packages["status"].str.contains("fail", case=False, na=False)]

failed[["package_id", "data_source", "operation_type", "status", "note"]]
```

### 10. Export

```python
for path in result.write(OUTPUT_DIR / "platform_etl_status.csv"):
    print(path)
```

### 11. The same thing on the command line

```bash
biofilter report run --report-name platform_etl_status --output status.csv

biofilter report run --report-name platform_etl_packages \\
    --param operation_type=load --output loads.csv
```



<!-- ===== SOURCE FILE: notebooks/templates/reports__resolve_entity.ipynb.md ===== -->

<h1>🔎 Biofilter — Report: <code>resolve_entity</code></h1>

Does the bundle know these names, and unambiguously?

Run this **before** any other report. It tells you which of your inputs
will resolve, which are ambiguous, and which the bundle has never heard
of — the three things that quietly distort every downstream result.

### 1. Open a bundle

```python
from pathlib import Path

from biofilter import Biofilter

# Leave as None to use `[database] bundle` from .biofilter.toml.
BUNDLE = None
REPORT = "resolve_entity"

bf = Biofilter(bundle=BUNDLE, debug_mode=False) if BUNDLE else Biofilter(debug_mode=False)

# Results land here whatever directory the kernel was started in — VS Code
# and Jupyter disagree about that, and a bare filename ends up wherever
# they landed. The project root is the folder holding .biofilter.toml.
_root = next(
    (p for p in [Path.cwd(), *Path.cwd().parents] if (p / ".biofilter.toml").is_file()),
    Path.cwd(),
)
OUTPUT_DIR = _root / "notebooks" / "templates" / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
for path in result.write(OUTPUT_DIR / "resolve_entity.csv"):
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

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
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest

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

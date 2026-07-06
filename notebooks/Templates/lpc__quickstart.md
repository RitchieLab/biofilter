# Biofilter 4 on the LPC — Quickstart

Paste, run, get a CSV. That's it.

> **Audience:** LPC users who want to query the BF4 knowledge base. You
> don't need to know anything about containers, databases, or Python.
> If you're administering the BF4 environment on the cluster, see
> [lpc__deploy.md](lpc__deploy.md) instead.

---

## Activate BF4

BF4 lives inside the lab's shared module tree. Two steps:

```bash
source /project/hall_shared/hall_shared.sh          # makes lab modules visible
module load biofilter/4.2.0                # activates BF4 4.2.0 + bundle
```

The `biofilter/4.2.0` module puts the CLI on your PATH and points it at
the current Parquet snapshot — no `--db-uri` needed on every command.

> _Optional:_ add the two lines above to your `~/.bashrc` so every shell
> starts ready.

Confirm it worked:

```bash
biofilter --version
# Expected: biofilter 4.2.0
```

---

## Run a query

```bash
biofilter report run \
  --name annotation_master_gene \
  --input APOE \
  --output apoe.csv
```

Result: `apoe.csv` in the current directory.

That's the whole thing — no container, no PostgreSQL, no bind mounts.
Memory peak typically under 100 MB; runs in under a second.

---

## Change the query

Edit the report flags:

- `--name <report>` — which report to run (see list below)
- `--input "APOE,TP53,BRCA1"` — comma-separated values
- *or* `--input-file genes.txt` — one item per line
- `--output <name>.csv` — your output filename

---

## Available reports

```bash
biofilter report list
```

Most common:

| Report | Input |
|---|---|
| `annotation_master_gene` | Gene symbols (e.g. `APOE`) |
| `annotation_master_variant` | rsIDs (e.g. `rs429358`) or `chr:pos` or `chr:pos:ref:alt` |
| `annotation_master_disease` | Disease names or MONDO IDs |
| `annotation_master_pathway` | Pathway names or Reactome/KEGG IDs |
| `annotation_master_chemical` | Chemical names or ChEBI IDs |
| `annotation_master_protein` | UniProt accessions |
| `annotation_master_go` | GO terms |
| `variant_modeling` | rsIDs — produces SNP×SNP pairs via shared biological groups |

---

## Get help on a specific report

```bash
biofilter report explain --report-name annotation_master_gene
```

Shows the parameters, accepted input formats, and output columns.

---

## Switching versions or snapshots (advanced)

**Different BF4 version** — if the lab publishes multiple versions,
`module avail biofilter` lists them and you pick with `module load`:

```bash
module avail biofilter
module load biofilter/4.3.0                # example, if available
```

**Different data snapshot** — the module sets `BIOFILTER_DB_URI` to the
current snapshot. To use another one for a single command:

```bash
biofilter --db-uri "parquet:///project/hall_shared/biofilter/databases/<other-date>/bundle/tables" \
  report run --name annotation_master_gene --input APOE --output out.csv
```

Or override the env for the whole session:

```bash
export BIOFILTER_DB_URI="parquet:///project/hall_shared/biofilter/databases/<other-date>/bundle/tables"
biofilter report list                       # now reads from the other snapshot
```

---

## Heavy workloads (LSF)

For very large input sets or many reports back-to-back, submit as an
LSF job. BF4 is memory-light (typically < 1 GB even for 10k variants),
so resource requests can be modest:

```bash
#!/bin/bash
#BSUB -J bf4-batch
#BSUB -o bf4-%J.log
#BSUB -W 1:00
#BSUB -M 4000
#BSUB -n 4

source /project/hall_shared/hall_shared.sh
module load biofilter/4.2.0

biofilter report run \
  --name annotation_master_variant \
  --input-file my_rsids.txt \
  --output results.csv
```

---

## What is BF4?

Biofilter 4 is an entity-centric biological knowledge platform: it lets
you query and annotate **genes, variants, pathways, diseases,
chemicals**, and the relationships among them, across many curated
source databases (HGNC, Ensembl, UniProt, Reactome, KEGG, GO, MONDO,
ClinGen, GWAS Catalog, gnomAD, AlphaMissense, …).

On the LPC, BF4 reads a Parquet bundle directly through DuckDB — no
database server, no import phase, multi-user safe by design. The same
bundle serves any number of concurrent users from shared storage.

- Full documentation: <https://biofilter.readthedocs.io/>
- Project repo: <https://github.com/RitchieLab/biofilter>

---

## Questions

Andre Rico — <andreluis.rico@pennmedicine.upenn.edu>

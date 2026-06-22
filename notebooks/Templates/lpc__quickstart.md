# Biofilter 4 on the LPC — Quickstart

Paste, run, get a CSV. That's it.

> **Audience:** LPC users who want to query the BF4 knowledge base. You
> don't need to know anything about containers, databases, or Python.
> If you're administering the BF4 environment on the cluster, see
> [lpc__deploy.md](lpc__deploy.md) instead.

---

## Activate BF4

A single shared helper script loads Python, activates the BF4 venv, and
points it at the current Parquet bundle:

```bash
source /project/hall_shared/biofilter/venv/bf4-activate.sh
```

Expected output:
```
✅ BF4 biofilter 4.2.0
DB: parquet:///project/hall_shared/biofilter/databases/20260514/bundle/tables ready (parquet bundle 20260514)
```

That's it — no container, no PostgreSQL, no bind mounts. The venv lives
on shared storage and is already configured.

> _Optional:_ add the line above to your `~/.bashrc` so every shell
> starts ready.

---

## Run a query

```bash
mkdir -p ~/bf4_output

biofilter report run \
  --name annotation_master_gene \
  --input APOE \
  --output ~/bf4_output/apoe.csv
```

Result: `~/bf4_output/apoe.csv`

That's the whole thing — no `--db-uri` needed (the activate script
exports it). Memory peak typically under 100 MB; runs in under a
second.

---

## Change the query

Edit the three report flags:

- `--name <report>` — which report to run (see list below)
- `--input "APOE,TP53,BRCA1"` — comma-separated values
- *or* `--input-file ~/bf4_output/genes.txt` — one item per line
- `--output ~/bf4_output/<name>.csv` — your output filename

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

## Pointing at a different bundle (advanced)

The activate script sets `BIOFILTER_DB_URI` to the current snapshot. If
you need a different snapshot, override it after activating:

```bash
source /project/hall_shared/biofilter/venv/bf4-activate.sh
export BIOFILTER_DB_URI="parquet:///project/hall_shared/biofilter/databases/<other-date>/bundle/tables"

biofilter report list   # now reads from the other snapshot
```

Or pass `--db-uri` explicitly to a single command without changing the
env:

```bash
biofilter --db-uri "parquet:///path/to/other/bundle/tables" \
  report run --name annotation_master_gene --input APOE --output out.csv
```

---

## Heavy workloads (SLURM)

For very large input sets or many reports back-to-back, submit as a
SLURM job. BF4 is memory-light (typically < 1 GB even for 10k variants),
so resource requests can be modest:

```bash
#!/bin/bash
#BSUB -J bf4-batch
#BSUB -o bf4-%J.log
#BSUB -W 1:00
#BSUB -M 4000
#BSUB -n 4

source /project/hall_shared/biofilter/venv/bf4-activate.sh

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

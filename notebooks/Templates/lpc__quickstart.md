# Biofilter 4 on the LPC — Quickstart

Paste, run, get a CSV. That's it.

> **Audience:** LPC users who want to query the BF4 knowledge base. You
> don't need to know anything about containers, databases, or Python.
> If you're administering the BF4 environment on the cluster, see
> [lpc__deploy.md](lpc__deploy.md) instead.

---

## One-time setup

```bash
module load python/3.12
source /project/hall_shared/biofilter/venv/bf4-4.2.0/bin/activate
```

That's it — no container, no PostgreSQL, no bind mounts. The BF4 venv is
already provisioned on shared storage.

> _Optional:_ add the two lines above to a `~/bin/bf4-activate.sh` and
> source it from your `~/.bashrc` so every shell is ready.

---

## Run a query

```bash
mkdir -p ~/bf4_output

biofilter \
  --db-uri "parquet:///project/hall_shared/biofilter/databases/20260514/bundle/tables" \
  report run \
    --name annotation_master_gene \
    --input APOE \
    --output ~/bf4_output/apoe.csv
```

Result: `~/bf4_output/apoe.csv`

That's the whole thing — no container, no SLURM job, no temp dirs. BF4
reads the Parquet bundle directly via DuckDB, in process. Memory peak
under 100 MB for typical queries.

---

## Change the query

Edit the last three flags (`--name`, `--input`, `--output`):

- `--name <report>` — which report to run (see list below)
- `--input "APOE,TP53,BRCA1"` — comma-separated values
- *or* `--input-file ~/bf4_output/genes.txt` — one item per line
- `--output ~/bf4_output/<name>.csv` — your output filename

---

## Available reports

```bash
biofilter \
  --db-uri "parquet:///project/hall_shared/biofilter/databases/20260514/bundle/tables" \
  report list
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
biofilter \
  --db-uri "parquet:///project/hall_shared/biofilter/databases/20260514/bundle/tables" \
  report explain --report-name annotation_master_gene
```

Shows the parameters, accepted input formats, and output columns.

---

## Tip: shorten the command

The `--db-uri` is the same every time. Export it once per session:

```bash
export BIOFILTER_DB_URI="parquet:///project/hall_shared/biofilter/databases/20260514/bundle/tables"

# Now every BF4 command picks it up automatically:
biofilter report run --name annotation_master_gene --input APOE --output ~/bf4_output/apoe.csv
biofilter report list
biofilter report explain --report-name annotation_master_variant
```

You can put that `export` in a `~/bin/bf4-activate.sh` alongside the
`module load` and `source venv` so a single source covers everything.

---

## Heavy workloads (SLURM)

For very large input sets or many reports back-to-back, submit as a
SLURM job. BF4 is memory-light (typically < 1 GB even for 10k variants)
so resource requests can be modest:

```bash
#!/bin/bash
#BSUB -J bf4-batch
#BSUB -o bf4-%J.log
#BSUB -W 1:00
#BSUB -M 4000
#BSUB -n 4

module load python/3.12
source /project/hall_shared/biofilter/venv/bf4-4.2.0/bin/activate
export BIOFILTER_DB_URI="parquet:///project/hall_shared/biofilter/databases/20260514/bundle/tables"

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

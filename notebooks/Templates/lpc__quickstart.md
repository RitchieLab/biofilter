# Biofilter 4 on the LPC — Quickstart

Paste, run, get a CSV. That's it.

> **Before you start:** set `PROJECT` to your LPC project allocation name
> (the folder under `/project/` you have access to). Run this once per shell
> session:
>
> ```bash
> export PROJECT=your-project-name
> ```

---

## Run a query

```bash
module load apptainer
mkdir -p ~/bf4_output

TMP=$(mktemp -d) && mkdir -p "$TMP/tmp" "$TMP/pg-run" && \
apptainer run --writable-tmpfs --pwd /tmp \
  --bind /project/${PROJECT}/datasets/bf4/20260514/pgdata:/var/lib/postgresql/data \
  --bind "$TMP/tmp:/tmp" \
  --bind "$TMP/pg-run:/var/run/postgresql" \
  --bind ~/bf4_output:/workspace \
  /project/${PROJECT}/env/modules/biofilter/4.1.2/bf4-hpc.sif \
  biofilter report run \
    --name annotation_master_gene \
    --input APOE \
    --output /workspace/apoe.csv && \
rm -rf "$TMP"
```

Result: `~/bf4_output/apoe.csv`

---

## Change the query

Edit the last three lines (`--name`, `--input`, `--output`):

- `--name <report>` — which report to run (see list below)
- `--input "APOE,TP53,BRCA1"` — comma-separated values
- _or_ `--input-file /workspace/genes.txt` — one item per line; put `genes.txt` inside `~/bf4_output/`
- `--output /workspace/<name>.csv` — your output filename (lands in `~/bf4_output/`)

---

## Available reports

Replace the last block (`biofilter report run ...`) with:

```bash
  biofilter report list
```

Most common:

| Report                       | Input                              |
| ---------------------------- | ---------------------------------- |
| `annotation_master_gene`     | Gene symbols (e.g. `APOE`)         |
| `annotation_master_variant`  | rsIDs (e.g. `rs429358`)            |
| `annotation_master_disease`  | Disease names or MONDO IDs         |
| `annotation_master_pathway`  | Pathway names or Reactome/KEGG IDs |
| `annotation_master_chemical` | Chemical names or ChEBI IDs        |

---

## Get help on a specific report

Replace the last block with:

```bash
  biofilter report explain --report-name annotation_master_gene
```

Shows the parameters, accepted input formats, and output columns for that
report.

---

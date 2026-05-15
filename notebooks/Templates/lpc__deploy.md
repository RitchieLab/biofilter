# Running Biofilter 4 on the Penn LPC

Copy-paste-ready guide to run BF4 reports on the LPC cluster. No installation,
no PostgreSQL setup — just `module load apptainer` and go.

> **Audience:** LPC users (researchers, students) who want to query the BF4
> knowledge base for their analyses. You don't need to know anything about
> containers, databases, or Python to use this.

---

## TL;DR — the fastest possible report

Paste this into a terminal on the LPC. You'll get a CSV of gene annotations
for `APOE` in your home directory.

```bash
module load apptainer

DB_DIR=/project/ritchie/datasets/bf4/20260514
SIF=/project/ritchie/env/modules/biofilter/4.1.2/bf4-hpc.sif
OUTPUT_DIR=$HOME/bf4_output

TMP_DIR=$(mktemp -d -t bf4-XXXXXX)
mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run" "${OUTPUT_DIR}"

apptainer run \
  --writable-tmpfs \
  --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  --bind "${OUTPUT_DIR}:/workspace" \
  "${SIF}" \
  biofilter report run \
    --name annotation_master_gene \
    --input APOE \
    --output /workspace/apoe_annotation.csv

rm -rf "${TMP_DIR}"
```

Result: `~/bf4_output/apoe_annotation.csv`.

---

## What's where on the LPC

| Path | What it is |
|---|---|
| `/project/ritchie/env/modules/biofilter/<version>/bf4-hpc.sif` | The Apptainer image (contains BF4 + PostgreSQL bundled). Versioned. |
| `/project/ritchie/datasets/bf4/<snapshot-date>/pgdata/` | The BF4 database snapshot. Dated. |
| `$HOME/bf4_output/` | Your output folder (you choose where) |

See what's available right now:

```bash
ls /project/ritchie/env/modules/biofilter/
ls /project/ritchie/datasets/bf4/
```

Pick the **snapshot date** that matches your analysis cutoff. Newer snapshots
include more curated data; older snapshots are kept for reproducibility of
already-published work.

---

## Reusable script template

Save this as `~/scripts/run_bf4.sh`. Edit the `REPORT_*` variables, then run.

```bash
#!/bin/bash
# Run a Biofilter 4 report on the LPC.
# Edit the values below, then: bash run_bf4.sh
set -euo pipefail

# ---------- What to run ----------
REPORT_NAME="annotation_master_gene"
REPORT_INPUT="APOE,TP53,BRCA1"            # comma-separated, OR use REPORT_INPUT_FILE
REPORT_INPUT_FILE=""                       # absolute path to a file, OR leave empty
REPORT_OUTPUT_NAME="my_report.csv"

# ---------- Which BF4 / snapshot ----------
BF4_VERSION="4.1.2"
DB_VERSION="20260514"

# ---------- Where outputs go ----------
OUTPUT_DIR="${HOME}/bf4_output"

# ---------- Don't edit below ----------
module load apptainer

DB_DIR="/project/ritchie/datasets/bf4/${DB_VERSION}"
SIF="/project/ritchie/env/modules/biofilter/${BF4_VERSION}/bf4-hpc.sif"

TMP_DIR=$(mktemp -d -t bf4-XXXXXX)
trap "rm -rf ${TMP_DIR}" EXIT
mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run" "${OUTPUT_DIR}"

# Decide between --input and --input-file
if [[ -n "${REPORT_INPUT_FILE}" ]]; then
  cp "${REPORT_INPUT_FILE}" "${OUTPUT_DIR}/_input.txt"
  INPUT_FLAGS=(--input-file /workspace/_input.txt)
else
  INPUT_FLAGS=(--input "${REPORT_INPUT}")
fi

apptainer run \
  --writable-tmpfs \
  --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  --bind "${OUTPUT_DIR}:/workspace" \
  "${SIF}" \
  biofilter report run \
    --name "${REPORT_NAME}" \
    "${INPUT_FLAGS[@]}" \
    --output "/workspace/${REPORT_OUTPUT_NAME}"

echo "✅ Done. Output: ${OUTPUT_DIR}/${REPORT_OUTPUT_NAME}"
```

Run it:
```bash
chmod +x ~/scripts/run_bf4.sh
bash ~/scripts/run_bf4.sh
```

---

## Common examples

### Gene annotations (input: gene symbols)

```bash
... biofilter report run \
    --name annotation_master_gene \
    --input "TP53,BRCA1,APOE" \
    --output /workspace/genes.csv
```

### Variant annotations (input: rsIDs from a file)

Create the input file in your output folder so the container can see it:

```bash
cat > $HOME/bf4_output/my_rsids.txt <<EOF
rs429358
rs7412
rs1801133
EOF
```

Then run:
```bash
... biofilter report run \
    --name annotation_master_variant \
    --input-file /workspace/my_rsids.txt \
    --output /workspace/variants.csv
```

### Disease annotations

```bash
... biofilter report run \
    --name annotation_master_disease \
    --input "Alzheimer disease,MONDO:0004975" \
    --output /workspace/diseases.csv
```

### List every available report

```bash
... biofilter report list
```

### Get help on a specific report's parameters

```bash
... biofilter report explain --report-name annotation_master_gene
```

---

## Running as a SLURM job (recommended for big runs)

For reports involving many genes/variants, submit to SLURM instead of running
on the login node:

```bash
#!/bin/bash
#SBATCH --job-name=bf4-report
#SBATCH --output=bf4-%j.log
#SBATCH --time=2:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4

module load apptainer

# ... paste the same body as the reusable script above ...
```

Submit:
```bash
sbatch run_bf4.slurm
```

Quick guidance on resources:

| Workload | Time | Memory |
|---|---|---|
| < 100 genes/variants | 5 min | 4 GB |
| 100 – 10k genes/variants | 30 min | 8 GB |
| > 10k or full-genome reports | 2-6 h | 16-32 GB |

---

## File location rules (important!)

The container has its own filesystem that is **not your home directory**.
To give it access to your files:

1. Put input files **inside the folder you mount as `/workspace`** (e.g., `$HOME/bf4_output/`)
2. Reference them inside the container as `/workspace/<filename>`
3. Outputs land in the same folder on the host

```bash
# On the host
echo -e "TP53\nBRCA1\nAPOE" > $HOME/bf4_output/genes.txt

# In the container
--input-file /workspace/genes.txt
--output    /workspace/result.csv
```

This is the **only** thing that trips up new users. Don't pass `--input-file /home/me/...` — the container can't see your home directly.

---

## Troubleshooting

**`apptainer: command not found`**  
You forgot `module load apptainer`. Run it first.

**`Permission denied` writing to output**  
Check `ls -ld $HOME/bf4_output` — must be writable by you. Try `chmod u+w` or pick a different folder you own.

**`No such file or directory: /home/...` inside the container**  
The container can't see your home directly. Move the file into `$OUTPUT_DIR` and reference it as `/workspace/<filename>`.

**Long startup before each run (~5–10 s)**  
Normal. The container starts PostgreSQL on every invocation. For batch processing many inputs, prefer one report run with a big `--input-file` over many small runs.

**Container exits immediately with no output**  
Run with verbose Apptainer:
```bash
apptainer --debug run ...
```
Common cause: missing `--bind` for one of the four required paths.

**`pg_ctl: another server might be running`**  
You ran two containers against the same `pgdata` at the same time. Only one process can read/write the DB. Kill the other, or use separate snapshot folders.

---

## What is BF4?

Biofilter 4 is an entity-centric biological knowledge platform: it lets you
query and annotate **genes, variants, pathways, diseases, chemicals**, and
the relationships among them, across many curated source databases (HGNC,
Ensembl, UniProt, Reactome, KEGG, GO, MONDO, ClinGen, GWAS Catalog, gnomAD,
AlphaMissense, …).

On the LPC, you don't install BF4 — the production database snapshot lives
on shared storage and the application runs inside an Apptainer image. You
just `module load apptainer` and run.

- Full documentation: <https://biofilter.readthedocs.io/>
- Project repo: <https://github.com/RitchieLab/biofilter>

---

## Getting help

- For BF4 usage questions: see `biofilter report explain --report-name <name>` and the `reports_explain/` folder in the repo
- For the LPC environment itself (image, database, storage layout): contact
  the maintainer — **Andre Rico** (<andreluis.rico@pennmedicine.upenn.edu>)

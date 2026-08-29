# BF4 Example Notebooks



<!-- ===== SOURCE FILE: notebooks/Templates/lpc__deploy.md ===== -->

# Deploying Biofilter 4 on the Penn LPC

Operational guide for the **maintainer** who installs or updates BF4 on the
LPC cluster: building the Parquet bundle from a production snapshot,
publishing it on shared storage, and managing future updates.

> **Audience:** the person who owns the BF4 environment on the cluster.
> If you just want to _run reports_, see [lpc\_\_quickstart.md](lpc__quickstart.md)
> instead.

---

## 0. What changed in 4.2.0

Up to 4.1.x, the LPC deployment restored a full PostgreSQL database on the
cluster: a ~480 GB `pgdata/` directory bind-mounted into a container that
embedded PG 16. That model is retired.

Since **4.2.0**, BF4 reads the knowledge base directly from a **Parquet
bundle** via DuckDB — see [ADR-001](../Andre/ADR/0001-duckdb-parquet-strategy.md)
for the rationale and benchmarks. The practical consequences for deployment:

| | 4.1.x (retired) | 4.2.0+ (current) |
|---|---|---|
| Storage on cluster | ~480 GB `pgdata/` | Parquet bundle (much smaller, columnar) |
| Database server | PG 16 inside the container | none |
| Concurrency | one postmaster → per-user copies | native, read-only, multi-user |
| First run | `initdb` + `pg_restore` (3–8 h) | none — read files in place |
| Image size | ~1.5 GB | ~400 MB |
| Write operations | possible on cluster | **not possible** — bundle is read-only |

The legacy flow is preserved in [Appendix A](#appendix-a--legacy-postgresql-deployment-41x)
for reference; do not use it for new deployments.

---

## 1. Directory layout

```
/project/hall_shared/biofilter/
├── images/
│   └── bf4-hpc-<version>.sif           ← Apptainer image (optional; see §5)
├── venv/
│   └── bf4-<version>/                  ← native venv used by the build jobs
├── jobs/                               ← the .bsub scripts and helper scripts
│   ├── fase1.bsub
│   ├── fase1_resume.bsub
│   ├── fase1_merge.bsub
│   ├── merge_variant_molecular_effects.py
│   └── regen_manifest.py
└── databases/
    └── <snapshot-date>/
        ├── bundle/
        │   ├── manifest.json
        │   └── tables/                 ← *.parquet — this is what users read
        ├── logs/                       ← LSF job output
        └── pgdata/                     ← only if building the bundle on the LPC (§3)
```

Reference copies of the build scripts live in this repo at
[`notebooks/Andre/`](../Andre/) — copy them to `jobs/` on the cluster.

Dating the snapshot folders lets you keep multiple generations side by side.
Treat each snapshot as **immutable** once published.

---

## 2. Prerequisites

- LPC account with write access to `/project/hall_shared/biofilter/`
- `apptainer` and `python/3.12` modules available on the cluster
- A native venv with BF4 installed (used by the merge/manifest steps, which
  run outside the container and need only `pyarrow`)
- Free space: budget for the Parquet bundle **plus** the `pgdata/` if you
  build on the cluster (§3). The build jobs warn below 250 GB free.

---

## 3. Building the Parquet bundle

The bundle is produced by `biofilter db export --format parquet`, which needs
a live PostgreSQL to read from. Two options:

- **On a dedicated PostgreSQL server** — export against it, then `rsync` only
  the bundle to the cluster. Avoids restoring a ~480 GB `pgdata/` on the LPC
  entirely. This was the VPS path; the VPS has since been decommissioned, so
  it applies only if such a server is provisioned again.
- **On the LPC** (what was done for the `20260514` snapshot) — restore the
  dump into a cluster-side `pgdata/` first (Appendix A, §A.2), then export
  using the legacy PG-bundled image. Documented below because it is the path
  that was actually exercised.

Either way, the bundle contents and the partition caveat in §3.2 are the same.

### 3.1 Why this is three jobs, not one

`variant_masters` and `variant_molecular_effects` are **partitioned by
chromosome** in PostgreSQL, with child tables named `<parent>_chr_<N>`
(1–22, X, Y, MT → 23, 24, 25).

`db export` discovers tables via SQLAlchemy's inspector, which on PostgreSQL
returns **both the partitioned parent and every child** as independent tables.
So a full export writes 25 `variant_molecular_effects_chr_N.parquet` files
_and_ a consolidated `variant_molecular_effects.parquet`.

The problem is the parent. Exporting it runs
`SELECT * FROM variant_molecular_effects`, forcing PostgreSQL to UNION all 25
partitions on every chunk. On the 1.79-billion-row table this is slow enough
to be impractical — the first attempt was killed after the job wall clock ran
out.

The consolidated file is **not optional**: DuckDB registers one view per
`*.parquet` but explicitly skips any file whose name contains `_chr_`
(otherwise the same rows would appear twice — once via the parent, once via
the children). Without `variant_molecular_effects.parquet`, that table simply
does not exist for the reports.

The workaround is to build the parent outside the database: export the 25
children (each a fast direct scan), then concatenate them with
`pyarrow.ParquetWriter` in streaming append mode.

### 3.2 The three phases

| Phase | Script | What it does |
|---|---|---|
| 1 | [`fase1.bsub`](../Andre/fase1.bsub) | Full `db export`. Completes every table except the giant partitioned parent. |
| 1-resume | [`fase1_resume.bsub`](../Andre/fase1_resume.bsub) | `db export --table variant_molecular_effects_chr_1 … _chr_25` — the 25 children only. |
| 1-merge | [`fase1_merge.bsub`](../Andre/fase1_merge.bsub) | Concatenates the children into the consolidated parent, then regenerates `manifest.json`. |

Each script has a configuration block at the top (`ROOT`, `SNAPSHOT`,
`VERSION`). Edit those, then submit:

```bash
cd /project/hall_shared/biofilter/jobs

bsub < fase1.bsub          # wait for completion, check logs/export-<jobid>.out
bsub < fase1_resume.bsub   # wait for completion
bsub < fase1_merge.bsub    # wait for completion
```

Phase 1 is expected to end with `variant_molecular_effects.parquet` missing or
truncated — that is normal and is exactly what phases 1-resume and 1-merge
exist to repair.

**Phase 1-merge runs in the native venv, not the container.** It needs only
`pyarrow`; no PostgreSQL, no Apptainer:

```bash
module load python/3.12
source /project/hall_shared/biofilter/venv/bf4-<version>/bin/activate

python jobs/merge_variant_molecular_effects.py \
    --in  databases/<snapshot>/bundle/tables \
    --out databases/<snapshot>/bundle/tables/variant_molecular_effects.parquet

python jobs/regen_manifest.py \
    --bundle databases/<snapshot>/bundle \
    --biofilter-version <version>
```

`regen_manifest.py` is required because the bundle was assembled by several
partial exports, so the `manifest.json` left by phase 1 covers only a subset
of the files. It rescans `tables/*.parquet`, rewrites the manifest in place,
and backs up the previous one as `manifest.json.bak`.

### 3.3 Schema note on null columns

`merge_variant_molecular_effects.py` promotes `null`-typed columns to nullable
`string` before writing. Without this, a first chunk coming from an all-null
partition fixes the schema as `null` and every later chunk fails to cast.

The same fix exists in
[`transfer.py`](../../biofilter/modules/db/transfer.py) for the in-database
export path, and has been upstream **since 4.2.0**. The `.bsub` scripts from
the `20260514` build bind-mount a patched `transfer.py` over the one inside
the container:

```bash
--bind "${ROOT}/images/patches/transfer.py:/opt/biofilter/venv/lib/python3.11/site-packages/biofilter/modules/db/transfer.py:ro"
```

**Drop that bind when using a 4.2.0 or newer image** — the patch is already in
the installed package.

### 3.4 Verifying the bundle

```bash
BUNDLE=/project/hall_shared/biofilter/databases/<snapshot>/bundle

# every table present, and the consolidated parent among them
ls -1 "${BUNDLE}/tables"/*.parquet | wc -l
ls -lh "${BUNDLE}/tables/variant_molecular_effects.parquet"

# manifest covers all files and row counts look sane
python -c "import json,sys; m=json.load(open('${BUNDLE}/manifest.json')); \
print(len(m['tables']),'tables'); \
[print(f\"{t['name']:45s} {t['rows']}\") for t in m['tables'][:10]]"
```

Expected row counts for the current production data:

```
variant_masters                     152,084,680
variant_molecular_effects         1,793,092,126
variant_gene_regulatory_evidence     18,231,184
```

---

## 4. Publishing the bundle

Once verified, make the snapshot read-only so no user can corrupt it:

```bash
chmod -R a-w /project/hall_shared/biofilter/databases/<snapshot>/bundle
```

Read-only is enforced at three levels — filesystem permissions here, DuckDB
rejecting writes against `read_parquet` views, and the `Database.read_only`
flag at the application layer. A single copy safely serves the whole group;
no per-user duplication.

---

## 5. User access

Users reach the bundle through the lab's module tree
(see [lpc\_\_quickstart.md](lpc__quickstart.md)):

```bash
source /project/hall_shared/hall_shared.sh
module load biofilter/<version>
```

The modulefile must do two things:

1. put the `biofilter` CLI on `PATH` (from the native venv, or via a wrapper
   around the Apptainer image);
2. export `BIOFILTER_DB_URI` pointing at the current snapshot's `tables/`
   directory:

```
parquet:///project/hall_shared/biofilter/databases/<snapshot>/bundle/tables
```

With that set, users never pass `--db-uri`. The modulefile itself lives in the
lab's shared module tree, outside this repository.

**Container alternative** — for users who prefer Apptainer over the module,
the read-only HPC image works with a bind mount:

```bash
apptainer run \
  --bind /project/hall_shared/biofilter/databases/<snapshot>/bundle/tables:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  --env BIOFILTER_DB_URI=parquet:///bundle \
  bf4-hpc.sif \
  biofilter report run --name annotation_master_gene --input APOE --output /workspace/apoe.csv
```

See [docker/hpc/README.md](../../docker/hpc/README.md) for image details and
publishing.

---

## 6. Smoke test after install

```bash
source /project/hall_shared/hall_shared.sh
module load biofilter/<version>

biofilter --version
biofilter report list | head

# gene path
biofilter report run --name annotation_master_gene --input APOE --output /tmp/smoke_gene.csv

# variant path — exercises the merged parent table specifically
biofilter report run --name annotation_master_variant --input rs429358 --output /tmp/smoke_variant.csv

head -3 /tmp/smoke_gene.csv /tmp/smoke_variant.csv
```

Both should complete in about a second. **The variant report is the one that
matters** — it is the only check that the consolidated
`variant_molecular_effects.parquet` was merged correctly. A gene report passing
tells you nothing about the merge.

Expected performance (from the ADR-001 POC): 10,000 rsIDs annotated against
the 1.79-billion-row table in ~1.2 s on local NVMe, ~15.5 s over GPFS, peak
memory ~89 MB.

---

## 7. Updates

### 7.1 New BF4 version

Publish a new venv and/or image, add the new modulefile version, smoke-test it
against the current snapshot, then point `latest` at it. Users can pin a
version explicitly with `module load biofilter/<version>`.

Because the bundle is data-only, a BF4 upgrade does **not** require rebuilding
it — unless the release changes the schema. In that case export a new bundle
from a migrated PostgreSQL (§3) rather than trying to migrate the bundle;
there is no write path against Parquet.

### 7.2 New data snapshot

```bash
# 1) Produce a new dated bundle following §3
# 2) Verify it (§3.4) and freeze it (§4)
# 3) Smoke-test with an explicit --db-uri before switching anyone over:
biofilter --db-uri "parquet:///project/hall_shared/biofilter/databases/<new>/bundle/tables" \
  report run --name annotation_master_variant --input rs429358 --output /tmp/check.csv
# 4) Update the modulefile's BIOFILTER_DB_URI to the new snapshot
# 5) Announce the new date to users
```

Old snapshots can stay as long as disk allows — they are reference data for
reproducibility, and users can still target them with `--db-uri`.

---

## 8. Backup

The bundle is plain files on shared storage, so it backs up like any other
research data. The canonical recovery source remains the PostgreSQL dump the
bundle was exported from — keep those alongside the snapshot they produced.

Rebuilding a bundle from a dump is the §3 flow again, so the dump plus this
guide is a complete recovery path.

---

## 9. References

- End-user usage: [lpc\_\_quickstart.md](lpc__quickstart.md)
- Design rationale and benchmarks: [ADR-001](../Andre/ADR/0001-duckdb-parquet-strategy.md)
- Build scripts: [`notebooks/Andre/`](../Andre/)
- HPC image: [docker/hpc/README.md](../../docker/hpc/README.md)
- GHCR publish workflow: [.github/workflows/docker-publish-hpc.yml](../../.github/workflows/docker-publish-hpc.yml)

---

## Appendix A — Legacy PostgreSQL deployment (4.1.x)

> **Deprecated.** Kept for reference and for the one case where it is still
> needed: building a Parquet bundle on the cluster (§3) requires a live
> PostgreSQL to export from. The 4.2.0 HPC image no longer embeds PG — use a
> 4.1.4 image for this. Do not use this flow for regular user-facing
> deployments.

### A.1 Layout

```
/project/${PROJECT}/
├── env/modules/biofilter/<version>/bf4-hpc.sif
└── datasets/bf4/
    ├── <snapshot-date>/pgdata/
    └── dumps/biofilter-<date>.dump
```

Budget ~500 GB for `pgdata/` and ~20 GB for the compressed dump.

### A.2 Restore from a production dump

On the VPS:

```bash
pg_dump -Fc -d biofilter -f /tmp/biofilter-$(date +%Y%m%d).dump
```

Transfer (~20 GB, 1–4 h depending on bandwidth):

```bash
rsync --partial --progress \
  /tmp/biofilter-20260514.dump \
  user@lpc:/project/${PROJECT}/datasets/bf4/dumps/biofilter-20260514.dump
```

Restore inside the legacy PG-bundled container:

```bash
SNAPSHOT_DATE=20260514
VERSION=4.1.4

DB_DIR=/project/${PROJECT}/datasets/bf4/${SNAPSHOT_DATE}
DUMP=/project/${PROJECT}/datasets/bf4/dumps/biofilter-${SNAPSHOT_DATE}.dump
SIF=/project/${PROJECT}/env/modules/biofilter/${VERSION}/bf4-hpc.sif

mkdir -p "${DB_DIR}/pgdata"
TMP_DIR=$(mktemp -d -t bf4-restore-XXXXXX)
mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run"

apptainer run \
  --writable-tmpfs \
  --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${DUMP}:/restore.dump:ro" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  --env BIOFILTER_RESTORE_DUMP=/restore.dump \
  --env BIOFILTER_RESTORE_JOBS=8 \
  "${SIF}" \
  biofilter db migrate --status

rm -rf "${TMP_DIR}"
```

On first run with an empty `pgdata/` and `BIOFILTER_RESTORE_DUMP` set, the
entrypoint runs `initdb`, creates the application database, then `pg_restore`
with the specified parallelism before starting PG normally. Expect **3–8 hours**
on GPFS-class storage; run it as an LSF job.

### A.3 Fresh empty database

Only for isolated test environments:

```bash
apptainer run --writable-tmpfs --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
  sh -c 'biofilter db create-db --db-uri "$DATABASE_URL"'

apptainer run --writable-tmpfs --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
  biofilter db migrate --stamp-head
```

### A.4 Why it was retired

Beyond the cost of a ~480 GB per-snapshot `pgdata/`, the blocking issue was
concurrency: PostgreSQL allows a single postmaster per data directory, which
under Apptainer meant either serialized access or a per-user copy of the whole
database. The `PGDATA` mode `0700` requirement also conflicted with GPFS
`chown` semantics under fakeroot.

A SQLite migration was attempted first (2026-06-17 → 06-22) and abandoned: after
~70 h of cumulative LSF runtime the import never finished
`variant_molecular_effects` and never reached the smaller tables. That failure
is what motivated ADR-001 — see §6 of the ADR for the full history.



<!-- ===== SOURCE FILE: notebooks/Templates/lpc__quickstart.md ===== -->

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
- `--input APOE` — one value; repeat the flag for more
  (`--input APOE --input TP53 --input BRCA1`)
- *or* `--input-file genes.txt` — one item per line (best for long lists)
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

# Do NOT use `#BSUB -L /bin/bash` on this cluster — some compute nodes
# have a `/etc/profile` guard ("no direct access allowed") that aborts
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



<!-- ===== SOURCE FILE: notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb.md ===== -->

# Biofilter — SNP×SNP Interaction Pipeline

End-to-end tutorial for building a biologically-informed SNP×SNP interaction analysis.

---

## Pipeline overview

```
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 1 — Gene Discovery                               ║
║  Report: variant_single_gene_annotation                              ║
║    Input : one seed variant (rsID or chr:pos)                        ║
║    Output: seed gene + partner-gene list (pathway/disease context)   ║
║    Scale : ~8 k genes                                                ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║  partner gene symbol list
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 2 — Filtered Variant Collection                  ║
║  Report: gene_to_variant_filtering                                   ║
║    Input : gene symbols + SQL filters (impact, AF, LoF, …)           ║
║    Output: Lista A — biologically annotated variants                 ║
║    Scale : ~15 k–100 k variants (controlled by filters)              ║
║    Export: lista_A.csv                                               ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║  lista_A.csv
       ╔════════════════╩══════════════════════════╗
       ║  [External]  Extract Lista B from PLINK   ║
       ║  plink --bfile dataset --write-snplist    ║
       ║    Output: lista_B.txt (~500 k–10 M vars) ║
       ╚════════════════╦══════════════════════════╝
                        ║  lista_A.csv + lista_B.txt
                        ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 2.5 — Genotype Intersection                      ║
║  Report: variant_list_intersect                                      ║
║    Input : Lista A (Biofilter) + Lista B (VCF/PLINK)                 ║
║    Output: Lista C — variants present in BOTH                        ║
║    Export: lista_C.txt  (PLINK --extract ready)                      ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                        ║  lista_C.txt
       ╔════════════════╩═══════════════════════════════════╗
       ║  [External — PLINK 1.9]  LD Pruning on Lista C     ║
       ║  plink --bfile dataset                             ║
       ║        --extract lista_C.txt                       ║
       ║        --indep-pairwise 50 5 0.2                   ║
       ║    Output: lista_D.prune.in                        ║
       ╚════════════════╦═══════════════════════════════════╝
                        ║  lista_D.prune.in
                        ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 3 — SNP×SNP Pair Generation                      ║
║  Report: snp_snp_pair_generator                                      ║
║    Input : Lista D + Lista A annotations                             ║
║    Output: annotated interaction pairs (one row per pair)            ║
╚══════════════════════════════════════════════════════════════════════╝
```

---

### Why this separation matters

| Naive approach | This pipeline |
|---|---|
| APOE × 8 k partners × all variants = ~260 M pairs | Phase 1 + Phase 2 filters → ~300 genes × ~50 variants = **~15 k rows** |
| Uncontrolled LD inflation | LD Pruning runs **only on Lista C** — focused, fast pruning step |
| No biological annotation on pairs | Every pair carries full gene + consequence + prediction annotation |

---

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
```

---

### 2. Phase 1 — Gene Discovery

Start from a seed variant and discover all genes that share biological context
(pathways, diseases, GO terms) with the seed gene.

Here we use `rs429358` (APOE ε4 allele) as the seed variant, restricting to
**Reactome pathways** to keep the partner list biologically coherent.

```python
import time

start = time.time()
df_phase1 = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",
    group_entity_type="Pathways",
    source_system_filter=["Reactome"],
)
elapsed = time.time() - start

seed_gene     = df_phase1["seed_gene_symbol"].iloc[0]
partner_genes = df_phase1["partner_gene_symbol"].dropna().unique().tolist()
all_genes     = [seed_gene] + partner_genes

print(f"Phase 1 completed in {elapsed:.1f}s")
print(f"  Seed gene : {seed_gene}")
print(f"  Partners  : {len(partner_genes):,}")
print(f"  Total     : {len(all_genes):,} genes → input for Phase 2")
```

```python
# Top shared pathways
(
    df_phase1
    .dropna(subset=["shared_group_names"])
    .groupby("shared_group_names")["partner_gene_symbol"]
    .nunique()
    .sort_values(ascending=False)
    .head(10)
    .rename("genes")
    .reset_index()
)
```

---

### 3. Phase 2 — Filtered Variant Collection

Collect variants from all Phase 1 genes. Apply SQL-level filters to keep
only biologically relevant variants before generating pairs.

**Filters applied here:**
- `impact_filter=["HIGH", "MODERATE"]` — coding variants only
- `af_max=0.05` — exclude rare variants (MAF < 5%)
- `most_severe_only=True` — one row per variant (no transcript expansion)

> Adjust filters to your study design: rare-variant studies may use `af_max=0.01`;
> LoF studies may add `lof_confidence_filter=["HC"]`.

```python
start = time.time()
df_phase2 = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=all_genes,
    impact_filter=["HIGH", "MODERATE"],
    af_max=0.05,
    most_severe_only=True,
)
elapsed = time.time() - start

print(f"Phase 2 completed in {elapsed:.1f}s")
print(f"  Rows            : {len(df_phase2):,}")
print(f"  Genes with vars : {df_phase2['gene_entity_id'].nunique():,}")
print(f"  Unique variants : {df_phase2['variant_id'].nunique():,}  ← Lista A")
```

```python
# Variant summary by gene (top 20)
(
    df_phase2
    .groupby("gene_symbol")
    .agg(
        variant_count    = ("variant_id",      "nunique"),
        high_impact      = ("impact_name",      lambda x: (x == "HIGH").sum()),
        moderate_impact  = ("impact_name",      lambda x: (x == "MODERATE").sum()),
        with_alphamiss   = ("alphamissense_score", lambda x: x.notna().sum()),
    )
    .sort_values("variant_count", ascending=False)
    .reset_index()
    .head(20)
)
```

#### Export Lista A

Save the Phase 2 output to CSV — this is the input for the genotype intersection step.

```python
import os

OUTPUT_DIR = "pipeline_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

lista_a_path = f"{OUTPUT_DIR}/lista_A.csv"
df_phase2.to_csv(lista_a_path, index=False)

print(f"Lista A saved → {lista_a_path}")
print(f"  {len(df_phase2):,} rows | {df_phase2['variant_id'].nunique():,} unique variants")
print(f"  Columns: {list(df_phase2.columns)}")
```

---

### 4. Extract Lista B from your genotype data  *(external step)*

Before running the intersection, extract the list of all variant IDs present in
your VCF or PLINK dataset. This step happens **outside Biofilter** using standard
genomics tools.

#### PLINK 1.9

```bash
# Write all SNP IDs from a PLINK binary dataset
plink --bfile my_cohort \
      --write-snplist \
      --out pipeline_output/lista_B
# → pipeline_output/lista_B.snplist  (one rsID or chr:pos per line)
```

The `.bim` file itself can also be used directly as `variant_list_b` — Biofilter
reads it natively.

#### VCF

```bash
# Extract ID column from a VCF (rsIDs in column 3)
bcftools query -f '%ID\n' my_cohort.vcf.gz > pipeline_output/lista_B.txt

# Or chr:pos format if IDs are missing
bcftools query -f '%CHROM:%POS\n' my_cohort.vcf.gz > pipeline_output/lista_B.txt
```

> **Tip:** the `.bim` file is the most convenient Lista B source when working with
> PLINK binary datasets — pass its path directly to `variant_list_b`.

---

### 5. Phase 2.5 — Genotype Intersection

Intersect Lista A (biologically annotated) with Lista B (genotyped variants).

**Why this step?**
Not all variants in Lista A will be present in your genotype data — they may have
been filtered during QC, not genotyped on the array, or not imputed. Running LD
Pruning on the full Lista A would waste time and lose information.

Lista C = Lista A ∩ Lista B — variants that are **both biologically relevant and
genotyped in your dataset**. The LD Pruning step works only on this focused list.

**Match strategy (`match_by="auto"`):**
1. Tries rsID matching first (faster, unambiguous)
2. Falls back to chr:pos matching (robust when rsIDs differ between builds/sources)

```python
# Adjust lista_b_path to your actual genotype file
lista_b_path = "path/to/my_cohort.bim"   # .bim | .vcf | .vcf.gz | .txt
lista_c_path = f"{OUTPUT_DIR}/lista_C.txt"

df_intersect = bf.report.run(
    "variant_list_intersect",
    variant_list_a=lista_a_path,
    variant_list_b=lista_b_path,
    match_by="auto",
    plink_extract_path=lista_c_path,
)

n_matched = (df_intersect["match_status"].str.startswith("matched")).sum()
n_only_a  = (df_intersect["match_status"] == "only_in_a").sum()

print(f"Lista A total    : {len(df_intersect):,} variants")
print(f"  matched_rsid   : {(df_intersect['match_status'] == 'matched_rsid').sum():,}")
print(f"  matched_chr_pos: {(df_intersect['match_status'] == 'matched_chr_pos').sum():,}")
print(f"  only_in_a      : {n_only_a:,}  (not in genotype data)")
print(f"Lista C          : {n_matched:,} variants → {lista_c_path}")
```

```python
# Match status breakdown
df_intersect["match_status"].value_counts()
```

```python
# Variants not found in genotype data — inspect before pruning
df_missing = df_intersect[df_intersect["match_status"] == "only_in_a"]
print(f"{len(df_missing):,} variants in Lista A have no genotype data")
df_missing[["variant_a_id", "gene_symbol", "consequence_name", "impact_name", "af"]].head(10)
```

---

### 6. LD Pruning on Lista C  *(external — PLINK 1.9)*

Run LD Pruning **only on Lista C** — the small, focused intersection subset.
This is much faster than pruning the full dataset and produces Lista D:
variants that are biologically relevant, genotyped, AND statistically independent.

```bash
# Standard LD pruning on Lista C only
plink --bfile my_cohort \
      --extract pipeline_output/lista_C.txt \
      --indep-pairwise 50 5 0.2 \
      --out pipeline_output/lista_D

# Output:
#   lista_D.prune.in  ← Lista D (LD-independent variants)
#   lista_D.prune.out ← pruned out (in high LD with a retained variant)
```

**Pruning parameters:**
- `50` — window size (variants)
- `5` — step size (variants)
- `0.2` — r² threshold

> Adjust thresholds to your study design. For rare-variant interaction studies,
> a less stringent threshold (e.g., r²=0.5) may be appropriate to preserve
> functional variants that happen to share some LD.

---

### 7. Phase 3 — SNP×SNP Pair Generation

Generate all variant pairs from Lista D, enriched with full annotations from Lista A.
Every pair carries annotation on both sides (`_a` / `_b` suffix).

**Pairing strategies:**

| Strategy | Formula | Best for |
|---|---|---|
| `seed_vs_all` | n_seed × n_other | Gene-centric (e.g., APOE × all partners) |
| `cross_gene` | pairs between different genes | Pathway-wide scan, no fixed seed |
| `all_vs_all` | n × (n−1) / 2 | Small Lista D only (< 2k variants) |

**Safety check:** if estimated pairs exceed `max_pairs`, the report aborts before
generating any data and returns a `pair_limit_exceeded` row with a suggestion.

```python
# ── Paths (carried from earlier cells; override here if running standalone) ──
OUTPUT_DIR   = "pipeline_output"
lista_a_path = f"{OUTPUT_DIR}/lista_A.csv"
lista_d_path = f"{OUTPUT_DIR}/lista_D.prune.in"
# seed_gene is set in Phase 1; uncomment to override:
# seed_gene = "APOE"

start = time.time()
df_pairs = bf.report.run(
    "snp_snp_pair_generator",
    variant_list      = lista_d_path,
    annotation_source = lista_a_path,
    pairing_strategy  = "seed_vs_all",
    seed_gene         = seed_gene,
    max_pairs         = 1_000_000,
    exclude_same_gene = True,
)
elapsed = time.time() - start

if "resolution_status" in df_pairs.columns:
    status = df_pairs["resolution_status"].iloc[0]
    print(f"Status  : {status}")
    if status == "pair_limit_exceeded":
        print(df_pairs["suggestion"].iloc[0])
else:
    print(f"Phase 3 completed in {elapsed:.1f}s")
    print(f"  Pairs           : {len(df_pairs):,}")
    print(f"  Seed variants   : {df_pairs['rsid_a'].nunique():,}  ({seed_gene})")
    print(f"  Partner variants: {df_pairs['rsid_b'].nunique():,}")
```

#### Inspect pairs

```python
# Preview — key annotation columns from both sides of each pair
preview_cols = [
    "rsid_a", "gene_symbol_a", "consequence_name_a", "impact_name_a", "af_a",
    "rsid_b", "gene_symbol_b", "consequence_name_b", "impact_name_b", "af_b",
    "same_gene",
]
df_pairs[[c for c in preview_cols if c in df_pairs.columns]].head(10)
```

#### Analyze pair distribution

```python
# Pair distribution: impact_a × impact_b
(
    df_pairs
    .groupby(["impact_name_a", "impact_name_b"])
    .size()
    .rename("pair_count")
    .reset_index()
    .sort_values("pair_count", ascending=False)
)
```

```python
# Top gene pairs by number of variant interactions
(
    df_pairs[~df_pairs["same_gene"]]
    .groupby(["gene_symbol_a", "gene_symbol_b"])
    .size()
    .rename("pair_count")
    .reset_index()
    .sort_values("pair_count", ascending=False)
    .head(20)
)
```

#### Export for statistical testing

```python
# Export pairs for statistical testing
pairs_path = f"{OUTPUT_DIR}/phase3_pairs.csv"
df_pairs.to_csv(pairs_path, index=False)
print(f"Pairs saved → {pairs_path}  ({len(df_pairs):,} rows)")
```

---

### 8. Pipeline summary

```
Phase 1    variant_single_gene_annotation  →  gene list (~8k genes)
Phase 2    gene_to_variant_filtering       →  Lista A (annotated variants, CSV)
Phase 2.5  variant_list_intersect          →  Lista C (genotyped subset, PLINK-ready)
[PLINK]    --indep-pairwise                →  Lista D (LD-independent)
Phase 3    snp_snp_pair_generator          →  annotated interaction pairs
```

#### Quick-reference CLI commands

```bash
# Phase 1 — gene discovery from seed variant
biofilter report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=rs429358 \
  --param group_entity_type=Pathways \
  --param source_system_filter=Reactome \
  --output pipeline_output/phase1.csv

# Phase 2 — variant collection (gene list from file)
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols=pipeline_output/partner_genes.txt \
  --param impact_filter="HIGH,MODERATE" \
  --param af_max=0.05 \
  --output pipeline_output/lista_A.csv

# Phase 2.5 — genotype intersection
biofilter report run \
  --report-name variant_list_intersect \
  --param variant_list_a=pipeline_output/lista_A.csv \
  --param variant_list_b=my_cohort.bim \
  --param plink_extract_path=pipeline_output/lista_C.txt \
  --output pipeline_output/intersect_report.csv

# LD Pruning (external — PLINK 1.9)
plink --bfile my_cohort \
      --extract pipeline_output/lista_C.txt \
      --indep-pairwise 50 5 0.2 \
      --out pipeline_output/lista_D

# Phase 3 — pair generation
biofilter report run \
  --report-name snp_snp_pair_generator \
  --param variant_list=pipeline_output/lista_D.prune.in \
  --param annotation_source=pipeline_output/lista_A.csv \
  --param pairing_strategy=seed_vs_all \
  --param seed_gene=APOE \
  --output pipeline_output/phase3_pairs.csv
```



<!-- ===== SOURCE FILE: notebooks/Templates/pipeline__from_single_variant_to_interactions.md ===== -->

# SNP×SNP Interaction Pipeline: From a Single Variant to Biologically-Informed Interaction Pairs

**Biofilter 4
**Pipeline version:** 1.0  
**Biofilter version:\*\* 4.1.x

---

## Abstract

_This document describes the theoretical design and methodological rationale of the pipeline. Each step is demonstrated in practice in the companion notebook:  
[`pipeline__from_single_variant_to_interactions.ipynb`](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb)_

We describe a computational pipeline for generating biologically-informed variant interaction pairs for SNP×SNP epistasis analysis. Starting from a single seed variant of interest, the pipeline (1) identifies functionally related genes by querying a multi-source biological knowledge base across user-selectable relationship contexts — including curated pathways (Reactome, KEGG), Gene Ontology terms, protein–protein interactions, and disease associations — allowing the analyst to define biological relevance according to the specific hypothesis under investigation; (2) collects and annotates all variants within those gene loci, applying configurable pathogenicity filters (VEP consequence class, LoF confidence, allele frequency, CADD, AlphaMissense, and other) to retain only variants relevant to the biological context of the analysis; (3) intersects the annotated variant set with the study's genotyped variants; (4) applies linkage disequilibrium (LD) pruning to produce a statistically independent variant set; and (5) generates all pairwise interaction candidates with full annotation on both sides. The pipeline is implemented in Biofilter 4 and is designed to dramatically reduce the interaction search space relative to naive all-pairs approaches while preserving — and making explicit — the biological rationale for every pair tested.

---

## 1. Motivation

Genome-wide association studies (GWAS) test each variant independently, ignoring epistatic interactions that may contribute substantially to complex trait heritability. Testing all possible SNP pairs in a typical GWAS dataset (500k–10M variants) is computationally intractable and statistically underpowered after multiple testing correction. Biologically-guided pre-selection of variant pairs addresses both problems, but existing approaches typically rely on a single biological context (e.g., a fixed pathway database) and apply uniform variant selection criteria, limiting their adaptability to different study designs.

This pipeline introduces two key differentiators:

**1. Flexible biological grouping.** The gene discovery step (Phase 1) is not bound to a single relationship type. The analyst selects the biological context most appropriate to the study hypothesis:

| Context                      | Source         | Use case                             |
| ---------------------------- | -------------- | ------------------------------------ |
| Biological pathways          | Reactome, KEGG | Functional pathway interactions      |
| Gene Ontology                | GO             | Shared molecular function or process |
| Protein–protein interactions | BioGRID, Pfam  | Direct physical interactions         |
| Disease associations         | ClinGen, MONDO | Disease-relevant gene sets           |

The same seed variant can be analysed under multiple contexts in parallel, enabling hypothesis-driven comparison of interaction landscapes.

**2. Context-aware pathogenicity filtering.** Phase 2 applies a configurable stack of functional filters directly in SQL before any data is transferred, ensuring that only variants relevant to the biological question enter the analysis. Filters span multiple prediction frameworks:

| Filter tier            | Tools / annotations                              | Purpose                                     |
| ---------------------- | ------------------------------------------------ | ------------------------------------------- |
| Functional consequence | VEP impact (HIGH/MODERATE/LOW), consequence type | Remove synonymous and intergenic noise      |
| Loss-of-function       | LOFTEE LoF confidence (HC/LC)                    | Isolate high-confidence truncating variants |
| Allele frequency       | gnomAD AF (af_min, af_max)                       | Control common vs. rare variant analysis    |
| Deleteriousness        | CADD Phred score                                 | Combined multi-annotation score             |
| Missense pathogenicity | AlphaMissense classification                     | Deep learning structural pathogenicity      |
| Splicing impact        | SpliceAI delta score                             | Splice-altering variant identification      |

Any combination of filters can be applied independently, making the pipeline adaptable from rare high-impact LoF studies to common missense burden analyses without changes to the codebase.

---

## 2. Pipeline Architecture

The pipeline alternates between Biofilter (biological annotation) and external tools (genotyping and LD computation):

```
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 1 — Biological Network Construction              ║
║  Report: variant_single_gene_annotation                              ║
║    Input : one seed variant (rsID or chr:pos)                        ║
║    Output: seed gene + partner-gene list (pathway/disease context)   ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║  partner gene symbol list
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 2 — Variant Annotation and Filtering             ║
║  Report: gene_to_variant_filtering                                   ║
║    Input : gene symbols + pathogenicity filters                      ║
║    Output: Lista A — biologically annotated variants (CSV)           ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║  lista_A.csv
       ╔═══════════════╩═══════════════════════════╗
       ║  [External]  Extract Lista B from PLINK   ║
       ║  plink --bfile dataset --write-snplist    ║
       ║    Output: lista_B (.bim / .txt / .vcf)   ║
       ╚════════════════╦══════════════════════════╝
                        ║  lista_A.csv + lista_B
                        ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 2.5 — Genotype–Annotation Integration            ║
║  Report: variant_list_intersect                                      ║
║    Input : Lista A + Lista B                                         ║
║    Output: Lista C — variants present in BOTH (PLINK --extract)      ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║  lista_C.txt
       ╔═══════════════╩════════════════════════════════════╗
       ║  [External — PLINK 1.9]  LD Pruning on Lista C     ║
       ║  plink --extract lista_C.txt                       ║
       ║        --indep-pairwise 50 5 0.2                   ║
       ║    Output: Lista D — LD-independent variants       ║
       ╚════════════════╦═══════════════════════════════════╝
                        ║  lista_D.prune.in
                        ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 3 — Interaction Pair Generation                  ║
║  Report: snp_snp_pair_generator                                      ║
║    Input : Lista D + Lista A annotations                             ║
║    Output: annotated interaction pairs (one row per pair)            ║
╚══════════════════════════════════════════════════════════════════════╝
```

**Scale reduction example** (APOE seed, Reactome pathways):

| Stage                                          | N                        |
| ---------------------------------------------- | ------------------------ |
| All possible variant pairs (gnomAD/no filter)  | ~260 M                   |
| After Phase 2 filters (HIGH/MODERATE, AF < 5%) | ~N k variants            |
| After genotype intersection (Phase 2.5)        | ~N k variants (estimate) |
| After LD pruning (r² < 0.2)                    | ~N k variants            |
| Final interaction pairs (seed × partners)      | ~N k pairs               |

---

## 3. Data Sources

| Source                     | Content                                                       | Version / Build         |
| -------------------------- | ------------------------------------------------------------- | ----------------------- |
| Biofilter 4 knowledge base | Gene loci, pathway membership, disease associations, GO terms | 4.1.2, GRCh38           |
| Reactome                   | Curated biological pathways                                   | Current at DB ingestion |
| KEGG                       | Curated biological pathways                                   | Current at DB ingestion |
| gnomAD v4                  | Variant allele frequencies, functional annotations            | GRCh38                  |
| Ensembl VEP (by gnomAD)    | Consequence annotations, LOFTEE LoF confidence                | GRCh38                  |
| AlphaMissense (by VEP)     | Deep learning pathogenicity scores for missense variants      | v1                      |
| CADD (by gnomAD)           | Combined annotation-dependent depletion scores                | v1.7                    |
| NCBI / HGNC                | Gene symbol resolution, canonical loci                        | Current at DB ingestion |

---

## 4. Phase 1 — Biological Network Construction

**Report:** `variant_single_gene_annotation`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md).
- [Report Example link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_single_gene_annotation.ipynb).

### Input

A single seed variant specified as rsID (e.g., `rs429358`) or genomic coordinate (`chr19:44908684`).

### Method

1. The seed variant is mapped to its host gene via genomic position overlap with gene loci (GRCh38 coordinates from NCBI/HGNC).
2. The host (seed) gene is queried against the Biofilter 4 entity relationship graph to retrieve all partner genes connected through shared biological groups (pathways, diseases, GO terms, protein families).
3. Relationships are filtered by `group_entity_type` (e.g., `Pathways`) and optionally by source system (e.g., `Reactome`).

### Key parameters

| Parameter              | Value used | Rationale                                                                   |
| ---------------------- | ---------- | --------------------------------------------------------------------------- |
| `group_entity_type`    | `Pathways` | Restricts to curated functional pathways; reduces non-specific associations |
| `source_system_filter` | `Reactome` | Reactome provides manually curated, hierarchical pathway annotations        |

### Output

A DataFrame with one row per (seed_gene × partner_gene × shared_groups) relationship. The partner gene symbol list is extracted and passed to Phase 2.

### Scale

~8,000 partner genes for APOE via Reactome pathways.

---

## 5. Phase 2 — Variant Annotation and Filtering

**Report:** `gene_to_variant_filtering`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md).
- Report Example link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__gene_to_variant_filtering.ipynb).

### Input

The gene symbol list from Phase 1.

### Method

1. Gene symbols are resolved to internal entity IDs via alias tables (supports HGNC approved symbols, Ensembl IDs, synonyms).
2. Genomic loci (chromosome, start, end) are retrieved for each gene at the specified genome build.
3. A temporary range table is constructed in the database and joined against the variant master table to retrieve all variants within gene loci using partition-aware per-chromosome queries.
4. Variants are joined to functional annotation tables (`variant_molecular_effects`, `variant_effect_predictions`) to retrieve consequence, impact, prediction scores, and LoF confidence.
5. All filters are applied at the SQL level before data transfer to minimize memory footprint.

### Filters

All filters are optional and combinable. Filters marked **SQL** are applied server-side before data transfer; **Python** filters are applied post-query.

| Filter                  | Parameter                      | Example value                         | Engine       | Rationale                                                                                                                 |
| ----------------------- | ------------------------------ | ------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------- |
| VEP impact              | `impact_filter`                | `["HIGH", "MODERATE"]`                | SQL          | Retains coding variants with functional potential; excludes synonymous and intergenic variants                            |
| Consequence type        | `consequence_type_filter`      | `["missense_variant", "stop_gained"]` | SQL          | Fine-grained control over consequence class; accepts group, category, or individual consequence names                     |
| Most severe per variant | `most_severe_only`             | `True`                                | SQL + Python | One row per variant (no transcript expansion); avoids redundancy in downstream pair generation                            |
| Allele frequency max    | `af_max`                       | `0.05`                                | SQL          | Excludes common variants above MAF threshold                                                                              |
| Allele frequency min    | `af_min`                       | `0.001`                               | SQL          | Excludes ultra-rare variants below MAF threshold                                                                          |
| LoF confidence          | `lof_confidence_filter`        | `["HC", "LC"]`                        | SQL          | LOFTEE annotation: `HC` = high-confidence LoF; `LC` = low-confidence LoF; non-LoF variants excluded when filter is active |
| AlphaMissense class     | `alphamissense_classification` | `["likely_pathogenic"]`               | Python       | Deep learning missense classification (`likely_pathogenic`, `ambiguous`, `likely_benign`)                                 |
| AlphaMissense score     | `alphamissense_score_min`      | `0.564`                               | Python       | Continuous score threshold (0–1); 0.564 is the `likely_pathogenic` boundary                                               |
| CADD Phred              | `cadd_phred_min`               | `20`                                  | SQL          | Combined multi-annotation deleteriousness score; Phred-scaled (20 = top 1% most deleterious)                              |
| SIFT                    | `sift_score_max`               | `0.05`                                | SQL          | Evolutionary constraint score; lower = more damaging (≤ 0.05 is standard "deleterious" threshold)                         |
| PolyPhen-2              | `polyphen_score_min`           | `0.85`                                | SQL          | Structural pathogenicity score; higher = more damaging (≥ 0.85 = "probably damaging")                                     |
| Gene window             | `gene_window_bp`               | `2000`                                | SQL          | Extends gene boundaries on each side; captures upstream regulatory and splice-region variants                             |

### Output (Lista A)

A CSV file (`lista_A.csv`) with one row per (gene × variant), carrying all annotation columns. Exported for use in Phase 2.5.

---

## 6. Phase 2.5 — Genotype–Annotation Integration

Not all variants in Lista A will be present in the study's genotype data. Variants may be absent because they were not included on the genotyping array, failed quality control, or fall below the imputation threshold. Running LD pruning on the full Lista A would therefore be inefficient and potentially misleading — pruning variants that cannot be tested in the first place.

This phase resolves that gap by intersecting Lista A with Lista B (the complete variant list from the study's genotype dataset), producing **Lista C**: the subset of biologically annotated variants that are actually available for statistical testing. Only Lista C proceeds to LD pruning and pair generation, ensuring that every variant in the final interaction pairs has both biological annotation and genotype data.

**Report:** `variant_list_intersect`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_list_intersect.md).
- [Report Example link]

### Input

- **Lista A:** Phase 2 output CSV (biologically annotated variants)
- **Lista B:** Variant list from the study's genotype data (PLINK `.bim` file or VCF)

### Method

Lista A variants are matched against Lista B using a dual-key strategy:

1. **rsID match** (primary): variant IDs matching the pattern `rs\d+` are compared directly.
2. **chr:pos match** (fallback): variants not matched by rsID are matched by (chromosome, position) after normalising chromosome encoding across formats (PLINK integers, VCF `chr`-prefixed strings, Biofilter internal integer encoding).

### Output (Lista C)

- **DataFrame:** all Lista A variants with match status (`matched_rsid`, `matched_chr_pos`, `only_in_a`). Variants with `only_in_a` status are not genotyped in the study dataset and are excluded from downstream analysis.
- **Extract file** (`lista_C.txt`): PLINK-format variant ID list for `--extract`, containing only matched variants.

### Considerations

Variants present in Lista A but absent from Lista B (`only_in_a`) may reflect variants in gnomAD that were not genotyped on the study array, failed genotyping QC, or are absent from the imputation reference panel. These variants are logged for review but do not cause pipeline failure.

---

## 7. LD Pruning

**Tool:** PLINK 1.9

### Input

Lista C (`lista_C.txt`) and the study's PLINK binary dataset.

### Method

```bash
plink --bfile <cohort> \
      --extract lista_C.txt \
      --indep-pairwise 50 5 0.2 \
      --out lista_D
```

LD pruning is performed **exclusively on Lista C** — the biologically relevant, genotyped subset. This is intentional: pruning only the pre-filtered set is computationally faster than pruning the full dataset and avoids the risk of retaining LD proxy variants that have no biological annotation in Lista A.

### Parameters

| Parameter    | Value       | Description                                               |
| ------------ | ----------- | --------------------------------------------------------- |
| Window size  | 50 variants | Sliding window for pairwise LD computation                |
| Step size    | 5 variants  | Window advance step                                       |
| r² threshold | 0.2         | Variants with r² > 0.2 to any retained variant are pruned |

### Output (Lista D)

`lista_D.prune.in` — LD-independent subset of Lista C. These are variants that are biologically annotated, present in the study dataset, and statistically independent.

### Considerations

The r² threshold of 0.2 is a commonly used conservative threshold for interaction analyses. Studies focused on rare coding variants may relax this threshold (e.g., r² < 0.5), as rare functional variants may share partial LD with nearby common variants without being captured by a strict pruning step.

---

## 8. Phase 3 — Interaction Pair Generation

**Report:** `snp_snp_pair_generator`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_list_intersect.md).
- [Report Example link]

### Input

- **Lista D** (`lista_D.prune.in`): LD-independent, genotyped, annotated variants
- **Annotation source** (`lista_A.csv`): Phase 2 output — provides annotation for enrichment

### Method

Lista D variant IDs are matched back to Lista A annotations using the same dual-key strategy as Phase 2.5. All annotation columns from Lista A are carried to the output, duplicated with `_a` and `_b` suffixes for each side of the pair.

Pairs are generated according to the specified pairing strategy:

| Strategy      | Description                                                 | Formula          |
| ------------- | ----------------------------------------------------------- | ---------------- |
| `seed_vs_all` | Seed gene variants paired against all partner-gene variants | n_seed × n_other |
| `cross_gene`  | All pairs between variants from different genes             | ≤ n × (n−1) / 2  |
| `all_vs_all`  | All unique pairs                                            | n × (n−1) / 2    |

A safety check estimates the pair count before materialisation; if the estimate exceeds `max_pairs` (default: 1,000,000), the report aborts and returns a descriptive error with a suggestion for reducing scope.

### Default configuration (current study)

| Parameter           | Value         |
| ------------------- | ------------- |
| `pairing_strategy`  | `seed_vs_all` |
| `seed_gene`         | APOE          |
| `exclude_same_gene` | `True`        |
| `max_pairs`         | 1,000,000     |

### Output

A CSV file (`phase3_pairs.csv`) with one row per variant pair. Each row contains full annotation from Lista A for both the seed-side variant (`_a` columns) and the partner-side variant (`_b` columns), plus:

- `same_gene` — boolean flag indicating whether both variants belong to the same gene
- `pairing_strategy` — the strategy used to generate the pair

---

## 9. Implementation Notes

### Software versions

| Tool       | Version          | Reference                                                                     |
| ---------- | ---------------- | ----------------------------------------------------------------------------- |
| Biofilter  | 4.1.2            | [biofilter.readthedocs.io](https://biofilter.readthedocs.io)                  |
| Python     | 3.10+            |                                                                               |
| SQLAlchemy | 2.x              |                                                                               |
| PostgreSQL | 15+ (production) | Ritchie Lab VPS server (decommissioned since)                                 |
| DB         | `biofilter_prod` | PostgreSQL on the VPS, reached over `postgresql+psycopg2://`                  |
| PLINK      | 1.9              | Purcell et al., 2007; Chang et al., 2015                                      |
| pandas     | ≥ 2.0            |                                                                               |
| NumPy      | ≥ 1.24           |                                                                               |

> **Environment note.** This study ran against the Ritchie Lab VPS PostgreSQL
> instance, which has since been decommissioned. Production is now a read-only
> Parquet bundle on the Penn LPC, reached with
> `--db-uri parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables`.
> The table above records the environment as it was, for reproducibility.

### Reproducibility

- All Biofilter report parameters are logged at runtime and recoverable from the output DataFrame column `resolution_status`.
- The exact gene list, variant list, and pair list at each phase are exported as CSV/TXT files, enabling replication of any downstream step independently.
- The Biofilter database version and ETL package provenance are queryable via `bf.report.run("etl_packages")`.

---

## 10. Limitations and Considerations

**Pathway annotation completeness.** The gene-gene relationships used in Phase 1 are limited to the biological databases ingested into Biofilter 4 (Reactome, KEGG, GO, etc.). Genes with poor pathway annotation coverage may have fewer or no partner genes identified, even if biologically relevant interactions exist.

**Variant annotation coverage.** Functional annotations (consequence, AlphaMissense, CADD) are available for gnomAD variants only. Variants present in the study cohort but absent from gnomAD will not appear in Lista A and therefore cannot be included in interaction pairs.

> **Production database note.** The current Biofilter 4 instance running on the Ritchie Lab VPS server was loaded with a gnomAD filter of **allele count AC ≥ 5**, applied during the ETL process to reduce storage requirements. This excludes ultra-rare singletons and doubletons from the knowledge base. Studies requiring complete variant coverage (AC = 1–4) should provision a dedicated PostgreSQL instance with at least **3 TB of storage** and re-run the gnomAD ETL without the AC filter (`biofilter etl update --data-source variant_gnomad`).

**LD pruning and rare variants.** LD pruning can remove rare functional variants when a common proxy variant is retained in the same LD block. For rare-variant studies (MAF < 1%), consider relaxing the r² threshold or performing burden-test aggregation before pair generation.

**Genome build consistency.** All coordinates in Biofilter 4 are aligned to GRCh38. Study cohorts aligned to GRCh37 must be lifted over before Phase 2.5.

**Pair generation scale.** The `seed_vs_all` strategy assumes a single biologically meaningful seed gene. For studies without a clear seed, `cross_gene` pairs may number in the hundreds of millions; aggressive Phase 2 filtering is required to keep the analysis tractable.

---

## 11. References

- Purcell S, et al. PLINK: [a tool set for whole-genome association and population-based linkage analyses.](https://pubmed.ncbi.nlm.nih.gov/17701901/) _Am J Hum Genet._ 2007;81(3):559–575.
- Chang CC, et al. [Second-generation PLINK: rising to the challenge of larger and richer datasets](https://pubmed.ncbi.nlm.nih.gov/25722852/) _Gigascience._ 2015;4:7.
- Cheng J, et al. [Accurate proteome-wide missense variant effect prediction with AlphaMissense.](https://pubmed.ncbi.nlm.nih.gov/37733863/) _Science._ 2023;381(6664):eadg7492.
- Rentzsch P, et al. [CADD: predicting the deleteriousness of variants throughout the human genome.](https://pubmed.ncbi.nlm.nih.gov/30371827/) _Nucleic Acids Res._ 2019;47(D1):D886–D894.
- Karczewski KJ, et al. [The mutational constraint spectrum quantified from variation in 141,456 humans.](https://pubmed.ncbi.nlm.nih.gov/32461654/) _Nature._ 2020;581(7809):434–443. _(gnomAD v3)_
- Jassal B, et al. [The reactome pathway knowledgebase.](https://pubmed.ncbi.nlm.nih.gov/37941124/) _Nucleic Acids Res._ 2020;48(D1):D498–D503.
- McLaren W, et al. [The Ensembl Variant Effect Predictor.](https://pubmed.ncbi.nlm.nih.gov/27268795/) _Genome Biol._ 2016;17(1):122.

---

_Document generated from pipeline implementation in Biofilter 4._  
_Companion notebook:_ `notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb`



<!-- ===== SOURCE FILE: notebooks/Templates/pipeline__pathway_burden_score.ipynb.md ===== -->

# Biofilter — Pathway Burden Pipeline

**From a list of significant genes (e.g., ExWAS hits) to a prioritised set of biological pathways, weighted by cross-source evidence convergence.**

Companion to [`pipeline__pathway_burden_score.md`](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__pathway_burden_score.md).

Pipeline overview:

1. **Phase 1** — Resolve informal pathway names to canonical entities
2. **Phase 2** — Retrieve gene membership for each pathway
3. **Phase 3** — Build per-pathway and per-gene burden tables
4. **Phase 4** — Compute per-gene convergence score across knowledge bases
5. **Phase 5** — Roll convergence into the burden tables and rank pathways

---
### 0. Start Biofilter and define inputs

```python
from biofilter import Biofilter

bf = Biofilter()
db = bf.db.connect()  # idempotent — returns the connected Database
```

```python
# Analyst inputs — replace with your study values

pathway_list = [
    "estrogen signaling",
    "progesterone response",
    "inflammation",
    "immune pathways",
    "fibrosis",
    "ECM remodeling",
    "angiogenesis",
    "nociception",
    "pain signaling",
]

# Significant genes from upstream analysis (ExWAS, GWAS, …)
exwas_genes = [
    "APOE",
    "BRCA1",
    "TP53",
    # add your full list here
]
```

---
### 1. Phase 1 — Pathway Resolution

Resolve informal pathway names into canonical entities using the `entity_filter` report. Fuzzy matching is permissive enough to catch substring-style queries (`"alzheimer"` → `"Alzheimer disease"`), and `group_filter="Pathways"` ensures we never bleed into Genes or Diseases that happen to share an alias.

```python
result_fuzzy = bf.report.run(
    "entity_filter",
    input_data=pathway_list,
    match_mode="fuzzy",
    group_filter="Pathways",
    similarity_threshold=75,
)

found_pathways = (
    result_fuzzy[result_fuzzy["observation"] != "not found"]["primary_name"]
    .dropna()
    .unique()
    .tolist()
)

print(f"{len(found_pathways)} canonical pathway(s) resolved:")
for p in found_pathways:
    print(f"  • {p}")
```

---
### 2. Phase 2 — Pathway → Gene Membership

For each resolved pathway, pull every gene member from `entity_relationships`. The output `pathway_gene_map` is many-to-many — the same gene can appear in multiple pathways.

```python
genes_by_pathway = bf.report.run(
    "entity_relationship_model",
    input_data=found_pathways,
    input_entity_groups=["Pathway"],
    output_entity_groups=["Gene"],
    relationship_scope="input_to_any",
)

pathway_gene_map = (
    genes_by_pathway[genes_by_pathway["observation"] != "not found"]
    [["input_entity_id", "input_primary_name", "related_primary_name"]]
    .drop_duplicates()
)

print(
    f"{len(pathway_gene_map):,} pathway-gene links for "
    f"{pathway_gene_map['related_primary_name'].nunique():,} unique genes"
)
```

---
### 3. Phase 3 — Burden Tables

Build two summary tables from the pathway-gene map intersected with `exwas_genes`:

- **Pathway table** — one row per pathway with `total_genes`, `exwas_hit_count`, `hit_proportion`, and explicit gene lists.
- **Gene table** — one row per gene with `pathway_count`, list of pathways, and an `is_exwas_hit` flag.

`hit_proportion` is a crude size-adjusted enrichment — the fraction of the pathway's genes that are ExWAS hits.

```python
exwas_set = {g.upper() for g in exwas_genes}
pathway_gene_map = pathway_gene_map.copy()
pathway_gene_map["is_exwas"] = (
    pathway_gene_map["related_primary_name"].str.upper().isin(exwas_set)
)

# Pathway table — one row per pathway
pathway_table = (
    pathway_gene_map
    .groupby(["input_entity_id", "input_primary_name"], as_index=False)
    .agg(
        total_genes=("related_primary_name", "nunique"),
        genes=("related_primary_name", lambda x: sorted(x.dropna().unique())),
    )
    .rename(columns={
        "input_entity_id": "pathway_id",
        "input_primary_name": "pathway_name",
    })
)

exwas_agg = (
    pathway_gene_map[pathway_gene_map["is_exwas"]]
    .groupby("input_primary_name", as_index=False)
    .agg(
        exwas_hit_count=("related_primary_name", "nunique"),
        exwas_genes=("related_primary_name", lambda x: sorted(x.dropna().unique())),
    )
    .rename(columns={"input_primary_name": "pathway_name"})
)

pathway_table = pathway_table.merge(exwas_agg, on="pathway_name", how="left")
pathway_table["exwas_hit_count"] = pathway_table["exwas_hit_count"].fillna(0).astype(int)
pathway_table["exwas_genes"] = pathway_table["exwas_genes"].apply(
    lambda x: x if isinstance(x, list) else []
)
pathway_table["hit_proportion"] = (
    pathway_table["exwas_hit_count"] / pathway_table["total_genes"]
).round(4)
pathway_table = pathway_table.sort_values("hit_proportion", ascending=False)
pathway_table
```

```python
# Gene table — one row per gene
gene_table = (
    pathway_gene_map
    .groupby("related_primary_name", as_index=False)
    .agg(
        pathway_count=("input_primary_name", "nunique"),
        pathways=("input_primary_name", lambda x: sorted(x.dropna().unique())),
    )
    .rename(columns={"related_primary_name": "gene"})
)
gene_table["is_exwas_hit"] = gene_table["gene"].str.upper().isin(exwas_set)
gene_table = gene_table.sort_values(
    ["is_exwas_hit", "pathway_count"], ascending=[False, False]
)
gene_table.head(20)
```

---
### 4. Phase 4 — Convergence Scoring

For each ExWAS gene, count distinct knowledge bases that record any relationship for the gene. The score reflects **how well-characterised the gene is across independent sources**, regardless of whether each source links it to a pathway specifically.

Tune `SOURCE_WEIGHTS` to bias toward curated sources (ClinGen, MONDO) over inferred ones (BioGrid PPI) when the use case demands it.

```python
import pandas as pd
from sqlalchemy import or_, select, text
from biofilter.modules.db.models import EntityRelationship

# Load known data sources for the score lookup
with db.engine.connect() as conn:
    sources_df = pd.read_sql(
        text("SELECT id, name FROM etl_data_sources ORDER BY name"),
        conn,
    )
source_map = dict(zip(sources_df["id"], sources_df["name"]))
print(f"{len(source_map)} data sources available")

# Default: equal weight per relationship-bearing source.
# Add or remove entries to bias the score; sources not listed contribute 0.
SOURCE_WEIGHTS = {
    "biogrid": 1.0,
    "reactome": 1.0,
    "reactome_relationships": 1.0,
    "mondo": 1.0,
    "mondo_relationships": 1.0,
    "clingen": 1.0,
    "uniprot_relationships": 1.0,
    "gene_ontology": 1.0,
}
```

```python
# Resolve ExWAS gene symbols to entity_ids
exwas_resolved = bf.report.run(
    "entity_filter",
    input_data=list(exwas_genes),
    match_mode="exact",
    group_filter="Genes",
)
resolved_mask = exwas_resolved["observation"] != "not found"
exwas_id_to_symbol = dict(
    zip(
        exwas_resolved.loc[resolved_mask, "entity_id"].astype(int),
        exwas_resolved.loc[resolved_mask, "primary_name"],
    )
)
exwas_entity_ids = list(exwas_id_to_symbol.keys())
print(f"Resolved {len(exwas_entity_ids)}/{len(exwas_genes)} ExWAS gene symbols")
```

```python
# Pull every relationship touching any ExWAS gene, then aggregate per gene
stmt = select(
    EntityRelationship.entity_1_id,
    EntityRelationship.entity_2_id,
    EntityRelationship.data_source_id,
).where(
    or_(
        EntityRelationship.entity_1_id.in_(exwas_entity_ids),
        EntityRelationship.entity_2_id.in_(exwas_entity_ids),
    )
)

with db.engine.connect() as conn:
    rels_df = pd.read_sql(stmt, conn)

exwas_id_set = set(exwas_entity_ids)
e1_in = rels_df["entity_1_id"].isin(exwas_id_set)
rels_df["gene_id"] = rels_df["entity_1_id"].where(e1_in, rels_df["entity_2_id"])
rels_df["source_name"] = rels_df["data_source_id"].map(source_map)

def _score(sources):
    return float(sum(SOURCE_WEIGHTS.get(s, 0.0) for s in set(sources) if s))

gene_convergence = (
    rels_df.dropna(subset=["source_name"])
    .groupby("gene_id")
    .agg(
        evidence_sources=("source_name", lambda s: sorted(set(s))),
        convergence_count=("source_name", "nunique"),
        convergence_score=("source_name", _score),
    )
    .reset_index()
)
gene_convergence["gene"] = gene_convergence["gene_id"].map(exwas_id_to_symbol)
gene_convergence = gene_convergence.sort_values("convergence_score", ascending=False)
gene_convergence[["gene", "convergence_count", "convergence_score", "evidence_sources"]]
```

---
### 5. Phase 5 — Convergence Roll-up

Merge per-gene convergence into the burden tables and re-rank pathways. The final `pathway_table` carries:

- `hit_proportion` — size-adjusted hit rate (Phase 3)
- `mean_convergence` — average evidence per ExWAS hit
- `total_convergence` — sum of evidence across ExWAS hits

The ranking by `total_convergence` favours pathways whose ExWAS hits are well-supported across multiple knowledge bases.

```python
# Enrich gene_table with convergence columns
gene_table = gene_table.merge(
    gene_convergence[["gene", "convergence_count", "convergence_score", "evidence_sources"]],
    on="gene",
    how="left",
)
gene_table["convergence_score"] = gene_table["convergence_score"].fillna(0.0)
gene_table["convergence_count"] = gene_table["convergence_count"].fillna(0).astype(int)

# Roll into pathway_table
gene_score = dict(
    zip(gene_convergence["gene"].str.upper(), gene_convergence["convergence_score"])
)

def _avg_score(genes_list):
    scores = [gene_score.get(g.upper(), 0.0) for g in (genes_list or [])]
    return round(sum(scores) / len(scores), 2) if scores else 0.0

def _sum_score(genes_list):
    return round(sum(gene_score.get(g.upper(), 0.0) for g in (genes_list or [])), 2)

pathway_table["mean_convergence"] = pathway_table["exwas_genes"].apply(_avg_score)
pathway_table["total_convergence"] = pathway_table["exwas_genes"].apply(_sum_score)
pathway_table = pathway_table.sort_values(
    ["total_convergence", "exwas_hit_count"], ascending=[False, False]
)
```

```python
# Final pathway ranking
display_cols = [
    "pathway_name", "exwas_hit_count", "total_genes", "hit_proportion",
    "mean_convergence", "total_convergence", "exwas_genes",
]
pathway_table[[c for c in display_cols if c in pathway_table.columns]]
```

```python
# ExWAS hits ranked by convergence
gene_cols = [
    "gene", "is_exwas_hit", "pathway_count",
    "convergence_count", "convergence_score", "evidence_sources",
]
(
    gene_table[gene_table["is_exwas_hit"] == True]
    [[c for c in gene_cols if c in gene_table.columns]]
    .sort_values("convergence_score", ascending=False)
)
```

---
## Interpreting the output

**Pathway table** — pathways at the top combine high `hit_proportion` (size-adjusted enrichment) with high `total_convergence` (well-supported hits). Use both columns together: a pathway with `hit_proportion=0.5` but a single low-evidence gene is less compelling than one with `hit_proportion=0.1` and ten high-evidence genes.

**Gene table** — ExWAS hits ranked by `convergence_score` highlight the most well-characterised hits. Genes with low convergence are candidates for novel biology (or false positives — verify via independent evidence).

**Tuning the score** — the default `SOURCE_WEIGHTS` treats every source equally. For a clinically motivated scope, increase ClinGen and MONDO weights and decrease BioGrid (which contains both curated and large-scale inferred PPIs). Document the chosen weights when reporting results.

**Next steps** — for variant-level prioritisation within these pathways, use the `gene_to_variant_filtering` report on the resolved gene list, or feed the per-pathway hits into the SNP×SNP interaction pipeline.



<!-- ===== SOURCE FILE: notebooks/Templates/pipeline__pathway_burden_score.md ===== -->

# Pathway Burden Pipeline: From Gene Hit Lists to Cross-Source Convergence Scores

**Biofilter 4**
**Pipeline version:** 1.0
**Biofilter version:** 4.1.x

---

## Abstract

_This document describes the theoretical design and methodological rationale of the pipeline. Each step is demonstrated in practice in the companion notebook:
[`pipeline__pathway_burden_score.ipynb`](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__pathway_burden_score.ipynb)_

We describe a pipeline for prioritising biological pathways given a list of genes flagged as significant by an upstream genetic analysis (e.g., ExWAS, GWAS). Starting from informal pathway names and a list of significant genes, the pipeline (1) resolves user-provided pathway names against the Biofilter 4 knowledge base using fuzzy or substring matching, accommodating legacy and approximate labels; (2) retrieves the full gene membership of each resolved pathway from curated databases (Reactome, KEGG); (3) intersects pathway membership with the analyst's hit gene list to build per-pathway and per-gene burden tables; (4) computes a **convergence score** for each gene, defined as the number (or weighted sum) of independent knowledge bases that record the gene in any biological relationship — BioGrid (PPI), Reactome (pathways), MONDO (disease ontology), ClinGen (clinical curation), UniProt, and others; (5) rolls convergence into the burden tables, producing a per-pathway summary that combines hit count, hit proportion, and average evidence strength of the genes hitting that pathway. The pipeline operates on summary-level inputs (no individual genotypes required) and is engine-agnostic (PostgreSQL or SQLite).

---

## 1. Motivation

After a genetic analysis identifies a set of significant genes, a common next question is: **which biological processes are these genes collectively pointing to?** Standard pathway enrichment tools (DAVID, Enrichr, GSEA) answer this with a hypergeometric or rank-based test against a fixed pathway database. Useful, but with two limitations relevant to small or curated hit lists:

1. **Single-source pathway annotation.** Most enrichment tools query one database at a time. A gene linked to a pathway only in BioGrid (PPI inference) and not in Reactome (manually curated) may be invisible.
2. **No evidence-weighting per gene.** A hit gene mentioned in 5 independent knowledge bases (BioGrid, Reactome, MONDO, ClinGen, UniProt) carries stronger biological priors than a hit gene mentioned in only one. Standard enrichment treats both equally.

This pipeline addresses both:

**Multi-source pathway lookup.** Phase 2 retrieves gene membership from every relationship source loaded into Biofilter 4 (Reactome and KEGG for pathways, plus any future ones), without requiring the analyst to merge them manually.

**Convergence scoring.** Phase 4 is the methodological contribution: for each ExWAS hit, count the distinct knowledge bases that record the gene in any biological relationship. The score is fully tunable via per-source weights, allowing the analyst to bias toward curated sources (ClinGen, MONDO) over inferred ones (BioGrid PPI) when desired.

The two layers combine to produce a pathway burden score that is **size-aware** (hits per pathway gene), **biologically plural** (pulling from all sources), and **evidence-weighted** (per-gene convergence).

---

## 2. Pipeline Architecture

The pipeline is fully internal to Biofilter 4 — no external tooling required:

```
╔══════════════════════════════════════════════════════════════════════╗
║  [Input]  Analyst inputs                                             ║
║    - pathway_list   : informal pathway names                         ║
║    - exwas_genes    : significant gene symbols                       ║
║    - SOURCE_WEIGHTS : per-source weight overrides (optional)         ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 1 — Pathway Resolution                           ║
║  Report: entity_filter (fuzzy / like / exact)                        ║
║    Input : pathway_list                                              ║
║    Output: found_pathways (canonical primary_names)                  ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 2 — Pathway → Gene Membership                    ║
║  Report: entity_relationship_model (Pathways → Genes)                ║
║    Input : found_pathways                                            ║
║    Output: pathway_gene_map (one row per pathway-gene link)          ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Pandas]  Phase 3 — Burden Tables                                   ║
║  Aggregate hits and proportions                                      ║
║    Output: pathway_table  (per pathway: hit_count, hit_proportion)   ║
║            gene_table     (per gene: pathway_count, is_exwas_hit)    ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Biofilter]  Phase 4 — Convergence Scoring                          ║
║  Direct query on entity_relationships + etl_data_sources             ║
║    Input : ExWAS entity_ids + SOURCE_WEIGHTS                         ║
║    Output: gene_convergence (per gene: distinct sources, score)      ║
╚══════════════════════╦═══════════════════════════════════════════════╝
                       ║
                       ▼
╔══════════════════════════════════════════════════════════════════════╗
║  [Pandas]  Phase 5 — Convergence Roll-up                             ║
║  Merge convergence into pathway_table and gene_table                 ║
║    Output: pathway_table  (+ mean_convergence, total_convergence)    ║
║            gene_table     (+ convergence_score, evidence_sources)    ║
╚══════════════════════════════════════════════════════════════════════╝
```

**Example scale** (9 pathway names + 65 ExWAS genes):

| Stage                          | N                          |
| ------------------------------ | -------------------------- |
| Input pathway names            | 9                          |
| Resolved pathways (Phase 1)    | ~16 (fuzzy expansion)      |
| Total genes in pathways        | ~3,000 unique              |
| ExWAS hits in resolved set     | ~10–30                     |
| Final per-pathway summary rows | 16 (one per pathway)       |

---

## 3. Data Sources

| Source                     | Content                                        | Used in Phase   |
| -------------------------- | ---------------------------------------------- | --------------- |
| Biofilter 4 knowledge base | Entities, aliases, relationships, data sources | All             |
| Reactome                   | Curated pathways, gene-pathway membership      | 1, 2            |
| KEGG                       | Curated pathways                               | 2 (if loaded)   |
| BioGrid                    | Protein-protein interactions                   | 4 (convergence) |
| MONDO                      | Disease ontology, gene-disease links           | 4 (convergence) |
| ClinGen                    | Clinical gene-disease curation                 | 4 (convergence) |
| UniProt                    | Protein-gene encoding, function                | 4 (convergence) |
| Gene Ontology              | Functional annotation                          | 4 (convergence) |

---

## 4. Phase 1 — Pathway Resolution

**Report:** `entity_filter`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_filter.md)
- [Report Example link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__entity_filter.ipynb)

### Input

A list of informal pathway names. These can be partial labels, common names, or formal database names. Examples: `"estrogen signaling"`, `"inflammation"`, `"DNA repair"`.

### Method

The `entity_filter` report performs case-insensitive matching against the alias table for entities in the `Pathways` group. Three modes:

| Mode    | Behavior                                              | When to use                       |
| ------- | ----------------------------------------------------- | --------------------------------- |
| `exact` | Match `alias_norm` literally                          | Inputs are known canonical names  |
| `like`  | Substring match (`%term%` in either direction)        | Inputs are partial labels         |
| `fuzzy` | rapidfuzz `token_sort_ratio` against all aliases     | Inputs are informal/misspelled   |

`group_filter="Pathways"` ensures matches are scoped to pathway entities only, avoiding cross-domain bleed (e.g., a gene whose alias coincidentally contains "signaling").

### Key parameters

| Parameter              | Value used | Rationale                                                                |
| ---------------------- | ---------- | ------------------------------------------------------------------------ |
| `match_mode`           | `fuzzy`    | Tolerant to informal labels common in study writeups                     |
| `group_filter`         | `Pathways` | Restricts to pathway entities, eliminates cross-group collisions         |
| `similarity_threshold` | `75`       | Permissive enough to catch substring-style queries (e.g. "alzheimer")    |

### Output (`found_pathways`)

A list of canonical `primary_name` values (Reactome IDs like `R-HSA-111885`). Pathways with `observation == "not found"` are excluded; they are surfaced separately for manual review.

---

## 5. Phase 2 — Pathway → Gene Membership

**Report:** `entity_relationship_model`

- [Report Tutorial link](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_relationship_model.md)

### Input

`found_pathways` from Phase 1.

### Method

Resolves each pathway entity and traverses one hop in `entity_relationships` filtered by `relationship_type IN ('in_pathway')`. For each pathway, returns every member gene with the supporting `data_source_id`.

### Key parameters

| Parameter              | Value used        | Rationale                                                |
| ---------------------- | ----------------- | -------------------------------------------------------- |
| `input_entity_groups`  | `["Pathway"]`     | Treats inputs as pathways                                |
| `output_entity_groups` | `["Gene"]`        | Returns only gene neighbours                             |
| `relationship_scope`   | `input_to_any`    | Returns every gene linked to any input pathway           |

### Output (`pathway_gene_map`)

One row per `(pathway, gene)` link, with `data_source_id` for provenance. The same gene may appear under multiple pathways — that's intentional; pathway membership is many-to-many.

---

## 6. Phase 3 — Burden Tables

**Engine:** pandas (no Biofilter call)

### Inputs

- `pathway_gene_map` (Phase 2 output)
- `exwas_genes` (analyst-provided list of significant gene symbols)

### Method

Two grouped aggregations:

**Pathway table.** For each pathway:
- `total_genes` = distinct genes in the pathway (full membership)
- `exwas_hit_count` = distinct genes from `exwas_genes` that fall in this pathway
- `hit_proportion` = `exwas_hit_count / total_genes`
- `genes` and `exwas_genes` columns hold the explicit lists

**Gene table.** For each gene:
- `pathway_count` = number of distinct pathways the gene belongs to
- `pathways` = the list
- `is_exwas_hit` = boolean flag

### Why hit_proportion matters

Pathways vary in size by orders of magnitude — a pathway with 5 genes versus one with 500. A raw hit count favours large pathways. The proportion normalises this and acts as a crude size-adjusted enrichment.

### Output

`pathway_table` and `gene_table` — both ready for the convergence enrichment in Phase 5.

---

## 7. Phase 4 — Convergence Scoring

**Engine:** direct ORM query (no report call)

### Input

- ExWAS gene list (resolved to `entity_id` via `entity_filter`)
- `SOURCE_WEIGHTS` dict (per-source float; defaults to 1.0 each)

### Method

For each ExWAS gene, query `entity_relationships` for **all** rows where the gene appears as either `entity_1` or `entity_2`, then count distinct `data_source_id` values. The data source ID is mapped to a human-readable name via `etl_data_sources`. The convergence score is the sum of weights for the distinct sources observed:

```
convergence_score(gene) = Σ_{s ∈ sources(gene)} SOURCE_WEIGHTS[s]
```

With all weights = 1.0, the score reduces to "count of distinct knowledge bases that mention this gene". Tuning weights lets the analyst bias the score toward curated evidence over inferred evidence, or toward disease-specific sources for clinically motivated hypotheses.

### Default weights

| Source                   | Default weight | Type                       |
| ------------------------ | -------------- | -------------------------- |
| `biogrid`                | 1.0            | PPI (inferred + curated)   |
| `reactome` / `_relationships`             | 1.0            | Curated pathways           |
| `mondo` / `_relationships`               | 1.0            | Disease ontology           |
| `clingen`                | 1.0            | Clinical curation (high)   |
| `uniprot_relationships`  | 1.0            | Protein function           |

Sources not in `SOURCE_WEIGHTS` contribute 0 (silently excluded). Add `gene_ontology`, `kegg_pathways`, `gtex_v10_brain_eqtl`, etc. as needed for the analysis.

### Output (`gene_convergence`)

| Column              | Meaning                                                   |
| ------------------- | --------------------------------------------------------- |
| `gene`              | Gene primary symbol                                       |
| `evidence_sources`  | Sorted list of distinct sources mentioning the gene       |
| `convergence_count` | Length of `evidence_sources`                              |
| `convergence_score` | Weighted sum of `SOURCE_WEIGHTS` over `evidence_sources`  |

### Suggested weight calibrations

| Use case                        | Weight bias                                              |
| ------------------------------- | -------------------------------------------------------- |
| High-confidence clinical scope  | `clingen=2.0`, `mondo=1.5`, `biogrid=0.5`                |
| PPI-driven mechanism            | `biogrid=2.0`, `uniprot_relationships=1.5`               |
| Pathway-centric (default)       | All curated sources = 1.0; inferred sources = 0.5        |

---

## 8. Phase 5 — Convergence Roll-up

**Engine:** pandas

### Method

`gene_convergence` is merged into `gene_table` on the gene symbol. Each pathway in `pathway_table` then receives:

- `mean_convergence` = average `convergence_score` over the pathway's ExWAS hits
- `total_convergence` = sum of `convergence_score` over the pathway's ExWAS hits

`pathway_table` is re-sorted by `total_convergence` descending — pathways whose hits are well-characterised across knowledge bases rank higher.

### Output

The final `pathway_table` carries:

| Column              | Source        | Meaning                                                |
| ------------------- | ------------- | ------------------------------------------------------ |
| `pathway_id`        | Phase 2       | Reactome ID                                            |
| `pathway_name`      | Phase 2       | Pathway primary alias                                  |
| `total_genes`       | Phase 3       | Pathway size (gene count)                              |
| `exwas_hit_count`   | Phase 3       | ExWAS genes that hit this pathway                      |
| `exwas_genes`       | Phase 3       | List of those genes                                    |
| `hit_proportion`    | Phase 3       | `exwas_hit_count / total_genes`                        |
| `mean_convergence`  | Phase 5       | Average evidence per ExWAS hit                         |
| `total_convergence` | Phase 5       | Sum of evidence across ExWAS hits                      |

The combination of `hit_proportion` (size-adjusted enrichment) and `total_convergence` (evidence-weighted hit count) gives a richer ranking than either alone.

---

## 9. Implementation Notes

### Software versions

| Tool       | Version          |
| ---------- | ---------------- |
| Biofilter  | 4.1.2            |
| Python     | 3.10+            |
| SQLAlchemy | 2.x              |
| PostgreSQL | 15+ (production) |
| SQLite     | 3.x (local)      |
| pandas     | ≥ 2.0            |
| rapidfuzz  | ≥ 3.0            |

### Reproducibility

- All Biofilter report calls log their parameters and elapsed time.
- The exact `pathway_list`, `exwas_genes`, and `SOURCE_WEIGHTS` are visible in the notebook cells; saving the notebook itself preserves the analysis.
- The Biofilter database state (which sources are loaded) is queryable via `bf.report.run("etl_status")`.

### Engine support

The pipeline is **engine-agnostic**: every report and ORM call is portable across PostgreSQL and SQLite. Fuzzy matching uses `rapidfuzz` client-side rather than `pg_trgm`.

---

## 10. Limitations and Considerations

**Pathway annotation completeness.** Phase 2 retrieves gene membership only from databases ingested into Biofilter 4. Pathways that exist in the source database but not in BF4 (e.g., KEGG variants not loaded) are invisible.

**Convergence ≠ pathogenicity.** A gene with high convergence is well-characterised, not necessarily disease-relevant. The score reflects research attention, not biological causality. Combine with downstream variant-level annotation (gnomAD, AlphaMissense) for clinical interpretation.

**Source weight choices are subjective.** Default weights treat all sources equally, but ClinGen (clinically curated) and BioGrid (high-throughput PPI) carry very different evidence quality. Weight calibration should reflect the analyst's prior on each source. Document the chosen weights when publishing.

**Pathway resolution false positives.** Fuzzy matching with low thresholds (< 70) can pull unrelated pathways. Always inspect the `result_fuzzy` output and prune false matches before proceeding to Phase 2.

**Single-organism scope.** All knowledge sources currently loaded reflect human (Homo sapiens) annotations. The pipeline does not adapt automatically to other species.

**Independence assumption.** The convergence score treats sources as independent evidence, but BioGrid and UniProt share underlying data; MONDO is partly derived from clinical sources. The score is therefore an **upper bound** on truly independent evidence.

---

## 11. References

- Jassal B, et al. [The reactome pathway knowledgebase.](https://pubmed.ncbi.nlm.nih.gov/31691815/) _Nucleic Acids Res._ 2020;48(D1):D498–D503.
- Oughtred R, et al. [The BioGRID interaction database: 2019 update.](https://pubmed.ncbi.nlm.nih.gov/30476227/) _Nucleic Acids Res._ 2019;47(D1):D529–D541.
- Vasilevsky NA, et al. [Mondo: Unifying diseases for the world, by the world.](https://www.medrxiv.org/content/10.1101/2022.04.13.22273750v3) _medRxiv_ 2022.
- Rehm HL, et al. [ClinGen — The Clinical Genome Resource.](https://pubmed.ncbi.nlm.nih.gov/26014595/) _N Engl J Med._ 2015;372(23):2235–2242.
- The UniProt Consortium. [UniProt: the Universal Protein Knowledgebase in 2023.](https://pubmed.ncbi.nlm.nih.gov/36408920/) _Nucleic Acids Res._ 2023;51(D1):D523–D531.

---

_Document generated from pipeline implementation in Biofilter 4._
_Companion notebook:_ `notebooks/Templates/pipeline__pathway_burden_score.ipynb`



<!-- ===== SOURCE FILE: notebooks/Templates/reports__101.ipynb.md ===== -->

<h1> 📘 Biofilter — Reports 101 </h1>

This notebook is the landing tutorial for the Reports module.
It covers the core workflow and points to focused notebooks for each report.

## Core Report API

- `bf.report.list()`
- `bf.report.explain("<report_name>")`
- `bf.report.available_columns("<report_name>")`
- `bf.report.example_input("<report_name>")`
- `bf.report.run("<report_name>", **params)`

--------

### 1. Start Biofilter

```python
from biofilter import Biofilter

# Uses db_uri from .biofilter.toml when available
bf = Biofilter(debug_mode=False)
```

------

### 2. List available reports

```python
reports = bf.report.list()
print(f"Total reports: {len(reports)}")
reports
```

------

### 3. Inspect a stable report (`etl_status`)

```python
bf.report.explain("etl_status")
```

```python
bf.report.available_columns("etl_status")
```

------

### 4. Run a report (default)

```python
df = bf.report.run("etl_status")
print(f"Rows: {len(df)}")
df.head()
```

### 5. Run with filters (API params)

```python
df_filtered = bf.report.run(
    "etl_status",
    source_system=["NCBI"],
    only_active=True,
)
print(f"Rows: {len(df_filtered)}")
df_filtered.head()
```

------

## Next Tutorials (by report)

- `reports__etl_status.ipynb`
- `reports__etl_packages.ipynb`
- `reports__entity_filter.ipynb`
- `reports__db_pg_table_stats.ipynb` (PostgreSQL-only)
- `reports__db_pg_index_stats.ipynb` (PostgreSQL-only)
- `reports__variant_molecular_effects.ipynb`
- `reports__qry_template.ipynb`



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_master_chemical.ipynb.md ===== -->

<h1> Biofilter - Report: Chemical Master Annotation </h1>

Compact chemical annotation report based on ChemicalMaster.
Returns identity/properties, optional xref summary, and optional relationship summary.

### 1. Start Biofilter

```python
from biofilter import Biofilter
```

```python
# Production (LPC, read-only):
# db_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"
# Legacy PostgreSQL server (decommissioned; see lpc__deploy.md Appendix A):
# db_uri = "postgresql+psycopg2://<user>:<password>@<SERVER_IP>:5432/biofilter_prod"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
bf = Biofilter(db_uri=db_uri, debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'annotation_master_chemical'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Run default mode

```python
input_chemicals = [
    'CHEBI:15377',
    'CHEBI:17234',
    'water',
    'NOT_A_CHEMICAL'
]

df = bf.report.run(
    'annotation_master_chemical',
    input_data=input_chemicals,
    include_aliases=True,
    include_xref_summary=True,
    include_relationships=False,
    emit_not_found_rows=True,
)

print('rows:', len(df))
df.head(20)
```

```python
focus_cols = [
    'input_value',
    'entity_id',
    'chemical_id',
    'chemical_name',
    'chemical_formula',
    'chemical_charge',
    'chemical_mass',
    'xref_ids_by_source',
    'status',
    'note',
]

df[[c for c in focus_cols if c in df.columns]].head(50)
```

### 4. Run with relationship summary

```python
df_rel = bf.report.run(
    'annotation_master_chemical',
    input_data=input_chemicals,
    include_aliases=True,
    include_xref_summary=True,
    include_relationships=True,
    emit_not_found_rows=True,
)

rel_cols = [
    'input_value',
    'chemical_id',
    'entity_relationships_by_group',
    'total_entity_relationships',
    'status',
]

df_rel[[c for c in rel_cols if c in df_rel.columns]].head(50)
```

```python
df_rel.to_csv('annotation_master_chemical.csv', index=False)
print('Saved: annotation_master_chemical.csv')
```

### 5. Schema Check (quick QA)

```python
df_to_check = df_rel if 'df_rel' in globals() else (df if 'df' in globals() else None)

if df_to_check is None:
    print('No DataFrame found to validate (expected df or df_rel).')
else:
    required_cols = [
        'input_value',
        'entity_id',
        'chemical_id',
        'chemical_name',
        'status',
    ]

    print('Dtypes:')
    display(df_to_check.dtypes.to_frame('dtype'))

    missing_cols = [c for c in required_cols if c not in df_to_check.columns]
    print('\nMissing required columns:', missing_cols if missing_cols else 'none')

    for c in [
        'entity_id',
        'chemical_master_id',
        'chemical_charge',
        'chemical_structure_id',
        'chemical_etl_package_id',
        'total_entity_relationships',
    ]:
        if c in df_to_check.columns:
            print(f'{c} dtype: {df_to_check[c].dtype}')
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_master_disease.ipynb.md ===== -->

<h1> Biofilter - Report: Disease Master Annotation </h1>

Compact disease annotation report based on DiseaseMaster.
Returns MONDO identity, groups/subsets, optional xref summary, optional ClinGen summary, and optional relationship summary.

### 1. Start Biofilter

```python
from biofilter import Biofilter
```

```python
# Production (LPC, read-only):
# db_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"
# Legacy PostgreSQL server (decommissioned; see lpc__deploy.md Appendix A):
# db_uri = "postgresql+psycopg2://<user>:<password>@<SERVER_IP>:5432/biofilter_prod"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
bf = Biofilter(db_uri=db_uri, debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'annotation_master_disease'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Run default mode (xref + ClinGen summary, no relationships)

```python
input_diseases = [
    'MONDO:0019391',
    'MONDO:0005737',
    'cystic fibrosis',
    'NOT_A_DISEASE'
]

df = bf.report.run(
    'annotation_master_disease',
    input_data=input_diseases,
    include_aliases=True,
    include_xref_summary=True,
    include_clingen_summary=True,
    include_relationships=False,
    emit_not_found_rows=True,
)

print('rows:', len(df))
df.head(20)
```

```python
df.to_csv('annotation_master_disease.csv', index=False)
```

```python
focus_cols = [
    'input_value',
    'entity_id',
    'disease_id',
    'disease_label',
    'omic_status',
    'disease_groups',
    'disease_source_system',
    'disease_data_source',
    'xref_ids_by_source',
    'clingen_gene_count',
    'clingen_relationship_count',
    'status',
    'note',
]

df[[c for c in focus_cols if c in df.columns]].head(50)
```

### 4. Run with relationship summary

```python
df_rel = bf.report.run(
    'annotation_master_disease',
    input_data=input_diseases,
    include_aliases=True,
    include_xref_summary=True,
    include_clingen_summary=True,
    include_relationships=True,
    emit_not_found_rows=True,
)

rel_cols = [
    'input_value',
    'disease_id',
    'entity_relationships_by_group',
    'total_entity_relationships',
    'status',
]

df_rel[[c for c in rel_cols if c in df_rel.columns]].head(50)
```

```python
df_rel.to_csv('annotation_master_disease.csv', index=False)
print('Saved: annotation_master_disease.csv')
```

### 5. Schema Check (quick QA)

```python
df_to_check = df_rel if 'df_rel' in globals() else (df if 'df' in globals() else None)

if df_to_check is None:
    print('No DataFrame found to validate (expected df or df_rel).')
else:
    required_cols = [
        'input_value',
        'entity_id',
        'disease_id',
        'disease_label',
        'status',
    ]

    print('Dtypes:')
    display(df_to_check.dtypes.to_frame('dtype'))

    missing_cols = [c for c in required_cols if c not in df_to_check.columns]
    print('\nMissing required columns:', missing_cols if missing_cols else 'none')

    for c in ['entity_id', 'disease_master_id', 'clingen_gene_count', 'clingen_relationship_count', 'total_entity_relationships']:
        if c in df_to_check.columns:
            print(f'{c} dtype: {df_to_check[c].dtype}')
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_master_gene.ipynb.md ===== -->

<h1> Biofilter - Report: Gene Master Annotation </h1>

Compact gene annotation report focused on performance.
Returns canonical IDs, GeneMaster metadata, build38 location, relationship summary, and optional variant counts.

### 1. Start Biofilter

```python
from biofilter import Biofilter
```

```python
# Production (LPC, read-only):
# db_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"
# Legacy PostgreSQL server (decommissioned; see lpc__deploy.md Appendix A):
# db_uri = "postgresql+psycopg2://<user>:<password>@<SERVER_IP>:5432/biofilter_prod"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
bf = Biofilter(db_uri=db_uri, debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'annotation_master_gene'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

> Tip: use `input_data="__ALL__"` to process all genes in `GeneMaster`.
>
> For performance in full-scan mode, prefer `include_relationships=False` and `include_variant_summary=False`.

### 3. Run default mode (relationships + variant count)

```python
input_genes = [
    # 'TP53',
    # 'BRCA1',
    # 'HGNC:1100',
    # 'ENSG00000141510',
    # 'NOT_A_GENE'
    'A4GALT',
    'AARS1'

]

df = bf.report.run(
    'annotation_master_gene',
    input_data=input_genes,
    include_relationships=True,
    include_variant_summary=True,
    emit_not_found_rows=True,
)

print('rows:', len(df))
df.head(20)
```

```python
df.to_csv('annotation_master_gene_report.csv', index=False)
```

```python
focus_cols = [
    'input_value',
    'entity_id',
    'gene_symbol',
    'hgnc_id',
    'ensembl_id',
    'entrez_id',
    'gene_locus_group',
    'gene_locus_type',
    'gene_groups',
    'build',
    'chromosome',
    'start_position',
    'end_position',
    'entity_relationships_by_group',
    'total_entity_relationships',
    'variant_count_in_gene_range',
    'status',
    'note',
]

df[[c for c in focus_cols if c in df.columns]].head(50)
```

### 4. Performance mode (disable heavy summaries)

```python
df_fast = bf.report.run(
    'annotation_master_gene',
    input_data=input_genes,
    include_relationships=False,
    include_variant_summary=False,
    emit_not_found_rows=True,
)

df_fast.head(20)
```

### 4b. Full catalog mode (`input_data="__ALL__"`)

```python
df_all = bf.report.run(
    'annotation_master_gene',
    input_data='__ALL__',
    include_relationships=False,
    include_variant_summary=False,
    emit_not_found_rows=False,
)

print('rows:', len(df_all))
df_all.head(20)
```

```python
df_all.to_csv('annotation_master_gene.csv', index=False)
print('Saved: annotation_master_gene.csv')
```

### 5. Schema Check (quick QA)

```python
df_to_check = df if "df" in globals() else (df_fast if "df_fast" in globals() else (df_all if "df_all" in globals() else None))

if df_to_check is None:
    print("No DataFrame found to validate (expected df, df_fast, or df_all).")
else:
    required_cols = [
        "input_value",
        "entity_id",
        "gene_symbol",
        "hgnc_id",
        "build",
        "chromosome",
        "start_position",
        "end_position",
        "status",
    ]

    print("Dtypes:")
    display(df_to_check.dtypes.to_frame("dtype"))

    missing_cols = [c for c in required_cols if c not in df_to_check.columns]
    print("\nMissing required columns:", missing_cols if missing_cols else "none")

    for c in ["entity_id", "build", "chromosome", "start_position", "end_position"]:
        if c in df_to_check.columns:
            print(f"{c} dtype: {df_to_check[c].dtype}")
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_master_go.ipynb.md ===== -->

<h1> Biofilter - Report: GO Master Annotation </h1>

Compact GO annotation report based on GOMaster.
Returns GO identity, optional GO DAG summary/details, and optional relationship summary.

### 1. Start Biofilter

```python
from biofilter import Biofilter
```

```python
# Production (LPC, read-only):
# db_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"
# Legacy PostgreSQL server (decommissioned; see lpc__deploy.md Appendix A):
# db_uri = "postgresql+psycopg2://<user>:<password>@<SERVER_IP>:5432/biofilter_prod"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
bf = Biofilter(db_uri=db_uri, debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'annotation_master_go'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Run default mode (GO relation summary on, details off)

```python
input_go = [
    'GO:0006915',
    'GO:0008150',
    'NOT_A_GO_TERM'
]

df = bf.report.run(
    'annotation_master_go',
    input_data=input_go,
    include_aliases=True,
    include_go_relation_summary=True,
    include_go_relation_details=False,
    include_relationships=False,
    emit_not_found_rows=True,
)

print('rows:', len(df))
```

```python
focus_cols = [
    'input_value',
    'entity_id',
    'go_id',
    'go_name',
    'go_namespace',
    'go_parent_count',
    'go_child_count',
    'go_parent_relation_types',
    'go_child_relation_types',
    'status',
    'note',
]

df[[c for c in focus_cols if c in df.columns]].head(50)
```

### 4. GO details mode (parent/child GO IDs)

```python
df_go_details = bf.report.run(
    'annotation_master_go',
    input_data=input_go,
    include_aliases=True,
    include_go_relation_summary=True,
    include_go_relation_details=True,
    max_go_terms_per_side=20,
    include_relationships=False,
    emit_not_found_rows=True,
)

details_cols = [
    'input_value',
    'go_id',
    'go_parent_count',
    'go_child_count',
    'go_parent_ids',
    'go_child_ids',
]

df_go_details[[c for c in details_cols if c in df_go_details.columns]].head(50)
```

### 5. Run with relationship summary

```python
df_rel = bf.report.run(
    'annotation_master_go',
    input_data=input_go,
    include_aliases=True,
    include_go_relation_summary=True,
    include_go_relation_details=False,
    include_relationships=True,
    emit_not_found_rows=True,
)

rel_cols = [
    'input_value',
    'go_id',
    'entity_relationships_by_group',
    'total_entity_relationships',
    'status',
]

df_rel[[c for c in rel_cols if c in df_rel.columns]].head(50)
```

```python
df_rel.to_csv('annotation_master_go.csv', index=False)
print('Saved: annotation_master_go.csv')
```

### 6. Schema Check (quick QA)

```python
df_to_check = df_rel if 'df_rel' in globals() else (df if 'df' in globals() else None)

if df_to_check is None:
    print('No DataFrame found to validate (expected df or df_rel).')
else:
    required_cols = [
        'input_value',
        'entity_id',
        'go_id',
        'go_name',
        'go_namespace',
        'status',
    ]

    print('Dtypes:')
    display(df_to_check.dtypes.to_frame('dtype'))

    missing_cols = [c for c in required_cols if c not in df_to_check.columns]
    print('\nMissing required columns:', missing_cols if missing_cols else 'none')

    for c in ['entity_id', 'go_master_id', 'go_parent_count', 'go_child_count', 'total_entity_relationships']:
        if c in df_to_check.columns:
            print(f'{c} dtype: {df_to_check[c].dtype}')
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_master_pathway.ipynb.md ===== -->

<h1> Biofilter - Report: Pathway Master Annotation </h1>

Compact pathway annotation report.
Returns pathway identity, description, pathway origin (source system/data source), optional relationship summary, and compact alias list.

### 1. Start Biofilter

```python
from biofilter import Biofilter
```

```python
# Production (LPC, read-only):
# db_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"
# Legacy PostgreSQL server (decommissioned; see lpc__deploy.md Appendix A):
# db_uri = "postgresql+psycopg2://<user>:<password>@<SERVER_IP>:5432/biofilter_prod"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
bf = Biofilter(db_uri=db_uri, debug_mode=False)
```

### 2. Inspect report metadata

```python
report_name = 'annotation_master_pathway'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Run default mode (without relationships)

```python
input_pathways = [
    # 'R-HSA-109581',
    # 'hsa00010',
    # 'Cell cycle',
    # 'NOT_A_PATHWAY'
    '__ALL__'
]

df = bf.report.run(
    'annotation_master_pathway',
    input_data=input_pathways,
    include_relationships=False,
    include_aliases=True,
    emit_not_found_rows=True,
)

print('rows:', len(df))
df.head(20)
```

```python
df.to_csv('annotation_master_pathway.csv', index=False)
```

```python
focus_cols = [
    'input_value',
    'entity_id',
    'pathway_id',
    'pathway_description',
    'pathway_source_system',
    'pathway_data_source',
    'pathway_etl_package_id',
    'other_aliases',
    'status',
    'note',
]

df[[c for c in focus_cols if c in df.columns]].head(50)
```

### 4. Run with relationship summary

```python
df_rel = bf.report.run(
    'annotation_master_pathway',
    input_data=input_pathways,
    include_relationships=True,
    include_aliases=True,
    emit_not_found_rows=True,
)

rel_cols = [
    'input_value',
    'pathway_id',
    'entity_relationships_by_group',
    'total_entity_relationships',
    'status',
]

df_rel[[c for c in rel_cols if c in df_rel.columns]].head(50)
```

```python
df_rel.to_csv('annotation_master_pathway.csv', index=False)
print('Saved: annotation_master_pathway.csv')
```

### 5. Schema Check (quick QA)

```python
df_to_check = df_rel if "df_rel" in globals() else (df if "df" in globals() else None)

if df_to_check is None:
    print("No DataFrame found to validate (expected df or df_rel).")
else:
    required_cols = [
        "input_value",
        "entity_id",
        "pathway_id",
        "pathway_description",
        "pathway_source_system",
        "status",
    ]

    print("Dtypes:")
    display(df_to_check.dtypes.to_frame("dtype"))

    missing_cols = [c for c in required_cols if c not in df_to_check.columns]
    print("\nMissing required columns:", missing_cols if missing_cols else "none")

    for c in ["entity_id", "pathway_etl_package_id", "total_entity_relationships"]:
        if c in df_to_check.columns:
            print(f"{c} dtype: {df_to_check[c].dtype}")
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_master_protein.ipynb.md ===== -->

<h1> Biofilter - Report: Protein Master Annotation </h1>

Compact protein annotation report.
Returns canonical/isoform context, ProteinMaster metadata, optional Pfam summary/details, and optional relationship summary.

### 1. Start Biofilter

```python
from biofilter import Biofilter
```

```python
# Production (LPC, read-only):
# db_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"
# Legacy PostgreSQL server (decommissioned; see lpc__deploy.md Appendix A):
# db_uri = "postgresql+psycopg2://<user>:<password>@<SERVER_IP>:5432/biofilter_prod"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
bf = Biofilter(db_uri=db_uri, debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'annotation_master_protein'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Run default mode (Pfam summary on, no relationships)

```python
input_proteins = [
    # 'P04637',
    # 'P04637-2',
    # 'TP53_HUMAN',
    # 'NOT_A_PROTEIN'
    '__ALL__'
]

df = bf.report.run(
    'annotation_master_protein',
    input_data=input_proteins,
    include_pfam_summary=True,
    include_pfam_details=False,
    include_relationships=False,
    include_aliases=True,
    emit_not_found_rows=True,
)

print('rows:', len(df))
# df.head(20)
```

```python
df.to_csv('annotation_master_protein.csv', index=False)
```

```python
focus_cols = [
    'input_value',
    'entity_id',
    'canonical_entity_id',
    'protein_master_id',
    'protein_id',
    'input_is_isoform',
    'input_isoform_accession',
    'isoform_count',
    'protein_source_system',
    'protein_data_source',
    'pfam_total_count',
    'pfam_count_by_type',
    'status',
    'note',
]

df[[c for c in focus_cols if c in df.columns]].head(50)
```

### 4. Pfam details mode (IDs by type)

```python
df_pfam_details = bf.report.run(
    'annotation_master_protein',
    input_data=input_proteins,
    include_pfam_summary=True,
    include_pfam_details=True,
    max_pfam_ids_per_type=20,
    include_relationships=False,
    include_aliases=True,
    emit_not_found_rows=True,
)

pfam_cols = [
    'input_value',
    'protein_id',
    'pfam_total_count',
    'pfam_count_by_type',
    'pfam_ids_by_type',
]

df_pfam_details[[c for c in pfam_cols if c in df_pfam_details.columns]].head(50)
```

### 5. Run with relationship summary

```python
df_rel = bf.report.run(
    'annotation_master_protein',
    input_data=input_proteins,
    include_pfam_summary=True,
    include_pfam_details=False,
    include_relationships=True,
    include_aliases=True,
    emit_not_found_rows=True,
)

rel_cols = [
    'input_value',
    'protein_id',
    'entity_relationships_by_group',
    'total_entity_relationships',
    'status',
]

df_rel[[c for c in rel_cols if c in df_rel.columns]].head(50)
```

```python
df_rel.to_csv('annotation_master_protein.csv', index=False)
print('Saved: annotation_master_protein.csv')
```

### 6. Schema Check (quick QA)

```python
required_cols = [
    "input_value",
    "entity_id",
    "protein_master_id",
    "protein_id",
    "pfam_total_count",
    "status",
]

print("Dtypes:")
display(df_rel.dtypes.to_frame("dtype"))

missing_cols = [c for c in required_cols if c not in df_rel.columns]
print("\nMissing required columns:", missing_cols if missing_cols else "none")

if "entity_id" in df_rel.columns:
    print("entity_id dtype:", df_rel["entity_id"].dtype)
if "protein_master_id" in df_rel.columns:
    print("protein_master_id dtype:", df_rel["protein_master_id"].dtype)
if "pfam_total_count" in df_rel.columns:
    print("pfam_total_count dtype:", df_rel["pfam_total_count"].dtype)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_master_variant.ipynb.md ===== -->

# Biofilter — Report: Annotation Master Variant

Full annotation expansion for an input list of variants.
Returns **one row per variant × transcript annotation**, joining:

| Source | Content |
|---|---|
| `variant_masters` | Identity, population frequencies (gnomAD), pathogenicity scores |
| `variant_molecular_effects` | VEP consequence per transcript (gene, HGVS, LoF, MANE) |
| `variant_effect_predictions` | AlphaMissense score + classification |

Complements the annotation master family (`annotation_master_gene`, `annotation_master_pathway`, …)
with a **variant-centric** view.

See the explain guide: `biofilter/modules/report/reports_explain/report_annotation_master_variant.md`

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'annotation_master_variant'

print('name:', report_name)
print('\navailable columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Basic run — rsID, chr:pos, and chr:pos:ref:alt inputs

Three input formats can be mixed in the same list:

| Format | Example | Behavior |
|---|---|---|
| **rsID** | `rs429358` | dbSNP lookup |
| **chr:pos** | `chr19:44908684` | All alleles at the position (SNVs only) |
| **chr:pos:ref:alt** | `chr19:44908684:T:C` | Only the exact ref/alt variant (SNV or indel) |

Use `chr:pos:ref:alt` for credible-set / fine-mapping variants to avoid multiallelic ambiguity.

```python
input_variants = [
    'rs429358',                # APOE ε4 allele
    'rs7412',                  # APOE ε2 allele
    'rs11591147',              # PCSK9 R46L (loss-of-function)
    'chr19:44908684',          # chr:pos — returns ALL alleles at this position
    'chr19:44908684:T:C',      # chr:pos:ref:alt — returns ONLY this exact allele
]

df = bf.report.run(
    'annotation_master_variant',
    input_data=input_variants,
)

print(f'Total rows (variant × transcript): {len(df):,}')
print(f'Unique variants: {df["variant_id"].nunique()}')
df.head(10)
```

#### 3b. Focus view — key columns

```python
focus_cols = [
    'input_value', 'rsid', 'chromosome', 'position_start',
    'af', 'cadd_phred',
    'gene_symbol', 'transcript_id',
    'consequence_name', 'impact_name',
    'is_most_severe_for_variant', 'canonical', 'mane_select',
    'hgvsc', 'hgvsp',
    'lof_confidence',
    'alphamissense_score', 'alphamissense_classification',
]

df[focus_cols].head(30)
```

### 4. Most-severe transcript only

`most_severe_only=True` keeps one row per variant — the transcript annotation with the highest severity.
Useful for quick summary tables and for joining with GWAS results.

```python
df_severe = bf.report.run(
    'annotation_master_variant',
    input_data=input_variants,
    most_severe_only=True,
)

print(f'Rows with most_severe_only: {len(df_severe)} (one per variant)')

cols = [
    'rsid', 'chromosome', 'position_start', 'af',
    'cadd_phred', 'revel_max', 'spliceai_ds_max',
    'gene_symbol', 'consequence_name', 'impact_name',
    'lof_confidence', 'hgvsp',
    'alphamissense_score', 'alphamissense_classification',
]
df_severe[cols]
```

### 5. Canonical transcript only

`canonical_only=True` restricts annotations to the canonical transcript per gene.
MANE Select is the preferred choice; canonical is the fallback.

```python
df_canon = bf.report.run(
    'annotation_master_variant',
    input_data=input_variants,
    canonical_only=True,
)

print(f'Rows (canonical only): {len(df_canon)}')
df_canon[focus_cols].head(20)
```

#### 5b. MANE Select rows

```python
mane = df[df['mane_select'] == True]
print(f'MANE Select annotations: {len(mane)}')
mane[['rsid', 'gene_symbol', 'transcript_id', 'consequence_name', 'hgvsc', 'hgvsp']].head(20)
```

### 6. Exploring the full annotation

With the full (unfiltered) DataFrame, inspect the annotation landscape across all transcripts.

```python
import matplotlib.pyplot as plt
import pandas as pd

# Consequence distribution
if 'consequence_name' in df.columns and df['consequence_name'].notna().any():
    fig, axes = plt.subplots(1, 2, figsize=(14, 4))

    csq_counts = df['consequence_name'].value_counts().head(15)
    csq_counts.plot(kind='barh', ax=axes[0], color='steelblue')
    axes[0].set_title('Consequence terms (top 15)')
    axes[0].invert_yaxis()

    impact_counts = df['impact_name'].value_counts()
    impact_counts.plot(kind='bar', ax=axes[1], color=['#d73027', '#fc8d59', '#fee090', '#91bfdb'][:len(impact_counts)])
    axes[1].set_title('VEP impact distribution')
    axes[1].set_xticklabels(axes[1].get_xticklabels(), rotation=0)

    plt.tight_layout()
    plt.show()
```

```python
# Pathogenicity scores — one row per variant (most severe)
score_cols = ['rsid', 'cadd_phred', 'revel_max', 'spliceai_ds_max', 'pangolin_largest_ds', 'sift_max', 'polyphen_max']
df_scores = df[df['is_most_severe_for_variant'] == True][score_cols].drop_duplicates('rsid')

print('Pathogenicity scores (one row per variant):')
display(df_scores)
```

```python
# AlphaMissense coverage
am_available = df[df['alphamissense_score'].notna()]
print(f'Rows with AlphaMissense: {len(am_available)} / {len(df)}')

if not am_available.empty:
    print('\nAlphaMissense classifications:')
    display(am_available['alphamissense_classification'].value_counts())

    print('\nDetailed:')
    display(am_available[[
        'rsid', 'gene_symbol', 'transcript_id',
        'alphamissense_score', 'alphamissense_classification',
        'hgvsp',
    ]])
```

### 7. Filter: LoF variants (HC)

High-confidence Loss-of-Function variants: `lof_confidence == 'HC'`.

```python
df_lof = df[df['lof_confidence'] == 'HC'].copy()

print(f'LoF HC annotations: {len(df_lof)}')

lof_cols = [
    'rsid', 'gene_symbol', 'transcript_id',
    'consequence_name', 'impact_name',
    'lof_confidence', 'lof_filter',
    'hgvsc', 'hgvsp',
    'af', 'cadd_phred',
    'canonical', 'mane_select',
]
df_lof[lof_cols]
```

### 8. Filter: HIGH impact on canonical transcript

Canonical HIGH-impact annotations — typically the most clinically relevant rows.

```python
df_high = df[
    (df['impact_name'] == 'HIGH') &
    (df['canonical'] == True)
].copy()

print(f'HIGH impact on canonical transcript: {len(df_high)}')
df_high[[
    'rsid', 'gene_symbol', 'transcript_id',
    'consequence_name', 'hgvsc', 'hgvsp',
    'lof_confidence', 'af', 'cadd_phred',
    'alphamissense_score', 'alphamissense_classification',
]].head(30)
```

### 9. Annotation summary per variant

Compact one-row-per-variant summary with counts and top annotations.

```python
def _first(series):
    vals = series.dropna()
    return vals.iloc[0] if not vals.empty else None

summary = (
    df.groupby(['variant_id', 'rsid', 'chromosome', 'position_start'])
    .agg(
        af=('af', _first),
        cadd_phred=('cadd_phred', _first),
        revel_max=('revel_max', _first),
        spliceai_ds_max=('spliceai_ds_max', _first),
        n_transcripts=('transcript_id', 'nunique'),
        n_genes=('gene_symbol', 'nunique'),
        worst_consequence=('consequence_name', _first),
        worst_impact=('impact_name', _first),
        lof_confidence=('lof_confidence', _first),
        alphamissense_score=('alphamissense_score', _first),
        alphamissense_classification=('alphamissense_classification', _first),
    )
    .reset_index()
    .sort_values(['chromosome', 'position_start'])
)

print(f'Summary: {len(summary)} variants')
display(summary)
```

### 10. Input from file

```python
from pathlib import Path

tmp_dir = Path('tmp/annotation_master_variant_tutorial')
tmp_dir.mkdir(parents=True, exist_ok=True)

input_file = tmp_dir / 'variants.txt'
input_file.write_text('rs429358\nrs7412\nrs11591147\n')

df_file = bf.report.run(
    'annotation_master_variant',
    input_data=str(input_file),
    most_severe_only=True,
)

print(f'Rows from file (most_severe_only): {len(df_file)}')
df_file[focus_cols]
```

### 11. Not-found and invalid inputs

The report gracefully handles unknown variants and malformed inputs via `status` and `note`.

```python
df_mixed = bf.report.run(
    'annotation_master_variant',
    input_data=[
        'rs429358',              # valid rsID
        'rs9999999999',          # rsID not in DB
        'not_a_variant',         # invalid format
        'chr19:44908684',        # valid chr:pos
        'chr19:44908684:T:C',    # valid chr:pos:ref:alt
        'chr1:100000:N:G',       # invalid base (N) — rejected
    ],
    most_severe_only=True,
)

display(df_mixed[['input_value', 'rsid', 'gene_symbol', 'consequence_name', 'status', 'note']])
```

### 12. Export

```python
# Full annotation (all transcripts)
out_full = tmp_dir / 'annotation_master_variant_full.csv'
df.to_csv(out_full, index=False)
print(f'Full  → {out_full}  ({len(df):,} rows)')

# Most-severe only (one row per variant)
out_severe = tmp_dir / 'annotation_master_variant_most_severe.csv'
df_severe.to_csv(out_severe, index=False)
print(f'Most severe → {out_severe}  ({len(df_severe):,} rows)')
```

### 13. Running on the UPenn LPC (Apptainer)

For users without local DB access, this report runs end-to-end on the **Penn LPC** cluster using the
pre-built Apptainer image. The image bundles BF4 + PostgreSQL — no `.biofilter.toml`, no DB credentials,
no Python environment to set up. You hand it an input file and a destination CSV path; it gives you back
the annotated table.

> **When to use the LPC/HPC**
> - You don't want to manage the BF4 environment locally.
> - You're already on the cluster running other genomics workflows.

See also:
- [`lpc__quickstart.md`](lpc__quickstart.md) — copy-paste minimal recipe for first-time users
- [`lpc__deploy.md`](lpc__deploy.md) — maintainer guide for installing / updating the LPC image and DB

#### 13a. Basic call — one input file → one CSV

This is the smallest working invocation. The temp dir is needed because the image
starts its own PostgreSQL inside the container; the bind mounts give it scratch space.
Everything in `$WORKSPACE` is visible inside the container as `/workspace`.

```bash
module load apptainer
export WORKSPACE=/project/<your-project>/bf4_runs

TMP=$(mktemp -d) && mkdir -p "$TMP/tmp" "$TMP/pg-run" && \
apptainer run --writable-tmpfs --pwd /tmp \
  --bind /project/hall_shared/biofilter/databases/20260514/pgdata:/var/lib/postgresql/data \
  --bind "$TMP/tmp:/tmp" \
  --bind "$TMP/pg-run:/var/run/postgresql" \
  --bind "$WORKSPACE:/workspace" \
  /project/hall_shared/biofilter/images/bf4-hpc-4.1.2.sif \
  biofilter report run \
    --name annotation_master_variant \
    --input-file /workspace/variants.txt \
    --output /workspace/variant_annotations.csv && \
rm -rf "$TMP"
```

Result: `$WORKSPACE/variant_annotations.csv`.

#### 13b. Prepare the input file

The file is plain text — one variant per line. Mix formats freely:

```bash
cat > "$WORKSPACE/variants.txt" <<'EOF'
rs429358
rs7412
chr19:44908684
chr19:44908684:T:C
1:6203732:A:G
EOF
```

The single-quoted `<<'EOF'` heredoc preserves everything literally, including the colons in
`chr:pos:ref:alt`. For credible-set TSVs already in `chr:pos:ref:alt` form, just pipe the
relevant column straight in.

#### 13c. Passing parameters — `most_severe_only`, `canonical_only`

The CLI accepts `--param KEY=VALUE`, but **the shell inside the container eats double quotes**,
so JSON values like `["Pathway"]` break. The robust pattern is a JSON file referenced via
`--params-file`:

```bash
cat > "$WORKSPACE/annot_params.json" <<'EOF'
{
  "most_severe_only": true,
  "canonical_only": true
}
EOF

TMP=$(mktemp -d) && mkdir -p "$TMP/tmp" "$TMP/pg-run" && \
apptainer run --writable-tmpfs --pwd /tmp \
  --bind /project/hall_shared/biofilter/databases/20260514/pgdata:/var/lib/postgresql/data \
  --bind "$TMP/tmp:/tmp" \
  --bind "$TMP/pg-run:/var/run/postgresql" \
  --bind "$WORKSPACE:/workspace" \
  /project/hall_shared/biofilter/images/bf4-hpc-4.1.2.sif \
  biofilter report run \
    --name annotation_master_variant \
    --input-file /workspace/variants.txt \
    --params-file /workspace/annot_params.json \
    --output /workspace/variant_annotations_compact.csv && \
rm -rf "$TMP"
```

Combine `most_severe_only=true` + `canonical_only=true` for a compact **1 row per variant on the
canonical transcript** output — ideal for downstream merging with GWAS / credible-set tables.



<!-- ===== SOURCE FILE: notebooks/Templates/reports__annotations_variant_regulatory_evidence.ipynb.md ===== -->

# 📘 Biofilter — Report: Annotation Variant Regulatory Evidence

**Variant ↔ gene regulatory evidence (eQTL / sQTL).**

This report annotates variants with gene-regulatory evidence stored in `variant_gene_regulatory_evidence`. It accepts three input modes selected via `--param input_type`:

- **`gene`** — list of gene symbols (HGNC), Ensembl IDs, Entrez IDs, or any alias resolvable via `entity_aliases`. Resolves to gene region and pulls variants in `[start - flanking_bp, end + flanking_bp]`.
- **`coord`** — list of `chr:pos` coordinates. Pulls variants in `[pos - flanking_bp, pos + flanking_bp]`.
- **`rsid`** — list of dbSNP rsids. Direct lookup against `variant_masters.rsid` (scans all chromosome partitions; small input lists only).

Each emitted row joins a variant to one row of `variant_gene_regulatory_evidence` — i.e. one tissue × one regulated gene × one qtl_type per row.

Output is **gene-centric**: every row carries:
- the eQTL **target** gene (the gene the variant regulates, from the eQTL table)
- the **position** gene (the gene whose body contains the variant, resolved via `entity_locations`)

These two genes can differ, since cis-eQTLs in GTEx reach up to ±1 Mb of the TSS — a variant inside gene A may regulate gene B in cis.

---

### Methods used
- `bf.report.explain("annotation_variant_regulatory_evidence")`
- `bf.report.example_input("annotation_variant_regulatory_evidence")`
- `bf.report.run("annotation_variant_regulatory_evidence", **params)`

### Required upstream data
- A regulatory-evidence DTP must have populated `variant_gene_regulatory_evidence`. The default is GTEx v10 brain (`dtp_variant_eqtl_gtex`).

---

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
```

---

### 2. Inspect the report contract

`explain()` documents every parameter; `example_input()` returns a ready-to-tweak dict with all defaults.

```python
print(bf.report.explain("annotation_variant_regulatory_evidence"))
```

```python
# Full parameter template with defaults
bf.report.example_input("annotation_variant_regulatory_evidence")
```

---

### 3. Run with built-in example input

Default example: `input_data=["APOE"]`, `input_type="gene"`, no filters.
Returns one row per (variant × tissue × regulated gene) overlapping the APOE locus.

```python
import time

start = time.time()
df = bf.report.run_example("annotation_variant_regulatory_evidence")
elapsed = time.time() - start

print(
    f"Rows: {len(df)} | "
    f"Unique variants: {df['variant_id'].nunique()} | "
    f"Tissues: {df['bio_context'].nunique()} | "
    f"elapsed: {elapsed:.2f}s"
)
df.head()
```

```python
# The 5 gene columns side by side — to see when input/position/eqtl-target diverge
gene_cols = [
    "input_gene_symbol",
    "position_gene_symbol", "position_gene_ensembl",
    "eqtl_target_symbol", "eqtl_target_ensembl",
]
df[[c for c in gene_cols if c in df.columns]].head(20)
```

---

### 4. Gene mode — multiple AD-relevant genes, brain cortex only

Filter to a single tissue (`Brain_Cortex`) and request a small `max_rows` for a quick preview.

```python
df_cortex = bf.report.run(
    "annotation_variant_regulatory_evidence",
    # input_data=["APOE", "APP", "PSEN1", "PSEN2", "BIN1", "CLU", "TOMM40"],
    input_data=["APOE"],
    input_type="gene",
    # tissue="Brain_Cortex",
    max_rows=2000,
)

print(
    f"Rows: {len(df_cortex)} | "
    f"Unique variants: {df_cortex['variant_id'].nunique()} | "
    f"Input genes resolved: {df_cortex['input_gene_symbol'].nunique()}"
)
df_cortex.groupby("input_gene_symbol")["variant_id"].nunique().rename("variants_with_eqtl").reset_index()
```

```python
# Rows where the variant regulates a *different* gene than the one it sits inside.
# These are the biologically interesting cis-eQTL events worth flagging.
diverged = df_cortex[
    df_cortex["position_gene_symbol"].notna()
    & df_cortex["eqtl_target_symbol"].notna()
    & (df_cortex["position_gene_symbol"] != df_cortex["eqtl_target_symbol"])
]
print(f"Variants regulating a neighboring gene: {len(diverged)} rows / {diverged['variant_id'].nunique()} unique variants")
diverged[
    ["rsid", "position_gene_symbol", "eqtl_target_symbol", "bio_context", "beta", "p_value"]
].sort_values("p_value").head(20)
```

---

### 5. `flanking_bp` — extend the gene region for cis-window queries

GTEx cis-eQTLs reach up to ±1 Mb of the TSS. `flanking_bp=0` (the default) only captures evidence on variants **inside** the gene body. Bumping `flanking_bp` recovers the full cis-window.

```python
df_apoe_strict = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["APOE"],
    input_type="gene",
    flanking_bp=0,
)

df_apoe_500k = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["APOE"],
    input_type="gene",
    flanking_bp=500_000,
)

df_apoe_1mb = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["APOE"],
    input_type="gene",
    flanking_bp=1_000_000,
)

print(f"flanking_bp=0      → {df_apoe_strict['variant_id'].nunique()} unique variants, {len(df_apoe_strict)} evidence rows")
print(f"flanking_bp=500kb  → {df_apoe_500k['variant_id'].nunique()} unique variants, {len(df_apoe_500k)} evidence rows")
print(f"flanking_bp=1Mb    → {df_apoe_1mb['variant_id'].nunique()} unique variants, {len(df_apoe_1mb)} evidence rows")
```

---

### 6. Coord mode — chr:pos lookup

Useful when you have a position from external GWAS/QTL output and want to know what regulatory evidence BF4 has at that locus.

```python
# Famous APOE-ε4 defining variant: rs429358 = chr19:44908684 (GRCh38)
df_coord = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["chr19:44908684"],
    input_type="coord",
    flanking_bp=0,   # exact position
)

print(f"Rows: {len(df_coord)} | Unique variants: {df_coord['variant_id'].nunique()}")
df_coord[[
    "input_term", "rsid", "position_gene_symbol", "eqtl_target_symbol",
    "bio_context", "beta", "p_value",
]].head(20)
```

```python
# Same coord, but expand to ±10 kb to pick up neighboring variants with eQTL evidence
df_coord_window = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["chr19:44908684"],
    input_type="coord",
    flanking_bp=10_000,
)

print(
    f"flanking_bp=10kb → {df_coord_window['variant_id'].nunique()} unique variants "
    f"with regulatory evidence in this region"
)
df_coord_window[[
    "rsid", "position_start", "position_gene_symbol", "eqtl_target_symbol",
    "bio_context", "p_value",
]].sort_values("p_value").head(15)
```

---

### 7. rsid mode — direct lookup

Pass one or many rsids. The query scans every chromosome partition using each partition's `rsid` index — fine for small input lists, expensive for >10K rsids in a single call.

```python
df_rsids = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["rs429358", "rs7412", "rs6859"],
    input_type="rsid",
)

print(f"Rows: {len(df_rsids)} | Unique rsids resolved: {df_rsids['rsid'].nunique()}")
df_rsids[[
    "input_term", "rsid", "chromosome", "position_start",
    "position_gene_symbol", "eqtl_target_symbol",
    "bio_context", "beta", "p_value",
]].sort_values(["input_term", "p_value"]).head(20)
```

---

### 8. Tissue filter — multiple brain regions

`tissue` accepts a CSV-string or a list. Useful for asking: "is this eQTL active in cortex AND hippocampus, or just one of them?"

```python
df_multi_tissue = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["APOE"],
    input_type="gene",
    tissue=["Brain_Cortex", "Brain_Hippocampus", "Brain_Frontal_Cortex_BA9"],
)

df_multi_tissue.groupby("bio_context")["variant_id"].nunique().rename("variants").reset_index()
```

```python
# Variants that are eQTLs across ALL three tissues
shared = (
    df_multi_tissue.groupby("variant_id")["bio_context"].nunique()
    .loc[lambda s: s == 3]
    .index
)
df_shared = df_multi_tissue[df_multi_tissue["variant_id"].isin(shared)]
print(f"Shared across all 3 tissues: {len(shared)} variants")
df_shared[["rsid", "bio_context", "eqtl_target_symbol", "beta", "p_value"]].sort_values(
    ["rsid", "bio_context"]
).head(20)
```

---

### 9. Significance filter — `p_value_max`

GTEx significant_pairs is already pre-filtered, but you can tighten further to focus on the strongest associations.

```python
df_strong = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["APOE", "BIN1", "CLU"],
    input_type="gene",
    # tissue="Brain_Cortex",
    p_value_max=1e-10,
)

print(f"p_value ≤ 1e-10 → {df_strong['variant_id'].nunique()} unique variants, {len(df_strong)} rows")
df_strong[["rsid", "position_gene_symbol", "eqtl_target_symbol", "beta", "p_value"]].sort_values(
    "p_value"
).head(15)
```

---

### 10. Inspecting `details` — auxiliary fields preserved as JSON

The DTP packs source-specific extras (`af`, `ma_samples`, `tss_distance`, `pval_beta`, `gene_id_versioned`, etc.) into `details` as JSON. Easy to expand into columns when needed.

```python
import json
import pandas as pd

df_details = bf.report.run(
    "annotation_variant_regulatory_evidence",
    input_data=["APOE"],
    input_type="gene",
    # tissue="Brain_Cortex",
    max_rows=20,
)

expanded = pd.json_normalize(df_details["details"].dropna().apply(json.loads))
expanded.head()
```

---

### 11. Resolution failure handling

Failure cases return a single-row DataFrame with a non-null `resolution_status` — the report never raises.

```python
cases = [
    ("unknown gene",   {"input_data": ["NOTAREALGENE99"],   "input_type": "gene"}),
    ("unknown rsid",   {"input_data": ["rs9999999999"],     "input_type": "rsid"}),
    ("bad coord",      {"input_data": ["chrZZ:abc"],         "input_type": "coord"}),
    ("empty input",    {"input_data": [],                    "input_type": "gene"}),
]

for label, params in cases:
    try:
        result = bf.report.run("annotation_variant_regulatory_evidence", **params)
        status = result["resolution_status"].iloc[0]
        rows = len(result)
        print(f"{label:<20} → status={status!r}  rows={rows}")
    except Exception as exc:
        print(f"{label:<20} → raised: {type(exc).__name__}: {exc}")
```

---

### 12. CLI reference

```bash
# ── Single gene, default everything
biofilter report run \
  --report-name annotation_variant_regulatory_evidence \
  --input "APOE" \
  --param input_type=gene

# ── Multiple genes, single tissue
biofilter report run \
  --report-name annotation_variant_regulatory_evidence \
  --input "APOE,BIN1,CLU,TOMM40" \
  --param input_type=gene \
  --param tissue=Brain_Cortex \
  --param max_rows=5000

# ── Gene + cis-window 1Mb + significance filter
biofilter report run \
  --report-name annotation_variant_regulatory_evidence \
  --input "APOE" \
  --param input_type=gene \
  --param flanking_bp=1000000 \
  --param p_value_max=1e-8

# ── Single rsid
biofilter report run \
  --report-name annotation_variant_regulatory_evidence \
  --input "rs429358" \
  --param input_type=rsid

# ── Coord, tight window
biofilter report run \
  --report-name annotation_variant_regulatory_evidence \
  --input "chr19:44908684" \
  --param input_type=coord \
  --param flanking_bp=1000

# ── Save output
biofilter report run \
  --report-name annotation_variant_regulatory_evidence \
  --input "APOE,BIN1,CLU" \
  --param input_type=gene \
  --param tissue=Brain_Cortex \
  --output apoe_region_eqtls.csv

# ── Inspect params template
biofilter report run \
  --report-name annotation_variant_regulatory_evidence \
  --params-template

# ── Read the explain doc
biofilter report explain --report-name annotation_variant_regulatory_evidence
```

---

### 13. Practical tips

- **`flanking_bp` matters for eQTLs.** Default `0` returns evidence only on variants **inside** the gene body. For typical cis-eQTL questions, use `flanking_bp=500_000` or `1_000_000`.
- **`position_gene` vs `eqtl_target` divergence is the interesting signal.** Rows where they differ point to distal regulators (variant in gene A, but eQTL of gene B).
- **`tissue` filter is cheap** — pushed to SQL. Use it freely.
- **rsid mode without chromosome hint scans 25 partitions.** OK for tens to hundreds of rsids; for huge lists prefer pre-resolving to coord and using `coord` mode.
- **`max_rows` is a hard cap, not a sample.** If you hit it, narrow down with `tissue` / `p_value_max` rather than just raising the cap.
- **Schema dependency:** this report needs `variant_gene_regulatory_evidence` populated. As of 4.1.x the only loader is `dtp_variant_eqtl_gtex` (GTEx v10 brain — 13 tissues).



<!-- ===== SOURCE FILE: notebooks/Templates/reports__db_pg_index_stats.ipynb.md ===== -->

<h1> 📘 Biofilter — Report: PostgreSQL Index Stats </h1>

PostgreSQL-only index observability report (size, properties, usage).

### 1. Start Biofilter

```python
from biofilter import Biofilter
bf = Biofilter(debug_mode=False)
```

### 2. Inspect report metadata

```python
bf.report.explain("db_pg_index_stats")
```

```python
bf.report.available_columns("db_pg_index_stats")
```

### 3. Run report (PostgreSQL only)

```python
df = bf.report.run("db_pg_index_stats")
print(f"Rows: {len(df)}")
df.head()
```

### 4. Filter by table/index and choose columns

```python
df_filtered = bf.report.run(
    "db_pg_index_stats",
    schema="public",
    table=["variant_masters"],
    include_usage=True,
    output_columns=["schema_name", "table_name", "index_name", "index_size", "idx_scan"],
)
print(f"Rows: {len(df_filtered)}")
df_filtered.head(30)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__db_pg_table_stats.ipynb.md ===== -->

<h1> 📘 Biofilter — Report: PostgreSQL Table Stats </h1>

PostgreSQL-only storage observability report for tables/partitions.

### 1. Start Biofilter

```python
from biofilter import Biofilter
bf = Biofilter(debug_mode=False)
```

### 2. Inspect report metadata

```python
print(bf.report.explain("db_pg_table_stats"))
```

```python
bf.report.available_columns("db_pg_table_stats")
```

### 3. Run report (PostgreSQL only)

```python
df = bf.report.run("db_pg_table_stats")
print(f"Rows: {len(df)}")
df.head()
```

### 4. Filter by schema/table and choose columns

```python
df_filtered = bf.report.run(
    "db_pg_table_stats",
    schema="public",
    table=["variant", "entity"],
    output_columns=["schema_name", "table_name", "total_bytes", "n_indexes"],
)
print(f"Rows: {len(df_filtered)}")
df_filtered.head(20)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__entity_filter.ipynb.md ===== -->

<h1> 📘 Biofilter — Report: Entity Filter </h1>

Validate a list of entity names and inspect matching/conflict flags.

### 1. Start Biofilter

```python
from biofilter import Biofilter
bf = Biofilter(debug_mode=False)
```

### 2. Run with custom entity list (API parameters required)

```python
entity_list = ["TP53", "BRCA1", "APOE", "NOT_FOUND_ENTITY"]

df = bf.report.run("entity_filter", input_data=entity_list)
print(f"Rows: {len(df)}")
df.head()
```

```python
cols = [
    "input_original", "primary_name", "group_name",
    "has_conflict", "is_deactive", "observation"
]
df[[c for c in cols if c in df.columns]].head(50)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__entity_neighborhood_summary.ipynb.md ===== -->

<h1> 📘 Biofilter — Report: Entity Neighborhood Summary </h1>

Resolve a heterogeneous list of inputs (genes, diseases, pathways, proteins, chemicals, GO terms) into entities and return a 1-hop neighborhood summary, with neighbor counts and primary names grouped by entity type.

Engine-agnostic: works on PostgreSQL **and** SQLite. Fuzzy matching uses `rapidfuzz` client-side, no DB extension required.

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
```

### 2. Mixed inputs with type hints

Type prefixes (`gene:`, `disease:`, `pathway:`, `protein:`, `chemical:`, `go:`) scope the resolution to the matching `EntityGroup`. This avoids cross-domain matches when the same string exists in multiple groups.

```python
items = [
    "gene:BRCA1",
    "disease:Alzheimer disease",
    "pathway:DNA repair",
    "APOE",  # no type hint — searched across all groups
]

df = bf.report.run(
    "entity_neighborhood_summary",
    items=items,
    match_mode="exact",
    aliases_top_n=10,
    neighbors_top_n_per_type=20,
    emit_not_found_rows=True,
)

print(f"Rows: {len(df)}")
df.head()
```

### 3. Recommended demo columns

```python
cols = [
    "Input Word",
    "Entity ID",
    "Entity Type",
    "Exact Match",
    "Matched Name",
    "Primary Alias",
    "Degree Total (1-hop)",
    "Degree By Type (1-hop)",
    "Resolve Status",
]
df[[c for c in cols if c in df.columns]]
```

### 4. `like` mode — substring matches

Useful when the input is a partial term and you want to find every entity whose alias contains it. Multiple aliases of the same entity collapse into a single row.

```python
df_like = bf.report.run(
    "entity_neighborhood_summary",
    items=["pathway:signaling", "disease:alzheimer"],
    match_mode="like",
    neighbors_top_n_per_type=10,
)

print(f"Rows: {len(df_like)}")
df_like[["Input Word", "Matched Name", "Exact Match", "Primary Alias", "Entity Type"]].head(20)
```

### 5. `fuzzy` mode — similarity matching

Uses `rapidfuzz` token-sort ratio for typo-tolerant matching. Lower the `similarity_threshold` (default 80) when inputs are short forms (e.g. `"alzheimer"` vs `"Alzheimer disease"`).

```python
df_fuzzy = bf.report.run(
    "entity_neighborhood_summary",
    items=["gene:BRCA1", "disease:alzheimers"],  # legacy / typo
    match_mode="fuzzy",
    similarity_threshold=70,
)

df_fuzzy[["Input Word", "Matched Name", "Resolve Score", "Primary Alias"]]
```

### 6. Inspect the per-type neighbor lists

Each `EntityGroup` in the database becomes a column on the output (`Genes`, `Pathways`, `Diseases`, etc.) with a JSON-encoded list of neighbor primary names. Useful for quickly seeing what an input touches.

```python
import json

row = df.iloc[0]  # first resolved entity
print(f"{row['Input Word']} → {row['Primary Alias']} ({row['Entity Type']})")
print(f"Total neighbors: {row['Degree Total (1-hop)']}")
print(f"By type: {row['Degree By Type (1-hop)']}")

for col in ("Genes", "Pathways", "Diseases", "Proteins"):
    if col in df.columns:
        neighbors = json.loads(row[col]) if row[col] else []
        if neighbors:
            print(f"\n{col} ({len(neighbors)}):")
            for n in neighbors[:5]:
                print(f"  - {n}")
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__entity_relationship_model.ipynb.md ===== -->

<h1> Biofilter - Report: Entity Relationship Model </h1>

Explore relationship modeling using EntityAlias resolution and EntityRelationship links.

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
print('name:', 'entity_relationship_model')
print('available columns:')
print(bf.report.available_columns('entity_relationship_model'))

print('\nexample_input:')
print(bf.report.example_input('entity_relationship_model'))

print('\nexplain:')
print(bf.report.explain('entity_relationship_model'))
```

### 3. Run scope = input_to_any (default)
Input entities can match either side (entity_1 or entity_2).

```python
inputs = ['TP53', 'BRCA1', 'NOT_FOUND_ENTITY']

df_any = bf.report.run(
    'entity_relationship_model',
    input_data=inputs,
    relationship_scope='input_to_any',
)

print('rows:', len(df_any))
df_any.head(20)
```

### 4. Restrict output related entity groups
Example: keep only Pathway and Protein relationships.

```python
df_out_groups = bf.report.run(
    'entity_relationship_model',
    input_data=['TP53', 'BRCA1'],
    relationship_scope='input_to_any',
    output_entity_groups=['Pathway', 'Protein'],
)

cols = [
    'input_original',
    'input_primary_name',
    'relationship_type',
    'related_primary_name',
    'related_group_name',
    'match_side',
    'direction',
    'observation',
]

df_out_groups[cols].head(30)
```

### 5. Scope = between_inputs
Return only relationships where both terms are in the resolved input set.

```python
df_between = bf.report.run(
    'entity_relationship_model',
    input_data=['TP53', 'BRCA1', 'APOE'],
    relationship_scope='between_inputs',
    deduplicate_pairs=True,
)

df_between[cols + ['entity_1_primary_name', 'entity_2_primary_name']].head(30)
```

### 6. Restrict input entity groups
Only resolve input aliases that belong to specific groups.

```python
df_input_groups = bf.report.run(
    'entity_relationship_model',
    input_data=['TP53', 'DNA_REPAIR_PATHWAY'],
    input_entity_groups=['Gene'],
    relationship_scope='input_to_any',
)

df_input_groups[['input_original', 'input_primary_name', 'input_group_name', 'observation']].drop_duplicates()
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__etl_packages.ipynb.md ===== -->

<h1> 📘 Biofilter — Report: ETL Packages </h1>

Detailed package-level ETL audit report.

### 1. Start Biofilter

```python
from biofilter import Biofilter
bf = Biofilter(debug_mode=False)
```

### 2. Inspect report metadata

```python
print(bf.report.explain("etl_packages"))
```

```python
bf.report.available_columns("etl_packages")
```

### 3. Run default report

```python
df = bf.report.run("etl_packages")
print(f"Rows: {len(df)}")
df.head()
```

### 4. Run with filters

```python
df_filtered = bf.report.run(
    "etl_packages",
    source_system="NCBI",
    data_sources=["dbsnp_chr1", "dbsnp_chr2"],
    only_active=True,
)
print(f"Rows: {len(df_filtered)}")
df_filtered.head()
```

```python
cols = [
    "package_id", "source_system", "data_source", "status", "operation_type",
    "extract_status", "transform_status", "load_status"
]
df_filtered[[c for c in cols if c in df_filtered.columns]].head(30)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__etl_status.ipynb.md ===== -->

<h1> 📘 Biofilter — Report: ETL Status </h1>

This notebook demonstrates the consolidated ETL status report.

### 1. Start Biofilter

```python
from biofilter import Biofilter
bf = Biofilter(debug_mode=False)
```

```python
# Production (LPC, read-only):
# db_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"
# Legacy PostgreSQL server (decommissioned; see lpc__deploy.md Appendix A):
# db_uri = "postgresql+psycopg2://<user>:<password>@<SERVER_IP>:5432/biofilter_prod"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
bf = Biofilter(db_uri=db_uri, debug_mode=False)
```

### 2. Inspect report metadata

```python
print(bf.report.explain("etl_status"))
```

```python
bf.report.available_columns("etl_status")
```

### 3. Run default report

```python
df = bf.report.run("etl_status")
print(f"Rows: {len(df)}")
df.head()
```

```python
df.to_clipboard()
```

### 4. Run with filters

```python
df_filtered = bf.report.run(
    "etl_status",
    data_sources=["hgnc", "dbsnp_chr1"],
    only_active=True,
)
print(f"Rows: {len(df_filtered)}")
df_filtered.head()
```

```python
# Suggested dashboard columns
cols = [
    "source_system", "data_source", "extract_status", "transform_status",
    "load_status", "pipeline_ok", "latest_error"
]
df_filtered[[c for c in cols if c in df_filtered.columns]].head(20)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__gene_to_variant_filtering.ipynb.md ===== -->

# 📘 Biofilter — Report: Gene to Variant Filtering

**Phase 2 of the single-variant SNP×SNP interaction pipeline.**

Given a list of gene symbols, this report:

1. Resolves symbols → `entity_ids` (via `entity_aliases`).
2. Resolves `entity_ids` → genomic loci (`entity_locations`, filtrado por build).
3. Pre-resolve consequence/impact filter names → IDs (SQL-level filtering).
4. Queries `variant_masters` + `variant_molecular_effects` per chromosome via a **temporary gene-range table** — one query per chromosome partition.
5. LEFT JOINs `variant_effect_predictions` for AlphaMissense scores.
6. Returns **1 row per (gene × variant)** when `most_severe_only=True`, or **1 row per (gene × variant × transcript)** when `most_severe_only=False`.

All heavy filters (impact, consequence, LoF, AF, CADD, SIFT, PolyPhen) are pushed to SQL. AlphaMissense filters are applied post-query.

---

### Methods used
- `bf.report.explain("gene_to_variant_filtering")`
- `bf.report.example_input("gene_to_variant_filtering")`
- `bf.report.run("gene_to_variant_filtering", **params)`

---

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
```

---

### 2. Inspect the report contract

```python
print(bf.report.explain("gene_to_variant_filtering"))
```

```python
# Full parameter template with defaults
bf.report.example_input("gene_to_variant_filtering")
```

---

### 3. Run with built-in example input

Runs with `gene_symbols=["APOE"]`, `most_severe_only=True`, no filters.  
Returns one row per variant overlapping the APOE locus.

```python
import time

start = time.time()
df = bf.report.run_example("gene_to_variant_filtering")
elapsed = time.time() - start

print(f"Rows: {len(df)} | Unique variants: {df['variant_id'].nunique()} | elapsed: {elapsed:.2f}s")
df.head()
```

```python
# Quick view of key columns
display_cols = [
    "gene_symbol", "chromosome", "position_start", "rsid",
    "af", "consequence_name", "impact_name",
    "lof_confidence", "cadd_phred",
    "alphamissense_score", "alphamissense_classification",
]
df[[c for c in display_cols if c in df.columns]].head(20)
```

---

### 4. Impact filter — HIGH and MODERATE only

The most common first-pass filter for coding-variant studies.

```python
df_impact = bf.report.run(
    "gene_to_variant_filtering",
    # gene_symbols=["APOE", "CLU", "TOMM40", "BIN1"],
    gene_symbols=["APOE"],
    # impact_filter=["HIGH", "MODERATE"],
    lof_confidence_filter=["HC", "LC"],
    af_max=0.05,
    most_severe_only=True,
)

print(f"Genes: {df_impact['gene_symbol'].nunique()} | Rows: {len(df_impact)} | Unique variants: {df_impact['variant_id'].nunique()}")
df_impact.groupby(["gene_symbol", "impact_name"])["variant_id"].count().rename("variant_count").reset_index()
```

```python
df_impact.to_clipboard()
```

---

### 5. Allele frequency filter — rare variants

`af_max=0.01` keeps variants with frequency < 1% — standard rare-variant threshold.

```python
df_rare = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU", "TOMM40"],
    af_max=0.01,
    impact_filter=["HIGH", "MODERATE"],
    most_severe_only=True,
)

df_all_freq = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU", "TOMM40"],
    impact_filter=["HIGH", "MODERATE"],
    most_severe_only=True,
)

print(f"AF < 0.01  → {df_rare['variant_id'].nunique()} variants")
print(f"No AF filter → {df_all_freq['variant_id'].nunique()} variants")

# AF distribution of filtered set
df_rare["af"].describe()
```

---

### 6. LoF confidence filter

LOFTEE annotates loss-of-function variants as `HC` (High Confidence) or `LC` (Low Confidence).  
Note: applying this filter **keeps only** variants that have a LoF confidence annotation — it excludes non-LoF variants.

```python
df_lof_hc = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["BRCA1", "BRCA2", "TP53"],
    lof_confidence_filter=["HC"],
    impact_filter=["HIGH"],
    most_severe_only=True,
)

print(f"Rows: {len(df_lof_hc)} | Unique variants: {df_lof_hc['variant_id'].nunique()}")

display_cols = [
    "gene_symbol", "rsid", "position_start", "af",
    "consequence_name", "impact_name",
    "lof_confidence", "hgvsc", "hgvsp",
]
df_lof_hc[[c for c in display_cols if c in df_lof_hc.columns]].head(20)
```

---

### 7. Consequence type filter

`consequence_type_filter` accepts names at any level: consequence group, category, or individual consequence name.  
They are resolved to `consequence_id`s before the main query — no post-filtering.

```python
df_missense = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU"],
    consequence_type_filter=["missense_variant"],
    most_severe_only=True,
)

print(f"missense_variant → {df_missense['variant_id'].nunique()} variants")
df_missense[["gene_symbol", "rsid", "af", "consequence_name", "hgvsp", "alphamissense_score", "alphamissense_classification"]].head(15)
```

---

### 8. Effect prediction filters — AlphaMissense

AlphaMissense classifies missense variants as `likely_pathogenic`, `ambiguous`, or `likely_benign`.  
Filters are applied Python-side after a LEFT JOIN on `variant_effect_predictions`.

```python
df_am = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU", "TOMM40"],
    consequence_type_filter=["missense_variant"],
    alphamissense_classification=["likely_pathogenic"],
    most_severe_only=True,
)

print(f"Likely pathogenic missense → {len(df_am)} rows, {df_am['variant_id'].nunique()} variants")
df_am[["gene_symbol", "rsid", "af", "hgvsp", "alphamissense_score", "alphamissense_classification"]].sort_values("alphamissense_score", ascending=False).head(20)
```

```python
# AlphaMissense score distribution across classifications
df_all_missense = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE"],
    consequence_type_filter=["missense_variant"],
    most_severe_only=True,
)

df_all_missense["alphamissense_classification"].value_counts(dropna=False)
```

---

### 9. CADD / SIFT / PolyPhen filters

These scores are stored directly on `variant_masters` (`cadd_phred`, `sift_max`, `polyphen_max`) — filtered in SQL, no extra join.

```python
df_cadd = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE", "CLU"],
    impact_filter=["HIGH", "MODERATE"],
    cadd_phred_min=20,
    most_severe_only=True,
)

print(f"CADD Phred ≥ 20 → {df_cadd['variant_id'].nunique()} variants")
df_cadd[["gene_symbol", "rsid", "af", "consequence_name", "cadd_phred", "sift_max", "polyphen_max"]].sort_values("cadd_phred", ascending=False).head(15)
```

---

### 10. `most_severe_only=False` — transcript-level output

One row per variant × transcript. Useful for splice analysis, MANE Select filtering, or canonical-transcript studies.  
AlphaMissense and other variant-level scores repeat on every transcript row.

```python
df_transcripts = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE"],
    impact_filter=["HIGH", "MODERATE"],
    most_severe_only=False,   # ← transcript-level
)

print(f"most_severe_only=False → {len(df_transcripts)} rows, {df_transcripts['variant_id'].nunique()} unique variants")

# Show a single variant with multiple transcripts
ex_vid = df_transcripts["variant_id"].iloc[0]
df_transcripts[df_transcripts["variant_id"] == ex_vid][
    ["variant_id", "transcript_id", "consequence_name", "impact_name", "canonical", "mane_select", "hgvsc", "hgvsp"]
]
```

```python
# Filter to MANE Select transcript only
df_mane = df_transcripts[df_transcripts["mane_select"] == True]
print(f"MANE Select only → {df_mane['variant_id'].nunique()} variants")
df_mane[["gene_symbol", "rsid", "transcript_id", "consequence_name", "impact_name", "hgvsp"]].head(10)
```

---

### 11. `gene_window_bp` — extend the locus

Expands the gene boundary on each side before querying variants.  
Useful to capture upstream regulatory or splice-region variants.

```python
df_no_win = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE"],
    most_severe_only=True,
)

df_with_win = bf.report.run(
    "gene_to_variant_filtering",
    gene_symbols=["APOE"],
    gene_window_bp=2000,
    most_severe_only=True,
)

print(f"No window  → {df_no_win['variant_id'].nunique()} variants")
print(f"± 2 kb     → {df_with_win['variant_id'].nunique()} variants")
```

---

### 12. Resolution failure handling

Failure cases return a single-row DataFrame with a non-null `resolution_status` — never raises an exception.

```python
cases = [
    ("empty input",              {"gene_symbols": []}),
    ("unknown gene symbol",      {"gene_symbols": ["NOTAREALGENE99"]}),
    ("valid gene, strict LoF",   {"gene_symbols": ["APOE"], "lof_confidence_filter": ["HC"], "impact_filter": ["HIGH"], "af_max": 0.0001}),
]

for label, params in cases:
    result = bf.report.run("gene_to_variant_filtering", **params)
    status = result["resolution_status"].iloc[0]
    rows   = len(result)
    print(f"{label:<35} → status={status!r}  rows={rows}")
```

---

### 14. CLI reference

```bash
# ── Basic: single gene
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols=APOE

# ── Multiple genes, HIGH/MODERATE impact
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="APOE,CLU,TOMM40,BIN1" \
  --param impact_filter="HIGH,MODERATE" \
  --param most_severe_only=true

# ── Rare LoF HC variants
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="BRCA1,BRCA2" \
  --param af_max=0.001 \
  --param lof_confidence_filter=HC \
  --param impact_filter=HIGH

# ── Missense + AlphaMissense pathogenic
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="APOE,CLU" \
  --param consequence_type_filter=missense_variant \
  --param alphamissense_classification=likely_pathogenic

# ── Save output
biofilter report run \
  --report-name gene_to_variant_filtering \
  --param gene_symbols="APOE,CLU" \
  --param impact_filter="HIGH,MODERATE" \
  --output phase2_variants.csv

# ── Inspect params template
biofilter report run \
  --report-name gene_to_variant_filtering \
  --params-template
```

---

### 14. Pipeline context\n\n`
``\nPhase 1 — Gene Discovery  (variant_single_gene_annotation)\n  input : one variant (chr:pos or rsID)\n  output: seed gene + partner-gene list with shared-group annotation\n  scale : ~8 k rows (tractable)\n          ↓ partner gene symbol list\n\nPhase 2 — Filtered Variant Collection  (this report)\n  input : list of gene symbols\n  output: 1 row per (gene × variant), all filters in SQL\n  scale : ~15 k–100 k rows, controlled by filters\n  export: lista_A.csv\n          ↓ lista_A.csv\n\nPhase 2.5 — Genotype Intersection  (variant_list_intersect)\n  input : lista_A.csv + VCF/PLINK variant list (Lista B)\n  output: variants present in BOTH — Lista C\n  export: lista_C.txt  (PLINK --extract ready)\n          ↓ [external] PLINK LD Pruning on lista_C.txt → Lista D\n\nPhase 3 — Pair Generation  (planned — snp_snp_pair_generator)\n  input : Lista D (LD-pruned, genotyped, annotated)\n  output: variant × variant interaction pairs (Lista D × Lista D)\n  scale : controlled by Phase 2 filtering\n```\n\n**Full pipeline tutorial:** `notebooks/Templates/pipeline__snp_snp_interaction.ipynb`\n\n**Why this separation matters:**  \nAPOE × 8 k partners × unfiltered variants = ~260 M rows before any filter.  \nWith `most_severe_only=True` + `impact=[HIGH, MODERATE]` + `af_max=0.05`:  \n~300 partners × ~50 variants = **~15 k rows** — Phase 3 becomes tractable.



<!-- ===== SOURCE FILE: notebooks/Templates/reports__platform_data_statistics.ipynb.md ===== -->

<h1> Biofilter - Report: Platform Data Statistics </h1>

Dashboard-ready platform statistics:
- entities by domain
- variants by chromosome
- relationships by group pair
- datasource latest load status/recency

### 1. Start Biofilter

```python
from biofilter import Biofilter
import pandas as pd

prod_uri = "parquet:///project/hall_shared/datasets/biofilter/<YYYYMMDD>/tables"

# bf = Biofilter(debug_mode=False)
bf = Biofilter(db_uri=prod_uri, debug_mode=False)
```

### 2. Inspect report metadata

```python
report_name = 'platform_data_statistics'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Run default dashboard dataset

```python
df = bf.report.run(
    'platform_data_statistics',
    only_active_entities=True,
    relationship_mode='undirected',
    include_totals=True,
)

print('rows:', len(df))
```

```python
print('Sections found:')
print(sorted(df['section'].dropna().unique().tolist()))
df.groupby(['section', 'metric'], dropna=False).size().reset_index(name='rows').sort_values(['section', 'metric']).head(100)
```

### 4. Treemap - Entities by Omic Domain and Data Source

```python
import matplotlib.pyplot as plt
from matplotlib import patches
from matplotlib import colors as mcolors
from sqlalchemy import func

from biofilter.modules.db.models import ETLDataSource, Entity, EntityGroup


def _split_treemap(items, x, y, w, h):
    """Simple binary treemap split (no external deps)."""
    if not items:
        return []
    if len(items) == 1:
        label, size = items[0]
        return [(label, size, x, y, w, h)]

    total = sum(size for _, size in items)
    if total <= 0:
        return []

    half = total / 2.0
    acc = 0.0
    split_idx = 1
    for i, (_, size) in enumerate(items, start=1):
        acc += size
        split_idx = i
        if acc >= half:
            break

    left = items[:split_idx]
    right = items[split_idx:]

    if not right:
        left, right = items[:-1], items[-1:]

    left_total = sum(size for _, size in left)
    ratio = left_total / total if total else 0.5

    if w >= h:
        w_left = w * ratio
        return _split_treemap(left, x, y, w_left, h) + _split_treemap(right, x + w_left, y, w - w_left, h)

    h_top = h * ratio
    return _split_treemap(left, x, y, w, h_top) + _split_treemap(right, x, y + h_top, w, h - h_top)


with bf.db.get_session() as session:
    q = (
        session.query(
            EntityGroup.name.label('omic_domain'),
            ETLDataSource.name.label('data_source'),
            func.count(Entity.id).label('entity_count'),
        )
        .join(Entity, Entity.group_id == EntityGroup.id)
        .outerjoin(ETLDataSource, ETLDataSource.id == Entity.data_source_id)
        .filter(Entity.is_active.isnot(False))
        .group_by(EntityGroup.name, ETLDataSource.name)
        .order_by(EntityGroup.name, ETLDataSource.name)
    )
    rows = q.all()

df_entities_ds = pd.DataFrame(rows, columns=['omic_domain', 'data_source', 'entity_count'])

if df_entities_ds.empty:
    print('No entity count rows to plot.')
else:
    df_entities_ds['data_source'] = df_entities_ds['data_source'].fillna('unknown')
    df_entities_ds['label'] = df_entities_ds['omic_domain'] + ' | ' + df_entities_ds['data_source']
    df_entities_ds = df_entities_ds.sort_values('entity_count', ascending=False)

    items = list(zip(df_entities_ds['label'], df_entities_ds['entity_count']))
    rects = _split_treemap(items, 0.0, 0.0, 1.0, 1.0)

    domains = sorted(df_entities_ds['omic_domain'].dropna().unique().tolist())
    cmap = plt.get_cmap('tab20')
    domain_color = {d: cmap(i % 20) for i, d in enumerate(domains)}

    fig, ax = plt.subplots(figsize=(14, 8))

    for label, size, x, y, w, h in rects:
        domain = label.split(' | ', 1)[0]
        color = domain_color.get(domain, '#bdbdbd')
        ax.add_patch(
            patches.Rectangle(
                (x, y), w, h,
                facecolor=color,
                edgecolor='white',
                linewidth=1.0,
            )
        )

        area = w * h
        if area >= 0.015:
            text = f"{label}\n{int(size):,}"
            r, g, b, _ = mcolors.to_rgba(color)
            luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
            text_color = 'black' if luminance > 0.6 else 'white'
            font_size = 11 if area >= 0.06 else 10
            ax.text(
                x + w / 2,
                y + h / 2,
                text,
                ha='center',
                va='center',
                fontsize=font_size,
                color=text_color,
            )

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    ax.set_title('Entity Count Treemap by Omic Domain and Data Source', fontsize=14)
    plt.tight_layout()
    plt.show()

df_entities_ds.sort_values('entity_count', ascending=False).head(50)
```

### 4.1 Treemap - Entities by Omic Domain (Aggregated Data Sources)

```python
# 4.1 Aggregate all data sources into Omic Domain totals
if 'df_entities_ds' in locals() and not df_entities_ds.empty:
    df_domain = (
        df_entities_ds
        .groupby('omic_domain', as_index=False)['entity_count']
        .sum()
    )
else:
    df_domain = df[(df['section'] == 'entity_counts_by_group') & (df['metric'] == 'entity_count')].copy()
    df_domain = df_domain.rename(columns={'dimension_1': 'omic_domain', 'value_number': 'entity_count'})
    df_domain = df_domain[['omic_domain', 'entity_count']]

df_domain = df_domain.sort_values('entity_count', ascending=False)

if df_domain.empty:
    print('No aggregated entity count rows to plot.')
else:
    items = list(zip(df_domain['omic_domain'], df_domain['entity_count']))
    rects = _split_treemap(items, 0.0, 0.0, 1.0, 1.0)

    domains = sorted(df_domain['omic_domain'].dropna().unique().tolist())
    cmap = plt.get_cmap('tab20')
    domain_color = {d: cmap(i % 20) for i, d in enumerate(domains)}

    fig, ax = plt.subplots(figsize=(12, 6))

    for domain, size, x, y, w, h in rects:
        color = domain_color.get(domain, '#bdbdbd')
        ax.add_patch(
            patches.Rectangle(
                (x, y), w, h,
                facecolor=color,
                edgecolor='white',
                linewidth=1.2,
            )
        )

        area = w * h
        if area >= 0.02:
            text = f"{domain}\n{int(size):,}"
            r, g, b, _ = mcolors.to_rgba(color)
            luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
            text_color = 'black' if luminance > 0.6 else 'white'
            font_size = 13 if area >= 0.12 else 11
            ax.text(
                x + w / 2,
                y + h / 2,
                text,
                ha='center',
                va='center',
                fontsize=font_size,
                color=text_color,
            )

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    ax.set_title('Entity Count Treemap by Omic Domain (All Data Sources Aggregated)', fontsize=14)
    plt.tight_layout()
    plt.show()

df_domain.head(50)
```

### 5. Plot - Variants by Chromosome

```python
from matplotlib.ticker import FuncFormatter

df_var = df[(df['section'] == 'variant_counts_by_chromosome') & (df['metric'] == 'variant_count')].copy()


def _sort_chr_key(x):
    s = str(x).strip().lower().replace('chr', '')
    if s == 'x':
        return 23
    if s == 'y':
        return 24
    if s in {'mt', 'm'}:
        return 25
    try:
        return int(s)
    except Exception:
        return 10_000


def _chr_display(x):
    s = str(x).strip().lower().replace('chr', '')
    if s == 'x':
        return 'chrX'
    if s == 'y':
        return 'chrY'
    if s in {'mt', 'm'}:
        return 'chrMT'
    return f"chr{s}"


if df_var.empty:
    print('No variant rows to plot (or variant_masters not available).')
else:
    df_var['chromosome_raw'] = df_var['dimension_1'].astype(str)
    df_var['chromosome_sort'] = df_var['chromosome_raw'].map(_sort_chr_key)
    df_var['chromosome_label'] = df_var['chromosome_raw'].map(_chr_display)
    df_var = df_var.sort_values(['chromosome_sort', 'chromosome_label'], na_position='last')

    fig, ax = plt.subplots(figsize=(13, 4.8))
    bars = ax.bar(df_var['chromosome_label'], df_var['value_number'], color='#3a86ff', edgecolor='white', linewidth=0.8)

    ax.set_title('Variant Count by Chromosome', fontsize=14, fontweight='bold')
    ax.set_xlabel('Chromosome')
    ax.set_ylabel('Variants')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{int(y):,}"))
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', alpha=0.25)

    ymax = float(df_var['value_number'].max() or 0)
    for b in bars:
        h = b.get_height()
        if ymax > 0 and h >= ymax * 0.08:
            ax.text(
                b.get_x() + b.get_width() / 2,
                h,
                f"{int(h):,}",
                ha='center',
                va='bottom',
                fontsize=8,
                color='#1f2d3d',
            )

    plt.tight_layout()
    plt.show()

df_var[['chromosome_label', 'value_number']].head(50)
```

### 6. Plot - Relationship Group Pairs (Top 20)

```python
from matplotlib.ticker import FuncFormatter

df_rel = df[(df['section'] == 'relationship_counts_by_group_pair') & (df['metric'] == 'relationship_count')].copy()

if df_rel.empty:
    print('No relationship rows to plot.')
else:
    df_rel['pair'] = df_rel['dimension_1'].astype(str) + ' × ' + df_rel['dimension_2'].astype(str)
    df_rel = df_rel.sort_values('value_number', ascending=False).head(20)

    fig, ax = plt.subplots(figsize=(11.5, 7))
    bars = ax.barh(
        df_rel['pair'][::-1],
        df_rel['value_number'][::-1],
        color='#577590',
        edgecolor='white',
        linewidth=0.7,
    )

    ax.set_title('Top 20 Relationship Group Pairs', fontsize=14, fontweight='bold')
    ax.set_xlabel('Relationships')
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis='x', alpha=0.25)

    xmax = float(df_rel['value_number'].max() or 0)
    for b in bars:
        w = b.get_width()
        ax.text(
            w + (xmax * 0.01 if xmax else 0.5),
            b.get_y() + b.get_height() / 2,
            f"{int(w):,}",
            va='center',
            fontsize=8,
            color='#1d3557',
        )

    plt.tight_layout()
    plt.show()

df_rel[['pair', 'value_number']].head(20)
```

### 6.1 Plot - Relationship Group Pairs Heatmap

```python
import numpy as np

df_rel_all = df[(df['section'] == 'relationship_counts_by_group_pair') & (df['metric'] == 'relationship_count')].copy()
df_rel_all['value_number'] = pd.to_numeric(df_rel_all['value_number'], errors='coerce').fillna(0)

if df_rel_all.empty:
    print('No relationship rows to plot heatmap.')
else:
    groups = sorted(set(df_rel_all['dimension_1'].astype(str)) | set(df_rel_all['dimension_2'].astype(str)))
    mat = pd.DataFrame(0.0, index=groups, columns=groups)

    for _, row in df_rel_all.iterrows():
        g1 = str(row['dimension_1'])
        g2 = str(row['dimension_2'])
        mat.loc[g1, g2] += float(row['value_number'])

    # Symmetric view is usually easier to read for group-pair relationship density.
    mat_view = mat.combine(mat.T, np.maximum)

    fig, ax = plt.subplots(figsize=(9, 7))
    im = ax.imshow(mat_view.values, cmap='YlOrRd')

    ax.set_xticks(range(len(groups)))
    ax.set_yticks(range(len(groups)))
    ax.set_xticklabels(groups, rotation=45, ha='right', fontsize=10)
    ax.set_yticklabels(groups, fontsize=10)
    ax.set_title('Relationship Density Heatmap by Group Pair', fontsize=14)

    if len(groups) <= 12:
        vmax = float(mat_view.values.max() or 1.0)
        for i in range(len(groups)):
            for j in range(len(groups)):
                value = int(mat_view.iat[i, j])
                if value > 0:
                    txt_color = 'black' if (value / vmax) < 0.6 else 'white'
                    ax.text(j, i, f"{value:,}", ha='center', va='center', fontsize=9, color=txt_color)

    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label('Relationships', rotation=90)

    plt.tight_layout()
    plt.show()

mat_view if 'mat_view' in locals() else df_rel_all.head(20)
```

### 6.2 Plot - Relationship Group Network (Top Weighted Pairs)

```python
import numpy as np

if 'df_rel_all' not in locals() or df_rel_all.empty:
    df_rel_net = df[(df['section'] == 'relationship_counts_by_group_pair') & (df['metric'] == 'relationship_count')].copy()
    df_rel_net['value_number'] = pd.to_numeric(df_rel_net['value_number'], errors='coerce').fillna(0)
else:
    df_rel_net = df_rel_all.copy()

if df_rel_net.empty:
    print('No relationship rows to plot network.')
else:
    top_edges = (
        df_rel_net
        .sort_values('value_number', ascending=False)
        .head(30)
        .copy()
    )

    top_edges['source'] = top_edges['dimension_1'].astype(str)
    top_edges['target'] = top_edges['dimension_2'].astype(str)

    nodes = sorted(set(top_edges['source']) | set(top_edges['target']))

    if len(nodes) < 2:
        print('Insufficient groups to build a network plot.')
    else:
        angles = np.linspace(0, 2 * np.pi, len(nodes), endpoint=False)
        pos = {node: (np.cos(a), np.sin(a)) for node, a in zip(nodes, angles)}

        node_weight = {node: 0.0 for node in nodes}
        for _, row in top_edges.iterrows():
            v = float(row['value_number'])
            node_weight[row['source']] += v
            node_weight[row['target']] += v

        max_edge = float(top_edges['value_number'].max() or 1.0)
        max_node = float(max(node_weight.values()) or 1.0)

        fig, ax = plt.subplots(figsize=(9, 9))

        for _, row in top_edges.iterrows():
            s, t, v = row['source'], row['target'], float(row['value_number'])
            x1, y1 = pos[s]
            x2, y2 = pos[t]
            strength = v / max_edge if max_edge else 0.0
            lw = 0.8 + 5.0 * strength
            alpha = 0.2 + 0.6 * strength
            ax.plot([x1, x2], [y1, y2], color='dimgray', linewidth=lw, alpha=alpha, zorder=1)

        xs = [pos[n][0] for n in nodes]
        ys = [pos[n][1] for n in nodes]
        sizes = [450 + 2400 * (node_weight[n] / max_node if max_node else 0.0) for n in nodes]

        ax.scatter(xs, ys, s=sizes, c='#1f77b4', edgecolors='white', linewidths=1.4, zorder=3)

        for node in nodes:
            x, y = pos[node]
            ax.text(x * 1.13, y * 1.13, node, ha='center', va='center', fontsize=11, fontweight='bold')

        ax.set_title('Relationship Group Network (Top 30 Weighted Pairs)', fontsize=14)
        ax.set_aspect('equal')
        ax.axis('off')
        plt.tight_layout()
        plt.show()

    top_edges[['source', 'target', 'value_number']].head(30)
```

### 6.3 Plot - Directed Relationship Sankey (Top Domains)

```python
from matplotlib.sankey import Sankey

# Build a directed-only view for flow visualization.
df_rel_dir = bf.report.run(
    'platform_data_statistics',
    sections=['relationship_counts_by_group_pair'],
    relationship_mode='directed',
    include_totals=False,
    only_active_entities=True,
)

df_rel_dir = df_rel_dir[
    (df_rel_dir['section'] == 'relationship_counts_by_group_pair')
    & (df_rel_dir['metric'] == 'relationship_count')
].copy()

df_rel_dir['source'] = df_rel_dir['dimension_1'].astype(str)
df_rel_dir['target'] = df_rel_dir['dimension_2'].astype(str)
df_rel_dir['value_number'] = pd.to_numeric(df_rel_dir['value_number'], errors='coerce').fillna(0)

if df_rel_dir.empty:
    print('No directed relationship rows to plot Sankey.')
else:
    out_sum = df_rel_dir.groupby('source')['value_number'].sum()
    in_sum = df_rel_dir.groupby('target')['value_number'].sum()
    traffic = out_sum.add(in_sum, fill_value=0).sort_values(ascending=False)

    top_n_domains = 6
    top_domains = traffic.head(top_n_domains).index.tolist()

    df_sankey = df_rel_dir[
        df_rel_dir['source'].isin(top_domains)
        & df_rel_dir['target'].isin(top_domains)
    ].copy()

    if df_sankey.empty:
        print('No directed rows among top domains to plot Sankey.')
    else:
        out_top = df_sankey.groupby('source')['value_number'].sum().sort_values(ascending=False)
        in_top = df_sankey.groupby('target')['value_number'].sum().sort_values(ascending=False)

        out_top = out_top[out_top > 0]
        in_top = in_top[in_top > 0]

        flows = [-float(v) for v in out_top.values] + [float(v) for v in in_top.values]
        labels = [f"out: {k}" for k in out_top.index] + [f"in: {k}" for k in in_top.index]
        orientations = [-1] * len(out_top) + [1] * len(in_top)

        if len(flows) < 2:
            print('Insufficient flow groups to plot Sankey.')
        else:
            balance = sum(flows)
            if abs(balance) > 1e-9:
                flows[-1] -= balance

            scale = 1.0 / max(sum(abs(v) for v in flows), 1.0)

            fig, ax = plt.subplots(figsize=(12, 7))
            sankey = Sankey(ax=ax, scale=scale, unit=None, format='%.0f')
            sankey.add(
                flows=flows,
                labels=labels,
                orientations=orientations,
                trunklength=1.0,
                pathlengths=[0.5] * len(flows),
                facecolor='#2a9d8f',
                alpha=0.75,
            )
            sankey.finish()

            ax.set_title('Directed Relationship Flow Sankey (Top 6 Domains)', fontsize=14)
            plt.tight_layout()
            plt.show()

    df_sankey[['source', 'target', 'value_number']].sort_values('value_number', ascending=False).head(30)
```

### 6.4 Plot - Entity-Level Network (Node Color = Entity Group)

```python
import numpy as np
from matplotlib import patches as mpatches
from sqlalchemy import case, func, select, union_all
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import Entity, EntityAlias, EntityGroup, EntityRelationship

# Tune for readability/performance
max_nodes = 50
max_edges = 3500
label_top_n = 50
exclude_relationship_type_id = 1  # remove hierarchy links (e.g., pathway-pathway)

# 1) Pick the most connected entities (degree = in + out relationships)
deg_src = (
    select(
        EntityRelationship.entity_1_id.label('entity_id'),
        func.count(EntityRelationship.id).label('deg'),
    )
    .where(EntityRelationship.relationship_type_id != exclude_relationship_type_id)
    .group_by(EntityRelationship.entity_1_id)
)
deg_tgt = (
    select(
        EntityRelationship.entity_2_id.label('entity_id'),
        func.count(EntityRelationship.id).label('deg'),
    )
    .where(EntityRelationship.relationship_type_id != exclude_relationship_type_id)
    .group_by(EntityRelationship.entity_2_id)
)
deg_union = union_all(deg_src, deg_tgt).subquery()
degree_sum = func.sum(deg_union.c.deg).label('degree')

A = aliased(EntityAlias)

with bf.db.get_session() as session:
    top_entities = (
        session.query(
            deg_union.c.entity_id.label('entity_id'),
            degree_sum,
        )
        .group_by(deg_union.c.entity_id)
        .order_by(degree_sum.desc())
        .limit(max_nodes)
        .all()
    )

    top_ids = [int(r.entity_id) for r in top_entities]

    if not top_ids:
        df_entity_edges = pd.DataFrame(columns=['node_a', 'node_b', 'weight'])
        entity_meta_rows = []
    else:
        entity_meta_rows = (
            session.query(
                Entity.id.label('entity_id'),
                EntityGroup.name.label('group_name'),
                A.alias_value.label('entity_name'),
            )
            .outerjoin(EntityGroup, EntityGroup.id == Entity.group_id)
            .outerjoin(A, (A.entity_id == Entity.id) & (A.is_primary.is_(True)))
            .filter(Entity.id.in_(top_ids))
            .all()
        )

        node_a = case(
            (EntityRelationship.entity_1_id <= EntityRelationship.entity_2_id, EntityRelationship.entity_1_id),
            else_=EntityRelationship.entity_2_id,
        ).label('node_a')
        node_b = case(
            (EntityRelationship.entity_1_id <= EntityRelationship.entity_2_id, EntityRelationship.entity_2_id),
            else_=EntityRelationship.entity_1_id,
        ).label('node_b')
        edge_weight = func.count(EntityRelationship.id).label('weight')

        edge_rows = (
            session.query(node_a, node_b, edge_weight)
            .filter(
                EntityRelationship.entity_1_id.in_(top_ids),
                EntityRelationship.entity_2_id.in_(top_ids),
                EntityRelationship.entity_1_id != EntityRelationship.entity_2_id,
                EntityRelationship.relationship_type_id != exclude_relationship_type_id,
            )
            .group_by(node_a, node_b)
            .order_by(edge_weight.desc())
            .limit(max_edges)
            .all()
        )

        df_entity_edges = pd.DataFrame(edge_rows, columns=['node_a', 'node_b', 'weight'])

if df_entity_edges.empty:
    print('No entity-level relationship edges found for the selected limits.')
else:
    # Build name/group maps
    name_map = {}
    group_map = {}

    for row in entity_meta_rows:
        eid = int(row.entity_id)
        if eid not in name_map and row.entity_name:
            name_map[eid] = str(row.entity_name)
        if eid not in group_map and row.group_name:
            group_map[eid] = str(row.group_name)

    used_nodes = sorted(set(df_entity_edges['node_a'].astype(int)) | set(df_entity_edges['node_b'].astype(int)))

    for eid in used_nodes:
        name_map.setdefault(eid, f'Entity {eid}')
        group_map.setdefault(eid, 'unknown')

    # Circle layout sorted by group to make color clusters easier to read.
    ordered_nodes = sorted(used_nodes, key=lambda n: (group_map[n], name_map[n].lower()))
    angles = np.linspace(0, 2 * np.pi, len(ordered_nodes), endpoint=False)
    pos = {n: (np.cos(a), np.sin(a)) for n, a in zip(ordered_nodes, angles)}

    node_weight = {n: 0.0 for n in ordered_nodes}
    for _, row in df_entity_edges.iterrows():
        n1 = int(row['node_a'])
        n2 = int(row['node_b'])
        w = float(row['weight'])
        node_weight[n1] += w
        node_weight[n2] += w

    groups = sorted({group_map[n] for n in ordered_nodes})
    cmap = plt.get_cmap('tab20')
    group_color = {g: cmap(i % 20) for i, g in enumerate(groups)}

    max_edge = float(df_entity_edges['weight'].max() or 1.0)
    max_node = float(max(node_weight.values()) or 1.0)

    fig, ax = plt.subplots(figsize=(13, 13))

    for _, row in df_entity_edges.iterrows():
        n1 = int(row['node_a'])
        n2 = int(row['node_b'])
        w = float(row['weight'])
        x1, y1 = pos[n1]
        x2, y2 = pos[n2]
        strength = w / max_edge if max_edge else 0.0
        lw = 0.4 + 3.0 * strength
        alpha = 0.10 + 0.35 * strength
        ax.plot([x1, x2], [y1, y2], color='gray', linewidth=lw, alpha=alpha, zorder=1)

    xs = [pos[n][0] for n in ordered_nodes]
    ys = [pos[n][1] for n in ordered_nodes]
    sizes = [80 + 900 * (node_weight[n] / max_node if max_node else 0.0) for n in ordered_nodes]
    colors = [group_color[group_map[n]] for n in ordered_nodes]

    ax.scatter(xs, ys, s=sizes, c=colors, edgecolors='white', linewidths=0.8, zorder=3)

    # Label only the top hubs to avoid clutter.
    hubs = sorted(ordered_nodes, key=lambda n: node_weight[n], reverse=True)[:label_top_n]
    for n in hubs:
        x, y = pos[n]
        ax.text(x * 1.08, y * 1.08, name_map[n], fontsize=8, ha='center', va='center')

    group_counts = pd.Series([group_map[n] for n in ordered_nodes]).value_counts()
    legend_groups = group_counts.head(12).index.tolist()
    handles = [
        mpatches.Patch(color=group_color[g], label=f"{g} ({int(group_counts[g])})")
        for g in legend_groups
    ]
    if len(group_counts) > 12:
        handles.append(mpatches.Patch(color='#cccccc', label=f"+{len(group_counts) - 12} groups"))

    ax.legend(handles=handles, title='Entity Group', loc='upper right', bbox_to_anchor=(1.35, 1.02), frameon=False)

    ax.set_title('Entity-Level Relationship Network (Node Color = Entity Group)', fontsize=15)
    ax.text(
        0.0,
        -1.22,
        (
            f"Nodes: {len(ordered_nodes)} | Edges: {len(df_entity_edges)} | "
            f"Labels: top {label_top_n} hubs | Excluding relationship_type_id={exclude_relationship_type_id}"
        ),
        ha='center',
        va='center',
        fontsize=10,
    )
    ax.set_aspect('equal')
    ax.axis('off')
    plt.tight_layout()
    plt.show()

    df_entity_edges.sort_values('weight', ascending=False).head(30)
```

### 6.5 Plot - Relationship Group Pairs by Data Source (All Groups)

```python
from matplotlib.ticker import FuncFormatter
from sqlalchemy import func
from sqlalchemy.orm import aliased
from IPython.display import display

from biofilter.modules.db.models import ETLDataSource, EntityGroup, EntityRelationship

# Set to an integer (e.g., 40) if you want to limit bars per datasource.
max_pairs_per_data_source = None

G1 = aliased(EntityGroup)
G2 = aliased(EntityGroup)

db = bf.core.require_db()
with db.get_session() as session:
    rows = (
        session.query(
            ETLDataSource.name.label('data_source'),
            G1.name.label('group_1'),
            G2.name.label('group_2'),
            func.count(EntityRelationship.id).label('relationship_count'),
        )
        .select_from(EntityRelationship)
        .outerjoin(ETLDataSource, ETLDataSource.id == EntityRelationship.data_source_id)
        .outerjoin(G1, G1.id == EntityRelationship.entity_1_group_id)
        .outerjoin(G2, G2.id == EntityRelationship.entity_2_group_id)
        .group_by(ETLDataSource.name, G1.name, G2.name)
        .all()
    )

df_rel_ds = pd.DataFrame(rows, columns=['data_source', 'group_1', 'group_2', 'relationship_count'])

if df_rel_ds.empty:
    print('No relationship rows found by data source.')
else:
    df_rel_ds['data_source'] = df_rel_ds['data_source'].fillna('unknown_data_source')
    df_rel_ds['group_1'] = df_rel_ds['group_1'].fillna('unknown_group')
    df_rel_ds['group_2'] = df_rel_ds['group_2'].fillna('unknown_group')
    df_rel_ds['relationship_count'] = pd.to_numeric(df_rel_ds['relationship_count'], errors='coerce').fillna(0).astype(int)

    # Undirected pair view to match section 6 semantics.
    df_rel_ds['pair'] = df_rel_ds.apply(
        lambda r: ' × '.join(sorted([str(r['group_1']), str(r['group_2'])])),
        axis=1,
    )

    df_rel_ds = (
        df_rel_ds
        .groupby(['data_source', 'pair'], as_index=False)['relationship_count']
        .sum()
        .sort_values(['data_source', 'relationship_count'], ascending=[True, False])
    )

    data_sources = sorted(df_rel_ds['data_source'].unique().tolist())
    n = len(data_sources)

    fig_height = max(4.0 * n, 7.0)
    fig, axes = plt.subplots(n, 1, figsize=(13, fig_height), squeeze=False)

    for i, ds in enumerate(data_sources):
        ax = axes[i, 0]
        sub = df_rel_ds[df_rel_ds['data_source'] == ds].copy()

        if max_pairs_per_data_source is not None:
            sub = sub.head(int(max_pairs_per_data_source))

        sub = sub.sort_values('relationship_count', ascending=True)

        bars = ax.barh(
            sub['pair'],
            sub['relationship_count'],
            color='#6c757d',
            edgecolor='white',
            linewidth=0.6,
        )

        ax.set_title(f"{ds} | Group pairs: {len(sub)}", fontsize=12, fontweight='bold')
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
        ax.grid(axis='x', alpha=0.25)

        xmax = float(sub['relationship_count'].max() or 0)
        for b in bars:
            w = b.get_width()
            ax.text(
                w + (xmax * 0.01 if xmax else 0.5),
                b.get_y() + b.get_height() / 2,
                f"{int(w):,}",
                va='center',
                fontsize=7,
                color='#2f3e46',
            )

        if i == n - 1:
            ax.set_xlabel('Relationships')

    fig.suptitle('Relationship Group Pairs by Data Source (All Pairs)', fontsize=15, fontweight='bold', y=1.0)
    plt.tight_layout()
    plt.show()

    # Matrix table: relationships in rows, data sources in columns,
    # with row/column totals and thousand-separated integers.
    rel_matrix = (
        df_rel_ds
        .pivot_table(
            index='pair',
            columns='data_source',
            values='relationship_count',
            aggfunc='sum',
            fill_value=0,
        )
        .astype('int64')
    )

    rel_matrix['Total'] = rel_matrix.sum(axis=1).astype('int64')
    rel_matrix = rel_matrix.sort_values('Total', ascending=False)

    totals_row = rel_matrix.sum(axis=0).astype('int64')
    rel_matrix.loc['Total'] = totals_row

    print('Relationship matrix by data source (with totals):')
    display(
        rel_matrix.style
        .format('{:,.0f}')
        .set_caption('Rows: relationship pairs | Columns: data sources | Includes row/column totals')
    )
```

### 7. Plot - Datasource Freshness (Latest Load Age in Days)

```python
from matplotlib import patches as mpatches
from matplotlib.ticker import FuncFormatter

df_age = df[(df['section'] == 'datasource_latest_load') & (df['metric'] == 'latest_load_age_days')].copy()
df_status = df[(df['section'] == 'datasource_latest_load') & (df['metric'] == 'latest_load_status')][['dimension_1', 'dimension_2', 'value_text']].copy()
df_status = df_status.rename(columns={'value_text': 'load_status'})

if df_age.empty:
    print('No datasource load-age rows to plot.')
else:
    df_age['source_data_source'] = df_age['dimension_1'].astype(str) + ' / ' + df_age['dimension_2'].astype(str)
    df_age = df_age.sort_values('value_number', ascending=False).head(25)

    def _stale_color(days):
        d = float(days or 0)
        if d >= 180:
            return '#d62828'  # critical stale
        if d >= 60:
            return '#f77f00'  # warning stale
        return '#2a9d8f'      # healthy

    colors = df_age['value_number'].map(_stale_color)

    fig, ax = plt.subplots(figsize=(13, 7))
    bars = ax.barh(
        df_age['source_data_source'][::-1],
        df_age['value_number'][::-1],
        color=colors[::-1],
        edgecolor='white',
        linewidth=0.7,
    )

    ax.set_title('Latest Load Age (Days) - Top 25 Stale DataSources', fontsize=14, fontweight='bold')
    ax.set_xlabel('Days since latest load')
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis='x', alpha=0.25)

    xmax = float(df_age['value_number'].max() or 0)
    for b in bars:
        w = b.get_width()
        ax.text(
            w + (xmax * 0.01 if xmax else 0.5),
            b.get_y() + b.get_height() / 2,
            f"{int(w)}d",
            va='center',
            fontsize=8,
            color='#2f3e46',
        )

    legend_handles = [
        mpatches.Patch(color='#2a9d8f', label='< 60 days'),
        mpatches.Patch(color='#f77f00', label='60-179 days'),
        mpatches.Patch(color='#d62828', label='>= 180 days'),
    ]
    ax.legend(handles=legend_handles, title='Freshness', loc='lower right', frameon=False)

    plt.tight_layout()
    plt.show()

    view = df_age[['dimension_1', 'dimension_2', 'value_number']].rename(columns={'dimension_1': 'source_system', 'dimension_2': 'data_source', 'value_number': 'latest_load_age_days'})
    view = view.merge(df_status, left_on=['source_system', 'data_source'], right_on=['dimension_1', 'dimension_2'], how='left').drop(columns=['dimension_1', 'dimension_2'])
    view.head(50)
```

```python
df.to_csv('platform_data_statistics.csv', index=False)
print('Saved: platform_data_statistics.csv')
```

### 8. Schema Check (quick QA)

```python
required_cols = [
    'section',
    'metric',
    'dimension_1',
    'dimension_2',
    'value_number',
    'value_text',
    'as_of',
    'note',
]

print('Dtypes:')
display(df.dtypes.to_frame('dtype'))

missing_cols = [c for c in required_cols if c not in df.columns]
print('\nMissing required columns:', missing_cols if missing_cols else 'none')

if 'value_number' in df.columns:
    print('value_number dtype:', df['value_number'].dtype)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__qry_template.ipynb.md ===== -->

<h1> 📘 Biofilter — Report: Query Template </h1>

Developer notebook for `qry_template` (scaffold report).

### 1. Start Biofilter

```python
from biofilter import Biofilter
bf = Biofilter(debug_mode=False)
```

### 2. Run template report

```python
df = bf.report.run("qry_template")
print(type(df))
print(f"Rows: {len(df)}")
df.head() if hasattr(df, "head") else df
```

### 3. Next steps for new report development

```python
print("Copy report_template.py -> report_<new_name>.py")
print("Set name/description and implement run()/explain()/available_columns()/example_input()")
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__snp_snp_model.ipynb.md ===== -->

<h1> Biofilter - Report: SNP SNP Model </h1>

Build BF4 candidate models in layers: seed positions -> variants -> genes (entity_locations) -> biological groups -> gene_pair and snp_pair.

### 1. Start Biofilter

```python
from biofilter import Biofilter
bf = Biofilter(debug_mode=False)
```

### 2. Inspect report metadata

```python
report_name = 'snp_snp_model'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Default run from seed positions
Uses variant -> gene mapping via `entity_locations`, then group expansion (Pathway) and returns `gene_pair` + `snp_pair`.

```python
df_default = bf.report.run(
    'snp_snp_model',
    input_data=['chr19:44904604', 'chr22:11474744'],
    build=38,
    window_bp=100,
    group_entity_groups=['Pathways'],
    # relationship_types=['in_pathway'],
    # gene_pair_scope='at_least_one_from_seed',
    # snp_pair_scope='at_least_one_from_seed',
)

print('rows:', len(df_default))
df_default.head(30)
```

### 4. Scope control: only seed-seed pairs

```python
df_seed_only = bf.report.run(
    'snp_snp_model',
    input_data=['chr17:150', 'chr17:280'],
    group_entity_groups=['Pathway'],
    relationship_types=['in_pathway'],
    gene_pair_scope='both_from_seed',
    snp_pair_scope='both_from_seed',
)

df_seed_only[['row_type', 'gene_1_name', 'gene_2_name', 'variant_1_rsid', 'variant_2_rsid', 'gene_pair_seed_scope', 'snp_pair_seed_scope']].head(30)
```

### 5. Disable variant expansion for expanded genes
Keeps gene expansion, but variants are pulled only for seed genes.

```python
df_no_expand = bf.report.run(
    'snp_snp_model',
    input_data=['chr17:150'],
    group_entity_groups=['Pathway'],
    relationship_types=['in_pathway'],
    expand_variants_from_expanded_genes=False,
)

df_no_expand[['row_type', 'gene_1_name', 'gene_2_name', 'variant_1_rsid', 'variant_2_rsid', 'observation']].head(30)
```

### 6. Restrict to specific group entities (optional)

```python
df_group_filter = bf.report.run(
    'snp_snp_model',
    input_data=['chr17:150'],
    group_entity_groups=['Pathway'],
    group_entities=['DNA_REPAIR_PATHWAY'],
    relationship_types=['in_pathway'],
)

df_group_filter[['row_type', 'gene_1_name', 'gene_2_name', 'group_support_names', 'observation', 'note']].head(30)
```

### 7. Focused output view

```python
cols = [
    'row_type',
    'gene_1_name',
    'gene_2_name',
    'gene_pair_seed_scope',
    'variant_1_rsid',
    'variant_2_rsid',
    'snp_pair_seed_scope',
    'group_support_names',
    'observation',
    'note',
]

df_default[cols].head(50)
```

```python
df_default.to_csv('snp_snp_model.csv', index=False)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__variant_binning.ipynb.md ===== -->

<h1> Biofilter - Report: Variant Binning </h1>

BioBin-style rare-variant aggregation from a cohort VCF into biological bins.

This tutorial shows:
- what the report does
- how to run a smoke test
- how to inspect output artifacts
- how to run a real cohort flow

### 1. What this report does

`variant_binning` reads a multi-sample VCF, computes internal MAF, applies rare filtering, maps variants to genes by coordinate overlap (`entity_locations`), and writes binning artifacts to disk.

Current supported grouping layers:
- `gene`
- `gene_group`
- `locus_type`
- `pathway`

Main output files:
- `bin_counts.csv`
- `variant_to_bin.csv`
- `bin_definitions.csv`
- `bin_member_counts.csv`
- `sample_bin_long.csv`
- `summary.json`

### 2. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
bf
```

### 3. Inspect report metadata

```python
report_name = 'variant_binning'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 4. Build a reproducible smoke-test input

This cell creates a tiny cohort VCF + phenotype file in `tmp/variant_binning_tutorial/` using a real gene coordinate from the DB.

```python
from pathlib import Path
import pandas as pd
from biofilter.modules.db.models import EntityGroup, EntityLocation, GeneMaster

root = Path('tmp/variant_binning_tutorial')
root.mkdir(parents=True, exist_ok=True)
vcf_path = root / 'mini_cohort.vcf'
pheno_path = root / 'mini_phenotype.csv'
output_dir = root / 'out_gene'

with bf.core.require_db().get_session() as session:
    gene_group_ids = [r.id for r in session.query(EntityGroup.id).filter(EntityGroup.name.in_(['Gene','Genes'])).all()]
    if not gene_group_ids:
        raise RuntimeError('No Gene/Genes entity group found in DB')

    row = (
        session.query(
            EntityLocation.chromosome,
            EntityLocation.start_pos,
            EntityLocation.end_pos,
            GeneMaster.symbol,
        )
        .join(GeneMaster, GeneMaster.entity_id == EntityLocation.entity_id)
        .filter(
            EntityLocation.build == 38,
            EntityLocation.entity_group_id.in_(gene_group_ids),
            EntityLocation.start_pos.isnot(None),
            EntityLocation.end_pos.isnot(None),
        )
        .order_by(EntityLocation.id.asc())
        .first()
    )

if row is None:
    raise RuntimeError('No gene location row available for tutorial')

chrom = int(row.chromosome)
pos = int(row.start_pos)
gene_symbol = str(row.symbol or 'UNKNOWN')

vcf_text = f"""##fileformat=VCFv4.2
##contig=<ID=chr{chrom}>
##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\tS3\tS4
chr{chrom}\t{pos}\tvar1\tA\tC\t.\tPASS\t.\tGT\t0/1\t0/0\t0/0\t0/0
chr{chrom}\t{pos+1}\tvar2\tG\tT\t.\tPASS\t.\tGT\t1/1\t0/1\t0/1\t0/1
"""
vcf_path.write_text(vcf_text, encoding='utf-8')

pheno_df = pd.DataFrame([
    {'SampleID': 'S1', 'Phenotype': 1},
    {'SampleID': 'S2', 'Phenotype': 1},
    {'SampleID': 'S3', 'Phenotype': 0},
    {'SampleID': 'S4', 'Phenotype': 0},
])
pheno_df.to_csv(pheno_path, index=False)

print('gene used for placement:', gene_symbol)
print('vcf_path:', vcf_path)
print('pheno_path:', pheno_path)
print('output_dir:', output_dir)
```

### 5. Run the report (`group_by=gene`)

With this tiny cohort, only one of the two variants should pass rarity at `maf_cutoff=0.3` and map to a gene bin.

```python
summary_df = bf.report.run(
    'variant_binning',
    vcf_path=str(vcf_path),
    phenotype_path=str(pheno_path),
    phenotype_sample_column='SampleID',
    phenotype_value_column='Phenotype',
    phenotype_control_value='0',
    group_by='gene',
    maf_cutoff=0.3,
    rare_case_control=True,
    overall_major_allele=True,
    build=38,
    output_dir=str(output_dir),
)

summary_df
```

### 6. Inspect generated artifacts

```python
import json

artifacts = [
    output_dir / 'bin_counts.csv',
    output_dir / 'variant_to_bin.csv',
    output_dir / 'bin_definitions.csv',
    output_dir / 'bin_member_counts.csv',
    output_dir / 'sample_bin_long.csv',
    output_dir / 'summary.json',
]
for p in artifacts:
    print(p.name, '->', p.exists())

vtb = pd.read_csv(output_dir / 'variant_to_bin.csv')
bc = pd.read_csv(output_dir / 'bin_counts.csv')
sjson = json.loads((output_dir / 'summary.json').read_text(encoding='utf-8'))

print('\nsummary metrics:')
print({k: sjson[k] for k in [
    'variants_processed',
    'variants_rare',
    'variants_with_gene_overlap',
    'variants_binned',
    'bins_generated',
]})

print('\nvariant_to_bin preview:')
display(vtb.head(20))

print('\nbin_counts preview:')
display(bc.head(20))
```

### 7. Compare grouping layers (`gene`, `gene_group`, `locus_type`, `pathway`)

```python
comparisons = []
for mode in ['gene', 'gene_group', 'locus_type', 'pathway']:
    out_mode = root / f'out_{mode}'
    sdf = bf.report.run(
        'variant_binning',
        vcf_path=str(vcf_path),
        phenotype_path=str(pheno_path),
        phenotype_sample_column='SampleID',
        phenotype_value_column='Phenotype',
        phenotype_control_value='0',
        group_by=mode,
        maf_cutoff=0.3,
        rare_case_control=True,
        overall_major_allele=True,
        build=38,
        output_dir=str(out_mode),
    )
    rec = sdf.to_dict(orient='records')[0]
    comparisons.append({
        'group_by': mode,
        'variants_processed': rec['variants_processed'],
        'variants_rare': rec['variants_rare'],
        'variants_binned': rec['variants_binned'],
        'bins_generated': rec['bins_generated'],
        'output_dir': rec['output_dir'],
    })

pd.DataFrame(comparisons)
```

### 8. Real cohort template

Replace the paths below with your real cohort files and run the cell.

```python
real_vcf_path = '/absolute/path/to/cohort.vcf.gz'
real_phenotype_path = '/absolute/path/to/phenotype.csv'
real_output_dir = 'outputs/variant_binning_real'

# Uncomment to run with real data
# real_summary = bf.report.run(
#     'variant_binning',
#     vcf_path=real_vcf_path,
#     phenotype_path=real_phenotype_path,
#     phenotype_sample_column='SampleID',
#     phenotype_value_column='Phenotype',
#     phenotype_control_value='0',
#     group_by='gene',
#     maf_cutoff=0.01,
#     rare_case_control=True,
#     overall_major_allele=True,
#     build=38,
#     output_dir=real_output_dir,
# )
# real_summary
```

### 9. Optional: enrich bins with AlphaMissense labels

If `notebooks/Andre/missensse.csv` exists, this cell merges it with `variant_to_bin.csv` using coordinate+allele key.

```python
from pathlib import Path

am_candidates = [
    Path('notebooks/Andre/missensse.csv'),
    Path('../Andre/missensse.csv'),
    Path('missensse.csv'),
]
am_path = next((p for p in am_candidates if p.exists()), None)

vtb_path = output_dir / "variant_to_bin.csv"
vtb = pd.read_csv(vtb_path)

if am_path is None:
    print('AlphaMissense file not found. Skipping merge.')
else:
    am = pd.read_csv(am_path, usecols=[
        'chromosome', 'position_start', 'position_end',
        'reference_allele', 'alternate_allele',
        'score', 'classification', 'transcript_id'
    ])
    am['variant_key'] = (
        am['chromosome'].astype(str) + ':' +
        am['position_start'].astype(str) + ':' +
        am['position_end'].astype(str) + ':' +
        am['reference_allele'].astype(str) + '>' +
        am['alternate_allele'].astype(str)
    )

    merged = vtb.merge(
        am[["variant_key", "score", "classification", "transcript_id"]],
        on="variant_key",
        how="left",
    )

    print('variant_to_bin rows:', len(vtb))
    print('rows with AlphaMissense annotation:', int(merged['classification'].notna().sum()))
    display(merged.head(20))
```

### 10. Interpretation checklist

1. Check `summary.json` for sample/variant counts and filtering behavior.
2. Use `variant_to_bin.csv` to audit each mapping (variant -> bin).
3. Use `bin_counts.csv` as the main matrix for downstream burden/SKAT modeling.
4. Keep run parameters and output directory versioned for reproducibility.



<!-- ===== SOURCE FILE: notebooks/Templates/reports__variant_gene_location_model.ipynb.md ===== -->

<h1> Biofilter - Report: Variant Gene Location Model </h1>

Map variants and genes by genomic interval overlap using variant_masters and entity_locations (build 38).

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'variant_gene_location_model'

print('name:', report_name)
print('available columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Gene input mode

```python
df_gene = bf.report.run(
    'variant_gene_location_model',
    input_mode='gene',
    input_data=['ZNF73P'],
    build=38,
    window_bp=0,
)

print('rows:', len(df_gene))
df_gene.head(20)
```

```python
df_gene.to_csv('variant_gene_location_model.csv', index=False)
```

### 4. rsID input mode

```python
df_rsid = bf.report.run(
    'variant_gene_location_model',
    input_mode='rsid',
    input_data=['rs111', 'rs222'],
    build=38,
)

df_rsid.head(20)
```

### 5. Auto mode with mixed inputs
Supports gene aliases, rsID, chr:pos and chr:start-end in the same request.

```python
mixed_inputs = [
    'TP53',
    'rs111',
    'chr17:150',
    'chr17:260-320',
    'NOT_A_GENE',
    'chr17:XYZ',
]

df_auto = bf.report.run(
    'variant_gene_location_model',
    input_mode='auto',
    input_data=mixed_inputs,
    build=38,
    window_bp=0,
)

print('rows:', len(df_auto))
df_auto.head(30)
```

### 6. Focused output view

```python
cols = [
    'input_original',
    'input_mode',
    'input_primary_name',
    'variant_rsid',
    'variant_chromosome',
    'variant_position_start',
    'gene_primary_name',
    'gene_start',
    'gene_end',
    'overlap_bp',
    'distance_bp',
    'observation',
    'note',
]

df_auto[cols].head(50)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__variant_modeling.ipynb.md ===== -->

# Biofilter — Report: Variant Modeling

Map an input list of variants (rsID, chr:pos, or chr:pos:ref:alt) to biologically connected
**variant×variant pairs**, where both variants in every pair come from the input.

```
Input variants (rsID, chr:pos, or chr:pos:ref:alt)
    ↓  DB lookup + window_bp
Genes overlapping input variants
    ↓  group membership (Pathway, GO, Disease, …)
Groups
    ↓  co-membership → Gene×Gene pairs
Gene×Gene pairs  [weight = # shared groups]
    ↓  cartesian of input variants per gene
Variant×Variant pairs  ← output
```

`group_support_count` is the biological weight: how many distinct groups connect the two genes.

See the explain guide: `biofilter/modules/report/reports_explain/report_variant_modeling.md`

### 1. Start Biofilter

```python
from biofilter import Biofilter

bf = Biofilter(debug_mode=False)
bf
```

### 2. Inspect report metadata

```python
report_name = 'variant_modeling'

print('name:', report_name)
print('\navailable columns:')
print(bf.report.available_columns(report_name))

print('\nexample_input:')
print(bf.report.example_input(report_name))

print('\nexplain:')
print(bf.report.explain(report_name))
```

### 3. Basic run — rsID inputs, Pathway grouping

Input: four variants from the APOE region and PCSK9.  
Grouping: Pathway (Reactome).  
Both variants in every output pair are from the input list.

```python
df = bf.report.run(
    'variant_modeling',
    input_data=[
        'rs429358',       # APOE ε4
        'rs7412',         # APOE ε2
        'rs2479409',      # PCSK9 promoter
        'rs11591147',     # PCSK9 R46L (loss-of-function)
    ],
    build=38,
    window_bp=0,
    group_entity_groups=['Pathway'],
)

print(f'Pairs: {len(df):,}')
df.head(20)
```

#### 3b. Top pairs by biological weight

```python
cols = [
    'variant_1_rsid', 'gene_1_name',
    'variant_2_rsid', 'gene_2_name',
    'group_support_count', 'group_support_names',
    'data_source_support_names',
]

df[cols].sort_values('group_support_count', ascending=False).head(20)
```

### 4. Mixed input — rsID, chr:pos, and chr:pos:ref:alt

The three formats can be mixed in the same list:

| Format | Example | Behavior |
|---|---|---|
| **rsID** | `rs429358` | dbSNP lookup |
| **chr:pos** | `chr19:44908684` | All alleles at the position (SNVs only) |
| **chr:pos:ref:alt** | `chr19:44908684:T:C` | Only the exact ref/alt variant (SNV or indel) |

Use `chr:pos:ref:alt` for credible-set / fine-mapping variants to avoid multiallelic ambiguity.

**Joining back to your source table.** Since BF4 4.1.4 the output carries two new columns,
`variant_1_input` and `variant_2_input`, that preserve the exact string you supplied. Use them
as the join key — no need to reparse `chr:pos:ref:alt` or look up rsID-to-coords after the fact.
Each variant also exposes `variant_*_ref` and `variant_*_alt` so multiallelic sites are
disambiguated.

```python
df_mixed = bf.report.run(
    'variant_modeling',
    input_data=[
        'rs429358',                # rsID
        'chr19:44908684',          # chr:pos (APOE region — all alleles at position)
        '2:21044574',              # bare chr:pos (APOB region)
        'chr19:44908684:T:C',      # chr:pos:ref:alt (exact APOE ε4 allele only)
    ],
    build=38,
    group_entity_groups=['Pathway'],
)

print(f'Pairs: {len(df_mixed):,}')

# Show the new 4.1.4 columns alongside the basics
preview_cols = [
    'variant_1_input', 'variant_1_rsid', 'variant_1_ref', 'variant_1_alt', 'gene_1_name',
    'variant_2_input', 'variant_2_rsid', 'variant_2_ref', 'variant_2_alt', 'gene_2_name',
    'group_support_count',
]
df_mixed[preview_cols].head(20)
```

#### 4b. Merging results back to a credible-set table

When the input came from a credible-set TSV (`locus / trait / SNP` columns, `SNP` in
`chr:pos:ref:alt` form), `variant_*_input` lets you merge the pair output directly back
to the source — no preprocessing, no rsID lookups, no allele parsing.

This is the canonical post-processing step for fine-mapping workflows.

```python
# Simulated credible-set table (same shape as multi_credible_sets_variants.tsv)
import pandas as pd

cs = pd.DataFrame({
    'locus': ['locus_apoe', 'locus_apoe', 'locus_apob'],
    'trait': ['LDL', 'LDL', 'LDL'],
    'SNP':   ['chr19:44908684:T:C', 'rs429358', '2:21044574'],
})

# Inner join on the input string — variant_1_input matches the SNP column verbatim
merged = cs.merge(
    df_mixed,
    left_on='SNP',
    right_on='variant_1_input',
    how='inner',
)

print(f'Merged rows: {len(merged):,}')
merged[[
    'locus', 'trait', 'SNP',
    'variant_1_input', 'variant_1_rsid', 'gene_1_name',
    'variant_2_input', 'variant_2_rsid', 'gene_2_name',
    'group_support_count',
]].head(20)
```

### 5. Multiple group types — Pathway + GO + Disease

Using multiple group types increases `group_support_count` when genes share more than one biological context.

```python
df_multi = bf.report.run(
    'variant_modeling',
    input_data=[
        'rs429358',
        'rs7412',
        'rs2479409',
        'rs11591147',
    ],
    build=38,
    group_entity_groups=['Pathway', 'GO', 'Disease'],
)

print(f'Pairs: {len(df_multi):,}')
df_multi[cols].sort_values('group_support_count', ascending=False).head(20)
```

#### 5b. group_support_count distribution

```python
import matplotlib.pyplot as plt

if not df_multi.empty:
    fig, ax = plt.subplots(figsize=(8, 4))
    df_multi['group_support_count'].value_counts().sort_index().plot(
        kind='bar', ax=ax, color='steelblue', edgecolor='white'
    )
    ax.set_xlabel('group_support_count (weight)')
    ax.set_ylabel('# variant pairs')
    ax.set_title('Biological weight distribution across variant pairs')
    plt.tight_layout()
    plt.show()
```

#### 5c. Gene pair heatmap (group_support_count)

```python
import pandas as pd

if not df_multi.empty:
    gene_pair_weight = (
        df_multi.groupby(['gene_1_name', 'gene_2_name'])['group_support_count']
        .max()
        .reset_index()
    )

    pivot = gene_pair_weight.pivot(index='gene_1_name', columns='gene_2_name', values='group_support_count').fillna(0)

    fig, ax = plt.subplots(figsize=(max(6, len(pivot.columns)), max(4, len(pivot))))
    im = ax.imshow(pivot.values, aspect='auto', cmap='Blues')
    plt.colorbar(im, ax=ax, label='group_support_count')
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha='right')
    ax.set_yticklabels(pivot.index)
    ax.set_title('Gene pair biological weight (max group_support_count)')
    plt.tight_layout()
    plt.show()

    print('Gene pair summary:')
    display(gene_pair_weight.sort_values('group_support_count', ascending=False))
```

### 6. Restrict to a specific data source

Use `group_data_sources` to filter group membership to a single source (e.g., Reactome only).

```python
df_reactome = bf.report.run(
    'variant_modeling',
    input_data=['rs429358', 'rs7412', 'rs2479409', 'rs11591147'],
    group_entity_groups=['Pathway'],
    group_data_sources=['Reactome'],
)

print(f'Reactome-only pairs: {len(df_reactome):,}')
df_reactome[cols].head(20)
```

### 7. Window extension

`window_bp` extends gene boundaries when assigning variants to genes.
Useful when variants fall in regulatory regions near gene loci.

```python
results = {}
for window in [0, 5_000, 25_000]:
    df_w = bf.report.run(
        'variant_modeling',
        input_data=['rs429358', 'rs7412', 'rs2479409', 'rs11591147'],
        group_entity_groups=['Pathway'],
        window_bp=window,
    )
    results[window] = len(df_w)
    print(f'window_bp={window:>6,}  →  {len(df_w):,} pairs')
```

### 8. Input from file

Pass a path to a plain-text file (one rsID or chr:pos per line).

```python
from pathlib import Path

# Create a temporary input file for the tutorial
tmp_dir = Path('tmp/variant_modeling_tutorial')
tmp_dir.mkdir(parents=True, exist_ok=True)

input_file = tmp_dir / 'variants.txt'
input_file.write_text('rs429358\nrs7412\nrs2479409\nrs11591147\n')

df_file = bf.report.run(
    'variant_modeling',
    input_data=str(input_file),
    group_entity_groups=['Pathway'],
)

print(f'Pairs from file: {len(df_file):,}')
df_file[cols].head(10)
```

### 9. Safety check — max_pairs

The report estimates pair count before materialising. If the estimate exceeds `max_pairs` it aborts safely.

```python
df_safe = bf.report.run(
    'variant_modeling',
    input_data=['rs429358', 'rs7412', 'rs2479409', 'rs11591147'],
    group_entity_groups=['Pathway', 'GO', 'Disease'],
    max_pairs=5,   # intentionally low to trigger the check
)

if 'resolution_status' in df_safe.columns:
    print('Safety abort triggered:')
    print(df_safe[['resolution_status', 'estimated_pairs', 'max_pairs', 'suggestion']].to_string())
else:
    print(f'{len(df_safe):,} pairs — no abort')
```

### 10. Export results

```python
output_path = tmp_dir / 'variant_modeling_pairs.csv'
df_multi.to_csv(output_path, index=False)
print(f'Saved {len(df_multi):,} pairs → {output_path}')
```

### 11. Running on the UPenn LPC (Apptainer)

For cohort-scale runs (thousands of input variants × pathway/GO/Disease grouping), the **Penn LPC**
is usually the right place to execute this report. The Apptainer image bundles BF4 + PostgreSQL —
no local DB required.

> **Why LPC for `variant_modeling` specifically**
> - Pair generation scales as O(N²) on input — large lists hit the `max_pairs` cap fast and benefit from cluster RAM.
> - The group co-membership joins (pathways × genes × variants) are I/O-heavy and run faster with the DB co-located in the container.
> - Credible-set studies typically produce `chr:pos:ref:alt` lists in the 1k–10k range — local notebook connection latency adds up.

See also:
- [`lpc__quickstart.md`](lpc__quickstart.md) — minimal copy-paste recipe for first runs
- [`lpc__deploy.md`](lpc__deploy.md) — maintainer guide for installing / updating the LPC image and DB

#### 11a. Prepare the input file

Plain text, one variant per line. The `chr:pos:ref:alt` form is preferred for credible sets:

```bash
module load apptainer
export WORKSPACE=/project/<your-project>/bf4_runs

cat > "$WORKSPACE/cs_variants.txt" <<'EOF'
1:6203732:A:G
1:46108752:C:T
2:71307487:T:A
chr19:44908684:T:C
EOF
```

The single-quoted `<<'EOF'` heredoc preserves the colons literally. For a TSV in
`locus / trait / SNP` format (typical credible-set output), pipe the `SNP` column:

```bash
awk -F'\t' 'NR>1 {print $3}' credible_sets.tsv | sort -u > "$WORKSPACE/cs_variants.txt"
```

#### 11b. Prepare the params file (avoids shell-quote breakage)

`variant_modeling` takes list parameters (`group_entity_groups`, `group_data_sources`).
**Passing them inline with `--param 'KEY=["Pathway"]'` fails** — the shell inside the container
strips the inner double quotes and you get errors like `No valid group_entity_groups found for ['[pathway]']`.

The robust pattern is a JSON file referenced via `--params-file`:

```bash
cat > "$WORKSPACE/vm_params.json" <<'EOF'
{
  "group_entity_groups": ["Pathway"],
  "window_bp": 0,
  "max_pairs": 1000000
}
EOF
```

For multi-group / multi-source runs:

```bash
cat > "$WORKSPACE/vm_params.json" <<'EOF'
{
  "group_entity_groups": ["Pathway", "GO", "Disease"],
  "group_data_sources": ["Reactome"],
  "window_bp": 5000,
  "max_pairs": 5000000
}
EOF
```

Sanity-check the JSON before running it through the container:

```bash
python3 -c "import json; print(json.load(open('$WORKSPACE/vm_params.json')))"
```

#### 11c. Run the report

Same boilerplate as `annotation_master_variant` — only `--name` and the file references change.
The temp dir + bind mounts give PostgreSQL inside the container its scratch space.

```bash
TMP=$(mktemp -d) && mkdir -p "$TMP/tmp" "$TMP/pg-run" && \
apptainer run --writable-tmpfs --pwd /tmp \
  --bind /project/hall_shared/biofilter/databases/20260514/pgdata:/var/lib/postgresql/data \
  --bind "$TMP/tmp:/tmp" \
  --bind "$TMP/pg-run:/var/run/postgresql" \
  --bind "$WORKSPACE:/workspace" \
  /project/hall_shared/biofilter/images/bf4-hpc-4.1.2.sif \
  biofilter report run \
    --name variant_modeling \
    --input-file /workspace/cs_variants.txt \
    --params-file /workspace/vm_params.json \
    --output /workspace/variant_modeling_pairs.csv && \
rm -rf "$TMP"
```

Result: `$WORKSPACE/variant_modeling_pairs.csv`.

> **Safety check first.** Start with `"max_pairs": 1000000` — if the estimator aborts, the CSV
> will contain a single row with `resolution_status`, `estimated_pairs`, `max_pairs`, and a
> `suggestion` column telling you how to tighten the filter. Re-tune (`group_data_sources`,
> stricter `group_entity_groups`, smaller `window_bp`) before raising the cap.

### 12. Real cohort template

Replace the input list with your study variants and adjust group filters.

```python
# Option A: explicit list (rsID, chr:pos, or chr:pos:ref:alt)
my_variants = [
    'rs429358',
    # ... add your variants
]

# Option B: load from file (one entry per line; mixed formats supported)
# my_variants = '/path/to/variants.txt'

# For credible-set / fine-mapping cohorts, prefer chr:pos:ref:alt to avoid
# multiallelic ambiguity at SNP positions:
#   ['1:6203732:A:G', 'chr19:44908684:T:C', ...]

# df_cohort = bf.report.run(
#     'variant_modeling',
#     input_data=my_variants,
#     build=38,
#     window_bp=0,
#     group_entity_groups=['Pathway', 'GO'],
#     group_data_sources=['Reactome'],
#     max_pairs=1_000_000,
# )

# print(f'Cohort pairs: {len(df_cohort):,}')
# df_cohort.to_csv('outputs/variant_modeling_cohort.csv', index=False)
# df_cohort.head(20)
```



<!-- ===== SOURCE FILE: notebooks/Templates/reports__variant_single_gene_annotation.ipynb.md ===== -->

# 📘 Biofilter — Report: Variant Single Gene Annotation

**Phase 1 of the single-variant SNP×SNP interaction pipeline.**

Given one input variant (`chr:pos` or rsID), this report:

1. Resolves the variant to a genomic position (queries `variant_masters` when an rsID is given).
2. Finds the **seed gene** at that position using `entity_locations` (with an optional base-pair window).
3. Expands through a configurable biological group type (Pathways, Diseases, GO, or direct Gene links) to collect **partner genes**.
4. Enriches every partner gene with coordinates, locus group, functional gene groups, and a variant count.

Output: one row per **(seed gene × partner gene)** pair.

---

### Methods used
- `bf.report.explain("variant_single_gene_annotation")`
- `bf.report.available_columns("variant_single_gene_annotation")`
- `bf.report.example_input("variant_single_gene_annotation")`
- `bf.report.run("variant_single_gene_annotation", **params)`

---

### 1. Start Biofilter

```python
from biofilter import Biofilter

# Uses db_uri from .biofilter.toml if available
bf = Biofilter(debug_mode=False)
```

---

### 2. Inspect the report contract

```python
print(bf.report.explain("variant_single_gene_annotation"))
```

```python
bf.report.available_columns("variant_single_gene_annotation")
```

```python
bf.report.example_input("variant_single_gene_annotation")
```

---

### 3. Run with built-in example input

Uses `chr19:44904604` — the **APOE** locus, a well-annotated gene with many Reactome and KEGG pathways.

---

### 4. Positional input — chr:pos

Directly supply a chromosome + position. Accepted separators: `:`, `;`, `,`, `-`, space.

```python
df_pos = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    build=38,
    group_entity_type="Pathways",
)

print(f"Rows: {len(df_pos)}")
print(f"Seed gene : {df_pos['seed_gene_symbol'].iloc[0]}")
print(f"Partners  : {df_pos['partner_gene_symbol'].nunique()} unique genes")
df_pos[["seed_gene_symbol", "partner_gene_symbol", "shared_group_count", "shared_group_names"]].head(10)
```

---

### 5. rsID input

Supply an rsID — the report resolves it to a position via `variant_masters` before the gene lookup.

```python
df_rs = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",    # APOE rs429358
    build=38,
    group_entity_type="Pathways",
)

print(f"Rows: {len(df_rs)}")
print(f"Resolved rsid      : {df_rs['seed_rsid'].iloc[0]}")
print(f"Resolved position  : chr{df_rs['seed_chromosome'].iloc[0]}:{df_rs['seed_position'].iloc[0]}")
print(f"Allele count       : {df_rs['seed_allele_count'].iloc[0]}")
df_rs[["seed_rsid", "seed_gene_symbol", "partner_gene_symbol", "shared_group_count"]].head(10)
```

---

### 6. Source system filter

Restrict expansion to specific source systems (e.g. Reactome only).
Filtering is done at the `entity_relationships.data_source_id` level — no post-processing.

Accepts a single string or a list: `"Reactome"` or `["Reactome", "KEGG"]`.

```python
df_reactome = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",
    build=38,
    group_entity_type="Pathways",
    source_system_filter=["Reactome"],
)

df_all = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="rs429358",
    build=38,
    group_entity_type="Pathways",
)

print(f"Reactome only → {df_reactome['partner_gene_symbol'].nunique()} partner genes, {len(df_reactome)} rows")
print(f"All sources   → {df_all['partner_gene_symbol'].nunique()} partner genes, {len(df_all)} rows")

# Sources present when no filter is applied
df_all["shared_group_sources"].str.split("|").explode().value_counts(dropna=True).head(10)
```

---

### 7. Direct gene-gene links (`group_entity_type="Genes"`)

1-hop expansion: partner genes are directly linked to the seed gene via `entity_relationships`
(no intermediary pathway/disease node). Useful for curated interaction databases (BioGRID, ClinGen).

```python
df_direct = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    group_entity_type="Genes",
)

print(f"Rows: {len(df_direct)}")
df_direct[["seed_gene_symbol", "partner_gene_symbol", "partner_gene_chromosome", "shared_group_sources"]].head(10)
```

---

### 8. Base-pair window — closest gene logic

`window_bp` extends the gene search around the given position.
When multiple genes are within the window, the **closest** is selected:
- distance = 0 if the position falls inside the gene body
- distance = gap to the nearest edge otherwise
- ties broken by smallest locus span (most specific gene)

```python
# Try a position that sits in an intergenic region — use a 10 kb window
df_win = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr7:117548628",
    window_bp=10000,
    group_entity_type="Pathways",
)

print(f"Rows: {len(df_win)}")
if not df_win.empty:
    g = df_win.iloc[0]
    print(f"Seed gene : {g['seed_gene_symbol']}  "
          f"(chr{g['seed_gene_chromosome']}:{g['seed_gene_start']}-{g['seed_gene_end']})")
df_win[["resolution_status", "seed_gene_symbol", "partner_gene_symbol", "shared_group_count"]].head(10)
```

---

### 9. Other group types — Diseases and GO

The `group_entity_type` parameter accepts any `EntityGroup` name in the database.
Common options: `"Pathways"`, `"Diseases"`, `"GO"`, `"Genes"`.

```python
df_dis = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    group_entity_type="Diseases",
)

print(f"Diseases expansion → {len(df_dis)} rows, {df_dis['partner_gene_symbol'].nunique()} partner genes")
df_dis[["seed_gene_symbol", "partner_gene_symbol", "shared_group_count", "shared_group_names"]].head(10)
```

```python
df_go = bf.report.run(
    "variant_single_gene_annotation",
    input_variant="chr19:44904604",
    group_entity_type="GO",
)

print(f"GO expansion → {len(df_go)} rows, {df_go['partner_gene_symbol'].nunique()} partner genes")
df_go[["seed_gene_symbol", "partner_gene_symbol", "shared_group_count", "shared_group_names"]].head(10)
```

---

### 10. Resolution failure handling

When the report cannot resolve the input, it returns a single-row DataFrame
with `resolution_status` set to a descriptive error code — never raises an exception.

```python
cases = [
    ("invalid input string",      {"input_variant": "not-a-variant"}),
    ("rsID not in database",      {"input_variant": "rs999999999999"}),
    ("unknown group type",        {"input_variant": "chr19:44904604", "group_entity_type": "UnknownGroup"}),
    ("intergenic, no window",      {"input_variant": "chr1:1", "window_bp": 0}),
]

for label, params in cases:
    result = bf.report.run("variant_single_gene_annotation", **params)
    status = result["resolution_status"].iloc[0]
    print(f"{label:<30} → {status}")
```

---

### 11. Suggested presentation view

```python
display_cols = [
    "seed_gene_symbol",
    "seed_gene_chromosome",
    "seed_gene_start",
    "seed_gene_end",
    "seed_gene_locus_group",
    "partner_gene_symbol",
    "partner_gene_chromosome",
    "shared_group_count",
    "shared_group_names",
    "seed_gene_variant_count",
    "partner_gene_variant_count",
]

# Use the Pathways run from section 4
present_df = (
    df_pos[[c for c in display_cols if c in df_pos.columns]]
    .sort_values("shared_group_count", ascending=False)
)
present_df.head(20)
```

---

### 12. CLI reference

All examples above can be reproduced from the command line using `biofilter report run`.

```bash
# ── Positional input, Pathways expansion
biofilter report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=chr19:44904604

# ── rsID input with Reactome-only filter
biofilter report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=rs429358 \
  --param group_entity_type=Pathways \
  --param source_system_filter=Reactome

# ── Direct gene-gene links (1-hop)
biofilter report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=chr19:44904604 \
  --param group_entity_type=Genes

# ── 10 kb window, Disease expansion
biofilter report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=chr7:117548628 \
  --param window_bp=10000 \
  --param group_entity_type=Diseases

# ── Save output to CSV
biofilter report run \
  --report-name variant_single_gene_annotation \
  --param input_variant=rs429358 \
  --output apoe_partners.csv

# ── Inspect available params
biofilter report run \
  --report-name variant_single_gene_annotation \
  --params-template
```

---

### 13. Pipeline context

This report is **Phase 1** of the single-variant SNP×SNP interaction pipeline.

```
Phase 1 — Gene Discovery  (this report)
  input : one variant (chr:pos or rsID)
  output: seed gene + partner-gene list with shared-group annotation
  scale : ~8 k rows (tractable)

Phase 2 — Filtered Variant Collection  (planned)
  input : Phase 1 partner-gene list
  output: variants per gene, pre-filtered to coding / functional consequences
  scale : ~100 k rows (SQL-level filtering)

Phase 3 — Pair Generation  (planned)
  input : Phase 2 variant sets per gene
  output: variant × variant interaction pairs (seed × partner)
  scale : controlled by Phase 2 filtering
```

Separating gene discovery from variant enumeration avoids the **combinatorial explosion**
that occurs when all variants are annotated before filtering
(e.g. APOE alone has ~1 k variants → 1 k × 260 k = 260 M rows without pre-filtering).

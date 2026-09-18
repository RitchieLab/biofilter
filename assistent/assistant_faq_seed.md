# BF4 FAQ Seed

Curated, high-signal answers for the assistant's knowledge base. The audience
is someone who was given a **bundle** and wants an answer out of it.

Every command here has been checked against the 4.3 CLI. When this file and a
report's own explain guide disagree about that report, the explain guide wins.

---

## A) Running reports — the common case

### A1) I just want to run a report and get a CSV

```bash
biofilter --bundle /path/to/bundles/20260914 \
  report run --report-name annotate_gene \
  --input TP53 --input BRCA1 \
  --output genes.csv
```

If the bundle is already configured (see B1), drop `--bundle`.

The format follows the extension: `.csv`, or `.parquet` to keep the
provenance inside the file.

### A2) What reports can I run?

```bash
biofilter report list --verbose
```

Seventeen, in six families. By the shape of the question:

| You have | Report |
|---|---|
| names that may not match | `resolve_entity` |
| genes, want what is known | `annotate_gene` |
| rsIDs or positions | `annotate_variant` |
| diseases, pathways, proteins, GO terms | `annotate_disease`, `annotate_pathway`, `annotate_protein`, `annotate_go` |
| genes, want the variants in them | `expand_gene_to_variant` |
| variants, want what they regulate | `expand_variant_regulatory` |
| entities, want what connects | `expand_entity_neighborhood`, `expand_entity_relationship` |
| variants, want candidate pairs | `pair_variants` |
| genes, want candidate variant pairs | `expand_gene_to_variant`, then `pair_variants` |
| genes, want which are related and by what | `pair_genes` |
| a gene-to-anything list of your own, want the pairs it implies | `pair_genes` |
| a cohort | `aggregate_cohort_variants` |
| a bundle you do not know | `platform_data_statistics` |
| to know what ran to build it | `platform_etl_status`, and `platform_etl_packages` for the raw record |

### A2b) `pair_variants` or `pair_genes`?

Whichever matches **where the link between a variant and a gene comes
from**.

`pair_variants` derives it from coordinates: a variant belongs to the
gene it sits inside. That is right for a coding variant.

`pair_genes` does no variant-to-gene mapping at all. Use it when the
attachment comes from somewhere else — a colocalization, a fine-mapping,
a curated assignment — and supply it as a two-column mapping, gene then
item. A regulatory variant usually acts on a gene it does not sit in, so
`pair_variants` would look for it among that gene's positional variants,
not find it, and drop it without an error.

The item in the mapping is never read, so it can be a position, an rsID,
a probe id, an exposure — anything. Biofilter is build 38 and managing
build is the user's; nothing in this report interprets a coordinate.

### A2c) I passed Entrez ids to `pair_genes` and got the wrong genes

Say which column they are:

```bash
--param gene_identifier=entrez
```

`gene_identifier` is `alias` by default, which searches every alias at
once. That is right when a user is typing gene names and wrong when their
list came from one source, because the same text can mean two genes:
174,410 gene aliases in the bundle are bare numbers (Entrez ids) and
14,335 of those are also the entity id of a different gene. Entrez `2` is
A2M; entity `2` is A1BG-AS1.

Naming the system narrows the search rather than only breaking the tie —
under `entrez`, `2` can only be A2M. The accepted values are `alias`,
`entity_id` (or `biofilter_id`), and any code system the bundle carries;
asking for one it lacks is refused with the list of what it has.

It governs the mapping too, not just `input_data`.

### A2d) `pair_variants` says my genes are not variants

It takes variants only — an rsID, `chr:pos`, or `chr:pos:ref:alt`. The
gene path was removed in 4.3.0.

Run `expand_gene_to_variant` on the genes, filter what comes back, and
feed those variant keys to `pair_variants`. That is one extra step and it
is the point: the old behaviour kept 100 of a gene's ~4,000 variants,
ranked by allele frequency, and never said which.

`membership` and `max_variants_per_gene` went with it. Passing either
raises rather than being ignored, because ignoring one would answer a
different question without saying so. `membership="either"` is now
`pair_genes(membership="either")`, then `expand_gene_to_variant` on the
partner genes, then pair.

### A2e) Does case matter in a parameter or an input?

No. `Pathways`, `pathways` and `PATHWAYS` are the same option value;
`CHEK2` and `chek2` the same gene; `rs429358` and `RS429358` the same
variant; `22:10510212:A:T` and `22:10510212:a:t` the same alleles. A
`chr` prefix is optional, in any case.

That covers option values, input values and the two-column mapping
`pair_genes` takes. A test asserts it across the reports so it stays
true.

The one thing case affects is the text coming back: a report returns the
bundle's spelling of a name, not the user's, because that is the one
another report will recognise.

### A3) What does a report accept?

```bash
biofilter report explain --report-name annotate_variant
biofilter report run --report-name annotate_variant --params-template
biofilter report available-columns --report-name annotate_variant
```

`explain` prints the full guide — parameters, columns, and how to read the
result. It is the authority for any single report.

### A4) How do I pass many inputs?

Repeat the flag, or read from a file. **There is no comma-separated form.**

```bash
--input APOE --input TP53 --input BRCA1
--input-file genes.txt                          # one value per line
--input-file cohort.csv --input-column symbol   # a CSV column
```

### A5) How do I pass options?

Options are a separate channel from input:

```bash
--param mapping=annotation
--param af_max=0.01
--param impact_filter='["HIGH","MODERATE"]'     # a list is JSON
--param consequence_type_filter=@./terms.txt    # @ reads from a file
--params-json '{"mapping":"annotation","af_max":0.01}'
--params-file ./params.yaml
```

Values are coerced: `true`/`false`, numbers, JSON, else a string.

### A6) I have genes and want the variants in them that are likely damaging

```bash
biofilter report run --report-name expand_gene_to_variant \
  --input BRCA1 --input CHEK2 \
  --param mapping=annotation \
  --param impact_filter=HIGH \
  --param af_max=0.01 \
  --param alphamissense_classification=likely_pathogenic \
  --output candidates.csv
```

First decide what "belongs to" means — it changes the answer:

| `mapping` | a variant belongs to a gene when |
|---|---|
| `position` | its coordinate falls inside the gene's build-38 range |
| `annotation` | VEP associated it with that gene |

Two defaults to know: `most_severe_only` is `true` (one row per gene and
variant), and `max_variants_per_gene` is `5000` — when a gene is capped, the
`variants_available` column reports its true total.

### A7) "Report not found"

```bash
biofilter report list --verbose
```

Then use an exact name. If you are following an older note, the name probably
changed: the 4.2.x names (`annotation_master_gene`, `entity_filter`,
`gene_to_variant_filtering`, `variant_binning`, `etl_status`, `snp_snp_*`) no
longer exist.

### A8) "Input conflict"

Keep records in `--input` / `--input-file`. Do not also pass `input_data`,
`items` or `input_path` through `--param`.

---

## B) Pointing at a bundle

### B1) How does Biofilter know which data to read?

In order: `--bundle`, then `--db-uri`, then `BIOFILTER_BUNDLE`, then
`DATABASE_URL` / `BIOFILTER_DB_URI`, then `.biofilter.toml`.

```bash
biofilter --bundle /path/to/bundles/20260914 report list   # one command
export BIOFILTER_BUNDLE=/path/to/bundles/20260914          # the session
```

```toml
# .biofilter.toml — a relative path here resolves against THIS FILE
[database]
bundle = "./biofilter_data/bundles/20260914"
```

`biofilter config show` prints what actually resolved.

Passing `--bundle` and `--db-uri` together is an error, not a guess.

### B2) Which directory exactly?

The **bundle root** — the one holding `manifest.json`. Not its `tables/`
subdirectory. Biofilter reads the manifest to learn which files make up each
table; pointed at `tables/` it will not find one and says so.

### B3) Can I point reports at our PostgreSQL?

No. Reports read a bundle. `--db-uri` exists for writing — a staging database
during a build — and is not a way to run reports.

### B4) On the LPC, how do I start?

```bash
source /project/hall_shared/hall_shared.sh
module load biofilter/4.3.0

biofilter report run --report-name annotate_gene --input APOE --output apoe.csv
```

The module sets `BIOFILTER_BUNDLE`, so you pass no path.

---

## C) Reading the result honestly

### C1) My result is empty. Is that the answer?

Not necessarily — check which kind of empty:

| Status | Means |
|---|---|
| `not_found` | the name did not resolve in this bundle |
| `no_location` | it resolved, but there are no coordinates for it |
| `no_variants` | it resolved and nothing met your criteria — a real negative |

`resolve_entity` tells you what your names actually matched.

### C2) A whole column is null

That may mean the source was never built into this bundle, not that the
answer is null. Every result records it:

```python
result.provenance["coverage"]     # optional tables that were absent
```

And to see what the bundle holds at all:

```bash
biofilter report run --report-name platform_data_statistics
```

Worth running once on any bundle you have just been handed.

### C3) Can I use these ids against another bundle?

**No.** `entities.id`, `variant_id` and every other surrogate are valid only
inside the bundle that produced them. The drift between builds is small, which
is what makes it dangerous — a stale id still resolves, to a different gene in
the same family, with no error.

Pin the bundle, not the id. Results carry `bundle_id` in their provenance, and
saving one writes a `.provenance.json` beside it. Across bundles, use natural
keys: `chromosome:position:ref:alt` for variants, symbols and HGNC ids for
genes.

### C4) Did anything go wrong that the table does not show?

```python
result.provenance["warnings"]
```

Always present. A report that copes with a problem rather than failing writes
it here — a chromosome it could not place, a `maf_cutoff` below what the
cohort can observe, samples with no phenotype. An empty list means nothing
went wrong; it never means nobody checked.

`aggregate_cohort_variants` is the one most likely to have something there.

### C5) I ran a report and think I am missing part of the answer

Some reports return more than one table. The result object holds them:

```python
result.table                      # the main one
result.extra_tables               # the rest, by name
result.tables.keys()              # everything, main one first
```

| Report | Main table | Beside it |
|---|---|---|
| `platform_data_statistics` | the long list of measurements | `storage`, `variants` |
| `aggregate_cohort_variants` | one row per bin and sample | `variant_to_bin` |

For `platform_data_statistics` the extras are not a detail: in the long shape
a table's size is the string `"3.4 MB"` and a chromosome sorts 1, 10, 11, 2.
In `storage` and `variants` they are integers.

```python
stats = bf.report.run("platform_data_statistics")
stats.extra_tables["storage"]     # table, branch, rows, bytes, files
stats.extra_tables["variants"]    # table, chromosome, rows
```

### C6) How do I save a result so I can come back to it?

Two verbs, and the difference matters:

```python
result.write("genes.csv")         # export: one table, flattened for a spreadsheet
result.save("./results/run_01")   # keep: every table, nothing lost
```

`write()` takes the main table only. Nested columns become JSON strings so a
spreadsheet can hold them, and extra tables and artifacts are not included —
which is fine for handing numbers to someone, and wrong for a multi-table
report.

`save()` writes a directory: each table as parquet, plus a manifest. Read it
back with:

```python
from biofilter.modules.report.result import ReportResult

later = ReportResult.load("./results/run_01")
later.extra_tables
later.provenance["source_bundle"]   # is the source bundle still on disk?
```

A saved result is written in a bundle's shape, so `Bundle.open()` can query it
too.

### C7) I loaded an old result and the bundle is gone

`load()` reports it:

```python
later.provenance["source_bundle"]["still_present"]   # False
```

The rows are unchanged and still belong to the build `bundle_id` names. What
you cannot do without that bundle is resolve the ids in them to anything else.
A result outliving its bundle is the normal case, and the reason for saving
one.

### C8) It warns the bundle was built by a different Biofilter

```
UserWarning: <bundle> was built by Biofilter 4.2.0; this is 4.3.0.
```

It opens and usually behaves. The warning exists because a column that changed
meaning between releases will not announce itself. To check:

```bash
biofilter db verify --in /path/to/bundle --schema
```

The result records it under `provenance["version_mismatch"]`.

---

## D) Containers

### D1) How do I run this in Docker?

Two mounts: the bundle read-only, and somewhere to write.

```bash
docker run --rm \
  -v /path/to/bundles/20260914:/bundle:ro \
  -v "$PWD/out:/workspace" \
  --user "$(id -u):$(id -g)" \
  ghcr.io/ritchielab/biofilter:latest \
  report run --report-name annotate_gene --input TP53 --output /workspace/genes.csv
```

`BIOFILTER_BUNDLE` already defaults to `/bundle` inside the image.

### D2) My output file is missing

`--output` writes **inside** the container. Point it at the mounted
`/workspace`, or the file leaves with the container.

### D3) The output files belong to the wrong user

Under Docker the image runs as its own user. Add
`--user "$(id -u):$(id -g)"`. Under Apptainer this does not happen — the
container runs as you.

### D4) Apptainer / Singularity

```bash
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter:latest

apptainer run \
  --bind /path/to/bundles/20260914:/bundle:ro \
  --bind ~/out:/workspace \
  bf4.sif \
  report run --report-name annotate_gene --input APOE --output /workspace/apoe.csv
```

The same image as the Docker example above; only the mount flag differs.

---

## E) Diagnosing

### E1) Which version, and what is it reading?

```bash
biofilter --version
biofilter config show
biofilter bundle info /path/to/bundle
```

### E2) Is the bundle intact?

```bash
biofilter db verify --in /path/to/bundle
biofilter db verify --in /path/to/bundle --schema   # also checks columns
```

### E3) More detail on a failure

```bash
biofilter --debug report run --report-name <name> --input <value>
```

### E4) Where do the explain guides live?

`biofilter/modules/report/reports_explain/report_<name>.md`, printed by
`biofilter report explain --report-name <name>`. Each report also has a worked
notebook at `notebooks/templates/reports__<name>.ipynb`.

---

## F) Out of scope — what to say

### F1) "How do I build my own bundle?"

That is a maintainer task: roughly 150 GB of working space and about two days,
through `bundle plan` and `bundle build`. Point at the Building Bundles guide
and suggest asking whoever maintains the bundle. Do not walk a scientist
through it as if it were setup.

### F2) "How do I update the data?"

A bundle is never updated in place. Refreshing means a **new** bundle gets
built and published. Same deferral as F1.

### F3) "How is X implemented?"

The knowledge base has no source code. Say it is outside your knowledge and
point to the repository.

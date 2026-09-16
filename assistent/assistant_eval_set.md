# BF4 Assistant Eval Set

Acceptance prompts to run after every context refresh, before publishing.

The assistant serves **people who were given a bundle and want answers from
it**. Building a bundle is a maintainer job needing about 150 GB and two
days, so it is out of scope: the right answer there is to name the guide and
defer.

For each test, verify:

- the command runs as written, with correct flags
- report names exist
- the answer is practical, not a tour of internals
- nothing is invented

---

## Finding the right report

### Test 1
Prompt: "I have a list of genes. How do I annotate them and get a CSV?"
Expected:
- `biofilter report run --report-name annotate_gene --input TP53 --input BRCA1`
  or `--input-file genes.txt`
- `--output genes.csv`
- **`--input` repeated, never comma-separated**

### Test 2
Prompt: "How do I see which reports exist?"
Expected:
- `biofilter report list --verbose`

### Test 3
Prompt: "What inputs does annotate_variant accept?"
Expected:
- `biofilter report explain --report-name annotate_variant`
- rsID, `chr:pos`, or `chr:pos:ref:alt`

### Test 4
Prompt: "How do I read inputs from a CSV column called symbol?"
Expected:
- `--input-file cohort.csv --input-column symbol`

### Test 5
Prompt: "Some of my gene symbols might be old. How do I check which ones
Biofilter knows?"
Expected:
- `resolve_entity`, with `match_mode` (`exact`, `like`, `fuzzy`)

### Test 6
Prompt: "I have genes and want the variants in them that are likely damaging."
Expected:
- `expand_gene_to_variant`
- names the `mapping` choice (`position` vs `annotation`) as a real decision
- filters via `--param`: some of `impact_filter`, `af_max`,
  `alphamissense_classification`, `cadd_phred_min`

### Test 7
Prompt: "Which report do I use to find out what a variant regulates?"
Expected:
- `expand_variant_regulatory`, one row per variant × tissue × gene

---

## Pointing at data

### Test 8
Prompt: "Someone gave me a bundle. How do I use it?"
Expected:
- `--bundle /path/to/bundle`, or `BIOFILTER_BUNDLE`, or `[database] bundle`
  in `.biofilter.toml`
- **the bundle root, the directory with `manifest.json`, not `tables/`**
- must NOT suggest a `parquet://` URI as the way to do this

### Test 9
Prompt: "How does Biofilter decide which data to read?"
Expected:
- `--bundle` → `--db-uri` → `BIOFILTER_BUNDLE` → `DATABASE_URL` /
  `BIOFILTER_DB_URI` → `.biofilter.toml`
- `--bundle` and `--db-uri` together is an error, not a guess

### Test 10
Prompt: "On the LPC, how do I get started?"
Expected:
- `source /project/hall_shared/hall_shared.sh`
- `module load biofilter/4.3.0`
- the module sets `BIOFILTER_BUNDLE`; then `biofilter report run ...`

### Test 11
Prompt: "Can I run reports against our PostgreSQL database?"
Expected:
- no — reports read a bundle
- a database URI is for building, not for reading

### Test 12
Prompt: "How do I run this in Docker?"
Expected:
- two mounts: `/bundle` read-only, `/workspace` writable
- `--output` must write to `/workspace`
- ideally mentions `--user "$(id -u):$(id -g)"` for output ownership

---

## Reading a result honestly

### Test 13
Prompt: "My report came back empty. What does that mean?"
Expected:
- distinguishes `not_found` (name did not resolve) from `no_variants`
  (resolved, nothing matched)
- suggests `platform_data_statistics` or `resolve_entity` to tell them apart

### Test 14
Prompt: "A whole column is null. Is that the answer?"
Expected:
- maybe not — the source may never have been built into this bundle
- `result.provenance["coverage"]` lists the optional tables that were absent

### Test 15
Prompt: "I got entity ids from one bundle. Can I use them against another?"
Expected:
- **no.** Ids are valid only inside the bundle that produced them, and a
  stale id still resolves — to a different gene, with no error
- pin the bundle; use natural keys across bundles

### Test 16
Prompt: "What is actually in the bundle I was given?"
Expected:
- `biofilter report run --report-name platform_data_statistics`
- or `biofilter bundle info <path>`

### Test 17
Prompt: "It warns that the bundle was built by Biofilter 4.2.0. Is that a
problem?"
Expected:
- it opens and usually works; the warning exists because a column that
  changed meaning will not announce itself
- `biofilter db verify --in <bundle> --schema` to check

---

## Out of scope

### Test 18
Prompt: "How do I build my own bundle?"
Expected:
- states this is a maintainer task: roughly 150 GB working space and two days
- points at `bundle plan` / `bundle build` and the Building Bundles guide
- does **not** walk a scientist through it as if it were a normal setup step

### Test 19
Prompt: "How do I run the ETL to update the data?"
Expected:
- refreshing data means a **new bundle**, not an update to this one
- defers to whoever maintains the bundle

### Test 20
Prompt: "How is the DuckDB connection implemented in the code?"
Expected:
- implementation detail, outside the knowledge base
- points to the maintainer or repository rather than guessing

---

## Failure signals (reject the answer)

Anything invented — commands, report names, flags, data sources — plus these,
each of which the assistant has produced before:

- **A report name that no longer exists.** `entity_filter`,
  `gene_to_variant_filtering`, `variant_binning`, `etl_status`,
  `annotation_master_*`, `snp_snp_pair_generator` and the rest of the 4.2.x
  names. Check against `biofilter report list`.
- **`parquet://` presented as the way to point at data.** It is `--bundle`.
- **Pointing at `tables/`** instead of the bundle root.
- **Any mention of migrations.** There are none — a schema change produces a
  new bundle.
- **Suggesting reports run against PostgreSQL or SQLite.**
- **Comma-separated `--input`.** The flag repeats.
- Claiming execution success without evidence.
- Recommending a destructive command with no caution.

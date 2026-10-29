# Biofilter 4: a versioned, file-based knowledge platform for knowledge-driven genomic analysis

*Draft manuscript section — platform and technology. Prepared for
incorporation into the ADSP interaction-analysis paper.*

Draft of 2026-10-21 · describes Biofilter 4.3.0 and bundle `9b8419b48be5004e`

> **How to use this draft.** Sections 1–8 are written as manuscript text and
> can be lifted, cut or merged. Section 9 is a short bridge to the ADSP
> application, which is deliberately left to the main paper. Items marked
> **[confirm]** need checking before submission; suggested figures and tables
> are listed at the end, along with the numbers this text relies on and where
> each comes from.

---

## Abstract

Knowledge-driven filtering — using what is already known about genes,
pathways, proteins and diseases to decide which variants and which variant
combinations are worth testing — is central to detecting epistasis in
genome-scale data, where the number of possible pairwise models is far beyond
what can be tested or corrected for. Biofilter has supported this strategy
since 2009. Biofilter 4 is a redesign for current data volumes. It integrates
genes, variants, proteins, pathways, diseases, Gene Ontology terms and
chemicals from curated public sources into a single entity-centric model, and
distributes that model as a **bundle**: a dated, read-only, self-describing
directory of Apache Parquet files. A bundle is queried in the analyst's own
process through the embedded DuckDB engine, with no database server, no import
step and no per-user copy. The current release integrates data from 14 source
systems — including the full gnomAD v4 joint callset with its VEP annotations,
AlphaMissense, GTEx eQTL and the GWAS Catalog — into 3.44 billion rows that
occupy 26.7 GB on disk. Analyses are expressed through 17 reusable reports
built on an extensible framework, and every result records the bundle, report
and parameters that produced it. New data sources and new reports can be added
by users through documented contracts, and pre-built bundles can be shared as
plain files so that most users never run the ingestion themselves.

---

## 1. Background: Biofilter and LOKI

Genome-wide association studies test variants one at a time. Interactions
between loci are a plausible source of the heritability those tests miss, but
an exhaustive pairwise search over a modern variant set is computationally
expensive and statistically underpowered: one million variants define roughly
5 × 10¹¹ pairs. Biofilter was introduced to make that search tractable by
restricting it to combinations that prior biological knowledge supports.

**Biofilter 1.0** (Bush et al., 2009) established the approach: integrate
public knowledge about genes and their groupings — pathways, protein
families, interaction networks, ontology terms — and use shared membership in
those groupings to propose gene–gene, and hence SNP–SNP, models for testing.

**Biofilter 2.0** (Pendergrass et al., 2013) separated the knowledge from the
tool. Knowledge was collected into **LOKI** (Library of Knowledge
Integration), a local SQLite database built by a set of per-source loaders —
Entrez Gene, dbSNP, UCSC, GO, KEGG, Reactome, BioGRID, MINT, Pfam, NetPath,
PharmGKB, ORegAnno and the GWAS Catalog among them — and Biofilter 2.0 queried
LOKI to filter, annotate and generate models for SNPs, genes and regions.

**Biofilter 3** **[confirm: release name, year and scope]** carried the
2.x design forward to Python 3, keeping LOKI's single-file SQLite model and
loader architecture.

These versions were widely used, and their central idea — knowledge as a
locally built, reusable database rather than as ad hoc lookups — remains the
basis of the platform. Two developments since then motivated a redesign
rather than a further update:

1. **Scale.** Variant-level resources have grown by orders of magnitude.
   gnomAD v4 describes hundreds of millions of variants, and its VEP
   annotations run to billions of variant–transcript–consequence records.
   A design in which each user builds and holds a local copy of a relational
   database does not scale to that.
2. **Kinds of knowledge.** Analyses now draw on evidence that is not a
   gene-grouping — predicted pathogenicity (AlphaMissense), tissue-specific
   regulation (GTEx eQTL), curated gene–disease validity (ClinGen), disease
   ontologies (MONDO) — and need it alongside the grouping knowledge
   Biofilter has always used.

---

## 2. Design goals

Biofilter 4 was designed around five requirements:

- **One model for many entity types.** Genes, variants, proteins, pathways,
  diseases, GO terms and chemicals are first-class entities, with aliases and
  typed relationships among them, rather than a gene table with satellites.
- **Build once, read everywhere.** The expensive work — downloading,
  parsing and harmonising sources — is done once by a maintainer. Everyone
  else receives the result ready to query.
- **Analysis-ready at genome scale on ordinary hardware.** Questions over
  billions of rows should be answerable on a laptop or a single cluster node,
  without a database server.
- **Reproducibility as a property of the data, not of discipline.** A
  knowledge release is immutable and identified, and every result says which
  release produced it.
- **Extensible by users.** Adding a data source or a new kind of query should
  follow a documented contract, not require changes to the platform's core.

---

## 3. Architecture

Biofilter 4 has two halves, separated by a file artifact rather than by a
service (Figure 1).

|                | Builds a bundle                          | Reads a bundle                 |
| -------------- | ---------------------------------------- | ------------------------------ |
| Who            | a data maintainer                        | every analyst                  |
| How often      | a few times a year                       | continuously                   |
| Engine         | SQLAlchemy models over a staging SQLite  | DuckDB, embedded in-process    |
| Can write      | yes — the only code that can             | no, by construction            |
| Interface      | `bundle`, `etl`, `db` commands           | `report` commands, Python API  |

**The entity-centric model.** Every biological object is an `Entity` with
a type, a set of `EntityAlias` records (symbols, accessions, synonyms from
every source that names it) and typed `EntityRelationship` records to other
entities. Domain tables (`GeneMaster`, `ProteinMaster`, `PathwayMaster`,
`DiseaseMaster`, `GOMaster`, `ChemicalMaster`) carry the attributes specific
to each type. Grouping knowledge — the pathways, protein families and
disease sets that drive model generation — is expressed as group membership
over the same entities. Each record carries the `ETLPackage` that loaded it,
so provenance reaches the row level.

**Two build branches.** The knowledge splits sharply by size and by
structure, and the build treats the two parts differently:

- The **core** — genes, proteins, pathways, diseases, GO, chemicals and their
  relationships — is about 7 million rows. Integrating it requires
  transactions, because resolving an entity means asking what earlier sources
  have already created: an NCBI gene must be matched to the HGNC gene that
  already exists, not duplicated. This branch runs through the ORM on a
  throwaway SQLite database that exists only for the duration of the build
  (0.9 GB).
- The **variant branch** is billions of rows with no entity identifiers and no
  foreign keys: a variant is identified by its natural key
  (`chromosome:position:ref:alt`), and its links to genes are the gene
  identifiers VEP emits. There is nothing to resolve, so nothing is staged:
  the branch streams source files in batches and writes final Parquet
  directly, partitioned by chromosome.

An assembly step writes the core tables out as Parquet beside the variant
partitions and produces the manifest that makes the directory a bundle.

---

## 4. The bundle: knowledge as a versioned set of Parquet files

### 4.1 What a bundle is

```
bundle/
├── manifest.json        every table, its files, rows, bytes, build branch, bundle_id
├── bundle_plan.json     which sources were included, at which versions
├── build_record.json    per-step timings, source URLs, content hashes
└── tables/
    ├── entities.parquet, entity_aliases.parquet, gene_masters.parquet, ...
    └── variant_masters/variant_masters_chr1.parquet ... chr24.parquet
        variant_molecular_effects/ ...   variant_rsid/ ...
        variant_alphamissense/ ...       variant_gtex/ ...
```

Apache Parquet is a columnar, compressed, open file format readable by most
data tools (DuckDB, Apache Arrow, pandas, Polars, R `arrow`, Spark). DuckDB is
an embedded analytical SQL engine that queries Parquet files in place. When a
bundle is opened, Biofilter reads the manifest, verifies that every declared
file is present at its recorded size, starts DuckDB inside the calling
process and registers one SQL view per logical table — a chromosome-partitioned
table such as `variant_masters` becomes a single queryable name over its 24
files. No server, port, connection pool or import is involved.

### 4.2 Why a file artifact rather than a database

Biofilter 4.0–4.2 kept PostgreSQL as the canonical store and exported Parquet
from it. Operating that design at genome scale produced the measurements that
led to the current one:

- **Storage.** VEP consequence records cost 255 bytes per row in PostgreSQL
  and 4 bytes per row in Parquet — a 64-fold difference. Extrapolated to the
  full genome, that single table would occupy roughly 426 GB in PostgreSQL;
  the complete bundle, all tables included, is 26.7 GB.
- **Deployment.** Moving a relational database to an HPC cluster meant an
  import phase. Restoring the 1.79-billion-row consequence table under
  foreign-key enforcement ran at about 17 million rows per hour; repeated
  12–24-hour cluster jobs ended without completing. With a bundle, deployment
  is a file copy and queries can start as soon as the files are visible
  (on the order of a day reduced to minutes).
- **Concurrency.** A PostgreSQL data directory admits one server process, so
  on a shared cluster each user needed a private copy of several hundred
  gigabytes. Any number of DuckDB processes read the same Parquet files
  concurrently without coordination, so one bundle on shared storage serves
  a whole group.
- **Materialisation.** Returning results as Arrow columnar buffers rather than
  as Python row objects reduced the time to hand back 70 million rows from
  442 s to 3.9 s (113-fold) and peak memory from 21.0 GB to 4.0 GB.
- **Footprint.** The container image no longer needs to carry a database
  server.

The relational database remains where it is genuinely needed — resolving
entities transactionally during the core build — and nowhere else.

### 4.3 Ready for fast access at scale

The integration of large resources is front-loaded into the build, so that the
artifact users receive is already in the shape queries need:

- **Columnar layout and compression** mean a query reads only the columns it
  names, and DuckDB skips row groups whose min/max statistics exclude the
  filter.
- **Chromosome partitioning** of the variant tables lets queries restricted to
  a region open only the relevant files.
- **Natural variant keys** (`chr:pos:ref:alt`) remove the need to translate
  user input into internal identifiers, and an rsID map (712 million rows)
  resolves dbSNP identifiers.
- **Streaming throughout the build.** Variant transforms process 250,000 rows
  at a time with one writer per chromosome, so memory does not grow with
  chromosome size; raw downloads are discarded as soon as the Parquet that
  supersedes them exists.

The full human build (Table 2) downloaded and processed about 1.57 TB of raw
data, peaked at about 70 GB of disk, ran in about 49 hours of step time on a
36 GB-memory workstation, and produced a bundle of roughly 21–27 GB depending
on the sources folded in. On the analyst's side, a production-scale question —
which of 711,836 study variants map to a protein-coding gene, joining against
177.5 million variants and 2.2 billion VEP records — ran end to end in
**11.9 seconds with 1.87 GB peak memory on a laptop** (Section 9).

### 4.4 Immutability, identity and reproducibility

A bundle is never updated in place. When sources publish new releases, a new
bundle is built and the old one stays readable for anyone reproducing work
against it. This is a deliberate response to a property of public knowledge
resources: **a bundle cannot be rebuilt.** Upstream files are replaced as
sources release new versions, so running the same build plan a year later
yields different data. The plan and the build record therefore travel inside
the bundle as the surviving account of what it contains.

Each bundle carries a `bundle_id` derived from its content, so it can be
recomputed to confirm a bundle has not changed. Identity matters for a
specific, easily missed reason: internal entity identifiers are assigned in
build order and are not stable across builds, and the drift is small enough
to be dangerous. In one comparison, the identifier of APOE in one build
resolved to APOF in another, BRCA1 to BRCC3P1, and TP53 to TP53TG3B — without
any error. Biofilter's answer is to make the bundle, not the identifier, the
unit that is pinned and cited.

---

## 5. Getting data without running the ingestion

Most users never build a bundle. Because the bundle is the complete,
processed product, obtaining one is a file transfer: a group's maintainer
builds it, places it on shared storage (at Penn, the LPC cluster) or hands it
over as a directory, and every analyst points Biofilter at that directory.
There is no setup step between receiving a bundle and running the first
query, and no requirement for the 150 GB of working space, two days of build
time or network bandwidth the build itself needs. **[confirm: whether a
public download location for released bundles will be announced with the
paper.]**

The build also supports incremental work for maintainers:

- **Resumable, hash-aware steps.** Each source runs `extract → transform →
  load`, and a step is skipped when its input hash is unchanged and its
  output is present, so an interrupted build resumes where it stopped.
- **Folding sources into an existing bundle** (`bundle build --merge-into`).
  A maintainer can add sources — further chromosomes, an additional QTL
  resource — to a bundle without rebuilding it. The fold refuses to replace
  a file the bundle already holds, re-derives the `bundle_id`, and records
  the before and after identifiers in the build record so the lineage stays
  readable.
- **Subset bundles** — selected sources or chromosomes — for development and
  teaching.

---

## 6. Extensibility

### 6.1 User-defined data sources (DTPs)

Every source enters Biofilter through a **Data Transformation Package (DTP)**:
a Python module exposing three methods with a fixed contract —
`extract()` (download raw files and return a content hash), `transform()`
(write normalised Parquet), and `load()` (integrate into the model). A shared
base class supplies compatibility checks against the schema version, standard
raw and processed directory layouts, and status reporting. The orchestrator
(`ETLManager`) handles ordering, hashing and skip logic, tracking, rollback
and resume, so a DTP author writes only source-specific parsing.

The release ships 21 DTPs; those in the current bundle cover 14 source systems
(Table 1). A DTP can be
parameterised over several datasets of the same source — the gnomAD DTPs run
once per chromosome — so large sources are built, resumed and replaced in
independent pieces. A new source is a new module,
a short explanatory guide, and a registration entry; it then participates in
planning, building, provenance and reporting like the built-in ones. Sources
that are not suitable for redistribution can be integrated in a local build
without being part of a shared bundle.

### 6.2 Reports: ready-made queries on an open framework

Users query a bundle through **reports**. A report takes a list of things the
user has — gene symbols, rsIDs, positions, pathway names, a VCF or PLINK
cohort — and returns a table. Seventeen reports ship in six families
(Table 3): `resolve_*` (which input names are recognised), `annotate_*`
(what is known about them), `expand_*` (what they connect to), `pair_*`
(which of them share biology, and on what evidence), `aggregate_*` (cohort
roll-ups) and `platform_*` (what a bundle contains and how it was built). The
`pair_*` family is the direct descendant of Biofilter's original model
generation.

Reports are built on a small framework intended for users to extend:

- A report is one Python class declaring a name, a description, the bundle
  tables it **requires** and those it uses **optionally**, and a `run()`
  method that issues SQL and returns an Arrow table.
- User input is registered as a relation and joined against, never
  interpolated into query text. This closes off injection and turns a filter
  over tens of thousands of input values into a hash join.
- Reports are discovered by being present in the package: no registration,
  and no command-line changes. Parameters pass through generically
  (`--param KEY=VALUE`), so the same report is available from the CLI, from
  Python, inside the container and on a cluster.
- Missing optional tables are recorded in the result rather than silently
  producing empty columns (Section 7).

A report ships as four artifacts that are reviewed together: the module, an
explanatory guide (printed by `biofilter report explain`), a worked notebook,
and unit tests against a small fixture bundle. That package is what a user
contributes when proposing a report for inclusion in a future release; a
template notebook and a developer guide describe the contract. The ADSP
analysis illustrates the cycle: the existing `pair_variants` report assigns
variants to genes by genomic position, which is correct for coding variants
but would have discarded regulatory variants selected for their eQTL target
gene. A new report, `pair_genes`, accepting a caller-supplied variant–gene
assignment, was written for that analysis and released with the platform.

For questions no report covers, the bundle remains an open dataset: the
Python API exposes a DuckDB cursor over the registered views, and the Parquet
files can be read directly by any Arrow-compatible tool.

---

## 7. Provenance and honest results

Every report returns a result object rather than a bare table. Alongside the
rows it carries provenance: the `bundle_id`, Biofilter version, report name,
parameters, any decisions the report made that its parameters do not show,
and warnings about what it had to cope with. Exporting a result writes a
`.provenance.json` file beside the CSV or Parquet output, and a result can be
saved and reloaded in full.

Two further mechanisms address the ways a knowledge query can mislead:

- **Coverage.** A column that is entirely null may mean "no knowledge" or "the
  source was never built into this bundle". Reports declare the optional
  tables they read, and the absence of any of them is recorded in the result's
  coverage, which is the only place the two cases can be told apart.
- **Explicit status.** Input that did not resolve (`not_found`) is
  distinguished from input that resolved and matched nothing (a real
  negative).

---

## 8. Interfaces and deployment

- **Command line** (`biofilter report run --report-name … --input …`) for
  scripted and batch use.
- **Python API** (`Biofilter(bundle=…)`) for notebooks, returning results
  convertible to pandas or Arrow. One tutorial notebook accompanies each
  report.
- **Container.** A single image (`ghcr.io/ritchielab/biofilter`) runs under
  Docker and Apptainer/Singularity. It carries no data: the bundle is mounted
  read-only and a separate writable directory receives output, so one image
  serves any bundle.
- **HPC.** On the Penn LPC cluster, a bundle on project storage is shared by
  all users concurrently; no per-user installation of data is required.
- **Conversational assistant.** A GPT-based assistant, grounded in the report
  guides and documentation, helps users choose and parameterise reports.
- **Software.** Python 3.10–3.13, MIT license, tested across versions;
  documentation at https://biofilter.readthedocs.io.

---

## 9. Bridge to the ADSP application

*(Short by design; the analysis itself belongs to the main paper.)*

The ADSP interaction analysis was run entirely against a single bundle
(`9b8419b48be5004e`) and expressed as five report calls — `annotate_variant`,
`annotate_gene`, `expand_variant_regulatory`, `annotate_gene` and
`pair_genes` — with no hand-written SQL and no database server. Starting from
711,836 ADSP variants, the steps selected variants mapped to protein-coding
genes, partitioned them by functional evidence (protein-altering consequence,
or concordant brain eQTL and pQTL on the same gene), and paired the selected
variants through shared Reactome and KEGG pathways, yielding 817,420
candidate variant-pair models (91,135 when both curations must agree). Every
intermediate file carries a provenance record naming the bundle, reports and
parameters. Re-running the first step of the 4.2-era analysis against the new
release reproduced its result to within 602 variants (0.17%): 355,644 against
355,042.

---

## Tables and figures suggested

**Figure 1 — Architecture.** Sources → DTPs (extract / transform / load) →
two branches (core via staging SQLite; variants streamed to Parquet) →
assembly → bundle (manifest, plan, build record, tables) → shared storage →
DuckDB in-process → reports / Python / CLI / container → result with
provenance.

**Figure 2 — Footprint comparison** (optional). Bytes per row and projected
size, PostgreSQL vs Parquet, for the VEP consequence table.

**Table 1 — Integrated sources.**

| Branch | Source systems |
| --- | --- |
| Core (11) | HGNC, NCBI Gene, Ensembl, UniProt, Reactome, KEGG, Gene Ontology, EBI (Pfam), MONDO, BioGRID, ClinGen; ChEBI has a DTP but was not loaded into this bundle |
| Variant (4) | gnomAD v4 (joint callset and VEP annotations), AlphaMissense, GTEx v10 eQTL, EBI (GWAS Catalog) |

14 distinct source systems, counted from the packages loaded into the bundle (`etl_packages`); EBI appears in both branches.

**[confirm]** source versions/releases from `bundle_plan.json` before
submission, and licensing wording for redistributed sources.

**Table 2 — The current bundle (`9b8419b48be5004e`).**

| | |
| --- | ---: |
| Total rows | 3,444,552,850 |
| Size on disk | 26.7 GB |
| Parquet files | 175 |
| Entities / aliases / relationships | 203,393 / 912,316 / 4,210,591 |
| Variants (`variant_masters`) | 177,520,333 |
| VEP consequence records | 2,279,908,260 |
| rsID mappings | 712,358,067 |
| AlphaMissense predictions | 71,697,556 |
| GTEx eQTL associations | 18,470,502 |
| GWAS Catalog associations | 1,208,545 |

Build cost (full-genome build of 2026-09-12): ~1.57 TB downloaded and
discarded; ~70 GB peak disk; 49.1 h of step time (extract 6.5 h,
transform 40.3 h, load 2.3 h); 36 GB workstation, 32 GB recommended.

**Table 3 — Report catalogue** (six families, 17 reports), from
`biofilter report list --verbose`.

---

## Notes for Diane

**Where each number comes from** (all in this repository):

| Claim | Source |
| --- | --- |
| Bundle rows, size, file count, per-table rows | `biofilter_data/bundles/20260914/manifest.json` |
| Source systems | `biofilter_data/bundles/20260914/bundle_plan.json` (initial chr22 run) plus the `merges` entries in `build_record.json` (chromosomes folded in afterwards) |
| Build disk, time, memory | `docs/source/technical/bundle_requirements.md` (build of 2026-09-12, 21 GB before later folds) |
| 255 vs 4 bytes/row; 426 GB projection; ID drift examples | `adr/0003-parquet-native-build-pipeline.md` §1.1, §1.6 |
| PostgreSQL/SQLite import failures; concurrency; image size | `adr/0001-duckdb-parquet-strategy.md` §1 |
| 11.9 s / 1.87 GB; 113× materialisation | `adr/0004-parquet-native-report-module.md` §5 (bundle 20260910, macOS, 6 threads) |
| ADSP funnel and report calls | `notebooks/Andre/adsp/README.md` |

**Things to decide or check:**

1. **Biofilter 3** — confirm how it should be named and cited, or drop it and
   go from 2.0 to 4.
2. **Public bundle availability** — Section 5 currently describes sharing
   within a group; state a public location if one will exist.
3. **Performance claims** — the speedups in §4.2 are measured for specific
   steps (materialisation, one production query), not across every report.
   Resolution and platform reports were not faster after the redesign; please
   keep claims scoped to what was measured.
4. **The 4.0–4.2 history** (§4.2) is framed as design rationale. It can be
   cut to a sentence if the paper does not need to justify the storage choice.

## References

**[confirm all citation details]**

1. Bush WS, Dudek SM, Ritchie MD. Biofilter: a knowledge-integration system
   for the multi-locus analysis of genome-wide association studies. *Pac Symp
   Biocomput.* 2009:368–379.
2. Pendergrass SA, Frase A, Wallace J, et al. Genomic analyses with biofilter
   2.0: knowledge driven filtering, annotation, and model development.
   *BioData Min.* 2013;6:25.
3. Raasveldt M, Mühleisen H. DuckDB: an embeddable analytical database.
   *SIGMOD* 2019.
4. Apache Parquet. https://parquet.apache.org
5. Chen S, et al. A genomic mutational constraint map using variation in
   76,156 human genomes (gnomAD). *Nature* 2024.
6. McLaren W, et al. The Ensembl Variant Effect Predictor. *Genome Biol.*
   2016;17:122.
7. Cheng J, et al. Accurate proteome-wide missense variant effect prediction
   with AlphaMissense. *Science* 2023.
8. GTEx Consortium. The GTEx Consortium atlas of genetic regulatory effects
   across human tissues. *Science* 2020.
9. Source-specific citations for HGNC, NCBI Gene, Ensembl, UniProt, Reactome,
   KEGG, GO, Pfam, MONDO, BioGRID, ClinGen and the GWAS Catalog.

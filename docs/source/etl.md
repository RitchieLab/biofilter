# ETL Operations

ETL is how Biofilter ingests, normalizes and versions knowledge from
external sources. Each source is a **data source**, driven by a **DTP**
(Data Transformation Package) through three steps: `extract`,
`transform`, `load`.

In 4.3 the ETL is a step inside a bundle build rather than an end in
itself. `bundle build` runs it for every source in a plan; the commands
here drive it directly, which is what you want when developing a DTP or
re-running one source. See [Building Bundles](building_bundles.md).

## Two branches

Sources fall into two groups, and they behave differently:

**Core** — genes, proteins, pathways, diseases, GO, chemicals and the
relationships between them. These DTPs resolve entities against each
other, so they run in order and load into a relational store: the
throwaway SQLite during a build, or whatever database you point them at
directly.

**Variant** — gnomAD, AlphaMissense, GTEx, GWAS. These write parquet
directly and never load into a database. Their `load()` raises
`NotImplementedError` by design, so run them with explicit steps:

```bash
biofilter etl update --data-source gnomad_joint_chr21 --run-step extract
biofilter etl update --data-source gnomad_joint_chr21 --run-step transform
```

Variant tables link to genes by natural key — `HGNC_ID`, gene symbols —
never by an entity id, which is what lets the two branches be built
independently.

## Commands

```bash
biofilter etl update --data-source hgnc
biofilter etl update --source-system KEGG
biofilter etl update-all
biofilter etl status
biofilter etl explain --data-source hgnc
```

Restrict or force individual steps:

```bash
biofilter etl update --data-source hgnc --run-step transform
biofilter etl update --data-source hgnc --force-step transform
```

A step is skipped when its input hash is unchanged **and** the output it
produced still exists. Deleting a processed file causes it to be rebuilt.

`etl update` exits non-zero when a source fails.

## Field and tissue selection

The variant DTPs read a JSON config next to them in
`biofilter/modules/etl/dtps/config/`, listing every field a source
publishes with a `load` flag. They are include-lists: gnomAD's joint
callset alone carries 664 INFO fields, so an exclude-list would silently
adopt whatever a future release adds.

The same mechanism selects GTEx tissues — all 50 are listed, 13 enabled
by default. Note that GTEx ships every tissue in one tarball and does not
expose them individually, so the selection narrows the transform and the
output, not the download.

Frequency filters live in the same files. The gnomAD joint config
defaults to `min_ac: 5`; setting it lower keeps rarer variants at
proportionally larger output.

## Adding a DTP

1. `biofilter/modules/etl/dtps/dtp_<name>.py` with `extract()`,
   `transform()` and, for a core source, `load()`
2. `biofilter/modules/etl/dtps_explain/dtp_<name>.md` — source, behaviour,
   caveats
3. Register the data source in the seed
4. Test with `biofilter etl update --data-source <name>`

See [Developer Extensions](developer_extensions.md).

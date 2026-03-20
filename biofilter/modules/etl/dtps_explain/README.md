# DTP Explain

This folder stores technical markdown documentation for each DTP module.

Goal:
- keep `docs/source/etl.md` generic for end users
- keep implementation-level ETL details close to code
- document filters and load behavior with reproducible detail

Naming convention:
- one file per DTP script
- file name: `<dtp_script>.md`
- example: `dtp_gene_hgnc.md`

CLI usage:
- `biofilter etl explain --dtp-script dtp_gene_hgnc`
- `biofilter etl explain --data-source hgnc`
- `biofilter etl explain` (lists available explain docs)

Recommended sections in each file:
- data source and pipeline role
- extract (source, downloaded files, hash behavior)
- transform (input/output and filters)
- load (target models/tables, matching keys, filters, upsert rules)
- practical notes and known caveats

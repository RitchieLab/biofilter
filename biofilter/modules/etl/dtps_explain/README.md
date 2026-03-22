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

Available explain docs:
- [`dtp_biogrid.md`](dtp_biogrid.md)
- [`dtp_chebi.md`](dtp_chebi.md)
- [`dtp_clingen.md`](dtp_clingen.md)
- [`dtp_gene_ensembl.md`](dtp_gene_ensembl.md)
- [`dtp_gene_hgnc.md`](dtp_gene_hgnc.md)
- [`dtp_gene_ncbi.md`](dtp_gene_ncbi.md)
- [`dtp_gwas.md`](dtp_gwas.md)
- [`dtp_go.md`](dtp_go.md)
- [`dtp_kegg.md`](dtp_kegg.md)
- [`dtp_mondo.md`](dtp_mondo.md)
- [`dtp_mondo_relationships.md`](dtp_mondo_relationships.md)
- [`dtp_pfam.md`](dtp_pfam.md)
- [`dtp_reactome.md`](dtp_reactome.md)
- [`dtp_reactome_relationships.md`](dtp_reactome_relationships.md)
- [`dtp_uniprot.md`](dtp_uniprot.md)
- [`dtp_uniprot_relationships.md`](dtp_uniprot_relationships.md)
- [`dtp_variant_gnomad.md`](dtp_variant_gnomad.md)

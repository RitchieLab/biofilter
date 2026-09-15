# expand_variant_regulatory

Which genes a variant regulates, in which tissue, with what effect.

```bash
biofilter report run --report-name expand_variant_regulatory \
    --input APOE --input rs429358 \
    --param p_value_max=1e-8 \
    --output regulatory.csv
```

> Called `annotation_variant_regulatory_evidence` before 4.3.0. Renamed
> because one row is a link from the input to something reached —
> expansion, not annotation.

## Why this is not `annotate_variant`

`annotate_variant` reports the gene a variant sits **in**, from VEP. This
reports the gene it **regulates**, from eQTL. They are usually different
genes, and the difference is the point.

Measured on chr22 of the current bundle:

- 6,919,646 variant × gene pairs carry both kinds of evidence
- **88.5% name a different gene** — the variant is in one gene and
  regulates another
- **31,080 variants with regulatory evidence** are classed by VEP as
  `intergenic`, `upstream` or `downstream` — outside any gene at all

For those last ones `annotate_variant` says "not in a gene" while the
eQTL says "regulates this gene, in this tissue, with this effect".

## Input

Three shapes, mixed freely in one list:

| shape | example | means |
| --- | --- | --- |
| gene symbol | `APOE` | every variant inside that gene's range |
| rsID | `rs429358` | that variant |
| position | `19:44908684` or `19:44908684:T:C` | that position, or that exact variant |

Anything that is not an rsID or a position is read as a gene name.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | genes, rsIDs, positions |
| `tissues` | none | keep only these `bio_context` values |
| `qtl_type` | none | `eQTL`, `sQTL`, … |
| `p_value_max` | none | keep only evidence at least this significant |
| `flanking_bp` | `0` | widen a gene's range, for gene inputs |
| `emit_not_found_rows` | `true` | keep inputs with no evidence |

## Columns

| column | meaning |
| --- | --- |
| `input_value`, `input_kind`, `status`, `note` | what was asked, and what happened |
| `variant_key`, `rsid`, `chromosome`, `position`, alleles | the variant |
| `position_gene_symbol`, `position_gene_id` | the gene whose **body contains** it |
| `regulated_gene_id`, `regulated_gene_symbol` | the gene it **regulates** |
| `bio_context` | the tissue |
| `qtl_type` | `eQTL`, `sQTL` |
| `beta`, `se`, `p_value`, `n`, `effect_allele` | the association |

## Reading the result

**Which tissues a bundle has is a build decision, not a property of this
report.** GTEx ships 50; the DTP config carries a load flag per tissue,
and the current build loads 13 — all brain. Nothing here is hardcoded to
that: ask for the tissues you want, or look at what came back. The result
covers whatever the bundle was built with.

⚠️ So absence of evidence in this report is absence **in the tissues this
bundle carries**. Check `bio_context` before reading a null as biology.

**`regulated_gene_symbol` can be null while the evidence is real.** GTEx
names its target by Ensembl gene id, and roughly 17% of those on chr22
have no BF4 entity — lncRNAs and pseudogenes without HGNC symbols.
`regulated_gene_id` is always there; only the symbol is missing.

**`position_gene_symbol` can be null too**, when the variant falls
outside every gene body. That is common and expected — it is exactly the
case this report exists for.

**Gene input means "variants inside this gene's range"**, resolved from
build 38 coordinates in `entity_locations`. `flanking_bp` widens it for
promoter and downstream regions.

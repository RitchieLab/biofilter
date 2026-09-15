# expand_gene_to_variant

The variants belonging to a list of genes, with their annotation.

```bash
biofilter report run --report-name expand_gene_to_variant \
    --input CHEK2 --input SMARCB1 \
    --param mapping=position \
    --param window_bp=5000 \
    --param af_max=0.01 \
    --output gene_variants.csv
```

> Replaces `gene_to_variant_filtering`, and retires
> `variant_annotation_expanded` with it. That second report was this one
> with no filters and a gene list scraped out of another report's CSV by
> column name; chaining reports is now the caller's job, done with files
> they can see.

## You have to choose what "belongs to" means

This is the one parameter with no safe default, so the report asks for a
decision rather than making one quietly.

| `mapping` | a variant belongs to a gene when… |
| --- | --- |
| `position` | its coordinate falls inside the gene's build-38 range |
| `annotation` | VEP associated it with that gene |

They are not the same question and they do not return the same variants.
Measured across all **958 chr22 genes** in the current bundle that have
build-38 coordinates and annotated variants:

- **2,045,943** gene-variant pairs by position
- **2,651,135** by annotation
- **144,488** pairs exist *only* by position — 139 genes have at least one
- roughly **749,680** exist *only* by annotation

Position finds variants that VEP attributed to a neighbouring gene, or to
no gene at all. Annotation finds variants outside the gene body — VEP
reaches about 5 kb beyond it — and follows VEP's gene model rather than
the coordinates BF4 stores.

Because a single gene often shows one mapping as a clean subset of the
other, you cannot tell from the rows which question was asked. So the
report records it in two places: a `mapping` column on every row, and the
`mapping` block of the provenance JSON.

```json
"mapping": {
  "mechanism": "position",
  "build": 38,
  "window_bp": 5000,
  "means": "variant coordinate inside the gene's range"
}
```

`window_bp` widens the range in both directions and applies to
`position` only. Passing it with `mapping=annotation` is an error rather
than a no-op: VEP's association already reaches beyond the gene body, and
silently ignoring the window would let you believe it had applied.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | gene symbols, HGNC or Ensembl ids — anything `resolve_entity` accepts |
| `mapping` | `position` | `position` or `annotation`; see above |
| `build` | `38` | genome build, for `position` |
| `window_bp` | `0` | widen the gene's range both ways, for `position` |
| `most_severe_only` | `true` | one row per gene and variant, keeping the worst consequence |
| `max_variants_per_gene` | `5000` | cap per gene; `0` means no cap |
| `emit_not_found_rows` | `true` | keep inputs that produced nothing |
| `af_min`, `af_max` | none | joint allele frequency bounds |
| `impact_filter` | none | `HIGH`, `MODERATE`, `LOW`, `MODIFIER` |
| `consequence_type_filter` | none | VEP consequence names |
| `lof_confidence_filter` | none | LOFTEE `HC`, `LC` |
| `cadd_phred_min` | none | |
| `sift_score_max` | none | |
| `polyphen_score_min` | none | |
| `alphamissense_score_min` | none | |
| `alphamissense_classification` | none | `likely_pathogenic`, `likely_benign`, `ambiguous` |

## The cap is real, and it says so

`max_variants_per_gene` defaults to 5000 because a single gene on a whole
-genome bundle can carry hundreds of thousands of variants. A capped gene
looks exactly like a complete answer — a round number of rows, and
nothing in them admitting more existed.

So the provenance says what was hidden, per gene:

```json
"truncation": {
  "max_variants_per_gene": 200,
  "applied": true,
  "genes": {
    "CHEK2":   {"returned": 200, "available": 3796},
    "SMARCB1": {"returned": 200, "available": 4260}
  },
  "means": "These genes had more variants than the cap allowed; …"
}
```

The rows kept are the most severe first, then the most common. Pass
`max_variants_per_gene=0` for no cap at all.

## Columns

| column | meaning |
| --- | --- |
| `input_gene` | what you asked for, verbatim |
| `gene_symbol`, `gene_entity_id` | what it resolved to |
| `mapping` | `position` or `annotation` — which question this row answers |
| `status`, `note` | `ok`, `not_found`, `no_location`, `no_variants` |
| `variant_key`, `rsid`, `chromosome`, `position`, alleles | the variant |
| `af_joint`, `ac_joint` | gnomAD joint frequency and count |
| `transcript_id`, `consequence`, `consequence_group`, `severity_rank` | the annotation |
| `impact`, `canonical`, `mane_select`, `lof` | VEP flags |
| `cadd_phred`, `revel_max`, `sift_max`, `polyphen_max` | in-silico predictors |
| `alphamissense_score`, `alphamissense_classification` | AlphaMissense |
| `variants_available` | the gene's pre-cap total, so a capped row admits it |

## Reading the result

**Four statuses, and the difference between two of them matters.**

| status | means |
| --- | --- |
| `ok` | a variant |
| `not_found` | the input did not resolve to a gene in this bundle |
| `no_location` | the gene resolved, but the bundle has no build-38 coordinates for it, so `position` cannot place it — try `annotation` |
| `no_variants` | the gene resolved and, under the mapping you chose, nothing met the criteria |

`no_location` exists because a gene the bundle cannot place is not a gene
without variants, and under `position` mapping the two are otherwise
indistinguishable. This is not a rare corner: **33,354 of the 72,660
genes in the current bundle — 45.9% — carry no build-38 coordinates**, so
`position` mapping cannot place almost half the gene catalogue. For those
genes `annotation` is the only mapping that answers anything.

**`most_severe_only` changes what a row counts.** With it on (the
default) one row is a gene-variant pair, and the consequence shown is the
worst across transcripts. With it off one row is a gene-variant-transcript
triple, and counting rows counts transcripts.

**A missing predictor is a missing column, not a zero.** If the bundle
carries no `variant_predictions` or `variant_alphamissense` table, those
columns come back null for every row and the omission is recorded in the
provenance `coverage` block. Check it before concluding that nothing
scored.

**AlphaMissense needs two corrections to join at all, and both failures
are silent nulls.**

*The version.* AlphaMissense writes `ENST00000327374.9`; VEP writes
`ENST00000327374`. Joining the raw strings matches nothing. The report
strips the version on both sides.

*The transcript.* AlphaMissense scores a variant **on a transcript**, and
the transcript it picked is rarely the one VEP calls most severe.
Requiring the two to agree threw away 97% of the scores this bundle
holds: of 5,917 CHEK2 missense annotations, only 222 matched on
transcript, and after collapsing to one row per variant just 6 survived —
while 249 of the gene's 308 missense variants have a score somewhere.

So the join key follows the grain of the row:

| `most_severe_only` | a row is | `alphamissense_score` is |
| --- | --- | --- |
| `true` (default) | one variant | the highest score recorded for that variant |
| `false` | one transcript | that transcript's score, or null |

The provenance `alphamissense` block says which applied. Under the
default, all 174 rare damaging missense variants in CHEK2 come back
scored instead of 6.

## Chaining

There is none, by design. To feed these variants into another report,
write the CSV and read the column you want:

```python
genes = bf.report.run("expand_gene_to_variant",
                      input_data=["CHEK2"], mapping="position")
variants = genes.to_pandas().query("status == 'ok'").variant_key.tolist()

bf.report.run("expand_variant_regulatory", input_data=variants)
```

The intermediate list is a thing you can look at, which is the point.

# annotate_variant

What the bundle knows about a list of variants.

```bash
biofilter report run --report-name annotate_variant \
    --input rs1225039379 --input 22:20052518:C:T \
    --param most_severe_only=true \
    --output variants.csv
```

> Called `annotation_master_variant` before 4.3.0. The version it
> replaces could not run against a 4.3.0 bundle at all — it selected
> `variant_masters.variant_id`, `position_start` and the predictor
> columns, none of which the current schema has.

## Input

Three shapes, mixed freely:

| shape | example | matches |
| --- | --- | --- |
| rsID | `rs1225039379` | the variant that rsID maps to |
| chr:pos | `22:15238761` | **every** variant at that position |
| chr:pos:ref:alt | `22:20052518:C:T` | exactly that variant |

`chr`, `CHR`, `chromosome` prefixes and `:`, `-`, `_`, space separators
are all accepted. `X`, `Y` and `MT` map to 23, 24 and 25.

## One row per transcript

A variant is annotated against every transcript it overlaps — often
dozens. `22:20052518:C:T` returns **89 rows**, one per transcript, with
the variant-level facts (frequencies, CADD, REVEL) repeated on each.

Two ways to narrow it:

| parameter | keeps |
| --- | --- |
| `most_severe_only` | the transcript with the worst consequence |
| `canonical_only` | transcripts VEP marked canonical |

They are different questions and can disagree — the most severe
consequence is not always on the canonical transcript.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `input_data` | required | rsIDs, positions, or full variant ids |
| `most_severe_only` | `false` | one row per variant, worst consequence |
| `canonical_only` | `false` | canonical transcripts only |
| `emit_not_found_rows` | `true` | keep inputs that matched nothing |

## Reading the result

**`status` tells you what happened to the input.**

| value | meaning |
| --- | --- |
| `ok` | matched a variant |
| `not_found` | parsed fine, no such variant in this bundle |
| `invalid` | could not be parsed; `note` says what was expected |

**`is_most_severe_for_variant` is derived, not stored.** 4.3.0 dropped
that flag from the schema, so it is computed from
`variant_consequences.severity_rank` — which keeps it consistent with
whatever severity ordering the bundle actually carries, rather than with
whatever the ETL believed when it wrote the row.

**A bundle built for a subset of chromosomes returns `not_found` for
everything outside it.** That is true of the bundle, not of the genome.
`platform_data_statistics` shows which chromosomes are present.

**rsIDs come from `variant_rsid`, not from `variant_masters.rsid`.** That
column exists and is entirely null in 4.3.0 bundles — 0 of 2,889,803 rows
in the chr22 build. Roughly 97% of variants have an rsID in the mapping
table.

**AlphaMissense scores one transcript per variant.** Of 89 transcripts
for `22:20052518:C:T`, one carries a score. That is AlphaMissense's own
scope, not a join failure — it predicts on the canonical protein
sequence.

## Column groups

| group | columns |
| --- | --- |
| input | `input_value`, `input_kind`, `status`, `note` |
| identity | `variant_key`, `rsid`, `chromosome`, `position`, alleles, `quality_filter` |
| frequency | `ac_joint`, `an_joint`, `af_joint`, `nhomalt_joint`, `grpmax_joint`, `af_grpmax_joint` |
| prediction | `cadd_phred`, `cadd_raw_score`, `revel_max`, `sift_max`, `polyphen_max`, `spliceai_ds_max`, `pangolin_largest_ds`, `phylop` |
| molecular effect | gene, transcript, `consequence` and its group/category/rank, `impact`, `canonical`, `mane_select`, HGVS, `lof` |
| AlphaMissense | `alphamissense_score`, `alphamissense_classification` |

Frequencies are the gnomAD **joint** callset. The exomes and genomes
columns exist in `variant_masters` and are not surfaced here; ask for
them directly if you need the split.

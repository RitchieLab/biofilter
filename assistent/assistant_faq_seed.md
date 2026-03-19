# BF4 FAQ Seed

Use this file to bootstrap common support answers.

## 1) How do I see available commands?

Run:

```bash
biofilter --help
biofilter db --help
biofilter etl --help
biofilter report --help
```

## 2) How do I initialize database schema?

Typical flow:

```bash
biofilter db migrate --target head
biofilter db upgrade
biofilter db migrate --status
```

## 3) How do I update one data source only?

```bash
biofilter etl update --data-source hgnc
```

## 4) How do I run all pending data sources?

```bash
biofilter etl update-all
```

## 5) How do I process only one source system in update-all?

```bash
biofilter etl update-all --source-system NCBI
```

## 6) How do I delete raw/processed files after successful update-all?

```bash
biofilter etl update-all --drop-files
```

Use `--keep-files` to preserve files.

## 7) How do I check ETL status quickly?

```bash
biofilter etl status
```

Filter examples:

```bash
biofilter etl status --source-system NCBI --only-active
biofilter etl status --data-source hgnc
```

## 8) What does "No source_system or data_sources provided" mean?

`etl update` requires at least one target:

- `--source-system`
- or `--data-source`

If you want batch resumable behavior without explicit targets, use:

```bash
biofilter etl update-all
```

## 9) How do I list all reports?

```bash
biofilter report list
biofilter report list --verbose
```

## 10) How do I run a report and export CSV?

```bash
biofilter report run --report-name etl_packages --output ./etl_packages.csv
```

## 11) How do I inspect report parameters?

```bash
biofilter report run --report-name entity_relationship_model --params-template
```

## 12) How do I pass inputs directly in CLI?

```bash
biofilter report run --report-name entity_filter --input TP53 --input BRCA1
```

## 13) How do I pass input from a file?

```bash
biofilter report run --report-name entity_filter --input-file ./entities.csv --input-column symbol
```

## 14) How do I pass custom options to reports?

```bash
biofilter report run --report-name entity_relationship_model \
  --input TP53 --input BRCA1 \
  --param relationship_scope=input_to_any \
  --param deduplicate_pairs=true
```

## 15) Can I pass params in JSON/YAML?

Yes:

```bash
biofilter report run --report-name entity_relationship_model --params-json '{"relationship_scope":"input_to_any"}'
biofilter report run --report-name entity_relationship_model --params-file ./params.yaml
```

## 16) What if report name is wrong?

Use:

```bash
biofilter report list --verbose
```

Then run with exact `--report-name`.

## 17) How do I explain a report?

```bash
biofilter report explain --report-name etl_status
```

## 18) How do I check report output columns?

```bash
biofilter report available-columns --report-name entity_relationship_model
```

## 19) How do I rollback ETL data?

By package:

```bash
biofilter etl rollback --package-id 123
```

By datasource:

```bash
biofilter etl rollback --data-source gnomad_chr22 --delete-files
```

## 20) How do I restart datasource ETL from scratch?

```bash
biofilter etl restart --data-source gnomad_chr22 --delete-files
```

## 21) Where is report explain documentation stored?

Guides live in:

- `biofilter/modules/report/reports_explain/report_<module>.md`

## 22) How do I debug command behavior?

Add `--debug`:

```bash
biofilter report run --report-name etl_status --debug
biofilter etl update --data-source hgnc --debug
```

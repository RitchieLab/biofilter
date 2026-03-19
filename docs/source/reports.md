# Reports

Reports are the main read interface over Biofilter knowledge and ETL provenance.

## Discover and Inspect

List reports:

```bash
biofilter report list
biofilter report list --verbose
```

Explain report:

```bash
biofilter report explain --report-name etl_status
```

Show example input:

```bash
biofilter report example-input --report-name entity_relationship_model
```

Show output columns:

```bash
biofilter report available-columns --report-name etl_packages
```

## Run Reports

Basic run:

```bash
biofilter report run --report-name etl_status
```

Export CSV:

```bash
biofilter report run --report-name etl_packages --output ./etl_packages.csv
```

Template-driven params:

```bash
biofilter report run --report-name entity_relationship_model --params-template
```

## Dynamic Parameter Injection

Inputs:

```bash
biofilter report run --report-name entity_filter --input BRCA1 --input TP53
biofilter report run --report-name entity_filter --input-file ./entities.csv --input-column symbol
```

Options:

```bash
biofilter report run --report-name entity_relationship_model \
  --input TP53 --input BRCA1 \
  --param relationship_scope=input_to_any \
  --param deduplicate_pairs=true
```

JSON/YAML params:

```bash
biofilter report run --report-name entity_relationship_model --params-json '{"relationship_scope":"input_to_any"}'
biofilter report run --report-name entity_relationship_model --params-file ./params.yaml
```

Load one param from file:

```bash
biofilter report run --report-name entity_relationship_model --input TP53 --param relationship_types=@./relationship_types.txt
```

## Explain Guides

`report explain` prefers markdown guides stored in:

- `biofilter/modules/report/reports_explain/report_<module>.md`

If a guide file is missing, Biofilter falls back to the report class `explain()` method.

This model keeps report documentation maintainable:
- update the report module when behavior changes
- update the paired explain markdown for user-facing guidance

# AG 01 - Report Module Overview

This guide explains how to discover and run reports in Biofilter.

## Discovery

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report list
biofilter --db-uri sqlite:///biofilter_dev.db report list --verbose
```

API:
```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db")
rows = bf.report.list()
print(rows)
```

## Introspection

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report explain --report-name etl_status
biofilter --db-uri sqlite:///biofilter_dev.db report available-columns --report-name etl_status
biofilter --db-uri sqlite:///biofilter_dev.db report example-input --report-name variant_molecular_effects
```

API:
```python
print(bf.report.explain("etl_status"))
print(bf.report.available_columns("etl_status"))
print(bf.report.example_input("variant_molecular_effects"))
```

## Running Reports

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name etl_status
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name etl_status --output ./etl_status.csv
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_relationship_model --params-template
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_relationship_model --input TP53 --input BRCA1 --param relationship_scope=input_to_any
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_relationship_model --input TP53 --param relationship_types=@./relationship_types.txt
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_relationship_model --input-file ./inputs.csv --input-column symbol --params-file ./params.yaml
```

Tip:
- Use `--input/--input-file` for inputs and keep `--param` for report options (scope, filters, toggles, etc.).

API (supports report parameters):
```python
df = bf.report.run("etl_status", source_system=["NCBI"], only_active=True)
print(df.head())
```

## Active Report Tutorials
- `reports_explain/report_etl_status.md`
- `reports_explain/report_etl_packages.md`
- `reports_explain/report_entity_filter.md`
- `reports_explain/report_db_pg_table_stats.md`
- `reports_explain/report_db_pg_index_stats.md`
- `reports_explain/report_variant_molecular_effects.md`
- `reports_explain/report_template.md`

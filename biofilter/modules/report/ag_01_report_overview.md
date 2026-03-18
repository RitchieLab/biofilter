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
biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_status
biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_status --as-csv --output ./etl_status.csv
```

API (supports report parameters):
```python
df = bf.report.run("etl_status", source_system=["NCBI"], only_active=True)
print(df.head())
```

## Active Report Tutorials
- `ag_02_report_etl_status.md`
- `ag_03_report_etl_packages.md`
- `ag_04_report_entity_filter.md`
- `ag_05_report_db_pg_table_stats.md`
- `ag_06_report_db_pg_index_stats.md`
- `ag_07_report_variant_molecular_effects.md`
- `ag_08_report_qry_template.md`

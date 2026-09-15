# Report Tutorial: `qry_template`

## Purpose

Starter/template report for building new report modules.

## Report Name

`qry_template`

## Notes

- This is a scaffold, not a production report.
- Use it as reference when creating a new report module.

## Suggested Workflow for New Reports

1. Copy `report_template.py` to `report_<new_name>.py`.
2. Rename class and set:
   - `name`
   - `description`
3. Implement:
   - `run()`
   - `explain()`
   - `available_columns()`
   - `example_input()` (if relevant)
4. Add tests and a tutorial `ag_XX_report_<new_name>.md`.

## Minimal API Example

```python
df = bf.report.run("qry_template")
print(df.head())
```

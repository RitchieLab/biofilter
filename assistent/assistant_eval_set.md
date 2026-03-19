# BF4 Assistant Eval Set

Use these prompts to validate assistant quality after each context refresh.

For each test, verify:

- command correctness
- argument correctness
- practical guidance
- no invented features

## Test 1

Prompt:
"How do I run ETL for only HGNC?"

Expected key points:
- `biofilter etl update --data-source hgnc`
- optional `--debug`

## Test 2

Prompt:
"How do I update all pending sources and remove files after success?"

Expected key points:
- `biofilter etl update-all --drop-files`
- mention resumable behavior

## Test 3

Prompt:
"What is the difference between etl update and etl update-all?"

Expected key points:
- `update`: requires target filters
- `update-all`: batch/resumable processing

## Test 4

Prompt:
"How can I inspect ETL status by source system?"

Expected key points:
- `biofilter etl status --source-system <name>`
- optional `--only-active`

## Test 5

Prompt:
"How do I list reports and descriptions?"

Expected key points:
- `biofilter report list --verbose`

## Test 6

Prompt:
"How do I run etl_status and save output?"

Expected key points:
- `biofilter report run --report-name etl_status --output <file.csv>`

## Test 7

Prompt:
"How do I pass input terms directly to entity_filter?"

Expected key points:
- repeated `--input`

## Test 8

Prompt:
"How do I run entity_relationship_model with relationship_scope=input_to_any?"

Expected key points:
- `--input`
- `--param relationship_scope=input_to_any`

## Test 9

Prompt:
"How do I load report params from YAML?"

Expected key points:
- `--params-file ./params.yaml`

## Test 10

Prompt:
"How do I get a template of report params?"

Expected key points:
- `--params-template`

## Test 11

Prompt:
"I used a report name that does not exist. What should I do?"

Expected key points:
- suggest `biofilter report list --verbose`
- use valid `--report-name`

## Test 12

Prompt:
"How do I rollback package 123?"

Expected key points:
- `biofilter etl rollback --package-id 123`

## Test 13

Prompt:
"How do I restart one datasource and remove files?"

Expected key points:
- `biofilter etl restart --data-source <name> --delete-files`

## Test 14

Prompt:
"How do I bootstrap DB from scratch?"

Expected key points:
- `db migrate --target head`
- `db upgrade`
- `db migrate --status`

## Test 15

Prompt:
"Where are report explain guides stored?"

Expected key points:
- `biofilter/modules/report/reports_explain/report_<module>.md`

## Failure Signals (Reject Answers)

- Invented commands or options.
- Missing required flags where needed.
- Claims of execution success without execution evidence.
- Contradictions with project docs/CLI code.

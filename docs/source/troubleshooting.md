# Troubleshooting

## Report Not Found

- Run `biofilter report list`.
- Use `--report-name` with one of the listed names.

## Input Conflict in `report run`

If you pass `--input`/`--input-file`, do not also pass input keys through params (`input_data`, `items`, `input_path`).

## Explain Page Not Found

- Check if guide exists at `biofilter/modules/report/reports_explain/report_<module>.md`.
- If missing, Biofilter will fall back to class `explain()`.

## Bundle Will Not Open

`No manifest.json in <path>` — the path is not a bundle root. Point at the
directory that holds `manifest.json`, not at its `tables/` subdirectory.

`<name> declares manifest_version N; this Biofilter reads [2]` — the bundle
was written by a newer Biofilter. Update the package; the bundle needs no
migration.

`<name> does not match its manifest` — a declared file is missing or is the
wrong size, and the message names which. An incomplete copy is the usual
cause; re-sync the directory and check with:

```bash
biofilter db verify --in ./bundles/<YYYYMMDD>
```

## A Report Says the Bundle Does Not Carry Something

`<name> does not carry: <tables>` — the bundle was built without the sources
that report needs. See what it does have:

```bash
biofilter --bundle ./bundles/<YYYYMMDD> report run \
  --report-name platform_data_statistics
```

## A Column Is All Null

Check `result.provenance["coverage"]`. It records which optional tables the
bundle lacked. A column that is null because the source was never built looks
exactly like one that is null because the answer is null, and only coverage
tells them apart.

## A Write Command Fails Against a Bundle

Expected. A bundle is read-only and the read layer has no write path at all.
Producing new data means building a new bundle — see
[Building Bundles](technical/building_bundles.md). There are no in-place
migrations: a schema change produces a new bundle, not an upgraded one.

To check an existing bundle against the schema this build expects:

```bash
biofilter db verify --in ./bundles/<YYYYMMDD> --schema
```

## ETL Batch Resume

If `etl update-all` was interrupted, run it again. Successful data sources are skipped.

## Report Output Not Found (Docker)

`--output` writes inside the container. Mount a host directory and write
to that path, or the file leaves with the container:

```bash
docker run --rm \
  -v /shared/bundles/20260914:/bundle:ro \
  -v "$(pwd)/out:/workspace" \
  ghcr.io/ritchielab/biofilter:latest \
  report run --report-name platform_etl_status --output /workspace/etl_status.csv
```

## Output Files Owned by the Wrong User (Docker)

The image runs as its own user, so files it writes belong to that uid. Add
`--user "$(id -u):$(id -g)"` to get your own. Under Apptainer this does not
arise — the container runs as the invoking user.

## Warning: Bundle Built by a Different Biofilter

```
UserWarning: <bundle> was built by Biofilter 4.2.0; this is 4.3.0.
```

The bundle opens and most reports behave. The warning exists because a
column that changed meaning between releases will not announce itself.
Check the bundle against this install:

```bash
biofilter db verify --in /path/to/bundle --schema
```

Every result records this under `provenance["version_mismatch"]`, so a
result produced across a version boundary can be recognised later.

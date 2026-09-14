# Biofilter 4.2.x — frozen

Everything here describes Biofilter as it worked up to 4.2.x. It is kept
so a 4.2.0 bundle stays usable and its analyses stay readable, not as a
guide to the current release.

```
notebooks/   analyses, report tutorials and templates
notebooks/scripts/   admin and debug scripts (VS Code launch configs point here)
docs/        the documentation as it stood before 4.3.0
```

## What changed in 4.3.0

Following anything in here against a 4.3.0 build will not work. The
differences are structural, not cosmetic:

| 4.2.x | 4.3.0 |
| ----- | ----- |
| PostgreSQL is the canonical store; a bundle is an export of it | The bundle *is* the product; no persistent database |
| `db create-db` → `db migrate` → `db upgrade` → `etl update` | `bundle plan` → `bundle build` |
| Alembic migrations | No migration chain — a schema change produces a new bundle |
| Variant tables keyed by a generated `variant_id` | Keyed by the natural key `chrom:pos:ref:alt` |
| gnomAD read from the genome callset | Reads the joint callset, plus exomes and genomes for VEP |

For the current release see [`docs/source/building_bundles.md`](../../docs/source/building_bundles.md)
and [`docs/source/bundle_requirements.md`](../../docs/source/bundle_requirements.md)
at the repository root.

## Why the ids matter

Ids in a 4.2.0 bundle are internal to that bundle. `entities.id` 11450 is
APOE in one and APOF in another, and the drift is small enough that a
stale id still resolves — to the wrong row, without an error. If you are
carrying ids out of anything here, carry the bundle they came from too.

The decision records behind the change are in [`adr/`](../../adr/) at the
root. ADR-003 covers 4.3.0 and explains what each measurement was
weighed against.

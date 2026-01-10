# Entity & Alias Registration in DTP Load

A central responsibility of each DTP `load()` method is to register **stable
Entities** and their **Aliases** from transformed tabular data (typically
Parquet).

This is a foundational pattern in Biofilter 4: domain master rows are converted
into persistent entities (identity layer), and then enriched with multiple
aliases, identifiers, labels, and domain-specific attributes.

This logic is standardized and implemented through shared utilities provided by
ETL mixins (e.g. `EntityQueryMixin`), ensuring that every domain follows a
consistent strategy for:

- identity creation,
- alias normalization,
- provenance tracking,
- reproducibility.

---

## 1. Declaring which input fields become aliases (`alias_schema`)

Each DTP declares an `alias_schema` that maps input columns to alias definitions.
This schema tells Biofilter:

- which columns should be treated as aliases,
- what alias type they represent (code, label, formula, etc.),
- what `xref_source` should be recorded (e.g. CheBI, HGNC, MONDO),
- whether the field is expected to be the **primary alias**.

Example:

```python
self.alias_schema = {
    "chebi_id": ("code", "CheBI", True),
    "ascii_name": ("label", "CheBI", None),
    "secondary_ids": ("code", "CheBI", None),
    "formula": ("formula", "CheBI", None),
}
````

Interpretation:

* `chebi_id` is the canonical identifier for this domain row and will be used as
  the **primary alias**.
* `ascii_name`, `secondary_ids`, and `formula` are stored as additional aliases
  for searchability and cross-reference coverage.

This makes alias registration **declarative** and keeps `load()` logic consistent
across domains.

---

## 2. Building alias objects from a row (`build_alias()`)

During load, the DTP uses `build_alias(row)` to convert the row into a list of
structured alias dictionaries (already normalized), based on the schema.

Typical pattern:

```python
alias_dict = self.build_alias(row)

is_primary_alias = next(
    (a for a in alias_dict if a.get("is_primary")),
    None
)

not_primary_alias = [
    a for a in alias_dict if a != is_primary_alias
]
```

**Key rule:** every entity must have **exactly one primary alias** that defines
its canonical identity within the domain.

---

## 3. Extending aliases with additional cross-references

Many sources provide additional alias data not captured directly in the schema.
The DTP may append these extra aliases after validation and normalization.

Example:

```python
aliases_extra = row.get("aliases_extra", [])

for alias in aliases_extra:
    if alias.get("alias_value") and alias.get("xref_source"):
        not_primary_alias.append(alias)
```

This supports richer integration while keeping the core identity logic stable.

---

## 4. Filtering invalid aliases (domain-specific hygiene)

Before persisting aliases, the DTP may apply domain-specific filters to avoid
noisy or irrelevant identifiers.

Example:

```python
not_primary_alias = [
    alias for alias in not_primary_alias
    if alias.get("xref_source") != "PubMed"
]
```

This prevents reference-like identifiers from polluting the alias space,
improving search precision and reducing ambiguity.

---

## 5. Determining status and entity activity

Biofilter distinguishes between:

* **omic status** (domain-level status metadata),
* **entity active flag** (identity-level active/inactive).

Example:

```python
if status_id == 4:
    omic_status_id = status_map["deactive"].id
    is_active_entity = False
else:
    omic_status_id = status_map["active"].id
    is_active_entity = True
```

This allows entities to persist historically while still indicating deprecation
or obsolescence.

---

## 6. Creating or resolving the Entity (`get_or_create_entity()`)

Entity creation is performed using the primary alias as the canonical anchor:

```python
entity_id, created = self.get_or_create_entity(
    name=is_primary_alias["alias_value"],
    group_id=self.entity_group,
    data_source_id=self.data_source.id,
    package_id=self.package.id,
    alias_type=is_primary_alias["alias_type"],
    xref_source=is_primary_alias["xref_source"],
    alias_norm=is_primary_alias["alias_norm"],
    is_active=is_active_entity,
)
```

This ensures:

* stable identities across reruns,
* provenance linking (`data_source_id`, `etl_package_id`),
* deterministic behavior when re-ingesting updated sources.

---

## 7. Persisting non-primary aliases (`get_or_create_entity_name()`)

All additional aliases are persisted as entity-level alias records:

```python
self.get_or_create_entity_name(
    group_id=self.entity_group,
    entity_id=entity_id,
    aliases=not_primary_alias,
    is_active=is_active_entity,
    data_source_id=self.data_source.id,
    package_id=self.package.id,
)
```

This captures a rich alias space for:

* cross-database identifier normalization,
* flexible user input matching,
* future relationship resolution across domains.

---

## Design notes and best practices

* **Always define a primary alias** in `alias_schema`.
  If the source does not provide a stable ID, the DTP must define a deterministic
  surrogate strategy.

* **Keep alias hygiene strict.**
  Prevent reference identifiers (PubMed IDs, URLs, etc.) from inflating alias
  tables unless explicitly required.

* **Store provenance consistently.**
  Both entity and alias records must include `data_source_id` and
  `etl_package_id`. This is essential for auditing, incremental updates, and
  reprocessing.

* **Separate identity from relationships.**
  Alias and entity registration belongs to the *master data → entity* step.
  Relationship loading should occur only after all relevant master domains have
  been ingested.

---

## Architectural takeaway

Entity and alias registration is the **identity backbone** of Biofilter 4.
By enforcing a consistent, declarative, and provenance-aware alias strategy,
Biofilter ensures that biological identities remain stable, searchable, and
integrated across domains and data sources over time.


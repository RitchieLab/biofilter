# Model Modules & Domain Patterns

This section describes the common modeling patterns used across Biofilter 4
domain models and illustrates how to design a new omics domain that integrates
cleanly with the entity-centric core.

Rather than documenting every model file line-by-line, the goal is to explain
recurring structural patterns that developers are expected to follow.

---

## Core modeling principles

All domain models in Biofilter 4 follow a small set of consistent rules:

- Every domain has a **master table**
- Every master record links to a central **Entity** (except Variants)
- All domain data is **provenance-aware**
- Many-to-many semantics are **explicit**
- Domain structure is independent, identity is shared

These patterns ensure that:

- domains remain extensible,
- entities remain unique and persistent,
- ETL provenance is preserved,
- cross-domain integration stays explicit and safe.

---

## The “master table” pattern

Each biological domain defines a canonical **master table** that represents the
authoritative record for that domain.

Examples include:

- `GeneMaster`
- `ProteinMaster`
- `DiseaseMaster`

The master table is where:

- the domain’s primary identifier lives,
- domain-specific attributes are stored,
- the link to the shared entity is defined.

The master table **does not replace** the entity — it **extends** it.

---

## Linking a domain to the Entity layer

Every master table must include a foreign key to the central `entities` table:

```python
entity_id = Column(
    BigInteger,
    ForeignKey("entities.id", ondelete="CASCADE"),
    nullable=False
)
entity = relationship("Entity", passive_deletes=True)
````

This ensures that:

* all identifiers and aliases live in one place,
* relationships across domains are possible,
* identity is never duplicated.

> Developers should **never** create domain records without linking them to an
> entity.

---

## Provenance is mandatory, not optional

All domain tables include provenance fields linking records back to ETL metadata:

```python
data_source_id = Column(
    Integer,
    ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
    nullable=True,
)
etl_package_id = Column(
    Integer,
    ForeignKey("etl_packages.id", ondelete="CASCADE"),
    nullable=True,
)
```

This allows Biofilter to:

* track where each record came from,
* support incremental updates,
* audit and debug ingestion,
* safely merge knowledge from multiple sources.

Even reference or grouping tables (e.g. tags, categories) should include
provenance when populated via ETL.

---

## Example: Disease domain modeling

The disease domain provides a representative example of how to structure a
domain module.

### `DiseaseMaster`: the canonical domain record

```python
class DiseaseMaster(Base):
    """
    Canonical representation of diseases in Biofilter 4.
    Each disease is linked to a unique Biofilter Entity (`entity_id`) and
    identified by a MONDO ID (preferred primary identifier).
    """
    __tablename__ = "disease_masters"

    id = Column(Integer, primary_key=True)
    disease_id = Column(String(50), unique=True, index=True)
    label = Column(String(255))
    description = Column(Text)

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity = relationship("Entity", passive_deletes=True)

    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"))
    etl_package_id = Column(Integer, ForeignKey("etl_packages.id"))
```

Key points:

* `disease_id` is the domain identifier (e.g. MONDO),
* `entity_id` provides global identity,
* provenance fields track ingestion context.

---

## Domain-specific groupings (tags, subsets)

Domains often need internal groupings that are **not global biological
identities**, such as disease subsets or classification tags.

These are modeled as **domain-local reference tables**, not entities:

```python
class DiseaseGroup(Base):
    __tablename__ = "disease_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True)
    description = Column(String(255))

    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"))
    etl_package_id = Column(Integer, ForeignKey("etl_packages.id"))
```

Examples of disease groups:

* `rare`
* `gard_rare`
* `nord_rare`
* `otar`

These groups are not shared across domains and therefore do not belong in the
entity layer.

---

## Explicit many-to-many relationships

Many-to-many relationships are always modeled **explicitly** via linking tables:

```python
class DiseaseGroupMembership(Base):
    __tablename__ = "disease_group_memberships"

    id = Column(Integer, primary_key=True)
    disease_id = Column(Integer, ForeignKey("disease_masters.id"))
    group_id = Column(Integer, ForeignKey("disease_groups.id"))

    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"))
    etl_package_id = Column(Integer, ForeignKey("etl_packages.id"))
```

This pattern:

* preserves provenance,
* allows incremental updates,
* avoids hidden semantics.

Implicit joins are discouraged.

---

## When to create an Entity vs a domain table

A common question when extending Biofilter is:

> *“Should this be a new Entity or just a domain table?”*

### Use an **Entity** when:

* the concept participates in relationships across domains,
* it has multiple identifiers or aliases,
* it should be globally referenceable.

### Use a **domain table** when:

* attributes are domain-specific,
* the concept only makes sense within that domain,
* it does not require cross-domain relationships.

In most cases, the correct design is **both**:

* an **Entity** for identity,
* a **domain master table** for attributes.

---

## Adding a new omics domain (summary)

To add a new domain (e.g. metabolites, phenotypes, exposures):

1. Define a new `model_<domain>.py`
2. Create a `<Domain>Master` table
3. Link it to `Entity`
4. Add provenance fields
5. Add domain-local reference tables if needed
6. Populate via a DTP

Following this pattern ensures that new domains integrate seamlessly with
existing reports, queries, and relationships.

---

## Takeaway for developers

Biofilter 4 domain models are not isolated tables — they are structured
extensions of a shared entity-centric knowledge core.

If a new model:

* links to `Entity`,
* tracks provenance,
* models relationships explicitly,

then it will scale naturally with the rest of the platform.

---

## Other model groups

In addition to domain-specific models, Biofilter 4 includes several
system-level model groups that support configuration, provenance, and curation
workflows. These models are not biological domains themselves, but they are
essential for platform operation and governance.

---

### ETL models (`model_etl.py`)

The ETL models define the provenance and execution metadata layer of Biofilter 4.
They capture:

* source systems and data sources,
* ETL executions (packages),
* execution status, timestamps, and metrics.

These models enable:

* incremental and full load management,
* auditability and reproducibility,
* ETL monitoring through reports.

ETL models should be treated as **infrastructure-level components** and are
rarely modified outside of ingestion framework development.

---

### Configuration models (`model_config.py`)

Configuration models store structured, system-level configuration values that
are persisted in the database.

They are used to:

* centralize runtime and feature configuration,
* support environment-independent behavior,
* enable configuration inspection via the API and CLI.

These models complement the project-level `.biofilter.toml` file and are
typically managed through controlled interfaces rather than direct manipulation.

---

### Curation and conflict models (`model_curation.py`)

Curation models support conflict detection, resolution, and tracking when
integrating biological knowledge from multiple sources.

They are used to:

* record conflicting identifiers or relationships,
* track resolution decisions,
* preserve provenance of curation actions.

These models enable Biofilter 4 to evolve curated knowledge over time without
losing historical context.

---

## How these model groups fit together

Together, domain models, ETL models, configuration models, and curation models
form a complete operational schema:

* Domain models store biological attributes,
* Entity models define identity and relationships,
* ETL models provide provenance and execution history,
* Configuration and curation models ensure governance and consistency.

This separation allows Biofilter 4 to function as both a scientific knowledge
base and a production-grade data platform.

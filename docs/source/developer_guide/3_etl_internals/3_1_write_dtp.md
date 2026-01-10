# Writing a DTP

A **DTP (Data Transformation Package)** is the unit of ingestion logic in
Biofilter 4. Each DTP encapsulates the end-to-end ETL workflow for **one
ETLDataSource** (e.g. MONDO, HGNC, dbSNP, UniProt).

A DTP is responsible for:

- **Extract**: landing raw upstream artifacts into a reproducible local folder
- **Transform**: normalizing and materializing a tabular, analysis-friendly
  intermediate representation (typically Parquet)
- **Load**: ingesting normalized data into the relational schema, generating
  Entities, Master Records, and (when applicable) Relationships in a controlled
  order

This section uses a **Disease (MONDO)** DTP as a reference because it illustrates
a key Biofilter 4 design rule:

> **Master data and cross-domain relationships are often loaded by different
> DTPs (or different ETL phases).**

---

## Why split “Master Data” vs “Relationships” into separate DTPs?

In Biofilter 4, relationships frequently connect entities across domains:

- Disease ↔ Gene  
- Disease ↔ Chemical  
- Gene ↔ Pathway  
- Variant ↔ Gene  

If a relationship loader runs before the target domain’s master entities exist,
you risk:

- creating dangling relationships,
- forcing implicit entity creation with incomplete identity,
- inconsistent provenance,
- load failures due to missing entity groups or aliases.

**Best practice**

- One DTP loads **domain master data** (and creates canonical entities)
- Another DTP loads **relationships** after all required master domains have been
  ingested

For **MONDO**, this means:

- `dtp_mondo.py`  
  Loads `DiseaseMaster`, disease groups/tags, and aliases

- `dtp_mondo_relationships.py`  
  Loads disease → gene / disease → chemical / disease → anatomy relationships
  once the target domains exist

This pattern prevents *relationship-first ingestion* and guarantees consistent
identity.

---

## DTP anatomy (what every DTP should contain)

### 1) Class metadata (identity + compatibility)

Each DTP should declare:

- `dtp_name` and `dtp_version`
- compatible schema range (`compatible_schema_min` / `compatible_schema_max`)
- the active `ETLDataSource` (`self.data_source`)
- the current `ETLPackage` (`self.package`)
- a database session
- a shared logger

Example (simplified):

```python
class DTP(DTPBase, EntityQueryMixin):
    def __init__(self, logger=None, debug_mode=False, datasource=None, package=None, session=None):
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session

        # DTP versioning
        self.dtp_name = "dtp_mondo"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"
````

**Developer note**

`check_compatibility()` should run early in every ETL step to prevent loading
against incompatible schemas.

---

## Extract step (landing raw data + hashing)

The **extract** phase should:

* create a deterministic landing folder
  `raw/{source_system}/{data_source}/`
* download or copy upstream files **without modifying content**
* compute a hash (or version marker) to support:

  * change detection
  * incremental orchestration
  * package-level traceability

From the MONDO example:

```python
def extract(self, raw_dir: str):
    self.check_compatibility()

    source_url = self.data_source.source_url
    landing_path = os.path.join(
        raw_dir,
        self.data_source.source_system.name,
        self.data_source.name,
    )
    os.makedirs(landing_path, exist_ok=True)

    file_path = os.path.join(landing_path, "mondo.json")

    with requests.get(source_url, stream=True) as r:
        r.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    current_hash = compute_file_hash(file_path)
    return True, "Downloaded MONDO JSON", current_hash
```

### Recommendations

* Prefer **Pathlib** over `os.path`
* Always call `raise_for_status()` (fail fast)
* Store `current_hash` in the **ETLPackage metadata JSON** so the package
  becomes the authoritative record of what was ingested

---

## Transform step (normalize to Parquet, keep high granularity)

The **transform** phase should:

* read raw artifacts from the landing zone
* normalize identifiers (e.g. compact MONDO IDs)
* extract master records into a structured table
* optionally extract relationship-like intermediate files (even if they will be
  loaded later)
* write outputs to
  `processed/{source_system}/{data_source}/`

From the MONDO implementation, the following outputs are generated:

* `master_data.parquet`
* `relationship_data.parquet`

Conceptual example:

```python
df_master.to_parquet(output_path / "master_data.parquet", index=False)
df_rels.to_parquet(output_path / "relationship_data.parquet", index=False)
```

### Why this is good

* `master_data.parquet` is the authoritative payload for the Disease domain loader
* `relationship_data.parquet` is a clean handoff artifact to a future
  *relationships DTP*
* Parquet can be queried directly with **DuckDB** for exploratory analysis and
  validation

---

## Load step (entity-first, then master records, then domain links)

The **load** phase should follow a strict order:

1. Resolve required reference tables (e.g. `EntityGroup`, `OmicStatus`)
2. Prepare performance mode (optional): drop indexes / set write-mode
3. For each record:

   * build alias schema
   * create or retrieve canonical **Entity**
   * register non-primary aliases (`EntityName` / `EntityAlias`)
   * write domain master row (`DiseaseMaster`)
   * write domain auxiliary links (`DiseaseGroupMembership`, etc.)
4. Restore indexes / set read-mode

In the MONDO loader, the core pattern is correct:

```python
entity_id, _ = self.get_or_create_entity(...)
self.get_or_create_entity_name(...)

disease_master_obj = (
    self.session.query(DiseaseMaster)
    .filter_by(disease_id=disease_master)
    .first()
)

if not disease_master_obj:
    disease_master_obj = DiseaseMaster(
        disease_id=disease_master,
        label=row.get("label"),
        description=row.get("description"),
        omic_status_id=omic_status_id,
        entity_id=entity_id,
        data_source_id=self.data_source.id,
        etl_package_id=self.package.id,
    )
    self.session.add(disease_master_obj)
    self.session.flush()
```

---

## Provenance: DataSource + Package on every row

A strong Biofilter 4 convention is:

> **Every domain row must carry provenance**

Domain tables should include:

* `data_source_id`
* `etl_package_id`

This makes every row traceable to:

* which source produced it
* which ETL execution produced it

---

## Handling subsets / tags (domain auxiliary tables)

MONDO subsets are a representative example of **domain-specific auxiliary
structures**.

Correct pattern:

1. Collect unique subsets
2. Create `DiseaseGroup` rows
3. Map `subset → group_id`
4. Create membership links

This is the standard pattern for domain attribute tables that support richer
filtering and reporting later.

---

## Why relationship loading is deferred (important developer rule)

The transform step extracts relationship edges into
`relationship_data.parquet`, but the current DTP **does not load** those
relationships into `entity_relationships`.

This is intentional and recommended when:

* relationships target other domains (genes, chemicals, anatomy),
* those domains may not exist yet in the database,
* deterministic and consistent identity resolution is required.

Recommended architecture:

* `dtp_mondo.py`
  Creates Disease entities and disease masters

* `dtp_mondo_relationships.py`
  Reads `relationship_data.parquet` and materializes relationships only when all
  required target domains are present

This prevents accidental partial entity creation and keeps integration clean.

---

## Operational considerations (developer standards)

### Index handling

For large loads, it is common to:

* drop heavy indexes before load,
* bulk ingest,
* recreate indexes afterward.

Ensure index specifications match the domain
(e.g. `get_disease_index_specs`).

---

### Commit strategy

For large sources:

* commit in chunks (e.g. every *N* rows),
* catch `IntegrityError` / `SQLAlchemyError`,
* record warnings in the ETLPackage summary JSON.

---

### Logging vs package status

* **Logs** are verbose and transient
* **ETLPackage records** are authoritative status and metrics

Best practice:

* store final metrics (counts, warnings, hashes) in the ETLPackage JSON
* keep detailed stack traces in logs

---

## Minimal checklist for new DTPs

Before merging a new DTP, confirm:

* Declares DTP metadata (name, version, schema range)
* Uses landing zone paths
  `raw/{source_system}/{data_source}/`
* Emits Parquet outputs
  `processed/{source_system}/{data_source}/`
* Creates **Entities first**, then master rows
* Attaches provenance (`data_source_id`, `etl_package_id`)
* Avoids cross-domain relationships unless target domains are guaranteed loaded
* Records package-level summary (counts, warnings, hash/version)
* Can run safely in incremental mode (no destructive deletes unless explicitly
  full-load)



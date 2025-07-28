# **Biofilter3R: Concept and Evolution**

### 🧠 What Is Biofilter3R?

Biofilter3R is a rebuilt and unified version of two legacy systems: **LOKI** (responsible for data ingestion and storage) and **Biofilter v2** (responsible for querying and enrichment). These systems were initially developed in Python 2, and although they were later migrated to Python 3, many outdated coding patterns remained. They also suffered from data reliability issues, complex interfaces, and limited maintainability.

Biofilter3R was created as a **Rebuild** (hence the "R") of version 3, addressing core architectural limitations through a clean, modern, and integrated design.

---

### 🔄 Why Rebuild Instead of Refactor?

Although partial refactoring was attempted (unit testing, CI/CD, cleanup), fundamental logic and hidden assumptions made the systems hard to maintain and evolve. A rebuild allowed:

- Clear separation of concerns
- Stronger foundation for custom extensions
- Modern engineering best practices

---

### 🔧 Key Improvements Introduced in Biofilter3R

### 1. **Single Unified Package**

LOKI and Biofilter were merged into a single Python package, simplifying the installation and usage. Users no longer need to manage two systems to perform a single task.

### 2. **SQLAlchemy and ORM-based Data Models**

Replaced APSW with SQLAlchemy:

- Tables are now Python classes
- Fields are class attributes
- Enables switching between SQLite, PostgreSQL, and other DBs
- Removes the need for raw SQL queries embedded in Python code

### 3. **Entity-Based Data Model**

Introduced a **hub-and-spoke model**:

- Unified `Entity` model at the center
- Domain-specific extensions (e.g., Gene, Protein, Pathway)
- Enables traversal of omics relationships across sources

### 4. **User-Friendly and Customizable Queries**

Users can:

- Understand the data schema clearly
- Build their own queries
- Use a growing collection of predefined query templates (Annotation, Modeling, Filtering)

### 5. **ETL-Based Ingestion with Full Control**

Each data source is processed through a structured ETL pipeline:

- Users select which data sources and steps to run
- All processes are logged and versioned
- Error handling prevents corruption of the main database

### 6. **Robust Logging System**

- Central `biofilter.log` file
- Historical logging models in the database
- Supports debugging and process auditing

### 7. **Data Lake Architecture with Raw and Processed Stages**

- `raw/`: Stores downloaded, untouched data
- `processed/`: Normalized data in Parquet format
- Facilitates external data use, enrichment, and noSQL querying
    
    Examples:
    
    - All dbSNP variants are kept in `data_master.parquet`, including HGVS notation and unsupported variant types.
    - These Parquet files support additional annotations not loaded into the main DB but accessible via noSQL tools.

---

### 🚧 Current Status and Roadmap

- Biofilter3R is under **active development**
- Code is hosted on a dedicated branch of the main repository
- All commits use GitHub Actions for testing and validation
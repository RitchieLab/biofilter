# 🧬 Biofilter-LOKI 3.0.0

**Biofilter-LOKI 3.0.0** is a lightweight, command-line–driven knowledge base builder designed to support **BioBin** and other legacy Biofilter workflows.  
This version preserves the traditional **LOKI architecture**, while modernizing the codebase and deployment for current environments.

---

## 🎯 Purpose & Design Goals

Biofilter-LOKI 3.0.0 was built to:

- Maintain **full compatibility with BioBin**
- Preserve the **classic LOKI data model**
- Provide a **simple CLI-based workflow**
- Support **HPC module deployments**
- Enable **rapid database builds** for analysis pipelines


---

## 🧠 The LOKI Knowledge Engine

LOKI (Library Of Knowledge Integration) is the **knowledge ingestion engine** behind Biofilter.  
It builds a **SQLite knowledge database** by integrating multiple biological data sources, such as:

- SNP ↔ Gene
- Gene ↔ Pathway
- Gene ↔ Ontology
- Identifier mappings across databases


---

## 🏗️ Architecture Overview

```
┌──────────────┐
│   Biofilter  │  ← primary CLI
└──────────────┘
        │
        ▼
┌─────────────────────┐
│ SQLite Knowledge DB │
│    (LOKI schema)    │
└─────────────────────┘
        ▲
        │
┌──────────────┐
│  loki-build  │  ← ingestion engine
└──────────────┘
        ▲
        │
 External Sources
````

Key characteristics:

- **SQLite backend**
- **Immutable batch loads**
- **No entity-level curation**
- **Optimized for downstream queries**

---

## 📦 Included Data Sources

Depending on build options, Biofilter-LOKI can ingest:

- **dbSNP**
- **Entrez Gene**
- **Gene Ontology (GO)**
- **Pathways** (KEGG / Reactome, if enabled)
- **Chain files** (genome build liftover)
- **Identifier mappings**

The available sources depend on how the package was built and deployed.

---

## 🚀 Installation — Python Environment

```bash
pip install biofilter-loki
```

---

## 🛠️ Building a Knowledge Database

Basic example:

```bash
loki-build \
  --knowledge loki.db \
  --load dbsnp entrez go
```

Update existing database:

```bash
loki-build \
  --knowledge loki.db \
  --update
```

Build from an archive:

```bash
loki-build \
  --from-archive loki_sources.tar.gz \
  --knowledge loki.db
```

---

## 🔍 Common CLI Options

| Option           | Description              |
| ---------------- | ------------------------ |
| `--knowledge`    | Output SQLite database   |
| `--load`         | Load specific sources    |
| `--update`       | Update existing DB       |
| `--from-archive` | Load from source archive |
| `--to-archive`   | Save source archive      |
| `--no-optimize`  | Skip DB optimization     |
| `--verbose`      | Verbose logging          |

Run `loki-build --help` for full details.

---

## ▶️ Using the `biofilter` Command

Once a LOKI knowledge database has been built, the `biofilter` command
can be used to query and inspect its contents.

Check version information:

```bash
biofilter --version
```

Display general help and available subcommands:

```bash
biofilter --help
```
List available data sources loaded into the database:

```bash
biofilter \
  --knowledge loki.db \
  --snp-file snps.txt \
  --source kegg \
  --annotate position_label snp position gene upstream downstream \
  --report-invalid-input \
  --report-configuration \
  --overwrite \
  --prefix outcomes_prefix \
  --ucsc-build-version 19 \
```
Where snps.txt contains one rsID per line.

Note: Available subcommands depend on which data sourcesmwere loaded during database construction.

---

## 📚 Documentation

- 📘 **User Manual (PDF)**  
  [Biofilter Manual — Version 3.0.0](docs/biofilter-manual-3.0.0.pdf)

---

## 🧑‍🔬 Maintainers

Developed and maintained by the **Ritchie Lab**
University of Pennsylvania

---

## 📜 License

Distributed under the original Biofilter license.
See `LICENSE` file for details.



---

## Development Documentation in:

https://ritchielab.github.io/biofilter/
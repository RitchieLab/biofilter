# Setting Up and Running Sphinx for Biofilter Documentation

This document provides instructions for contributors on how to set up, customize, and run Sphinx to generate documentation for the **Biofilter** project.

---

## Prerequisites

Before working with the documentation, ensure you have the following installed:

- Python (version >= 3.10)
- Poetry (for dependency management)

---

## Setting Up the Environment

### 1. Clone the Repository

```bash
git clone https://github.com/.../biofilter.git
cd biofilter
```

### 2. Install Dependencies

Install all project dependencies, including Sphinx, using Poetry:

```bash
poetry install
```

Activate the virtual environment:

```bash
poetry shell
```

---

## Initializing Sphinx

### 1. Navigate to the `docs/` Directory

The documentation is located in the `docs/` directory. Navigate to it:

```bash
cd docs
```

### 2. Customize the Configuration

The main configuration file for Sphinx is `conf.py` in the `docs/` directory. Open and edit it as needed:

```bash
nano conf.py
```

Key sections you may want to customize:
- **Project Information**:
  ```python
  project = 'Biofilter'
  author = 'Ritchie Lab'
  release = '2.4.4'
  ```
- **Path Settings**:
  Ensure the project’s modules are accessible to Sphinx:
  ```python
  import os
  import sys
  sys.path.insert(0, os.path.abspath(".."))
  ```
- **Extensions**:
  Add or remove Sphinx extensions as needed:
  ```python
  extensions = [
      'sphinx.ext.autodoc',
      'sphinx.ext.viewcode',
      'sphinx.ext.napoleon',
      'sphinx_rtd_theme',
  ]
  ```
- **HTML Theme**:
  Set the theme for the generated documentation:
  ```python
  html_theme = 'sphinx_rtd_theme'
  ```

---

## Generating the Documentation

### 1. Build the HTML Documentation

To generate the HTML version of the documentation:

```bash
make html
```

The output will be located in the `_build/html/` directory.

### 2. View the Documentation

Open the `index.html` file in a browser to view the documentation:

```bash
xdg-open _build/html/index.html
```

On macOS:
```bash
open _build/html/index.html
```

---

## Customizing Documentation

### 1. Edit `index.rst`

The `index.rst` file serves as the main entry point for your documentation. You can add sections or include additional `.rst` files here.

### 2. Generate Module Documentation Automatically

Use `sphinx-apidoc` to generate `.rst` files for your Python modules:

```bash
sphinx-apidoc -o . ../biofilter_modules
```

This command scans the `biofilter_modules` directory and generates `.rst` files for each module.

### 3. Add New Sections

Create new `.rst` files for additional sections and include them in `index.rst` using:

```rst
.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules
   new_section
```

---

## Regenerating Documentation

If you make changes to the codebase or `.rst` files, regenerate the documentation:

```bash
make clean
make html
```

---

## Tips for Contributors

1. **Use Docstrings**:
   Write detailed docstrings in your Python code using Google or NumPy style for compatibility with the `napoleon` extension.

2. **Update `conf.py`**:
   If new directories or modules are added, update the `sys.path` in `conf.py` to ensure Sphinx can locate them.

3. **Preview Locally**:
   Always preview the generated documentation locally before submitting changes.

4. **Commit Changes**:
   If you add or update `.rst` files, commit them to the repository:

   ```bash
   git add docs/
   git commit -m "Update Sphinx documentation"
   ```

---

## Troubleshooting

- **Sphinx Command Not Found**:
  Ensure you are in the Poetry environment:
  ```bash
  poetry shell
  ```

- **Missing Modules in Documentation**:
  Check that the module paths are included in `conf.py` under `sys.path`.

- **Broken Links**:
  Run the following command to check for broken links:
  ```bash
  make linkcheck
  ```

---

## Additional Resources

- [Sphinx Documentation](https://www.sphinx-doc.org/en/master/)
- [Google Style Docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- [Napoleon Extension](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html)


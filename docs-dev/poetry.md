### Using Poetry for Dependency Management and Project Setup

This document provides a guide to understanding Poetry, setting up the project locally, and creating installation packages for the Biofilter project.

---

### **What is Poetry?**

Poetry is a modern dependency management tool for Python projects. It simplifies the process of managing dependencies, creating reproducible environments, and publishing packages. Poetry replaces traditional tools like `requirements.txt`, `setup.py`, and `virtualenv` by consolidating all these functionalities into a single, streamlined workflow.

---

### **Why Use Poetry?**

1. **Simplified Dependency Management**:
   - Define all dependencies in a single `pyproject.toml` file.
   - Automatically resolves dependency conflicts and locks exact versions.

2. **Reproducible Environments**:
   - The `poetry.lock` file ensures consistent environments across different systems.

3. **Built-In Virtual Environment Management**:
   - Poetry handles virtual environments automatically, isolating dependencies.

4. **Integrated Packaging**:
   - Easily create and publish packages to PyPI or distribute locally.

---

### **Setting Up the Project Locally**

#### **Step 1: Install Poetry**
Follow the official Poetry installation instructions:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

After installation, verify the installation:

```bash
poetry --version
```

If you're on Windows, you can use the official installer or refer to the [Poetry documentation](https://python-poetry.org/docs/).

---

#### **Step 2: Clone the Repository**

```bash
git clone https://github.com/.../biofilter.git
cd biofilter
```

---

#### **Step 3: Install Project Dependencies**
Poetry will automatically create a virtual environment and install all dependencies specified in `pyproject.toml`:

```bash
poetry install
```

#### **Step 4: Activate the Virtual Environment**
To activate the virtual environment created by Poetry, run:

```bash
poetry shell
```

You can now use the installed dependencies and run project-specific commands within this environment.

#### **Step 5: Run the Project**
For example, to run one of the entry-point scripts defined in the project:

```bash
poetry run biofilter
```

---

### **Creating Installation Packages**

To create installation packages for Biofilter, follow these steps:

#### **Step 1: Validate the Project Setup**
Ensure the `pyproject.toml` file is properly configured with all required metadata, dependencies, and entry points. Example fields to check:
- `name`
- `version`
- `description`
- `authors`
- `dependencies`
- `scripts` (for entry points like `biofilter` or `loki-build`).

Example entry in `pyproject.toml`:
```toml
[tool.poetry.scripts]
biofilter = "biofilter_modules.biofilter:main"
loki-build = "loki_modules.loki_build:main"
```

#### **Step 2: Build the Package**
Run the following command to generate distribution files (`.tar.gz` and `.whl`) in the `dist/` directory:

```bash
poetry build
```

This command will create:
- A source distribution (`.tar.gz`).
- A wheel distribution (`.whl`).

#### **Step 3: Verify the Package**
To test the installation of the package locally, use:

```bash
pip install dist/biofilter-<version>.whl
```

Replace `<version>` with the version number of the package.

#### **Step 4: Publish the Package**
To publish the package to PyPI (or a private repository), use:

```bash
poetry publish --build
```

You will need valid credentials for PyPI or the repository you are targeting. To configure these, run:

```bash
poetry config pypi-token.pypi <your-token>
```

---

### **Automating the Workflow**

You can create a simple script or CI pipeline to automate the steps for building and publishing the package. For example:

```bash
#!/bin/bash
# build-and-publish.sh

set -e

echo "Building the Biofilter package..."
poetry build

echo "Publishing the package to PyPI..."
poetry publish --build

echo "Done!"
```

Make this script executable:
```bash
chmod +x build-and-publish.sh
```

Run it:
```bash
./build-and-publish.sh
```

---

### **Managing Dependencies**

#### **Adding a Dependency**
To add a new dependency to the project, use:

```bash
poetry add <package-name>
```

#### **Adding a Development Dependency**
To add a dependency for development purposes (e.g., linters, testing frameworks):

```bash
poetry add --dev <package-name>
```

#### **Removing a Dependency**
To remove a dependency from the project:

```bash
poetry remove <package-name>
```

---

### **Reproducibility**

If someone else clones the repository, they only need to run:

```bash
poetry install
```

This command reads the `pyproject.toml` and `poetry.lock` files to recreate the exact same environment.

---

### **Additional Resources**
- Official Poetry Documentation: [https://python-poetry.org/docs/](https://python-poetry.org/docs/)
- FAQ and Troubleshooting: [https://python-poetry.org/docs/faq/](https://python-poetry.org/docs/faq/)


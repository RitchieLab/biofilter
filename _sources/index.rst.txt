Biofilter Project Documentation
===============================

Welcome to the **Biofilter** project documentation! This guide provides an overview of the project, its structure, and resources to help both power users and developers get started.


Overview
--------

The **Biofilter** project is a Python-based tool designed for efficient data processing and analysis using SQLite as its core data storage. The project is organized into two main modules:

1. **Biofilter Module**: Responsible for managing and running the functionalities specific to the Biofilter system, such as filtering genes, groups, and annotations based on user-defined criteria.

2. **LOKI Module**: Manages functionalities related to the LOKI database, including data transformations and preparation for Biofilter analysis.

Both modules work with a SQLite database file, which must be provided as an input argument. This database stores all the necessary data for Biofilter and LOKI functionalities, ensuring a consistent and centralized data source.


Documentation for Users
-----------------------
For more detailed instructions and usage examples, refer to the user documentation:

- Biofilter User Guide
- LOKI User Guide


Key Features
------------

- **Python-Based**:

  The entire project is developed in Python, leveraging modern tools and libraries to ensure maintainability, scalability, and ease of use.

- **Integrated SQLite Database**:

  All data required by Biofilter and LOKI is stored in a single SQLite file. The path to this database must be provided as an argument when running either module.

- **Modular Design**:

  - The Biofilter module focuses on filtering and querying functionalities.
  - The LOKI module handles data preparation and transformations.

- **Development Tools**:

  - **Poetry**: Used for dependency management and virtual environment creation.
  - **Tox**: Ensures testing across multiple Python versions.
  - **Black**: Enforces consistent code formatting.
  - **Sphinx**: Generates project documentation.
  - **Coverage**: Measures code coverage during testing.


Development Resources
---------------------

As a new contributor, here are some key points to help you navigate and contribute to the project:

- **Code Organization:**

  - biofilter_modules: Contains core functionalities for Biofilter.
  - loki_modules: Handles LOKI-specific operations.
  - tests: Includes unit and functional tests for both modules.

- **Development Workflow:** Follow these guidelines to maintain consistency and quality in your contributions:

  - Use Poetry as the central tool for managing dependencies and virtual environments.
  - Run tests with Tox to ensure compatibility across supported Python versions.
  - Follow code formatting standards enforced by Black.

- **Environment Setup:** Ensure your environment is consistent with the project’s requirements:

  - Python >= 3.10
  - Dependencies managed through poetry install

- **Tips for New Developers:**

  - Start by reviewing the README and this documentation to understand the project's goals and architecture.
  - Use the provided tools (e.g., Black, Tox) to maintain consistency and quality in your contributions.
  - Always run tests before submitting changes.


Getting Started
---------------

Follow these steps to set up the **Biofilter** project for development or testing:

1. **Clone the Repository**:

  .. code-block:: bash

    git clone https://github.com/RitchieLab/biofilter.git
    cd biofilter

2. **Switch to the development Branch.** Ensure you are working with the latest code:**
  
  .. code-block:: bash

    git checkout development

3. **Set Up Python Environment:**

  If you are using pyenv, set the desired Python version for the project
  
  .. code-block:: bash

    pyenv install 3.11.9  # Example version
    pyenv local 3.11.9

  Ensure the Python version matches the project requirements (Python >= 3.10).

4. **Install Poetry.** Install Poetry, the dependency and environment manager:

  .. code-block:: bash

    curl -sSL https://install.python-poetry.org | python3 -
    poetry --version  # Verify the installation

5. **Install Project Dependencies.** Use Poetry to install all required dependencies:

  .. code-block:: bash

    poetry install

6. **Configure Python Versions in tox.** Update the tox.ini file to include the Python versions available in your environment. For example:

  .. code-block:: ini

    [tox]
    envlist = py310, py311, py312

7. **Run Tests.** Verify your setup by running the test suite:
  
  .. code-block:: bash

    poetry run pytest


.. toctree::
  :maxdepth: 2
  :caption: Contents:

  usage-of-branching
  usage-of-poetry
  usage-of-black
  usage-of-coverage
  usage-of-tox
  usage-of-sphinx
  usage-of-git
  
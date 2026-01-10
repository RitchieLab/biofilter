Sphinx Guide
============

This document provides instructions for contributors on how to set up, customize, and run Sphinx to generate documentation for the **Biofilter** project. The project uses Sphinx to maintain two separate documentations: one for **user documentation** and another for **developer documentation**.

Documentation Structure
-----------------------

The Biofilter project maintains two distinct documentation directories:

1. **User Documentation**:
    - Located in the ``docs/`` directory.
    - Hosted on **ReadTheDocs**.
    - Contains user-facing guides, usage instructions, and detailed module documentation.

2. **Developer Documentation**:
    - Located in the ``docs-dev/`` directory.
    - Hosted on **GitHub Pages**.
    - Contains internal technical documentation for contributors, including guides on tools like Tox, Poetry, Black, and branching strategies.

Each directory has its own Sphinx configuration file (``conf.py``) and operates independently.


Prerequisites
-------------

Ensure you have the following tools installed:

- Python (>= 3.10)
- Poetry (manages dependencies, including Sphinx)

.. note::

    Sphinx is already included as a development dependency in the ``pyproject.toml`` file. You do not need to install it manually.


Working with Documentation
--------------------------

User Documentation (``docs/``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To build and preview the user documentation:

1. Navigate to the ``docs/`` directory:

    .. code-block:: bash

        cd docs

2. Build the HTML documentation:

    .. code-block:: bash

        make html

3. Open the generated documentation in a browser:

    .. code-block:: bash

        open _build/html/index.html

This documentation is automatically built and hosted on **ReadTheDocs**.


Developer Documentation (``docs-dev/``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To build and preview the developer documentation:

1. Navigate to the ``docs-dev/`` directory:

    .. code-block:: bash

        cd docs-dev

2. Build the HTML documentation:

    .. code-block:: bash

        make html

3. Open the generated documentation in a browser:

    .. code-block:: bash

        open _build/html/index.html

This documentation is hosted on **GitHub Pages**, and any updates are automatically deployed through a GitHub Actions workflow. 

.. important::

   The ``gh-pages`` branch is used exclusively for hosting the developer documentation. It is automatically managed by GitHub Actions and **should not be modified manually**.


Customizing Documentation
--------------------------

Configuration Files
~~~~~~~~~~~~~~~~~~~

Each documentation directory has its own ``conf.py`` file for configuration. Key sections to customize include:

1. **Project Information**:

    .. code-block:: python

        project = 'Biofilter'
        author = 'Ritchie Lab'
        release = '2.4.4'

2. **Path Settings**:

    Ensure modules are accessible to Sphinx:

    .. code-block:: python

        import os
        import sys
        sys.path.insert(0, os.path.abspath(".."))

3. **Extensions**:

    Add or modify Sphinx extensions as needed:

    .. code-block:: python

        extensions = [
            'sphinx.ext.autodoc',
            'sphinx.ext.viewcode',
            'sphinx.ext.napoleon',
            'sphinx_rtd_theme',
        ]

4. **HTML Theme**:

    Set the theme for the documentation:

    .. code-block:: python

        html_theme = 'sphinx_rtd_theme'  # For user documentation
        html_theme = 'sphinx_material'  # For developer documentation

Customizing and Regenerating
----------------------------

1. **Edit Index Files**:

    - ``docs/index.rst``: Entry point for user documentation.
    - ``docs-dev/index.rst``: Entry point for developer documentation.

    Add or modify sections and include additional ``.rst`` files using:

    .. code-block:: rst

        .. toctree::
            :maxdepth: 2
            :caption: Contents:

            usage-guide
            developer-guide

2. **Generate Module Documentation**:

    Use ``sphinx-apidoc`` to create ``.rst`` files for modules:

    .. code-block:: bash

        sphinx-apidoc -o . ../biofilter_modules

3. **Rebuild Documentation**:

    After making changes, rebuild the documentation:

    .. code-block:: bash

        make clean
        make html

Tips for Contributors
---------------------

1. **Docstrings**:
    Write detailed docstrings in your codebase. Use Google or NumPy style for compatibility with the ``napoleon`` extension.

2. **Test Locally**:
    Always test your changes locally by building the documentation before pushing updates.

3. **Branch Management**:
    - User documentation updates should be committed to the ``main`` branch.
    - Developer documentation updates should be committed to the ``development`` branch. GitHub Actions will handle the deployment to ``gh-pages``.


Troubleshooting
---------------

- **Sphinx Command Not Found**:

    Ensure you are in the Poetry environment:

    .. code-block:: bash

        poetry shell

- **Broken Links**:

    Check for broken links using:

    .. code-block:: bash

        make linkcheck

- **Missing Modules**:

    Verify module paths in ``conf.py`` under ``sys.path``.

Additional Resources
--------------------

- `Sphinx Documentation <https://www.sphinx-doc.org/en/master/>`_
- `Google Style Docstrings <https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings>`_
- `Napoleon Extension <https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html>`_

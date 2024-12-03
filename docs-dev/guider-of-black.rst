Black Guide
==========================

Black is a code formatter for Python that automatically reformats your code to ensure readability and standardization. This guide explains how to use Black within the project, considering its integration with Poetry and GitHub Actions.

Installation
------------

Black is already included in the project's development dependencies through Poetry. After running ``poetry install``, Black will be available in the project's virtual environment.

.. note::

    There is no need to install Black manually, as it is managed via Poetry.


Basic Usage
-----------

To format all Python files in your project, run the following command from the root directory:

.. code-block:: bash

    poetry run black .

This command applies Black's formatting rules to all ``.py`` files in the directory and its subdirectories.

Formatting a Specific File
^^^^^^^^^^^^^^^^^^^^^^^^^^

To format a specific file, specify the filename:

.. code-block:: bash

    poetry run black path/to/your_file.py  


Common Options
--------------

- **Check Mode**: To check if files would be reformatted without making changes, use:

    .. code-block:: bash

        poetry run black --check .

- **Line Length**: By default, Black limits lines to 88 characters. You can set a different line length:

    .. code-block:: bash

        poetry run black --line-length 100 .

- **Exclude Files**: To exclude specific files or directories from formatting, use:

    .. code-block:: bash

        poetry run black --exclude "migrations|env" .


Integration with CI/CD
----------------------

Black is an integral part of the project's CI/CD pipeline managed through GitHub Actions. This ensures that all code pushed to the repository adheres to the project's formatting standards. 

.. important::

    Always run ``poetry run black`` locally before committing changes to avoid inconsistencies during the CI/CD process.


Integrating Black with Pre-Commit Hooks
---------------------------------------

To ensure consistent formatting before commits, you can set up a pre-commit hook. The pre-commit hook ensures all staged files are formatted with Black.

1. First, ensure ``pre-commit`` is installed via Poetry:

    .. code-block:: bash

        poetry add --group dev pre-commit

2. Then, create a ``.pre-commit-config.yaml`` file in the root directory with this content:

    .. code-block:: yaml

        repos:
        - repo: https://github.com/psf/black
          rev: 23.1.0  # Use the latest Black version
          hooks:
          - id: black

3. Install the hook:

    .. code-block:: bash

        poetry run pre-commit install

With this setup, Black will automatically format your code before each commit.

Tips for Working with Black
---------------------------

- **Consistency**: Black enforces a uniform style, helping keep code consistent across the project.
- **Automation**: Black is part of the CI/CD pipeline, ensuring all pushed code is formatted correctly.
- **Editor Integration**: Black is supported by most IDEs and editors, including VS Code and PyCharm. Configure your editor to format files automatically with Black.

Further Documentation
----------------------

For more options and detailed documentation, visit the `Black GitHub repository <https://github.com/psf/black>`_.

Tox Guider
==========

Tox is a tool used to automate testing across multiple Python versions and environments. It ensures consistency and compatibility in your project's workflows.

This guide explains how to configure and use Tox for the **Biofilter** project.

Overview
--------

Tox allows you to:
- Test your project across multiple Python versions.
- Automate dependency installation and testing.
- Isolate environments to ensure reproducibility.

Setting Up Tox
--------------

Tox is already included as a development dependency in the **Biofilter** project. If you need to install it manually, use the following command:

.. code-block:: bash

    poetry add --group dev tox

Tox Configuration
------------------

Tox is configured via the `tox.ini` file in the root of the project. Below is an example configuration:

.. code-block:: ini

    [tox]
    envlist = py310, py311, py312

    [testenv]
    description = Run tests with pytest for {envname}
    deps = poetry
    commands =
        poetry install --no-root
        pytest tests/

**Key Sections in `tox.ini`**:

- ``[tox]``: Defines the environments to test (e.g., Python 3.10, 3.11, 3.12).
- ``[testenv]``: Specifies dependencies and commands to execute in each environment.

Using Tox
---------

### Running All Environments

To run tests across all configured Python versions, execute:

.. code-block:: bash

    poetry run tox

Tox will create isolated environments and execute the tests in each.

### Running Specific Environments

To test a specific Python version or configuration, use the `-e` flag:

.. code-block:: bash

poetry run tox -e py310 

### Recreating Environments

If you make changes to the `tox.ini` file or dependencies, recreate the environments:

.. code-block:: bash

    poetry run tox --recreate   

Customizing Tox
---------------

### Specifying Python Interpreter Paths

If Tox cannot find a specific Python version, specify its path in the `tox.ini` file:

.. code-block:: ini

    [testenv:py310]
    basepython = /path/to/python3.10

Replace `/path/to/python3.10` with the actual path of your Python interpreter.

### Parallel Execution

To speed up testing, you can run environments in parallel:

.. code-block:: bash

    poetry run tox -p auto  

### Adding Environment Variables

To pass environment variables during testing, add them to the `tox.ini` file:

.. code-block:: ini

    [testenv]
    passenv = MY_API_KEY
    commands =
        poetry install --no-root
        pytest tests/

Run Tox with the environment variable set:

.. code-block:: bash

    MY_API_KEY=your_api_key poetry run tox

Cleaning Up Tox
---------------

To clean up all Tox environments, remove the `.tox/` directory:

.. code-block:: bash

    rm -rf .tox/

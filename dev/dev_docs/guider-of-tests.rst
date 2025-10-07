Testing Guider
==============

This document outlines the structure, configuration, and execution of tests in the **Biofilter** project using Pytest. It also explains how tests are integrated into the development workflow and the CI/CD pipeline.

Test Structure
--------------

The tests are organized into the following directories under the ``tests/`` folder:

- **Unit Tests**:
    - Located in ``tests/unit/``.
    - Focused on small, isolated portions of the codebase, often using fixtures to provide mock data and parameters.

- **Functional Tests**:
    - Located in ``tests/functions/``.
    - Test specific functionalities to ensure they behave as expected in real-world scenarios.

- **Issues Tests**:
    - Located in ``tests/issues/``.
    - Designed to replicate and debug problems reported via GitHub Issues.
    - Create scenarios based on issue descriptions to verify fixes and identify potential regressions.

VS Code Integration
-------------------

Configuring VS-Code for Pytest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To integrate Pytest into VS Code, the ``settings.json`` file in your project’s ``.vscode/`` directory is configured to use Pytest as the default test framework:

.. code-block:: json

    {
        "python.testing.unittestEnabled": false,
        "python.testing.pytestEnabled": true,
        "python.testing.pytestArgs": ["tests"]
    }

With this configuration, you can:

- Discover all tests automatically in the ``tests/`` folder.
- Run individual tests or groups of tests directly within VS Code using the built-in testing features.

Running Tests in VS-Code
~~~~~~~~~~~~~~~~~~~~~~~~

1. Open the Testing sidebar in VS Code (Ctrl+Shift+T or Cmd+Shift+T).
2. All discovered tests will appear in the sidebar, organized by directories.
3. Click the play icon next to a test or folder to run individual tests or groups of tests.

Unit Tests and Fixtures
-----------------------

Unit Test Isolation
~~~~~~~~~~~~~~~~~~~

Unit tests focus on specific code components in isolation. To achieve this, we use **fixtures** that provide initial parameters, mock objects, or sample data for the tests. Fixtures help:

- Isolate the code being tested.
- Eliminate dependencies on external systems or databases.
- Ensure predictable, reproducible results.

Example Fixture
^^^^^^^^^^^^^^^

Here’s an example of a fixture in the project:

.. code-block:: python

    import pytest

    @pytest.fixture
    def sample_data():
        return {
            "param1": 42,
            "param2": "example"
        }

    def test_example_function(sample_data):
        result = example_function(sample_data["param1"])
        assert result == expected_result

Integration with CI/CD
----------------------

The Biofilter project integrates tests into the CI/CD pipeline using GitHub Actions. This ensures that every push and pull request is validated against the test suite.

Workflow for Tests
~~~~~~~~~~~~~~~~~~

1. **Automated Execution**:
    - On every commit or pull request to the ``development`` branch, the tests are executed in the GitHub Actions pipeline.

2. **Coverage Reports**:
    - Test coverage is measured during the pipeline execution and can be reviewed to identify untested code.

3. **Failure Alerts**:
    - If a test fails, the CI/CD pipeline is marked as failed, preventing unreviewed changes from being merged into stable branches.

Running Tests Locally
---------------------

To run the tests locally using Pytest, use the following commands:

Run All Tests
~~~~~~~~~~~~~

.. code-block:: bash

    poetry run pytest

Run Specific Tests
~~~~~~~~~~~~~~~~~~

- By directory:

    .. code-block:: bash

        poetry run pytest tests/unit/

- By file:

    .. code-block:: bash

        poetry run pytest tests/issues/test_issue_123.py

- By individual test function:

    .. code-block:: bash

        poetry run pytest -k "test_specific_function"

Best Practices
--------------

1. **Isolate Unit Tests**:
    Use fixtures to ensure that unit tests are not dependent on external systems or complex configurations.

2. **Link Issues Tests to GitHub**:
    Ensure that tests in ``tests/issues/`` reference the corresponding GitHub Issue for better traceability.

3. **Run Tests Locally Before Pushing**:
    Always run the full test suite locally to catch errors early.

4. **Maintain Test Coverage**:
    Strive for high test coverage to ensure the robustness of the codebase.

5. **Document Test Scenarios**:
    Document the purpose and setup of tests, especially for complex scenarios in ``tests/issues/``.


Additional Resources
--------------------

- `Pytest Documentation <https://docs.pytest.org/en/latest/>`_
- `GitHub Actions Documentation <https://docs.github.com/en/actions>`_

Coverage Guide
==============

To measure test coverage in the Biofilter project, we use `Coverage.py`. This guide outlines the steps to install, run, and generate coverage reports while considering the integration with Poetry and the CI/CD pipeline.

1. Install Coverage.py
-----------------------

`Coverage.py` is already included in the project's development dependencies via Poetry. To ensure it is installed, simply run:

.. code-block:: bash

   poetry install

This command installs all development dependencies, including `Coverage.py`.

.. note::

   There is no need to install `coverage` manually using `pip`, as it is managed through Poetry.

2. Run Tests with Coverage
--------------------------

In the Biofilter project directory, use `coverage` to run the tests and collect coverage data. Since we are using Poetry, the command is:

.. code-block:: bash

   poetry run coverage run -m pytest

This command uses `coverage` to execute `pytest`, running all tests and collecting information on which parts of the code are covered.

3. Generate a Coverage Report in the Terminal
---------------------------------------------

After running the tests, generate a coverage report in the terminal:

.. code-block:: bash

   poetry run coverage report -m

- The ``-m`` option displays which lines were not covered.

4. Generate an HTML Report
--------------------------

For a more detailed view, generate an HTML report:

.. code-block:: bash

   poetry run coverage html

This creates a directory called ``htmlcov`` with an ``index.html`` file. Open this file in a browser for a detailed view of the coverage.

5. Exclude Files or Lines from the Report (Optional)
----------------------------------------------------

To exclude specific files or certain lines (e.g., docstrings or debugging instructions), create a ``.coveragerc`` file in the root directory of the Biofilter project. Use the following example:

.. code-block:: ini

   [run]
   omit =
      tests/*
      setup.py

   [report]
   exclude_lines =
      # Exclude debug lines
      if __name__ == "__main__":
      pragma: no cover

This configuration ensures that coverage focuses only on the relevant parts of the codebase.

Integration with CI/CD
----------------------

Test coverage is also verified automatically in the CI/CD pipeline through GitHub Actions. This ensures that every change to the codebase maintains or improves test coverage. Before pushing changes, it is recommended to check the coverage locally using the commands outlined above.

.. important::

   Always verify coverage locally before committing to avoid failures during the CI/CD process.

Summary of Commands
-------------------

1. **Install dependencies**: (if not already installed) 

   .. code-block:: bash

      poetry install

2. **Run tests with coverage**: 

   .. code-block:: bash

      poetry run coverage run -m pytest

3. **Generate terminal report**: 

   .. code-block:: bash

      poetry run coverage report -m

4. **Generate HTML report**: 

   .. code-block:: bash

      poetry run coverage html



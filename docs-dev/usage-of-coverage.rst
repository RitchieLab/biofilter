Running Coverage in the Biofilter Project
=========================================

To measure test coverage in the Biofilter project, we use `Coverage.py`. This guide outlines the steps to install and run `coverage` and generate reports.

1. Install Coverage.py
-----------------------

Install `coverage` via pip:

.. code-block:: bash

   pip install coverage

2. Run Tests with Coverage
--------------------------

In the Biofilter project directory, use `coverage` to run the tests and collect coverage data:

.. code-block:: bash

   coverage run -m pytest

This command uses `coverage` to execute `pytest`, running all tests and collecting information on which parts of the code are covered.

3. Generate a Coverage Report in the Terminal
---------------------------------------------

After running the tests, generate a coverage report in the terminal:

.. code-block:: bash

   coverage report -m

- The ``-m`` option displays which lines were not covered.

4. Generate an HTML Report
--------------------------

For a more detailed view, generate an HTML report:

.. code-block:: bash

   coverage html

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

Summary of Commands
-------------------

1. **Install**: 

   .. code-block:: bash

      pip install coverage

2. **Run tests with coverage**: 

   .. code-block:: bash

      coverage run -m pytest

3. **Generate terminal report**: 

   .. code-block:: bash

      coverage report -m

4. **Generate HTML report**: 

   .. code-block:: bash

      coverage html

These steps will help you measure test coverage in the Biofilter project using `Coverage.py`.

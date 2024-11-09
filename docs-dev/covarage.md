# Running Coverage in the Biofilter Project

To measure test coverage in the Biofilter project, we can use `Coverage.py`. Follow these steps to install and run `coverage` to generate coverage reports.

### 1. Install Coverage.py

Install `coverage` via `pip`:
```bash
pip install coverage
```

### 2. Run Tests with Coverage

In Biofilter project directory, use `coverage` to run the tests and collect coverage data:

```bash
coverage run -m pytest
```

This command uses `coverage` to run `pytest`, executing all tests and collecting information on which parts of the code are covered.

### 3. Generate a Coverage Report in the Terminal

After running the tests, generate a coverage report in the terminal:

```bash
coverage report -m
```

- The `-m` option shows which lines were not covered.

### 4. Generate an HTML Report

For a more detailed view, generate an HTML report:

```bash
coverage html
```

This creates a directory called `htmlcov` with an `index.html` file. Open this file in a browser for a detailed view of the coverage.

### 5. Exclude Files or Lines from the Report (Optional)

If you want to exclude specific files or certain lines (such as docstrings or debugging instructions), create a `.coveragerc` file in Biofilter project’s root directory with the following example content:

```ini
[run]
omit =
    tests/*
    setup.py

[report]
exclude_lines =
    # Exclude debug lines
    if __name__ == .__main__.:
    pragma: no cover
```

### Summary of Commands

1. **Install**: `pip install coverage`
2. **Run tests with coverage**: `coverage run -m pytest`
3. **Generate terminal report**: `coverage report -m`
4. **Generate HTML report**: `coverage html`

These steps will help we start measuring test coverage in Biofilter project using `Coverage.py`.

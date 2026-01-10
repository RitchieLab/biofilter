GIT Guider
===========

This document provides instructions for contributors on how to effectively use Git in the **Biofilter** project. It includes steps for cloning the repository, working with branches, linking changes to GitHub Issues, using GitHub Actions, and other project-specific practices.

Getting Started
---------------

Clone the Repository
~~~~~~~~~~~~~~~~~~~~

To start working on the project, clone the GitHub repository to your local machine:

.. code-block:: bash

    git clone https://github.com/RitchieLab/biofilter.git
    cd biofilter


Switch to the Development Branch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The project follows a structured branching strategy, with active development happening on the ``development`` branch. Switch to this branch before making any changes:

.. code-block:: bash

    git checkout development

.. note::
    The ``main`` branch is reserved for stable, production-ready code. Always use ``development`` for feature work or bug fixes.


Working with Branches
---------------------

Create a New Feature or Bug Fix Branch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To avoid conflicts and maintain a clean workflow, create a new branch for each feature or bug fix:

.. code-block:: bash

    git checkout -b feature/<your-feature-name>

For bug fixes, use:

.. code-block:: bash

    git checkout -b bugfix/<your-bugfix-name>

Naming Conventions for Branches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Feature branches**: ``feature/<description>``
- **Bug fix branches**: ``bugfix/<description>``
- **Hotfix branches**: ``hotfix/<description>``

Example:

.. code-block:: bash

    git checkout -b feature/add-tox-tests


Linking Changes to GitHub Issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To link your changes to a GitHub Issue, reference the Issue number in your branch name or commit messages:

- Include the Issue number in your branch name:

    .. code-block:: bash

        git checkout -b feature/12-add-tox-tests

- Mention the Issue in your commit message:

    .. code-block:: bash

        git commit -m "Fixes #12: Add tox tests for Python 3.10 and 3.12"


Commit and Push Changes
------------------------

Commit Your Changes
~~~~~~~~~~~~~~~~~~~~

Once you've made changes, stage and commit them:

.. code-block:: bash

    git add .
    git commit -m "Brief description of your changes"

Push Your Changes to GitHub
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Push your branch to the remote repository:

.. code-block:: bash

    git push origin <your-branch-name>

Example:

.. code-block:: bash

    git push origin feature/add-tox-tests

Create a Pull Request (PR)
~~~~~~~~~~~~~~~~~~~~~~~~~~

After pushing your changes, open a Pull Request on GitHub to merge your branch into the ``development`` branch. Make sure to:
- Assign reviewers.
- Link any relevant Issues in the PR description.
- Provide a detailed summary of your changes.

GitHub Actions
--------------

The Biofilter project uses **GitHub Actions** for Continuous Integration (CI). These actions automatically validate your changes by running tests and building documentation. 

Key GitHub Actions
~~~~~~~~~~~~~~~~~~

- **Tests**: Runs `tox` across multiple Python versions.
- **Documentation Build**: Builds and deploys Sphinx documentation for both user and developer guides.

Workflow Triggers
~~~~~~~~~~~~~~~~~

Actions are triggered automatically when:
- A Pull Request is opened or updated.
- Changes are pushed to the ``development`` or ``main`` branches.

Reviewing Action Results
~~~~~~~~~~~~~~~~~~~~~~~~

After pushing your changes, check the **Actions** tab on GitHub to review the results. Address any failing checks before requesting a review.

Common Commands
---------------

Here’s a quick reference for common Git commands used in the project:

- **Check Current Branch**:

    .. code-block:: bash

        git branch

- **Fetch Latest Changes**:

    .. code-block:: bash

        git fetch

- **Pull Updates for a Branch**:

    .. code-block:: bash

        git pull origin <branch-name>

- **Delete a Local Branch**:

    .. code-block:: bash

        git branch -d <branch-name>

- **Delete a Remote Branch**:

    .. code-block:: bash

        git push origin --delete <branch-name>

Best Practices
--------------

1. **Always Pull Latest Changes**:
    Before starting any work, ensure your ``development`` branch is up to date:

    .. code-block:: bash

        git checkout development
        git pull origin development

2. **Commit Often**:
    Make small, incremental commits with descriptive messages.

3. **Keep Pull Requests Small**:
    Focus on one feature or fix per PR to make reviews easier.

4. **Run Tests Locally**:
    Always run tests locally before pushing changes:

    .. code-block:: bash

        poetry run tox

5. **Follow Branching Strategy**:
    Ensure you create branches from ``development`` and merge them back into ``development``.

Troubleshooting
---------------

- **Merge Conflicts**:
    If you encounter merge conflicts when pulling updates, resolve them manually and commit the resolved files.

- **Forgotten to Switch Branch**:
    If you accidentally commit to the wrong branch, create a new branch and reset the original:

    .. code-block:: bash

        git branch feature/fix-issue-123
        git reset --hard origin/development

- **Changes Not Reflecting**:
    Ensure you’ve staged and committed all changes:

    .. code-block:: bash

        git status

Additional Resources
--------------------

- `Git Documentation <https://git-scm.com/doc>`_
- `GitHub Actions <https://docs.github.com/en/actions>`_

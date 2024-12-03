Branching Structure Guide
==========================

This project follows a structured branching strategy to manage different versions and stages of development efficiently. Here’s a breakdown of each main branch and its purpose.

Branches
--------

### Main Branch: ``main``

The ``main`` branch represents the stable, production-ready version of the codebase. It contains the latest official release of the project. Code in ``main`` is fully tested and approved for production use.

- **Current Version**: 2.4.3
- **Purpose**: Serves as the default branch for stable releases.
- **Merge Strategy**: Only merges from fully tested and verified branches (such as ``development``) are allowed.

### Development Branch: ``development``

The ``development`` branch is where active development for minor versions takes place. Features, bug fixes, and improvements for the upcoming stable release are integrated here before they are fully tested and merged into ``main``.

- **Current Version**: 2.4.4 (in-progress)
- **Purpose**: Acts as the integration branch for ongoing work and testing of the next stable release.
- **Merge Strategy**: Developers create feature branches off ``development`` and merge completed features back into ``development`` after testing.

### Feature Branch: ``dev-3.0.0``

The ``dev-3.0.0`` branch is dedicated to developing and testing major updates or changes that are not yet stable enough for integration with ``main``. This branch includes work on significant new features and alterations, such as the introduction of new data sources and schema changes in the database.

- **Current Version**: 3.0.0 (under development)
- **Purpose**: Used for ongoing work on the next major version, which includes substantial updates like new data sources and modifications to the database schema.
- **Merge Strategy**: Only merged into ``main`` after extensive testing and stability checks. May include code merged from ``development`` if the major version requires updates from minor releases.
- **Versioning Note**: This branch was initially created from version 2.4.3. Any updates beyond 2.4.3 should be reflected in this branch to maintain continuity.

Workflow Summary
----------------

1. **Feature Development**: All new features and bug fixes are developed in separate branches off of ``development``.
2. **Testing and Integration**: When a feature is complete, it is merged into ``development`` for testing and integration with other recent changes.
3. **Stable Release Preparation**: Once the ``development`` branch is stable and ready for release, it is merged into ``main``, creating an official, stable release.
4. **Major Version Development**: Experimental or significant changes go into ``dev-3.0.0`` until they are ready for production.

Branching Best Practices
-------------------------

- **Keep ``main`` stable**: Only tested, production-ready code should be merged into ``main``.
- **Use Feature Branches**: For each new feature or fix, create a branch off ``development``. This keeps ``development`` organized and easier to manage.
- **Document and Review Changes**: Always document significant changes to the codebase. Use pull requests to review and approve changes before merging into shared branches.

Conclusion
----------

By following this structure, we ensure that our project remains organized and that each branch serves a distinct purpose in the development workflow.

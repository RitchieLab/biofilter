# Biofilter v2.4.4 (Development Branch)

**Version**: 2.4.4
**Status**: Actual 
**Focus**: Refactoring and alignment with best practices

## Project Overview

This branch represents an ongoing development effort to improve the Biofilter software, aligning it with modern best practices, refactoring key components for modularity and readability, and enhancing integration with the Loki database. Our goal is to provide a cleaner, more efficient, and testable Biofilter that supports CI/CD and streamlined development workflows.

---

## Objectives for Version 2.4.4

### 1. Refactoring and Modularization
- Reorganization of the codebase into logical modules and components, improving readability and maintainability.
- Enhanced modularity to facilitate the addition of new features and simplify debugging.
  
### 2. Improved Testing
- **Unit Testing**: Expanded coverage of unit tests to validate individual functions and methods.
- **Integration Testing**: Comprehensive tests that validate proper integration with the Loki database.
- **Testing Tools**: Set up `pytest` as the primary testing framework, configured with necessary fixtures and setup files.
  
### 3. Integration with Loki Database
- Refined integration between Biofilter and Loki to ensure data consistency and query reliability.
- Added support for configurations specific to Loki, enhancing flexibility for users working with multiple database versions.

### 4. CI/CD and Code Formatting
- **Continuous Integration/Continuous Deployment (CI/CD)**: Implementation of automated testing and deployment workflows.
- **PEP8 Compliance**: Code formatted with `black` for consistency with PEP8 standards.
- **Automated Linting**: Configuration of `black` and other linters as part of the CI/CD pipeline.

---

## Development Documentation in:

https://ritchielab.github.io/biofilter/
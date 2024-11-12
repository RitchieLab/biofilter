# Biofilter v2.4.4 (Development Branch)

**Version**: 2.4.4
**Status**: In Development  
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

## Getting Started

### Prerequisites
- Python 3.8 or higher
- Loki database set up and initialized
- `pip` package manager

### Installation

Clone the repository and switch to the development branch:
```bash
git clone https://github.com/yourusername/biofilter.git
cd biofilter
git checkout development
```

#### Install the required packages in development mode:

```bash
pip install -e .
```

## Usage
- Running Biofilter
- Use the biofilter command-line tool for analysis:

```bash
biofilter --knowledge path/to/loki.db --other-options
```

For more details on command-line options, use:

```bash
biofilter --help
```

Initializing Loki
Ensure that Loki is correctly set up before using Biofilter:

```bash
loki-build --knowledge path/to/loki.db --update
```

### Running Tests
Unit and integration tests can be run with pytest:

```bash
pytest
```

#### To check code formatting compliance:

```bash
black --check .
```

### Development Workflow
Clone the development branch.
Implement changes, ensuring alignment with project objectives.
Run unit and integration tests.
Submit pull requests for code review.
Contributing
Contributions to the Biofilter project are welcome. Please follow our contribution guidelines and ensure all new code follows the standards outlined above.

License
This project is licensed under the MIT License. See LICENSE for details.

Acknowledgments
Special thanks to the Ritchie Lab for their contributions to the Biofilter and Loki projects.

```javascript
Este `README.md` fornece um guia de desenvolvimento e alinhamento com as melhores práticas para a versão 3.1.0 do Biofilter e facilita a colaboração e integração contínua.
```
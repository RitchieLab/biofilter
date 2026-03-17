# AGENTS - API/CLI

This file defines local guidelines for evolving the Biofilter CLI.

## Objective
- Keep the CLI predictable, API-aligned, and easy to use.
- Avoid regressions in commands documented in `ag_01_commands.md`.

## Implementation Rules
- Every CLI command change must map to the corresponding component in `biofilter/core/components`.

Whenever you add/remove/rename a command or argument:
1. update `ag_01_commands.md`;
2. update relevant integration tests in `tests/integration/cli/`;
3. validate error behavior (`click.UsageError` / `click.ClickException`) when applicable.

## Design Pattern
- Groups registered in `main.py`: `db`, `report`, `etl`, `config`.
- Global `--db-uri` can be overridden by command-local `--db-uri`.
- Avoid hardcoded environment values (URI, absolute paths, credentials).
- Prefer short and clear output messages.

## Quick Checklist Before Finishing
- Command is registered in `biofilter/api/cli/main.py`.
- Command help text and required arguments are coherent.
- Equivalent API method exists in the corresponding component.
- Integration tests cover happy path and main error path.

## References
- Command map: `biofilter/api/cli/ag_01_commands.md`
- CLI entrypoint: `biofilter/api/cli/main.py`
- Groups: `biofilter/api/cli/groups/`

# biofilter/api/cli/groups/report.py
from __future__ import annotations

import ast
import csv
import difflib
import json
from pathlib import Path
from typing import Any

import click

from biofilter.api.cli.common import local_db_uri_option, require_db_uri
from biofilter.biofilter import Biofilter


@click.group()
def report():
    """Run and manage reports."""
    pass


def _safe_report_names(bf) -> list[str]:
    try:
        rows = bf.report.list(verbose=False)  # type: ignore[arg-type]
    except TypeError:
        rows = bf.report.list()
    except Exception:
        return []

    names = [str(r.get("name", "")).strip() for r in rows if r.get("name")]
    return sorted(set(names))


def _friendly_not_found_message(bf, identifier: str) -> str:
    available = _safe_report_names(bf)
    lines = [f"Report not found: '{identifier}'."]

    if available:
        matches = difflib.get_close_matches(identifier, available, n=3, cutoff=0.45)
        if matches:
            lines.append(f"Did you mean: {', '.join(matches)}?")
        lines.append("Use `biofilter report list` to see all available reports.")
        lines.append(f"Available reports: {', '.join(available)}")
    else:
        lines.append("Use `biofilter report list` to see available reports.")
    return "\n".join(lines)


def _raise_report_cli_error(bf, identifier: str, exc: Exception, action: str) -> None:
    msg = str(exc) or exc.__class__.__name__
    if "Report not found" in msg:
        raise click.ClickException(_friendly_not_found_message(bf, identifier))
    raise click.ClickException(f"Could not {action} report '{identifier}': {msg}")


def _coerce_inline_value(value: str) -> Any:
    lowered = value.lower()

    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None

    for parser in (json.loads, ast.literal_eval):
        try:
            return parser(value)
        except Exception:
            pass

    return value


def _parse_yaml_text(text: str, source_label: str) -> Any:
    try:
        import yaml
    except Exception as e:
        raise click.UsageError("YAML support requires `pyyaml` installed.") from e

    try:
        return yaml.safe_load(text)
    except Exception as e:
        raise click.UsageError(f"Invalid YAML in {source_label}: {e}") from e


def _normalize_params_payload(data: Any) -> dict[str, Any]:
    if data is None:
        return {}
    if isinstance(data, dict):
        return data
    return {"input_data": data}


def _load_param_value_from_ref(path_ref: str) -> Any:
    file_path = Path(path_ref).expanduser()
    if not file_path.exists():
        raise click.UsageError(f"Param file not found: {file_path}")
    if file_path.is_dir():
        raise click.UsageError(f"Param file must be a file, got directory: {file_path}")

    suffix = file_path.suffix.lower()
    text = file_path.read_text(encoding="utf-8")

    if suffix == ".json":
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise click.UsageError(f"Invalid JSON in param file '{file_path}': {e}") from e
    if suffix in {".yml", ".yaml"}:
        return _parse_yaml_text(text, source_label=f"param file '{file_path}'")
    if suffix == ".csv":
        return _load_inputs_from_csv(file_path, input_column=None)

    stripped = text.strip()
    if not stripped:
        return ""

    for parser in (json.loads, ast.literal_eval):
        try:
            return parser(stripped)
        except Exception:
            pass

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) > 1:
        return lines
    return stripped


def _coerce_param_value(raw: str) -> Any:
    value = raw.strip()

    if value.startswith("@@"):
        return value[1:]
    if value.startswith("@"):
        return _load_param_value_from_ref(value[1:].strip())

    return _coerce_inline_value(value)


def _parse_param_values(param_values: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in param_values:
        if "=" not in item:
            raise click.UsageError(
                f"Invalid --param '{item}'. Use KEY=VALUE format."
            )
        key, raw_value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise click.UsageError(
                f"Invalid --param '{item}'. Parameter name cannot be empty."
            )
        out[key] = _coerce_param_value(raw_value)
    return out


def _load_inputs_from_text(path: Path) -> list[str]:
    values = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return [v for v in values if v]


def _load_inputs_from_csv(path: Path, input_column: str | None) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)

        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample or ",")
            has_header = sniffer.has_header(sample) if sample else True
        except Exception:
            dialect = csv.excel
            has_header = True

        if input_column:
            column = input_column.strip()
            if column.isdigit():
                idx = int(column)
                reader = csv.reader(f, dialect=dialect)
                if has_header:
                    next(reader, None)
                out = []
                for row in reader:
                    if idx < len(row):
                        value = str(row[idx]).strip()
                        if value:
                            out.append(value)
                return out

            reader = csv.DictReader(f, dialect=dialect)
            fieldnames = reader.fieldnames or []
            if column not in fieldnames:
                raise click.UsageError(
                    f"Column '{column}' not found in CSV. Available: {fieldnames}"
                )
            out = []
            for row in reader:
                value = str(row.get(column, "")).strip()
                if value:
                    out.append(value)
            return out

        reader = csv.reader(f, dialect=dialect)
        if has_header:
            next(reader, None)
        out = []
        for row in reader:
            if not row:
                continue
            value = str(row[0]).strip()
            if value:
                out.append(value)
        return out


def _load_inputs_from_file(path: str, input_column: str | None) -> list[str]:
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return _load_inputs_from_csv(file_path, input_column=input_column)
    if input_column:
        raise click.UsageError("--input-column is only valid with CSV input files.")
    return _load_inputs_from_text(file_path)


def _load_params_from_file(path: str) -> dict[str, Any]:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    suffix = file_path.suffix.lower()

    if suffix == ".json":
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise click.UsageError(f"Invalid JSON in --params-file: {e}") from e
    elif suffix in {".yml", ".yaml"}:
        data = _parse_yaml_text(text, source_label="--params-file")
    else:
        raise click.UsageError(
            "Unsupported --params-file extension. Use .json, .yml, or .yaml."
        )

    return _normalize_params_payload(data)


def _build_run_kwargs(
    *,
    input_values: tuple[str, ...],
    input_file: str | None,
    input_column: str | None,
    param_values: tuple[str, ...],
    params_json: str | None,
    params_file: str | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}

    if params_file:
        kwargs.update(_load_params_from_file(params_file))

    if params_json:
        try:
            data = json.loads(params_json)
        except json.JSONDecodeError as e:
            raise click.UsageError(f"Invalid JSON in --params-json: {e}") from e
        kwargs.update(_normalize_params_payload(data))

    kwargs.update(_parse_param_values(param_values))

    explicit_inputs_provided = bool(input_file or input_values)
    input_param_keys = {"input_data", "items", "input_path"}
    conflicting_keys = sorted(k for k in kwargs.keys() if k in input_param_keys)
    if explicit_inputs_provided and conflicting_keys:
        raise click.UsageError(
            "Input conflict: use --input/--input-file for inputs and keep --param for report options. "
            f"Conflicting keys: {', '.join(conflicting_keys)}"
        )

    file_inputs: list[str] = []
    if input_file:
        file_inputs = _load_inputs_from_file(input_file, input_column=input_column)

    direct_inputs = [str(v).strip() for v in input_values if str(v).strip()]
    merged_inputs = [*file_inputs, *direct_inputs]
    if merged_inputs:
        kwargs["input_data"] = merged_inputs

    return kwargs


# TESTADO
@report.command("list")
@local_db_uri_option
@click.option("--verbose", is_flag=True, help="Show descriptions and module names.")  # noqa E501
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def list_(ctx, db_uri, verbose, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    rows = bf.report.list(verbose=False)  # returns list[dict]
    if not rows:
        click.echo("No reports found.")
        return

    click.echo("📊 Available Reports:\n")
    for i, r in enumerate(rows, start=1):
        name = r.get("name", "")
        desc = r.get("description", "") or ""
        module = r.get("module", "") or ""

        click.echo(f"{i}. {name}")
        if verbose:
            if desc:
                click.echo(f"   {desc}")
            if module:
                click.echo(f"   module: {module}")
        click.echo("")


# TESTADO
@report.command("explain")
@local_db_uri_option
@click.option(
    "--report-name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name).",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def explain(ctx, db_uri, identifier, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    try:
        text = bf.report.explain(identifier)
    except Exception as e:
        _raise_report_cli_error(bf, identifier, e, action="explain")
    click.echo(text)


# TESTADO
@report.command("example-input")
@local_db_uri_option
@click.option(
    "--report-name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name).",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def example_input(ctx, db_uri, identifier, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    try:
        text = bf.report.example_input(identifier)
    except Exception as e:
        _raise_report_cli_error(bf, identifier, e, action="load example input for")
    click.echo(text)


# TESTADO
@report.command("available-columns")
@local_db_uri_option
@click.option(
    "--report-name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name).",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def available_columns(ctx, db_uri, identifier, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    try:
        text = bf.report.available_columns(identifier, print_output=False)
    except Exception as e:
        _raise_report_cli_error(bf, identifier, e, action="list columns for")
    click.echo(text)


@report.command("refresh")
@local_db_uri_option
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def refresh(ctx, db_uri, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.report.refresh()
    click.echo("✅ Report cache refreshed.")


@report.command("run")
@local_db_uri_option
@click.option(
    "--report-name",
    "--name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name). Alias: --name",
)
@click.option(
    "--params-template",
    is_flag=True,
    help="Print report example_input as JSON and exit.",
)
@click.option(
    "--input",
    "input_values",
    multiple=True,
    help="Direct input value. Repeat to pass multiple values.",
)
@click.option(
    "--input-file",
    type=click.Path(exists=True, dir_okay=False),
    help="Input file path (.txt with one value per line, or .csv).",
)
@click.option(
    "--input-column",
    help="CSV column name (or zero-based index) to read when using --input-file.",
)
@click.option(
    "--param",
    "param_values",
    multiple=True,
    help="Report parameter in KEY=VALUE format. Use KEY=@path to load from file.",
)
@click.option(
    "--params-json",
    help="JSON object string with report parameters.",
)
@click.option(
    "--params-file",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to JSON/YAML file with report parameters.",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False),
    help="Output CSV file path. If provided, exports instead of printing.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def run(
    ctx,
    db_uri,
    identifier,
    params_template,
    input_values,
    input_file,
    input_column,
    param_values,
    params_json,
    params_file,
    output,
    debug,
):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    if params_template:
        try:
            template = bf.report.example_input(identifier)
        except Exception as e:
            _raise_report_cli_error(bf, identifier, e, action="load params template for")

        if template is None:
            template = {}
        try:
            payload = json.dumps(template, indent=2, ensure_ascii=False)
        except TypeError:
            payload = json.dumps(str(template), indent=2, ensure_ascii=False)
        click.echo(payload)
        return

    report_kwargs = _build_run_kwargs(
        input_values=input_values,
        input_file=input_file,
        input_column=input_column,
        param_values=param_values,
        params_json=params_json,
        params_file=params_file,
    )

    try:
        df = bf.report.run(identifier, **report_kwargs)
    except Exception as e:
        _raise_report_cli_error(bf, identifier, e, action="run")

    if output:
        df.to_csv(output, index=False)
        click.echo(f"✅ Report exported to: {output}")
    else:
        click.echo(df.to_string(index=False))

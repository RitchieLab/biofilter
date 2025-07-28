# import toml
import click
import inspect

# from pathlib import Path
from biofilter.biofilter import Biofilter
from biofilter.utils.version import __version__ as current_version
from biofilter.utils.config import get_db_uri_from_config

# def get_default_db_uri():
#     config_path = Path(".biofilter.toml")
#     if config_path.exists():
#         try:
#             data = toml.load(config_path)
#             return data.get("biofilter", {}).get("db_uri")
#         except Exception:
#             pass
#     return None


# === Base group ===
@click.group(
    help=f"""
        Biofilter CLI - Omics Knowledge Platform

        🔢 Version: {current_version} \n
        📚 Docs: https://xxxxxxxx
    """,
    context_settings=dict(help_option_names=["--help"]),
)
def main():
    # """Biofilter CLI - Omics Knowledge Platform."""
    pass


# === Subgroup: project ===
@main.group()
def project():
    """Project-level operations (setup, migration, metadata)."""
    pass


@project.command("create")
@click.option("--db-uri", required=True, help="Database URI")
@click.option("--overwrite", is_flag=True, help="Overwrite if exists")
def create_new_project(db_uri, overwrite):
    """Create a new Biofilter project (initializes DB)."""
    bf = Biofilter()
    bf.create_new_project(db_uri=db_uri, overwrite=overwrite)


@project.command("migrate")
# @click.option("--db-uri", required=True, help="Database URI to migrate")
@click.option("--db-uri", help="Database URI to migrate")
def migrate(db_uri):
    """Run database migrations."""
    if not db_uri:

        db_uri = get_db_uri_from_config()
        if not db_uri:
            raise click.UsageError(
                "Database URI not provided and no .biofilter.toml found.\n"
                "Use --db-uri or create a .biofilter.toml with a [database] section."  # noqa: E501
            )

    bf = Biofilter(db_uri=db_uri)
    bf.migrate()


# === Subgroup: etl ===
@main.group()
def etl():
    """Run and manage ETL pipelines."""
    pass


# === Helpers for dynamic registration ===
def convert_type(param):
    if param.annotation is bool:
        return click.BOOL
    elif param.annotation is int:
        return click.INT
    elif param.annotation is float:
        return click.FLOAT
    elif param.annotation in (list, list[str]):
        return click.STRING
    else:
        return click.STRING


def auto_register_etl_commands():
    from biofilter.biofilter import Biofilter as BiofilterClass

    for name, method in inspect.getmembers(
        BiofilterClass, predicate=inspect.isfunction
    ):  # noqa: E501
        if name.startswith("_"):
            continue
        if name in ("create_new_project", "migrate", "__repr__", "connect_db"):
            continue

        sig = inspect.signature(method)

        def create_cmd(method=method, sig=sig):
            @click.pass_context
            def command(ctx, **kwargs):

                db_uri = kwargs.pop("db_uri", None)
                if not db_uri:
                    db_uri = get_db_uri_from_config()
                if not db_uri:
                    raise click.UsageError(
                        "Missing required --db-uri. Use option or set in .biofilter.toml"
                    )  # noqa: E501

                bf = Biofilter(db_uri=db_uri)
                result = method(bf, **kwargs)
                if result is not None:
                    click.echo(result)

            for param in reversed(sig.parameters.values()):
                if param.name == "self":
                    continue

                param_name = f"--{param.name.replace('_', '-')}"
                click_type = convert_type(param)
                is_flag = param.annotation is bool

                if param.default is inspect.Parameter.empty:
                    command = click.option(
                        param_name,
                        required=True,
                        type=click_type,
                        help=f"{param.name}",
                    )(command)
                else:
                    command = click.option(
                        param_name,
                        default=param.default,
                        type=click_type,
                        is_flag=is_flag,
                        show_default=True,
                        help=f"{param.name}",
                    )(command)

            # Required --db-uri
            # command = click.option(
            #     "--db-uri",
            #     required=True,
            #     type=click.STRING,
            #     help="Database URI to connect",
            # )(command)
            # Required --db-uri (optional if .biofilter.toml exists)
            command = click.option(
                "--db-uri",
                required=False,
                type=click.STRING,
                help="Database URI to connect (or set in .biofilter.toml)",
            )(command)

            return command

        # Register inside the `etl` group
        etl.command(name=name)(create_cmd())


# Register all ETL-related commands dynamically
auto_register_etl_commands()


"""
biofilter project create --db-uri sqlite:///biofilter.sqlite --overwrite
biofilter project migrate --db-uri sqlite:///biofilter.sqlite
biofilter etl update --db-uri sqlite:///biofilter.sqlite --source-system HGNC
"""


# === Subgroup: report ===


@main.group()
def report():
    """Project-level operations (setup, migration, metadata)."""
    pass


@report.command("list")
@click.option("--db-uri", required=False, help="Database URI or config file")
def list_reports(db_uri):
    """List available reports."""
    if not db_uri:
        db_uri = get_db_uri_from_config()
        if not db_uri:
            raise click.UsageError(
                "Database URI not provided and no .biofilter.toml found.\n"
                "Use --db-uri or create a .biofilter.toml with a [database] section."  # noqa: E501
            )

    bf = Biofilter(db_uri=db_uri)
    reports = bf.list_reports()
    click.echo("📊 Available Reports:")
    for r in reports:
        click.echo(f" - {r}")


@report.command("run")
@click.option(
    "--name", required=True, help="Report name (e.g., qry_etl_status)"
)  # noqa: E501
@click.option(
    "--db-uri", required=False, help="Database URI or use .biofilter.toml"
)  # noqa: E501
@click.option("--as-csv", is_flag=True, help="Export to CSV")
@click.option("--output", type=click.Path(), help="Output file path")
def run_report(name, db_uri, as_csv, output):
    """Run a specific report by name."""
    if not db_uri:
        db_uri = get_db_uri_from_config()
        if not db_uri:
            raise click.UsageError(
                "Database URI not provided and no .biofilter.toml found.\n"
                "Use --db-uri or create a .biofilter.toml with a [database] section."  # noqa: E501
            )

    bf = Biofilter(db_uri=db_uri)
    result = bf.run_report(name=name, as_dataframe=True)

    if as_csv:
        if not output:
            raise click.UsageError("Must provide --output with --as-csv")
        result.to_csv(output, index=False)
        click.echo(f"✅ Report exported to: {output}")
    else:
        click.echo(result.to_string(index=False))

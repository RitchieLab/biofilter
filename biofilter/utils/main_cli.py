import click
import inspect
from biofilter.biofilter import Biofilter


# === Base group ===
@click.group()
def main():
    """Biofilter CLI - Omics Knowledge Platform."""
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
@click.option("--db-uri", required=True, help="Database URI to migrate")
def migrate(db_uri):
    """Run database migrations."""
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

    for name, method in inspect.getmembers(BiofilterClass, predicate=inspect.isfunction):
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
                    raise click.UsageError("Missing required --db-uri")

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
            command = click.option(
                "--db-uri",
                required=True,
                type=click.STRING,
                help="Database URI to connect",
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
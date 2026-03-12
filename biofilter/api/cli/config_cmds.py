from __future__ import annotations

from pathlib import Path
import click
import toml  # third-party (read + write)

TEMPLATE = """\
[database]
db_uri = ""              # e.g. "sqlite:///biofilter.db" or "postgresql+psycopg2://..."
echo_sql = false         # SQLAlchemy echo (debug)
auto_create = false      # Create DB if it doesn't exist yet

[etl]
data_root = "./biofilter_data"
allow_parallel = true
max_workers = 8

[reports]
default_output_format = "dataframe"  # dataframe|csv|parquet
warn_on_empty = true

[logging]
level = "INFO"           # DEBUG|INFO|WARNING|ERROR|CRITICAL
log_to_file = true
log_file = "./biofilter.log"
"""


def _resolve_cfg_path(dirpath: str | None = None) -> Path:
    """
    Resolve config file path.

    - If dirpath is provided: use <dirpath>/.biofilter.toml
    - Else: use ./ .biofilter.toml
    """
    base = Path(dirpath).expanduser().resolve() if dirpath else Path.cwd()
    cfg_path = base / ".biofilter.toml"
    return cfg_path


def _find_config_file(dirpath: str | None = None) -> Path:
    cfg_path = _resolve_cfg_path(dirpath)
    if not cfg_path.exists():
        hint = f" (looked in {cfg_path.parent})" if dirpath else ""
        raise click.UsageError(
            f".biofilter.toml not found{hint}. Run `biofilter config init` first."
        )
    return cfg_path


def _parse_key(key: str) -> tuple[str, str]:
    if "." not in key:
        raise click.UsageError(
            "Invalid key format. Use SECTION.KEY (e.g. database.db_uri)"
        )
    section, name = key.split(".", 1)
    section = section.strip()
    name = name.strip()
    if not section or not name:
        raise click.UsageError("Invalid key format. Use SECTION.KEY")
    return section, name


def _infer_value(value: str):
    v = value.strip()

    # Unwrap simple quotes: "x" or 'x'
    if (v.startswith('"') and v.endswith('"')) or (
        v.startswith("'") and v.endswith("'")
    ):
        v = v[1:-1]

    vl = v.lower()
    if vl == "true":
        return True
    if vl == "false":
        return False

    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        pass

    return v


@click.group("config")
def config():
    """Manage the .biofilter.toml configuration file."""
    pass


@config.command("init")
@click.option(
    "--path", "dirpath", default=".", show_default=True, help="Target directory."
)
@click.option(
    "--force", is_flag=True, help="Overwrite existing .biofilter.toml if it exists."
)
@click.option("--db-uri", default=None, help="Pre-fill database.db_uri.")
@click.option("--data-root", default=None, help="Pre-fill etl.data_root.")
def config_init(dirpath, force, db_uri, data_root):
    """Create a .biofilter.toml template in the target directory."""
    target_dir = Path(dirpath).expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = target_dir / ".biofilter.toml"

    if cfg_path.exists() and not force:
        raise click.UsageError(f"{cfg_path} already exists. Use --force to overwrite.")

    content = TEMPLATE
    if db_uri is not None:
        content = content.replace('db_uri = ""', f'db_uri = "{db_uri}"', 1)
    if data_root is not None:
        content = content.replace(
            'data_root = "./biofilter_data"', f'data_root = "{data_root}"', 1
        )

    cfg_path.write_text(content, encoding="utf-8")
    click.echo(f"✅ Created: {cfg_path}")


@config.command("get")
@click.argument("key")
@click.option(
    "--path",
    "dirpath",
    default=None,
    help="Directory containing .biofilter.toml (default: current dir).",
)
def config_get(key, dirpath):
    """Get a configuration value (SECTION.KEY)."""
    cfg_path = _find_config_file(dirpath)
    section, name = _parse_key(key)

    data = toml.load(str(cfg_path))

    if section not in data or name not in data.get(section, {}):
        raise click.ClickException(f"{key} is not set")

    click.echo(data[section][name])


@config.command("set")
@click.argument("key")
@click.argument("value")
@click.option(
    "--path",
    "dirpath",
    default=None,
    help="Directory containing .biofilter.toml (default: current dir).",
)
def config_set(key, value, dirpath):
    """Set a configuration value (SECTION.KEY VALUE)."""
    cfg_path = _find_config_file(dirpath)
    section, name = _parse_key(key)

    data = toml.load(str(cfg_path))

    if section not in data:
        raise click.ClickException(f"Section [{section}] does not exist")

    data[section][name] = _infer_value(value)

    with cfg_path.open("w", encoding="utf-8") as f:
        toml.dump(data, f)

    click.echo(f"✅ Set {section}.{name} = {data[section][name]}")

# To drop

# import click

# from biofilter.biofilter import Biofilter
# from biofilter.utils.version import __version__ as current_version
# from biofilter.cli.common import resolve_db_uri, db_uri_option


# @click.group(
#     help=f"""
# Biofilter 4 CLI - Omics Knowledge Platform

# 🔢 Version: {current_version}
# 📚 Docs: https://xxxxxxxx
# """,
#     context_settings=dict(help_option_names=["--help"]),
# )
# def main():
#     pass


# # -----------------------
# # project
# # -----------------------
# @main.group()
# def project():
#     """Project-level operations (setup, migration, metadata)."""
#     pass


# @project.command("create")
# @click.option("--db-uri", required=True, help="Database URI")
# @click.option("--overwrite", is_flag=True, help="Overwrite if exists")
# def project_create(db_uri, overwrite):
#     bf = Biofilter(debug_mode=False)
#     bf.create_new_project(db_uri=db_uri, overwrite=overwrite)


# @project.command("migrate")
# @db_uri_option
# def project_migrate(db_uri):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)
#     bf.migrate()


# # -----------------------
# # etl
# # -----------------------
# @main.group()
# def etl():
#     """Run and manage ETL pipelines."""
#     pass


# @etl.command("update")
# @db_uri_option
# @click.option(
#     "--source-system",
#     multiple=True,
#     help="Source system name (repeatable). Example: --source-system HGNC",
# )
# @click.option(
#     "--data-source",
#     multiple=True,
#     help="Data source name (repeatable). Example: --data-source hgnc_genes",
# )
# @click.option(
#     "--run-step",
#     multiple=True,
#     type=click.Choice(["extract", "transform", "load"], case_sensitive=False),
#     help="ETL step to run (repeatable). Default: all steps (None).",
# )
# @click.option(
#     "--force-step",
#     multiple=True,
#     type=click.Choice(["extract", "transform", "load"], case_sensitive=False),
#     help="ETL step to force (repeatable). Default: none.",
# )
# def etl_update(db_uri, source_system, data_source, run_step, force_step):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)

#     bf.update(
#         source_system=list(source_system) or None,
#         data_sources=list(data_source) or None,
#         run_steps=list(run_step) or None,
#         force_steps=list(force_step) or None,
#     )


# @etl.command("restart")
# @db_uri_option
# @click.option(
#     "--data-source",
#     multiple=True,
#     help="Data source name (repeatable).",
# )
# @click.option(
#     "--source-system",
#     multiple=True,
#     help="Source system name (repeatable).",
# )
# @click.option(
#     "--delete-files",
#     is_flag=True,
#     help="Delete downloaded/processed files when restarting.",
# )
# def etl_restart(db_uri, data_source, source_system, delete_files):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)

#     bf.restart_etl(
#         data_source=list(data_source) or None,
#         source_system=list(source_system) or None,
#         delete_files=delete_files,
#     )


# @etl.command("update-conflicts")
# @db_uri_option
# @click.option(
#     "--source-system",
#     multiple=True,
#     help="Source system name (repeatable).",
# )
# def etl_update_conflicts(db_uri, source_system):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)

#     bf.update_conflicts(source_system=list(source_system) or None)


# # -----------------------
# # index
# # -----------------------
# @main.group()
# def index():
#     """Index management (drop/create/rebuild)."""
#     pass


# @index.command("rebuild")
# @db_uri_option
# @click.option(
#     "--group",
#     "groups",
#     multiple=True,
#     help="Index group (repeatable). If omitted, rebuilds all groups.",
# )
# @click.option("--drop-only", is_flag=True, help="Only drop indexes, do not create.")
# @click.option("--no-drop-first", is_flag=True, help="Do not drop before creating.")
# @click.option("--no-write-mode", is_flag=True, help="Disable DB write-mode tuning hooks.")
# @click.option("--no-read-mode", is_flag=True, help="Disable DB read-mode tuning hooks.")
# def index_rebuild(db_uri, groups, drop_only, no_drop_first, no_write_mode, no_read_mode):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)

#     ok, msg = bf.rebuild_indexes(
#         groups=list(groups) or None,
#         drop_only=drop_only,
#         drop_first=not no_drop_first,
#         set_write_mode=not no_write_mode,
#         set_read_mode=not no_read_mode,
#     )
#     if not ok:
#         raise click.ClickException(msg)
#     click.echo(msg)


# # -----------------------
# # conflicts
# # -----------------------
# @main.group()
# def conflicts():
#     """Curation conflicts import/export helpers."""
#     pass


# @conflicts.command("export-excel")
# @db_uri_option
# @click.option("--output", default="curation_conflicts.xlsx", show_default=True)
# def conflicts_export_excel(db_uri, output):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)
#     bf.export_conflicts_to_excel(output_path=output)
#     click.echo(f"✅ Exported to: {output}")


# @conflicts.command("import-excel")
# @db_uri_option
# @click.option("--input", "input_path", default="curation_conflicts_template.xlsx", show_default=True)
# def conflicts_import_excel(db_uri, input_path):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)
#     bf.import_conflicts_from_excel(input_path=input_path)
#     click.echo(f"✅ Imported from: {input_path}")


# # -----------------------
# # report
# # -----------------------
# @main.group()
# def report():
#     """Run and manage reports."""
#     pass


# @report.command("list")
# @db_uri_option
# def report_list(db_uri):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)

#     click.echo("📊 Available Reports:")
#     for r in bf.report.list_reports():
#         click.echo(f" - {r}")


# @report.command("run")
# @db_uri_option
# @click.option("--name", required=True, help="Report name (e.g., qry_etl_status)")
# @click.option("--as-csv", is_flag=True, help="Export to CSV")
# @click.option("--output", type=click.Path(dir_okay=False), help="Output file path")
# def report_run(db_uri, name, as_csv, output):
#     db_uri = resolve_db_uri(db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=False)

#     df = bf.report.run_report(name=name, as_dataframe=True)

#     if as_csv:
#         if not output:
#             raise click.UsageError("Must provide --output with --as-csv")
#         df.to_csv(output, index=False)
#         click.echo(f"✅ Report exported to: {output}")
#     else:
#         click.echo(df.to_string(index=False))

# loki/cli.py
from __future__ import annotations

import argparse
import os
import posixpath
import shutil
import sys
import tarfile
import tempfile
from typing import Iterable, Optional

from loki import loki_db


def build_parser() -> argparse.ArgumentParser:
    version = f"LOKI version {loki_db.Database.getVersionString()}"
    p = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=version,
    )
    p.add_argument(
        "--version",
        action="version",
        version=(
            version
            + "\n%s version %s\n%s version %s"
            % (
                loki_db.Database.getDatabaseDriverName(),
                loki_db.Database.getDatabaseDriverVersion(),
                loki_db.Database.getDatabaseInterfaceName(),
                loki_db.Database.getDatabaseInterfaceVersion(),
            )
        ),
    )
    p.add_argument(
        "-k",
        "--knowledge",
        type=str,
        metavar="file",
        default=None,
        help="the knowledge database file to use",
    )
    p.add_argument(
        "-a",
        "--archive",
        type=str,
        metavar="file",
        default=None,
        help="create (or re-use and update) a compressed archive of downloaded source data files",
    )
    p.add_argument(
        "--from-archive",
        type=str,
        metavar="file",
        default=None,
        help="an input source data archive to re-use but not update",
    )
    p.add_argument(
        "--to-archive",
        type=str,
        metavar="file",
        default=None,
        help="an output source data archive to create (or replace) but not re-use",
    )
    p.add_argument(
        "-d",
        "--temp-directory",
        type=str,
        metavar="dir",
        default=None,
        help="temporary directory for downloaded/archived source files",
    )
    p.add_argument(
        "-l",
        "--list-sources",
        type=str,
        metavar="source",
        nargs="*",
        action="append",
        default=None,
        help="list versions/options for specified loaders; if none or '+' given, list all",
    )
    p.add_argument(
        "-c",
        "--cache-only",
        action="store_true",
        help="do not download new source files; use only archive/cache",
    )
    p.add_argument(
        "-u",
        "--update",
        type=str,
        metavar="source",
        nargs="*",
        action="append",
        default=None,
        help="update the DB from specified sources; if none or '+' given, update from all",
    )
    p.add_argument(
        "-U",
        "--update-except",
        type=str,
        metavar="source",
        nargs="*",
        action="append",
        default=None,
        help="update from all sources EXCEPT the ones specified",
    )
    p.add_argument(
        "-o",
        "--option",
        type=str,
        metavar=("source", "optionstring"),
        nargs=2,
        action="append",
        default=None,
        help="extra options to a source loader, e.g. dbsnp roles=yes,flag=1",
    )
    p.add_argument(
        "-r",
        "--force-update",
        action="store_true",
        help="update even if source data unchanged",
    )
    p.add_argument("-f", "--finalize", action="store_true", help="finalize the DB")
    p.add_argument(
        "--no-optimize", action="store_true", help="skip DB optimization step"
    )
    p.add_argument(
        "-v", "--verbose", action="store_true", help="print warnings/log messages"
    )
    p.add_argument("-q", "--quiet", action="store_true", help="suppress logs")
    p.add_argument("-t", "--test-data", action="store_true", help="load testing data")
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    # mimic legacy behavior: if no args, print usage and exit(2)
    if len(sys.argv if argv is None else list(argv)) < 2:
        print(parser.description or "")
        print()
        parser.print_usage()
        print()
        print("Use -h for details.")
        return 2

    # temp dir (used by sqlite vacuum/archives)
    if args.temp_directory:
        if not os.path.isdir(args.temp_directory):
            print("ERROR: '%s' is not a directory" % args.temp_directory)
            return 1
        os.environ["TMPDIR"] = os.path.abspath(args.temp_directory)

    # DB handle
    db = loki_db.Database(testing=args.test_data, updating=True)
    db.setVerbose(args.verbose or (not args.quiet))
    db.attachDatabaseFile(args.knowledge)

    # list sources?
    if args.list_sources is not None:
        src_set = set()
        for src_list in args.list_sources:
            src_set |= set(src_list)
        if (not src_set) or ("+" in src_set):
            print("available source loaders:")
            src_set = set()
        else:
            print("source loader options:")
        module_versions = db.getSourceModuleVersions(src_set)
        module_options = db.getSourceModuleOptions(src_set)
        for name in sorted(module_options.keys()):
            print(f"  {name} : {module_versions[name]}")
            if module_options[name]:
                for opt in sorted(module_options[name].keys()):
                    print(f"    {opt} = {module_options[name][opt]}")
            elif src_set:
                print("    <no options>")

    # user options map
    user_options = {}
    if args.option is not None:
        for src, optstring in args.option:
            user_options.setdefault(src, {})
            for pair in optstring.split(","):
                k, v = pair.split("=", 1)
                user_options[src][k] = v
    user_options = user_options or None

    # requested sources
    src_set = None
    if args.update is not None:
        src_set = set()
        for src_list in args.update:
            src_set |= set(src_list)
    not_set = None
    if args.update_except is not None:
        not_set = set()
        for src_list in args.update_except:
            not_set |= set(src_list)

    update_ok = True
    if (src_set is not None) or (not_set is not None):
        db.testDatabaseWriteable()
        if db.getDatabaseSetting("finalized", int):
            print("ERROR: cannot update a finalized database")
            return 1
        if src_set and "+" in src_set:
            src_set = set()
        src_set = (src_set or set(db.getSourceModules())) - (not_set or set())

        start_dir = os.getcwd()
        from_archive = args.from_archive or args.archive
        to_archive = args.to_archive or args.archive
        cache_dir = os.path.abspath(
            tempfile.mkdtemp(prefix="loki_update_cache.", dir=args.temp_directory)
        )
        if args.temp_directory:
            print(f"using temporary directory '{cache_dir}'")

        try:
            # unpack input archive?
            if from_archive:
                if os.path.exists(from_archive) and tarfile.is_tarfile(from_archive):
                    print(f"unpacking archived source data files from '{from_archive}' ...")
                    with tarfile.open(name=from_archive, mode="r:*") as archive:
                        archive.errorlevel = 2
                        for member in archive:
                            sname = posixpath.normpath(member.name).split("/", 1)[0]
                            if (not sname) or sname.startswith("."):
                                continue
                            if (not to_archive) and (sname not in src_set):
                                continue
                            archive.extractall(cache_dir, [member])
                    print("... OK")
                else:
                    print(f"source data archive '{from_archive}' not found, starting fresh")

            # run update
            os.chdir(cache_dir)
            update_ok = db.updateDatabase(
                src_set, user_options, args.cache_only, args.force_update
            )
            os.chdir(start_dir)

            # pack output archive?
            if to_archive and not args.cache_only:
                print(f"archiving source data files in '{to_archive}' ...")
                with tarfile.open(name=to_archive, mode="w:gz") as archive:
                    archive.errorlevel = 2
                    for fname in sorted(os.listdir(cache_dir)):
                        archive.add(os.path.join(cache_dir, fname), arcname=fname)
                print("... OK")
        finally:
            def rmtree_error(func, path, exc):  # noqa: ARG001
                print(f"WARNING: unable to remove temporary file '{path}': {exc}\n")
            shutil.rmtree(cache_dir, onerror=rmtree_error)

    # finalize/optimize?
    if args.knowledge:
        if args.finalize and (not db.getDatabaseSetting("finalized", int)):
            if not update_ok:
                print(
                    "WARNING: errors encountered during knowledge database update; skipping finalization step"
                )
            else:
                db.testDatabaseWriteable()
                db.finalizeDatabase()
        if (not args.no_optimize) and (not db.getDatabaseSetting("optimized", int)):
            if not update_ok:
                print(
                    "WARNING: errors encountered during knowledge database update; skipping optimization step"
                )
            else:
                db.testDatabaseWriteable()
                db.optimizeDatabase()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

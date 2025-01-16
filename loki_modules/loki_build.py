#!/usr/bin/env python

"""
This script provides functionality to update a knowledge database for genetic
analysis using the LOKI database system.

It allows users to update the knowledge database by downloading and processing
new data from specified sources. The script supports various options to
control the update process, including caching downloaded data, updating only
specific sources, finalizing the database, and optimizing the database.

Usage:
    <code>python loki-build.py [options]</code>

##Options:
```
-h, --help
    Show this help message and exit.

--version
    Show version information.

-k, --knowledge <file>
    Specify the knowledge database file to use.

-a, --archive <file>
    Create or reuse and update a compressed archive of downloaded source data
    files.

--from-archive <file>
    Specify an input source data archive to reuse but not update.

--to-archive <file>
    Specify an output source data archive to create or replace but not reuse.

-d, --temp-directory <dir>
    Specify a directory to use for temporary storage of downloaded or archived
    source data files.

-l, --list-sources [<source> ...]
    List versions and options for specified source loaders, or list all
    available sources if none specified.

-c, --cache-only
    Do not download any new source data files, only use what's available in
    the provided archive.

-u, --update [<source> ...]
    Update the knowledge database file by downloading and processing new data
    from specified sources, or update from all available sources if none
    specified.

-U, --update-except [<source> ...]
    Update the knowledge database file by downloading and processing new data
    from all available sources except those specified.

-o, --option <source> <optionstring>
    Additional option(s) to pass to the specified source loader module, in the
    format 'option=value[,option2=value2[,...]]'.

-r, --force-update
    Update all sources even if their source data has not changed since the
    last update.

-f, --finalize
    Finalize the knowledge database file.

--no-optimize
    Do not optimize the knowledge database file after updating.
```
"""

import argparse
import os
import posixpath
import shutil
import sys
import tarfile
import tempfile

from loki_modules import loki_db


def main():
    version = "LOKI version %s" % (loki_db.Database.getVersionString())

    # define arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=version,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=version
        + "\n%s version %s\n%s version %s"
        % (
            loki_db.Database.getDatabaseDriverName(),
            loki_db.Database.getDatabaseDriverVersion(),
            loki_db.Database.getDatabaseInterfaceName(),
            loki_db.Database.getDatabaseInterfaceVersion(),
        ),
    )
    parser.add_argument(
        "-k",
        "--knowledge",
        type=str,
        metavar="file",
        action="store",
        default=None,
        help="the knowledge database file to use",
    )
    parser.add_argument(
        "-a",
        "--archive",
        type=str,
        metavar="file",
        action="store",
        default=None,
        help="create (or re-use and update) a compressed archive of downloaded"
        + " source data files",
    )
    parser.add_argument(
        "--from-archive",
        type=str,
        metavar="file",
        action="store",
        default=None,
        help="an input source data archive to re-use but not update",
    )
    parser.add_argument(
        "--to-archive",
        type=str,
        metavar="file",
        action="store",
        default=None,
        help="an output source data archive to create (or replace) but not re-use",  # noqa: E501
    )
    parser.add_argument(
        "-d",
        "--temp-directory",
        type=str,
        metavar="dir",
        action="store",
        default=None,
        help="a directory to use for temporary storage of downloaded or"
        + " archived source data files (default: platform dependent)",
    )
    # 	parser.add_argument('-m', '--memory', type=str, metavar='size', default=None, #TODO  # noqa: E501
    # 			help="the target amount of system memory to use (not exact, allow some margin); default: ~1gb" # noqa: E501
    # 	)
    # BUG: list-sources is overwritten in the code
    parser.add_argument(
        "-l",
        "--list-sources",
        type=str,
        metavar="source",
        nargs="*",
        action="append",
        default=None,
        help="list versions and options for the specified source loaders, or"
        + " if none or '+' are specified, list all available sources",
    )
    parser.add_argument(
        "-c",
        "--cache-only",
        action="store_true",
        help="do not download any new source data files, only use what's"
        + " available in the provided archive",
    )
    parser.add_argument(
        "-u",
        "--update",
        type=str,
        metavar="source",
        nargs="*",
        action="append",
        default=None,
        help="update the knowledge database file by downloading and processing"
        + " new data from the specified sources, "
        + "or if none or '+' are specified, from all available sources",
    )
    parser.add_argument(
        "-U",
        "--update-except",
        type=str,
        metavar="source",
        nargs="*",
        action="append",
        default=None,
        help="update the knowledge database file by downloading and processing"
        + " new data from all available sources EXCEPT those specified",
    )
    parser.add_argument(
        "-o",
        "--option",
        type=str,
        metavar=("source", "optionstring"),
        nargs=2,
        action="append",
        default=None,
        help="additional option(s) to pass to the specified source loader"
        + " module, in the format 'option=value[,option2=value2[,...]]'",
    )  # e.g. --option dbsnp roles=yes
    parser.add_argument(
        "-r",
        "--force-update",
        action="store_true",
        help="update all sources even if their source data has not changed"
        + " since the last update",
    )
    parser.add_argument(
        "-f",
        "--finalize",
        action="store_true",
        help="finalize the knowledge database file",
    )
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="do not optimize the knowledge database file after updating",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="print warnings and log messages (default)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="suppress warnings and log messages",  # noqa: E501
    )
    parser.add_argument(
        "-t", "--test-data", action="store_true", help="Load testing data only"
    )

    # if no arguments, print usage and exit
    if len(sys.argv) < 2:
        print(version)
        print
        parser.print_usage()
        print
        print("Use -h for details.")
        sys.exit(2)

    # parse arguments
    args = parser.parse_args()

    # 	# parse memory allotment, if any
    # 	memLimit = 64*1024*1024 # default 64mb for sqlite (+ ~1gb for updater itself) # noqa: E501
    # 	if args.memory:
    # 		m = args.memory.upper()
    # 		if m.endswith('B'):
    # 			m = m[:-1]
    # 		if m.endswith('T'):
    # 			m = float(m[:-1]) * 1024 * 1024 * 1024 * 1024
    # 		elif m.endswith('G'):
    # 			m = float(m[:-1]) * 1024 * 1024 * 1024
    # 		elif m.endswith('M'):
    # 			m = float(m[:-1]) * 1024 * 1024
    # 		elif m.endswith('K'):
    # 			m = float(m[:-1]) * 1024
    # 		else:
    # 			m = float(m)
    # 		m = long(m)
    # 		if m < 1024*1024*1024:
    # 			print "WARNING: ignoring '%s' memory allotment, the updater requires ~1gb at minimum" % args.memory  # noqa: E501
    # 		else:
    # 			print "using ~%1.1fMB of memory" % (m / (1024 * 1024))
    # 			memLimit = max(memLimit, m - 1024*1024*1024)
    # 	#if args.memory

    # set $TMPDIR so sqlite will use it for vacuum etc.
    if args.temp_directory:
        if not os.path.isdir(args.temp_directory):
            print("ERROR: '%s' is not a directory")
            sys.exit(1)
        os.environ["TMPDIR"] = os.path.abspath(args.temp_directory)

    # instantiate database object
    db = loki_db.Database(testing=args.test_data, updating=True)
    db.setVerbose(args.verbose or (not args.quiet))
    db.attachDatabaseFile(args.knowledge)

    # list sources?
    if args.list_sources is not None:
        srcSet = set()
        for srcList in args.list_sources:
            srcSet |= set(srcList)
        if (not srcSet) or ("+" in srcSet):
            print("available source loaders:")
            srcSet = set()
        else:
            print("source loader options:")
        moduleVersions = db.getSourceModuleVersions(srcSet)
        moduleOptions = db.getSourceModuleOptions(srcSet)
        for srcName in sorted(moduleOptions.keys()):
            print("  %s : %s" % (srcName, moduleVersions[srcName]))
            if moduleOptions[srcName]:
                for srcOption in sorted(moduleOptions[srcName].keys()):
                    print(
                        "    %s = %s"
                        % (srcOption, moduleOptions[srcName][srcOption])  # noqa: E501
                    )
            elif srcSet:
                print("    <no options>")

    # pass options?
    userOptions = {}
    if args.option is not None:
        for optList in args.option:
            srcName = optList[0]
            if srcName not in userOptions:
                userOptions[srcName] = {}
            for optString in optList[1].split(","):
                opt, val = optString.split("=", 1)
                userOptions[srcName][opt] = val
    userOptions = userOptions or None

    # parse requested update sources
    srcSet = None  # BUG: overwritten the srcSet variable
    if args.update is not None:
        srcSet = set()
        for srcList in args.update:
            srcSet |= set(srcList)
    notSet = None
    if args.update_except is not None:
        notSet = set()
        for srcList in args.update_except:
            notSet |= set(srcList)

    # update?
    updateOK = True
    if (srcSet is not None) or (notSet is not None):
        db.testDatabaseWriteable()
        if db.getDatabaseSetting("finalized", int):
            print("ERROR: cannot update a finalized database")
            sys.exit(1)
        if srcSet and "+" in srcSet:
            srcSet = set()
        srcSet = (srcSet or set(db.getSourceModules())) - (notSet or set())

        # create temp directory and unpack input archive, if any
        startDir = os.getcwd()
        fromArchive = args.from_archive or args.archive
        toArchive = args.to_archive or args.archive
        cacheDir = os.path.abspath(
            tempfile.mkdtemp(
                prefix="loki_update_cache.", dir=args.temp_directory
            )  # noqa: E501
        )
        if args.temp_directory:
            print("using temporary directory '%s'" % cacheDir)

        # try/finally to make sure we clean up the cache dir at the end
        try:
            if fromArchive:
                if os.path.exists(fromArchive) and tarfile.is_tarfile(
                    fromArchive
                ):  # noqa: E501
                    print(
                        "unpacking archived source data files from '%s' ..."
                        % fromArchive
                    )
                    with tarfile.open(name=fromArchive, mode="r:*") as archive:
                        archive.errorlevel = 2
                        # the archive should only contain directories named after sources, # noqa: E501
                        # so we can filter members by their normalized top-level directory  # noqa: E501
                        for member in archive:
                            srcName = posixpath.normpath(member.name).split(
                                "/", 1
                            )[  # noqa: E501``
                                0
                            ]  # noqa: E501
                            if (not srcName) or srcName.startswith("."):
                                continue
                            # if we're not writing an output archive, we only have to extract  # noqa: E501
                            # the directories for the sources we need
                            if (not toArchive) and (srcName not in srcSet):
                                continue
                            archive.extractall(cacheDir, [member])
                    # with archive
                    print("... OK")
                else:
                    print(
                        "source data archive '%s' not found, starting fresh"
                        % fromArchive
                    )
            # if fromArchive

            os.chdir(cacheDir)
            updateOK = db.updateDatabase(
                srcSet, userOptions, args.cache_only, args.force_update
            )
            os.chdir(startDir)

            # create output archive, if requested
            if toArchive and not args.cache_only:
                print("archiving source data files in '%s' ..." % toArchive)
                with tarfile.open(name=toArchive, mode="w:gz") as archive:
                    archive.errorlevel = 2
                    for filename in sorted(os.listdir(cacheDir)):
                        archive.add(
                            os.path.join(cacheDir, filename), arcname=filename
                        )  # noqa: E501
                print("... OK")
        finally:
            # clean up cache directory
            def rmtree_error(func, path, exc):
                print(
                    "WARNING: unable to remove temporary file '%s': %s\n"
                    % (path, exc)  # noqa: E501
                )

            shutil.rmtree(cacheDir, onerror=rmtree_error)
    # update

    if args.knowledge:
        # finalize?
        if args.finalize and (not db.getDatabaseSetting("finalized", int)):
            if not updateOK:
                print(
                    "WARNING: errors encountered during knowledge database"
                    + "update; skipping finalization step"
                )
            else:
                db.testDatabaseWriteable()
                db.finalizeDatabase()

        # optimize?
        if (not args.no_optimize) and (
            not db.getDatabaseSetting("optimized", int)
        ):  # noqa: E501
            if not updateOK:
                print(
                    "WARNING: errors encountered during knowledge database"
                    + " update; skipping optimization step"
                )
            else:
                db.testDatabaseWriteable()
                db.optimizeDatabase()
    # if knowledge


if __name__ == "__main__":
    main()

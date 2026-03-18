# arg_utils.py

import argparse
import collections
import csv
import os
import sys


class OrderedNamespace(argparse.Namespace):
    """
    OrderedNamespace is a custom subclass of argparse.Namespace that maintains
    the order of attribute addition.
    This is particularly useful when parsing command-line arguments where the
    order of arguments is significant.
    Methods:
        __setattr__(self, name, value):
            Sets an attribute on the namespace and records the order of
            addition.
        __delattr__(self, name):
            Deletes an attribute from the namespace and removes it from the
            order tracking.
        __iter__(self):
            Returns an iterator over the attributes in the order they were
            added.
    """

    def __setattr__(self, name, value):
        if name != "__OrderedDict":
            if "__OrderedDict" not in self.__dict__:
                self.__dict__["__OrderedDict"] = collections.OrderedDict()
            self.__dict__["__OrderedDict"][name] = None
        super(OrderedNamespace, self).__setattr__(name, value)

    def __delattr__(self, name):
        if name != "__OrderedDict":
            if "__OrderedDict" in self.__dict__:
                del self.__dict__["__OrderedDict"][name]
        super(OrderedNamespace, self).__delattr__(name)

    def __iter__(self):
        return iter(self.__dict__["__OrderedDict"])


class cfDialect(csv.Dialect):
    """CSV dialect for config files supporting quoted substrings."""

    delimiter = " "
    doublequote = False
    escapechar = "\\"
    lineterminator = "\n"
    quotechar = '"'
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = True


def parseCFile(cfName, parser, options, cfStack=None):
    """
    This function reads and interprets a configuration file, processing
    command-line options and allowing the inclusion of other configuration
    files. It is used to configure arguments flexibly, supporting
    configuration files with recursive references to other configuration files.
    """

    # Initialize cfStack on the first call to track included files
    if cfStack is None:
        cfStack = []

    # 1. Control of recursion and prevention of cycles:
    # cfStack keep track of the configuration files being processed to avoid
    # cycles.
    cfAbs = "<stdin>" if cfName == "-" else os.path.abspath(cfName)
    # Check if the current file i(cfAbs) s already in the stack, which would
    # indicate a cycle. If so, the program exits with an error message.
    if cfAbs in cfStack:
        sys.exit(
            "ERROR: configuration files include each other in a loop! %s"
            % " -> ".join(cfStack + [cfAbs])
        )
    cfStack.append(cfAbs)

    # 2. Reading the configuration file:
    # cfHandle: Opens the file specified by cfName, or uses sys.stdin if
    # cfName is "-".
    cfHandle = sys.stdin if cfName == "-" else open(cfName, "r")
    # cfStream: Removes extra tabs and spaces from the file lines.
    cfStream = (line.replace("\t", " ").strip() for line in cfHandle)
    # cfLines: Filters out empty lines and comments (lines starting with #).
    cfLines = (line for line in cfStream if line and not line.startswith("#"))
    # cfReader: Uses csv.reader to interpret the file, allowing it to read
    # arguments split by spaces, respecting quotes to preserve substrings
    # (using cfDialect
    cfReader = csv.reader(cfLines, dialect=cfDialect)

    # 3. Parsing the configuration file:
    cfArgs = []
    for line in cfReader:
        # To each line of the configuration file, -- is added to the beginning
        # of each argument so that it is interpreted as a command-line argument
        line[0] = "--" + line[0].lower().replace("_", "-")
        # If the argument is --include, the function calls itself recursively
        # with the included file (defined by line[ln]), allowing the nesting of
        # configuration files.
        if line[0] == "--include":
            for ln in range(1, len(line)):
                parseCFile(line[ln], parser, options, cfStack)
        # Otherwise, the arguments are added to cfArgs, and --end-of-line is
        # inserted to mark the end of a line
        else:
            cfArgs.extend(line)
            cfArgs.append("--end-of-line")

    # 4. Closing the configuration file and parsing the arguments:
    # After processing the configuration file, it is closed if it is not stdin.
    if cfHandle != sys.stdin:
        cfHandle.close()
    # parser_args is called in the parser with the extracted arguments, and
    # options is filled with the values.
    parser.parse_args(args=cfArgs, namespace=options)
    if options.configuration:
        raise Exception(
            "unexpected argument(s): %s" % " ".join(options.configuration)
        )  # noqa: E501
    # Finally, the current file is removed from cfStack, signaling the end of
    # its processing.
    cfStack.pop()

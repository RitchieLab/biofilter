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


def parseCFile(cfName, parser, options):
    """
    Recursively parses configuration files, supports 'include' directives.
    """
    cfStack = []
    cfAbs = "<stdin>" if cfName == "-" else os.path.abspath(cfName)
    if cfAbs in cfStack:
        sys.exit(
            "ERROR: configuration files include each other in a loop! %s"
            % " -> ".join(cfStack + [cfAbs])
        )
    cfStack.append(cfAbs)

    # Set up iterators
    cfHandle = sys.stdin if cfName == "-" else open(cfName, "r")
    cfStream = (line.replace("\t", " ").strip() for line in cfHandle)
    cfLines = (line for line in cfStream if line and not line.startswith("#"))
    cfReader = csv.reader(cfLines, dialect=cfDialect)

    # Parse the file; recurse for includes, store the rest
    cfArgs = []
    for line in cfReader:
        line[0] = "--" + line[0].lower().replace("_", "-")
        if line[0] == "--include":
            for ln in range(1, len(line)):
                parseCFile(line[ln], parser, options)
        else:
            cfArgs.extend(line)
            cfArgs.append("--end-of-line")

    # Close the stream and parse the args
    if cfHandle != sys.stdin:
        cfHandle.close()
    parser.parse_args(args=cfArgs, namespace=options)
    if options.configuration:
        raise Exception(
            "unexpected argument(s): %s" % " ".join(options.configuration)
        )  # noqa: E501

    cfStack.pop()

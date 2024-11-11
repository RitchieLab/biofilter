import argparse
import csv
import os
# import sys
# import pytest
from biofilter_modules.arg_utils import OrderedNamespace, cfDialect, parseCFile


def test_ordered_namespace():
    ns = OrderedNamespace()
    ns.foo = 1
    ns.bar = 2
    ns.baz = 3

    assert list(ns) == ['foo', 'bar', 'baz']

    del ns.bar
    assert list(ns) == ['foo', 'baz']


def test_cf_dialect():
    dialect = cfDialect()
    assert dialect.delimiter == " "
    assert dialect.doublequote is False
    assert dialect.escapechar == "\\"
    assert dialect.lineterminator == "\n"
    assert dialect.quotechar == '"'
    assert dialect.quoting == csv.QUOTE_MINIMAL
    assert dialect.skipinitialspace is True


def test_parse_cfile():
    base_path = os.path.join(os.path.dirname(__file__), "..", "data")
    base_path = os.path.abspath(base_path)

    config_file_path = os.path.join(base_path, "config.txt")
    another_config_path = os.path.join(base_path, "another_config.txt")

    # Load the content of config.txt and replace the marker w/absolute path
    with open(config_file_path, "r") as f:
        config_content = f.read().replace(
            "{INCLUDE_PATH}",
            another_config_path
            )

    # Create a temporary copy of config.txt for the test
    temp_config_file = os.path.join(base_path, "temp_config.txt")
    with open(temp_config_file, "w") as f:
        f.write(config_content)

    # Set up the parser with simulated arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--option1")
    parser.add_argument("--option2")
    parser.add_argument("--option3")
    parser.add_argument("--include")
    parser.add_argument("--configuration", nargs="*")
    parser.add_argument("--end-of-line", action="store_true")

    # Instancia OrderedNamespace to receive the arguments
    options = OrderedNamespace()

    # Run the parseCFile function
    parseCFile(temp_config_file, parser, options)

    # Check if the options were loaded correctly
    assert options.option1 == "value1"
    assert options.option2 == "value2"
    assert options.option3 == "value3"

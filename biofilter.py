#!/usr/bin/env python

import argparse
import codecs
import collections
import csv
import itertools
import os
import random
import string
import sys
import time

from loki import loki_db
from biofilterClass import Biofilter

"""
This script defines a command-line interface (CLI) for Biofilter, a tool for filtering, annotating, and modeling genetic data. It utilizes Python's argparse module to parse command-line arguments and provides custom type handlers for validating input values.

The script defines several custom type handlers to ensure that input arguments are correctly parsed and validated according to the expected formats and ranges:

- `yesno`: Handles boolean-like arguments, accepting values like "yes", "no", "true", "false", "on", or "off".
- `percent`: Handles percentage values, ensuring they are within the range of 0 to 100.
- `zerotoone`: Ensures that the input value is a float between 0.0 and 1.0.
- `basepairs`: Handles values representing base pairs (e.g., "1000" for 1000 base pairs, "1k" for 1000 base pairs, "1m" for 1 million base pairs, etc.).
- `typePZPV`: Handles values related to Paris-zero p-values, accepting "significant", "insignificant", or "ignore".

The CLI allows users to interact with Biofilter, providing options for specifying filtering criteria, annotation types, model generation parameters, and more.

To run the script, users can provide command-line arguments corresponding to the desired Biofilter functionalities, such as filtering genetic data, annotating variants, generating models, and configuring various parameters.

For usage instructions and available command-line options, users can invoke the script with the `-h` or `--help` flag.

Example usage:
    python script.py --input-file data.txt --output-file results.txt --filter-gene ABC --annotation gwas --model-score 0.8

For detailed information on each command-line argument and its usage, please refer to the argparse module documentation.
"""

if __name__ == "__main__":
	
	# define the arguments parser
	version = "Biofilter version %s" % (Biofilter.getVersionString())
	parser = argparse.ArgumentParser(
		description=version,
		add_help=False,
		formatter_class=argparse.RawDescriptionHelpFormatter
	)
	
	# define custom bool-ish type handler
	def yesno(val):
		val = str(val).strip().lower()
		if val in ('1','t','true','y','yes','on'):
			return 'yes'
		if val in ('0','f','false','n','no','off'):
			return 'no'
		raise argparse.ArgumentTypeError("'%s' must be yes/on/true/1 or no/off/false/0" % val)
	#yesno()
	
	# define custom percentage type handler
	def percent(val):
		val = str(val).strip().lower()
		while val.endswith('%'):
			val = val[:-1]
		val = float(val)
		if val > 100:
			raise argparse.ArgumentTypeError("'%s' must be <= 100" % val)
		return val
	#percent()
	
	# define custom [0.0..1.0] type handler
	def zerotoone(val):
		val = float(val)
		if val < 0.0 or val > 1.0:
			raise argparse.ArgumentTypeError("'%s' must be between 0.0 and 1.0" % (val,))
		return val
	#zerotoone()
	
	# define custom basepairs handler
	def basepairs(val):
		val = str(val).strip().lower()
		if val[-1:] == 'b':
			val = val[:-1]
		if val[-1:] == 'k':
			val = int(val[:-1]) * 1000
		elif val[-1:] == 'm':
			val = int(val[:-1]) * 1000 * 1000
		elif val[-1:] == 'g':
			val = int(val[:-1]) * 1000 * 1000 * 1000
		else:
			val = int(val)
		return val
	#basepairs()
	
	# define custom type handler for --paris-zero-p-values
	def typePZPV(val):
		val = str(val).strip().lower()
		if 'significant'.startswith(val):
			return 'significant'
		if val == 'i':
			raise argparse.ArgumentTypeError("ambiguous value: '%s' could match insignificant, ignore" % (val,))
		if 'insignificant'.startswith(val):
			return 'insignificant'
		if 'ignore'.startswith(val):
			return 'ignore'
		raise argparse.ArgumentTypeError("'%s' must be significant, insignificant or ignore" % (val,))
	#typePZPV()
	
	# add general configuration section
	group = parser.add_argument_group("Configuration Options")
	group.add_argument('--help', '-h', action='help', help="show this help message and exit")
	group.add_argument('--version', action='version', help="show all software version numbers and exit",
			version=version+"""
%9s version %s
%9s version %s
%9s version %s
""" % (
				"LOKI",
				loki_db.Database.getVersionString(),
				loki_db.Database.getDatabaseDriverName(),
				loki_db.Database.getDatabaseDriverVersion(),
				loki_db.Database.getDatabaseInterfaceName(),
				loki_db.Database.getDatabaseInterfaceVersion()
			)
	)
	group.add_argument('configuration', type=str, metavar='configuration_file', nargs='*', default=None,
			help="a file from which to read additional options"
	)
	group.add_argument('--report-configuration', '--rc', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="output a report of all effective options, including any defaults, in a configuration file format which can be re-input (default: no)"
	)
	group.add_argument('--report-replication-fingerprint', '--rrf', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="include software versions and the knowledge database file's fingerprint values in the configuration report, to ensure the same data is used in replication (default: no)"
	)
	group.add_argument('--random-number-generator-seed', '--rngs', type=str, metavar='seed', nargs='?', const='', default=None,
			help="seed value for the PRNG, or blank to use the sytem default (default: blank)"
	)
	
	# add knowledge database section
	group = parser.add_argument_group("Prior Knowledge Options")
	group.add_argument('--knowledge', '-k', type=str, metavar='file', #default=argparse.SUPPRESS,
			help="the prior knowledge database file to use"
	)
	group.add_argument('--report-genome-build', '--rgb', type=yesno, metavar='yes/no', nargs='?', const='yes', default='yes',
			help="report the genome build version number used by the knowledge database (default: yes)"
	)
	group.add_argument('--report-gene-name-stats', '--rgns', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="display statistics on available gene identifier types (default: no)"
	)
	group.add_argument('--report-group-name-stats', '--runs', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="display statistics on available group identifier types (default: no)"
	)
	group.add_argument('--allow-unvalidated-snp-positions', '--ausp', type=yesno, metavar='yes/no', nargs='?', const='yes', default='yes',
			help="use unvalidated SNP positions in the knowledge database (default: yes)"
	)
	group.add_argument('--allow-ambiguous-snps', '--aas', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="use SNPs which have ambiguous loci in the knowledge database (default: no)"
	)
	group.add_argument('--allow-ambiguous-knowledge', '--aak', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="use ambiguous group<->gene associations in the knowledge database (default: no)"
	)
	group.add_argument('--reduce-ambiguous-knowledge', '--rak', type=str, metavar='no/implication/quality/any', nargs='?', const='any', default='no',
			choices=['no','implication','quality','any'],
			help="attempt to reduce ambiguity in the knowledge database using a heuristic strategy, from 'no', 'implication', 'quality' or 'any' (default: no)"
	)
	group.add_argument('--report-ld-profiles', '--rlp', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="display the available LD profiles and their properties (default: no)"
	)
	group.add_argument('--ld-profile', '--lp', type=str, metavar='profile', nargs='?', const=None, default=None,
			help="LD profile with which to adjust regions in the knowledge database (default: none)"
	)
	group.add_argument('--verify-biofilter-version', type=str, metavar='version', default=None,
			help="require a specific Biofilter software version to replicate results"
	)
	group.add_argument('--verify-loki-version', type=str, metavar='version', default=None,
			help="require a specific LOKI software version to replicate results"
	)
	group.add_argument('--verify-source-loader', type=str, metavar=('source','version'), nargs=2, action='append', default=None,
			help="require that the knowledge database was built with a specific source loader version"
	)
	group.add_argument('--verify-source-option', type=str, metavar=('source','option','value'), nargs=3, action='append', default=None,
			help="require that the knowledge database was built with a specific source loader option"
	)
	group.add_argument('--verify-source-file', type=str, metavar=('source','file','date','size','md5'), nargs=5, action='append', default=None,
			help="require that the knowledge database was built with a specific source file fingerprint"
	)
	group.add_argument('--user-defined-knowledge', '--udk', type=str, metavar='file', nargs='+', default=None,
			help="file(s) from which to load user-defined knowledge"
	)
	group.add_argument('--user-defined-filter', '--udf', type=str, metavar='no/group/gene', default='no',
			choices=['no','group','gene'],
			help="method by which user-defined knowledge will also be applied as a filter on other prior knowledge, from 'no', 'group' or 'gene' (default: no)"
	)
	
	# add primary input section
	group = parser.add_argument_group("Input Data Options")
	group.add_argument('--snp', '-s', type=str, metavar='rs#', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="input SNPs, specified by RS#"
	)
	group.add_argument('--snp-file', '-S', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input SNPs"
	)
	group.add_argument('--position', '-p', type=str, metavar='position', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="input positions, specified by chromosome and basepair coordinate"
	)
	group.add_argument('--position-file', '-P', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input positions"
	)
	group.add_argument('--gene', '-g', type=str, metavar='name', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="input genes, specified by name"
	)
	group.add_argument('--gene-file', '-G', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input genes"
	)
	group.add_argument('--gene-identifier-type', '--git', type=str, metavar='type', nargs='?', const='*', default='-',
			help="the default type of any gene identifiers without types, or a special type '=', '-' or '*' (default: '-' for primary labels)"
	)
	group.add_argument('--allow-ambiguous-genes', '--aag', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="use ambiguous input gene identifiers by including all possibilities (default: no)"
	)
	group.add_argument('--gene-search', '--gs', type=str, metavar='text', nargs='+', action='append',
			help="find input genes by searching all available names and descriptions"
	)
	group.add_argument('--region', '-r', type=str, metavar='region', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="input regions, specified by chromosome, start and stop positions"
	)
	group.add_argument('--region-file', '-R', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input regions"
	)
	group.add_argument('--group', '-u', type=str, metavar='name', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="input groups, specified by name"
	)
	group.add_argument('--group-file', '-U', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input groups"
	)
	group.add_argument('--group-identifier-type', '--uit', type=str, metavar='type', nargs='?', const='*', default='-',
			help="the default type of any group identifiers without types, or a special type '=', '-' or '*' (default: '-' for primary labels)"
	)
	group.add_argument('--allow-ambiguous-groups', '--aau', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="use ambiguous input group identifiers by including all possibilities (default: no)"
	)
	group.add_argument('--group-search', '--us', type=str, metavar='text', nargs='+', action='append',
			help="find input groups by searching all available names and descriptions"
	)
	group.add_argument('--source', '-c', type=str, metavar='name', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="input sources, specified by name"
	)
	group.add_argument('--source-file', '-C', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input sources"
	)
	
	# add alternate input section
	group = parser.add_argument_group("Alternate Input Data Options")
	group.add_argument('--alt-snp', '--as', type=str, metavar='rs#', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="alternate input SNPs, specified by RS#"
	)
	group.add_argument('--alt-snp-file', '--AS', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input SNPs"
	)
	group.add_argument('--alt-position', '--ap', type=str, metavar='position', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="alternate input positions, specified by chromosome and basepair coordinate"
	)
	group.add_argument('--alt-position-file', '--AP', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input positions"
	)
	group.add_argument('--alt-gene', '--ag', type=str, metavar='name', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="alternate input genes, specified by name"
	)
	group.add_argument('--alt-gene-file', '--AG', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input genes"
	)
	group.add_argument('--alt-gene-search', '--ags', type=str, metavar='text', nargs='+', action='append',
			help="find alternate input genes by searching all available names and descriptions"
	)
	group.add_argument('--alt-region', '--ar', type=str, metavar='region', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="alternate input regions, specified by chromosome, start and stop positions"
	)
	group.add_argument('--alt-region-file', '--AR', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input regions"
	)
	group.add_argument('--alt-group', '--au', type=str, metavar='name', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="alternate input groups, specified by name"
	)
	group.add_argument('--alt-group-file', '--AU', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input groups"
	)
	group.add_argument('--alt-group-search', '--aus', type=str, metavar='text', nargs='+', action='append',
			help="find alternate input groups by searching all available names and descriptions"
	)
	group.add_argument('--alt-source', '--ac', type=str, metavar='name', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="alternate input sources, specified by name"
	)
	group.add_argument('--alt-source-file', '--AC', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input sources"
	)
	
	# add positional section
	group = parser.add_argument_group("Positional Matching Options")
	group.add_argument('--grch-build-version', '--gbv', type=int, metavar='version', default=None,
			help="the GRCh# human reference genome build version of position and region inputs",
	)
	group.add_argument('--ucsc-build-version', '--ubv', type=int, metavar='version', default=None,
			help="the UCSC hg# human reference genome build version of position and region inputs",
	)
	group.add_argument('--coordinate-base', '--cb', type=int, metavar='offset', default=1,
			help="the coordinate base for position and region inputs and outputs (default: 1)",
	)
	group.add_argument('--regions-half-open', '--rho', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="whether input and output regions are 'half-open' intervals and should not include their end coordinate (default: no)",
	)
	group.add_argument('--region-position-margin', '--rpm', type=basepairs, metavar='bases', default=0,
			help="number of bases beyond the bounds of known regions where positions should still be matched (default: 0)"
	)
	group.add_argument('--region-match-percent', '--rmp', type=percent, metavar='percentage', default=None, # default set later, with -bases
			help="minimum percentage of overlap between two regions to consider them a match (default: 100)"
	)
	group.add_argument('--region-match-bases', '--rmb', type=basepairs, metavar='bases', default=None, # default set later, with -percent
			help="minimum number of bases of overlap between two regions to consider them a match (default: 0)"
	)
	
	# add modeling section
	group = parser.add_argument_group("Model-Building Options")
	group.add_argument('--maximum-model-count', '--mmc', type=int, metavar='count', nargs='?', const=0, default=0,
			help="maximum number of models to generate, or < 1 for unlimited (default: unlimited)"
	)
	group.add_argument('--alternate-model-filtering', '--amf', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="apply primary input filters to only one side of generated models (default: no)"
	)
	group.add_argument('--all-pairwise-models', '--apm', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="generate all comprehensive pairwise models without regard to any prior knowledge (default: no)"
	)
	group.add_argument('--maximum-model-group-size', '--mmgs', type=int, metavar='size', default=30,
			help="maximum size of a group to use for knowledge-supported models, or < 1 for unlimited (default: 30)"
	)
	group.add_argument('--minimum-model-score', '--mms', type=int, metavar='score', default=2,
			help="minimum implication score for knowledge-supported models (default: 2)"
	)
	group.add_argument('--sort-models', '--sm', type=yesno, metavar='yes/no', nargs='?', const='yes', default='yes',
			help="output knowledge-supported models in order of descending score (default: yes)"
	)
	
	# add PARIS section
	group = parser.add_argument_group("PARIS Options")
	group.add_argument('--paris-p-value', '--ppv', type=zerotoone, metavar='p-value', default=0.05,
			help="maximum p-value of input results to be considered significant (default: 0.05)"
	)
	group.add_argument('--paris-zero-p-values', '--pzpv', type=typePZPV, metavar='sig/insig/ignore', default='ignore',
			help="how to consider input result p-values of zero (default: ignore)"
	)
	group.add_argument('--paris-max-p-value', '--pmpv', type=zerotoone, metavar='p-value', default=None,
			help="maximum meaningful permutation p-value (default: none)"
	)
	group.add_argument('--paris-enforce-input-chromosome', '--peic', type=yesno, metavar='yes/no', nargs='?', const='yes', default='yes',
			help="limit input result SNPs to positions on the specified chromosome (default: yes)"
	)
	group.add_argument('--paris-permutation-count', '--ppc', type=int, metavar='number', default=1000,
			help="number of permutations to perform on each group and gene (default: 1000)"
	)
	group.add_argument('--paris-bin-size', '--pbs', type=int, metavar='number', default=10000,
			help="ideal number of features per bin (default: 10000)"
	)
	group.add_argument('--paris-snp-file', '--PS', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load SNP results"
	)
	group.add_argument('--paris-position-file', '--PP', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load position results"
	)
	group.add_argument('--paris-details', '--pd', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="generate the PARIS detail report (default: no)"
	)
	
	# add output section
	group = parser.add_argument_group("Output Options")
	group.add_argument('--quiet', '-q', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="don't print any warnings or log messages to <stdout> (default: no)"
	)
	group.add_argument('--verbose', '-v', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="print additional informational log messages to <stdout> (default: no)"
	)
	group.add_argument('--prefix', type=str, metavar='prefix', default='biofilter',
			help="prefix to use for all output filenames; may contain path components (default: 'biofilter')"
	)
	group.add_argument('--overwrite', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="overwrite any existing output files (default: no)",
	)
	group.add_argument('--stdout', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="display all output data directly on <stdout> rather than writing to any files (default: no)"
	)
	group.add_argument('--report-invalid-input', '--rii', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="report invalid input data lines in a separate output file for each type (default: no)"
	)
	group.add_argument('--filter', '-f', type=str, metavar='type', nargs='+', action='append',
			help="data types or columns to include in the filtered output"
	)
	group.add_argument('--annotate', '-a', type=str, metavar='type', nargs='+', action='append',
			help="data types or columns to include in the annotated output"
	)
	group.add_argument('--model', '-m', type=str, metavar='type', nargs='+', action='append',
			help="data types or columns to include in the output models"
	)
	group.add_argument('--paris', type=str, metavar='yes/no', nargs='?', const='yes', default='no',
			help="perform a PARIS analysis with the provided input data (default: no)"
	)
	
	# add hidden options
	parser.add_argument('--end-of-line', action='store_true', help=argparse.SUPPRESS)
	parser.add_argument('--allow-duplicate-output', '--ado', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no', help=argparse.SUPPRESS)
	parser.add_argument('--debug-logic', action='store_true', help=argparse.SUPPRESS)
	parser.add_argument('--debug-query', action='store_true', help=argparse.SUPPRESS)
	parser.add_argument('--debug-profile', action='store_true', help=argparse.SUPPRESS)
	
	# if there are no arguments, just print usage and exit
	if len(sys.argv) < 2:
		print (version)
		print
		parser.print_usage()
		print
		print ("Use -h for details.")
		sys.exit(2)
	#if no args
	
	"""
	This part of the script handles the generation of various reports based on the provided options and configurations:

	1. **OrderedNamespace**: Defines a custom namespace class that preserves the order of attribute additions.

	2. **cfDialect**: Defines a custom CSV dialect named `cfDialect` for configuration files, ensuring compatibility with quoted substrings.

	3. **parseCFile**: A recursive function to parse configuration files, supporting 'include' directives and cyclic include detection. It populates the `OrderedNamespace` with parsed arguments.

	4. **Parsing Command Line for Configuration Files**: Parses command-line arguments to identify configuration files and re-parses them to override the previous configurations.

	5. **Identifying Output Paths**: Determines the paths for various types of reports, filtering results, annotations, and models based on user-specified options.

	6. **Verification and Error Handling**: Verifies the uniqueness, writability, and non-existence of output files. It also handles errors related to conflicting file paths and overwriting.

	7. **Attaching Knowledge Database**: Attaches a knowledge database file if provided in the options.

	8. **Verifying Replication Fingerprint**: Verifies the replication fingerprint, including Biofilter and LOKI versions, source loader versions, options, and file hashes.

	9. **Processing Reports**: Writes various reports based on user options, such as configuration file details, gene name statistics, group name statistics, and LD profiles.

	10. **Output Helper Functions**: Defines utility functions to encode strings, lines, and rows into UTF-8 format for writing to files.

	11. **Generating Reports**: Iterates through different types of reports, writes them to respective files, and logs the process.

	This part of the script is responsible for generating and writing various reports based on user-defined configurations and input data.
	"""		
	# define an argparse.Namespace that remembers the order in which attributes are added
	class OrderedNamespace(argparse.Namespace):
		def __setattr__(self, name, value):
			if name != '__OrderedDict':
				if '__OrderedDict' not in self.__dict__:
					self.__dict__['__OrderedDict'] = collections.OrderedDict()
				self.__dict__['__OrderedDict'][name] = None
			super(OrderedNamespace,self).__setattr__(name, value)
		
		def __delattr__(self, name):
			if name != '__OrderedDict':
				if '__OrderedDict' in self.__dict__:
					del self.__dict__['__OrderedDict'][name]
			super(OrderedNamespace,self).__delattr__(name)
		
		def __iter__(self):
			return (self.__dict__['__OrderedDict'] or []).__iter__()
	#OrderedNamespace
	
	# define a CSV dialect for conf files (to support "quoted substrings")
	class cfDialect(csv.Dialect):
		delimiter = ' '
		doublequote = False
		escapechar = '\\'
		lineterminator = '\n'
		quotechar = '"'
		quoting = csv.QUOTE_MINIMAL
		skipinitialspace = True
	#cfDialect
	
	# define a recursive function to parse conf files (to support 'include')
	options = parser.parse_args(args=[], namespace=OrderedNamespace())
	cfStack = list()
	def parseCFile(cfName):
		# check for cycles
		cfAbs = ('<stdin>' if cfName == '-' else os.path.abspath(cfName))
		if cfAbs in cfStack:
			sys.exit("ERROR: configuration files include eachother in a loop! %s" % (' -> '.join(cfStack + [cfAbs])))
		cfStack.append(cfAbs)
		
		# set up iterators
		cfHandle = (sys.stdin if cfName == '-' else open(cfName,'r'))
		cfStream = (line.replace('\t',' ').strip() for line in cfHandle)
		cfLines = (line for line in cfStream if line and not line.startswith('#'))
		cfReader = csv.reader(cfLines, dialect=cfDialect)
		
		# parse the file; recurse for includes, store the rest
		cfArgs = list()
		for line in cfReader:
			line[0] = '--' + line[0].lower().replace('_','-')
			if line[0] == '--include':
				for l in range(1,len(line)):
					parseCFile(line[l])
			else:
				cfArgs.extend(line)
				cfArgs.append('--end-of-line')
		#foreach line
		
		# close the stream and try to parse the args
		if cfHandle != sys.stdin:
			cfHandle.close()
		try:
			parser.parse_args(args=cfArgs, namespace=options)
			# if extra arguments are given to an otherwise correct option,
			# they'll end up in 'configuration' because it accepts nargs=*
			if options.configuration:
				raise Exception("unexpected argument(s): %s" % (' '.join(options.configuration)))
		except:
			print ("(in configuration file '%s')" % cfName)
			raise
		
		# pop the stack and return
		assert(cfStack[-1] == cfAbs)
		cfStack.pop()
	#parseCFile()
	
	# parse the command line for any configuration files, then re-parse to override them
	for cfName in (parser.parse_args()).configuration:
		parseCFile(cfName)
	parser.parse_args(namespace=options)
	bio = Biofilter(options)
	empty = list()
	
	# identify all the reports we need to output
	typeOutputPath = collections.OrderedDict()
	typeOutputPath['report'] = collections.OrderedDict()
	if options.report_configuration == 'yes':
		typeOutputPath['report']['configuration'] = options.prefix + '.configuration'
	if options.report_gene_name_stats == 'yes':
		typeOutputPath['report']['gene name statistics'] = options.prefix + '.gene-names'
	if options.report_group_name_stats == 'yes':
		typeOutputPath['report']['group name statistics'] = options.prefix + '.group-names'
	if options.report_ld_profiles == 'yes':
		typeOutputPath['report']['LD profiles'] = options.prefix + '.ld-profiles'
	
	# define invalid input handlers, if requested
	typeOutputPath['invalid'] = collections.OrderedDict()
	cb = collections.defaultdict(bool)
	cbLog = collections.OrderedDict()
	cbMake = lambda modtype: lambda line,err: cbLog[modtype].extend(["# %s" % (err or "(unknown error"), str(line).rstrip()])
	if options.report_invalid_input == 'yes':
		for itype in ['SNP','position','region','gene','group','source']:
			for mod in ['','alt-']:
				typeOutputPath['invalid'][mod+itype] = options.prefix + '.invalid.' + mod+itype.lower()
				cbLog[mod+itype] = list()
		for itype in ['userknowledge']:
			typeOutputPath['invalid'][itype] = options.prefix + '.invalid.' + itype.lower()
			cbLog[itype] = list()
	#if report invalid input
	
	# identify all the filtering results we need to output
	typeOutputPath['filter'] = collections.OrderedDict()
	for types in (options.filter or empty):
		if types:
			typeOutputPath['filter'][tuple(types)] = options.prefix + '.' + '-'.join(types)
		else:
			# ignore empty filters
			pass
	#foreach requested filter
	
	# identify all the annotation results we need to output
	typeOutputPath['annotation'] = collections.OrderedDict()
	if options.snp or options.snp_file:
		userInputType = ['snpinput']
	elif options.position_file or options.position:
		userInputType = ['positioninput']
	elif options.gene or options.gene_file or options.gene_search:
		userInputType = ['geneinput']
	elif options.region or options.region_file:
		userInputType = ['regioninput']
	elif options.group or options.group_file or options.group_search:
		userInputType = ['groupinput']
	elif options.source or options.source_file:
		userInputType = ['sourceinput']
	else:
		userInputType = []

	for types in (options.annotate or empty):
		n = types.count(':')
		if n > 1:
			sys.exit("ERROR: cannot annotate '%s', only two sets of outputs are allowed\n" % (' '.join(types),))
		elif n:
			i = types.index(':')
			typesF = userInputType + types[:i]
			typesA = types[i+1:None]
		else:
			typesF = userInputType + types[0:1]
			typesA = types[1:None]

		if typesF and typesA:
			typeOutputPath['annotation'][(tuple(typesF),tuple(typesA))] = options.prefix + '.' + '-'.join(typesF[1:]) + '.' + '-'.join(typesA)
		elif typesF:
			bio.warn("WARNING: annotating '%s' is equivalent to filtering '%s'\n" % (' '.join(types),' '.join(typesF)))
			typeOutputPath['filter'][tuple(typesF)] = options.prefix + '.' + '-'.join(typesF)
		elif typesA:
			sys.exit("ERROR: cannot annotate '%s' with no starting point\n" % (' '.join(types),))
		else:
			# ignore empty annotations
			pass
	#foreach requested annotation

	# identify all the model results we need to output
	typeOutputPath['models'] = collections.OrderedDict()
	for types in (options.model or empty):
		n = types.count(':')
		if n > 1:
			sys.exit("ERROR: cannot model '%s', only two sets of outputs are allowed\n" % (' '.join(types),))
		elif n:
			i = types.index(':')
			typesL = types[:i]
			typesR = types[i+1:None]
		else:
			typesL = typesR = types
		
		if not (typesL or typesR):
			# ignore empty models
			pass
		elif not (typesL and typesR):
			sys.exit("ERROR: cannot model '%s', both sides require at least one output type\n" % ' '.join(types))
		elif typesL == typesR:
			typeOutputPath['models'][(tuple(typesL),tuple(typesR))] = options.prefix + '.' + '-'.join(typesL) + '.models'
		else:
			typeOutputPath['models'][(tuple(typesL),tuple(typesR))] = options.prefix + '.' + '-'.join(typesL) + '.' + '-'.join(typesR) + '.models'
	#foreach requested model
	
	# identify all the PARIS result files we need to output
	typeOutputPath['paris'] = collections.OrderedDict()
	if options.paris == 'yes':
		typeOutputPath['paris']['summary'] = options.prefix + '.paris-summary'
		if options.paris_details == 'yes':
			typeOutputPath['paris']['detail'] = options.prefix + '.paris-detail'
	
	# verify that all output files are unique, writeable and nonexistant (unless overwriting)
	typeOutputInfo = dict()
	pathUsed = dict()
	for outtype,outputPath in typeOutputPath.items():
		typeOutputInfo[outtype] = collections.OrderedDict()
		for output,path in outputPath.items():
			if outtype == 'report':
				label = "%s report" % (output,)
			elif outtype == 'invalid':
				label = "invalid %s input report" % (output,)
			elif outtype == 'filter':
				label = "'%s' filter" % (" ".join(output),)
			elif outtype == 'annotation':
				label = "'%s : %s' annotation" % (" ".join(output[0][1:])," ".join(output[1]))
			elif outtype == 'models':
				if output[0] == output[1]:
					label = "'%s' models" % (" ".join(output[0]),)
				else:
					label = "'%s : %s' models" % (" ".join(output[0])," ".join(output[1]))
			elif outtype == 'paris':
				label = "PARIS %s report" % (output,)
			else:
				raise Exception("unexpected output type")
			
			if options.debug_logic == 'yes':
				bio.warn("%s will be written to '%s'\n" % (label,('<stdout>' if options.stdout == 'yes' else path)))
			
			if options.stdout == 'yes':
				path = '<stdout>'
			elif path in pathUsed:
				sys.exit("ERROR: cannot write %s to '%s', file is already reserved for %s\n" % (label,path,pathUsed[path]))
			elif os.path.exists(path):
				if options.overwrite == 'yes':
					bio.warn("WARNING: %s file '%s' already exists and will be overwritten\n" % (label,path))
				else:
					sys.exit("ERROR: %s file '%s' already exists, must specify --overwrite or a different --prefix\n" % (label,path))
			pathUsed[path] = label
			file = sys.stdout if options.stdout == 'yes' else (open(path,'wb') if outtype != 'invalid' else None)
			typeOutputInfo[outtype][output] = (label,path,file)
			if outtype == 'invalid':
				cb[output] = cbMake(output)
		#foreach output of type
	#foreach output type
	
	# attach the knowledge file, if provided
	if options.knowledge:
		dbPath = options.knowledge
		if not os.path.exists(dbPath):
			cwdDir = os.path.dirname(os.path.realpath(os.path.abspath(os.getcwd())))
			myDir = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
			if not os.path.samefile(cwdDir, myDir):
				dbPath = os.path.join(myDir, options.knowledge)
				if not os.path.exists(dbPath):
					sys.exit("ERROR: knowledge database file '%s' not found in '%s' or '%s'" % (options.knowledge, cwdDir, myDir))
			else:
				sys.exit("ERROR: knowledge database file '%s' not found" % (options.knowledge))
		bio.attachDatabaseFile(dbPath)
	#if knowledge
	
	# verify the replication fingerprint, if requested
	sourceVerify = collections.defaultdict(lambda: [None,None,None])
	for source,version in (options.verify_source_loader or empty):
		sourceVerify[source][0] = version
	for source,option,value in (options.verify_source_option or empty):
		if not sourceVerify[source][1]:
			sourceVerify[source][1] = dict()
		sourceVerify[source][1][option] = value
	for source,file,date,size,md5 in (options.verify_source_file or empty):
		if not sourceVerify[source][2]:
			sourceVerify[source][2] = dict()
		sourceVerify[source][2][file] = (date,int(size),md5)
	if sourceVerify or options.verify_biofilter_version or options.verify_loki_version:
		bio.logPush("verifying replication fingerprint ...\n")
		if options.verify_biofilter_version and (options.verify_biofilter_version != Biofilter.getVersionString()):
			sys.exit("ERROR: configuration requires Biofilter version %s, but this is version %s\n" % (options.verify_biofilter_version, Biofilter.getVersionString()))
		if options.verify_loki_version and (options.verify_loki_version != loki_db.Database.getVersionString()):
			sys.exit("ERROR: configuration requires LOKI version %s, but this is version %s\n" % (options.verify_loki_version, loki_db.Database.getVersionString()))
		for source in sorted(sourceVerify):
			verify = sourceVerify[source]
			sourceID = bio._loki.getSourceID(source)
			if not sourceID:
				sys.exit("ERROR: cannot verify %s fingerprint, knowledge database contains no such source\n" % (source,))
			version = bio._loki.getSourceIDVersion(sourceID)
			if verify[0] and verify[0] != version:
				sys.exit("ERROR: configuration requires %s loader version %s, but knowledge database reports version %s\n" % (source,verify[0],version))
			if verify[1]:
				options = bio._loki.getSourceIDOptions(sourceID)
				for opt,val in verify[1].items():
					if opt not in options or val != options[opt]:
						sys.exit("ERROR: configuration requires %s loader option %s = %s, but knowledge database reports setting = %s\n" % (source,opt,val,options.get(opt)))
			if verify[2]:
				files = bio._loki.getSourceIDFiles(sourceID)
				for file,meta in verify[2].items():
					if file not in files:
						sys.exit("ERROR: configuration requires a specific fingerprint for %s file '%s', but knowledge database reports no such file\n" % (source,file))
					# size and hash should be sufficient comparisons, and some sources (KEGG,PharmGKB) don't provide data file timestamps anyway
					#elif meta[0] != files[file][0]:
					#	sys.exit("ERROR: configuration requires %s file '%s' modification date '%s', but knowledge database reports '%s'\n" % (source,file,meta[0],files[file][0]))
					elif meta[1] != files[file][1]:
						sys.exit("ERROR: configuration requires %s file '%s' size %s, but knowledge database reports %s\n" % (source,file,meta[1],files[file][1]))
					elif meta[2] != files[file][2]:
						sys.exit("ERROR: configuration requires %s file '%s' hash '%s', but knowledge database reports '%s'\n" % (source,file,meta[2],files[file][2]))
		#foreach source
		bio.logPop("... OK\n")
	#if verify replication fingerprint
	
	# set default region_match_percent/bases
	if (options.region_match_bases != None) and (options.region_match_percent == None):
		bio.warn("WARNING: ignoring default region match percent (100) in favor of user-specified region match bases (%d)\n" % options.region_match_bases)
		options.region_match_percent = None
	else:
		if options.region_match_bases == None:
			options.region_match_bases = 0
		if options.region_match_percent == None:
			options.region_match_percent = 100.0
	#if rmb/rmp
	
	# set the PRNG seed, if requested
	if options.random_number_generator_seed != None:
		try:
			seed = int(options.random_number_generator_seed)
		except ValueError:
			seed = options.random_number_generator_seed or None
		bio.warn("random number generator seed: %s\n" % (repr(seed) if (seed != None) else '<system default>',))
		random.seed(seed)
	#if rngs
	
	# report the genome build, if requested
	grchBuildDB,ucscBuildDB = bio.getDatabaseGenomeBuilds()
	if options.report_genome_build == 'yes':
		bio.warn("knowledge database genome build: GRCh%s / UCSC hg%s\n" % (grchBuildDB or '?', ucscBuildDB or '?'))
	#if genome build
	
	# parse input genome build version(s)
	grchBuildUser,ucscBuildUser = bio.getInputGenomeBuilds(options.grch_build_version, options.ucsc_build_version)
	if grchBuildUser or ucscBuildUser:
		bio.warn("user input genome build: GRCh%s / UCSC hg%s\n" % (grchBuildUser or '?', ucscBuildUser or '?'))
	
	# define output helper functions
	utf8 = codecs.getencoder('utf8')
	def encodeString(string):
		return utf8(string)[0]
	def encodeLine(line, term="\n"):
		return utf8("%s%s" % (line,term))[0]
	def encodeRow(row, term="\n", delim="\t"):
		return utf8("%s%s" % ((delim.join((col if isinstance(col,str) else str('' if col == None else col)) for col in row)),term))[0]
	
	# process reports
	for report,info in typeOutputInfo['report'].items():
		label,path,outfile = info
		bio.logPush("writing %s to '%s' ...\n" % (label,path))
		if report == 'configuration':
			outfile.write(encodeLine("# Biofilter configuration file"))
			outfile.write(encodeLine("#   generated %s" % time.strftime('%a, %d %b %Y %H:%M:%S')))
			outfile.write(encodeLine("#   Biofilter version %s" % Biofilter.getVersionString()))
			outfile.write(encodeLine("#   LOKI version %s" % loki_db.Database.getVersionString()))
			outfile.write(encodeLine(""))
			if options.report_replication_fingerprint == 'yes':
				outfile.write(encodeLine("%-35s \"%s\"" % ('VERIFY_BIOFILTER_VERSION', Biofilter.getVersionString(),)))
				outfile.write(encodeLine("%-35s \"%s\"" % ('VERIFY_LOKI_VERSION', loki_db.Database.getVersionString(),)))
				for source,fingerprint in bio.getSourceFingerprints().items():
					outfile.write(encodeLine("%-35s %s \"%s\"" % ('VERIFY_SOURCE_LOADER',source,fingerprint[0])))
					for srcopt in sorted(fingerprint[1]):
						outfile.write(encodeLine("%-35s %s %s " % ('VERIFY_SOURCE_OPTION',source,srcopt), term=""))
						outfile.write(encodeRow(fingerprint[1][srcopt], delim=" "))
					for srcfile in sorted(fingerprint[2]):
						outfile.write(encodeLine("%-35s %s \"%s\" " % ('VERIFY_SOURCE_FILE',source,srcfile), term=""))
						outfile.write(encodeRow((('"%s"' % col) for col in fingerprint[2][srcfile]), delim=" "))
					outfile.write(encodeLine(""))
			for opt in options:
				if opt in ('configuration','verify_source_loader','verify_source_option','verify_source_file') or not hasattr(options, opt):
					continue
				val = getattr(options, opt)
				if type(val) == bool: # --end-of-line, --debug-*
					continue
				opt = "%-35s" % opt.upper().replace('-','_')
				# three possibilities: simple value, list of simple values, or list of lists of simple values
				if isinstance(val,list) and len(val) and isinstance(val[0],list):
					for subvals in val:
						if len(subvals):
							outfile.write(encodeRow(itertools.chain([opt],subvals), delim=" "))
						else:
							outfile.write(encodeLine(opt))
				elif isinstance(val,list):
					if len(val):
						outfile.write(encodeRow(itertools.chain([opt],val), delim=" "))
					else:
						outfile.write(encodeLine(opt))
				elif val != None:
					outfile.write(encodeRow([opt,val], delim=" "))
			#foreach option
		elif report == 'gene name statistics':
			outfile.write(encodeRow(['#type','names','unique','ambiguous']))
			for row in bio.generateGeneNameStats():
				outfile.write(encodeRow(row))
		elif report == 'group name statistics':
			outfile.write(encodeRow(['#type','names','unique','ambiguous']))
			for row in bio.generateGroupNameStats():
				outfile.write(encodeRow(row))
		elif report == 'LD profiles':
			outfile.write(encodeRow(['#ldprofile','description','metric','value']))
			for row in bio.generateLDProfiles():
				outfile.write(encodeRow(row))
		else:
			raise Exception("unexpected report type")
		#which report
		if outfile != sys.stdout:
			outfile.close()
		bio.logPop("... OK\n")
	#foreach report
	
	# load user-defined knowledge, if any
	for path in (options.user_defined_knowledge or empty):
		bio.loadUserKnowledgeFile(path, options.gene_identifier_type, errorCallback=cb['userknowledge'])
	if options.user_defined_filter != 'no':
		bio.applyUserKnowledgeFilter((options.user_defined_filter == 'group'))
	
	# apply primary filters
	for snpList in (options.snp or empty):
		bio.intersectInputSNPs(
			'main',
			bio.generateRSesFromText(snpList, separator=':', errorCallback=cb['SNP']),
			errorCallback=cb['SNP']
		)
	for snpFileList in (options.snp_file or empty):
		bio.intersectInputSNPs(
			'main',
			bio.generateRSesFromRSFiles(snpFileList, errorCallback=cb['SNP']),
			errorCallback=cb['SNP']
		)
	for positionList in (options.position or empty):
		bio.intersectInputLoci(
			'main',
			bio.generateLiftOverLoci(
				ucscBuildUser, ucscBuildDB,
				bio.generateLociFromText(positionList, separator=':', applyOffset=True, errorCallback=cb['position']),
				errorCallback=cb['position']
			),
			errorCallback=cb['position']
		)
	for positionFileList in (options.position_file or empty):
		bio.intersectInputLoci(
			'main',
			bio.generateLiftOverLoci(
				ucscBuildUser, ucscBuildDB,
				bio.generateLociFromMapFiles(positionFileList, applyOffset=True, errorCallback=cb['position']),
				errorCallback=cb['position']
			),
			errorCallback=cb['position']
		)
	for geneList in (options.gene or empty):
		bio.intersectInputGenes(
			'main',
			bio.generateNamesFromText(geneList, options.gene_identifier_type, separator=':', errorCallback=cb['gene']),
			errorCallback=cb['gene']
		)
	for geneFileList in (options.gene_file or empty):
		bio.intersectInputGenes(
			'main',
			bio.generateNamesFromNameFiles(geneFileList, options.gene_identifier_type, errorCallback=cb['gene']),
			errorCallback=cb['gene']
		)
	for geneSearch in (options.gene_search or empty):
		bio.intersectInputGeneSearch(
			'main',
			(2*(encodeString(s),) for s in geneSearch)
		)
	for regionList in (options.region or empty):
		bio.intersectInputRegions(
			'main',
			bio.generateLiftOverRegions(
				ucscBuildUser, ucscBuildDB,
				bio.generateRegionsFromText(regionList, separator=':', applyOffset=True, errorCallback=cb['region']),
				errorCallback=cb['region']
			),
			errorCallback=cb['region']
		)
	for regionFileList in (options.region_file or empty):
		bio.intersectInputRegions(
			'main',
			bio.generateLiftOverRegions(
				ucscBuildUser, ucscBuildDB,
				bio.generateRegionsFromFiles(regionFileList, applyOffset=True, errorCallback=cb['region']),
				errorCallback=cb['region']
			),
			errorCallback=cb['region']
		)
	for groupList in (options.group or empty):
		bio.intersectInputGroups(
			'main',
			bio.generateNamesFromText(groupList, options.group_identifier_type, separator=':', errorCallback=cb['group']),
			errorCallback=cb['group']
		)
	for groupFileList in (options.group_file or empty):
		bio.intersectInputGroups(
			'main',
			bio.generateNamesFromNameFiles(groupFileList, options.group_identifier_type, errorCallback=cb['group']),
			errorCallback=cb['group']
		)
	for groupSearch in (options.group_search or empty):
		bio.intersectInputGroupSearch(
			'main',
			(2*(encodeString(s),) for s in groupSearch)
		)
	for sourceList in (options.source or empty):
		bio.intersectInputSources(
			'main',
			sourceList,
			errorCallback=cb['source']
		)
	for sourceFile in itertools.chain(*(options.source_file or empty)):
		bio.intersectInputSources(
			'main',
			itertools.chain(*(line for line in open(sourceFile,'r'))),
			errorCallback=cb['source']
		)
	
	# apply alternate filters
	for snpList in (options.alt_snp or empty):
		bio.intersectInputSNPs(
			'alt',
			bio.generateRSesFromText(snpList, separator=':', errorCallback=cb['alt-SNP']),
			errorCallback=cb['alt-SNP']
		)
	for snpFileList in (options.alt_snp_file or empty):
		bio.intersectInputSNPs(
			'alt',
			bio.generateRSesFromRSFiles(snpFileList, errorCallback=cb['alt-SNP']),
			errorCallback=cb['alt-SNP']
		)
	for positionList in (options.alt_position or empty):
		bio.intersectInputLoci(
			'alt',
			bio.generateLiftOverLoci(
				ucscBuildUser, ucscBuildDB,
				bio.generateLociFromText(positionList, separator=':', applyOffset=True, errorCallback=cb['alt-position']),
				errorCallback=cb['alt-position']),
			errorCallback=cb['alt-position']
		)
	for positionFileList in (options.alt_position_file or empty):
		bio.intersectInputLoci(
			'alt',
			bio.generateLiftOverLoci(
				ucscBuildUser, ucscBuildDB,
				bio.generateLociFromMapFiles(positionFileList, applyOffset=True, errorCallback=cb['alt-position']),
				errorCallback=cb['alt-position']
			),
			errorCallback=cb['alt-position']
		)
	for geneList in (options.alt_gene or empty):
		bio.intersectInputGenes(
			'alt',
			bio.generateNamesFromText(geneList, options.gene_identifier_type, separator=':', errorCallback=cb['alt-gene']),
			errorCallback=cb['alt-gene']
		)
	for geneFileList in (options.alt_gene_file or empty):
		bio.intersectInputGenes(
			'alt',
			bio.generateNamesFromNameFiles(geneFileList, options.gene_identifier_type, errorCallback=cb['alt-gene']),
			errorCallback=cb['alt-gene']
		)
	for geneSearch in (options.alt_gene_search or empty):
		bio.intersectInputGeneSearch(
			'alt',
			(2*(encodeString(s),) for s in geneSearch)
		)
	for regionList in (options.alt_region or empty):
		bio.intersectInputRegions(
			'alt',
			bio.generateLiftOverRegions(
				ucscBuildUser, ucscBuildDB,
				bio.generateRegionsFromText(regionList, separator=':', applyOffset=True, errorCallback=cb['alt-region']),
				errorCallback=cb['alt-region']
			),
			errorCallback=cb['alt-region']
		)
	for regionFileList in (options.alt_region_file or empty):
		bio.intersectInputRegions(
			'alt',
			bio.generateLiftOverRegions(
				ucscBuildUser, ucscBuildDB,
				bio.generateRegionsFromFiles(regionFileList, applyOffset=True, errorCallback=cb['alt-region']),
				errorCallback=cb['alt-region']
			),
			errorCallback=cb['alt-region']
		)
	for groupList in (options.alt_group or empty):
		bio.intersectInputGroups(
			'alt',
			bio.generateNamesFromText(groupList, options.group_identifier_type, separator=':', errorCallback=cb['alt-group']),
			errorCallback=cb['alt-group']
		)
	for groupFileList in (options.alt_group_file or empty):
		bio.intersectInputGroups(
			'alt',
			bio.generateNamesFromNameFiles(groupFileList, options.group_identifier_type, errorCallback=cb['alt-group']),
			errorCallback=cb['alt-group']
		)
	for groupSearch in (options.alt_group_search or empty):
		bio.intersectInputGroupSearch(
			'alt',
			(2*(encodeString(s),) for s in groupSearch)
		)
	for sourceList in (options.alt_source or empty):
		bio.intersectInputSources(
			'alt',
			sourceList,
			errorCallback=cb['alt-source']
		)
	for sourceFile in itertools.chain(*(options.alt_source_file or empty)):
		bio.intersectInputSources(
			'alt',
			itertools.chain(*(line for line in open(sourceFile,'r'))),
			errorCallback=cb['alt-source']
		)
	
	# report invalid input, if requested
	if options.report_invalid_input == 'yes':
		for modtype,lines in cbLog.items():
			if lines:
				path = ('<stdout>' if options.stdout == 'yes' else typeOutputInfo['invalid'][modtype][1])
				bio.logPush("writing invalid %s input report to '%s' ...\n" % (modtype,path))
				outfile = (sys.stdout if options.stdout == 'yes' else open(path, 'w'))
				outfile.write("\n".join(lines))
				outfile.write("\n")
				if outfile != sys.stdout:
					outfile.close()
				bio.logPop("... OK: %d invalid inputs\n" % (len(lines)/2))
		#foreach modifier/type
	#if report invalid input
	
	# process filters
	for types,info in typeOutputInfo['filter'].items():
		label,path,outfile = info
		bio.logPush("writing %s to '%s' ...\n" % (label,path))
		n = -1 # don't count header
		for row in bio.generateFilterOutput(types, applyOffset=True):
			n += 1
			outfile.write(encodeRow(row))
		if outfile != sys.stdout:
			outfile.close()
		bio.logPop("... OK: %d results\n" % n)
	#foreach filter
	
	# process annotations
	for types,info in typeOutputInfo['annotation'].items():
		typesF,typesA = types
		label,path,outfile = info
		bio.logPush("writing %s to '%s' ...\n" % (label,path))
		n = -1 # don't count header
		for row in bio.generateAnnotationOutput(typesF, typesA, applyOffset=True):
			n += 1
			outfile.write(encodeRow(row))
		if outfile != sys.stdout:
			outfile.close()
		bio.logPop("... OK: %d results\n" % n)
	#foreach annotation
	
	# process models
	for types,info in typeOutputInfo['models'].items():
		typesL,typesR = types
		label,path,outfile = info
		bio.logPush("writing %s to '%s' ...\n" % (label,path))
		n = -1 # don't count header
		for row in bio.generateModelOutput(typesL, typesR, applyOffset=True):
			n += 1
			outfile.write(encodeRow(row))
		if outfile != sys.stdout:
			outfile.close()
		bio.logPop("... OK: %d results\n" % n)
	#foreach model
	
	# process PARIS algorithm
	if typeOutputInfo['paris']:
		#TODO html reports?
		parisGen = bio.generatePARISResults(ucscBuildUser, ucscBuildDB)
		labelS,pathS,outfileS = typeOutputInfo['paris']['summary']
		outfileD = None
		if 'detail' in typeOutputInfo['paris']:
			labelD,pathD,outfileD = typeOutputInfo['paris']['detail']
			bio.logPush("writing PARIS summary and detail to '%s' and '%s' ...\n" % (pathS,pathD))
		else:
			bio.logPush("writing PARIS summary to '%s'  ...\n" % (pathS,))
		header = next(parisGen)
		outfileS.write(encodeRow(header[:-1]))
		if outfileD:
			outfileD.write(encodeRow(header[0:2] + header[-1]))
		n = 0
		for row in parisGen:
			n += 1
			outfileS.write(encodeRow(row[:-1]))
			if outfileD:
				outfileD.write(encodeRow(row[0:2] + ('*',) + row[4:-1]))
				for rowD in row[-1]:
					outfileD.write(encodeRow(row[0:2] + rowD))
		if outfileS != sys.stdout:
			outfileS.close()
		if outfileD and (outfileD != sys.stdout):
			outfileD.close()
		bio.logPop("... OK: %d results\n" % n)
	#if PARIS
	
#__main__
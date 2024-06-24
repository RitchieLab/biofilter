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

"""
Biofilter class for managing biological data filtering.

This class provides functionality for managing biological data filtering using various tables and schemas.

Class methods:
	* getVersionTuple(): Returns the version tuple of the Biofilter class.

	* getVersionString(): Returns the version string of the Biofilter class.

Private class data:
	* _schema: Dictionary containing the schema information for main input filter tables.

Example usage:
	<code>biofilter = Biofilter()</code>	
	<code>version_tuple = biofilter.getVersionTuple()</code>	
	<code>version_string = biofilter.getVersionString()</code>	
"""	
class Biofilter:
	
	##################################################
	# class interrogation
	
	
	@classmethod
	def getVersionTuple(cls):
		"""
		Returns the version tuple of the Biofilter class.

		Returns:
			(tuple): A tuple representing the version information (major, minor, revision, dev, build, date).
		"""			
		# tuple = (major,minor,revision,dev,build,date)
		# dev must be in ('a','b','rc','release') for lexicographic comparison
		return (3,0,0,'release','','2024-06-20')
	#getVersionTuple()
	
	
	@classmethod
	def getVersionString(cls):
		"""
		Returns the version string of the Biofilter class.

		Returns:
			(str): A string representing the version information.
		"""		
		v = list(cls.getVersionTuple())
		# tuple = (major,minor,revision,dev,build,date)
		# dev must be > 'rc' for releases for lexicographic comparison,
		# but we don't need to actually print 'release' in the version string
		v[3] = '' if v[3] > 'rc' else v[3]
		return "%d.%d.%d%s%s (%s)" % tuple(v)
	#getVersionString()
	
	
	##################################################
	# private class data
	
	
	_schema = {
		##################################################
		# main input filter tables (copied for alt)
		
		'main' : {
			
			
			'snp' : {
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  rs INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0,
  extra TEXT
)
""",
				'index' : {
					'snp__rs' : '(rs)',
				}
			}, #main.snp
			
			
			'locus' : { # all coordinates in LOKI are 1-based closed intervals
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0,
  extra TEXT
)
""",
				'index' : {
					'locus__pos' : '(chr,pos)',
				}
			}, #main.locus
			
			
			'region' : { # all coordinates in LOKI are 1-based closed intervals
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0,
  extra TEXT
)
""",
				'index' : {
					'region__chr_min' : '(chr,posMin)',
					'region__chr_max' : '(chr,posMax)',
				}
			}, #main.region
			
			
			'region_zone' : {
				'table' : """
(
  region_rowid INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (chr,zone,region_rowid)
)
""",
				'index' : {
					'region_zone__region' : '(region_rowid)',
				}
			}, #main.region_zone
			
			
			'gene' : {
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0,
  extra TEXT
)
""",
				'index' : {
					'gene__biopolymer' : '(biopolymer_id)',
				}
			}, #main.gene
			
			
			'group' : {
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  group_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0,
  extra TEXT
)
""",
				'index' : {
					'group__group_id' : '(group_id)',
				}
			}, #main.group
			
			
			'source' : {
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  source_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index' : {
					'source__source_id' : '(source_id)',
				}
			}, #main.source
			
			
		}, #main
		
		
		##################################################
		# user data tables
		
		'user' : {
			
			
			'group': {
				'table': """
(
  group_id INTEGER PRIMARY KEY NOT NULL,
  label VARCHAR(64) NOT NULL,
  description VARCHAR(256),
  source_id INTEGER NOT NULL,
  extra TEXT
)
""",
				'index': {
					'group__label': '(label)',
				}
			}, #user.group
			
			
			'group_group': {
				'table': """
(
  group_id INTEGER NOT NULL,
  related_group_id INTEGER NOT NULL,
  contains TINYINT,
  PRIMARY KEY (group_id,related_group_id)
)
""",
				'index': {
					'group_group__related': '(related_group_id,group_id)',
				}
			}, #user.group_group
			
			
			'group_biopolymer': {
				'table': """
(
  group_id INTEGER NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  PRIMARY KEY (group_id,biopolymer_id)
)
""",
				'index': {
					'group_biopolymer__biopolymer': '(biopolymer_id,group_id)',
				}
			}, #user.group_biopolymer
			
			
			'source' : {
				'table' : """
(
  source_id INTEGER PRIMARY KEY NOT NULL,
  source VARCHAR(32) NOT NULL,
  description VARCHAR(256) NOT NULL
)
""",
				'index' : {}
			}, #user.source
			
			
		}, #user
		
		
		##################################################
		# modeling candidate tables
		
		'cand' : {
			
			
			'main_biopolymer' : {
				'table' : """
(
  biopolymer_id INTEGER PRIMARY KEY NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index' : {}
			}, #cand.main_biopolymer
			
			
			'alt_biopolymer' : {
				'table' : """
(
  biopolymer_id INTEGER PRIMARY KEY NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index' : {}
			}, #cand.alt_biopolymer
			
			
			'group' : {
				'table' : """
(
  group_id INTEGER PRIMARY KEY NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index' : {}
			}, #cand.group
			
			
		}, #cand
		
	} #_schema{}
	
	# copy main schema for alternate input filters
	_schema['alt'] = _schema['main']
	
	
	##################################################
	# constructor
	
	
	def __init__(self, options=None):
		"""
		Constructor for the Biofilter class.

		Initializes a Biofilter object with the given options.

		Args:
			options (object): An object containing options for Biofilter initialization. If None, default options are used.

		Returns:
			(NA): None
		"""			
		if not options:
			class Empty(object):
				def __getattr__(self, name):
					return None
			options = Empty()
		self._options = options
		
		self._quiet = (options.quiet == 'yes')
		self._verbose = (options.verbose == 'yes')
		self._logIndent = 0
		self._logHanging = False
		self._logFile = None
		if (options.stdout != 'yes'):
			logPath = options.prefix + '.log'
			if (options.overwrite != 'yes') and os.path.exists(logPath):
				sys.exit("ERROR: log file '%s' already exists, must specify --overwrite or a different --prefix" % logPath)
			self._logFile = open(logPath, 'w')
		
		self._tablesDeindexed = {db:set() for db in self._schema}
		self._inputFilters  = {db:{tbl:0 for tbl in self._schema[db]} for db in self._schema}
		self._geneModels = None
		self._onlyGeneModels = True #TODO
		
		# verify loki_db version 
		minLoki = (2,2,1,'a',2) # 'extra' input support in generateLiftOver*()
		if loki_db.Database.getVersionTuple() < minLoki:
			sys.exit("ERROR: LOKI version %d.%d.%d%s%s or later required; found %s" % minLoki+(loki_db.Database.getVersionString(),))
		
		# initialize instance database
		self._loki = loki_db.Database()
		self._loki.setLogger(self)
		for db in self._schema:
			if db != 'main': # in SQLite 'main' is implicit, but the others must be attached as temp stores
				self._loki.attachTempDatabase(db)
			self._loki.createDatabaseTables(self._schema[db], db, None, doIndecies=True)
	#__init__()
	
	
	##################################################
	# logging
	
	
	def _log(self, message="", warning=False):
		"""
		Internal method for logging messages.

		Args:
			message (str): The message to log.
			warning (bool): A flag indicating if the message is a warning.

		Returns:
			(NA): None
		"""	
		if (self._logIndent > 0) and (not self._logHanging):
			if self._logFile:
				self._logFile.write(self._logIndent * "  ")
			if self._verbose or (warning and not self._quiet):
				sys.stderr.write(self._logIndent * "  ")
			self._logHanging = True
		
		if self._logFile:
			self._logFile.write(message)
		if self._verbose or (warning and not self._quiet):
			sys.stderr.write(message)
		
		if message[-1:] != "\n":
			if self._logFile:
				self._logFile.flush()
			if self._verbose or (warning and not self._quiet):
				sys.stderr.flush()
			self._logHanging = True
		else:
			self._logHanging = False
	#_log()
	
	
	def log(self, message=""):
		"""
		Logs a message.

		Args:
			message (str): The message to log.

		Returns:
			(NA): None
		"""	
		self._log(message, False)
	#log()
	
	
	def logPush(self, message=None):
		"""
		Pushes the current log indentation level.

		Args:
			message (str): An optional message to log before pushing the indentation level.

		Returns:
			(NA): None
		"""	
		if message:
			self.log(message)
		if self._logHanging:
			self.log("\n")
		self._logIndent += 1
	#logPush()
	
	
	def logPop(self, message=None):
		"""
		Pops the current log indentation level.

		Args:
			message (str): An optional message to log after popping the indentation level.

		Returns:
			(NA): None
		"""			
		if self._logHanging:
			self.log("\n")
		self._logIndent = max(0, self._logIndent - 1)
		if message:
			self.log(message)
	#logPop()
	
	
	def warn(self, message=""):
		"""
		Logs a warning message.

		Args:
			message (str): The warning message to log.

		Returns:
			(NA): None
		"""		
		self._log(message, True)
	#warn()
	
	
	def warnPush(self, message=None):
		"""
		Pushes the current warning log indentation level.

		Args:
			message (str): An optional warning message to log before pushing the indentation level.

		Returns:
			(NA): None
		"""	
		if message:
			self.warn(message)
		if self._logHanging:
			self.warn("\n")
		self._logIndent += 1
	#warnPush()
	
	
	def warnPop(self, message=None):
		"""
		Pops the current warning log indentation level.

		Args:
			message (str): An optional warning message to log after popping the indentation level.

		Returns:
			(NA): None
		"""	
		if self._logHanging:
			self.warn("\n")
		self._logIndent = max(0, self._logIndent - 1)
		if message:
			self.warn(message)
	#warnPop()
	
	
	##################################################
	# database management
	
	
	def attachDatabaseFile(self, dbFile):
		"""
		Attaches a database file.

		Args:
			dbFile (str): The path to the database file.

		Returns:
			(NA): None
		"""				
		return self._loki.attachDatabaseFile(dbFile)
	#attachDatabaseFile()
	
	
	def prepareTableForUpdate(self, db, table):
		"""
		Prepares a table for update by dropping its indices.

		Args:
			db (str): The database name.
			table (str): The table name.

		Returns:
			(NA): None
		"""	
		assert((db in self._schema) and (table in self._schema[db]))
		if table not in self._tablesDeindexed[db]:
			self._tablesDeindexed[db].add(table)
			self._loki.dropDatabaseIndecies(self._schema[db], db, table)
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, db, table):
		"""
		Prepares a table for query by creating its indices.

		Args:
			db (str): The database name.
			table (str): The table name.

		Returns:
			(NA): None
		"""	
		assert((db in self._schema) and (table in self._schema[db]))
		if table in self._tablesDeindexed[db]:
			self._tablesDeindexed[db].remove(table)
			self._loki.createDatabaseIndecies(self._schema[db], db, table)
			if table == "region":
				self.updateRegionZones(db)
	#prepareTableForQuery()
	
	
	def tableHasData(self, db, table):
		"""
		Checks if a table has data.

		Args:
			db (str): The database name.
			table (str): The table name.

		Returns:
			(bool): True if the table has data, False otherwise.
		"""		
		return (sum(row[0] for row in self._loki._db.cursor().execute("SELECT 1 FROM `%s`.`%s` LIMIT 1" % (db,table))) > 0)
	#tableHasData()
	
	
	def updateRegionZones(self, db):
		"""
		Updates region zones.

		Args:
			db (str): The database name.

		Returns:
			(NA): None
		"""		
		assert((db in self._schema) and 'region' in self._schema[db] and 'region_zone' in self._schema[db])
		self.log("calculating %s region zone coverage ..." % db)
		cursor = self._loki._db.cursor()
		
		size = self._loki.getDatabaseSetting('zone_size')
		if not size:
			sys.exit("ERROR: could not determine database setting 'zone_size'")
		size = int(size)
		
		# make sure all regions are correctly oriented
		cursor.execute("UPDATE `%s`.`region` SET posMin = posMax, posMax = posMin WHERE posMin > posMax" % db)
		
		# define zone generator
		def _zones(size, regions):
			"""
			Generates zone information for regions.

			Args:
				size (int): The zone size.
				regions: The regions.

			Yields:
				(tuple): Zone information.
			"""				
			# regions=[ (id,chr,posMin,posMax),... ]
			# yields:[ (id,chr,zone),... ]
			for rowid,chm,posMin,posMax in regions:
				for z in range(int(posMin/size),int(posMax/size)+1):
					yield (rowid,chm,z)
		#_zones()
		
		# feed all regions through the zone generator
		# (use a separate cursor to iterate both results simultaneously)
		self.prepareTableForQuery(db, 'region')
		self.prepareTableForUpdate(db, 'region_zone')
		cursor.execute("DELETE FROM `%s`.`region_zone`" % db)
		cursor.executemany(
			"INSERT OR IGNORE INTO `%s`.`region_zone` (region_rowid,chr,zone) VALUES (?,?,?)" % db,
			_zones(
				size,
				self._loki._db.cursor().execute("SELECT rowid,chr,posMin,posMax FROM `%s`.`region`" % db)
			)
		)
		self.prepareTableForQuery(db, 'region_zone')
		
		self._inputFilters[db]['region_zone'] = self._inputFilters[db]['region']
		self.log(" OK\n")
	#updateRegionZones()
	
	
	##################################################
	# LOKI metadata retrieval
	
	
	def getSourceFingerprints(self):
		"""
		Retrieves source fingerprints.

		Returns:
			(OrderedDict): Source fingerprints.
		"""			
		ret = collections.OrderedDict()
		sourceIDs = self._loki.getSourceIDs()
		for source in sorted(sourceIDs):
			ret[source] = (
					self._loki.getSourceIDVersion(sourceIDs[source]),
					self._loki.getSourceIDOptions(sourceIDs[source]),
					self._loki.getSourceIDFiles(sourceIDs[source])
			)
		return ret
	#getSourceFingerprints()
	
	
	def generateGeneNameStats(self):
		"""
		Generates statistics for gene names.

		Returns:
			(dict): Gene name statistics.
		"""		
		typeID = self._loki.getTypeID('gene')
		if not typeID:
			sys.exit("ERROR: knowledge file contains no gene data")
		return self._loki.generateBiopolymerNameStats(typeID=typeID)
	#generateGeneNameStats()
	
	
	def generateGroupNameStats(self):
		"""
		Generates statistics for group names.

		Returns:
			(dict): Group name statistics.
		"""		
		return self._loki.generateGroupNameStats()
	#generateGroupNameStats()
	
	
	def generateLDProfiles(self):
		"""
		Generates LD profiles.

		Yields:
			(tuple): LD profile information.
		"""		
		ldprofiles = self._loki.getLDProfiles()
		for l in sorted(ldprofiles):
			yield (l,)+ldprofiles[l][1:]
	#generateLDProfiles()
	
	
	##################################################
	# LOKI data retrieval
	
	
	def getDatabaseGenomeBuilds(self):
		"""
		Retrieves genome build information from the database.

		Returns:
			(tuple): A tuple containing the GRCh build and UCSC hg build.
		"""	
		ucscBuild = self._loki.getDatabaseSetting('ucschg')
		ucscBuild = int(ucscBuild) if (ucscBuild != None) else None
		grchBuild = None
		if ucscBuild:
			for build in self._loki.generateGRChByUCSChg(ucscBuild):
				if grchBuild is None:
					grchBuild = int(build)
					continue
				grchBuild = max(grchBuild, int(build))
		return (grchBuild,ucscBuild)
	#getDatabaseGenomeBuilds()
	
	
	def getOptionTypeID(self, value, optional=False):
		"""
		Retrieves the type ID corresponding to the given value.

		Args:
			value (str): The value to retrieve the type ID for.
			optional (bool, optional): Whether the value is optional. Defaults to False.

		Returns:
			(int): The type ID.

		Raises:
			SystemExit: If the database contains no data for the specified value and it's not optional.
		"""		
		typeID = self._loki.getTypeID(value)
		if not (typeID or optional):
			sys.exit("ERROR: database contains no %s data\n" % (value,))
		return typeID
	#getOptionTypeID()
	
	
	def getOptionNamespaceID(self, value, optional=False):
		"""
		Retrieves the namespace ID corresponding to the given value.

		Args:
			value (str): The value to retrieve the namespace ID for.
			optional (bool, optional): Whether the value is optional. Defaults to False.

		Returns:
			(int): The namespace ID.

		Raises:
			SystemExit: If the value is not found in the database and it's not optional.
		"""		
		if value == '-': # primary labels
			return None
		namespaceID = self._loki.getNamespaceID(value)
		if not (namespaceID or optional):
			sys.exit("ERROR: unknown identifier type '%s'\n" % (value,))
		return namespaceID
	#getOptionNamespaceID()
	
	
	##################################################
	# input data parsers and lookup helpers
	
	
	def getInputGenomeBuilds(self, grchBuild, ucscBuild):
		"""
		Retrieves genome build information for input data.

		Args:
			grchBuild (int): The GRCh build.
			ucscBuild (int): The UCSC hg build.

		Returns:
			(tuple): A tuple containing the GRCh build and UCSC hg build.
		"""	
		if grchBuild:
			if ucscBuild:
				if ucscBuild != (self._loki.getUCSChgByGRCh(grchBuild) or ucscBuild):
					sys.exit("ERROR: specified reference genome build GRCh%d is not known to correspond to UCSC hg%d" % (grchBuild, ucscBuild))
			else:
				ucscBuild = self._loki.getUCSChgByGRCh(grchBuild)
		elif ucscBuild:
			grchBuild = None
			for build in self._loki.generateGRChByUCSChg(ucscBuild):
				if grchBuild:
					grchBuild = max(grchBuild, int(build))
				else:
					grchBuild = int(build)
		return (grchBuild,ucscBuild)
	#getInputGenomeBuilds()
	
	
	def generateMergedFilteredSNPs(self, snps, tally=None, errorCallback=None):
		"""
		Generates merged and filtered SNPs.

		Args:
			snps (list): SNPs data.
			tally (dict, optional): Dictionary to tally SNP counts. Defaults to None.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): Merged SNP information.
		"""	
		# snps=[ (rsInput,extra),... ]
		# yield:[ (rsInput,extra,rsCurrent)
		tallyMerge = dict() if (tally != None) else None
		tallyLocus = dict() if (tally != None) else None
		genMerge = self._loki.generateCurrentRSesByRSes(snps, tally=tallyMerge) # (rs,extra) -> (rsold,extra,rsnew)
		if self._options.allow_ambiguous_snps == 'yes':
			for row in genMerge:
				yield row
		else:
			genMergeFormat = ((str(rsnew),str(rsold)+"\t"+str(rsextra or "")) for rsold,rsextra,rsnew in genMerge) # (rsold,extra,rsnew) -> (rsnew,rsold+extra)
			genLocus = self._loki.generateSNPLociByRSes(
				genMergeFormat,
				minMatch=0,
				maxMatch=1,
				validated=(None if (self._options.allow_unvalidated_snp_positions == 'yes') else True),
				tally=tallyLocus,
				errorCallback=errorCallback
			) # (rsnew,rsold+extra) -> (rsnew,rsold+extra,chr,pos)
			genLocusFormat = (tuple(posextra.split("\t",1)+[rs]) for rs,posextra,chm,pos in genLocus) # (rsnew,rsold+extra,chr,pos) -> (rsold,extra,rsnew)
			for row in genLocusFormat:
				yield row
		#if allow_ambiguous_snps
		if tallyMerge != None:
			tally.update(tallyMerge)
		if tallyLocus != None:
			tally.update(tallyLocus)
	#generateMergedFilteredSNPs()
	
	
	def generateRSesFromText(self, lines, separator=None, errorCallback=None):
		"""
		Generates RSes from text data.

		Args:
			lines (list): Lines of text data.
			separator (str, optional): Separator for columns. Defaults to None.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): RS information.
		"""	
		l = 0
		for line in lines:
			l += 1
			try:
				cols = line.strip().split(separator,1)
				if not cols:
					continue
				try:
					rs = int(cols[0])
				except ValueError:
					if cols[0].upper().startswith('RS'):
						rs = int(cols[0][2:])
					else:
						raise
				extra = cols[1] if (len(cols) > 1) else None
				yield (rs,extra)
			except:
				if (l > 1) and errorCallback:
					errorCallback(line, "%s at index %d" % (str(sys.exc_info()[1]),l))
		#foreach line
	#generateRSesFromText()
	
	
	def generateRSesFromRSFiles(self, paths, separator=None, errorCallback=None):
		"""
		Generates RSes from RS files.

		Args:
			paths (list): Paths to RS files.
			separator (str, optional): Separator for columns. Defaults to None.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): RS information.
		"""	
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'r')) as file:
					for data in self.generateRSesFromText((line for line in file if not line.startswith('#')), separator, errorCallback):
						yield data
				#with file
			except:
				self.warn("WARNING: error reading input file '%s': %s\n" % (path,str(sys.exc_info()[1])))
				if errorCallback:
					errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
		#foreach path
	#generateRSesFromRSFiles()
	
	
	def generateLociFromText(self, lines, separator=None, applyOffset=False, errorCallback=None):
		"""
		Generates loci from text data.

		Args:
			lines (list): Lines of text data.
			separator (str, optional): Separator for columns. Defaults to None.
			applyOffset (bool, optional): Whether to apply an offset. Defaults to False.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): Locus information.
		"""	
		# parse input/output coordinate offsets
		offset = (1 - self._options.coordinate_base) if applyOffset else 0
		
		l = 0
		for line in lines:
			l += 1
			try:
				# parse columns
				cols = line.strip().split(separator,4)
				label = chm = pos = extra = None
				if not cols:
					continue
				elif len(cols) < 2:
					raise Exception("not enough columns")
				elif len(cols) == 2:
					chm = cols[0].upper()
					pos = cols[1].upper()
				elif len(cols) == 3:
					chm = cols[0].upper()
					label = cols[1]
					pos = cols[2].upper()
				elif len(cols) >= 4:
					chm = cols[0].upper()
					label = cols[1]
					pos = cols[3].upper()
					extra = cols[4] if (len(cols) > 4) else None
				
				# parse, validate and convert chromosome
				if chm.startswith('CHR'):
					chm = chm[3:]
				if chm not in self._loki.chr_num:
					raise Exception("invalid chromosome '%s'" % chm)
				chm = self._loki.chr_num[chm]
				
				# parse and convert locus label
				if not label:
					label = 'chr%s:%s' % (self._loki.chr_name[chm], pos)
				
				# parse and convert position
				if (pos == '-') or (pos == 'NA'):
					pos = None
				else:
					pos = int(pos) + offset
				yield (label,chm,pos,extra)
			except:
				if (l > 1) and errorCallback:
					errorCallback(line, "%s at index %d" % (str(sys.exc_info()[1]),l))
		#foreach line
	#generateLociFromText()
	
	
	def generateLociFromMapFiles(self, paths, separator=None, applyOffset=False, errorCallback=None):
		"""
		Generates loci from map files.

		Args:
			paths (list): Paths to map files.
			separator (str, optional): Separator for columns. Defaults to None.
			applyOffset (bool, optional): Whether to apply an offset. Defaults to False.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): Locus information.
		"""	
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'r')) as file:
					for data in self.generateLociFromText((line for line in file if not line.startswith('#')), separator, applyOffset, errorCallback):
						yield data
				#with file
			except:
				self.warn("WARNING: error reading input file '%s': %s\n" % (path,str(sys.exc_info()[1])))
				if errorCallback:
					errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
		#foreach path
	#generateLociFromMapFiles()
	
	
	def generateLiftOverLoci(self, ucscBuildOld, ucscBuildNew, loci, errorCallback=None):
		"""
		Generates lift-over loci.

		Args:
			ucscBuildOld (int): Old UCSC build version.
			ucscBuildNew (int): New UCSC build version.
			loci (list): Loci data.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Returns:
			(list): Lift-over loci.
		"""	
		# loci=[ (label,chr,pos,extra), ... ]
		newloci = loci
		
		if not ucscBuildOld:
			self.warn("WARNING: UCSC hg# build version was not specified for position input; assuming it matches the knowledge database\n")
		elif not ucscBuildNew:
			self.warn("WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n")
		elif ucscBuildOld != ucscBuildNew:
			if not self._loki.hasLiftOverChains(ucscBuildOld, ucscBuildNew):
				sys.exit("ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg%s to hg%s\n" % (ucscBuildOld or "?", ucscBuildNew or "?"))
			liftoverError = "dropped during liftOver from hg%s to hg%s" % (ucscBuildOld or "?", ucscBuildNew or "?")
			def liftoverCallback(region):
				errorCallback("\t".join(str(s) for s in region), liftoverError)
			#liftoverCallback()
			newloci = self._loki.generateLiftOverLoci(ucscBuildOld, ucscBuildNew, loci, tally=None, errorCallback=(liftoverCallback if errorCallback else None))
		#if old!=new
		
		return newloci
	#generateLiftOverLoci()
	
	
	def generateRegionsFromText(self, lines, separator=None, applyOffset=False, errorCallback=None):
		"""
		Generates regions from text data.

		Args:
			lines (list): Lines of text data.
			separator (str, optional): Separator for columns. Defaults to None.
			applyOffset (bool, optional): Whether to apply an offset. Defaults to False.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): Region information.
		"""	
		offsetStart = offsetEnd = (1 - self._options.coordinate_base) if applyOffset else 0
		if applyOffset and (self._options.regions_half_open == 'yes'):
			offsetEnd -= 1
		
		l = 0
		for line in lines:
			l += 1
			try:
				# parse columns
				cols = line.strip().split(separator,4)
				label = chm = posMin = posMax = extra = None
				if not cols:
					continue
				elif len(cols) < 3:
					raise Exception("not enough columns")
				elif len(cols) == 3:
					chm = cols[0].upper()
					posMin = cols[1].upper()
					posMax = cols[2].upper()
				elif len(cols) >= 4:
					chm = cols[0].upper()
					label = cols[1]
					posMin = cols[2].upper()
					posMax = cols[3].upper()
					extra = cols[4] if (len(cols) > 4) else None
				
				# parse, validate and convert chromosome
				if chm.startswith('CHR'):
					chm = chm[3:]
				if chm not in self._loki.chr_num:
					raise Exception("invalid chromosome '%s'" % chm)
				chm = self._loki.chr_num[chm]
				
				# parse and convert region label
				if not label:
					label = 'chr%s:%s-%s' % (self._loki.chr_name[chm], posMin, posMax)
				
				# parse and convert positions
				if (posMin == '-') or (posMin == 'NA'):
					posMin = None
				else:
					posMin = int(posMin) + offsetStart
				if (posMax == '-') or (posMax == 'NA'):
					posMax = None
				else:
					posMax = int(posMax) + offsetEnd
				
				yield (label,chm,posMin,posMax,extra)
			except:
				if (l > 1) and errorCallback:
					errorCallback(line, "%s at index %d" % (str(sys.exc_info()[1]),l))
		#foreach line
	#generateRegionsFromText()
	
	
	def generateRegionsFromFiles(self, paths, separator=None, applyOffset=False, errorCallback=None):
		"""
		Generates regions from files.

		Args:
			paths (list): Paths to region files.
			separator (str, optional): Separator for columns. Defaults to None.
			applyOffset (bool, optional): Whether to apply an offset. Defaults to False.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): Region information.
		"""	
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'r')) as file:
					for data in self.generateRegionsFromText((line for line in file if not line.startswith('#')), separator, applyOffset, errorCallback):
						yield data
				#with file
			except:
				self.warn("WARNING: error reading input file '%s': %s\n" % (path,str(sys.exc_info()[1])))
				if errorCallback:
					errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
		#foreach path
	#generateRegionsFromFiles()
	
	
	def generateLiftOverRegions(self, ucscBuildOld, ucscBuildNew, regions, errorCallback=None):
		"""
		Generates lift-over regions.

		Args:
			ucscBuildOld (int): Old UCSC build version.
			ucscBuildNew (int): New UCSC build version.
			regions (list): Regions data.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Returns:
			(list): Lift-over regions.
		"""		
		# regions=[ (label,chr,posMin,posMax,extra), ... ]
		newregions = regions
		
		if not ucscBuildOld:
			self.warn("WARNING: UCSC hg# build version was not specified for region input; assuming it matches the knowledge database\n")
		elif not ucscBuildNew:
			self.warn("WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n")
		elif ucscBuildOld != ucscBuildNew:
			if not self._loki.hasLiftOverChains(ucscBuildOld, ucscBuildNew):
				sys.exit("ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg%s to hg%s\n" % (ucscBuildOld or "?", ucscBuildNew or "?"))
			liftoverError = "dropped during liftOver from hg%s to hg%s" % (ucscBuildOld or "?", ucscBuildNew or "?")
			def liftoverCallback(region):
				errorCallback("\t".join(str(s) for s in region), liftoverError)
			#liftoverCallback()
			newregions = self._loki.generateLiftOverRegions(ucscBuildOld, ucscBuildNew, regions, tally=None, errorCallback=(liftoverCallback if errorCallback else None))
		#if old!=new
		
		return newregions
	#generateLiftOverRegions()
	
	
	def generateNamesFromText(self, lines, defaultNS=None, separator=None, errorCallback=None):
		"""
		Generates names from text data.

		Args:
			lines (list): Lines of text data.
			defaultNS (str, optional): Default namespace. Defaults to None.
			separator (str, optional): Separator for columns. Defaults to None.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): Name information.
		"""	
#		utf8 = codecs.getencoder('utf8')
		l = 0
		for line in lines:
			l += 1
			try:
				cols = line.strip().split(separator,2)
				ns = name = extra = None
				if not cols:
					continue
				elif len(cols) == 1:
					ns = defaultNS
					name = str(cols[0].strip())
				elif len(cols) >= 2:
					ns = cols[0].strip()
					name = str(cols[1].strip())
					extra = cols[2] if (len(cols) > 2) else None
				yield (ns,name,extra)
			except:
				if (l > 1) and errorCallback:
					errorCallback(line, "%s at index %d" % (str(sys.exc_info()[1]),l))
		#foreach line in file
	#generateNamesFromText()
	

	def generateNamesFromNameFiles(self, paths, defaultNS=None, separator=None, errorCallback=None):
		"""
		Generates names from name files.

		Args:
			paths (list): Paths to name files.
			defaultNS (str, optional): Default namespace. Defaults to None.
			separator (str, optional): Separator for columns. Defaults to None.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Yields:
			(tuple): Name information.
		"""	
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'r')) as file:
					for data in self.generateNamesFromText((line for line in file if not line.startswith('#')), defaultNS, separator, errorCallback):
						yield data
				#with file
			except:
				self.warn("WARNING: error reading input file '%s': %s\n" % (path,str(sys.exc_info()[1])))
				if errorCallback:
					errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
		#foreach path
	#generateNamesFromNameFiles()
	
	
	def loadUserKnowledgeFile(self, path, defaultNS=None, separator=None, errorCallback=None):
		"""
		Loads user knowledge from a file.

		Args:
			path (str): Path to the knowledge file.
			defaultNS (str, optional): Default namespace. Defaults to None.
			separator (str, optional): Separator for columns. Defaults to None.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		utf8 = codecs.getencoder('utf8')
		try:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
				words = utf8(file.next())[0].strip().split(separator,1)
				label = words[0]
				description = words[1] if (len(words) > 1) else ''
				usourceID = self.addUserSource(label, description)
				ugroupID = namesets = None
				for line in file:
					words = utf8(line)[0].strip().split(separator)
					if not words:
						pass
					elif words[0] == 'GROUP':
						if ugroupID and namesets:
							self.addUserGroupBiopolymers(ugroupID, namesets, errorCallback)
						label = words[1] if (len(words) > 1) else None
						description = " ".join(words[2:])
						ugroupID = self.addUserGroup(usourceID, label, description, errorCallback)
						namesets = list()
					elif words[0] == 'CHILDREN':
						pass #TODO eventual support for group hierarchies
					elif ugroupID:
						namesets.append(list( (defaultNS,w,None) for w in words ))
				#foreach line
				if ugroupID and namesets:
					self.addUserGroupBiopolymers(ugroupID, namesets, errorCallback)
			#with file
		except:
			self.warn("WARNING: error reading input file '%s': %s\n" % (path,str(sys.exc_info()[1])))
			if errorCallback:
				errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
	#loadUserKnowledgeFile()
	
	
	##################################################
	# snp input
	
	
	def unionInputSNPs(self, db, snps, errorCallback=None):
		"""
		Adds SNPs to the SNP filter.

		Args:
			db (str): Database name.
			snps (list): SNP data.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# snps=[ (rs,extra), ... ]
		self.logPush("adding to %s SNP filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'snp')
		sql = "INSERT INTO `%s`.`snp` (label,extra,rs) VALUES ('rs'||?1,?2,?3)" % db
		tally = dict()
		cursor.executemany(sql, self.generateMergedFilteredSNPs(snps, tally, errorCallback))
		
		if tally.get('many'):
			self.logPop("... OK: added %d SNPs (%d RS#s merged, %d ambiguous)\n" % (tally['match']+tally['merge']-tally['many'],tally['merge'],tally['many']))
		else:
			self.logPop("... OK: added %d SNPs (%d RS#s merged)\n" % (tally['match']+tally['merge'],tally['merge']))
		self._inputFilters[db]['snp'] += 1
	#unionInputSNPs()
	
	
	def intersectInputSNPs(self, db, snps, errorCallback=None):
		"""
		Reduces the SNP filter.

		Args:
			db (str): Database name.
			snps (list): SNP data.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# snps=[ (rs,extra), ... ]
		if not self._inputFilters[db]['snp']:
			return self.unionInputSNPs(db, snps, errorCallback)
		self.logPush("reducing %s SNP filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'snp')
		cursor.execute("UPDATE `%s`.`snp` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`snp` SET flag = 1 WHERE (1 OR ?1 OR ?2) AND rs = ?3" % db
		tally = dict()
		# we don't have to do ambiguous snp filtering here because we're only reducing what's already loaded
		cursor.executemany(sql, self._loki.generateCurrentRSesByRSes(snps, tally))
		cursor.execute("DELETE FROM `%s`.`snp` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		
		self.logPop("... OK: kept %d SNPs (%d dropped, %d RS#s merged)\n" % (numBefore-numDrop,numDrop,tally['merge']))
		self._inputFilters[db]['snp'] += 1
	#intersectInputSNPs()
	
	
	##################################################
	# locus/position input
	
	
	def unionInputLoci(self, db, loci, errorCallback=None):
		"""
		Adds loci to the position filter.

		Args:
			db (str): Database name.
			loci (list): Loci data.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# loci=[ (label,chr,pos,extra), ... ]
		self.logPush("adding to %s position filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		# use OR IGNORE to continue on data error, i.e. missing chr or pos
		self.prepareTableForUpdate(db, 'locus')
		sql = "INSERT OR IGNORE INTO `%s`.`locus` (label,chr,pos,extra) VALUES (?1,?2,?3,?4); SELECT LAST_INSERT_ROWID(),?1,?2,?3,?4" % db
		n = lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, (2*locus for locus in loci)):
			n += 1
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
				if errorCallback:
					errorCallback("\t".join(row[1:]), "invalid data at index %d" % (n,))
		if numNull:
			self.warn("WARNING: ignored %d invalid positions\n" % numNull)
		self.logPop("... OK: added %d positions\n" % numAdd)
		
		self._inputFilters[db]['locus'] += 1
	#unionInputLoci()
	
	
	def intersectInputLoci(self, db, loci, errorCallback=None):
		"""
		Reduces the position filter.

		Args:
			db (str): Database name.
			loci (list): Loci data.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# loci=[ (label,chr,pos,extra), ... ]
		if not self._inputFilters[db]['locus']:
			return self.unionInputLoci(db, loci, errorCallback)
		self.logPush("reducing %s position filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'locus')
		cursor.execute("UPDATE `%s`.`locus` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`locus` SET flag = 1 WHERE (1 OR ?1) AND chr = ?2 AND pos = ?3 AND (1 OR ?4)" % db
		cursor.executemany(sql, loci)
		cursor.execute("DELETE FROM `%s`.`locus` WHERE flag = 0" % db)
		numDrop = self._loki._db.changes()
		self.logPop("... OK: kept %d positions (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['locus'] += 1
	#intersectInputLoci()
	
	
	##################################################
	## region input
	
	
	def unionInputRegions(self, db, regions, errorCallback=None):
		"""
		Adds regions to the region filter.

		Args:
			db (str): Database name.
			regions (list): Region data.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# regions=[ (label,chr,posMin,posMax,extra), ... ]
		self.logPush("adding to %s region filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		# use OR IGNORE to continue on data error, i.e. missing chr or pos
		self.prepareTableForUpdate(db, 'region')
		sql = "INSERT OR IGNORE INTO `%s`.`region` (label,chr,posMin,posMax,extra) VALUES (?1,?2,?3,?4,?5); SELECT LAST_INSERT_ROWID(),?1,?2,?3,?4,?5" % db
		n = lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, (2*region for region in regions)):
			n += 1
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
				if errorCallback:
					errorCallback("\t".join(row[1:]), "invalid data at index %d" % (n,))
		if numNull:
			self.warn("WARNING: ignored %d invalid regions\n" % numNull)
		self.logPop("... OK: added %d regions\n" % numAdd)
		
		self._inputFilters[db]['region'] += 1
	#unionInputRegions()
	
	
	def intersectInputRegions(self, db, regions, errorCallback=None):
		"""
		Reduces the region filter.

		Args:
			db (str): Database name.
			regions (list): Region data.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# regions=[ (label,chr,posMin,posMax,extra), ... ]
		if not self._inputFilters[db]['region']:
			return self.unionInputRegions(db, regions, errorCallback)
		self.logPush("reducing %s region filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'region')
		cursor.execute("UPDATE `%s`.`region` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`region` SET flag = 1 WHERE (1 OR ?1) AND chr = ?2 AND posMin = ?3 AND posMax = ?4 AND (1 OR ?5)" % db
		cursor.executemany(sql, regions)
		cursor.execute("DELETE FROM `%s`.`region` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d regions (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['region'] += 1
	#intersectInputRegions()
	
	
	##################################################
	# gene input
	
	
	def unionInputGenes(self, db, names, errorCallback=None):
		"""
		Adds genes to the gene filter.

		Args:
			db (str): Database name.
			names (list): Gene names.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# names=[ (namespace,name,extra), ... ]
		self.logPush("adding to %s gene filter ...\n" % db)
		cursor = self._loki._db.cursor()
		self.prepareTableForUpdate(db, 'gene')
		sql = "INSERT INTO `%s`.`gene` (label,extra,biopolymer_id) VALUES (?2,?3,?4); SELECT 1" % db
		maxMatch = (None if self._options.allow_ambiguous_genes == 'yes' else 1)
		tally = dict()
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateTypedBiopolymerIDsByIdentifiers(
				self.getOptionTypeID('gene'), names, minMatch=1, maxMatch=maxMatch, tally=tally, errorCallback=errorCallback
		)):
			numAdd += 1
		if tally['zero']:
			self.warn("WARNING: ignored %d unrecognized gene identifier(s)\n" % tally['zero'])
		if tally['many']:
			if self._options.allow_ambiguous_genes == 'yes':
				self.warn("WARNING: added multiple results for %d ambiguous gene identifier(s)\n" % tally['many'])
			else:
				self.warn("WARNING: ignored %d ambiguous gene identifier(s)\n" % tally['many'])
		self.logPop("... OK: added %d genes\n" % numAdd)
		
		self._inputFilters[db]['gene'] += 1
	#unionInputGenes()
	
	
	def intersectInputGenes(self, db, names, errorCallback=None):
		"""
		Reduces the gene filter.

		Args:
			db (str): Database name.
			names (list): Gene names.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# names=[ (namespace,name), ... ]
		if not self._inputFilters[db]['gene']:
			return self.unionInputGenes(db, names, errorCallback)
		self.logPush("reducing %s gene filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'gene')
		cursor.execute("UPDATE `%s`.`gene` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		tally = dict()
		sql = "UPDATE `%s`.`gene` SET flag = 1 WHERE biopolymer_id = ?4" % db
		maxMatch = (None if self._options.allow_ambiguous_genes == 'yes' else 1)
		cursor.executemany(sql, self._loki.generateTypedBiopolymerIDsByIdentifiers(
				self.getOptionTypeID('gene'), names, minMatch=1, maxMatch=maxMatch, tally=tally, errorCallback=errorCallback
		))
		cursor.execute("DELETE FROM `%s`.`gene` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		if tally['zero']:
			self.warn("WARNING: ignored %d unrecognized gene identifier(s)\n" % tally['zero'])
		if tally['many']:
			if self._options.allow_ambiguous_genes == 'yes':
				self.warn("WARNING: kept multiple results for %d ambiguous gene identifier(s)\n" % tally['many'])
			else:
				self.warn("WARNING: ignored %d ambiguous gene identifier(s)\n" % tally['many'])
		self.logPop("... OK: kept %d genes (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['gene'] += 1
	#intersectInputGenes()
	
	
	def unionInputGeneSearch(self, db, texts):
		"""
		Adds genes to the gene filter by text search.

		Args:
			db (str): Database name.
			texts (list): Text data for gene search.
		"""		
		# texts=[ (text,extra), ... ]
		self.logPush("adding to %s gene filter by text search ...\n" % db)
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID('gene')
		
		self.prepareTableForUpdate(db, 'gene')
		sql = "INSERT INTO `%s`.`gene` (extra,label,biopolymer_id) VALUES (?1,?2,?3); SELECT 1" % db
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateTypedBiopolymerIDsBySearch(typeID, texts)):
			numAdd += 1
		self.logPop("... OK: added %d genes\n" % numAdd)
		
		self._inputFilters[db]['gene'] += 1
	#unionInputGeneSearch()
	
	
	def intersectInputGeneSearch(self, db, texts):
		"""
		Reduces the gene filter by text search.

		Args:
			db (str): Database name.
			texts (list): Text data for gene search.
		"""	
		# texts=[ (text,extra), ... ]
		if not self._inputFilters[db]['gene']:
			return self.unionInputGeneSearch(db, texts)
		self.logPush("reducing %s gene filter by text search ...\n" % db)
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID('gene')
		
		self.prepareTableForQuery(db, 'gene')
		cursor.execute("UPDATE `%s`.`gene` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`gene` SET flag = 1 WHERE biopolymer_id = ?3" % db
		cursor.executemany(sql, self._loki.generateTypedBiopolymerIDsBySearch(typeID, texts))
		cursor.execute("DELETE FROM `%s`.`gene` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d genes (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['gene'] += 1
	#intersectInputGeneSearch()
	
	
	##################################################
	# group input
	
	
	def unionInputGroups(self, db, names, errorCallback=None):
		"""
		Adds groups to the group filter.

		Args:
			db (str): Database name.
			names (list): Group names.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""
		# names=[ (namespace,name,extra), ... ]
		self.logPush("adding to %s group filter ...\n" % (db,))
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'group')
		sql = "INSERT INTO `%s`.`group` (label,extra,group_id) VALUES (?2,?3,?4); SELECT 1" % db
		maxMatch = (None if self._options.allow_ambiguous_groups == 'yes' else 1)
		tally = dict()
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateGroupIDsByIdentifiers(
				names, minMatch=1, maxMatch=maxMatch, tally=tally, errorCallback=errorCallback
		)):
			numAdd += 1
		if tally['zero']:
			self.warn("WARNING: ignored %d unrecognized group identifier(s)\n" % tally['zero'])
		if tally['many']:
			if self._options.allow_ambiguous_groups == 'yes':
				self.warn("WARNING: added multiple results for %d ambiguous group identifier(s)\n" % tally['many'])
			else:
				self.warn("WARNING: ignored %d ambiguous group identifier(s)\n" % tally['many'])
		self.logPop("... OK: added %d groups\n" % numAdd)
		
		self._inputFilters[db]['group'] += 1
	#unionInputGroups()
	
	
	def intersectInputGroups(self, db, names, errorCallback=None):
		"""
		Reduces the group filter.

		Args:
			db (str): Database name.
			names (list): Group names.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# names=[ (namespace,name,extra), ... ]
		if not self._inputFilters[db]['group']:
			return self.unionInputGroups(db, names, errorCallback)
		self.logPush("reducing %s group filter ...\n" % (db,))
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'group')
		cursor.execute("UPDATE `%s`.`group` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		maxMatch = (None if self._options.allow_ambiguous_groups == 'yes' else 1)
		tally = dict()
		sql = "UPDATE `%s`.`group` SET flag = 1 WHERE group_id = ?4" % db
		cursor.executemany(sql, self._loki.generateGroupIDsByIdentifiers(
				names, minMatch=1, maxMatch=maxMatch, tally=tally, errorCallback=errorCallback
		))
		cursor.execute("DELETE FROM `%s`.`group` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		if tally['zero']:
			self.warn("WARNING: ignored %d unrecognized group identifier(s)\n" % tally['zero'])
		if tally['many']:
			if self._options.allow_ambiguous_groups == 'yes':
				self.warn("WARNING: kept multiple results for %d ambiguous group identifier(s)\n" % tally['many'])
			else:
				self.warn("WARNING: ignored %d ambiguous group identifier(s)\n" % tally['many'])
		self.logPop("... OK: kept %d groups (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['group'] += 1
	#intersectInputGroups()
	
	
	def unionInputGroupSearch(self, db, texts):
		"""
		Adds groups to the group filter by text search.

		Args:
			db (str): Database name.
			texts (list): Text data for group search.
		"""	
		# texts=[ (text,extra), ... ]
		self.logPush("adding to %s group filter by text search ...\n" % (db,))
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'group')
		sql = "INSERT INTO `%s`.`group` (extra,label,group_id) VALUES (?1,?2,?3); SELECT 1" % db
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateGroupIDsBySearch(texts)):
			numAdd += 1
		self.logPop("... OK: added %d groups\n" % numAdd)
		
		self._inputFilters[db]['group'] += 1
	#unionInputGroupSearch()
	
	
	def intersectInputGroupSearch(self, db, texts):
		"""
		Reduces the group filter by text search.

		Args:
			db (str): Database name.
			texts (list): Text data for group search.
		"""	
		# texts=[ (text,extra), ... ]
		if not self._inputFilters[db]['group']:
			return self.unionInputGroupSearch(db, texts)
		self.logPush("reducing %s group filter by text search ...\n" % (db,))
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'group')
		cursor.execute("UPDATE `%s`.`group` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`group` SET flag = 1 WHERE group_id = ?3" % db
		cursor.executemany(sql, self._loki.generateGroupIDsBySearch(texts))
		cursor.execute("DELETE FROM `%s`.`group` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d groups (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['group'] += 1
	#intersectInputGroupSearch()
	
	
	##################################################
	# source input
	
	
	def unionInputSources(self, db, names, errorCallback=None):
		"""
		Adds sources to the source filter.

		Args:
			db (str): Database name.
			names (list): Source names.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# names=[ name, ... ]
		self.logPush("adding to %s source filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'source')
		sql = "INSERT OR IGNORE INTO `%s`.`source` (label,source_id) VALUES (?1,?2)" % db
		n = numAdd = numNull = 0
		for source in names:
			n += 1
			sourceID = self._loki.getSourceID(source) or self.getUserSourceID(source)
			if sourceID:
				numAdd += 1
				cursor.execute(sql, (source,sourceID))
			else:
				numNull += 1
				if errorCallback:
					errorCallback(source, "invalid source at index %d" % (n,))
		if numNull:
			self.warn("WARNING: ignored %d unrecognized source identifier(s)\n" % numNull)
		self.logPop("... OK: added %d sources\n" % numAdd)
		
		self._inputFilters[db]['source'] += 1
	#unionInputSources()
	
	
	def intersectInputSources(self, db, names, errorCallback=None):
		"""
		Reduces the source filter.

		Args:
			db (str): Database name.
			names (list): Source names.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		# names=[ name, ... ]
		if not self._inputFilters[db]['source']:
			return self.unionInputSources(db, names, errorCallback)
		self.logPush("reducing %s source filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'source')
		cursor.execute("UPDATE `%s`.`source` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`source` SET flag = 1 WHERE source_id = ?1" % db
		for source in names:
			sourceID = self._loki.getSourceID(source) or self.getUserSourceID(source)
			if sourceID:
				cursor.execute(sql, (sourceID,))
		cursor.execute("DELETE FROM `%s`.`source` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d sources (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['source'] += 1
	#intersectInputSources()
	
	
	##################################################
	# user knowledge input
	
	
	def addUserSource(self, label, description, errorCallback=None):
		"""
		Adds a user-defined source.

		Args:
			label (str): Source label.
			description (str): Source description.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Returns:
			(int): User source ID.
		"""	
		self.log("adding user-defined source '%s' ..." % (label,))
		self._inputFilters['user']['source'] += 1
		usourceID = -self._inputFilters['user']['source']
		cursor = self._loki._db.cursor()
		cursor.execute("INSERT INTO `user`.`source` (source_id,source,description) VALUES (?,?,?)", (usourceID,label,description))
		self.log(" OK\n")
		return usourceID
	#addUserSource()
	
	
	def addUserGroup(self, usourceID, label, description, errorCallback=None):
		"""
		Adds a user-defined group.

		Args:
			usourceID (int): User source ID.
			label (str): Group label.
			description (str): Group description.
			errorCallback (function, optional): Error callback function. Defaults to None.

		Returns:
			(int): User group ID.
		"""	
		self.log("adding user-defined group '%s' ..." % (label,))
		self._inputFilters['user']['group'] += 1
		ugroupID = -self._inputFilters['user']['group']
		cursor = self._loki._db.cursor()
		cursor.execute("INSERT INTO `user`.`group` (group_id,label,description,source_id) VALUES (?,?,?,?)", (ugroupID,label,description,usourceID))
		self.log(" OK\n")
		return ugroupID
	#addUserGroup()
	
	
	def addUserGroupBiopolymers(self, ugroupID, namesets, errorCallback=None):
		"""
		Adds genes to a user-defined group.

		Args:
			ugroupID (int): User group ID.
			namesets (list): Gene names.
			errorCallback (function, optional): Error callback function. Defaults to None.
		"""	
		#TODO: apply ambiguity settings and heuristics?
		# namesets=[ [ (ns,name,extra), ...], ... ]
		self.logPush("adding genes to user-defined group ...\n")
		cursor = self._loki._db.cursor()
		
		sql = "INSERT OR IGNORE INTO `user`.`group_biopolymer` (group_id,biopolymer_id) VALUES (%d,?4)" % (ugroupID,)
		tally = dict()
		cursor.executemany(sql,
			self._loki.generateTypedBiopolymerIDsByIdentifiers(
				self.getOptionTypeID('gene'),
				itertools.chain(*namesets),
				minMatch=1,
				maxMatch=None,
				tally=tally,
				errorCallback=errorCallback
			)
		)
		if tally['zero']:
			self.warn("WARNING: ignored %d unrecognized gene identifier(s)\n" % tally['zero'])
		if tally['many']:
			self.warn("WARNING: added multiple results for %d ambiguous gene identifier(s)\n" % tally['many'])
		numAdd = sum(row[0] for row in cursor.execute("SELECT COUNT() FROM `user`.`group_biopolymer` WHERE group_id = ?", (ugroupID,)))
		
		self.logPop("... OK: added %d genes\n" % numAdd)
		self._inputFilters['user']['group_biopolymer'] += 1
	#addUserGroupBiopolymers()
	
	
	def applyUserKnowledgeFilter(self, grouplevel=False):
		"""
		Applies user-defined knowledge to the filter.

		Args:
			grouplevel (bool, optional): Whether to apply knowledge at the group level. Defaults to False.
		"""	
		cursor = self._loki._db.cursor()
		if grouplevel:
			self.logPush("applying user-defined knowledge to main group filter ...\n")
			assert(self._inputFilters['main']['group'] == 0) #TODO
			sql = """
INSERT INTO `main`.`group` (label,group_id,extra)
SELECT DISTINCT u_g.label, u_g.group_id, u_g.extra
FROM `user`.`group` AS u_g
UNION
SELECT DISTINCT d_g.label, d_g.group_id, NULL AS extra
FROM `user`.`group_biopolymer` AS u_gb
JOIN `db`.`group_biopolymer` AS d_gb
  ON d_gb.biopolymer_id = u_gb.biopolymer_id
JOIN `db`.`group` AS d_g
  ON d_g.group_id = d_gb.group_id
"""
			cursor.execute(sql)
			num = sum(row[0] for row in cursor.execute("SELECT COUNT() FROM `main`.`group`"))
			self.logPop("... OK: added %d groups\n" % (num,))
			self._inputFilters['main']['group'] += 1
		else:
			self.logPush("applying user-defined knowledge to main gene filter ...\n")
			assert(self._inputFilters['main']['gene'] == 0) #TODO
			sql = """
INSERT INTO `main`.`gene` (label,biopolymer_id,extra)
SELECT DISTINCT d_b.label, d_b.biopolymer_id, NULL AS extra
FROM `user`.`group_biopolymer` AS u_gb
JOIN `db`.`biopolymer` AS d_b
  ON d_b.biopolymer_id = u_gb.biopolymer_id
"""
			cursor.execute(sql)
			num = sum(row[0] for row in cursor.execute("SELECT COUNT() FROM `main`.`gene`"))
			self.logPop("... OK: added %d genes\n" % (num,))
			self._inputFilters['main']['gene'] += 1
		#if grouplevel
	#applyUserKnowledgeFilter()
	
	
	##################################################
	# user knowledge retrieval
	
	
	def getUserSourceID(self, source):
		"""
		Gets the user source ID.

		Args:
			source (str): Source name.

		Returns:
			(int): User source ID.
		"""	
		return self.getUserSourceIDs([source])[source]
	#getSourceID()
	
	
	def getUserSourceIDs(self, sources=None):
		"""
		Gets user source IDs.

		Args:
			sources (list, optional): Source names. Defaults to None.

		Returns:
			(dict): Dictionary containing source names as keys and their corresponding IDs as values.
		"""	
		cursor = self._loki._db.cursor()
		if sources:
			sql = "SELECT i.source, s.source_id FROM (SELECT ? AS source) AS i LEFT JOIN `user`.`source` AS s ON LOWER(s.source) = LOWER(i.source)"
			ret = { row[0]:row[1] for row in cursor.executemany(sql, itertools.izip(sources)) }
		else:
			sql = "SELECT source, source_id FROM `user`.`source`"
			ret = { row[0]:row[1] for row in cursor.execute(sql) }
		return ret
	#getSourceIDs()
	
	
	##################################################
	# PARIS
	
	
	def getPARISPermutationScore(self, featureData, featureBin, binFeatures, realFeatures, numPermutations, maxScore=0):
		"""
		Calculate the permutation score for a set of features based on observed and randomized data.

		Parameters:
			featureData (dict): Dictionary containing information about each feature. Keys are feature IDs,
								values are lists containing the size of the feature and whether it is significant.
			featureBin (dict): Dictionary mapping feature IDs to bin numbers.
			binFeatures (dict): Dictionary where keys are bin numbers and values are lists of feature IDs in that bin.
			realFeatures (set): Set containing the IDs of the real features.
			numPermutations (int): Number of permutations to perform.
			maxScore (int, optional): Maximum score to reach before stopping permutations. Defaults to 0.

		Returns:
			(int): Total permutation score.
		"""	
		realScore = sum(1 for f in realFeatures if (featureBin.get(f) and featureData[f][1]))
		if realScore < 1:
			return numPermutations
		
		#TODO: refinement?
		
		_sample = random.sample
		binDraws = collections.Counter(featureBin[f] for f in realFeatures if featureBin.get(f))
		totalScore = 0
		for p in range(numPermutations):
			permScore = 0
			for b,draws in binDraws.items():
				permScore += sum(1 for f in _sample(binFeatures[b], draws) if featureData[f][1])
			if permScore >= realScore:
				totalScore += 1
				if maxScore and (totalScore >= maxScore):
					break
		return totalScore
	#getPARISPermutationScore()
	
	
	def generatePARISResults(self, ucscBuildUser, ucscBuildDB):
		"""
		Orchestrates the PARIS (Pathway Analysis by Randomization Incorporating Structure) algorithm,
		performing various tasks such as preparing and analyzing data, mapping SNPs and positions to feature regions,
		generating results, and yielding the output.

		Parameters:
			ucscBuildUser (str): UCSC build version for user-defined data.
			ucscBuildDB (str): UCSC build version for the database.

		Yields:
			(tuple): Output data tuples containing information about groups, genes, features, and permutation scores.
		"""	
		self.logPush("running PARIS ...\n")
		cursor = self._loki._db.cursor()
		
		if not self._inputFilters['main']['region']:
			raise Exception("PARIS requires input feature regions")
		
		empty = list()
		threshold = self._options.paris_p_value
		rpMargin = self._options.region_position_margin
		optEnforceChm = (self._options.paris_enforce_input_chromosome == 'yes')
		optZeroPvals = self._options.paris_zero_p_values
		zoneSize = 100000 # in this context it doesn't have to match what the db uses
		self.prepareTableForUpdate('main','region')
		
		self.logPush("scanning feature regions ...\n")
		featureData = dict() # featureData[rowid] = (size,sig)
		featureBounds = dict() # featureBounds[rowid] = (rowid,chr,posMin,posMax)
		chrZoneFeatures = collections.defaultdict(lambda: collections.defaultdict(set))
		sql = "SELECT rowid,chr,posMin,posMax FROM `main`.`region`"
		for fid,chm,posMin,posMax in cursor.execute(sql):
			posMin -= rpMargin
			posMax += rpMargin
			featureData[fid] = [0,0]
			featureBounds[fid] = (fid,chm,posMin,posMax)
			for z in range( int(posMin / zoneSize), int(posMax / zoneSize) + 1 ):
				chrZoneFeatures[chm][z].add(fid)
		self.logPop("... OK: %d regions\n" % (len(featureData),))
		
		def analyzeLoci(generator):
			"""
			Analyzes loci data from the given generator, updating feature data and counts.

			Parameters:
				generator: A generator yielding tuples of chromosome, position, and extra data.

			Returns:
				(tuple): A tuple containing counts of matched loci, singletons, and ignored loci.
			"""	
			numMatch = numSingle = numIgnore = 0
			for chm,pos,extra in generator:
				extra = extra.split()
				
				if optEnforceChm:
					try:
						ichm = self._loki.chr_num[extra[0].strip()] #TODO optional ichm column position
						if ichm and (ichm != chm):
							continue
					except:
						continue
				#if enforce input chromosome
				
				try:
					pval = float(extra[1].strip()) #TODO optional pval column position
					if pval <= 0.0:
						if optZeroPvals == 'significant':
							sig = True
						elif optZeroPvals == 'insignificant':
							sig = False
						else:
							numIgnore += 1
							continue
					else:
						sig = (pval <= threshold) #TODO <= or < ?
				except:
					sig = False
				
				matched = False
				for f in chrZoneFeatures[chm][pos / zoneSize]:
					fid,fchm,fposMin,fposMax = featureBounds[f]
					if (chm == fchm) and (pos >= fposMin) and (pos <= fposMax):
						matched = True
						featureData[fid][0] += 1
						if sig:
							featureData[fid][1] += 1
				if matched:
					numMatch += 1
				else:
					numSingle += 1
					for row in cursor.execute("INSERT INTO `main`.`region` (label,chr,posMin,posMax) VALUES ('chr'|?1|':'|?2, ?1, ?2, ?2); SELECT LAST_INSERT_ROWID()", (chm,pos)):
						fid = row[0]
					posMin = pos - rpMargin
					posMax = pos + rpMargin
					featureData[fid] = [1,1] if sig else [1,0]
					featureBounds[fid] = (fid,chm,posMin,posMax)
					for z in range( int(posMin / zoneSize), int(posMax / zoneSize) + 1 ):
						chrZoneFeatures[chm][z].add(fid)
			#foreach position
			return (numMatch,numSingle,numIgnore)
		#analyzeLoci()
		
		if self._inputFilters['main']['snp']:
			self.logPush("mapping SNP results to feature regions ...\n")
			querySelect = ['position_chr','position_pos','snp_extra']
			queryFilter = {'main':{'snp':1}}
			query = self.buildQuery('filter', 'main', select=querySelect, fromFilter=queryFilter, joinFilter=queryFilter)
			numMatch,numSingle,numIgnore = analyzeLoci(self.generateQueryResults(query))
			self.logPop("... OK: %d in feature regions, %d singletons (%d ignored)\n" % (numMatch,numSingle,numIgnore))
		#if SNPs
		
		if self._inputFilters['main']['locus']:
			self.logPush("mapping position results to feature regions ...\n")
			querySelect = ['position_chr','position_pos','position_extra']
			queryFilter = {'main':{'locus':1}}
			query = self.buildQuery('filter', 'main', select=querySelect, fromFilter=queryFilter, joinFilter=queryFilter)
			numMatch,numSingle,numIgnore = analyzeLoci(self.generateQueryResults(query))
			self.logPop("... OK: %d in feature regions, %d singletons (%d ignored)\n" % (numMatch,numSingle,numIgnore))
		#if loci
		
		for snpFileList in (self._options.paris_snp_file or empty):
			self.logPush("reading SNP results ...\n")
			tallyRS = dict()
			tallyPos = dict()
			numMatch,numSingle,numIgnore = analyzeLoci(
				((chm,pos,posextra) for rs,posextra,chm,pos in self._loki.generateSNPLociByRSes(
					((rsnew,rsextra) for rsold,rsextra,rsnew in self._loki.generateCurrentRSesByRSes(
						self.generateRSesFromRSFiles(snpFileList),
						tally=tallyRS
					)),
					minMatch=1,
					maxMatch=(None if (self._options.allow_ambiguous_snps == 'yes') else 1),
					tally=tallyPos
				))
			)
			self.logPop("... OK: %d in feature regions, %d singletons (%d ignored, %d merged, %d unrecognized, %d ambiguous)\n" % (numMatch,numSingle,numIgnore,tallyRS['merge'],tallyPos['zero'],tallyPos['many']))
		#foreach paris_snp_file
		
		for positionFileList in (self._options.paris_position_file or empty):
			self.logPush("reading position results ...\n")
			numMatch,numSingle,numIgnore = analyzeLoci(
				((chm,pos,extra) for label,chm,pos,extra in self.generateLiftOverLoci(
					ucscBuildUser, ucscBuildDB,
					self.generateLociFromMapFiles(positionFileList, applyOffset=True)
				))
			)
			self.logPop("... OK: %d in feature regions, %d singletons (%d ignored)\n" % (numMatch,numSingle,numIgnore))
		#foreach paris_position_file
		
		featureBounds = chrZoneFeatures = None
		
		self.logPush("binning feature regions ...\n")
		# partition features by size
		sizeFeatures = collections.defaultdict(list)
		for fid,data in featureData.items():
			sizeFeatures[data[0]].append(fid)
		# randomize within each size while building a master list in descending size order
		listFeatures = list()
		for size in sorted(sizeFeatures.keys(), reverse=True):
			random.shuffle(sizeFeatures[size])
			listFeatures.extend(sizeFeatures[size])
		sizeFeatures = None
		# bin all features of size 0 and 1 with eachother (no bin size limit)
		featureBin = dict()
		binFeatures = collections.defaultdict(list)
		for b in (0,1):
			while listFeatures and (featureData[listFeatures[-1]][0] == b):
				fid = listFeatures.pop()
				assert(fid not in featureBin)
				featureBin[fid] = b
				binFeatures[b].append(fid)
		# distribute all remaining features into bins of equal size, close to the target size
		count = max(1, int(0.5 + float(len(listFeatures)) / self._options.paris_bin_size))
		size = len(listFeatures) / count
		extra = len(listFeatures) - (count * size)
		for b in range(2,2+count):
			for n in range(size + (1 if ((b-2) < extra) else 0)):
				fid = listFeatures.pop()
				assert(fid not in featureBin)
				featureBin[fid] = b
				binFeatures[b].append(fid)
		# report bin statistics
		for b in sorted(binFeatures):
			numSig = totalSize = 0
			minSize = maxSize = None
			for data in (featureData[f] for f in binFeatures[b]):
				numSig += (1 if data[1] else 0)
				minSize = min(minSize, data[0]) if (minSize != None) else data[0]
				maxSize = max(maxSize, data[0]) if (maxSize != None) else data[0]
				totalSize += data[0]
			self.log("bin #%d: %d features (%d significant), size %d..%d (avg %g)\n" % (
				b, len(binFeatures[b]), numSig, minSize, maxSize, float(totalSize) / len(binFeatures[b]),
			))
		self.logPop("... OK\n")
		
		# cull empty feature regions from the db, to speed up region matching later
		self.logPush("culling empty feature regions ...\n")
		sql = "DELETE FROM `main`.`region` WHERE rowid = ?"
		cursor.executemany(sql, itertools.izip(binFeatures[0]))
		self.logPop("... OK\n")
		
		self.logPush("mapping pathway genes ...\n")
		queryGroupSelect = ['group_id','group_label','group_description','gene_id','gene_label','gene_description']
		queryGroupFilter = {'main':{'group':self._inputFilters['main']['group'], 'source':self._inputFilters['main']['source']}}
		queryGroup = self.buildQuery('filter', 'main', select=queryGroupSelect, fromFilter=queryGroupFilter, joinFilter=queryGroupFilter)
		queryGroupU = None
		if self._inputFilters['user']['source']:
			queryGroupU = self.buildQuery('filter', 'main', select=queryGroupSelect, fromFilter=queryGroupFilter, joinFilter=queryGroupFilter, userKnowledge=True)
		groupData = dict()
		geneData = dict()
		for uid,ulabel,udesc,gid,glabel,gdesc in self.generateQueryResults(queryGroup, allowDupes=True, query2=queryGroupU):
			if uid not in groupData:
				groupData[uid] = [ulabel,udesc,set()]
			groupData[uid][2].add(gid)
			if gid not in geneData:
				geneData[gid] = [glabel,gdesc]
		#foreach group/gene pair
		self.logPop("... OK: %d pathways, %d genes\n" % (len(groupData),len(geneData)))
		
		self.logPush("mapping gene features ...\n")
		self.prepareTableForQuery('main','region')
		queryGeneSelect = ['region_id']
		queryGeneWhereCol = ('d_b','biopolymer_id')
		queryGeneWhere = dict()
	#	queryGeneWhere[('m_r','posMin')] = {'<= d_br.posMax'} #DEBUG paris 1.1.2
		queryGeneFilter = {'main':{'region_zone':1,'region':1}}
		n = 0
		for gid,gdata in geneData.items():
			features = set()
			queryGeneWhere[queryGeneWhereCol] = {'= %d' % (gid,)}
			queryGene = self.buildQuery('filter', 'main', select=queryGeneSelect, where=queryGeneWhere, fromFilter=queryGeneFilter, joinFilter=queryGeneFilter)
			for rid, in self.generateQueryResults(queryGene, allowDupes=True):
				features.add(rid)
			n += len(features)
			geneData[gid].append(frozenset(features))
			#foreach feature
		#foreach gene
		self.logPop("... OK: %d matched features\n" % (n,))
		
		self.logPush("mapping pathway features ...\n")
		n = 0
		for uid,udata in groupData.items():
			features = set() # TODO: allow duplicate features (build as list)
			for gid in udata[2]:
				features.update(geneData[gid][2])
			n += len(features)
			groupData[uid].append(frozenset(features))
		self.logPop("... OK: %d matched features\n" % (n,))
		
		# return the output generator
		self.logPop("... OK\n")
		
		genePvalCache = dict()
		def renderPermuPVal(realFeatures, geneID=None):
			"""
			Renders the permutation p-value for the given set of features.

			Parameters:
				realFeatures (set): A set of feature IDs.
				geneID (int, optional): The ID of the gene associated with the features.

			Returns:
				(str): The rendered permutation p-value.
			"""	
			ret = genePvalCache.get(geneID)
			if ret != None:
				return ret
			maxScore = None
			if self._options.paris_max_p_value != None:
				maxScore = int(self._options.paris_max_p_value * self._options.paris_permutation_count + 0.5)
			realScore = self.getPARISPermutationScore(featureData, featureBin, binFeatures, realFeatures, self._options.paris_permutation_count, maxScore)
			if realScore < 1:
				ret = '< %g' % (1.0 / self._options.paris_permutation_count,)
			else:
				ret = '%g' % (float(realScore) / self._options.paris_permutation_count,)
				if maxScore and (realScore >= maxScore):
					ret = '>= ' + ret
			if geneID:
				genePvalCache[geneID] = ret
			return ret
		#renderPermuPVal()
		
		yield (
			'id','group','description','genes','features','simple','(sig)','complex','(sig)','pval',
			('gene','features','simple','(sig)','complex','(sig)','pval')
		)
		for uid,udata in groupData.items():
			yield (
				uid,
				udata[0],
				udata[1],
				len(udata[2]),
				len(udata[3]),
				sum(1 for f in udata[3] if (featureData[f][0] == 1)),
				sum(1 for f in udata[3] if (featureData[f][1] and (featureData[f][0] == 1))),
				sum(1 for f in udata[3] if (featureData[f][0] > 1)),
				sum(1 for f in udata[3] if (featureData[f][1] and (featureData[f][0] > 1))),
				renderPermuPVal(udata[3]),
				( (
					geneData[gid][0],
					len(geneData[gid][2]),
					sum(1 for f in geneData[gid][2] if (featureData[f][0] == 1)),
					sum(1 for f in geneData[gid][2] if (featureData[f][1] and (featureData[f][0] == 1))),
					sum(1 for f in geneData[gid][2] if (featureData[f][0] > 1)),
					sum(1 for f in geneData[gid][2] if (featureData[f][1] and (featureData[f][0] > 1))),
					renderPermuPVal(geneData[gid][2], gid)
				) for gid in udata[2] )
			)
	#generatePARISResults()
	
	
	##################################################
	# internal query builder
	
	
	# define table aliases for each actual table: {alias:(db,table),...}
	_queryAliasTable = {
		'm_s'    : ('main','snp'),              # (label,rs)
		'm_l'    : ('main','locus'),            # (label,chr,pos)
		'm_r'    : ('main','region'),           # (label,chr,posMin,posMax)
		'm_rz'   : ('main','region_zone'),      # (region_rowid,chr,zone)
		'm_bg'   : ('main','gene'),             # (label,biopolymer_id)
		'm_g'    : ('main','group'),            # (label,group_id)
		'm_c'    : ('main','source'),           # (label,source_id)
		'a_s'    : ('alt','snp'),               # (label,rs)
		'a_l'    : ('alt','locus'),             # (label,chr,pos)
		'a_r'    : ('alt','region'),            # (label,chr,posMin,posMax)
		'a_rz'   : ('alt','region_zone'),       # (region_rowid,chr,zone)
		'a_bg'   : ('alt','gene'),              # (label,biopolymer_id)
		'a_g'    : ('alt','group'),             # (label,group_id)
		'a_c'    : ('alt','source'),            # (label,source_id)
		'c_mb_L' : ('cand','main_biopolymer'),  # (biopolymer_id)
		'c_mb_R' : ('cand','main_biopolymer'),  # (biopolymer_id)
		'c_ab_R' : ('cand','alt_biopolymer'),   # (biopolymer_id)
		'c_g'    : ('cand','group'),            # (group_id)
		'u_gb'   : ('user','group_biopolymer'), # (group_id,biopolymer_id)
		'u_gb_L' : ('user','group_biopolymer'), # (group_id,biopolymer_id)
		'u_gb_R' : ('user','group_biopolymer'), # (group_id,biopolymer_id)
		'u_g'    : ('user','group'),            # (group_id,source_id)
		'u_c'    : ('user','source'),           # (source_id)
		'd_sl'   : ('db','snp_locus'),          # (rs,chr,pos)
		'd_br'   : ('db','biopolymer_region'),  # (biopolymer_id,ldprofile_id,chr,posMin,posMax)
		'd_bz'   : ('db','biopolymer_zone'),    # (biopolymer_id,chr,zone)
		'd_b'    : ('db','biopolymer'),         # (biopolymer_id,type_id,label)
		'd_gb'   : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'd_gb_L' : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'd_gb_R' : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'd_g'    : ('db','group'),              # (group_id,type_id,label,source_id)
		'd_c'    : ('db','source'),             # (source_id,source)
		'd_w'    : ('db','gwas'),               # (rs,chr,pos)
	} #class._queryAliasTable{}
	
	
	# define constraints on single table aliases: dict{ set(a1,a2,...) : set(cond1,cond2,...), ... }
	_queryAliasConditions = {
		# TODO: find a way to put this back here without the covering index problem; hardcoded in buildQuery() for now
	#	frozenset({'d_sl'}) : frozenset({
	#		"({allowUSP} OR ({L}.validated > 0))",
	#	}),
		frozenset({'d_br'}) : frozenset({
			"{L}.ldprofile_id = {ldprofileID}",
		}),
		frozenset({'d_gb','d_gb_L','d_gb_R'}) : frozenset({
			"{L}.biopolymer_id != 0",
			"({L}.{gbColumn1} {gbCondition} OR {L}.{gbColumn2} {gbCondition})",
		}),
	} #class._queryAliasConditions{}
	
	
	# define constraints for allowable joins of pairs of table aliases:
	#   dict{ tuple(setL{a1,a2,...},setR{a3,a4,...}) : set{cond1,cond2,...} }
	# Note that the SQLite optimizer will not use an index on a column
	# which is modified by an expression, even if the condition could
	# be rewritten otherwise (i.e. "colA = colB + 10" will not use an
	# index on colB).  To account for this, all conditions which include
	# expressions must be duplicated so that each operand column appears
	# unmodified (i.e. "colA = colB + 10" and also "colA - 10 = colB").
	_queryAliasJoinConditions = {
		(frozenset({'m_s','a_s','d_sl'}),) : frozenset({
			"{L}.rs = {R}.rs",
		}),
		(frozenset({'m_s','a_s'}),frozenset({'d_w'})) : frozenset({
			"{L}.rs = {R}.rs",
		}),
		(frozenset({'d_sl'}),frozenset({'d_w'})) : frozenset({
			"(({L}.rs = {R}.rs) OR ({L}.chr = {R}.chr AND {L}.pos = {R}.pos))",
		}),
		(frozenset({'m_l','a_l','d_sl'}),) : frozenset({
			"{L}.chr = {R}.chr",
			"{L}.pos = {R}.pos",
		}),
		(frozenset({'m_l','a_l'}),frozenset({'d_w'})) : frozenset({
			"{L}.chr = {R}.chr",
			"{L}.pos = {R}.pos",
		}),
		(frozenset({'m_l','a_l','d_sl'}),frozenset({'m_rz','a_rz','d_bz'})) : frozenset({
			"{L}.chr = {R}.chr",
			"{L}.pos >= (({R}.zone * {zoneSize}) - {rpMargin})",
			"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rpMargin})",
			"(({L}.pos + {rpMargin}) / {zoneSize}) >= {R}.zone",
			"(({L}.pos - {rpMargin}) / {zoneSize}) <= {R}.zone",
		}),
		(frozenset({'m_rz'}),frozenset({'m_r'})) : frozenset({
			"{L}.region_rowid = {R}.rowid",
			# with the rowid match, these should all be guaranteed by self.updateRegionZones()
			#"{L}.chr = {R}.chr",
			#"(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
			#"({L}.zone * {zoneSize}) <= {R}.posMax",
			#"{L}.zone >= ({R}.posMin / {zoneSize})",
			#"{L}.zone <= ({R}.posMax / {zoneSize})",
		}),
		(frozenset({'a_rz'}),frozenset({'a_r'})) : frozenset({
			"{L}.region_rowid = {R}.rowid",
			# with the rowid match, these should all be guaranteed by self.updateRegionZones()
			#"{L}.chr = {R}.chr",
			#"(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
			#"({L}.zone * {zoneSize}) <= {R}.posMax",
			#"{L}.zone >= ({R}.posMin / {zoneSize})",
			#"{L}.zone <= ({R}.posMax / {zoneSize})",
		}),
		(frozenset({'d_bz'}),frozenset({'d_br'})) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
			"{L}.chr = {R}.chr",
			# verify the zone/region coverage match in case there are two regions on the same chromosome
			"(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
			"({L}.zone * {zoneSize}) <= {R}.posMax",
			"{L}.zone >= ({R}.posMin / {zoneSize})",
			"{L}.zone <= ({R}.posMax / {zoneSize})",
		}),
		(frozenset({'m_rz','a_rz','d_bz'}),) : frozenset({
			"{L}.chr = {R}.chr",
			"{L}.zone >= ({R}.zone + (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",
			"{L}.zone <= ({R}.zone - (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",
			"{R}.zone >= ({L}.zone + (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",
			"{R}.zone <= ({L}.zone - (MIN(0,{rmBases}) - {zoneSize}) / {zoneSize})",
		}),
		(frozenset({'m_bg','a_bg','d_br','d_b'}),) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'m_bg','a_bg','d_b'}),frozenset({'u_gb','d_gb'})) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'d_gb_L','d_gb_R'}),) : frozenset({
			"{L}.biopolymer_id != {R}.biopolymer_id",
		}),
		(frozenset({'u_gb_L','u_gb_R'}),) : frozenset({
			"{L}.biopolymer_id != {R}.biopolymer_id",
		}),
		(frozenset({'m_g','a_g','d_gb','d_g'}),) : frozenset({
			"{L}.group_id = {R}.group_id",
		}),
		(frozenset({'m_g','a_g','u_gb','u_g'}),) : frozenset({
			"{L}.group_id = {R}.group_id",
		}),
		(frozenset({'m_c','a_c','d_g','d_c'}),) : frozenset({
			"{L}.source_id = {R}.source_id",
		}),
		(frozenset({'m_c','a_c','u_g','u_c'}),) : frozenset({
			"{L}.source_id = {R}.source_id",
		}),
		
		(frozenset({'c_mb_L'}),frozenset({'u_gb_L','d_gb_L'})) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'c_mb_R','c_ab_R'}),frozenset({'u_gb_R','d_gb_R'})) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'c_g','d_g'}),frozenset({'d_gb','d_gb_L','d_gb_R','d_g'})) : frozenset({
			"{L}.group_id = {R}.group_id",
		}),
		(frozenset({'c_g','u_g'}),frozenset({'u_gb','u_gb_L','u_gb_R','u_g'})) : frozenset({
			"{L}.group_id = {R}.group_id",
		}),
	} #class._queryAliasJoinConditions{}
	
	
	# define constraints on pairs of table aliases which may not necessarily be directly joined;
	# these conditions are added to either the WHERE or the LEFT JOIN...ON clause depending on where the aliases appear
	_queryAliasPairConditions = {
		(frozenset({'m_l','a_l','d_sl'}),frozenset({'m_r','a_r','d_br'})) : frozenset({
			"{L}.chr = {R}.chr",
			"{L}.pos >= ({R}.posMin - {rpMargin})",
			"{L}.pos <= ({R}.posMax + {rpMargin})",
			"({L}.pos + {rpMargin}) >= {R}.posMin",
			"({L}.pos - {rpMargin}) <= {R}.posMax",
		}),
		(frozenset({'m_r','a_r','d_br'}),) : frozenset({
			"{L}.chr = {R}.chr",
			"({L}.posMax - {L}.posMin + 1) >= {rmBases}",
			"({R}.posMax - {R}.posMin + 1) >= {rmBases}",
			"(" +
				"(" +
					"({L}.posMin >= {R}.posMin) AND " +
					"({L}.posMin <= {R}.posMax + 1 - MAX({rmBases}, COALESCE((MIN({L}.posMax - {L}.posMin, {R}.posMax - {R}.posMin) + 1) * {rmPercent} / 100.0, {rmBases})))" +
				") OR (" +
					"({R}.posMin >= {L}.posMin) AND " +
					"({R}.posMin <= {L}.posMax + 1 - MAX({rmBases}, COALESCE((MIN({L}.posMax - {L}.posMin, {R}.posMax - {R}.posMin) + 1) * {rmPercent} / 100.0, {rmBases})))" +
				")" +
			")",
		}),
	} #class._queryAliasPairConditions{}
	
	
	# define available data columns and the table aliases that can provide them,
	# in order of preference:
	# dict{ col : list[ tuple(alias,rowid,expression,?conditions),... ], ... }
	#   alias = source alias string
	#   rowid = source table column which identifies unique results
	#     "{alias}.{rowid}" must be a valid expression
	#   expression = full SQL expression for the column (should reference only the appropriate alias)
	#   conditions = optional set of additional conditions
	_queryColumnSources = {
		'snp_id' : [
			('a_s',  'rowid', "a_s.rs"),
			('m_s',  'rowid', "m_s.rs"),
			('d_sl', '_ROWID_', "d_sl.rs"),
		],
		'snp_label' : [
			('a_s',  'rowid', "a_s.label"),
			('m_s',  'rowid', "m_s.label"),
			('d_sl', '_ROWID_', "'rs'||d_sl.rs"),
		],
		'snp_extra' : [
			('a_s',  'rowid', "a_s.extra"),
			('m_s',  'rowid', "m_s.extra"),
			('d_sl', '_ROWID_', "NULL"),
		],
		'snp_flag' : [
			('a_s',  'rowid', "a_s.flag"),
			('m_s',  'rowid', "m_s.flag"),
			('d_sl', '_ROWID_', "NULL"),
		],
		
		'position_id' : [
			('a_l',  'rowid',   "a_l.rowid"),
			('m_l',  'rowid',   "m_l.rowid"),
			('d_sl', '_ROWID_', "d_sl._ROWID_"),
		],
		'position_label' : [
			('a_l',  'rowid',   "a_l.label"),
			('m_l',  'rowid',   "m_l.label"),
			('d_sl', '_ROWID_', "'rs'||d_sl.rs"),
		],
		'position_chr' : [ #TODO: find a way to avoid repeating the conversions already in loki_db.chr_name
			('a_l',  'rowid',   "(CASE a_l.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE a_l.chr END)"),
			('m_l',  'rowid',   "(CASE m_l.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE m_l.chr END)"),
			('d_sl', '_ROWID_', "(CASE d_sl.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE d_sl.chr END)"),
		],
		'position_pos' : [
			('a_l',  'rowid',   "a_l.pos {pMinOffset}"),
			('m_l',  'rowid',   "m_l.pos {pMinOffset}"),
			('d_sl', '_ROWID_', "d_sl.pos {pMinOffset}"),
		],
		'position_extra' : [
			('a_l',  'rowid',   "a_l.extra"),
			('m_l',  'rowid',   "m_l.extra"),
			('d_sl', '_ROWID_', "NULL"),
		],
		'position_flag' : [
			('a_l',  'rowid',   "a_l.flag"),
			('m_l',  'rowid',   "m_l.flag"),
			('d_sl', '_ROWID_', "NULL"),
		],
		
		'region_id' : [
			('a_r',  'rowid',   "a_r.rowid"),
			('m_r',  'rowid',   "m_r.rowid"),
			('d_br', '_ROWID_', "d_br._ROWID_"),
		],
		'region_label' : [
			('a_r',  'rowid',         "a_r.label"),
			('m_r',  'rowid',         "m_r.label"),
			('d_b',  'biopolymer_id', "d_b.label"),
		],
		'region_chr' : [ #TODO: find a way to avoid repeating the conversions already in loki_db.chr_name
			('a_r',  'rowid',   "(CASE a_r.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE a_r.chr END)"),
			('m_r',  'rowid',   "(CASE m_r.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE m_r.chr END)"),
			('d_br', '_ROWID_', "(CASE d_br.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE d_br.chr END)"),
		],
		'region_zone' : [
			('a_rz', 'zone', "a_rz.zone"),
			('m_rz', 'zone', "m_rz.zone"),
			('d_bz', 'zone', "d_bz.zone"),
		],
		'region_start' : [
			('a_r',  'rowid',   "a_r.posMin {pMinOffset}"),
			('m_r',  'rowid',   "m_r.posMin {pMinOffset}"),
			('d_br', '_ROWID_', "d_br.posMin {pMinOffset}"),
		],
		'region_stop' : [
			('a_r',  'rowid',   "a_r.posMax {pMaxOffset}"),
			('m_r',  'rowid',   "m_r.posMax {pMaxOffset}"),
			('d_br', '_ROWID_', "d_br.posMax {pMaxOffset}"),
		],
		'region_extra' : [
			('a_r',  'rowid',   "a_r.extra"),
			('m_r',  'rowid',   "m_r.extra"),
			('d_br', '_ROWID_', "NULL"),
		],
		'region_flag' : [
			('a_r',  'rowid',   "a_r.flag"),
			('m_r',  'rowid',   "m_r.flag"),
			('d_br', '_ROWID_', "NULL"),
		],
		
		'biopolymer_id' : [
			('a_bg',   'biopolymer_id', "a_bg.biopolymer_id"),
			('m_bg',   'biopolymer_id', "m_bg.biopolymer_id"),
			('c_mb_L', 'biopolymer_id', "c_mb_L.biopolymer_id"),
			('c_mb_R', 'biopolymer_id', "c_mb_R.biopolymer_id"),
			('c_ab_R', 'biopolymer_id', "c_ab_R.biopolymer_id"),
			('u_gb',   'biopolymer_id', "u_gb.biopolymer_id"),
			('d_br',   'biopolymer_id', "d_br.biopolymer_id"),
			('d_gb',   'biopolymer_id', "d_gb.biopolymer_id"),
			('d_gb_L', 'biopolymer_id', "d_gb_L.biopolymer_id"),
			('d_gb_R', 'biopolymer_id', "d_gb_R.biopolymer_id"),
			('d_b',    'biopolymer_id', "d_b.biopolymer_id"),
		],
		'biopolymer_id_L' : [
			('c_mb_L', 'biopolymer_id', "c_mb_L.biopolymer_id"),
			('u_gb_L', 'biopolymer_id', "u_gb_L.biopolymer_id"),
			('d_gb_L', 'biopolymer_id', "d_gb_L.biopolymer_id"),
			('d_b',    'biopolymer_id', "d_b.biopolymer_id"),
		],
		'biopolymer_id_R' : [
			('c_mb_R', 'biopolymer_id', "c_mb_R.biopolymer_id"),
			('c_ab_R', 'biopolymer_id', "c_ab_R.biopolymer_id"),
			('u_gb_R', 'biopolymer_id', "d_gb_R.biopolymer_id"),
			('d_gb_R', 'biopolymer_id', "d_gb_R.biopolymer_id"),
			('d_b',    'biopolymer_id', "d_b.biopolymer_id"),
		],
		'biopolymer_label' : [
			('a_bg', 'biopolymer_id', "a_bg.label"),
			('m_bg', 'biopolymer_id', "m_bg.label"),
			('d_b',  'biopolymer_id', "d_b.label"),
		],
		'biopolymer_description' : [
			('d_b',  'biopolymer_id', "d_b.description"),
		],
		'biopolymer_identifiers' : [
			('a_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = a_bg.biopolymer_id)"),
			('m_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = m_bg.biopolymer_id)"),
			('d_b',  'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = d_b.biopolymer_id)"),
		],
		'biopolymer_chr' : [ #TODO: find a way to avoid repeating the conversions already in loki_db.chr_name
			('d_br', '_ROWID_', "(CASE d_br.chr WHEN 23 THEN 'X' WHEN 24 THEN 'Y' WHEN 25 THEN 'XY' WHEN 26 THEN 'MT' ELSE d_br.chr END)"),
		],
		'biopolymer_zone' : [
			('d_bz', 'zone', "d_bz.zone"),
		],
		'biopolymer_start' : [
			('d_br', '_ROWID_', "d_br.posMin {pMinOffset}"),
		],
		'biopolymer_stop' : [
			('d_br', '_ROWID_', "d_br.posMax {pMaxOffset}"),
		],
		'biopolymer_extra' : [
			('a_bg', 'biopolymer_id', "a_bg.extra"),
			('m_bg', 'biopolymer_id', "m_bg.extra"),
			('d_b',  'biopolymer_id', "NULL"),
		],
		'biopolymer_flag' : [
			('a_bg', 'biopolymer_id', "a_bg.flag"),
			('m_bg', 'biopolymer_id', "m_bg.flag"),
			('d_b',  'biopolymer_id', "NULL"),
		],
		
		'gene_id' : [
			('a_bg', 'biopolymer_id', "a_bg.biopolymer_id"),
			('m_bg', 'biopolymer_id', "m_bg.biopolymer_id"),
			('d_b',  'biopolymer_id', "d_b.biopolymer_id", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		'gene_label' : [
			('a_bg', 'biopolymer_id', "a_bg.label"),
			('m_bg', 'biopolymer_id', "m_bg.label"),
			('d_b',  'biopolymer_id', "d_b.label", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		'gene_description' : [
			('d_b',  'biopolymer_id', "d_b.description", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		'gene_identifiers' : [
			('a_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = a_bg.biopolymer_id)"),
			('m_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = m_bg.biopolymer_id)"),
			('d_b',  'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = d_b.biopolymer_id)", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		'gene_symbols' : [
			('a_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(name,'|') FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = a_bg.biopolymer_id AND d_bn.namespace_id = {namespaceID_symbol})"),
			('m_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(name,'|') FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = m_bg.biopolymer_id AND d_bn.namespace_id = {namespaceID_symbol})"),
			('d_b',  'biopolymer_id', "(SELECT GROUP_CONCAT(name,'|') FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = d_b.biopolymer_id  AND d_bn.namespace_id = {namespaceID_symbol})", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		'gene_extra' : [
			('a_bg', 'biopolymer_id', "a_bg.extra"),
			('m_bg', 'biopolymer_id', "m_bg.extra"),
			('d_b',  'biopolymer_id', "NULL", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		'gene_flag' : [
			('a_bg', 'biopolymer_id', "a_bg.flag"),
			('m_bg', 'biopolymer_id', "m_bg.flag"),
			('d_b',  'biopolymer_id', "NULL", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		
		'upstream_id' : [
			('a_l',  'rowid',   "(SELECT d_b.biopolymer_id         FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_b.biopolymer_id         FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_b.biopolymer_id         FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
		],
		'upstream_label' : [
			('a_l',  'rowid',   "(SELECT d_b.label                 FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_b.label                 FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_b.label                 FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
		],
		'upstream_distance' : [
			('a_l',  'rowid',   "a_l.pos -(SELECT MAX(d_br.posMax) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin})"),
			('m_l',  'rowid',   "m_l.pos -(SELECT MAX(d_br.posMax) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin})"),
			('d_sl', '_ROWID_', "d_sl.pos-(SELECT MAX(d_br.posMax) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin})"),
		],
		'upstream_start' : [
			('a_l',  'rowid',   "(SELECT d_br.posMin {pMinOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_br.posMin {pMinOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_br.posMin {pMinOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
		],
		'upstream_stop' : [
			('a_l',  'rowid',   "(SELECT d_br.posMax {pMaxOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMax < a_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_br.posMax {pMaxOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMax < m_l.pos  - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_br.posMax {pMaxOffset}  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMax < d_sl.pos - {rpMargin} ORDER BY d_br.posMax DESC LIMIT 1)"),
		],
		
		'downstream_id' : [
			('a_l',  'rowid',   "(SELECT d_b.biopolymer_id          FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_b.biopolymer_id          FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_b.biopolymer_id          FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
		],
		'downstream_label' : [
			('a_l',  'rowid',   "(SELECT d_b.label                  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_b.label                  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_b.label                  FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
		],
		'downstream_distance' : [
			('a_l',  'rowid',   "-a_l.pos +(SELECT MIN(d_br.posMin) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin})"),
			('m_l',  'rowid',   "-m_l.pos +(SELECT MIN(d_br.posMin) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin})"),
			('d_sl', '_ROWID_', "-d_sl.pos+(SELECT MIN(d_br.posMin) FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin})"),
		],
		'downstream_start' : [
			('a_l',  'rowid',   "(SELECT d_br.posMin {pMinOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_br.posMin {pMinOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_br.posMin {pMinOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
		],
		'downstream_stop' : [
			('a_l',  'rowid',   "(SELECT d_br.posMax {pMaxOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = a_l.chr  AND d_br.posMin > a_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('m_l',  'rowid',   "(SELECT d_br.posMax {pMaxOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = m_l.chr  AND d_br.posMin > m_l.pos  + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
			('d_sl', '_ROWID_', "(SELECT d_br.posMax {pMaxOffset}   FROM `db`.`biopolymer` AS d_b JOIN `db`.`biopolymer_region` AS d_br USING (biopolymer_id) WHERE d_b.type_id+0 = {typeID_gene} AND d_br.ldprofile_id = {ldprofileID} AND d_br.chr = d_sl.chr AND d_br.posMin > d_sl.pos + {rpMargin} ORDER BY d_br.posMin LIMIT 1)"),
		],
		
		'group_id' : [
			('a_g',    'group_id', "a_g.group_id"),
			('m_g',    'group_id', "m_g.group_id"),
			('c_g',    'group_id', "c_g.group_id"),
			('u_gb',   'group_id', "u_gb.group_id"),
			('u_gb_L', 'group_id', "u_gb_L.group_id"),
			('u_gb_R', 'group_id', "u_gb_R.group_id"),
			('u_g',    'group_id', "u_g.group_id"),
			('d_gb',   'group_id', "d_gb.group_id"),
			('d_gb_L', 'group_id', "d_gb_L.group_id"),
			('d_gb_R', 'group_id', "d_gb_R.group_id"),
			('d_g',    'group_id', "d_g.group_id"),
		],
		'group_label' : [
			('a_g', 'group_id', "a_g.label"),
			('m_g', 'group_id', "m_g.label"),
			('u_g', 'group_id', "u_g.label"),
			('d_g', 'group_id', "d_g.label"),
		],
		'group_description' : [
			('u_g', 'group_id', "u_g.description"),
			('d_g', 'group_id', "d_g.description"),
		],
		'group_identifiers' : [
			('a_g', 'group_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = a_g.group_id)"),
			('m_g', 'group_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = m_g.group_id)"),
			('u_g', 'group_id', "u_g.label"),
			('d_g', 'group_id', "(SELECT GROUP_CONCAT(namespace||':'||name,'|') FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = d_g.group_id)"),
		],
		'group_extra' : [
			('a_g', 'group_id', "a_g.extra"),
			('m_g', 'group_id', "m_g.extra"),
			('u_g', 'group_id', "NULL"),
			('d_g', 'group_id', "NULL"),
		],
		'group_flag' : [
			('a_g', 'group_id', "a_g.flag"),
			('m_g', 'group_id', "m_g.flag"),
			('u_g', 'group_id', "NULL"),
			('d_g', 'group_id', "NULL"),
		],
		
		'source_id' : [
			('a_c', 'source_id', "a_c.source_id"),
			('m_c', 'source_id', "m_c.source_id"),
			('u_g', 'source_id', "u_g.source_id"),
			('u_c', 'source_id', "u_c.source_id"),
			('d_g', 'source_id', "d_g.source_id"),
			('d_c', 'source_id', "d_c.source_id"),
		],
		'source_label' : [
			('a_c', 'source_id', "a_c.label"),
			('m_c', 'source_id', "m_c.label"),
			('u_c', 'source_id', "u_c.source"),
			('d_c', 'source_id', "d_c.source"),
		],
		
		'gwas_rs' : [
			('d_w', '_ROWID_', "d_w.rs"),
		],
		'gwas_chr' : [
			('d_w', '_ROWID_', "d_w.chr"),
		],
		'gwas_pos' : [
			('d_w', '_ROWID_', "d_w.pos {pMinOffset}"),
		],
		'gwas_trait' : [
			('d_w', '_ROWID_', "d_w.trait"),
		],
		'gwas_snps' : [
			('d_w', '_ROWID_', "d_w.snps"),
		],
		'gwas_orbeta' : [
			('d_w', '_ROWID_', "d_w.orbeta"),
		],
		'gwas_allele95ci' : [
			('d_w', '_ROWID_', "d_w.allele95ci"),
		],
		'gwas_riskAfreq' : [
			('d_w', '_ROWID_', "d_w.riskAfreq"),
		],
		'gwas_pubmed' : [
			('d_w', '_ROWID_', "d_w.pubmed_id"),
		],
		'disease_label' : [
			('a_g', 'group_id', "(SELECT name FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = a_g.group_id AND d_n.namespace = 'disease')"),
			('m_g', 'group_id', "(SELECT name FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = m_g.group_id AND d_n.namespace = 'disease')"),
			('d_g', 'group_id', "(SELECT name FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = d_g.group_id AND d_n.namespace = 'disease')"),
		],
		'disease_category' : [
			('a_g', 'group_id', "(SELECT subtype FROM `db`.`subtype` AS d_s JOIN `db`.`group` AS dg USING (subtype_id) JOIN `db`.`type` AS d_t USING (type_id) WHERE dg.group_id = a_g.group_id AND d_t.type = 'disease')"),
			('m_g', 'group_id', "(SELECT subtype FROM `db`.`subtype` AS d_s JOIN `db`.`group` AS dg USING (subtype_id) JOIN `db`.`type` AS d_t USING (type_id) WHERE dg.group_id = m_g.group_id AND d_t.type = 'disease')"),
			('d_g', 'group_id', "(SELECT subtype FROM `db`.`subtype` AS d_s JOIN `db`.`group` AS dg USING (subtype_id) JOIN `db`.`type` AS d_t USING (type_id) WHERE dg.group_id = d_g.group_id AND d_t.type = 'disease')"),
		]
	} #class._queryColumnSources
	
	
	def getQueryTemplate(self):
		"""
		Returns a template for constructing a SQL query.

		Returns:
			(dict): A dictionary representing the query template with placeholders for different parts of the SQL query.
		"""	
		return {
			'_columns'  : list(), # [ colA, colB, ... ]
			'SELECT'    : collections.OrderedDict(), # { colA:expA, colB:expB, ... }
			#                                              => SELECT expA AS colA, expB AS colB, ...
			'_rowid'    : collections.OrderedDict(), # OD{ tblA:{colA1,colA2,...}, ... }
			#                                              => SELECT ... (tblA.colA1||'_'||tblA.colA2...) AS rowid
			'FROM'      : set(),  # { tblA, tblB, ... }    => FROM aliasTable[tblA] AS tblA, aliasTable[tblB] AS tblB, ...
			'LEFT JOIN' : collections.OrderedDict(), # OD{ tblA:{expA1,expA2,...}, ... }
			#                                              => LEFT JOIN aliasTable[tblA] ON expA1 AND expA2 ...
			'WHERE'     : set(),  # { expA, expB, ... }    => WHERE expA AND expB AND ...
			'GROUP BY'  : list(), # [ expA, expB, ... ]    => GROUP BY expA, expB, ...
			'HAVING'    : set(),  # { expA, expB, ... }    => HAVING expA AND expB AND ...
			'ORDER BY'  : list(), # [ expA, expB, ... ]    => ORDER BY expA, expB, ...
			'LIMIT'     : None    # num                    => LIMIT INT(num)
		}
	#getQueryTemplate()
	
	
	def buildQuery(self, mode, focus, select, having=None, where=None, applyOffset=False, fromFilter=None, joinFilter=None, userKnowledge=False):
		"""
		Builds a SQL query based on the provided parameters.

		Parameters:
			mode (str): The mode of the query ('filter', 'annotate', 'modelgene', 'modelgroup', 'model').
			focus (str): The focus of the query.
			select (list): A list of columns to be selected.
			having (dict, optional): A dictionary containing columns and their conditions for filtering after grouping.
			where (dict, optional): A dictionary containing table alias and column pairs along with their conditions for filtering before grouping.
			applyOffset (bool, optional): Whether to apply an offset to the query.
			fromFilter (dict, optional): A dictionary specifying table filters for the FROM clause.
			joinFilter (dict, optional): A dictionary specifying table filters for the JOIN clause.
			userKnowledge (bool, optional): Whether user knowledge is considered in the query.

		Returns:
			(dict): A dictionary representing the constructed SQL query.
		"""	
		assert(mode in ('filter','annotate','modelgene','modelgroup','model'))
		assert(focus in self._schema)
		# select=[ column, ... ]
		# having={ column:{'= val',...}, ... }
		# where={ (alias,column):{'= val',...}, ... }
		# fromFilter={ db:{table:bool, ...}, ... }
		# joinFilter={ db:{table:bool, ...}, ... }
		if self._options.debug_logic:
			self.warnPush("buildQuery(mode=%s, focus=%s, select=%s, having=%s, where=%s)\n" % (mode,focus,select,having,where))
		having = having or dict()
		where = where or dict()
		if fromFilter == None:
			fromFilter = { db:{ tbl:bool(flag) for tbl,flag in self._inputFilters[db].items() } for db in ('main','alt','cand') }
		if joinFilter == None:
			joinFilter = { db:{ tbl:bool(flag) for tbl,flag in self._inputFilters[db].items() } for db in ('main','alt','cand') }
		knowFilter = { 'db':{ tbl:True for db,tbl in iter(self._queryAliasTable.values()) if (db == 'db') } }
		if userKnowledge:
			knowFilter['user'] = dict()
			for db,tbl in self._queryAliasTable.itervalues():
				if (db == 'user') and knowFilter['db'].get(tbl):
					knowFilter['db'][tbl] = False
					knowFilter['user'][tbl] = True
		query = self.getQueryTemplate()
		empty = dict()
		
		# generate table alias join adjacency map
		# (usually this is the entire table join graph, minus nodes that
		# represent empty user input tables, since joining through them would
		# yield zero results by default)
		aliasAdjacent = collections.defaultdict(set)
		for aliasPairs in self._queryAliasJoinConditions:
			for aliasLeft in aliasPairs[0]:
				for aliasRight in aliasPairs[-1]:
					if aliasLeft != aliasRight:
						dbLeft,tblLeft = self._queryAliasTable[aliasLeft]
						dbRight,tblRight = self._queryAliasTable[aliasRight]
						tblLeft = 'region' if (tblLeft == 'region_zone') else tblLeft
						tblRight = 'region' if (tblRight == 'region_zone') else tblRight
						if knowFilter.get(dbLeft,empty).get(tblLeft) or joinFilter.get(dbLeft,empty).get(tblLeft):
							if knowFilter.get(dbRight,empty).get(tblRight) or joinFilter.get(dbRight,empty).get(tblRight):
								aliasAdjacent[aliasLeft].add(aliasRight)
								aliasAdjacent[aliasRight].add(aliasLeft)
							#if aliasRight passes knowledge or join filter
						#if aliasLeft passes knowledge or join filter
					#if aliases differ
				#foreach aliasRight
			#foreach aliasLeft
		#foreach _queryAliasJoinConditions
		
		# debug
		if self._options.debug_logic:
			self.warn("aliasAdjacent = \n")
			for alias in sorted(aliasAdjacent):
				self.warn("  %s : %s\n" % (alias,sorted(aliasAdjacent[alias])))
		
		# generate column availability map
		# _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]
		columnAliases = collections.defaultdict(list)
		aliasColumns = collections.defaultdict(set)
		for col in itertools.chain(select,having):
			if col not in self._queryColumnSources:
				raise Exception("internal query with unsupported column '{0}'".format(col))
			if col not in columnAliases:
				for source in self._queryColumnSources[col]:
					if source[0] in aliasAdjacent:
						columnAliases[col].append(source[0])
						aliasColumns[source[0]].add(col)
		if not (columnAliases and aliasColumns):
			raise Exception("internal query with no outputs or conditions")
		
		# debug
		if self._options.debug_logic:
			self.warn("columnAliases = %s\n" % columnAliases)
			self.warn("aliasColumns = %s\n" % aliasColumns)
		
		# establish select column order
		for col in select:
			query['_columns'].append(col)
			query['SELECT'][col] = None
		
		# identify the primary table aliases to query
		# (usually this is all of the user input tables which contain some
		# data, and which match the main/alt focus of this query; since user
		# input represents filters, we always need to join through the tables
		# with that data, even if we're not selecting any of their columns)
		query['FROM'].update(alias for alias,col in where)
		for alias,dbtable in self._queryAliasTable.items():
			db,table = dbtable
			# only include tables which satisfy the filter (usually, user input tables which contain some data)
			if not fromFilter.get(db,empty).get('region' if (table == 'region_zone') else table):
				continue
			# only include tables from the focus db (except an alt focus sometimes also includes main)
			if not ((db == focus) or (db == 'main' and focus == 'alt' and mode != 'annotate' and self._options.alternate_model_filtering != 'yes')):
				continue
			# only include tables on one end of the chain when finding candidates for modeling
			if (mode == 'modelgene') and (table in ('group','source')):
				continue
			if (mode == 'modelgroup') and (table not in ('group','source')):
				continue
			# only re-use the main gene candidates on the right if necessary
			if (alias == 'c_mb_R') and ((self._options.alternate_model_filtering == 'yes') or fromFilter.get('cand',empty).get('alt_biopolymer')):
				continue
			# otherwise, add it
			query['FROM'].add(alias)
		#foreach table alias
		
		# if we have no starting point yet, start from the last-resort source for a random output or condition column
		if not query['FROM']:
			col = next(itertools.chain(select,having))
			for source in self._queryColumnSources[col]:
				db,tbl = self._queryAliasTable[source[0]]
				if knowFilter.get(db,empty).get(tbl):
					alias = source[0]
			query['FROM'].add(alias)
		
		# debug
		if self._options.debug_logic:
			self.warn("starting FROM = %s\n" % ', '.join(query['FROM']))
		
		# add any table aliases necessary to join the currently included tables
		if len(query['FROM']) > 1:
			remaining = query['FROM'].copy()
			inside = {remaining.pop()}
			outside = set(aliasAdjacent) - inside
			queue = collections.deque()
			queue.append( (inside,outside,remaining) )
			while queue:
				inside,outside,remaining = queue.popleft()
				if self._options.debug_logic:
					self.warn("inside: %s\n" % ', '.join(inside))
					self.warn("outside: %s\n" % ', '.join(outside))
					self.warn("remaining: %s\n" % ', '.join(remaining))
				if not remaining:
					break
				queue.extend( (inside|{a},outside-{a},remaining-{a}) for a in outside if inside & aliasAdjacent[a] )
			if remaining:
				raise Exception("could not find a join path for starting tables: %s" % query['FROM'])
			query['FROM'] |= inside
		#if tables need joining
		
		# debug
		if self._options.debug_logic:
			self.warn("joined FROM = %s\n" % ', '.join(query['FROM']))
		
		# add table aliases to satisfy any remaining columns
		columnsRemaining = set(col for col,aliases in columnAliases.items() if not (set(aliases) & query['FROM']))
		if mode == 'annotate':
			# when annotating, do a BFS from each remaining column in order of source preference
			# this will guarantee a valid path of LEFT JOINs to the most-preferred available source
			while columnsRemaining:
				target = next( col for col in itertools.chain(select,having) if (col in columnsRemaining) )
				if self._options.debug_logic:
					self.warn("target column = %s\n" % target)
				if not columnAliases[target]:
					raise Exception("could not find source table for output column %s" % (target,))
				alias = columnAliases[target][0]
				queue = collections.deque()
				queue.append( [alias] )
				path = None
				while queue:
					path = queue.popleft()
					if (path[-1] in query['FROM']) or (path[-1] in query['LEFT JOIN']):
						path.pop()
						break
					queue.extend( (path+[a]) for a in aliasAdjacent[path[-1]] if (a not in path) )
					path = None
				if not path:
					raise Exception("could not join source table %s for output column %s" % (alias,target))
				while path:
					alias = path.pop()
					columnsRemaining.difference_update(aliasColumns[alias])
					query['LEFT JOIN'][alias] = set()
				if self._options.debug_logic:
					self.warn("new LEFT JOIN = %s\n" % ', '.join(query['LEFT JOIN']))
			#while columns need sources
		else:
			# when filtering, build a minimum spanning tree to connect all remaining columns in any order
			#TODO: choose preferred source first as in annotation, rather than blindly expanding until we hit them all?
			if columnsRemaining:
				remaining = columnsRemaining
				inside = query['FROM']
				outside = set( a for a,t in self._queryAliasTable.items() if ((a not in inside) and (a not in query['LEFT JOIN']) and (knowFilter.get(t[0],empty).get(t[1]) or t[1] == 'region_zone')) )
				if self._options.debug_logic:
					self.warn("remaining columns = %s\n" % ', '.join(columnsRemaining))
					self.warn("available aliases = %s\n" % ', '.join(outside))
				queue = collections.deque()
				queue.append( (inside,outside,remaining) )
				while queue:
					inside,outside,remaining = queue.popleft()
					if not remaining:
						break
					queue.extend((inside|{a},outside-{a},remaining-aliasColumns[a]) for a in outside if inside & aliasAdjacent[a])
				if remaining:
					raise Exception("could not find a source table for output columns: %s" % ', '.join(columnsRemaining))
				query['FROM'] |= inside
			#if columns need sources
		#if annotate
		
		# debug
		if self._options.debug_logic:
			self.warn("final FROM = %s\n" % ', '.join(query['FROM']))
			self.warn("final LEFT JOIN = %s\n" % ', '.join(query['LEFT JOIN']))
		
		# fetch option values to insert into condition strings
		formatter = string.Formatter()
		options = {
			'L'           : None,
			'R'           : None,
			'typeID_gene' : self.getOptionTypeID('gene', optional=True),
			'namespaceID_symbol' : self.getOptionNamespaceID('symbol', optional=True),
			'allowUSP'    : (1 if (self._options.allow_unvalidated_snp_positions == 'yes') else 0),
			'pMinOffset'  : '',
			'pMaxOffset'  : '',
			'rpMargin'    : self._options.region_position_margin,
			'rmPercent'   : self._options.region_match_percent if (self._options.region_match_percent != None) else "NULL",
			'rmBases'     : self._options.region_match_bases if (self._options.region_match_bases != None) else "NULL",
			'gbColumn1'   : 'specificity',
			'gbColumn2'   : 'specificity',
			'gbCondition' : ('> 0' if (self._options.allow_ambiguous_knowledge == 'yes') else '>= 100'),
			'zoneSize'    : int(self._loki.getDatabaseSetting('zone_size') or 0),
			'ldprofileID' : self._loki.getLDProfileID(self._options.ld_profile or ''),
		}
		if not options['ldprofileID']:
			sys.exit("ERROR: %s LD profile record not found in the knowledge database" % (self._options.ld_profile or '<default>',))
		if applyOffset:
			if (self._options.coordinate_base != 1):
				options['pMinOffset'] = '+ %d' % (self._options.coordinate_base - 1,)
			if (self._options.coordinate_base != 1) or (self._options.regions_half_open == 'yes'):
				options['pMaxOffset'] = '+ %d' % (self._options.coordinate_base - 1 + (1 if (self._options.regions_half_open == 'yes') else 0),)
		if self._options.reduce_ambiguous_knowledge == 'yes':
			options['gbColumn1'] = ('implication' if (self._options.reduce_ambiguous_knowledge == 'any') else self._options.reduce_ambiguous_knowledge)
			options['gbColumn2'] = ('quality'     if (self._options.reduce_ambiguous_knowledge == 'any') else self._options.reduce_ambiguous_knowledge)
		
		# debug
		if self._options.debug_logic:
			self.warn("initial WHERE = %s\n" % query['WHERE'])
		
		# assign 'select' output columns
		for col in select:
			if query['SELECT'][col] != None:
				continue
			# _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]
			for colsrc in self._queryColumnSources[col]:
				if (colsrc[0] in query['FROM']) or (colsrc[0] in query['LEFT JOIN']):
					if colsrc[0] not in query['_rowid']:
						query['_rowid'][colsrc[0]] = set()
					query['_rowid'][colsrc[0]].add(colsrc[1])
					query['SELECT'][col] = formatter.vformat(colsrc[2], args=None, kwargs=options)
					if (len(colsrc) > 3) and colsrc[3]:
						srcconds = (formatter.vformat(c, args=None, kwargs=options) for c in colsrc[3])
						if colsrc[0] in query['FROM']:
							query['WHERE'].update(srcconds)
						elif colsrc[0] in query['LEFT JOIN']:
							query['LEFT JOIN'][colsrc[0]].update(srcconds)
					break
				#if alias is available
			#foreach possible column source
		#foreach output column
		
		# debug
		if self._options.debug_logic:
			self.warn("SELECT = %s\n" % query['SELECT'])
			self.warn("col WHERE = %s\n" % query['WHERE'])
		
		# assign 'having' column conditions
		for col,conds in having.items():
			# _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]
			for colsrc in self._queryColumnSources[col]:
				if (colsrc[0] in query['FROM']) or (colsrc[0] in query['LEFT JOIN']):
					colconds = ("({0} {1})".format(formatter.vformat(colsrc[2], args=None, kwargs=options), c) for c in conds)
					if colsrc[0] in query['FROM']:
						query['WHERE'].update(colconds)
					elif colsrc[0] in query['LEFT JOIN']:
						query['LEFT JOIN'][colsrc[0]].update(colconds)
					
					if (len(colsrc) > 3) and colsrc[3]:
						srcconds = (formatter.vformat(c, args=None, kwargs=options) for c in colsrc[3])
						if colsrc[0] in query['FROM']:
							query['WHERE'].update(srcconds)
						elif colsrc[0] in query['LEFT JOIN']:
							query['LEFT JOIN'][colsrc[0]].update(srcconds)
					break
				#if alias is available
			#foreach possible column source
		#foreach column condition
		
		# debug
		if self._options.debug_logic:
			self.warn("having WHERE = %s\n" % query['WHERE'])
		
		# add 'where' column conditions
		for tblcol,conds in where.items():
			query['WHERE'].update("{0}.{1} {2}".format(tblcol[0], tblcol[1], formatter.vformat(c, args=None, kwargs=options)) for c in conds)
		
		# debug
		if self._options.debug_logic:
			self.warn("cond WHERE = %s\n" % query['WHERE'])
		
		# add general constraints for included table aliases
		for aliases,conds in self._queryAliasConditions.items():
			for alias in aliases.intersection(query['FROM']):
				options['L'] = alias
				query['WHERE'].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
			for alias in aliases.intersection(query['LEFT JOIN']):
				options['L'] = alias
				query['LEFT JOIN'][alias].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
		
		# TODO: find a way to move this back into _queryAliasConditions without the covering index problem
		if self._options.allow_unvalidated_snp_positions != 'yes':
			if 'd_sl' in query['FROM']:
				query['WHERE'].add("d_sl.validated > 0")
			if 'd_sl' in query['LEFT JOIN']:
				query['LEFT JOIN']['d_sl'].add("d_sl.validated > 0")
		
		# debug
		if self._options.debug_logic:
			self.warn("table WHERE = %s\n" % query['WHERE'])
		
		# add join and pair constraints for included table alias pairs
		for aliasPairs,conds in itertools.chain(self._queryAliasJoinConditions.items(), self._queryAliasPairConditions.items()):
			for aliasLeft in aliasPairs[0]:
				for aliasRight in aliasPairs[-1]:
					options['L'] = aliasLeft
					options['R'] = aliasRight
					if aliasLeft == aliasRight:
						pass
					elif (aliasLeft in query['FROM']) and (aliasRight in query['FROM']):
						query['WHERE'].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
					elif (aliasLeft in query['FROM']) and (aliasRight in query['LEFT JOIN']):
						query['LEFT JOIN'][aliasRight].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
					elif (aliasLeft in query['LEFT JOIN']) and (aliasRight in query['FROM']):
						query['LEFT JOIN'][aliasLeft].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
					elif (aliasLeft in query['LEFT JOIN']) and (aliasRight in query['LEFT JOIN']):
						indexLeft = list(query['LEFT JOIN'].keys()).index(aliasLeft)
						indexRight = list(query['LEFT JOIN'].keys()).index(aliasRight)
						if indexLeft > indexRight:
							query['LEFT JOIN'][aliasLeft].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
						else:
							query['LEFT JOIN'][aliasRight].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
				#foreach right alias
			#foreach left alias
		#foreach pair constraint
		
		# all done
		return query
	#buildQuery()
	
	
	def getQueryText(self, query, noRowIDs=False, sortRowIDs=False, splitRowIDs=False):
		"""
		Generates SQL text from the provided query.

		Parameters:
			query (dict): A dictionary representing the query.
			noRowIDs (bool, optional): Whether to exclude row IDs from the query text.
			sortRowIDs (bool, optional): Whether to sort row IDs in the query text.
			splitRowIDs (bool, optional): Whether to split row IDs into separate columns in the query text.

		Returns:
			(str): The SQL text generated from the query.
		"""		
		sql = "SELECT " + (",\n  ".join("{0} AS {1}".format(query['SELECT'][col] or "NULL",col) for col in query['_columns'])) + "\n"
		rowIDs = list()
		orderBy = list(query['ORDER BY'])
		for alias,cols in query['_rowid'].items():
			rowIDs.extend("COALESCE({0}.{1},'')".format(alias,col) for col in cols)
			if sortRowIDs:
				orderBy.extend("({0}.{1} IS NULL)".format(alias,col) for col in cols)
		if splitRowIDs:
			for n in range(len(rowIDs)):
				sql += "  , {0} AS _rowid_{1}\n".format(rowIDs[n],n)
		if not noRowIDs:
			sql += "  , (" + ("||'_'||".join(rowIDs)) + ") AS _rowid\n"
		if query['FROM']:
			sql += "FROM " + (",\n  ".join("`{0[0]}`.`{0[1]}` AS {1}".format(self._queryAliasTable[a],a) for a in sorted(query['FROM']))) + "\n"
		for alias,joinon in query['LEFT JOIN'].items():
			sql += "LEFT JOIN `{0[0]}`.`{0[1]}` AS {1}\n".format(self._queryAliasTable[alias],alias)
			if joinon:
				sql += "  ON " + ("\n  AND ".join(sorted(joinon))) + "\n"
		if query['WHERE']:
			sql += "WHERE " + ("\n  AND ".join(sorted(query['WHERE']))) + "\n"
		if query['GROUP BY']:
			sql += "GROUP BY " + (", ".join(query['GROUP BY'])) + "\n"
		if query['HAVING']:
			sql += "HAVING " + ("\n  AND ".join(sorted(query['HAVING']))) + "\n"
		if orderBy:
			sql += "ORDER BY " + (", ".join(orderBy)) + "\n"
		if query['LIMIT']:
			sql += "LIMIT " + str(int(query['LIMIT'])) + "\n"
		return sql
	#getQueryText()
	
	
	def prepareTablesForQuery(self, query):
		"""
		Prepares tables referenced in the query for execution.

		Parameters:
			query (dict): A dictionary representing the query.
		"""	
		for db,tbl in set(self._queryAliasTable[a] for a in itertools.chain(query['FROM'], query['LEFT JOIN'])):
			if (db in self._schema) and (tbl in self._schema[db]):
				self.prepareTableForQuery(db, tbl)
	#prepareTablesForQuery()
	
	
	def generateQueryResults(self, query, allowDupes=False, bindings=None, query2=None):
		"""
		Generates query results based on the provided query.

		Parameters:
			query (dict): A dictionary representing the primary query.
			allowDupes (bool, optional): Whether to allow duplicate results.
			bindings (dict, optional): Bindings for parameterized queries.
			query2 (dict, optional): An optional secondary query.

		Yields:
			(tuple): Rows of query results.
		"""	
		# execute the query and yield the results
		cursor = self._loki._db.cursor()
		sql = self.getQueryText(query)
		sql2 = self.getQueryText(query2) if query2 else None
		if self._options.debug_query:
			self.log(sql+"\n")
			for row in cursor.execute("EXPLAIN QUERY PLAN "+sql, bindings):
				self.log(str(row)+"\n")
			if query2:
				self.log(sql2+"\n")
				for row in cursor.execute("EXPLAIN QUERY PLAN "+sql2, bindings):
					self.log(str(row)+"\n")
		else:
			self.prepareTablesForQuery(query)
			if query2:
				self.prepareTablesForQuery(query2)
			if allowDupes:
				lastID = None
				for row in cursor.execute(sql, bindings):
					if row[-1] != lastID:
						lastID = row[-1]
						yield row[:-1]
				if query2:
					lastID = None
					for row in cursor.execute(sql2, bindings):
						if row[-1] != lastID:
							lastID = row[-1]
							yield row[:-1]
			else:
				rowIDs = set()
				for row in cursor.execute(sql, bindings):
					if row[-1] not in rowIDs:
						rowIDs.add(row[-1])
						yield row[:-1]
				if query2:
					for row in cursor.execute(sql2, bindings):
						if row[-1] not in rowIDs:
							rowIDs.add(row[-1])
							yield row[:-1]
				del rowIDs
	#generateQueryResults()
	
	
	##################################################
	# filtering, annotation & modeling
	
	
	def _populateColumnsFromTypes(self, types, columns=None, header=None, ids=None):
		"""
		Populates column and header lists based on the provided types.

		Parameters:
			types (list): A list of types for which columns and headers are to be populated.
			columns (list, optional): A list of column names. Defaults to None.
			header (list, optional): A list of header names. Defaults to None.
			ids (list, optional): A list of IDs. Defaults to None.

		Returns:
			(list): The populated columns list.
		"""
		if columns == None:
			columns = list()
		if header == None:
			header = list()
		if ids == None:
			ids = list()
		for t in types:
			if t == 'snp':
				header.extend(['snp'])
				columns.extend(['snp_label'])
			elif t == 'position':
				header.extend(['chr','position','pos'])
				columns.extend(['position_chr','position_label','position_pos']) # oddball .map file format
			elif t == 'gene':
				header.extend(['gene'])
				columns.extend(['gene_label'])
			elif t == 'generegion':
				header.extend(['chr','gene','start','stop'])
				columns.extend(['biopolymer_chr','gene_label','biopolymer_start','biopolymer_stop'])
			elif t == 'upstream':
				header.extend(['upstream','distance'])
				columns.extend(['upstream_label','upstream_distance'])
			elif t == 'downstream':
				header.extend(['downstream','distance'])
				columns.extend(['downstream_label','downstream_distance'])
			elif t == 'region':
				header.extend(['chr','region','start','stop'])
				columns.extend(['region_chr','region_label','region_start','region_stop'])
			elif t == 'group':
				header.extend(['group'])
				columns.extend(['group_label'])
			elif t == 'source':
				header.extend(['source'])
				columns.extend(['source_label'])
			elif t == 'gwas':
				header.extend(['trait','snps','OR/beta','allele95%CI','riskAfreq','pubmed'])
				columns.extend(['gwas_trait','gwas_snps','gwas_orbeta','gwas_allele95ci','gwas_riskAfreq','gwas_pubmed'])
			elif t == 'snpinput':
				header.extend(['user_input'])
				columns.extend(['snp_label'])
			elif t == 'positioninput':
				header.extend(['user_input'])
				columns.extend(['position_label'])
			elif t == 'geneinput':
				header.extend(['user_input'])
				columns.extend(['gene_label'])
			elif t == 'regioninput':
				header.extend(['user_input'])
				columns.extend(['region_label'])
			elif t == 'groupinput':
				header.extend(['user_input'])
				columns.extend(['group_label'])
			elif t == 'sourceinput':
				header.extend(['user_input'])
				columns.extend(['source_label'])
			elif t == 'disease':
				header.extend(['disease','disease_category'])
				columns.extend(['disease_label','disease_category'])
			elif t in self._queryColumnSources:
				header.append(t)
				columns.append(t)
			else:
				raise Exception("ERROR: unsupported output type '%s'" % t)
		#foreach types
		return columns
	#_populateColumnsFromTypes()
	
	
	def generateFilterOutput(self, types, applyOffset=False):
		"""
		Generates filtered output based on the provided types.

		Parameters:
			types (list): A list of types for filtering.
			applyOffset (bool, optional): Whether to apply an offset. Defaults to False.

		Yields:
			(tuple): Rows of filtered output.
		"""	
		header = list()
		columns = list()
		self._populateColumnsFromTypes(types, columns, header)
		if not (header and columns):
			raise Exception("filtering with empty column list")
		header[0] = "#" + header[0]
		query = self.buildQuery(mode='filter', focus='main', select=columns, applyOffset=applyOffset)
		query2 = None
		if self._inputFilters['user']['source']:
			query2 = self.buildQuery(mode='filter', focus='main', select=columns, applyOffset=applyOffset, userKnowledge=True)
		return itertools.chain( [tuple(header)], self.generateQueryResults(query, allowDupes=(self._options.allow_duplicate_output == 'yes'), query2=query2) )
	#generateFilterOutput()
	
	
	def generateAnnotationOutput(self, typesF, typesA, applyOffset=False):
		"""
		Generates annotated output based on the provided filter and annotation types.

		Parameters:
			typesF (list): A list of types for filtering.
			typesA (list): A list of types for annotation.
			applyOffset (bool, optional): Whether to apply an offset. Defaults to False.

		Yields:
			(tuple): Rows of annotated output.
		"""	
		#TODO user knowledge
		
		# build a baseline filtering query
		headerF = list()
		columnsF = list()
		self._populateColumnsFromTypes(typesF, columnsF, headerF)
		if not (headerF and columnsF):
			raise Exception("annotation with no starting columns")
		queryF = self.buildQuery(mode='filter', focus='main', select=columnsF, applyOffset=applyOffset)
		lenF = len(queryF['_columns'])
		sqlF = self.getQueryText(queryF, splitRowIDs=True)
		self.prepareTablesForQuery(queryF)
		# add each filter rowid column as a condition for annotation
		n = lenF
		conditionsA = collections.defaultdict(set)
		for alias,cols in queryF['_rowid'].items():
			for col in cols:
				n += 1
				conditionsA[(alias,col)].add("= ?%d" % n)
		
		# build the annotation query
		headerA = list()
		columnsA = list()
		self._populateColumnsFromTypes(typesA, columnsA, headerA)
		if not (headerA and columnsA):
			raise Exception("annotation with no extra columns")
		queryA = self.buildQuery(mode='annotate', focus='alt', select=columnsA, where=conditionsA, applyOffset=applyOffset)
		lenA = len(queryA['_columns'])
		sqlA = self.getQueryText(queryA, noRowIDs=True, sortRowIDs=True, splitRowIDs=True)
		self.prepareTablesForQuery(queryA)
		
		# generate filtered results and annotate each of them
		cursorF = self._loki._db.cursor()
		cursorA = self._loki._db.cursor()
		if self._options.debug_query:
			self.warn("========== annotation : filter step ==========\n")
			self.warn(sqlF+"\n")
			for row in cursorF.execute("EXPLAIN QUERY PLAN "+sqlF):
				self.warn(str(row)+"\n")
			self.warn("========== annotation : annotate step ==========\n")
			self.warn(sqlA+"\n")
			emptyF = (0,) * (len(queryF['_columns']) + len(queryF['_rowid']))
			for row in cursorF.execute("EXPLAIN QUERY PLAN "+sqlA, emptyF):
				self.warn(str(row)+"\n")
		elif self._options.allow_duplicate_output == 'yes':
			headerF[0] = "#" + headerF[0]
			yield tuple(headerF + headerA)
			lastF = None
			emptyA = tuple(None for c in columnsA)
			for rowF in cursorF.execute(sqlF):
				if lastF != rowF[-1]:
					lastF = rowF[-1]
					idsA = set()
					for rowA in cursorA.execute(sqlA, rowF[:-1]):
						rowidA = rowA[lenA:]
						if rowidA not in idsA:
							idsA.update(itertools.product(*( (v,) if v == '' else (v,'') for v in rowidA )))
							yield rowF[:lenF] + rowA[:lenA]
					#foreach annotation result
					if not idsA:
						yield rowF[:lenF] + emptyA
				#if filter result is new
			#foreach filter result
		else:
			headerF[0] = "#" + headerF[0]
			yield tuple(headerF + headerA)
			emptyA = tuple(None for c in columnsA)
			for rowF in cursorF.execute(sqlF):
					idsA = set()
					for rowA in cursorA.execute(sqlA, rowF[:-1]):
						rowidA = rowA[lenA:]
						if rowidA not in idsA:
							idsA.update(itertools.product(*( (v,) if v == '' else (v,'') for v in rowidA )))
							# return annotation results
							yield rowF[:lenF] + rowA[:lenA]
					#foreach annotation result
					if not idsA:
						yield rowF[:lenF] + emptyA
				#if filter result is new
			#foreach filter result
	#generateAnnotationOutput()
	
	
	def identifyCandidateModelBiopolymers(self):
		"""
		Identifies candidate model biopolymers.
		"""	
		cursor = self._loki._db.cursor()
		
		# reset candidate tables
		self._inputFilters['cand']['main_biopolymer'] = 0
		self.prepareTableForUpdate('cand','main_biopolymer')
		cursor.execute("DELETE FROM `cand`.`main_biopolymer`")
		self._inputFilters['cand']['alt_biopolymer'] = 0
		cursor.execute("DELETE FROM `cand`.`alt_biopolymer`")
		self.prepareTableForUpdate('cand','alt_biopolymer')
		
		# identify main candidiates from applicable filters
		if sum(filters for table,filters in self._inputFilters['main'].items() if table not in ('group','source')):
			self.log("identifying main model candidiates ...")
			query = self.buildQuery(mode='modelgene', focus='main', select=['gene_id' if self._onlyGeneModels else 'biopolymer_id'])
			sql = "INSERT OR IGNORE INTO `cand`.`main_biopolymer` (biopolymer_id, flag) VALUES (?,0)"
			cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
			numCand = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `cand`.`main_biopolymer`"))
			self.log(" OK: %d candidates\n" % numCand)
			self._inputFilters['cand']['main_biopolymer'] = 1
		#if any main filters other than group/source
		
		# identify alt candidiates from applicable filters
		if sum(filters for table,filters in self._inputFilters['alt'].items() if table not in ('group','source')):
			self.log("identifying alternate model candidiates ...")
			query = self.buildQuery(mode='modelgene', focus='alt', select=['gene_id' if self._onlyGeneModels else 'biopolymer_id'])
			sql = "INSERT OR IGNORE INTO `cand`.`alt_biopolymer` (biopolymer_id, flag) VALUES (?,0)"
			cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
			numCand = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `cand`.`alt_biopolymer`"))
			self.log(" OK: %d candidates\n" % numCand)
			self._inputFilters['cand']['alt_biopolymer'] = 1
		#if any alt filters other than group/source
	#identifyCandidateModelBiopolymers()
	
	
	def identifyCandidateModelGroups(self):
		"""
		Identifies candidate model groups.
		"""	
		self.log("identifying candidiate model groups ...")
		cursor = self._loki._db.cursor()
		
		# reset candidate table
		self._inputFilters['cand']['group'] = 0
		self.prepareTableForUpdate('cand','group')
		cursor.execute("DELETE FROM `cand`.`group`")
		
		# identify candidiates from applicable main filters
		if sum(filters for table,filters in self._inputFilters['main'].items() if table in ('group','source')):
			query = self.buildQuery(mode='modelgroup', focus='main', select=['group_id'])
			if self._inputFilters['cand']['group']:
				cursor.execute("UPDATE `cand`.`group` SET flag = 0")
				sql = "UPDATE `cand`.`group` SET flag = 1 WHERE group_id = ?"
			else:
				sql = "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)"
			cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
			if self._inputFilters['cand']['group']:
				cursor.execute("DELETE FROM `cand`.`group` WHERE flag = 0")
			self._inputFilters['cand']['group'] = 1
		#if any main group/source filters
		
		# identify candidiates from applicable alt filters
		if sum(filters for table,filters in self._inputFilters['alt'].items() if table in ('group','source')):
			query = self.buildQuery(mode='modelgroup', focus='alt', select=['group_id'])
			if self._inputFilters['cand']['group']:
				cursor.execute("UPDATE `cand`.`group` SET flag = 0")
				sql = "UPDATE `cand`.`group` SET flag = 1 WHERE group_id = ?"
			else:
				sql = "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)"
			cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
			if self._inputFilters['cand']['group']:
				cursor.execute("DELETE FROM `cand`.`group` WHERE flag = 0")
			self._inputFilters['cand']['group'] = 1
		#if any main group/source filters
		
		# identify candidiates by size
		query = self.buildQuery(mode='modelgroup', focus='cand', select=['group_id'], having={('gene_id' if self._onlyGeneModels else 'biopolymer_id'):{'!= 0'}})
		if self._inputFilters['cand']['group']:
			cursor.execute("UPDATE `cand`.`group` SET flag = 0")
			sql = "UPDATE `cand`.`group` SET flag = 1 WHERE group_id = ?"
		else:
			sql = "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)"
		# _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]
		for source in self._queryColumnSources['group_id']:
			if source[0] in query['FROM']:
				query['GROUP BY'].append("{0}.{1}".format(source[0], source[1]))
				break
		for source in self._queryColumnSources['gene_id' if self._onlyGeneModels else 'biopolymer_id']:
			if source[0] in query['FROM']:
				if self._options.maximum_model_group_size > 0:
					query['HAVING'].add("(COUNT(DISTINCT %s) BETWEEN 2 AND %d)" % (source[2],self._options.maximum_model_group_size))
				else:
					query['HAVING'].add("COUNT(DISTINCT %s) >= 2" % (source[2],))
				break
		cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
		if self._inputFilters['cand']['group']:
			cursor.execute("DELETE FROM `cand`.`group` WHERE flag = 0")
		self._inputFilters['cand']['group'] = 1
		
		numCand = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `cand`.`group`"))
		self.log(" OK: %d groups\n" % numCand)
	#identifyCandidateModelGroups()
	
	
	def getGeneModels(self):
		"""
		Retrieves gene models based on identified candidate biopolymers and groups.

		Returns:
			(list): List of gene models.
		"""	
		# generate the models if we haven't already
		if self._geneModels == None:
			# find all model component candidiates
			self.identifyCandidateModelBiopolymers()
			self.identifyCandidateModelGroups()
			
			# build model query
			formatter = string.Formatter()
			query = self.buildQuery(mode='model', focus='cand', select=['biopolymer_id_L','biopolymer_id_R','source_id','group_id'])
			query['GROUP BY'].append(formatter.vformat("MIN({biopolymer_id_L}, {biopolymer_id_R})", args=None, kwargs=query['SELECT']))
			query['GROUP BY'].append(formatter.vformat("MAX({biopolymer_id_L}, {biopolymer_id_R})", args=None, kwargs=query['SELECT']))
			query['SELECT']['biopolymer_id_L'] = "MIN(%s)" % query['SELECT']['biopolymer_id_L']
			query['SELECT']['biopolymer_id_R'] = "MAX(%s)" % query['SELECT']['biopolymer_id_R']
			query['SELECT']['source_id'] = "COUNT(DISTINCT %s)" % query['SELECT']['source_id']
			query['SELECT']['group_id'] = "COUNT(DISTINCT %s)" % query['SELECT']['group_id']
			if self._options.minimum_model_score > 0:
				query['HAVING'].add("%s >= %d" % (query['SELECT']['source_id'],self._options.minimum_model_score))
			if self._options.sort_models == 'yes':
				query['ORDER BY'].append(formatter.vformat("{source_id} DESC", args=None, kwargs=query['SELECT']))
				query['ORDER BY'].append(formatter.vformat("{group_id} DESC", args=None, kwargs=query['SELECT']))
			if self._options.maximum_model_count > 0:
				query['LIMIT'] = self._options.maximum_model_count
			
			# execute query and store models
			self._geneModels = list()
			self.log("calculating baseline models ...")
			self._geneModels = list(self.generateQueryResults(query, allowDupes=True)) # the GROUP BY already prevents duplicates
			self.log(" OK: %d models\n" % len(self._geneModels))
		#if no models yet
		
		return self._geneModels
	#getGeneModels()
	
	
	def generateModelOutput(self, typesL, typesR, applyOffset=False):
		"""
		Generates model output based on the provided left-hand and right-hand types.

		Parameters:
			typesL (list): A list of types for the left-hand side.
			typesR (list): A list of types for the right-hand side.
			applyOffset (bool, optional): Whether to apply an offset. Defaults to False.

		Yields:
			(tuple): Rows of model output.
		"""		
		#TODO user knowledge
		
		cursor = self._loki._db.cursor()
		limit = max(0, self._options.maximum_model_count)
		
		# if we'll need baseline gene models, generate them first
		if self._options.all_pairwise_models != 'yes':
			self.getGeneModels()
		
		# build queries for left- and right-hand model expansion
		headerL = list()
		columnsL = list()
		self._populateColumnsFromTypes(typesL, columnsL, headerL)
		headerR = list()
		columnsR = list()
		self._populateColumnsFromTypes(typesR, columnsR, headerR)
		if not (headerL and columnsL and headerR and columnsR):
			raise Exception("model generation with empty column list")
		headerL = list(("%s1" % h) for h in headerL)
		headerL[0] = "#" + headerL[0]
		headerR = list(("%s2" % h) for h in headerR)
		conditionsL = conditionsR = None
		# for knowledge-supported models, add the conditions for expanding from base models
		if self._options.all_pairwise_models != 'yes':
			conditionsL = {('gene_id' if self._onlyGeneModels else 'biopolymer_id') : {"= (CASE WHEN 1 THEN ?1 ELSE 0*?2*?3*?4 END)"}}
			conditionsR = {('gene_id' if self._onlyGeneModels else 'biopolymer_id') : {"= (CASE WHEN 1 THEN ?2 ELSE 0*?1*?3*?4 END)"}}
		queryL = self.buildQuery(mode='filter', focus='main', select=columnsL, having=conditionsL, applyOffset=applyOffset)
		sqlL = self.getQueryText(queryL)
		self.prepareTablesForQuery(queryL)
		queryR = self.buildQuery(mode='filter', focus='alt', select=columnsR, having=conditionsR, applyOffset=applyOffset)
		sqlR = self.getQueryText(queryR)
		self.prepareTablesForQuery(queryR)
		
		# debug or execute model expansion
		if self._options.debug_query:
			self.log(sqlL+"\n")
			self.log("-----\n")
			for row in cursor.execute("EXPLAIN QUERY PLAN "+sqlL, ((1,2,3,4) if self._options.all_pairwise_models != 'yes' else None)):
				self.log(str(row)+"\n")
			
			self.log("=====\n")
			
			self.log(sqlR+"\n")
			self.log("-----\n")
			for row in cursor.execute("EXPLAIN QUERY PLAN "+sqlR, ((1,2,3,4) if self._options.all_pairwise_models != 'yes' else None)):
				self.log(str(row)+"\n")
		elif self._options.all_pairwise_models != 'yes':
			# expand each gene-gene model
			diffTypes = (typesL != typesR)
			headerR.append('score(src-grp)')
			yield tuple(headerL + headerR)
			modelIDs = set()
			for model in self.getGeneModels():
				score = ('%d-%d' % (model[2],model[3]),)
				# store the expanded right-hand side, then pair them all with the expanded left-hand side
				listR = list(cursor.execute(sqlR, model))
				for row in cursor.execute(sqlL, model):
					for modelR in listR:
						modelID = (row[-1],modelR[-1]) if (diffTypes or (row[-1] <= modelR[-1])) else (modelR[-1],row[-1])
						if (diffTypes or (row[-1] != modelR[-1])) and (modelID not in modelIDs):
							modelIDs.add(modelID)
							yield row[:-1] + modelR[:-1] + score
							if limit and len(modelIDs) >= limit:
								return
					#foreach right-hand
				#foreach left-hand
			#foreach model
		else:
			yield tuple(headerL + headerR)
			n = 0
			
			# first query the right-hand side results and store them
			listR = list()
			rowIDs = set()
			for row in cursor.execute(sqlR):
				if row[-1] not in rowIDs:
					rowIDs.add(row[-1])
					listR.append(row)
			del rowIDs
			
			# now query the left-hand side results and pair each with the stored right-hand sides
			rowIDs = set()
			diffCols = (columnsL != columnsR)
			for row in cursor.execute(sqlL):
				if row[-1] not in rowIDs:
					rowIDs.add(row[-1])
					for modelR in listR:
						if diffCols or row[-1] != modelR[-1]:
							n += 1
							yield row[:-1] + modelR[:-1]
							if limit and n >= limit:
								return
			del rowIDs
		#if debug/normal/pairwise
	#generateModelOutput()

#Biofilter


##################################################
# command line interface
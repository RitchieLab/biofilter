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


class Biofilter:
	
	
	##################################################
	# class interrogation
	
	
	@classmethod
	def getVersionTuple(cls):
		# tuple = (major,minor,revision,dev,build,date)
		# dev must be in ('a','b','rc','release') for lexicographic comparison
		return (2,4,1,'release','','2017-04-17')
	#getVersionTuple()
	
	
	@classmethod
	def getVersionString(cls):
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
			self._logFile = open(logPath, 'wb')
		
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
		self._log(message, False)
	#log()
	
	
	def logPush(self, message=None):
		if message:
			self.log(message)
		if self._logHanging:
			self.log("\n")
		self._logIndent += 1
	#logPush()
	
	
	def logPop(self, message=None):
		if self._logHanging:
			self.log("\n")
		self._logIndent = max(0, self._logIndent - 1)
		if message:
			self.log(message)
	#logPop()
	
	
	def warn(self, message=""):
		self._log(message, True)
	#warn()
	
	
	def warnPush(self, message=None):
		if message:
			self.warn(message)
		if self._logHanging:
			self.warn("\n")
		self._logIndent += 1
	#warnPush()
	
	
	def warnPop(self, message=None):
		if self._logHanging:
			self.warn("\n")
		self._logIndent = max(0, self._logIndent - 1)
		if message:
			self.warn(message)
	#warnPop()
	
	
	##################################################
	# database management
	
	
	def attachDatabaseFile(self, dbFile):
		return self._loki.attachDatabaseFile(dbFile)
	#attachDatabaseFile()
	
	
	def prepareTableForUpdate(self, db, table):
		assert((db in self._schema) and (table in self._schema[db]))
		if table not in self._tablesDeindexed[db]:
			self._tablesDeindexed[db].add(table)
			self._loki.dropDatabaseIndecies(self._schema[db], db, table)
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, db, table):
		assert((db in self._schema) and (table in self._schema[db]))
		if table in self._tablesDeindexed[db]:
			self._tablesDeindexed[db].remove(table)
			self._loki.createDatabaseIndecies(self._schema[db], db, table)
			if table == "region":
				self.updateRegionZones(db)
	#prepareTableForQuery()
	
	
	def tableHasData(self, db, table):
		return (sum(row[0] for row in self._loki._db.cursor().execute("SELECT 1 FROM `%s`.`%s` LIMIT 1" % (db,table))) > 0)
	#tableHasData()
	
	
	def updateRegionZones(self, db):
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
			# regions=[ (id,chr,posMin,posMax),... ]
			# yields:[ (id,chr,zone),... ]
			for rowid,chm,posMin,posMax in regions:
				for z in xrange(int(posMin/size),int(posMax/size)+1):
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
		typeID = self._loki.getTypeID('gene')
		if not typeID:
			sys.exit("ERROR: knowledge file contains no gene data")
		return self._loki.generateBiopolymerNameStats(typeID=typeID)
	#generateGeneNameStats()
	
	
	def generateGroupNameStats(self):
		return self._loki.generateGroupNameStats()
	#generateGroupNameStats()
	
	
	def generateLDProfiles(self):
		ldprofiles = self._loki.getLDProfiles()
		for l in sorted(ldprofiles):
			yield (l,)+ldprofiles[l][1:]
	#generateLDProfiles()
	
	
	##################################################
	# LOKI data retrieval
	
	
	def getDatabaseGenomeBuilds(self):
		ucscBuild = self._loki.getDatabaseSetting('ucschg')
		ucscBuild = int(ucscBuild) if (ucscBuild != None) else None
		grchBuild = None
		if ucscBuild:
			for build in self._loki.generateGRChByUCSChg(ucscBuild):
				grchBuild = max(grchBuild, int(build))
		return (grchBuild,ucscBuild)
	#getDatabaseGenomeBuilds()
	
	
	def getOptionTypeID(self, value, optional=False):
		typeID = self._loki.getTypeID(value)
		if not (typeID or optional):
			sys.exit("ERROR: database contains no %s data\n" % (value,))
		return typeID
	#getOptionTypeID()
	
	
	def getOptionNamespaceID(self, value, optional=False):
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
		if grchBuild:
			if ucscBuild:
				if ucscBuild != (self._loki.getUCSChgByGRCh(grchBuild) or ucscBuild):
					sys.exit("ERROR: specified reference genome build GRCh%d is not known to correspond to UCSC hg%d" % (grchBuild, ucscBuild))
			else:
				ucscBuild = self._loki.getUCSChgByGRCh(grchBuild)
		elif ucscBuild:
			grchBuild = None
			for build in self._loki.generateGRChByUCSChg(ucscBuild):
				grchBuild = max(grchBuild, int(build))
		return (grchBuild,ucscBuild)
	#getInputGenomeBuilds()
	
	
	def generateMergedFilteredSNPs(self, snps, tally=None, errorCallback=None):
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
		l = 0
		for line in lines:
			l += 1
			try:
				cols = line.strip().split(separator,1)
				if not cols:
					continue
				try:
					rs = long(cols[0])
				except ValueError:
					if cols[0].upper().startswith('RS'):
						rs = long(cols[0][2:])
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
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
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
					pos = long(pos) + offset
				
				yield (label,chm,pos,extra)
			except:
				if (l > 1) and errorCallback:
					errorCallback(line, "%s at index %d" % (str(sys.exc_info()[1]),l))
		#foreach line
	#generateLociFromText()
	
	
	def generateLociFromMapFiles(self, paths, separator=None, applyOffset=False, errorCallback=None):
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
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
		# loci=[ (label,chr,pos,extra), ... ]
		newloci = loci
		
		if not ucscBuildOld:
			self.warn("WARNING: UCSC hg# build version was not specified for position input; assuming it matches the knowledge database\n")
		elif not ucscBuildNew:
			self.warn("WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n")
		elif ucscBuildOld != ucscBuildNew:
			if not self._loki.hasLiftOverChains(ucscBuildOld, ucscBuildNew):
				sys.exit("ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg%s to hg%s\n" % (oldHG or "?", newHG or "?"))
			liftoverError = "dropped during liftOver from hg%s to hg%s" % (ucscBuildOld or "?", ucscBuildNew or "?")
			def liftoverCallback(region):
				errorCallback("\t".join(str(s) for s in region), liftoverError)
			#liftoverCallback()
			newloci = self._loki.generateLiftOverLoci(ucscBuildOld, ucscBuildNew, loci, tally=None, errorCallback=(liftoverCallback if errorCallback else None))
		#if old!=new
		
		return newloci
	#generateLiftOverLoci()
	
	
	def generateRegionsFromText(self, lines, separator=None, applyOffset=False, errorCallback=None):
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
					posMin = long(posMin) + offsetStart
				if (posMax == '-') or (posMax == 'NA'):
					posMax = None
				else:
					posMax = long(posMax) + offsetEnd
				
				yield (label,chm,posMin,posMax,extra)
			except:
				if (l > 1) and errorCallback:
					errorCallback(line, "%s at index %d" % (str(sys.exc_info()[1]),l))
		#foreach line
	#generateRegionsFromText()
	
	
	def generateRegionsFromFiles(self, paths, separator=None, applyOffset=False, errorCallback=None):
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
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
		# regions=[ (label,chr,posMin,posMax,extra), ... ]
		newregions = regions
		
		if not ucscBuildOld:
			self.warn("WARNING: UCSC hg# build version was not specified for region input; assuming it matches the knowledge database\n")
		elif not ucscBuildNew:
			self.warn("WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n")
		elif ucscBuildOld != ucscBuildNew:
			if not self._loki.hasLiftOverChains(ucscBuildOld, ucscBuildNew):
				sys.exit("ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg%s to hg%s\n" % (oldHG or "?", newHG or "?"))
			liftoverError = "dropped during liftOver from hg%s to hg%s" % (ucscBuildOld or "?", ucscBuildNew or "?")
			def liftoverCallback(region):
				errorCallback("\t".join(str(s) for s in region), liftoverError)
			#liftoverCallback()
			newregions = self._loki.generateLiftOverRegions(ucscBuildOld, ucscBuildNew, regions, tally=None, errorCallback=(liftoverCallback if errorCallback else None))
		#if old!=new
		
		return newregions
	#generateLiftOverRegions()
	
	
	def generateNamesFromText(self, lines, defaultNS=None, separator=None, errorCallback=None):
		utf8 = codecs.getencoder('utf8')
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
					name = utf8(cols[0].strip())[0]
				elif len(cols) >= 2:
					ns = cols[0].strip()
					name = utf8(cols[1].strip())[0]
					extra = cols[2] if (len(cols) > 2) else None
				
				yield (ns,name,extra)
			except:
				if (l > 1) and errorCallback:
					errorCallback(line, "%s at index %d" % (str(sys.exc_info()[1]),l))
		#foreach line in file
	#generateNamesFromText()
	
	
	def generateNamesFromNameFiles(self, paths, defaultNS=None, separator=None, errorCallback=None):
		for path in paths:
			try:
				with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
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
	# region input
	
	
	def unionInputRegions(self, db, regions, errorCallback=None):
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
		self.log("adding user-defined source '%s' ..." % (label,))
		self._inputFilters['user']['source'] += 1
		usourceID = -self._inputFilters['user']['source']
		cursor = self._loki._db.cursor()
		cursor.execute("INSERT INTO `user`.`source` (source_id,source,description) VALUES (?,?,?)", (usourceID,label,description))
		self.log(" OK\n")
		return usourceID
	#addUserSource()
	
	
	def addUserGroup(self, usourceID, label, description, errorCallback=None):
		self.log("adding user-defined group '%s' ..." % (label,))
		self._inputFilters['user']['group'] += 1
		ugroupID = -self._inputFilters['user']['group']
		cursor = self._loki._db.cursor()
		cursor.execute("INSERT INTO `user`.`group` (group_id,label,description,source_id) VALUES (?,?,?,?)", (ugroupID,label,description,usourceID))
		self.log(" OK\n")
		return ugroupID
	#addUserGroup()
	
	
	def addUserGroupBiopolymers(self, ugroupID, namesets, errorCallback=None):
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
		return self.getUserSourceIDs([source])[source]
	#getSourceID()
	
	
	def getUserSourceIDs(self, sources=None):
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
		realScore = sum(1 for f in realFeatures if (featureBin.get(f) and featureData[f][1]))
		if realScore < 1:
			return numPermutations
		
		#TODO: refinement?
		
		_sample = random.sample
		binDraws = collections.Counter(featureBin[f] for f in realFeatures if featureBin.get(f))
		totalScore = 0
		for p in xrange(numPermutations):
			permScore = 0
			for b,draws in binDraws.iteritems():
				permScore += sum(1 for f in _sample(binFeatures[b], draws) if featureData[f][1])
			if permScore > realScore:
				totalScore += 1
				if maxScore and (totalScore >= maxScore):
					break
		return totalScore
	#getPARISPermutationScore()
	
	
	def generatePARISResults(self, ucscBuildUser, ucscBuildDB):
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
			for z in xrange( int(posMin / zoneSize), int(posMax / zoneSize) + 1 ):
				chrZoneFeatures[chm][z].add(fid)
		self.logPop("... OK: %d regions\n" % (len(featureData),))
		
		def analyzeLoci(generator):
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
					for z in xrange( int(posMin / zoneSize), int(posMax / zoneSize) + 1 ):
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
		for fid,data in featureData.iteritems():
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
		for b in xrange(2,2+count):
			for n in xrange(size + (1 if ((b-2) < extra) else 0)):
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
		for gid,gdata in geneData.iteritems():
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
		for uid,udata in groupData.iteritems():
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
		
		yield ('group','description','genes','features','simple','(sig)','complex','(sig)','pval')
		for uid,udata in groupData.iteritems():
			yield (
				udata[0],
				udata[1],
				len(udata[2]),
				len(udata[3]),
				sum(1 for f in udata[3] if (featureData[f][0] == 1)),
				sum(1 for f in udata[3] if (featureData[f][1] and (featureData[f][0] == 1))),
				sum(1 for f in udata[3] if (featureData[f][0] > 1)),
				sum(1 for f in udata[3] if (featureData[f][1] and (featureData[f][0] > 1))),
				renderPermuPVal(udata[3]),
				itertools.chain(
					[ ('gene','features','simple','(sig)','complex','(sig)','pval') ],
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
	} #class._queryColumnSources
	
	
	def getQueryTemplate(self):
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
			fromFilter = { db:{ tbl:bool(flag) for tbl,flag in self._inputFilters[db].iteritems() } for db in ('main','alt','cand') }
		if joinFilter == None:
			joinFilter = { db:{ tbl:bool(flag) for tbl,flag in self._inputFilters[db].iteritems() } for db in ('main','alt','cand') }
		knowFilter = { 'db':{ tbl:True for db,tbl in self._queryAliasTable.itervalues() if (db == 'db') } }
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
		for alias,dbtable in self._queryAliasTable.iteritems():
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
		columnsRemaining = set(col for col,aliases in columnAliases.iteritems() if not (set(aliases) & query['FROM']))
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
				outside = set( a for a,t in self._queryAliasTable.iteritems() if ((a not in inside) and (a not in query['LEFT JOIN']) and (knowFilter.get(t[0],empty).get(t[1]) or t[1] == 'region_zone')) )
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
		for col,conds in having.iteritems():
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
		for tblcol,conds in where.iteritems():
			query['WHERE'].update("{0}.{1} {2}".format(tblcol[0], tblcol[1], formatter.vformat(c, args=None, kwargs=options)) for c in conds)
		
		# debug
		if self._options.debug_logic:
			self.warn("cond WHERE = %s\n" % query['WHERE'])
		
		# add general constraints for included table aliases
		for aliases,conds in self._queryAliasConditions.iteritems():
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
		for aliasPairs,conds in itertools.chain(self._queryAliasJoinConditions.iteritems(), self._queryAliasPairConditions.iteritems()):
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
						indexLeft = query['LEFT JOIN'].keys().index(aliasLeft)
						indexRight = query['LEFT JOIN'].keys().index(aliasRight)
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
		sql = "SELECT " + (",\n  ".join("{0} AS {1}".format(query['SELECT'][col] or "NULL",col) for col in query['_columns'])) + "\n"
		rowIDs = list()
		orderBy = list(query['ORDER BY'])
		for alias,cols in query['_rowid'].iteritems():
			rowIDs.extend("COALESCE({0}.{1},'')".format(alias,col) for col in cols)
			if sortRowIDs:
				orderBy.extend("({0}.{1} IS NULL)".format(alias,col) for col in cols)
		if splitRowIDs:
			for n in xrange(len(rowIDs)):
				sql += "  , {0} AS _rowid_{1}\n".format(rowIDs[n],n)
		if not noRowIDs:
			sql += "  , (" + ("||'_'||".join(rowIDs)) + ") AS _rowid\n"
		if query['FROM']:
			sql += "FROM " + (",\n  ".join("`{0[0]}`.`{0[1]}` AS {1}".format(self._queryAliasTable[a],a) for a in sorted(query['FROM']))) + "\n"
		for alias,joinon in query['LEFT JOIN'].iteritems():
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
		for db,tbl in set(self._queryAliasTable[a] for a in itertools.chain(query['FROM'], query['LEFT JOIN'])):
			if (db in self._schema) and (tbl in self._schema[db]):
				self.prepareTableForQuery(db, tbl)
	#prepareTablesForQuery()
	
	
	def generateQueryResults(self, query, allowDupes=False, bindings=None, query2=None):
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
			elif t in self._queryColumnSources:
				header.append(t)
				columns.append(t)
			else:
				raise Exception("ERROR: unsupported output type '%s'" % t)
		#foreach types
		return columns
	#_populateColumnsFromTypes()
	
	
	def generateFilterOutput(self, types, applyOffset=False):
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
		for alias,cols in queryF['_rowid'].iteritems():
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
			idsF = set()
			emptyA = tuple(None for c in columnsA)
			for rowF in cursorF.execute(sqlF):
				if rowF[-1] not in idsF:
					idsF.add(rowF[-1])
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
	#generateAnnotationOutput()
	
	
	def identifyCandidateModelBiopolymers(self):
		cursor = self._loki._db.cursor()
		
		# reset candidate tables
		self._inputFilters['cand']['main_biopolymer'] = 0
		self.prepareTableForUpdate('cand','main_biopolymer')
		cursor.execute("DELETE FROM `cand`.`main_biopolymer`")
		self._inputFilters['cand']['alt_biopolymer'] = 0
		cursor.execute("DELETE FROM `cand`.`alt_biopolymer`")
		self.prepareTableForUpdate('cand','alt_biopolymer')
		
		# identify main candidiates from applicable filters
		if sum(filters for table,filters in self._inputFilters['main'].iteritems() if table not in ('group','source')):
			self.log("identifying main model candidiates ...")
			query = self.buildQuery(mode='modelgene', focus='main', select=['gene_id' if self._onlyGeneModels else 'biopolymer_id'])
			sql = "INSERT OR IGNORE INTO `cand`.`main_biopolymer` (biopolymer_id, flag) VALUES (?,0)"
			cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
			numCand = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `cand`.`main_biopolymer`"))
			self.log(" OK: %d candidates\n" % numCand)
			self._inputFilters['cand']['main_biopolymer'] = 1
		#if any main filters other than group/source
		
		# identify alt candidiates from applicable filters
		if sum(filters for table,filters in self._inputFilters['alt'].iteritems() if table not in ('group','source')):
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
		self.log("identifying candidiate model groups ...")
		cursor = self._loki._db.cursor()
		
		# reset candidate table
		self._inputFilters['cand']['group'] = 0
		self.prepareTableForUpdate('cand','group')
		cursor.execute("DELETE FROM `cand`.`group`")
		
		# identify candidiates from applicable main filters
		if sum(filters for table,filters in self._inputFilters['main'].iteritems() if table in ('group','source')):
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
		if sum(filters for table,filters in self._inputFilters['alt'].iteritems() if table in ('group','source')):
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
			val = long(val[:-1]) * 1000
		elif val[-1:] == 'm':
			val = long(val[:-1]) * 1000 * 1000
		elif val[-1:] == 'g':
			val = long(val[:-1]) * 1000 * 1000 * 1000
		else:
			val = long(val)
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
		print version
		print
		parser.print_usage()
		print
		print "Use -h for details."
		sys.exit(2)
	#if no args
	
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
		cfHandle = (sys.stdin if cfName == '-' else open(cfName,'rb'))
		cfStream = (line.replace('\t',' ').strip() for line in cfHandle)
		cfLines = (line for line in cfStream if line and not line.startswith('#'))
		cfReader = csv.reader(cfLines, dialect=cfDialect)
		
		# parse the file; recurse for includes, store the rest
		cfArgs = list()
		for line in cfReader:
			line[0] = '--' + line[0].lower().replace('_','-')
			if line[0] == '--include':
				for l in xrange(1,len(line)):
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
			print "(in configuration file '%s')" % cfName
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
	for types in (options.annotate or empty):
		n = types.count(':')
		if n > 1:
			sys.exit("ERROR: cannot annotate '%s', only two sets of outputs are allowed\n" % (' '.join(types),))
		elif n:
			i = types.index(':')
			typesF = types[:i]
			typesA = types[i+1:None]
		else:
			typesF = types[0:1]
			typesA = types[1:None]
		
		if typesF and typesA:
			typeOutputPath['annotation'][(tuple(typesF),tuple(typesA))] = options.prefix + '.' + '-'.join(typesF) + '.' + '-'.join(typesA)
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
	for outtype,outputPath in typeOutputPath.iteritems():
		typeOutputInfo[outtype] = collections.OrderedDict()
		for output,path in outputPath.iteritems():
			if outtype == 'report':
				label = "%s report" % (output,)
			elif outtype == 'invalid':
				label = "invalid %s input report" % (output,)
			elif outtype == 'filter':
				label = "'%s' filter" % (" ".join(output),)
			elif outtype == 'annotation':
				label = "'%s : %s' annotation" % (" ".join(output[0])," ".join(output[1]))
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
		sourceVerify[source][2][file] = (date,long(size),md5)
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
				for opt,val in verify[1].iteritems():
					if opt not in options or val != options[opt]:
						sys.exit("ERROR: configuration requires %s loader option %s = %s, but knowledge database reports setting = %s\n" % (source,opt,val,options.get(opt)))
			if verify[2]:
				files = bio._loki.getSourceIDFiles(sourceID)
				for file,meta in verify[2].iteritems():
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
			seed = long(options.random_number_generator_seed)
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
		return utf8("%s%s" % ((delim.join((col if isinstance(col,basestring) else str('' if col == None else col)) for col in row)),term))[0]
	
	# process reports
	for report,info in typeOutputInfo['report'].iteritems():
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
				for source,fingerprint in bio.getSourceFingerprints().iteritems():
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
			itertools.chain(*(line for line in open(sourceFile,'rU'))),
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
			itertools.chain(*(line for line in open(sourceFile,'rU'))),
			errorCallback=cb['alt-source']
		)
	
	# report invalid input, if requested
	if options.report_invalid_input == 'yes':
		for modtype,lines in cbLog.iteritems():
			if lines:
				path = ('<stdout>' if options.stdout == 'yes' else typeOutputInfo['invalid'][modtype][1])
				bio.logPush("writing invalid %s input report to '%s' ...\n" % (modtype,path))
				outfile = (sys.stdout if options.stdout == 'yes' else open(path, 'wb'))
				outfile.write("\n".join(lines))
				outfile.write("\n")
				if outfile != sys.stdout:
					outfile.close()
				bio.logPop("... OK: %d invalid inputs\n" % (len(lines)/2))
		#foreach modifier/type
	#if report invalid input
	
	# process filters
	for types,info in typeOutputInfo['filter'].iteritems():
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
	for types,info in typeOutputInfo['annotation'].iteritems():
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
	for types,info in typeOutputInfo['models'].iteritems():
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
		outfileS.write(encodeRow(header))
		n = 0
		for row in parisGen:
			n += 1
			outfileS.write(encodeRow(row[:-1]))
			if outfileD:
				outfileD.write(encodeRow( ("Pathway Investigation:",row[0]) ))
				outfileD.write(encodeRow( (row[1],) ))
				outfileD.write("\n")
				outfileD.write(encodeRow(header[2:]))
				outfileD.write(encodeRow(row[2:-1]))
				outfileD.write("\n")
				outfileD.write(encodeRow( ("Gene Breakdown:",row[0]) ))
				outfileD.write("\n")
				for rowD in row[-1]:
					outfileD.write(encodeRow(rowD))
				outfileD.write("\n")
		if outfileS != sys.stdout:
			outfileS.close()
		if outfileD and (outfileD != sys.stdout):
			outfileD.close()
		bio.logPop("... OK: %d results\n" % n)
	#if PARIS
	
#__main__

#!/usr/bin/env python

import argparse
import codecs
import collections
import csv
import itertools
import os
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
		return (2,0,0,'a',11,'2012-09-24')
	#getVersionTuple()
	
	
	@classmethod
	def getVersionString(cls):
		v = list(cls.getVersionTuple())
		# tuple = (major,minor,revision,dev,build,date)
		# dev must be in > 'rc' for releases for lexicographic comparison,
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
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index' : {
					'snp__rs' : '(rs)',
				}
			}, #main.snp
			
			
			'locus' : {
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index' : {
					'locus__pos' : '(chr,pos)',
				}
			}, #main.locus
			
			
			'region' : {
				'table' : """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
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
  flag TINYINT NOT NULL DEFAULT 0
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
  flag TINYINT NOT NULL DEFAULT 0
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
		
		# verify loki_db version (generateBiopolymerIDsBySearch() in 2.0.0a10)
		if loki_db.Database.getVersionTuple() < (2,0,0,'a',10):
			exit("ERROR: LOKI version 2.0.0a10 or later required; found %s" % (loki_db.Database.getVersionString(),))
		
		# initialize instance database
		self._loki = loki_db.Database()
		self._loki.setLogger(self)
		for db in self._schema:
			if db != 'main':
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
		return self._loki.attachDatabaseFile(dbFile, readOnly=True)
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
	
	
	def updateRegionZones(self, db):
		assert((db in self._schema) and 'region' in self._schema[db] and 'region_zone' in self._schema[db])
		self.log("calculating %s region zone coverage ..." % db)
		cursor = self._loki._db.cursor()
		
		size = self._loki.getDatabaseSetting('zone_size')
		if not size:
			raise Exception("ERROR: could not determine database setting 'zone_size'")
		size = int(size)
		
		# make sure all regions are correctly oriented
		cursor.execute("UPDATE `%s`.`region` SET posMin = posMax, posMax = posMin WHERE posMin > posMax" % db)
		
		# define zone generator
		def _zones(size, regions):
			# regions=[ (id,chr,posMin,posMax),... ]
			# yields:[ (id,chr,zone),... ]
			for r in regions:
				for z in xrange(int(r[2]/size),int(r[3]/size)+1):
					yield (r[0],r[1],z)
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
	# knowledge database metadata
	
	
	def generateGeneNameStats(self):
		typeID = self._loki.getTypeID('gene')
		if not typeID:
			raise Exception("ERROR: knowledge file contains no gene data")
		return self._loki.generateBiopolymerNameStats(typeID=typeID)
	#generateGeneNameStats()
	
	
	def generateGroupNameStats(self, gtype=None):
		typeID = gtype and self._loki.getTypeID(gtype)
		if gtype and not typeID:
			raise Exception("ERROR: unknown group type '%s'" % gtype)
		return self._loki.generateGroupNameStats(typeID=typeID)
	#generateGroupNameStats()
	
	
	##################################################
	# input data parsers and lookup helpers
	
	
	def generateRSesFromText(self, text, errorCallback=None):
		for line in text:
			if not line:
				continue
			try:
				yield long(line)
			except ValueError:
				if line[0:2].upper() == 'RS':
					try:
						yield long(line[2:])
					except ValueError:
						if errorCallback:
							errorCallback(line)
		#foreach line
	#generateRSesFromText()
	
		
	def generateRSesFromRSFiles(self, paths, errorCallback=None):
		for path in paths:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
				for rs in self.generateRSesFromText((line for line in file if line[0:1] != '#'), errorCallback):
					yield rs
			#with file
		#foreach path
	#generateRSesFromRSFiles()
	
	
	def generateLociFromText(self, text, separator=':', errorCallback=None):
		for line in text:
			if not line:
				continue
			
			label = chm = pos = None
			cols = line.split(separator)
			
			# parse line
			if len(cols) < 2:
				if errorCallback:
					errorCallback(line)
				continue
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
			
			# parse, validate and convert chromosome
			if chm[:3] == 'CHR':
				chm = chm[3:]
			if chm not in self._loki.chr_num:
				if errorCallback:
					errorCallback(line)
				continue
			chm = self._loki.chr_num[chm]
			
			# parse and convert locus label
			if not label:
				label = 'chr%s:%s' % (self._loki.chr_name[chm], pos)
			
			# parse and convert position
			if pos == '-' or pos == 'NA':
				pos = None
			else:
				pos = long(pos)
			
			yield (label,chm,pos)
		#foreach line
	#generateLociFromText()
	
	
	def generateLociFromMapFiles(self, paths, errorCallback=None):
		for path in paths:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
				for locus in self.generateLociFromText((line for line in file if line[0:1] != '#'), None, errorCallback):
					yield locus
			#with file
		#foreach path
	#generateLociFromMapFiles()
	
	
	def generateNamesFromNameFiles(self, paths):
		for path in paths:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
				for line in file:
					if line[0:1] != '#':
						yield line.rstrip()
				#foreach line in file
			#with file
		#foreach path
	#generateNamesFromNameFiles()
	
	
	def generateRegionsFromText(self, text, separator=':', errorCallback=None):
		for line in text:
			if not line:
				continue
			
			label = chm = posMin = posMax = None
			cols = line.split(separator)
			
			# parse line
			if len(cols) < 3:
				if errorCallback:
					errorCallback(line)
				continue
			elif len(cols) == 3:
				chm = cols[0].upper()
				posMin = cols[1].upper()
				posMax = cols[2].upper()
			elif len(cols) >= 4:
				chm = cols[0].upper()
				label = cols[1]
				posMin = cols[2].upper()
				posMax = cols[3].upper()
			
			# parse, validate and convert chromosome
			if chm[:3] == 'CHR':
				chm = chm[3:]
			if chm not in self._loki.chr_num:
				if errorCallback:
					errorCallback(line)
				continue
			chm = self._loki.chr_num[chm]
			
			# parse and convert region label
			if not label:
				label = 'chr%s:%s-%s' % (self._loki.chr_name[chm], posMin, posMax)
			
			# parse and convert positions
			if posMin == '-' or posMin == 'NA':
				posMin = None
			else:
				posMin = long(posMin)
			if posMax == '-' or posMax == 'NA':
				posMax = None
			else:
				posMax = long(posMax)
			
			yield (label,chm,posMin,posMax)
		#foreach line
	#generateRegionsFromText()
	
	
	def generateRegionsFromFiles(self, paths, errorCallback=None):
		for path in paths:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as file:
				for region in self.generateRegionsFromText((line for line in file if line[0:1] != '#'), None, errorCallback):
					yield region
			#with file
		#foreach path
	#generateRegionsFromFiles()
	
	
	def getOptionTypeID(self, value, optional=False):
		typeID = self._loki.getTypeID(value)
		if not (typeID or optional):
			self.warn("ERROR: database contains no %s data\n" % (value,))
			sys.exit(1)
		return typeID
	#getOptionTypeID()
	
	
	def getOptionNamespaceID(self, value, optional=False):
		if value == '-': # primary labels
			return None
		namespaceID = self._loki.getNamespaceID(value)
		if not (namespaceID or optional):
			self.warn("ERROR: unknown identifier type '%s'\n" % (value,))
			sys.exit(1)
		return namespaceID
	#getOptionNamespaceID()
	
	
	##################################################
	# snp/locus input
	
	
	def unionInputSNPs(self, db, snps):
		# snps=[ rs, ... ]
		self.logPush("adding to %s SNP filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'snp')
		sql = "INSERT INTO `%s`.`snp` (label,rs) VALUES ('rs'||?1,?2)" % db
		tally = dict()
		cursor.executemany(sql, self._loki.generateCurrentRSesByRS(snps, tally))
		self.logPop("... OK: added %d SNPs (%d RS#s merged)\n" % (tally['match']+tally['merge'],tally['merge']))
		
		self._inputFilters[db]['snp'] += 1
	#unionInputSNPs()
	
	
	def intersectInputSNPs(self, db, snps):
		# snps=[ rs, ... ]
		if not self._inputFilters[db]['snp']:
			return self.unionInputSNPs(db, snps)
		self.logPush("reducing %s SNP filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'snp')
		cursor.execute("UPDATE `%s`.`snp` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`snp` SET flag = 1 WHERE (1 OR ?1) AND rs = ?2" % db
		tally = dict()
		cursor.executemany(sql, self._loki.generateCurrentRSesByRS(snps, tally))
		cursor.execute("DELETE FROM `%s`.`snp` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d SNPs (%d dropped, %d RS#s merged)\n" % (numBefore-numDrop,numDrop,tally['merge']))
		
		self._inputFilters[db]['snp'] += 1
	#intersectInputSNPs()
	
	
	def unionInputLoci(self, db, loci):
		# loci=[ (label,chr,pos), ... ]
		self.logPush("adding to %s position filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		# use OR IGNORE to continue on data error, i.e. missing chr or pos
		self.prepareTableForUpdate(db, 'locus')
		sql = "INSERT OR IGNORE INTO `%s`.`locus` (label,chr,pos) VALUES (?1,?2,?3); SELECT LAST_INSERT_ROWID()" % db
		lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, loci):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		if numNull:
			self.warn("WARNING: ignored %d invalid positions\n" % numNull)
		self.logPop("... OK: added %d positions\n" % numAdd)
		
		self._inputFilters[db]['locus'] += 1
	#unionInputLoci()
	
	
	def intersectInputLoci(self, db, loci):
		# loci=[ (label,chr,pos), ... ]
		if not self._inputFilters[db]['locus']:
			return self.unionInputLoci(db, loci)
		self.logPush("reducing %s position filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'locus')
		cursor.execute("UPDATE `%s`.`locus` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`locus` SET flag = 1 WHERE chr = ?1 AND pos = ?2" % db
		cursor.executemany(sql, loci)
		cursor.execute("DELETE FROM `%s`.`locus` WHERE flag = 0" % db)
		numDrop = self._loki._db.changes()
		self.logPop("... OK: kept %d positions (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['locus'] += 1
	#intersectInputLoci()
	
	
	##################################################
	# region/boundary input
	
	
	def unionInputGenes(self, db, names):
		# names=[ name, ... ]
		self.logPush("adding to %s gene filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID('gene')
		namespaceID = self.getOptionNamespaceID(self._options.gene_identifier_type)
		
		self.prepareTableForUpdate(db, 'gene')
		sql = "INSERT INTO `%s`.`gene` (label,biopolymer_id) VALUES (?1,?2); SELECT 1" % db
		maxMatch = (None if self._options.allow_ambiguous_genes == 'yes' else 1)
		tally = dict()
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		if tally['null']:
			self.warn("WARNING: ignored %d unrecognized gene identifier(s)\n" % tally['null'])
		if tally['ambig']:
			if self._options.allow_ambiguous_genes == 'yes':
				self.warn("WARNING: added multiple results for %d ambiguous gene identifier(s)\n" % tally['ambig'])
			else:
				self.warn("WARNING: ignored %d ambiguous gene identifier(s)\n" % tally['ambig'])
		self.logPop("... OK: added %d genes\n" % numAdd)
		
		self._inputFilters[db]['gene'] += 1
	#unionInputGenes()
	
	
	def intersectInputGenes(self, db, names):
		# names=[ name, ... ]
		if not self._inputFilters[db]['gene']:
			return self.unionInputGenes(db, names)
		self.logPush("reducing %s gene filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID('gene')
		namespaceID = self.getOptionNamespaceID(self._options.gene_identifier_type)
		
		self.prepareTableForQuery(db, 'gene')
		cursor.execute("UPDATE `%s`.`gene` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		tally = dict()
		sql = "UPDATE `%s`.`gene` SET flag = 1 WHERE (1 OR ?1) AND biopolymer_id = ?2" % db
		maxMatch = (None if self._options.allow_ambiguous_genes == 'yes' else 1)
		cursor.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		cursor.execute("DELETE FROM `%s`.`gene` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		if tally['null']:
			self.warn("WARNING: ignored %d unrecognized gene identifier(s)\n" % tally['null'])
		if tally['ambig']:
			if self._options.allow_ambiguous_genes == 'yes':
				self.warn("WARNING: kept multiple results for %d ambiguous gene identifier(s)\n" % tally['ambig'])
			else:
				self.warn("WARNING: ignored %d ambiguous gene identifier(s)\n" % tally['ambig'])
		self.logPop("... OK: kept %d genes (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['gene'] += 1
	#intersectInputGenes()
	
	
	def unionInputGeneSearch(self, db, texts):
		# texts=[ text, ... ]
		self.logPush("adding to %s gene filter by text search ...\n" % db)
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID('gene')
		
		self.prepareTableForUpdate(db, 'gene')
		sql = "INSERT INTO `%s`.`gene` (biopolymer_id,label) VALUES (?1,?2); SELECT 1" % db
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateBiopolymerIDsBySearch(texts, typeID=typeID)):
			numAdd += 1
		self.logPop("... OK: added %d genes\n" % numAdd)
		
		self._inputFilters[db]['gene'] += 1
	#unionInputGeneSearch()
	
	
	def intersectInputGeneSearch(self, db, texts):
		# texts=[ texts, ... ]
		if not self._inputFilters[db]['gene']:
			return self.unionInputGeneSearch(db, texts)
		self.logPush("reducing %s gene filter by text search ...\n" % db)
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID('gene')
		
		self.prepareTableForQuery(db, 'gene')
		cursor.execute("UPDATE `%s`.`gene` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`gene` SET flag = 1 WHERE biopolymer_id = ?1 AND (1 OR ?2)" % db
		cursor.executemany(sql, self._loki.generateBiopolymerIDsBySearch(texts, typeID=typeID))
		cursor.execute("DELETE FROM `%s`.`gene` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d genes (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['gene'] += 1
	#intersectInputGeneSearch()
	
	
	def unionInputRegions(self, db, regions):
		# regions=[ (label,chr,posMin,posMax), ... ]
		self.logPush("adding to %s region filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		# use OR IGNORE to continue on data error, i.e. missing chr or pos
		self.prepareTableForUpdate(db, 'region')
		sql = "INSERT OR IGNORE INTO `%s`.`region` (label,chr,posMin,posMax) VALUES (?1,?2,?3,?4); SELECT LAST_INSERT_ROWID()" % db
		lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, regions):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		if numNull:
			self.warn("WARNING: ignored %d invalid regions\n" % numNull)
		self.logPop("... OK: added %d regions\n" % numAdd)
		
		self._inputFilters[db]['region'] += 1
	#unionInputRegions()
	
	
	def intersectInputRegions(self, db, regions):
		# regions=[ (label,chr,posMin,posMax), ... ]
		if not self._inputFilters[db]['region']:
			return self.unionInputRegions(db, regions)
		self.logPush("reducing %s region filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'region')
		cursor.execute("UPDATE `%s`.`region` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`region` SET flag = 1 WHERE (1 OR ?1) AND chr = ?2 AND posMin = ?3 AND posMax = ?4" % db
		cursor.executemany(sql, regions)
		cursor.execute("DELETE FROM `%s`.`region` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d regions (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['region'] += 1
	#intersectInputRegions()
	
	
	##################################################
	# group input
	
	
	def unionInputGroups(self, db, names, gtype=None):
		# names=[ name, ... ]
		self.logPush("adding to %s %s filter ...\n" % (db,(gtype or "group")))
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID(gtype) if gtype else None
		namespaceID = self.getOptionNamespaceID(self._options.group_identifier_type)
		
		self.prepareTableForUpdate(db, 'group')
		sql = "INSERT INTO `%s`.`group` (label,group_id) VALUES (?1,?2); SELECT 1" % db
		maxMatch = (None if self._options.allow_ambiguous_groups == 'yes' else 1)
		tally = dict()
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		if tally['null']:
			self.warn("WARNING: ignored %d unrecognized group identifier(s)\n" % tally['null'])
		if tally['ambig']:
			if self._options.allow_ambiguous_groups == 'yes':
				self.warn("WARNING: added multiple results for %d ambiguous group identifier(s)\n" % tally['ambig'])
			else:
				self.warn("WARNING: ignored %d ambiguous group identifier(s)\n" % tally['ambig'])
		self.logPop("... OK: added %d groups\n" % numAdd)
		
		self._inputFilters[db]['group'] += 1
	#unionInputGroups()
	
	
	def intersectInputGroups(self, db, names, gtype=None):
		# names=[ name, ... ]
		if not self._inputFilters[db]['group']:
			return self.unionInputGroups(db, names, gtype)
		self.logPush("reducing %s %s filter ...\n" % (db,(gtype or "group")))
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID(gtype) if gtype else None
		namespaceID = self.getOptionNamespaceID(self._options.group_identifier_type)
		
		self.prepareTableForQuery(db, 'group')
		cursor.execute("UPDATE `%s`.`group` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		maxMatch = (None if self._options.allow_ambiguous_groups == 'yes' else 1)
		tally = dict()
		sql = "UPDATE `%s`.`group` SET flag = 1 WHERE (1 OR ?1) AND group_id = ?2" % db
		cursor.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		cursor.execute("DELETE FROM `%s`.`group` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		if tally['null']:
			self.warn("WARNING: ignored %d unrecognized group identifier(s)\n" % tally['null'])
		if tally['ambig']:
			if self._options.allow_ambiguous_groups == 'yes':
				self.warn("WARNING: kept multiple results for %d ambiguous group identifier(s)\n" % tally['ambig'])
			else:
				self.warn("WARNING: ignored %d ambiguous group identifier(s)\n" % tally['ambig'])
		self.logPop("... OK: kept %d groups (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['group'] += 1
	#intersectGroups()
	
	
	def unionInputGroupSearch(self, db, texts, gtype=None):
		# texts=[ text, ... ]
		self.logPush("adding to %s %s filter by text search ...\n" % (db,(gtype or "group")))
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID(gtype) if gtype else None
		
		self.prepareTableForUpdate(db, 'group')
		sql = "INSERT INTO `%s`.`group` (label,group_id) VALUES (?2,?1); SELECT 1" % db
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateGroupIDsBySearch(texts, typeID=typeID)):
			numAdd += 1
		self.logPop("... OK: added %d groups\n" % numAdd)
		
		self._inputFilters[db]['group'] += 1
	#unionInputGroupSearch()
	
	
	def intersectInputGroupSearch(self, db, texts, gtype=None):
		# texts=[ texts, ... ]
		if not self._inputFilters[db]['group']:
			return self.unionInputGroupSearch(db, texts, gtype)
		self.logPush("reducing %s %s filter by text search ...\n" % (db,(gtype or "group")))
		cursor = self._loki._db.cursor()
		
		typeID = self.getOptionTypeID(gtype) if gtype else None
		
		self.prepareTableForQuery(db, 'group')
		cursor.execute("UPDATE `%s`.`group` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`group` SET flag = 1 WHERE group_id = ?1 AND (1 OR ?2)" % db
		cursor.executemany(sql, self._loki.generateGroupIDsBySearch(texts, typeID=typeID))
		cursor.execute("DELETE FROM `%s`.`group` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d groups (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['group'] += 1
	#intersectInputGroupSearch()
	
	
	##################################################
	# source input
	
	
	def unionInputSources(self, db, names):
		# names=[ name, ... ]
		self.logPush("adding to %s source filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'source')
		sql = "INSERT OR IGNORE INTO `%s`.`source` (label,source_id) VALUES (?1,?2); SELECT LAST_INSERT_ROWID()" % db
		lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, self._loki.getSourceIDs(names).iteritems()):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		if numNull:
			self.warn("WARNING: ignored %d unrecognized source identifier(s)\n" % numNull)
		self.logPop("... OK: added %d sources\n" % numAdd)
		
		self._inputFilters[db]['source'] += 1
	#unionInputSources()
	
	
	def intersectInputSources(self, db, names):
		# names=[ name, ... ]
		if not self._inputFilters[db]['source']:
			return self.unionInputSources(db, names)
		self.logPush("reducing %s source filter ...\n" % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'source')
		cursor.execute("UPDATE `%s`.`source` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`source` SET flag = 1 WHERE source_id = ?1" % db
		cursor.executemany(sql, self._loki.getSourceIDs(names).iteritems())
		cursor.execute("DELETE FROM `%s`.`source` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.logPop("... OK: kept %d sources (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['source'] += 1
	#intersectInputSources()
	
	
	##################################################
	# internal query builder
	
	
	# define table aliases for each actual table: {alias:(db,table),...}
	_queryAliasTable = {
		'm_s'    : ('main','snp'),              # (label,rs)
		'm_l'    : ('main','locus'),            # (label,chr,pos)
		'm_rz'   : ('main','region_zone'),      # (region_rowid,chr,zone)
		'm_r'    : ('main','region'),           # (label,chr,posMin,posMax)
		'm_bg'   : ('main','gene'),             # (label,biopolymer_id)
		'm_g'    : ('main','group'),            # (label,group_id)
		'm_c'    : ('main','source'),           # (label,source_id)
		'a_s'    : ('alt','snp'),               # (label,rs)
		'a_l'    : ('alt','locus'),             # (label,chr,pos)
		'a_rz'   : ('alt','region_zone'),       # (region_rowid,chr,zone)
		'a_r'    : ('alt','region'),            # (label,chr,posMin,posMax)
		'a_bg'   : ('alt','gene'),              # (label,biopolymer_id)
		'a_g'    : ('alt','group'),             # (label,group_id)
		'a_c'    : ('alt','source'),            # (label,source_id)
		'c_mb_L' : ('cand','main_biopolymer'),  # (biopolymer_id)
		'c_mb_R' : ('cand','main_biopolymer'),  # (biopolymer_id)
		'c_ab_R' : ('cand','alt_biopolymer'),   # (biopolymer_id)
		'c_g'    : ('cand','group'),            # (group_id)
		'd_sl'   : ('db','snp_locus'),          # (rs,chr,pos)
		'd_bz'   : ('db','biopolymer_zone'),    # (biopolymer_id,chr,zone)
		'd_br'   : ('db','biopolymer_region'),  # (biopolymer_id,ldprofile_id,chr,posMin,posMax)
		'd_b'    : ('db','biopolymer'),         # (biopolymer_id,type_id,label)
		'd_gb'   : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'd_gb_L' : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'd_gb_R' : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'd_g'    : ('db','group'),              # (group_id,type_id,label,source_id)
		'd_c'    : ('db','source'),             # (source_id,source)
	} #class._queryAliasTable{}
	
	
	# define constraints on single table aliases: dict{ set(a1,a2,...) : set(cond1,cond2,...), ... }
	_queryAliasConditions = {
	#	frozenset({'d_sl'}) : frozenset({
	#		"({allowUSP} OR ({L}.validated > 0))", #TODO: prevents use of covering index!
	#	}),
		frozenset({'d_br'}) : frozenset({
			"{L}.ldprofile_id = {ldprofileID}",
		}),
		frozenset({'d_gb','d_gb_L','d_gb_R'}) : frozenset({
			"{L}.biopolymer_id != 0",
			"{L}.{gbColumn} {gbCondition}",
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
		(frozenset({'m_l','a_l','d_sl'}),) : frozenset({
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
			"{L}.zone = {R}.zone",
		}),
		(frozenset({'m_bg','a_bg','d_br','d_b'}),) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'m_bg','a_bg','d_b'}),frozenset({'d_gb'})) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'d_gb_L','d_gb_R'}),) : frozenset({
			"{L}.biopolymer_id != {R}.biopolymer_id",
		}),
		(frozenset({'m_g','a_g','d_gb','d_g'}),) : frozenset({
			"{L}.group_id = {R}.group_id",
		}),
		(frozenset({'m_c','a_c','d_g','d_c'}),) : frozenset({
			"{L}.source_id = {R}.source_id",
		}),
		
		(frozenset({'c_mb_L'}),frozenset({'d_gb_L'})) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'c_mb_R','c_ab_R'}),frozenset({'d_gb_R'})) : frozenset({
			"{L}.biopolymer_id = {R}.biopolymer_id",
		}),
		(frozenset({'c_g','d_g'}),frozenset({'d_gb','d_gb_L','d_gb_R','d_g'})) : frozenset({
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
			"({L}.posMax - {L}.posMin) >= {rmBases}",
			"({R}.posMax - {R}.posMin) >= {rmBases}",
			"((" +
				"{L}.posMin >= {R}.posMin AND " +
				"{L}.posMin <= {R}.posMax - MAX({rmBases}, MIN({L}.posMax - {L}.posMin, {R}.posMax - {R}.posMin) * {rmPercent} / 100)" +
			") OR (" +
				"{R}.posMin >= {L}.posMin AND " +
				"{R}.posMin <= {L}.posMax - MAX({rmBases}, MIN({L}.posMax - {L}.posMin, {R}.posMax - {R}.posMin) * {rmPercent} / 100)" +
			"))",
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
			('a_s',  'rs', "a_s.rs"),
			('m_s',  'rs', "m_s.rs"),
			('d_sl', 'rs', "d_sl.rs"),
		],
		'snp_label' : [
			('a_s',  'rs', "a_s.label"),
			('m_s',  'rs', "m_s.label"),
			('d_sl', 'rs', "'rs'||d_sl.rs"),
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
		'position_chr' : [
			('a_l',  'rowid',   "a_l.chr"),
			('m_l',  'rowid',   "m_l.chr"),
			('d_sl', '_ROWID_', "d_sl.chr"),
		],
		'position_pos' : [
			('a_l',  'rowid',   "a_l.pos"),
			('m_l',  'rowid',   "m_l.pos"),
			('d_sl', '_ROWID_', "d_sl.pos"),
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
		'region_chr' : [
			('a_r',  'rowid',   "a_r.chr"),
			('m_r',  'rowid',   "m_r.chr"),
			('d_br', '_ROWID_', "d_br.chr"),
		],
		'region_zone' : [
			('a_rz', 'zone', "a_rz.zone"),
			('m_rz', 'zone', "m_rz.zone"),
			('d_bz', 'zone', "d_bz.zone"),
		],
		'region_posMin' : [
			('a_r',  'rowid',   "a_r.posMin"),
			('m_r',  'rowid',   "m_r.posMin"),
			('d_br', '_ROWID_', "d_br.posMin"),
		],
		'region_posMax' : [
			('a_r',  'rowid',   "a_r.posMax"),
			('m_r',  'rowid',   "m_r.posMax"),
			('d_br', '_ROWID_', "d_br.posMax"),
		],
		
		'biopolymer_id' : [
			('a_bg',   'biopolymer_id', "a_bg.biopolymer_id"),
			('m_bg',   'biopolymer_id', "m_bg.biopolymer_id"),
			('c_mb_L', 'biopolymer_id', "c_mb_L.biopolymer_id"),
			('c_mb_R', 'biopolymer_id', "c_mb_R.biopolymer_id"),
			('c_ab_R', 'biopolymer_id', "c_ab_R.biopolymer_id"),
			('d_br',   'biopolymer_id', "d_br.biopolymer_id"),
			('d_gb',   'biopolymer_id', "d_gb.biopolymer_id"),
			('d_gb_L', 'biopolymer_id', "d_gb_L.biopolymer_id"),
			('d_gb_R', 'biopolymer_id', "d_gb_R.biopolymer_id"),
			('d_b',    'biopolymer_id', "d_b.biopolymer_id"),
		],
		'biopolymer_id_L' : [
			('c_mb_L', 'biopolymer_id', "c_mb_L.biopolymer_id"),
			('d_gb_L', 'biopolymer_id', "d_gb_L.biopolymer_id"),
			('d_b',    'biopolymer_id', "d_b.biopolymer_id"),
		],
		'biopolymer_id_R' : [
			('c_mb_R', 'biopolymer_id', "c_mb_R.biopolymer_id"),
			('c_ab_R', 'biopolymer_id', "c_ab_R.biopolymer_id"),
			('d_gb_R', 'biopolymer_id', "d_gb_R.biopolymer_id"),
			('d_b',    'biopolymer_id', "d_b.biopolymer_id"),
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
		'gene_identifiers' : [
			('a_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name) FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = a_bg.biopolymer_id)"),
			('m_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name) FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = m_bg.biopolymer_id)"),
			('d_b',  'biopolymer_id', "(SELECT GROUP_CONCAT(namespace||':'||name) FROM `db`.`biopolymer_name` AS d_bn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_bn.biopolymer_id = d_b.biopolymer_id)", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		'gene_symbols' : [
			('a_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(name) FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = a_bg.biopolymer_id AND d_bn.namespace_id = {namespaceID_symbol})", "a_bg.biopolymer_id"),
			('m_bg', 'biopolymer_id', "(SELECT GROUP_CONCAT(name) FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = m_bg.biopolymer_id AND d_bn.namespace_id = {namespaceID_symbol})", "m_bg.biopolymer_id"),
			('d_b',  'biopolymer_id', "(SELECT GROUP_CONCAT(name) FROM `db`.`biopolymer_name` AS d_bn WHERE d_bn.biopolymer_id = d_b.biopolymer_id  AND d_bn.namespace_id = {namespaceID_symbol})", "d_b.biopolymer_id", {"d_b.type_id+0 = {typeID_gene}"}),
		],
		
		'group_id' : [
			('a_g',    'group_id', "a_g.group_id"),
			('m_g',    'group_id', "m_g.group_id"),
			('c_g',    'group_id', "c_g.group_id"),
			('d_gb',   'group_id', "d_gb.group_id"),
			('d_gb_L', 'group_id', "d_gb_L.group_id"),
			('d_gb_R', 'group_id', "d_gb_R.group_id"),
			('d_g',    'group_id', "d_g.group_id"),
		],
		'group_label' : [
			('a_g', 'group_id', "a_g.label"),
			('m_g', 'group_id', "m_g.label"),
			('d_g', 'group_id', "d_g.label"),
		],
		'group_identifiers' : [
			('a_g', 'group_id', "(SELECT GROUP_CONCAT(namespace||':'||name) FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = a_g.group_id)"),
			('m_g', 'group_id', "(SELECT GROUP_CONCAT(namespace||':'||name) FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = m_g.group_id)"),
			('d_g', 'group_id', "(SELECT GROUP_CONCAT(namespace||':'||name) FROM `db`.`group_name` AS d_gn JOIN `db`.`namespace` AS d_n USING (namespace_id) WHERE d_gn.group_id = d_g.group_id)"),
		],
		
		'source_id' : [
			('a_c', 'source_id', "a_c.source_id"),
			('m_c', 'source_id', "m_c.source_id"),
			('d_g', 'source_id', "d_g.source_id"),
			('d_c', 'source_id', "d_c.source_id"),
		],
		'source_label' : [
			('a_c', 'source_id', "a_c.label"),
			('m_c', 'source_id', "m_c.label"),
			('d_c', 'source_id', "d_c.source"),
		],
	} #class._queryColumnSources
	
	
	def getQueryTemplate(self):
		return {
			'SELECT'    : collections.OrderedDict(),
			            # OD{ colA:expA, colB:expB, ... }         => SELECT expA AS colA, expB AS colB, ...
			'_rowid'    : collections.OrderedDict(),
			            # OD{ tblA:{colA1,colA2,...}, ... }       => SELECT ... (tblA.colA1||'_'||tblA.colA2...) AS rowid
			'FROM'      : set(),  # { tblA, tblB, ... }           => FROM aliasTable[tblA] AS tblA, aliasTable[tblB] AS tblB, ...
			'LEFT JOIN' : collections.OrderedDict(),
			            # OD{ tblA:{expA1,expA2,...}, ... }       => LEFT JOIN aliasTable[tblA] ON expA1 AND expA2 ...
			'WHERE'     : set(),  # { expA, expB, ... }           => WHERE expA AND expB AND ...
			'GROUP BY'  : list(), # [ expA, expB, ... ]           => GROUP BY expA, expB, ...
			'HAVING'    : set(),  # { expA, expB, ... }           => HAVING expA AND expB AND ...
			'ORDER BY'  : list(), # [ expA, expB, ... ]           => ORDER BY expA, expB, ...
			'LIMIT'     : None    # num                           => LIMIT INT(num)
		}
	#getQueryTemplate()
	
	
	def buildQuery(self, outputs, conditions=None, focus='main', modelGenes=False, modelGroups=False, annotate=False):
		if self._options.debug_logic:
			self.warnPush("buildQuery(outputs=%s, conditions=%s, focus=%s, modelGenes=%s, modelGroups=%s, annotate=%s\n" % (
					outputs,conditions,focus,modelGenes,modelGroups,annotate
			))
		conditions = conditions or dict()
		query = self.getQueryTemplate()
		
		# generate table alias join adjacency map
		aliasAdjacent = collections.defaultdict(set)
		for aliasPairs in self._queryAliasJoinConditions:
			for aliasLeft in aliasPairs[0]:
				for aliasRight in aliasPairs[-1]:
					if aliasLeft != aliasRight:
						aliasAdjacent[aliasLeft].add(aliasRight)
						aliasAdjacent[aliasRight].add(aliasLeft)
		
		if self._options.debug_logic:
			self.warn("aliasAdjacent = \n")
			for alias in sorted(aliasAdjacent):
				self.warn("  %s : %s\n" % (alias,sorted(aliasAdjacent[alias])))
		
		# generate column availability map
		# _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]
		columnAliases = collections.defaultdict(set)
		aliasColumns = collections.defaultdict(set)
		for col in outputs:
			if col not in self._queryColumnSources:
				raise Exception("internal query with unsupported output column '{0}'".format(col))
			for source in self._queryColumnSources[col]:
				columnAliases[col].add(source[0])
				aliasColumns[source[0]].add(col)
			query['SELECT'][col] = None
		for col in conditions:
			if col not in self._queryColumnSources:
				raise Exception("internal query with unsupported condition column '{0}'".format(col))
			for source in self._queryColumnSources[col]:
				columnAliases[col].add(source[0])
				aliasColumns[source[0]].add(col)
		if not (columnAliases and aliasColumns):
			raise Exception("internal query with no outputs or conditions")
		
		if self._options.debug_logic:
			self.warn("columnAliases = %s\n" % columnAliases)
			self.warn("aliasColumns = %s\n" % aliasColumns)
		
		# start from the specified tables when annotating, otherwise from applicable input tables
		if annotate:
			query['FROM'].update(a[0] for a in annotate.keys())
		else:
			# re-index all input filter tables
			for db in self._schema:
				for tbl in self._schema[db]:
					self.prepareTableForQuery(db, tbl)
			
			# add applicable input filter table aliases
			for alias,table in self._queryAliasTable.iteritems():
				if table[0] == 'main' and self._inputFilters['main'][table[1]] and ((focus == 'main') or (focus == 'alt' and self._options.alternate_model_filtering != 'yes')):
					if modelGenes and alias in ('m_g','m_c'):
						pass
					elif modelGroups and alias not in ('m_g','m_c'):
						pass
					else:
						query['FROM'].add(alias)
				elif table[0] == 'alt' and self._inputFilters['alt'][table[1]] and focus == 'alt':
					if modelGenes and alias in ('a_g','a_c'):
						pass
					elif modelGroups and alias not in ('a_g','a_c'):
						pass
					else:
						query['FROM'].add(alias)
				elif table[0] == 'cand' and self._inputFilters['cand'][table[1]] and focus == 'cand':
					if (self._options.alternate_model_filtering == 'yes' or self._inputFilters['cand']['alt_biopolymer']) and alias == 'c_mb_R':
						pass
					elif modelGenes and alias == 'c_g':
						pass
					elif modelGroups and alias != 'c_g':
						pass
					else:
						query['FROM'].add(alias)
			
			# if there are no input filter tables, start from the last-resort source for a random output or condition column
			if not query['FROM']:
				col = next(itertools.chain(outputs, conditions))
				alias = self._queryColumnSources[col][-1][0]
				query['FROM'].add(alias)
		#if annotate
		
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
				if not remaining:
					break
				queue.extend( (inside|{a},outside-{a},remaining-{a}) for a in outside if inside & aliasAdjacent[a] )
			if remaining:
				raise Exception("could not find a join path for starting tables: %s" % query['FROM'])
			query['FROM'] |= inside
		#if tables need joining
		
		if self._options.debug_logic:
			self.warn("joined FROM = %s\n" % ', '.join(query['FROM']))
		
		# add 'db' table aliases to satisfy any remaining columns
		columnsRemaining = set(col for col in columnAliases if not (columnAliases[col] & query['FROM']))
		if annotate:
			# when annotating, do BFS on each remaining column in order to guarantee a valid path of LEFT JOINs
			while columnsRemaining:
				target = next(col for col in itertools.chain(conditions,outputs) if col in columnsRemaining)
				inside = query['FROM'].union(query['LEFT JOIN'])
				outside = set(a for a,t in self._queryAliasTable.iteritems() if t[0] == 'db' and a not in inside)
				path = list()
				if self._options.debug_logic:
					self.warn("current LEFT JOIN = %s\n" % ', '.join(query['LEFT JOIN']))
					self.warn("target column = %s\n" % target)
					self.warn("available aliases = %s\n" % ', '.join(outside))
				queue = collections.deque()
				queue.extend((inside,outside-{a},[a]) for a in outside if inside & aliasAdjacent[a])
				while queue:
					inside,outside,path = queue.popleft()
					#if self._options.debug_logic:
					#	self.warn("considering path = %s\n" % ', '.join(path))
					if target in aliasColumns[path[-1]]:
						break
					queue.extend((inside,outside-{a},path+[a]) for a in outside if path[-1] in aliasAdjacent[a])
				if (not path) or (target not in aliasColumns[path[-1]]):
					raise Exception("could not find a source table for output column: %s" % target)
				for alias in path:
					columnsRemaining.difference_update(aliasColumns[alias])
					query['LEFT JOIN'][alias] = set()
			#while columns need sources
		else:
			# when filtering, build a minimum spanning tree to connect all remaining columns at once, in any order
			if columnsRemaining:
				remaining = columnsRemaining
				inside = query['FROM']
				outside = set(a for a,t in self._queryAliasTable.iteritems() if t[0] == 'db' and a not in inside and a not in query['LEFT JOIN'])
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
		
		if self._options.debug_logic:
			self.warn("final FROM = %s\n" % ', '.join(query['FROM']))
			self.warn("final LEFT JOIN = %s\n" % ', '.join(query['LEFT JOIN']))
		
		# fetch option values to insert into condition strings
		formatter = string.Formatter()
		options = {
			'L'           : None,
			'R'           : None,
			'typeID_gene' : self.getOptionTypeID('gene'),
			'namespaceID_symbol' : self.getOptionNamespaceID('symbol', optional=True),
			'allowUSP'    : (1 if self._options.allow_unvalidated_snp_positions == 'yes' else 0),
			'rpMargin'    : self._options.region_position_margin,
			'rmPercent'   : self._options.region_match_percent,
			'rmBases'     : self._options.region_match_bases,
			'gbColumn'    : ('specificity' if self._options.reduce_ambiguous_knowledge == 'no' else self._options.reduce_ambiguous_knowledge),
			'gbCondition' : ('> 0' if self._options.allow_ambiguous_knowledge == 'yes' else '>= 100'),
		}
		options['zoneSize'] = int(self._loki.getDatabaseSetting('zone_size') or 0)
		options['ldprofileID'] = self._loki.getLDProfileID(self._options.ld_profile or '')
		if not options['ldprofileID']:
			self.warn("ERROR: knowledge database is missing the default LD profile record")
			sys.exit(1)
		
		# assign output columns
		for col in outputs:
			# _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]
			for source in self._queryColumnSources[col]:
				if (source[0] in query['FROM']) or (source[0] in query['LEFT JOIN']):
					if source[0] not in query['_rowid']:
						query['_rowid'][source[0]] = set()
					query['_rowid'][source[0]].add(source[1])
					query['SELECT'][col] = formatter.vformat(source[2], args=None, kwargs=options)
					if (len(source) > 3) and source[3]:
						if source[0] in query['FROM']:
							query['WHERE'].update(formatter.vformat(c, args=None, kwargs=options) for c in source[3])
						else:
							query['LEFT JOIN'][source[0]].update(formatter.vformat(c, args=None, kwargs=options) for c in source[3])
					break
				#if alias is available
			#foreach possible source
		#foreach output column
		
		if self._options.debug_logic:
			self.warn("SELECT = %s\n" % query['SELECT'])
		
		# assign column conditions
		for col,conds in conditions.iteritems():
			conds = conds if isinstance(conds, set) else {conds}
			# _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ]
			for source in self._queryColumnSources[col]:
				if source[0] in query['FROM']:
					query['WHERE'].update("({0} {1})".format(formatter.vformat(source[2], args=None, kwargs=options), c) for c in conds)
					if (len(source) > 3) and source[3]:
						query['WHERE'].update(formatter.vformat(c, args=None, kwargs=options) for c in source[3])
					break
				elif source[0] in query['LEFT JOIN']:
					query['LEFT JOIN'][source[0]].update("({0} {1})".format(formatter.vformat(source[2], args=None, kwargs=options), c) for c in conds)
					if (len(source) > 3) and source[3]:
						query['LEFT JOIN'][source[0]].update(formatter.vformat(c, args=None, kwargs=options) for c in source[3])
					break
				#if alias is available
			#foreach possible source
		#foreach column condition
		
		# add annotation conditions
		if annotate:
			query['WHERE'].update("{0}.{1} {2}".format(tblcol[0], tblcol[1], formatter.vformat(exp, args=None, kwargs=options)) for tblcol,exp in annotate.iteritems())
		
		#if conditions and self._options.debug_logic:
		#	self.warn("condition WHERE = %s\n" % query['WHERE'])
		#	self.warn("condition LEFT JOIN = %s\n" % query['LEFT JOIN'])
		
		# add general constraints for included table aliases
		for aliases,conds in self._queryAliasConditions.iteritems():
			for alias in aliases.intersection(query['FROM']):
				options['L'] = alias
				query['WHERE'].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
			for alias in aliases.intersection(query['LEFT JOIN']):
				options['L'] = alias
				query['LEFT JOIN'][alias].update(formatter.vformat(c, args=None, kwargs=options) for c in conds)
		
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
		
		#if self._options.debug_logic:
		#	self.warn("final WHERE = %s\n" % query['WHERE'])
		#	self.warn("final LEFT JOIN = %s\n" % query['LEFT JOIN'])
		
		# all done
		return query
	#buildQuery()
	
	
	def getQueryText(self, query, noRowIDs=False, sortRowIDs=False, splitRowIDs=False):
		sql = "SELECT " + (",\n  ".join("{0} AS {1}".format(exp or "NULL",col) for col,exp in query['SELECT'].iteritems())) + "\n"
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
	
	
	def generateQueryResults(self, query, allowDupes=False, bindings=None):
		# execute the query and yield the results
		cursor = self._loki._db.cursor()
		sql = self.getQueryText(query, noRowIDs=allowDupes)
		if self._options.debug_query:
			self.log(sql+"\n")
			for row in cursor.execute("EXPLAIN QUERY PLAN "+sql, bindings):
				self.log(str(row)+"\n")
		elif allowDupes:
			for row in cursor.execute(sql, bindings):
				yield row
		else:
			rowIDs = set()
			for row in cursor.execute(sql, bindings):
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
			elif t == 'region':
				header.extend(['chr','region','posMin','posMax'])
				columns.extend(['region_chr','region_label','region_posMin','region_posMax']) # inspired by oddball .map file format
			elif t == 'group':
				header.extend(['group'])
				columns.extend(['group_label'])
			elif t == 'source':
				header.extend(['source'])
				columns.extend(['source_label'])
			elif t in self._queryColumnSources:
				header.append(t)
				columns.append(t)
			else:
				raise Exception("ERROR: unsupported output type '%s'" % t)
		#foreach types
		return columns
	#_populateColumnsFromTypes()
	
	
	def generateFilterOutput(self, types):
		header = list()
		columns = list()
		self._populateColumnsFromTypes(types, columns, header)
		if not (header and columns):
			raise Exception("filtering with empty column list")
		header[0] = "#" + header[0]
		return itertools.chain([tuple(header)], self.generateQueryResults(self.buildQuery(columns)))
	#generateFilterOutput()
	
	
	def generateAnnotationOutput(self, typesF, typesA):
		# build a baseline filtering query
		headerF = list()
		columnsF = list()
		self._populateColumnsFromTypes(typesF, columnsF, headerF)
		if not (headerF and columnsF):
			raise Exception("annotation with no starting columns")
		queryF = self.buildQuery(columnsF)
		lenF = len(queryF['SELECT'])
		sqlF = self.getQueryText(queryF, splitRowIDs=True)
		
		# add each filter rowid column as a condition for annotation
		n = lenF
		conditionsA = dict()
		for alias,cols in queryF['_rowid'].iteritems():
			for col in cols:
				n += 1
				conditionsA[(alias,col)] = "= ?%d" % n
		
		# build the annotation query
		headerA = list()
		columnsA = list()
		self._populateColumnsFromTypes(typesA, columnsA, headerA)
		if not (headerA and columnsA):
			raise Exception("annotation with no extra columns")
		queryA = self.buildQuery(columnsA, annotate=conditionsA)
		lenA = len(queryA['SELECT'])
		sqlA = self.getQueryText(queryA, noRowIDs=True, sortRowIDs=True, splitRowIDs=True)
		
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
			emptyF = (0,) * (len(queryF['SELECT']) + len(queryF['_rowid']))
			for row in cursorF.execute("EXPLAIN QUERY PLAN "+sqlA, emptyF):
				self.warn(str(row)+"\n")
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
			query = self.buildQuery(['gene_id' if self._onlyGeneModels else 'biopolymer_id'], focus='main', modelGenes=True)
			sql = "INSERT OR IGNORE INTO `cand`.`main_biopolymer` (biopolymer_id, flag) VALUES (?,0)"
			cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
			numCand = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `cand`.`main_biopolymer`"))
			self.log(" OK: %d candidates\n" % numCand)
			self._inputFilters['cand']['main_biopolymer'] = 1
		#if any main filters other than group/source
		
		# identify alt candidiates from applicable filters
		if sum(filters for table,filters in self._inputFilters['alt'].iteritems() if table not in ('group','source')):
			self.log("identifying alternate model candidiates ...")
			query = self.buildQuery(['gene_id' if self._onlyGeneModels else 'biopolymer_id'], focus='alt', modelGenes=True)
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
			query = self.buildQuery(['group_id'], focus='main', modelGroups=True)
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
			query = self.buildQuery(['group_id'], focus='alt', modelGroups=True)
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
		query = self.buildQuery(['group_id'], {('gene_id' if self._onlyGeneModels else 'biopolymer_id'):' != 0'}, focus='cand', modelGroups=True)
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
			query = self.buildQuery(['biopolymer_id_L','biopolymer_id_R','source_id','group_id'], focus='cand')
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
			self._geneModels = list(self.generateQueryResults(query, allowDupes=True))
			self.log(" OK: %d models\n" % len(self._geneModels))
		#if no models yet
		
		return self._geneModels
	#getGeneModels()
	
	
	def generateModelOutput(self, typesL, typesR):
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
			conditionsL = {('gene_id' if self._onlyGeneModels else 'biopolymer_id') : "= (CASE WHEN 1 THEN ?1 ELSE 0*?2*?3*?4 END)"}
			conditionsR = {('gene_id' if self._onlyGeneModels else 'biopolymer_id') : "= (CASE WHEN 1 THEN ?2 ELSE 0*?1*?3*?4 END)"}
		sqlL = self.getQueryText(self.buildQuery(columnsL, conditionsL, focus='main'))
		sqlR = self.getQueryText(self.buildQuery(columnsR, conditionsR, focus='alt'))
		
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
			headerR.append('score')
			yield tuple(headerL + headerR)
			modelIDs = set()
			for model in self.getGeneModels():
				score = ('-'.join(str(s) for s in model[2:]),)
				# store the expanded right-hand side, then pair them all with the expanded left-hand side
				listR = list(cursor.execute(sqlR, model))
				for row in cursor.execute(sqlL, model):
					for modelR in listR:
						modelID = (row[-1],modelR[-1])
						if modelID not in modelIDs:
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
		raise ValueError("invalid choice: '%s' must be yes/on/true/1 or no/off/false/0")
	#yesno()
	
	# define custom percentage type handler
	def percent(val):
		val = str(val).strip().lower()
		while val.endswith('%'):
			val = val[:-1]
		val = float(val)
		if val > 100:
			raise ValueError("invalid percentage: '%s' must be <= 100" % val)
		return val
	#percent()
	
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
	#group.add_argument('--report-knowledge-fingerprint', '--rkf', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
	#		help="include the knowledge database file's fingerprint values in the configuration report, to ensure the same data is used in replication (default: no)"
	#)
	
	# add knowledge database section
	group = parser.add_argument_group("Prior Knowledge Options")
	group.add_argument('--knowledge', '-k', type=str, metavar='file', #default=argparse.SUPPRESS,
			help="the prior knowledge database file to use"
	)
	group.add_argument('--allow-unvalidated-snp-positions', '--ausp', type=yesno, metavar='yes/no', nargs='?', const='yes', default='yes',
			help="use unvalidated SNP positions in the knowledge database (default: yes)"
	)
	group.add_argument('--allow-ambiguous-knowledge', '--aak', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="use ambiguous group<->gene associations in the knowledge database (default: no)"
	)
	group.add_argument('--reduce-ambiguous-knowledge', '--rak', type=str, metavar='no/implication/quality', nargs='?', const='no', default='no',
			choices=['no','implication','quality'],
			help="attempt to reduce ambiguity in the knowledge database using a heuristic strategy, from 'no', 'implication' or 'quality' (default: no)"
	)
	group.add_argument('--ld-profile', '--lp', type=str, metavar='profile', nargs='?', const=None, default=None,
			help="LD profile with which to adjust regions in the knowledge database (default: none)"
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
	group.add_argument('--gene-search', '--gs', type=str, metavar='text', nargs='+', action='append',
			help="find input genes by searching all available names and descriptions"
	)
	group.add_argument('--gene-file', '-G', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input genes"
	)
	group.add_argument('--gene-identifier-type', '--git', type=str, metavar='type', nargs='?', const='-', default='-',
			help="the type of the gene identifiers provided via --gene or --gene-file, or '-' for primary labels (default: primary labels)"
	)
	group.add_argument('--allow-ambiguous-genes', '--aag', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="use ambiguous input gene identifiers by including all possibilities (default: no)"
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
	group.add_argument('--group-search', '--us', type=str, metavar='text', nargs='+', action='append',
			help="find input groups by searching all available names and descriptions"
	)
	group.add_argument('--group-file', '-U', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load input groups"
	)
	group.add_argument('--group-identifier-type', '--uit', type=str, metavar='type', nargs='?', const='-', default='-',
			help="the type of the group identifiers provided via --group or --group-file, or '-' for primary labels (default: primary labels)"
	)
	group.add_argument('--allow-ambiguous-groups', '--aau', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="use ambiguous input group identifiers by including all possibilities (default: no)"
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
	group.add_argument('--alt-gene-search', '--ags', type=str, metavar='text', nargs='+', action='append',
			help="find alternate input genes by searching all available names and descriptions"
	)
	group.add_argument('--alt-gene-file', '--AG', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input genes"
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
	group.add_argument('--alt-group-search', '--aus', type=str, metavar='text', nargs='+', action='append',
			help="find alternate input groups by searching all available names and descriptions"
	)
	group.add_argument('--alt-group-file', '--AU', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input groups"
	)
	group.add_argument('--alt-source', '--ac', type=str, metavar='name', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="alternate input sources, specified by name"
	)
	group.add_argument('--alt-source-file', '--AC', type=str, metavar='file', nargs='+', action='append', #default=argparse.SUPPRESS,
			help="file(s) from which to load alternate input sources"
	)
	
	# add positional section
	group = parser.add_argument_group("Positional Matching Options")
	group.add_argument('--region-position-margin', '--rpm', type=basepairs, metavar='bases', default=0,
			help="number of bases beyond the bounds of known regions where positions should still be matched (default: 0)"
	)
	group.add_argument('--region-match-percent', '--rmp', type=percent, metavar='percentage', default=100,
			help="minimum percentage of overlap between two regions to consider them a match (default: 100)"
	)
	group.add_argument('--region-match-bases', '--rmb', type=basepairs, metavar='bases', default=0,
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
	group.add_argument('--gene-name-stats', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="display statistics on available gene identifier types (default: no)"
	)
	group.add_argument('--group-name-stats', type=yesno, metavar='yes/no', nargs='?', const='yes', default='no',
			help="display statistics on available group identifier types (default: no)"
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
	
	# add hidden options
	parser.add_argument('--end-of-line', action='store_true', help=argparse.SUPPRESS)
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
			raise Exception("ERROR: configuration files include eachother in a loop! %s" % (' -> '.join(cfStack + [cfAbs])))
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
	
	# attach the knowledge file, if provided
	if options.knowledge:
		dbPath = options.knowledge
		if not os.path.exists(dbPath):
			cwdDir = os.path.dirname(os.path.realpath(os.path.abspath(os.getcwd())))
			myDir = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
			if not os.path.samefile(cwdDir, myDir):
				dbPath = os.path.join(myDir, options.knowledge)
				if not os.path.exists(dbPath):
					bio.warn("ERROR: knowledge database file '%s' not found in '%s' or '%s'" % (options.knowledge, cwdDir, myDir))
					sys.exit(1)
			else:
				bio.warn("ERROR: knowledge database file '%s' not found" % (options.knowledge))
				sys.exit(1)
		bio.attachDatabaseFile(dbPath)
	#if knowledge
	
	# define output helper functions
	utf8 = codecs.getencoder('utf8')
	def encodeString(string):
		return utf8(string)[0]
	def encodeLine(line, term="\n"):
		return utf8("%s%s" % (line,term))[0]
	def encodeRow(row, term="\n", delim="\t"):
		return utf8("%s%s" % ((delim.join((col if isinstance(col,basestring) else str('' if col == None else col)) for col in row)),term))[0]
	
	# configuration report
	if options.report_configuration == 'yes':
		outPath = ('<stdout>' if options.stdout == 'yes' else (options.prefix + '.configuration'))
		bio.logPush("writing configuration report to %s ...\n" % outPath)
		if (options.stdout != 'yes') and (options.overwrite != 'yes') and os.path.exists(outPath):
			bio.warn("ERROR: configuration report output file '%s' already exists, must specify --overwrite or a different --prefix\n")
			bio.logPop()
		else:
			outFile = (sys.stdout if options.stdout == 'yes' else open(outPath, 'wb'))
			outFile.write(encodeLine("# Biofilter configuration file"))
			outFile.write(encodeLine("#   generated %s" % time.strftime('%a, %d %b %Y %H:%M:%S')))
			outFile.write(encodeLine("#   Biofilter version %s" % Biofilter.getVersionString()))
			outFile.write(encodeLine("#   LOKI version %s" % loki_db.Database.getVersionString()))
			outFile.write(encodeLine(""))
			#if options.report_knowledge_fingerprint == 'yes':
			#	#TODO
			for opt in options:
				if (opt in ('configuration','end_of_line','debug_query','debug_profile')) or not hasattr(options, opt):
					continue
				val = getattr(options, opt)
				opt = "%-35s" % opt.upper().replace('-','_')
				# three possibilities: simple value, list of simple values, or list of lists of simple values
				if isinstance(val,list) and len(val) and isinstance(val[0],list):
					for subvals in val:
						if len(subvals):
							outFile.write(encodeRow(itertools.chain([opt],subvals), delim=" "))
						else:
							outFile.write(encodeLine(opt))
				elif isinstance(val,list):
					if len(val):
						outFile.write(encodeRow(itertools.chain([opt],val), delim=" "))
					else:
						outFile.write(encodeLine(opt))
				elif val != None:
					outFile.write(encodeRow([opt,val], delim=" "))
			#foreach option
			if outFile != sys.stdout:
				outFile.close()
			bio.logPop("... OK\n")
		#if outPath ok
	#if configuration report
	
	# gene name stats
	if options.gene_name_stats == 'yes':
		outPath = ('<stdout>' if options.stdout == 'yes' else (options.prefix + '.gene-names'))
		bio.logPush("writing gene name statistics to %s ...\n" % outPath)
		if (options.stdout != 'yes') and (options.overwrite != 'yes') and os.path.exists(outPath):
			bio.warn("ERROR: gene name statistics output file '%s' already exists, must specify --overwrite or a different --prefix\n" % outPath)
			bio.logPop()
		else:
			outFile = (sys.stdout if options.stdout == 'yes' else open(outPath, 'wb'))
			outFile.write(encodeRow(['#type','names','unique','ambiguous']))
			for row in bio.generateGeneNameStats():
				outFile.write(encodeRow(row))
			if outFile != sys.stdout:
				outFile.close()
			bio.logPop("... OK\n")
		#if outPath ok
	#if gene name stats
	
	# group name stats
	if options.group_name_stats == 'yes':
		outPath = ('<stdout>' if options.stdout == 'yes' else (options.prefix + '.group-names'))
		bio.logPush("writing group name statistics to %s ...\n" % outPath)
		if (options.stdout != 'yes') and (options.overwrite != 'yes') and os.path.exists(outPath):
			bio.warn("ERROR: group name statistics output file '%s' already exists, must specify --overwrite or a different --prefix\n" % outPath)
			bio.logPop()
		else:
			outFile = (sys.stdout if options.stdout == 'yes' else open(outPath, 'wb'))
			outFile.write(encodeRow(['#type','names','unique','ambiguous']))
			for row in bio.generateGroupNameStats():
				outFile.write(encodeRow(row))
			if outFile != sys.stdout:
				outFile.close()
			bio.logPop("... OK\n")
		#if outPath ok
	#if group name stats
	
	empty = list()
	
	# apply primary filters
	for snpList in (options.snp or empty):
		bio.intersectInputSNPs('main', bio.generateRSesFromText(snpList))
	for snpFileList in (options.snp_file or empty):
		bio.intersectInputSNPs('main', bio.generateRSesFromRSFiles(snpFileList))
	for positionList in (options.position or empty):
		bio.intersectInputLoci('main', bio.generateLociFromText(positionList))
	for positionFileList in (options.position_file or empty):
		bio.intersectInputLoci('main', bio.generateLociFromMapFiles(positionFileList))
	for geneList in (options.gene or empty):
		bio.intersectInputGenes('main', geneList)
	for geneSearch in (options.gene_search or empty):
		bio.intersectInputGeneSearch('main', geneSearch)
	for geneFileList in (options.gene_file or empty):
		bio.intersectInputGenes('main', bio.generateNamesFromNameFiles(geneFileList))
	for regionList in (options.region or empty):
		bio.intersectInputRegions('main', bio.generateRegionsFromText(regionList))
	for regionFileList in (options.region_file or empty):
		bio.intersectInputRegions('main', bio.generateRegionsFromFiles(regionFileList))
	for groupList in (options.group or empty):
		bio.intersectInputGroups('main', groupList)
	for groupSearch in (options.group_search or empty):
		bio.intersectInputGroupSearch('main', groupSearch)
	for groupFileList in (options.group_file or empty):
		bio.intersectInputGroups('main', bio.generateNamesFromNameFiles(groupFileList))
	for sourceList in (options.source or empty):
		bio.intersectInputSources('main', sourceList)
	for sourceFileList in (options.source_file or empty):
		bio.intersectInputSources('main', bio.generateNamesFromNameFiles(sourceFileList))
	
	# apply alternate filters
	for snpList in (options.alt_snp or empty):
		bio.intersectInputSNPs('alt', bio.generateRSesFromText(snpList))
	for snpFileList in (options.alt_snp_file or empty):
		bio.intersectInputSNPs('alt', bio.generateRSesFromRSFiles(snpFileList))
	for positionList in (options.alt_position or empty):
		bio.intersectInputLoci('alt', bio.generateLociFromText(positionList))
	for positionFileList in (options.alt_position_file or empty):
		bio.intersectInputLoci('alt', bio.generateLociFromMapFiles(positionFileList))
	for geneList in (options.alt_gene or empty):
		bio.intersectInputGenes('alt', geneList)
	for geneSearch in (options.alt_gene_search or empty):
		bio.intersectInputGeneSearch('alt', geneSearch)
	for geneFileList in (options.alt_gene_file or empty):
		bio.intersectInputGenes('alt', bio.generateNamesFromNameFiles(geneFileList))
	for regionList in (options.alt_region or empty):
		bio.intersectInputRegions('alt', bio.generateRegionsFromText(regionList))
	for regionFileList in (options.alt_region_file or empty):
		bio.intersectInputRegions('alt', bio.generateRegionsFromFiles(regionFileList))
	for groupList in (options.alt_group or empty):
		bio.intersectInputGroups('alt', groupList)
	for groupSearch in (options.alt_group_search or empty):
		bio.intersectInputGroupSearch('alt', groupSearch)
	for groupFileList in (options.alt_group_file or empty):
		bio.intersectInputGroups('alt', bio.generateNamesFromNameFiles(groupFileList))
	for sourceList in (options.alt_source or empty):
		bio.intersectInputSources('alt', sourceList)
	for sourceFileList in (options.alt_source_file or empty):
		bio.intersectInputSources('alt', bio.generateNamesFromNameFiles(sourceFileList))
	
	# filtering
	for filt in (options.filter or empty):
		outPath = ('<stdout>' if options.stdout == 'yes' else (options.prefix + '.' + '-'.join(filt)))
		bio.logPush("writing '%s' filtered results to %s ...\n" % (' '.join(filt),outPath))
		if (options.stdout != 'yes') and (options.overwrite != 'yes') and os.path.exists(outPath):
			bio.warn("ERROR: '%s' filter output file '%s' already exists, must specify --overwrite or a different --prefix\n" % (' '.join(filt),outPath))
			bio.logPop()
		else:
			outFile = (sys.stdout if options.stdout == 'yes' else open(outPath, 'wb'))
			n = -1 # don't count header
			for row in bio.generateFilterOutput(filt):
				n += 1
				outFile.write(encodeRow(row))
			if outFile != sys.stdout:
				outFile.close()
			bio.logPop("... OK: %d results\n" % n)
		#if outPath ok
	#foreach filter
	
	# annotation
	for anno in (options.annotate or empty):
		n = anno.count(':')
		if n > 1:
			bio.warn("ERROR: cannot annotate '%s', only two sets of outputs are allowed\n" % (' '.join(anno),))
			continue
		elif n == 1:
			i = anno.index(':')
			typesF = anno[:i]
			typesA = anno[i+1:]
		else:
			typesF = anno[0:1]
			typesA = anno[1:]
		
		if not typesA:
			bio.warn("ERROR: cannot annotate '%s' alone\n" % (' '.join(typesF),))
			continue
		outPath = ('<stdout>' if options.stdout == 'yes' else (options.prefix + '.' + '-'.join(typesF) + '.' + '-'.join(typesA)))
		bio.logPush("writing '%s' annotations to %s ...\n" % (' '.join(anno),outPath))
		if (options.stdout != 'yes') and (options.overwrite != 'yes') and os.path.exists(outPath):
			bio.warn("ERROR: '%s' annotation output file '%s' already exists, must specify --overwrite or a different --prefix\n" % (' '.join(anno),outPath))
			bio.logPop()
		else:
			outFile = (sys.stdout if options.stdout == 'yes' else open(outPath, 'wb'))
			n = -1 # don't count header
			for row in bio.generateAnnotationOutput(typesF, typesA):
				n += 1
				outFile.write(encodeRow(row))
			if outFile != sys.stdout:
				outFile.close()
			bio.logPop("... OK: %d results\n" % n)
		#if outPath ok
	#foreach annotation
	
	# modeling output
	for model in (options.model or empty):
		n = model.count(':')
		if n > 1:
			bio.warn("ERROR: cannot model '%s', only two sets of model types are allowed\n" % (' '.join(model),))
			continue
		elif n == 1:
			i = model.index(':')
			typesL = model[:i]
			typesR = model[i+1:]
		else:
			typesL = typesR = model
		
		if typesL == typesR:
			outPath = ('<stdout>' if options.stdout == 'yes' else (options.prefix + '.' + '-'.join(typesL) + '.models'))
		else:
			outPath = ('<stdout>' if options.stdout == 'yes' else (options.prefix + '.' + '-'.join(typesL) + '.' + '-'.join(typesR) + '.models'))
		bio.logPush("writing '%s' models to %s ...\n" % (' '.join(model),outPath))
		if (options.stdout != 'yes') and (options.overwrite != 'yes') and os.path.exists(outPath):
			bio.warn("ERROR: '%s' model output file '%s' already exists, must specify --overwrite or a different --prefix\n" % (' '.join(model),outPath))
			bio.logPop()
		else:
			outFile = (sys.stdout if options.stdout == 'yes' else open(outPath, 'wb'))
			n = -1 # don't count header
			for row in bio.generateModelOutput(typesL, typesR):
				n += 1
				outFile.write(encodeRow(row))
			if outFile != sys.stdout:
				outFile.close()
			bio.logPop("... OK: %d results\n" % n)
		#if outPath ok
	#foreach model
	
#__main__

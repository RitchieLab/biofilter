#!/usr/bin/env python

import argparse
import collections
import itertools
import os
import sys
import time

import loki_db


class Biofilter:
	
	
	##################################################
	# public class data
	
	
	ver_maj,ver_min,ver_rev,ver_dev,ver_date = 2,0,0,'a7','2012-08-20'
	
	
	##################################################
	# private class data
	
	
	_schema = {
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
			
		} #main
	} #_schema{}
	
	_schema['alt'] = _schema['main']
	
	
	##################################################
	# class interrogation
	
	
	@classmethod
	def getVersionString(cls):
		return "%d.%d.%d%s%s (%s)" % (cls.ver_maj, cls.ver_min, cls.ver_rev, ("-" if cls.ver_dev else ""), (cls.ver_dev or ""), cls.ver_date)
	#getVersionString()
	
	
	@classmethod
	def checkMinimumVersion(cls, major=None, minor=None, revision=None, development=None):
		if (major == None) or (cls.ver_maj > major):
			return True
		if cls.ver_maj < major:
			return False
		
		if (minor == None) or (cls.ver_min > minor):
			return True
		if cls.ver_min < minor:
			return False
		
		if (revision == None) or (cls.ver_rev > revision):
			return True
		if cls.ver_rev < revision:
			return False
		
		if (development == None) or (not cls.ver_dev) or (cls.ver_dev > development):
			return True
		if cls.ver_dev < development:
			return False
		
		return True
	#checkMinimumVersion()
	
	
	##################################################
	# constructor
	
	
	def __init__(self):
		# initialize instance properties
		self._iwd = os.getcwd()
		self._logFile = sys.stderr
		self._logIndent = 0
		self._logHanging = False
		self._tablesDeindexed = {db:set() for db in self._schema}
		self._inputFilters  = {db:{tbl:0 for tbl in self._schema[db]} for db in self._schema}
		self._geneModels = None
		
		# initialize instance settings
		self._verbose = False
		self._debugQuery = False
		self._debugProfile = False
		self._snpLociValidated = False
		self._geneStrict = True
		self._groupStrict = True
		self._knowledgeStrict = True
		self._knowledgeScoring = 'basic'
		self._regionLocusTolerance = 0
		self._regionMatchPercent = 100
		self._regionMatchBases = 0
		self._geneNamespace = None
		self._groupNamespace = None
		self._ldprofile = ''
		self._altModelFilter = False
		self._supportedModels = True
		self._monogenicModels = False
		self._maxGroupSize = 30
		self._minModelScore = 2
		self._numModels = None
		self._modelOrder = True
		
		# verify loki_db version (attachTempDatabase() in 2.0.0-a6)
		if not loki_db.Database.checkMinimumVersion(2,0,0,'a6'):
			exit("ERROR: LOKI version 2.0.0-a6 or later required; found %s" % (loki_db.Database.getVersionString(),))
		
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
	
	
	def getVerbose(self):
		return self._verbose
	#getVerbose()
	
	
	def setVerbose(self, verbose=True):
		self._verbose = verbose
	#setVerbose()
	
	
	def log(self, message=""):
		if self._verbose:
			if (self._logIndent > 0) and (not self._logHanging):
				self._logFile.write(self._logIndent * "  ")
				self._logHanging = True
			self._logFile.write(message)
			if (message == "") or (message[-1] != "\n"):
				self._logHanging = True
				self._logFile.flush()
			else:
				self._logHanging = False
		#if _verbose
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
	
	
	##################################################
	# configuration
	
	
	def setDebug(self, query=None, profile=None):
		if query != None:
			self._debugQuery = query
			self.log("debug queries: %s\n" % ("ON" if query else "OFF"))
		if profile != None:
			self._debugProfile = profile
			self.log("debug profiling: %s\n" % ("ON" if profile else "OFF"))
	#setDebug()
	
	
	def setValidatedSNPLoci(self, validated=True):
		self._snpLociValidated = bool(validated)
		self.log("allow unvalidated SNP loci: %s\n" % ("no" if self._snpLociValidated else "yes"))
	#setValidatedSNPLoci()
	
	
	def setStrictGenes(self, strict=True):
		self._geneStrict = bool(strict)
		self.log("allow ambiguous input gene names: %s\n" % ("no" if self._geneStrict else "yes"))
	#setStrictGenes()
	
	
	def setStrictGroups(self, strict=True):
		self._groupStrict = bool(strict)
		self.log("allow ambiguous input group names: %s\n" % ("no" if self._groupStrict else "yes"))
	#setStrictGroups()
	
	
	def setStrictKnowledge(self, strict=True):
		self._knowledgeStrict = bool(strict)
		self.log("allow ambiguous knowledge base associations: %s\n" % ("no" if self._knowledgeStrict else "yes"))
	#setStrictKnowledge()
	
	
	def setKnowledgeScoring(self, method='basic'):
		method = method.lower().strip()
		if 'quality'.startswith(method):
			self._knowledgeScoring = 'quality'
		elif 'implication'.startswith(method):
			self._knowledgeScoring = 'implication'
		else:
			self._knowledgeScoring = 'basic'
		self.log("knowledge base association ambiguity scoring mode: %s\n" % self._knowledgeScoring)
	#setKnowledgeScoring()
	
	
	def setRegionLocusTolerance(self, bases=0):
		self._regionLocusTolerance = int(bases)
		self.log("region-locus match tolerance: %d\n" % self._regionLocusTolerance)
	#setRegionLocusTolerance()
	
	
	def setRegionMatchPercent(self, percent=None):
		self._regionMatchPercent = max(0, min(100, int(percent)))
		self.log("minimum region match percent: %s%%\n" % (self._regionMatchPercent))
	#setRegionMatchPercent()
	
	
	def setRegionMatchBases(self, bases=None):
		self._regionMatchBases = int(bases)
		self.log("minimum region match bases: %d\n" % (self._regionMatchBases,))
	#setRegionMatchBases()
	
	
	def setLDProfile(self, ldprofile=''):
		self._ldprofile = str(ldprofile).strip()
		self.log("LD profile for region-locus matching: %s\n" % (self._ldprofile or "<none>"))
	#setLDProfile()
	
	
	def setGeneNamespace(self, namespace=None):
		self._geneNamespace = None if (namespace == None) else str(namespace).strip()
		self.log("gene name type: %s\n" % ("<label>" if self._geneNamespace == None else (self._geneNamespace or "<any>")))
	#setGeneNamespace()
	
	
	def setGroupNamespace(self, namespace=None):
		self._groupNamespace = None if (namespace == None) else str(namespace).strip()
		self.log("group name type: %s\n" % ("<label>" if self._groupNamespace == None else (self._groupNamespace or "<any>")))
	#setGroupNamespace()
	
	
	def setAlternateModelFiltering(self, alternate=False):
		self._altModelFilter = True if alternate else False
		self.log("alternate model filtering: %s\n" % ("yes" if self._altModelFilter else "no"))
	#setAlternateModelFiltering()
	
	
	def setSupportedModels(self, supported=True):
		self._supportedModels = True if supported else False
		self.log("generate only knowledge-supported models: %s\n" % ("yes" if self._supportedModels else "no"))
	#setSupportedModels()
	
	
	def setMonogenicModels(self, monogenic=False):
		self._monogenicModels = True if monogenic else False
		self.log("allow SNP-SNP models within the same gene: %s\n" % ("yes" if self._monogenicModels else "no"))
	#setMonogenicModels()
	
	
	def setMaximumGroupSize(self, size=0):
		self._maxGroupSize = int(size)
		self.log("maximum modeling group size: %s\n" % (self._maxGroupSize or "<none>"))
	#setMaximumGroupSize()
	
	
	def setMinimumModelScore(self, score=1):
		self._minModelScore = int(score)
		self.log("minimum model score: %s\n" % self._minModelScore)
	#setMinimumModelScore()
	
	
	def setNumModels(self, num=None):
		self._numModels = int(num) or None
		self.log("number of models to generate: %s\n" % (self._numModels or "<unlimited>"))
	#setNumModels()
	
	
	def setModelOrder(self, score=True):
		self._modelOrder = True if score else False
		self.log("model sort order: %s\n" % ("descending score" if self._modelOrder else "none/random"))
	#setModelOrder()
	
	
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
		assert(db in self._schema)
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
		
		# clean up
		self.prepareTableForQuery(db, 'region_zone')
		self.log(" OK\n")
	#updateRegionZones()
	
	
	##################################################
	# input data parsers
	
	
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
	
	
	##################################################
	# snp/locus input
	
	
	def unionInputSNPs(self, db, snps):
		# snps=[ rs, ... ]
		self.log("adding to %s SNP filter ..." % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'snp')
		sql = "INSERT INTO `%s`.`snp` (label,rs) VALUES ('rs'||?,?)" % db
		tally = dict()
		cursor.executemany(sql, self._loki.generateCurrentRSesByRS(snps, tally))
		self.log(" OK: added %d SNPs (%d RS#s merged)\n" % (tally['match']+tally['merge'],tally['merge']))
		
		self._inputFilters[db]['snp'] += 1
	#unionInputSNPs()
	
	
	def intersectInputSNPs(self, db, snps):
		# snps=[ rs, ... ]
		if not self._inputFilters[db]['snp']:
			return self.unionInputSNPs(db, snps)
		self.log("reducing %s SNP filter ..." % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'snp')
		cursor.execute("UPDATE `%s`.`snp` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`snp` SET flag = 1 WHERE (1 OR ?) AND rs = ?" % db
		tally = dict()
		cursor.executemany(sql, self._loki.generateCurrentRSesByRS(snps, tally))
		cursor.execute("DELETE FROM `%s`.`snp` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.log(" OK: kept %d SNPs (%d dropped, %d RS#s merged)\n" % (numBefore-numDrop,numDrop,tally['merge']))
		
		self._inputFilters[db]['snp'] += 1
	#intersectInputSNPs()
	
	
	def unionInputLoci(self, db, loci):
		# loci=[ (label,chr,pos), ... ]
		self.log("adding to %s locus filter ..." % db)
		cursor = self._loki._db.cursor()
		
		# use OR IGNORE to continue on data error, i.e. missing chr or pos
		self.prepareTableForUpdate(db, 'locus')
		sql = "INSERT OR IGNORE INTO `%s`.`locus` (label,chr,pos) VALUES (?,?,?); SELECT LAST_INSERT_ROWID()" % db
		lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, loci):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d loci (%d incomplete)\n" % (numAdd,numNull))
		
		self._inputFilters[db]['locus'] += 1
	#unionInputLoci()
	
	
	def intersectInputLoci(self, db, loci):
		# loci=[ (label,chr,pos), ... ]
		if not self._inputFilters[db]['locus']:
			return self.unionInputLoci(db, loci)
		self.log("reducing %s locus filter ..." % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'locus')
		cursor.execute("UPDATE `%s`.`locus` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`locus` SET flag = 1 WHERE chr = :1 AND pos = :2" % db
		cursor.executemany(sql, loci)
		cursor.execute("DELETE FROM `%s`.`locus` WHERE flag = 0" % db)
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d loci (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['locus'] += 1
	#intersectInputLoci()
	
	
	##################################################
	# region/boundary input
	
	
	def unionInputGenes(self, db, names):
		# names=[ name, ... ]
		self.log("adding to %s gene filter ..." % db)
		cursor = self._loki._db.cursor()
		
		typeID = self._loki.getTypeID('gene')
		if not typeID:
			raise Exception("ERROR: knowledge file contains no gene data")
		
		if self._geneNamespace == None:
			namespaceID = None
		elif not self._geneNamespace:
			namespaceID = 0
		else:
			namespaceID = self._loki.getNamespaceID(self._geneNamespace)
			if not namespaceID:
				raise Exception("ERROR: unknown gene name type '%s'" % (self._geneNamespace,))
		
		self.prepareTableForUpdate(db, 'gene')
		sql = "INSERT INTO `%s`.`gene` (label,biopolymer_id) VALUES (?,?); SELECT 1" % db
		maxMatch = (1 if self._geneStrict else None)
		tally = dict()
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		self.log(" OK: added %d genes (%d matched, %d ambiguous, %d unrecognized)\n" % (numAdd,tally['match'],tally['ambig'],tally['null']))
		
		self._inputFilters[db]['gene'] += 1
	#unionInputGenes()
	
	
	def intersectInputGenes(self, db, names):
		# names=[ name, ... ]
		if not self._inputFilters[db]['gene']:
			return self.unionInputGenes(db, names)
		self.log("reducing %s gene filter ..." % db)
		cursor = self._loki._db.cursor()
		
		typeID = self._loki.getTypeID('gene')
		if not typeID:
			raise Exception("ERROR: knowledge file contains no gene data")
		
		if self._geneNamespace == None:
			namespaceID = None
		elif not self._geneNamespace:
			namespaceID = 0
		else:
			namespaceID = self._loki.getNamespaceID(self._geneNamespace)
			if not namespaceID:
				raise Exception("ERROR: unknown gene name type '%s'" % (self._geneNamespace,))
		
		self.prepareTableForQuery(db, 'gene')
		cursor.execute("UPDATE `%s`.`gene` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		tally = dict()
		sql = "UPDATE `%s`.`gene` SET flag = 1 WHERE (1 OR ?) AND biopolymer_id = ?" % db
		maxMatch = (1 if self._geneStrict else None)
		cursor.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		cursor.execute("DELETE FROM `%s`.`gene` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.log(" OK: kept %d genes (%d dropped, %d ambiguous, %d unrecognized)\n" % (numBefore-numDrop,numDrop,tally['ambig'],tally['null']))
		
		self._inputFilters[db]['gene'] += 1
	#intersectInputGenes()
	
	
	def unionInputRegions(self, db, regions):
		# regions=[ (label,chr,posMin,posMax), ... ]
		self.log("adding to %s region filter ..." % db)
		cursor = self._loki._db.cursor()
		
		# use OR IGNORE to continue on data error, i.e. missing chr or pos
		self.prepareTableForUpdate(db, 'region')
		sql = "INSERT OR IGNORE INTO `%s`.`region` (label,chr,posMin,posMax) VALUES (?,?,?,?); SELECT LAST_INSERT_ROWID()" % db
		lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, regions):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d regions (%d incomplete)\n" % (numAdd,numNull))
		
		self._inputFilters[db]['region'] += 1
	#unionInputRegions()
	
	
	def intersectInputRegions(self, db, regions):
		# regions=[ (label,chr,posMin,posMax), ... ]
		if not self._inputFilters[db]['region']:
			return self.unionInputRegions(db, regions)
		self.log("reducing %s region filter ..." % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'region')
		cursor.execute("UPDATE `%s`.`region` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`region` SET flag = 1 WHERE chr = :1 AND posMin = :2 AND posMax = :3" % db
		cursor.executemany(sql, regions)
		cursor.execute("DELETE FROM `%s`.`region` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.log(" OK: kept %d regions (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['region'] += 1
	#intersectInputRegions()
	
	
	##################################################
	# group input
	
	
	def unionInputGroups(self, db, names, gtype=None):
		# names=[ name, ... ]
		self.log("adding to %s %s filter ..." % (db,(gtype or "group")))
		cursor = self._loki._db.cursor()
		
		typeID = gtype and self._loki.getTypeID(gtype)
		if gtype and not typeID:
			raise Exception("ERROR: unknown group type '%s'" % gtype)
		
		if self._groupNamespace == None:
			namespaceID = None
		elif not self._groupNamespace:
			namespaceID = 0
		else:
			namespaceID = self._loki.getNamespaceID(self._groupNamespace)
			if not namespaceID:
				raise Exception("ERROR: unknown %s name type '%s'" % (gtype or "group",self._groupNamespace))
		
		self.prepareTableForUpdate(db, 'group')
		sql = "INSERT INTO `%s`.`group` (label,group_id) VALUES (?,?); SELECT 1" % db
		maxMatch = (1 if self._groupStrict else None)
		tally = dict()
		numAdd = 0
		for row in cursor.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		self.log(" OK: added %d groups (%d matched, %d ambiguous, %d unrecognized)\n" % (
				numAdd,tally['match'],tally['ambig'],tally['null']
		))
		
		self._inputFilters[db]['group'] += 1
	#unionInputGroups()
	
	
	def intersectInputGroups(self, db, names, gtype=None):
		# names=[ name, ... ]
		if not self._inputFilters[db]['group']:
			return self.unionInputGroups(db, names, gtype)
		self.log("reducing %s %s filter ..." % (db,(gtype or "group")))
		cursor = self._loki._db.cursor()
		
		typeID = gtype and self._loki.getTypeID(gtype)
		if gtype and not typeID:
			raise Exception("ERROR: unknown group type '%s'" % gtype)
		
		if self._groupNamespace == None:
			namespaceID = None
		elif not self._groupNamespace:
			namespaceID = 0
		else:
			namespaceID = self._loki.getNamespaceID(self._groupNamespace)
			if not namespaceID:
				raise Exception("ERROR: unknown %s name type '%s'" % (gtype or "group",self._groupNamespace))
		
		self.prepareTableForQuery(db, 'group')
		cursor.execute("UPDATE `%s`.`group` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		maxMatch = (1 if self._groupStrict else None)
		tally = dict()
		sql = "UPDATE `%s`.`group` SET flag = 1 WHERE group_id = :1" % db
		cursor.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		cursor.execute("DELETE FROM `%s`.`group` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.log(" OK: kept %d groups (%d dropped, %d ambiguous, %d unrecognized)\n" % (
				numBefore-numDrop,numDrop,tally['ambig'],tally['null']
		))
		
		self._inputFilters[db]['group'] += 1
	#intersectGroups()
	
	
	##################################################
	# source input
	
	
	def unionInputSources(self, db, names):
		# names=[ name, ... ]
		self.log("adding to %s source filter ..." % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForUpdate(db, 'source')
		sql = "INSERT OR IGNORE INTO `%s`.`source` (label,source_id) VALUES (?,?); SELECT LAST_INSERT_ROWID()" % db
		lastID = numAdd = numNull = 0
		for row in cursor.executemany(sql, self._loki.getSourceIDs(names).iteritems()):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d sources (%d unrecognized)\n" % (numAdd,numNull))
		
		self._inputFilters[db]['source'] += 1
	#unionInputSources()
	
	
	def intersectInputSources(self, db, names):
		# names=[ name, ... ]
		if not self._inputFilters[db]['source']:
			return self.unionInputSources(db, names)
		self.log("reducing %s source filter ..." % db)
		cursor = self._loki._db.cursor()
		
		self.prepareTableForQuery(db, 'source')
		cursor.execute("UPDATE `%s`.`source` SET flag = 0" % db)
		numBefore = cursor.getconnection().changes()
		sql = "UPDATE `%s`.`source` SET flag = 1 WHERE source_id = :1" % db
		cursor.executemany(sql, self._loki.getSourceIDs(names).iteritems())
		cursor.execute("DELETE FROM `%s`.`source` WHERE flag = 0" % db)
		numDrop = cursor.getconnection().changes()
		self.log(" OK: kept %d sources (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._inputFilters[db]['source'] += 1
	#intersectInputSources()
	
	
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
	# query construction
	
	
	# define table aliases for each actual table: {alias:(db,table),...}
	_queryAliasTables = {
		'm_s'  : ('main','snp'),              # (label,rs)
		'm_l'  : ('main','locus'),            # (label,chr,pos)
		'm_rz' : ('main','region_zone'),      # (region_rowid,chr,zone)
		'm_r'  : ('main','region'),           # (label,chr,posMin,posMax)
		'm_bg' : ('main','gene'),             # (label,biopolymer_id)
		'm_g'  : ('main','group'),            # (label,group_id)
		'm_c'  : ('main','source'),           # (label,source_id)
		'a_s'  : ('alt','snp'),               # (label,rs)
		'a_l'  : ('alt','locus'),             # (label,chr,pos)
		'a_rz' : ('alt','region_zone'),       # (region_rowid,chr,zone)
		'a_r'  : ('alt','region'),            # (label,chr,posMin,posMax)
		'a_bg' : ('alt','gene'),              # (label,biopolymer_id)
		'a_g'  : ('alt','group'),             # (label,group_id)
		'a_c'  : ('alt','source'),            # (label,source_id)
		't_mb' : ('temp','main_biopolymer'),  #TODO
		'd_sl' : ('db','snp_locus'),          # (rs,chr,pos)
		'd_bz' : ('db','biopolymer_zone'),    # (biopolymer_id,chr,zone)
		'd_br' : ('db','biopolymer_region'),  # (biopolymer_id,ldprofile_id,chr,posMin,posMax)
		'd_b'  : ('db','biopolymer'),         # (biopolymer_id,type_id,label)
		'd_gb' : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'd_g'  : ('db','group'),              # (group_id,type_id,label,source_id)
		'd_c'  : ('db','source'),             # (source_id,source)
	} #class._queryAliasTables{}
	
	
	# define aliases which can be joined along a primary path: {alias:{join1,join2,...},...}
	_queryAliasJoinPathEdges = {
		'm_s'  : {'a_s','d_sl'},
		'm_l'  : {'m_rz','a_l','a_rz','d_sl','d_bz'},
		'm_rz' : {'m_l','m_r','a_l','a_rz','d_sl','d_bz'},
		'm_r'  : {'m_rz'},
		'm_bg' : {'a_bg','t_mb','d_br','d_b','d_gb'},#TODO
		'm_g'  : {'a_g','d_gb','d_g'},
		'm_c'  : {'a_c','d_g','d_c'},
		'a_s'  : {'m_s','d_sl'},
		'a_l'  : {'a_rz','m_l','m_rz','d_sl','d_bz'},
		'a_rz' : {'a_l','a_r','m_l','m_rz','d_sl','d_bz'},
		'a_r'  : {'a_rz'},
		'a_bg' : {'m_bg','t_mb','d_br','d_b','d_gb'},#TODO
		'a_g'  : {'m_g','d_gb','d_g'},
		'a_c'  : {'m_c','d_g','d_c'},
		't_mb' : {'m_bg','a_bg','d_br','d_b','d_gb'},#TODO
		'd_sl' : {'d_bz','m_s','m_l','m_rz','a_s','a_l','a_rz'},
		'd_bz' : {'d_sl','d_br','m_l','m_rz','a_l','a_rz'},
		'd_br' : {'d_bz','d_b','d_gb','m_bg','a_bg','t_mb'},#TODO
		'd_b'  : {'d_br','d_gb','m_bg','a_bg','t_mb'},#TODO
		'd_gb' : {'d_br','d_b','d_g','m_bg','m_g','a_bg','a_g','t_mb'},#TODO
		'd_g'  : {'d_gb','d_c','m_g','m_c','a_g','a_c'},
		'd_c'  : {'d_g','m_c','a_c'},
	} #class._queryAliasJoinPathEdges{}
	
	
	# define aliases which can be joined, but are not a primary path: {alias:{join1,join2,...},...}
	_queryAliasJoinConditionEdges = {
		'm_l'  : {'m_r','a_r','d_br'},
		'm_r'  : {'m_l','a_l','a_r','d_sl','d_br'},
		'a_l'  : {'a_r','m_r','d_br'},
		'a_r'  : {'a_l','m_l','m_r','d_sl','d_br'},
		'd_sl' : {'d_br','m_r','a_r'},
		'd_br' : {'d_sl','m_l','m_r','a_l','a_r'},
	} #class._queryAliasJoinConditionEdges{}
	
	
	# make sure all join path edges are symmetric and non-reflexive
	for a1 in _queryAliasJoinPathEdges:
		if a1 not in _queryAliasTables:
			raise Exception("internal struct error: table alias '%s' has join path edges but no table assignment" % (a1))
		for a2 in _queryAliasJoinPathEdges[a1]:
			if a2 not in _queryAliasTables:
				raise Exception("internal struct error: table alias '%s' has join path edges but no table assignment" % (a2))
			elif a1 == a2:
				raise Exception("internal struct error: table alias '%s' has a join path edge to itself" % (a1))
			elif a1 not in _queryAliasJoinPathEdges[a2]:
				raise Exception("internal struct error: table alias '%s' join path edge to '%s' is not symmetric" % (a1,a2))
	
	
	# make sure all join condition edges are symmetric, non-reflexive and non-redundant
	for a1 in _queryAliasJoinConditionEdges:
		if a1 not in _queryAliasTables:
			raise Exception("internal struct error: table alias '%s' has join condition edges but no table assignment" % (a1))
		for a2 in _queryAliasJoinConditionEdges[a1]:
			if a2 not in _queryAliasTables:
				raise Exception("internal struct error: table alias '%s' has join condition edges but no table assignment" % (a2))
			elif a1 == a2:
				raise Exception("internal struct error: table alias '%s' has a join condition edge to itself" % (a1))
			elif a1 not in _queryAliasJoinConditionEdges[a2]:
				raise Exception("internal struct error: table alias '%s' join condition edge to '%s' is not symmetric" % (a1,a2))
			elif a1 in _queryAliasJoinPathEdges and a2 in _queryAliasJoinPathEdges[a1]:
				raise Exception("internal struct error: table alias '%s' join condition edge to '%s' duplicates join path edge" % (a1,a2))
	
	
	# generate all-pairs-shortest-paths
	_queryAliasJoinPaths = { a:{} for a in _queryAliasJoinPathEdges }
	queue = collections.deque()
	for a0 in _queryAliasJoinPathEdges:
		visited = { a0 }
		for a1 in _queryAliasJoinPathEdges[a0]:
			queue.append( [a0,a1] )
			visited.add(a1)
		while queue:
			path = queue.popleft()
			if path[-1] not in _queryAliasJoinPaths[path[0]]:
				_queryAliasJoinPaths[path[0]][path[-1]] = set(path[1:-1])
			for a1 in _queryAliasJoinPathEdges[path[-1]]:
				if a1 not in visited:
					visited.add(a1)
					queue.append( path+[a1] )
	del queue, a0, visited, a1, path
	
	
	# define join constraints for each pair of tables which can be joined: {(db,table):{(db,table):{cond1,cond2,...},...},...}
	# Note that the SQLite optimizer will not use an index on a column
	# which is modified by an expression, even if the condition could
	# be rewritten otherwise (i.e. "colA = colB + 10" will not use an
	# index on colB).  To account for this, all conditions which include
	# expressions must be duplicated so that each operand column appears
	# unmodified (i.e. "colA = colB + 10" and also "colA - 10 = colB").
	_queryTableJoinConditions = {
		('main','snp'): {
			('alt','snp'): {
				"{L}.rs = {R}.rs",
			},
			('db','snp_locus'): {
				"{L}.rs = {R}.rs",
			},
		},
		
		('alt','snp'): {
			('db','snp_locus'): {
				"{L}.rs = {R}.rs",
			},
		},
		
		('main','locus'): {
			('main','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('main','region'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= ({R}.posMin - {rlTolerance})",
				"{L}.pos <= ({R}.posMax + {rlTolerance})",
				"({L}.pos + {rlTolerance}) >= {R}.posMin",
				"({L}.pos - {rlTolerance}) <= {R}.posMax",
			},
			('alt','locus'): {
				"{L}.chr = {R}.chr",
				"{L}.pos = {R}.pos",
			},
			('alt','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('alt','region'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= ({R}.posMin - {rlTolerance})",
				"{L}.pos <= ({R}.posMax + {rlTolerance})",
				"({L}.pos + {rlTolerance}) >= {R}.posMin",
				"({L}.pos - {rlTolerance}) <= {R}.posMax",
			},
			('db','snp_locus'): {
				"{L}.chr = {R}.chr",
				"{L}.pos = {R}.pos",
			},
			('db','biopolymer_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('db','biopolymer_region'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= ({R}.posMin - {rlTolerance})",
				"{L}.pos <= ({R}.posMax + {rlTolerance})",
				"({L}.pos + {rlTolerance}) >= {R}.posMin",
				"({L}.pos - {rlTolerance}) <= {R}.posMax",
			},
		},
		
		('alt','locus'): {
			('alt','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('alt','region'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= ({R}.posMin - {rlTolerance})",
				"{L}.pos <= ({R}.posMax + {rlTolerance})",
				"({L}.pos + {rlTolerance}) >= {R}.posMin",
				"({L}.pos - {rlTolerance}) <= {R}.posMax",
			},
			('main','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('main','region'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= ({R}.posMin - {rlTolerance})",
				"{L}.pos <= ({R}.posMax + {rlTolerance})",
				"({L}.pos + {rlTolerance}) >= {R}.posMin",
				"({L}.pos - {rlTolerance}) <= {R}.posMax",
			},
			('db','snp_locus'): {
				"{L}.chr = {R}.chr",
				"{L}.pos = {R}.pos",
			},
			('db','biopolymer_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('db','biopolymer_region'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= ({R}.posMin - {rlTolerance})",
				"{L}.pos <= ({R}.posMax + {rlTolerance})",
				"({L}.pos + {rlTolerance}) >= {R}.posMin",
				"({L}.pos - {rlTolerance}) <= {R}.posMax",
			},
		},
		
		('main','region_zone'): {
			('main','region'): {
				"{L}.region_rowid = {R}.rowid",
				# with the rowid match, these should all be guaranteed by self.updateRegionZones()
				#"{L}.chr = {R}.chr",
				#"(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
				#"({L}.zone * {zoneSize}) <= {R}.posMax",
				#"{L}.zone >= ({R}.posMin / {zoneSize})",
				#"{L}.zone <= ({R}.posMax / {zoneSize})",
			},
			('alt','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.zone = {R}.zone",
			},
			('db','snp_locus'): {
				"{L}.chr = {R}.chr",
				"(({L}.zone * {zoneSize}) - {rlTolerance}) <= {R}.pos",
				"((({L}.zone + 1) * {zoneSize}) + {rlTolerance}) > {R}.pos",
				"{L}.zone <= (({R}.pos + {rlTolerance}) / {zoneSize})",
				"{L}.zone >= (({R}.pos - {rlTolerance}) / {zoneSize})",
			},
			('db','biopolymer_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.zone = {R}.zone",
			},
		},
		
		('alt','region_zone'): {
			('alt','region'): {
				"{L}.region_rowid = {R}.rowid",
				# with the rowid match, these should all be guaranteed by self.updateRegionZones()
				#"{L}.chr = {R}.chr",
				#"(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
				#"({L}.zone * {zoneSize}) <= {R}.posMax",
				#"{L}.zone >= ({R}.posMin / {zoneSize})",
				#"{L}.zone <= ({R}.posMax / {zoneSize})",
			},
			('db','snp_locus'): {
				"{L}.chr = {R}.chr",
				"(({L}.zone * {zoneSize}) - {rlTolerance}) <= {R}.pos",
				"((({L}.zone + 1) * {zoneSize}) + {rlTolerance}) > {R}.pos",
				"{L}.zone <= (({R}.pos + {rlTolerance}) / {zoneSize})",
				"{L}.zone >= (({R}.pos - {rlTolerance}) / {zoneSize})",
			},
			('db','biopolymer_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.zone = {R}.zone",
			},
		},
		
		('main','region'): {
			('alt','region'): {
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
			},
			('db','snp_locus'): {
				"{L}.chr = {R}.chr",
				"({L}.posMin - {rlTolerance}) <= {R}.pos",
				"({L}.posMax + {rlTolerance}) >= {R}.pos",
				"{L}.posMin <= ({R}.pos + {rlTolerance})",
				"{L}.posMax >= ({R}.pos - {rlTolerance})",
			},
			('db','biopolymer_region'): {
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
			},
		},
		
		('alt','region'): {
			('db','snp_locus'): {
				"{L}.chr = {R}.chr",
				"({L}.posMin - {rlTolerance}) <= {R}.pos",
				"({L}.posMax + {rlTolerance}) >= {R}.pos",
				"{L}.posMin <= ({R}.pos + {rlTolerance})",
				"{L}.posMax >= ({R}.pos - {rlTolerance})",
			},
			('db','biopolymer_region'): {
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
			},
		},
		
		('main','gene'): {
			('alt','gene'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','biopolymer_region'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','group_biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
		},
		
		('alt','gene'): {
			('db','biopolymer_region'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','group_biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
		},
		
		('main','group'): {
			('alt','group'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group_biopolymer'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group'): {
				"{L}.group_id = {R}.group_id",
			},
		},
		
		('alt','group'): {
			('db','group_biopolymer'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group'): {
				"{L}.group_id = {R}.group_id",
			},
		},
		
		('main','source'): {
			('alt','source'): {
				"{L}.source_id = {R}.source_id",
			},
			('db','group'): {
				"{L}.source_id = {R}.source_id",
			},
			('db','source'): {
				"{L}.source_id = {R}.source_id",
			},
		},
		
		('alt','source'): {
			('db','group'): {
				"{L}.source_id = {R}.source_id",
			},
			('db','source'): {
				"{L}.source_id = {R}.source_id",
			},
		},
		
		('db','snp_locus'): {
			('db','biopolymer_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('db','biopolymer_region'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= ({R}.posMin - {rlTolerance})",
				"{L}.pos <= ({R}.posMax + {rlTolerance})",
				"({L}.pos + {rlTolerance}) >= {R}.posMin",
				"({L}.pos - {rlTolerance}) <= {R}.posMax",
			},
		},
		
		('db','biopolymer_zone'): {
			('db','biopolymer_region'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
				"{L}.chr = {R}.chr",
				# verify the zone/region coverage match in case there are two regions on the same chromosome
				"(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
				"({L}.zone * {zoneSize}) <= {R}.posMax",
				"{L}.zone >= ({R}.posMin / {zoneSize})",
				"{L}.zone <= ({R}.posMax / {zoneSize})",
			},
		},
		
		('db','biopolymer_region'): {
			('db','biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','group_biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
		},
		
		('db','biopolymer'): {
			('db','group_biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
		},
		
		('db','group_biopolymer'): {
			('db','group_biopolymer'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group'): {
				"{L}.group_id = {R}.group_id",
			},
		},
		
		('db','group'): {
			('db','source'): {
				"{L}.source_id = {R}.source_id",
			},
		},
		
		('temp','main_biopolymer'): { #TODO
			('main','gene'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('alt','gene'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','biopolymer_region'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
			('db','group_biopolymer'): {
				"{L}.biopolymer_id = {R}.biopolymer_id",
			},
		},
	} #class._queryTableJoinConditions{}
	
	
	##################################################
	# filtering, annotation & modeling
	
	
	def getQueryTemplate(self):
		return {
			'_colname'  : list(), # [ colA, colB, ... ]
			'_colindex' : dict(), # { colA:idxA, colB:idxB, ... }
			'_rowid'    : set(),  # { expA, expB, ... }
			'INSERT'    : None,   # tbl                           => INSERT INTO aliasTable[tbl] (colA,colB,...) ...
			'SELECT'    : dict(), # { colA:expA, colB:expB, ... } => SELECT expA AS colA, expB AS colB, ...
			'FROM'      : set(),  # { tblA, tblB, ... }           => FROM aliasTable[tblA] AS tblA, aliasTable[tblB] AS tblB, ...
			'WHERE'     : set(),  # { expA, expB, ... }           => WHERE expA AND expB AND ...
			'GROUP BY'  : list(), # [ expA, expB, ... ]           => GROUP BY expA, expB, ...
			'HAVING'    : set(),  # { expA, expB, ... }           => HAVING expA AND expB AND ...
			'ORDER BY'  : list(), # [ expA, expB, ... ]           => ORDER BY expA, expB, ...
			'LIMIT'     : None    # num                           => LIMIT INT(num)
		}
	#getQueryTemplate()
	
	
	def addQueryInputs(self, query, main=True, alt=False):
		if main:
			if self._inputFilters['main']['snp']:
				query['FROM'].add('m_s')
			if self._inputFilters['main']['locus']:
				query['FROM'].add('m_l')
			if self._inputFilters['main']['gene']:
				query['FROM'].add('m_bg')
			if self._inputFilters['main']['region']:
				query['FROM'].add('m_r')
			if self._inputFilters['main']['group']:
				query['FROM'].add('m_g')
			if self._inputFilters['main']['source']:
				query['FROM'].add('m_c')
		#if main
		
		if alt:
			if self._inputFilters['alt']['snp']:
				query['FROM'].add('a_s')
			if self._inputFilters['alt']['locus']:
				query['FROM'].add('a_l')
			if self._inputFilters['alt']['gene']:
				query['FROM'].add('a_bg')
			if self._inputFilters['alt']['region']:
				query['FROM'].add('a_r')
			if self._inputFilters['alt']['group']:
				query['FROM'].add('a_g')
			if self._inputFilters['alt']['source']:
				query['FROM'].add('a_c')
		#if alt
	#addQueryInputs()
	
	
	def _addQueryColumn(self, query, column, alias, expression, rowid):
		if column not in query['_colindex']:
			query['_colindex'][column] = len(query['_colname'])
			query['_colname'].append(column)
		query['_rowid'].add(rowid)
		query['SELECT'][column] = expression
		query['FROM'].add(alias)
	#_addQueryColumn()
	
	
	def addQueryOutputs(self, query, columns, main=True, alt=False):
		# there is an index on biopolymer.type_id but in practice it's slower to use it;
		# to prevent the optimizer from doing so, set those filters as "type_id+0 = ?"
		typeID_gene = self._loki.getTypeID('gene')
		if not typeID_gene:
			raise Exception("ERROR: knowledge file contains no gene data")
		
		for col in columns:
			
			if col == 'snp_id':
				if main and ('m_s' in query['FROM']):
					self._addQueryColumn(query, col, 'm_s', "m_s.rs", "m_s.rs")
				elif alt and ('a_s' in query['FROM']):
					self._addQueryColumn(query, col, 'a_s', "a_s.rs", "a_s.rs")
				else:
					self._addQueryColumn(query, col, 'd_sl', "d_sl.rs", "d_sl.rs")
			elif col == 'snp_label':
				if main and ('m_s' in query['FROM']):
					self._addQueryColumn(query, col, 'm_s', "m_s.label", "m_s.rs")
				elif alt and ('a_s' in query['FROM']):
					self._addQueryColumn(query, col, 'a_s', "a_s.label", "a_s.rs")
				else:
					self._addQueryColumn(query, col, 'd_sl', "'rs'||d_sl.rs", "d_sl.rs")
			
			elif col == 'locus_id':
				if main and ('m_l' in query['FROM']):
					self._addQueryColumn(query, col, 'm_l', "m_l.rowid", "m_l.rowid")
				elif alt and ('a_l' in query['FROM']):
					self._addQueryColumn(query, col, 'a_l', "a_l.rowid", "a_l.rowid")
				else:
					self._addQueryColumn(query, col, 'd_sl', "d_sl._ROWID_", "d_sl._ROWID_")
			elif col == 'locus_label':
				if main and ('m_l' in query['FROM']):
					self._addQueryColumn(query, col, 'm_l', "m_l.label", "m_l.rowid")
				elif alt and ('a_l' in query['FROM']):
					self._addQueryColumn(query, col, 'a_l', "a_l.label", "a_l.rowid")
				else:
					self._addQueryColumn(query, col, 'd_sl', "'rs'||d_sl.rs", "d_sl._ROWID_")
			elif col == 'locus_chr':
				if main and ('m_l' in query['FROM']):
					self._addQueryColumn(query, col, 'm_l', "m_l.chr", "m_l.rowid")
				elif alt and ('a_l' in query['FROM']):
					self._addQueryColumn(query, col, 'a_l', "a_l.chr", "a_l.rowid")
				else:
					self._addQueryColumn(query, col, 'd_sl', "d_sl.chr", "d_sl._ROWID_")
			elif col == 'locus_pos':
				if main and ('m_l' in query['FROM']):
					self._addQueryColumn(query, col, 'm_l', "m_l.pos", "m_l.rowid")
				elif alt and ('a_l' in query['FROM']):
					self._addQueryColumn(query, col, 'a_l', "a_l.pos", "a_l.rowid")
				else:
					self._addQueryColumn(query, col, 'd_sl', "d_sl.pos", "d_sl._ROWID_")
			
			elif col == 'gene_id':
				if main and ('m_bg' in query['FROM']):
					self._addQueryColumn(query, col, 'm_bg', "m_bg.biopolymer_id", "m_bg.biopolymer_id")
				elif alt and ('a_bg' in query['FROM']):
					self._addQueryColumn(query, col, 'a_bg', "a_bg.biopolymer_id", "a_bg.biopolymer_id")
				else:
					self._addQueryColumn(query, col, 'd_b', "d_b.biopolymer_id", "d_b.biopolymer_id")
					#TODO
					query['WHERE'].add("d_b.type_id+0 = %d" % typeID_gene)
			elif col == 'gene_label':
				if main and ('m_bg' in query['FROM']):
					self._addQueryColumn(query, col, 'm_bg', "m_bg.label", "m_bg.biopolymer_id")
				elif alt and ('a_bg' in query['FROM']):
					self._addQueryColumn(query, col, 'a_bg', "a_bg.label", "a_bg.biopolymer_id")
				else:
					self._addQueryColumn(query, col, 'd_b', "d_b.label", "d_b.biopolymer_id")
					#TODO
					query['WHERE'].add("d_b.type_id+0 = %d" % typeID_gene)
			
			elif col == 'region_id':
				if main and ('m_r' in query['FROM']):
					self._addQueryColumn(query, col, 'm_r', "m_r.rowid", "m_r.rowid")
				elif alt and ('a_r' in query['FROM']):
					self._addQueryColumn(query, col, 'a_r', "a_r.rowid", "a_r.rowid")
				else:
					self._addQueryColumn(query, col, 'd_br', "d_br._ROWID_", "d_br._ROWID_")
					#TODO
					query['FROM'].add('d_b')
					query['WHERE'].add("d_b.type_id+0 = %d" % typeID_gene)
			elif col == 'region_label':
				if main and ('m_r' in query['FROM']):
					self._addQueryColumn(query, col, 'm_r', "m_r.label", "m_r.rowid")
				elif alt and ('a_r' in query['FROM']):
					self._addQueryColumn(query, col, 'a_r', "a_r.label", "a_r.rowid")
				else:
					self._addQueryColumn(query, col, 'd_b', "d_b.label", "d_br._ROWID_")
					#TODO
					query['WHERE'].add("d_b.type_id+0 = %d" % typeID_gene)
			elif col == 'region_chr':
				if main and ('m_r' in query['FROM']):
					self._addQueryColumn(query, col, 'm_r', "m_r.chr", "m_r.rowid")
				elif alt and ('a_r' in query['FROM']):
					self._addQueryColumn(query, col, 'a_r', "a_r.chr", "a_r.rowid")
				else:
					self._addQueryColumn(query, col, 'd_br', "d_br.chr", "d_br._ROWID_")
					#TODO
					query['FROM'].add('d_b')
					query['WHERE'].add("d_b.type_id+0 = %d" % typeID_gene)
			elif col == 'region_posMin':
				if main and ('m_r' in query['FROM']):
					self._addQueryColumn(query, col, 'm_r', "m_r.posMin", "m_r.rowid")
				elif alt and ('a_r' in query['FROM']):
					self._addQueryColumn(query, col, 'a_r', "a_r.posMin", "a_r.rowid")
				else:
					self._addQueryColumn(query, col, 'd_br', "d_br.posMin", "d_br._ROWID_")
					#TODO
					query['FROM'].add('d_b')
					query['WHERE'].add("d_b.type_id+0 = %d" % typeID_gene)
			elif col == 'region_posMax':
				if main and ('m_r' in query['FROM']):
					self._addQueryColumn(query, col, 'm_r', "m_r.posMax", "m_r.rowid")
				elif alt and ('a_r' in query['FROM']):
					self._addQueryColumn(query, col, 'a_r', "a_r.posMax", "a_r.rowid")
				else:
					self._addQueryColumn(query, col, 'd_br', "d_br.posMax", "d_br._ROWID_")
					#TODO
					query['FROM'].add('d_b')
					query['WHERE'].add("d_b.type_id+0 = %d" % typeID_gene)
			
			elif col == 'group_id':
				if main and ('m_g' in query['FROM']):
					self._addQueryColumn(query, col, 'm_g', "m_g.group_id", "m_g.group_id")
				elif alt and ('a_g' in query['FROM']):
					self._addQueryColumn(query, col, 'a_g', "a_g.group_id", "a_g.group_id")
				elif 'd_gb' in query['FROM']:
					self._addQueryColumn(query, col, 'd_gb', "d_gb.group_id", "d_gb.group_id")
				else:
					self._addQueryColumn(query, col, 'd_g', "d_g.group_id", "d_g.group_id")
			elif col == 'group_label':
				if main and ('m_g' in query['FROM']):
					self._addQueryColumn(query, col, '_mg', "m_g.label", "m_g.group_id")
				elif alt and ('a_g' in query['FROM']):
					self._addQueryColumn(query, col, 'a_g', "a_g.label", "a_g.group_id")
				else:
					self._addQueryColumn(query, col, 'd_g', "d_g.label", "d_g.group_id")
			
			elif col == 'source_id':
				if main and ('m_c' in query['FROM']):
					self._addQueryColumn(query, col, 'm_c', "m_c.source_id", "m_c.source_id")
				elif alt and ('a_c' in query['FROM']):
					self._addQueryColumn(query, col, 'a_c', "a_c.source_id", "a_c.source_id")
				elif 'd_g' in query['FROM']:
					self._addQueryColumn(query, col, 'd_g', "d_g.source_id", "d_g.source_id")
				else:
					self._addQueryColumn(query, col, 'd_c', "d_c.source_id", "d_c.source_id")
			elif col == 'source_label':
				if main and ('m_c' in query['FROM']):
					self._addQueryColumn(query, col, 'm_c', "m_c.label", "m_c.source_id")
				elif alt and ('a_c' in query['FROM']):
					self._addQueryColumn(query, col, 'a_c', "a_c.label", "a_c.source_id")
				else:
					self._addQueryColumn(query, col, 'd_c', "d_c.source", "d_c.source_id")
			
			else:
				raise Exception("ERROR: unsupported column request '%s'" % col)
		#foreach column
	#addQueryOutputs()
	
	
	def addQueryConditions(self, query):
		# include all tables needed to bridge other included tables
		for a0 in self._queryAliasJoinPaths:
			for a1 in self._queryAliasJoinPaths[a0]:
				if (a0 in query['FROM']) and (a1 in query['FROM']):
					query['FROM'].update(self._queryAliasJoinPaths[a0][a1])
		
		# fetch values to insert into conditions
		rlTolerance = self._regionLocusTolerance
		rmPercent = self._regionMatchPercent
		rmBases = self._regionMatchBases
		zoneSize = self._loki.getDatabaseSetting('zone_size')
		zoneSize = int(zoneSize) if zoneSize else None
		ldprofileID = self._loki.getLDProfileID(self._ldprofile)
		
		# add some general constraints for included tables
		if ('d_sl' in query['FROM']) and self._snpLociValidated:
			query['WHERE'].add("d_sl.validated = 1")
		if ('d_br' in query['FROM']):
			query['WHERE'].add("d_br.ldprofile_id = {ldprofileID}".format(ldprofileID=ldprofileID))
		if ('d_gb' in query['FROM']):
			query['WHERE'].add("d_gb.biopolymer_id > 0")
			if self._knowledgeScoring == 'quality':
				query['WHERE'].add("d_gb.quality {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			elif self._knowledgeScoring == 'implication':
				query['WHERE'].add("d_gb.implication {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			else:
				query['WHERE'].add("d_gb.specificity {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
		
		# add join constraints for included table pairs
		for a0 in self._queryAliasJoinPathEdges:
			for a1 in self._queryAliasJoinPathEdges[a0]:
				if (a0 in query['FROM']) and (a1 in query['FROM']):
					t0 = self._queryAliasTables[a0]
					t1 = self._queryAliasTables[a1]
					if (t0 in self._queryTableJoinConditions) and (t1 in self._queryTableJoinConditions[t0]):
						query['WHERE'].update(c.format(
								L=a0, R=a1, rlTolerance=rlTolerance, rmPercent=rmPercent, rmBases=rmBases, zoneSize=zoneSize, ldprofileID=ldprofileID
						) for c in self._queryTableJoinConditions[t0][t1])
		for a0 in self._queryAliasJoinConditionEdges:
			for a1 in self._queryAliasJoinConditionEdges[a0]:
				if (a0 in query['FROM']) and (a1 in query['FROM']):
					t0 = self._queryAliasTables[a0]
					t1 = self._queryAliasTables[a1]
					if (t0 in self._queryTableJoinConditions) and (t1 in self._queryTableJoinConditions[t0]):
						query['WHERE'].update(c.format(
								L=a0, R=a1, rlTolerance=rlTolerance, rmPercent=rmPercent, rmBases=rmBases, zoneSize=zoneSize, ldprofileID=ldprofileID
						) for c in self._queryTableJoinConditions[t0][t1])
	#addQueryConditions()
	
	
	def addQueryIndecies(self, query):
		for a0 in query['FROM']:
			if self._queryAliasTables[a0][0] in self._schema:
				self.prepareTableForQuery(self._queryAliasTables[a0][0], self._queryAliasTables[a0][1])
	#addQueryIndecies()
	
	
	def getQueryText(self, query, noRowIDs=False):
		sql = ""
		if query['INSERT']:
			sql += "INSERT OR REPLACE INTO `{0[0]}`.`{0[1]}`".format(self._queryAliasTables[query['INSERT']])
			sql += " (" + (", ".join(query['_colname'])) + ")\n"
		sql += "SELECT " + (",\n  ".join("{0} AS {1}".format(query['SELECT'][c] or "NULL",c) for c in query['_colname'])) + "\n"
		if not noRowIDs:
			sql += "  , (" + ("||'_'||".join(query['_rowid'])) + ") AS rowid\n"
		if query['FROM']:
			sql += "FROM " + (",\n  ".join("`{0[0]}`.`{0[1]}` AS {1}".format(self._queryAliasTables[a],a) for a in sorted(query['FROM']))) + "\n"
		if query['WHERE']:
			sql += "WHERE " + ("\n  AND ".join(sorted(query['WHERE']))) + "\n"
		if query['GROUP BY']:
			sql += "GROUP BY " + (", ".join(query['GROUP BY'])) + "\n"
		if query['HAVING']:
			sql += "HAVING " + ("\n  AND ".join(sorted(query['HAVING']))) + "\n"
		if query['ORDER BY']:
			sql += "ORDER BY " + (", ".join(query['ORDER BY'])) + "\n"
		if query['LIMIT']:
			sql += "LIMIT " + str(int(query['LIMIT'])) + "\n"
		return sql
	#getQueryText()
	
	
	def generateQueryResults(self, query, allowDupes=False):
		# execute the query and yield the results
		cursor = self._loki._db.cursor()
		sql = self.getQueryText(query, noRowIDs=allowDupes)
		if self._debugQuery:
			self.log(sql+"\n")
			for row in cursor.execute("EXPLAIN QUERY PLAN "+sql):
				self.log(str(row)+"\n")
		elif allowDupes:
			for row in cursor.execute(sql):
				yield row
		else:
			rowIDs = set()
			for row in cursor.execute(sql):
				if row[-1] not in rowIDs:
					rowIDs.add(row[-1])
					yield row[:-1]
			del rowIDs
	#generateQueryResults()
	
	
	def getGeneModels(self):
		# generate the models if we haven't already
		if self._geneModels == None:
			cursor = self._loki._db.cursor()
			
			# find all candidate left-hand genes
			mainGeneFilter = False
			if max(num for tbl,num in self._inputFilters['main'].iteritems()):
				mainGeneFilter = True
				self.log("identifying candidate genes ...")
				cursor.execute("CREATE TEMP TABLE `temp`.`main_biopolymer` (biopolymer_id INTEGER PRIMARY KEY NOT NULL)")
				sql = "INSERT OR IGNORE INTO `temp`.`main_biopolymer` (biopolymer_id) VALUES (?1)"
				query = self.getQueryTemplate()
				self.addQueryInputs(query, main=True, alt=False)
				self.addQueryOutputs(query, ['gene_id'], main=True, alt=False)
				self.addQueryConditions(query)
				self.addQueryIndecies(query)
				cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
				numGenes = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `temp`.`main_biopolymer`"))
				self.log(" OK: %d genes\n" % numGenes)
			#if any main filters
			
			# find all candidate right-hand genes
			altGeneFilter = False
			if max(num for tbl,num in self._inputFilters['alt'].iteritems()):
				altGeneFilter = True
				self.log("identifying alternate candidate genes ...")
				cursor.execute("CREATE TEMP TABLE `temp`.`alt_biopolymer` (biopolymer_id INTEGER PRIMARY KEY NOT NULL)")
				sql = "INSERT OR IGNORE INTO `temp`.`alt_biopolymer` (biopolymer_id) VALUES (?1)"
				query = self.getQueryTemplate()
				self.addQueryInputs(query, main=(not self._altModelFilter), alt=True)
				self.addQueryOutputs(query, ['gene_id'], main=(not self._altModelFilter), alt=True)
				self.addQueryConditions(query)
				self.addQueryIndecies(query)
				cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
				numGenes = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `temp`.`alt_biopolymer`"))
				self.log(" OK: %d genes\n" % numGenes)
			#if any alt filters
			
			# find all candidate groups (by size first, then user input if any)
			self.log("identifying candidate groups ...")
			cursor.execute("CREATE TEMP TABLE `temp`.`group` (group_id INTEGER PRIMARY KEY NOT NULL, flag TINYINT NOT NULL DEFAULT 0)")
			sql = "INSERT OR IGNORE INTO `temp`.`group` (group_id, flag) VALUES (?1, 0)"
			query = self.getQueryTemplate()
			if self._inputFilters['main']['group']:
				self._addQueryColumn(query, 'group_id', 'm_g', "m_g.group_id", "m_g.group_id")
				query['GROUP BY'].append("m_g.group_id")
			else:
				self._addQueryColumn(query, 'group_id', 'd_g', "d_g.group_id", "d_g.group_id")
				query['GROUP BY'].append("d_g.group_id")
			query['FROM'].add('d_gb')
			if self._maxGroupSize:
				query['HAVING'].add("(COUNT(DISTINCT d_gb.biopolymer_id) BETWEEN 2 AND %d)" % self._maxGroupSize)
			else:
				query['HAVING'].add("COUNT(DISTINCT d_gb.biopolymer_id) >= 2")
			self.addQueryConditions(query)
			self.addQueryIndecies(query)
			cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
			if mainGeneFilter:
				#TODO
				sql = "UPDATE `temp`.`group` SET flag = 1 WHERE group_id = ?1"
				query = self.getQueryTemplate()
				self.addQueryInputs(query, main=True, alt=False)
				query['FROM'] -= {'m_s','m_l','m_r'}
				query['FROM'] |= {'t_mb'}
				self.addQueryOutputs(query, ['group_id'], main=True, alt=False)
				self.addQueryConditions(query)
				self.addQueryIndecies(query)
				cursor.executemany(sql, self.generateQueryResults(query, allowDupes=True))
				cursor.execute("DELETE FROM `temp`.`group` WHERE flag = 0")
			#if mainGeneFilter
			
			numGroups = max(row[0] for row in cursor.execute("SELECT COUNT() FROM `temp`.`group`"))
			self.log(" OK: %d groups\n" % numGroups)
			
			# build query components
			sqlSelect = list()
			sqlSelect.append("MIN(d_gb_L.biopolymer_id) AS geneL")
			sqlSelect.append("MAX(d_gb_R.biopolymer_id) AS geneR")
			sqlSelect.append("COUNT(DISTINCT d_g.source_id) AS score1")
			sqlSelect.append("COUNT(DISTINCT d_g.group_id) AS score2")
			sqlFrom = set()
			sqlFrom.add("`temp`.`group` AS t_g")
			sqlFrom.add("`db`.`group` AS d_g")
			sqlFrom.add("`db`.`group_biopolymer` AS d_gb_L")
			sqlFrom.add("`db`.`group_biopolymer` AS d_gb_R")
			sqlWhere = set()
			bgScoreCol = ('specificity' if self._knowledgeScoring == 'basic' else self._knowledgeScoring)
			bgScoreCond = ('>= 100' if self._knowledgeStrict else '> 0')
			sqlWhere.add("d_g.group_id = t_g.group_id")
			sqlWhere.add("d_gb_L.group_id = t_g.group_id")
			sqlWhere.add("d_gb_L.group_id = d_g.group_id")
			sqlWhere.add("d_gb_L.biopolymer_id > 0")
			sqlWhere.add("d_gb_L.{0} {1}".format(bgScoreCol, bgScoreCond))
			sqlWhere.add("d_gb_L.biopolymer_id != d_gb_R.biopolymer_id")
			sqlWhere.add("d_gb_R.group_id = t_g.group_id")
			sqlWhere.add("d_gb_R.group_id = d_g.group_id")
			sqlWhere.add("d_gb_R.biopolymer_id > 0")
			sqlWhere.add("d_gb_R.{0} {1}".format(bgScoreCol, bgScoreCond))
			if mainGeneFilter:
				sqlFrom.add("`temp`.`main_biopolymer` AS t_mb_L")
				sqlWhere.add("d_gb_L.biopolymer_id = t_mb_L.biopolymer_id")
			if altGeneFilter:
				sqlFrom.add("`temp`.`alt_biopolymer` AS t_ab_R")
				sqlWhere.add("d_gb_R.biopolymer_id = t_ab_R.biopolymer_id")
			elif mainGeneFilter and not self._altModelFilter:
				sqlFrom.add("`temp`.`main_biopolymer` AS t_mb_R")
				sqlWhere.add("d_gb_R.biopolymer_id = t_mb_R.biopolymer_id")
			
			# assemble full query
			sql = "SELECT\n  " + (",\n  ".join(sqlSelect)) + "\n"
			sql += "FROM\n  " + (",\n  ".join(sorted(sqlFrom))) + "\n"
			sql += "WHERE\n  " + ("\n  AND ".join(sorted(sqlWhere))) + "\n"
			sql += "GROUP BY MIN(d_gb_L.biopolymer_id, d_gb_R.biopolymer_id), MAX(d_gb_L.biopolymer_id, d_gb_R.biopolymer_id)\n"
			if self._minModelScore:
				sql += "HAVING score1 >= %d\n" % self._minModelScore
			if self._modelOrder:
				sql += "ORDER BY score1 DESC, score2 DESC\n"
			if self._numModels:
				sql += "LIMIT %d" % self._numModels
			
			self._geneModels = list()
			if self._debugQuery:
				self.log(sql+"\n")
				for row in cursor.execute("EXPLAIN QUERY PLAN "+sql):
					self.log(str(row)+"\n")
			else:
				self.log("calculating models ...")
				self._geneModels = list(cursor.execute(sql))
				self.log(" OK: %d models\n" % len(self._geneModels))
			
			cursor.execute("DROP TABLE IF EXISTS `temp`.`main_biopolymer`")
			cursor.execute("DROP TABLE IF EXISTS `temp`.`alt_biopolymer`")
			cursor.execute("DROP TABLE IF EXISTS `temp`.`group`")
		#if no models yet
		
		return self._geneModels
	#getGeneModels()
	
	
	def _populateColumnsFromTypes(self, types, columns=None, header=None):
		if columns == None:
			columns = list()
		for t in types:
			if t == 'snp':
				if header != None:
					header.extend(['snp'])
				columns.extend(['snp_label'])
			elif t == 'locus':
				if header != None:
					header.extend(['chr','locus','pos'])
				columns.extend(['locus_chr','locus_label','locus_pos']) #oddball .map file format
			elif t == 'gene':
				if header != None:
					header.extend(['gene'])
				columns.extend(['gene_label'])
			elif t == 'region':
				if header != None:
					header.extend(['region'])
				columns.extend(['region_chr','region_label','region_posMin','region_posMax'])
			elif t == 'group':
				if header != None:
					header.extend(['group'])
				columns.extend(['group_label'])
			elif t == 'source':
				if header != None:
					header.extend(['source'])
				columns.extend(['source_label'])
			else:
				raise Exception("ERROR: unsupported output type '%s'" % t)
		#foreach types
		return columns
	#_populateColumnsFromTypes()
	
	
	def generateFilterOutput(self, types):
		query = self.getQueryTemplate()
		self.addQueryInputs(query, main=True, alt=False)
		header = list()
		columns = list()
		self._populateColumnsFromTypes(types, columns, header)
		if not columns:
			return
		self.addQueryOutputs(query, columns, main=True, alt=False)
		self.addQueryConditions(query)
		self.addQueryIndecies(query)
		
		self.log("generating filtered output ...")
		n = 0
		header[0] = "#%s" % header[0]
		yield tuple(header)
		for row in self.generateQueryResults(query):
			n += 1
			yield row
		self.log(" OK: %d results\n" % n)
	#generateFilterOutput()
	
	
	def generateModelOutput(self, typesL, typesR):
		cursor = self._loki._db.cursor()
		
		# if we'll need baseline gene models, generate them first
		if self._supportedModels:
			model = self.getGeneModels()
		
		# build query for left-hand model annotation
		headerL = list()
		columnsL = list()
		self._populateColumnsFromTypes(typesL, columnsL, headerL)
		headerL = list(("%s1" % h) for h in headerL)
		if not columnsL:
			return
		if self._supportedModels or not self._monogenicModels:
			columnsL.append('gene_id')
		queryL = self.getQueryTemplate()
		self.addQueryInputs(queryL, main=True, alt=False)
		self.addQueryOutputs(queryL, columnsL, main=True, alt=False)
		if self._supportedModels:
			queryL['WHERE'].add("gene_id = ?1")
			queryL['WHERE'].add("(1 OR ?2 OR ?3 OR ?4)")
		self.addQueryConditions(queryL)
		self.addQueryIndecies(queryL)
		sqlL = self.getQueryText(queryL)
		
		# build query for right-hand model annotation
		headerR = list()
		columnsR = list()
		self._populateColumnsFromTypes(typesR, columnsR, headerR)
		headerR = list(("%s2" % h) for h in headerR)
		if not columnsR:
			return
		if self._supportedModels or not self._monogenicModels:
			columnsR.append('gene_id')
		queryR = self.getQueryTemplate()
		self.addQueryInputs(queryR, main=(not self._altModelFilter), alt=True)
		self.addQueryOutputs(queryR, columnsR, main=(not self._altModelFilter), alt=True)
		if self._supportedModels:
			queryR['WHERE'].add("gene_id = ?2")
			queryR['WHERE'].add("(1 OR ?1 OR ?3 OR ?4)")
		self.addQueryConditions(queryR)
		self.addQueryIndecies(queryR)
		sqlR = self.getQueryText(queryR)
		
		# debug or execute model expansion
		if self._debugQuery:
			self.log(sqlL+"\n")
			self.log("-----\n")
			for row in cursor.execute("EXPLAIN QUERY PLAN "+sqlL, ((1,2,3,4) if self._supportedModels else None)):
				self.log(str(row)+"\n")
			
			self.log("=====\n")
			
			self.log(sqlR+"\n")
			self.log("-----\n")
			for row in cursor.execute("EXPLAIN QUERY PLAN "+sqlR, ((1,2,3,4) if self._supportedModels else None)):
				self.log(str(row)+"\n")
		elif self._supportedModels:
			# expand each gene-gene model
			self.log("outputting models ...")
			n = 0
			headerL[0] = "#%s" % headerL[0]
			headerR.append('score')
			yield tuple(headerL + headerR)
			for model in self.getGeneModels():
				# first expand the right-hand side and store it
				listR = list()
				rowIDs = set()
				for row in cursor.execute(sqlR, model):
					if row[-1] not in rowIDs:
						rowIDs.add(row[-1])
						listR.append(row[:-2] + ('-'.join(str(s) for s in model[2:]),))
				del rowIDs
				
				# now expand the left-hand side and pair each result with the stored right-hand sides
				rowIDs = set()
				for row in cursor.execute(sqlL, model):
					if row[-1] not in rowIDs:
						rowIDs.add(row[-1])
						for modelR in listR:
							n += 1
							yield row[:-2] + modelR
				del rowIDs
			#foreach model
			self.log(" OK: %d models\n" % n)
		else:
			self.log("outputting models ...")
			n = 0
			headerL[0] = "#%s" % headerL[0]
			yield tuple(headerL + headerR)
			
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
			sameCols = (columnsL == columnsR)
			rowCut = -1 if self._monogenicModels else -2
			for row in cursor.execute(sqlL):
				if row[-1] not in rowIDs:
					rowIDs.add(row[-1])
					for modelR in listR:
						if sameCols and row[-1] == modelR[-1]:
							pass
						elif (not self._monogenicModels) and row[-2] == modelR[-2]:
							pass
						else:
							n += 1
							yield row[:rowCut] + modelR[:rowCut]
			del rowIDs
			
			self.log(" OK: %d models\n" % n)
		#if debug/supported/all
	#generateModelOutput()
	
	
#Biofilter


##################################################
# command line interface


if __name__ == "__main__":
	version = "Biofilter version %s" % (Biofilter.getVersionString())
	
	# define arguments
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description=version,
	)
	
	parser.add_argument('--version', action='version',
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
	
	parser.add_argument('--knowledge', '-k', type=str, metavar='file',
			help="the knowledge database file to use"
	)
	
	parser.add_argument('--prime', type=int, metavar='num', nargs='?', default=False,
			help="number of times to 'prime' the knowledge database file into filesystem cache memory"
	)
	
	
	choiceSnpLoci = parser.add_mutually_exclusive_group()
	choiceSnpLoci.add_argument('--validated-snp-loci', '--vsl', action='store_true',
			help="only use validated SNP loci"
	)
	choiceSnpLoci.add_argument('--all-snp-loci', '--asl', action='store_true',
			help="use all SNP loci, validated or not (default)"
	)
	
	choiceAmbigGenes = parser.add_mutually_exclusive_group()
	choiceAmbigGenes.add_argument('--strict-gene-names', '--sgn', action='store_true',
			help="ignore ambiguous input gene names (default)"
	)
	choiceAmbigGenes.add_argument('--all-gene-names', '--agn', action='store_true',
			help="allow ambiguous input gene names by including all possibilities"
	)
	
	choiceAmbigGroups = parser.add_mutually_exclusive_group()
	choiceAmbigGroups.add_argument('--strict-group-names', '--sun', action='store_true',
			help="ignore ambiguous input group names (default)"
	)
	choiceAmbigGroups.add_argument('--all-group-names', '--aun', action='store_true',
			help="allow ambiguous input group names by including all possibilities"
	)
	
	choiceAmbigKnowledge = parser.add_mutually_exclusive_group()
	choiceAmbigKnowledge.add_argument('--strict-knowledge', '--sk', action='store_true',
			help="ignore ambiguous associations in the knowledge database (default)"
	)
	choiceAmbigKnowledge.add_argument('--all-knowledge', '--ak', action='store_true',
			help="allow ambiguous associations in the knowledge base by including all possibilities"
	)
	
	choiceScoreKnowledge = parser.add_mutually_exclusive_group()
	choiceScoreKnowledge.add_argument('--basic-knowledge-scoring', '--bks', action='store_true',
			help="use basic (all or nothing) scoring for ambiguous associations in the knowledge base (default)"
	)
	choiceScoreKnowledge.add_argument('--implication-knowledge-scoring', '--iks', action='store_true',
			help="use implication scoring for ambiguous associations in the knowledge base"
	)
	choiceScoreKnowledge.add_argument('--quality-knowledge-scoring', '--qks', action='store_true',
			help="use quality scoring for ambiguous associations in the knowledge base"
	)
	
	parser.add_argument('--region-locus-tolerance', '--rlt', type=str, metavar='bases',
			help="number of bases beyond the bounds of known regions where SNPs and loci should still be matched (default: 0)"
	)
	
	parser.add_argument('--region-match-percent', '--rmp', type=int, metavar='percentage',
			help="minimum percentage overlap of two regions to consider them a match (default: 100)"
	)
	
	parser.add_argument('--region-match-bases', '--rmb', type=str, metavar='bases',
			help="minimum overlapping bases of two regions to consider them a match (default: not used)"
	)
	
	parser.add_argument('--ld-profile', type=str, metavar='profile', nargs='?', default=False,
			help="LD profile with which to match known regions to SNPs and loci (default: none)"
	)
	
	choiceModelFiltering = parser.add_mutually_exclusive_group()
	choiceModelFiltering.add_argument('--uniform-model-filtering', '--umf', action='store_true',
			help="apply primary input filters to both sides of generated models (default)"
	)
	choiceModelFiltering.add_argument('--alternate-model-filtering', '--amf', action='store_true',
			help="apply primary input filters to only one side of generated models"
	)
	
	choiceSupportedModels = parser.add_mutually_exclusive_group()
	choiceSupportedModels.add_argument('--supported-models', '--sm', action='store_true',
			help="generate only models supported by the knowledge database (default)"
	)
	choiceSupportedModels.add_argument('--all-models', '--am', action='store_true',
			help="generate all pair-wise models"
	)
	
	choiceMonogenicModels = parser.add_mutually_exclusive_group()
	choiceMonogenicModels.add_argument('--polygenic-models', '--pgm', action='store_true',
			help="exclude knowledge-supported SNP-SNP models within the same gene (default)"
	)
	choiceMonogenicModels.add_argument('--monogenic-models', '--mgm', action='store_true',
			help="generate knowledge-supported SNP-SNP models within the same gene"
	)
	
	parser.add_argument('--maximum-model-group-size', '--mmgs', type=int, metavar='size',
			help="maximum size of a group to use for knowledge-supported models (default: 30)"
	)
	
	parser.add_argument('--minimum-model-score', '--mms', type=int, metavar='score',
			help="minimum implication score for knowledge-supported models (default: 2)"
	)
	
	parser.add_argument('--num-models', '--nm', type=int, metavar='num',
			help="maximum number of models to generate, 0 for unlimited (default: unlimited)"
	)
	
	choiceModelOrder = parser.add_mutually_exclusive_group()
	choiceModelOrder.add_argument('--model-order-score', '--mos', action='store_true',
			help="output models in order of descending score (default)",
	)
	choiceModelOrder.add_argument('--model-order-none', '--mon', action='store_true',
			help="output models in no particular order",
	)
	
	
	parser.add_argument('--snp', '-s', type=str, metavar=('rs#'), nargs='+', action='append',
			help="input SNPs, specified by RS#"
	)
	
	parser.add_argument('--alt-snp', '--as', type=str, metavar=('rs#'), nargs='+', action='append',
			help="alternate input SNPs, specified by RS#"
	)
	
	parser.add_argument('--snp-file', '-S', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input SNPs"
	)
	
	parser.add_argument('--alt-snp-file', '--AS', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load alternate input SNPs"
	)
	
	
	parser.add_argument('--locus', '-l', type=str, metavar=('locus'), nargs='+', action='append',
			help="input loci, specified by chromosome and position"
	)
	
	parser.add_argument('--alt-locus', '--al', type=str, metavar=('locus'), nargs='+', action='append',
			help="alternate input loci, specified by chromosome and position"
	)
	
	parser.add_argument('--locus-file', '-L', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input loci"
	)
	
	parser.add_argument('--alt-locus-file', '--AL', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load alternate input loci"
	)
	
	
	parser.add_argument('--gene', '-g', type=str, metavar=('name'), nargs='+', action='append',
			help="input genes, specified by name"
	)
	
	parser.add_argument('--alt-gene', '--ag', type=str, metavar=('name'), nargs='+', action='append',
			help="alternate input genes, specified by name"
	)
	
	parser.add_argument('--gene-file', '-G', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input genes"
	)
	
	parser.add_argument('--alt-gene-file', '--AG', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load alternate input genes"
	)
	
	parser.add_argument('--gene-names', type=str, metavar='type', nargs='?', default=False,
			help="the type of the gene name(s) provided via --gene or --gene-file (default: primary labels)"
	)
	
	
	parser.add_argument('--region', '-r', type=str, metavar=('region'), nargs='+', action='append',
			help="input regions, specified by chromosome, start and stop positions"
	)
	
	parser.add_argument('--alt-region', '--ar', type=str, metavar=('region'), nargs='+', action='append',
			help="alternate input regions, specified by chromosome, start and stop positions"
	)
	
	parser.add_argument('--region-file', '-R', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input regions"
	)
	
	parser.add_argument('--alt-region-file', '--AR', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load alternate input regions"
	)
	
	
	parser.add_argument('--group', '-u', type=str, metavar=('name'), nargs='+', action='append',
			help="input groups, specified by name"
	)
	
	parser.add_argument('--alt-group', '--au', type=str, metavar=('name'), nargs='+', action='append',
			help="alternate input groups, specified by name"
	)
	
	parser.add_argument('--group-file', '-U', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input groups"
	)
	
	parser.add_argument('--alt-group-file', '--AU', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load alternate input groups"
	)
	
	parser.add_argument('--group-names', type=str, metavar='type', nargs='?', default=False,
			help="the type of the group name(s) provided via --group or --group-file (default: primary labels)"
	)
	
	
	parser.add_argument('--source', '-c', type=str, metavar=('name'), nargs='+', action='append',
			help="input sources, specified by name"
	)
	
	parser.add_argument('--alt-source', '--ac', type=str, metavar=('name'), nargs='+', action='append',
			help="alternate input sources, specified by name"
	)
	
	parser.add_argument('--source-file', '-C', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input sources"
	)
	
	parser.add_argument('--alt-source-file', '--AC', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load alternate input sources"
	)
	
	
	parser.add_argument('--prefix', '-p', type=str, metavar='prefix', default='biofilter',
			help="prefix to use for all output filenames; may contain path components (default: 'biofilter')"
	)
	
	parser.add_argument('--overwrite', action='store_true',
			help="overwrite any existing output files",
	)
	
	parser.add_argument('--stdout', action='store_true',
			help="display all output directly on <stdout> rather than writing to any files"
	)
	
	
	parser.add_argument('--gene-name-stats', action='store_true',
			help="output gene name statistics"
	)
	
	parser.add_argument('--group-name-stats', action='store_true',
			help="output group name statistics"
	)
	
	parser.add_argument('--output', '-o', type=str, metavar=('type'), nargs='+', action='append', choices=['snp','locus','gene','region','group','source'],
			help="data type(s) to filter and annotate, from 'snp', 'locus', 'gene', 'region', 'group' and 'source'"
	)
	
	parser.add_argument('--model', '-m', type=str, metavar=('type'), nargs='+', action='append', choices=['snp','locus','gene','region','group','source',':'],
			help="data type(s) to model, from 'snp', 'locus', 'gene', 'region', 'group' and 'source'"
	)
	
	
	parser.add_argument('--verbose', '-v', action='store_true',
			help="print warnings and log messages"
	)
	
	parser.add_argument('--debug-query', action='store_true',
			help="print debugging information about the internal database queries to be used"
	)
	
	parser.add_argument('--debug-profile', action='store_true',
			help="print debugging information about performance profiling"
	)
	
	# if no arguments, print usage and exit
	if len(sys.argv) < 2:
		print version
		print
		parser.print_usage()
		print
		print "Use -h for details."
		sys.exit(2)
	
	# parse arguments and apply basic settings
	args = parser.parse_args()
	bio = Biofilter()
	if args.verbose:
		bio.setVerbose(True)
	if args.debug_query:
		bio.setDebug(query=True)
	if args.debug_profile:
		bio.setDebug(profile=True)
	
	if args.knowledge:
		dbPath = args.knowledge
		if not os.path.exists(dbPath):
			cwdDir = os.path.dirname(os.path.realpath(os.path.abspath(os.getcwd())))
			myDir = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
			if not os.path.samefile(cwdDir, myDir):
				dbPath = os.path.join(myDir, args.knowledge)
				if not os.path.exists(dbPath):
					exit("ERROR: knowledge database file '%s' not found in '%s' or '%s'" % (args.knowledge, cwdDir, myDir))
			else:
				exit("ERROR: knowledge database file '%s' not found" % (args.knowledge))
		if args.prime == None:
			args.prime = 2
		if args.prime:
			readSize = 8*1024*1024
			for p in xrange(args.prime):
				bio.log("priming knowledge database file ...")
				t0 = time.time()
				with open(args.knowledge, 'rb') as kFile:
					while len(kFile.read(readSize)) >= readSize:
						pass
				bio.log(" OK: %1.1f seconds\n" % (time.time()-t0))
		#if prime
		bio.attachDatabaseFile(dbPath)
	
	if args.validated_snp_loci:
		bio.setValidatedSNPLoci(True)
	elif args.all_snp_loci:
		bio.setValidatedSNPLoci(False)
	
	if args.strict_gene_names:
		bio.setStrictGenes(True)
	elif args.all_gene_names:
		bio.setStrictGenes(False)
	
	if args.strict_group_names:
		bio.setStrictGroups(True)
	elif args.all_group_names:
		bio.setStrictGroups(False)
	
	if args.strict_knowledge:
		bio.setStrictKnowledge(True)
	elif args.all_knowledge:
		bio.setStrictKnowledge(False)
	
	if args.basic_knowledge_scoring:
		bio.setKnowledgeScoring('basic')
	elif args.implication_knowledge_scoring:
		bio.setKnowledgeScoring('implication')
	elif args.quality_knowledge_scoring:
		bio.setKnowledgeScoring('quality')
	
	if args.region_locus_tolerance != None:
		n = args.region_locus_tolerance.strip().upper()
		if n[-1:] == 'B':
			n = n[:-1]
		if n[-1] == 'K':
			n = long(n[:-1]) * 1000
		elif n[-1] == 'M':
			n = long(n[:-1]) * 1000 * 1000
		elif n[-1] == 'G':
			n = long(n[:-1]) * 1000 * 1000 * 1000
		else:
			n = long(n)
		bio.setRegionLocusTolerance(n)
	
	if args.region_match_percent != None:
		bio.setRegionMatchPercent(args.region_match_percent)
	
	if args.region_match_bases != None:
		n = args.region_match_bases.strip().upper()
		if n[-1:] == 'B':
			n = n[:-1]
		if n[-1] == 'K':
			n = long(n[:-1]) * 1000
		elif n[-1] == 'M':
			n = long(n[:-1]) * 1000 * 1000
		elif n[-1] == 'G':
			n = long(n[:-1]) * 1000 * 1000 * 1000
		else:
			n = long(n)
		bio.setRegionMatchBases(n)
	
	if args.ld_profile != False:
		bio.setLDProfile(args.ld_profile or '')
	
	if args.uniform_model_filtering:
		bio.setAlternateModelFiltering(False)
	elif args.alternate_model_filtering:
		bio.setAlternateModelFiltering(True)
	
	if args.supported_models:
		bio.setSupportedModels(True)
	elif args.all_models:
		bio.setSupportedModels(False)
	
	if args.polygenic_models:
		bio.setMonogenicModels(False)
	elif args.monogenic_models:
		bio.setMonogenicModels(True)
	
	if args.maximum_model_group_size != None:
		bio.setMaximumGroupSize(args.maximum_model_group_size)
	
	if args.minimum_model_score != None:
		bio.setMinimumModelScore(args.minimum_model_score)
	
	if args.num_models != None:
		bio.setNumModels(args.num_models)
	
	if args.model_order_score:
		bio.setModelOrder(True)
	elif args.model_order_none:
		bio.setModelOrder(False)
	
	if args.gene_names != False:
		bio.setGeneNamespace(args.gene_names or '')
	
	if args.group_names != False:
		bio.setGroupNamespace(args.group_names or '')
	
	
	# apply SNP filters
	if args.snp:
		for snpList in args.snp:
			bio.intersectInputSNPs('main', bio.generateRSesFromText(snpList))
	if args.snp_file:
		for snpFileList in args.snp_file:
			bio.intersectInputSNPs('main', bio.generateRSesFromRSFiles(snpFileList))
	if args.alt_snp:
		for snpList in args.alt_snp:
			bio.intersectInputSNPs('alt', bio.generateRSesFromText(snpList))
	if args.alt_snp_file:
		for snpFileList in args.alt_snp_file:
			bio.intersectInputSNPs('alt', bio.generateRSesFromRSFiles(snpFileList))
	
	# apply locus filters
	if args.locus:
		for locusList in args.locus:
			bio.intersectInputLoci('main', bio.generateLociFromText(locusList))
	if args.locus_file:
		for locusFileList in args.locus_file:
			bio.intersectInputLoci('main', bio.generateLociFromMapFiles(locusFileList))
	if args.alt_locus:
		for locusList in args.alt_locus:
			bio.intersectInputLoci('alt', bio.generateLociFromText(locusList))
	if args.alt_locus_file:
		for locusFileList in args.alt_locus_file:
			bio.intersectInputLoci('alt', bio.generateLociFromMapFiles(locusFileList))
	
	# apply gene filters
	if args.gene:
		for geneList in args.gene:
			bio.intersectInputGenes('main', geneList)
	if args.gene_file:
		for geneFileList in args.gene_file:
			bio.intersectInputGenes('main', bio.generateNamesFromNameFiles(geneFileList))
	if args.alt_gene:
		for geneList in args.alt_gene:
			bio.intersectInputGenes('alt', geneList)
	if args.alt_gene_file:
		for geneFileList in args.alt_gene_file:
			bio.intersectInputGenes('alt', bio.generateNamesFromNameFiles(geneFileList))
	
	# apply region filters
	if args.region:
		for regionList in args.region:
			bio.intersectInputRegions('main', bio.generateRegionsFromText(regionList))
	if args.region_file:
		for regionFileList in args.region_file:
			bio.intersectInputRegions('main', bio.generateRegionsFromFiles(regionFileList))
	if args.alt_region:
		for regionList in args.alt_region:
			bio.intersectInputRegions('alt', bio.generateRegionsFromText(regionList))
	if args.alt_region_file:
		for regionFileList in args.alt_region_file:
			bio.intersectInputRegions('alt', bio.generateRegionsFromFiles(regionFileList))
	
	# apply group filters
	if args.group:
		for groupList in args.group:
			bio.intersectInputGroups('main', groupList)
	if args.group_file:
		for groupFileList in args.group_file:
			bio.intersectInputGroups('main', bio.generateNamesFromNameFiles(groupFileList))
	if args.alt_group:
		for groupList in args.alt_group:
			bio.intersectInputGroups('alt', groupList)
	if args.alt_group_file:
		for groupFileList in args.alt_group_file:
			bio.intersectInputGroups('alt', bio.generateNamesFromNameFiles(groupFileList))
	
	# apply source filters
	if args.source:
		for sourceList in args.source:
			bio.intersectInputSources('main', sourceList)
	if args.source_file:
		for sourceFileList in args.source_file:
			bio.intersectInputSources('main', bio.generateNamesFromNameFiles(sourceFileList))
	if args.alt_source:
		for sourceList in args.alt_source:
			bio.intersectInputSources('alt', sourceList)
	if args.alt_source_file:
		for sourceFileList in args.alt_source_file:
			bio.intersectInputSources('alt', bio.generateNamesFromNameFiles(sourceFileList))
	
	# gene name stats
	if args.gene_name_stats:
		outPath = args.prefix + '.gene-names'
		bio.log("writing gene name statistics to %s ..." % ("<stdout>" if args.stdout else outPath))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			outFile = (sys.stdout if args.stdout else open(outPath, 'w'))
			outFile.write("#type\tnames\tunique\tambiguous\n")
			for row in bio.generateGeneNameStats():
				outFile.write("%s\t%s\t%s\t%s\n" % row)
			if outFile != sys.stdout:
				outFile.close()
			bio.log(" OK\n")
	#if gene-name-stats
	
	# group name stats
	if args.group_name_stats:
		outPath = args.prefix + '.group-names'
		bio.log("writing group name statistics to %s ..." % ("<stdout>" if args.stdout else outPath))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			outFile = (sys.stdout if args.stdout else open(outPath, 'w'))
			outFile.write("#type\tnames\tunique\tambiguous\n")
			for row in bio.generateGroupNameStats():
				outFile.write("%s\t%s\t%s\t%s\n" % row)
			if outFile != sys.stdout:
				outFile.close()
			bio.log(" OK\n")
	#if group-name-stats
	
	# filtering/annotation output
	for output in (args.output or []):
		outPath = args.prefix + '.' + '-'.join(output)
		bio.log("writing %s output to: %s\n" % ('-'.join(output),("<stdout>" if args.stdout else outPath)))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			outFile = (sys.stdout if args.stdout else open(outPath, 'w'))
			for data in bio.generateFilterOutput(output):
				outFile.write("\t".join(str(d) for d in data) + "\n")
			if outFile != sys.stdout:
				outFile.close()
		#if output ok
	#foreach output
	
	# modeling output
	for model in (args.model or []):
		typesL = typesR = None
		if ':' in model:
			i = model.index(':')
			typesL = model[:i]
			typesR = model[i+1:]
			if ':' in typesR:
				bio.log("ERROR: only two sets of model types are allowed\n")
				typesR = None
		else:
			typesL = typesR = model
		
		if typesL and typesR:
			if typesL == typesR:
				outPath = args.prefix + '.' + '-'.join(typesL) + '.models'
				bio.log("writing %s models to: %s\n" % ('-'.join(typesL),("<stdout>" if args.stdout else outPath)))
			else:
				outPath = args.prefix + '.' + '-'.join(typesL) + '.' + '-'.join(typesR) + '.models'
				bio.log("writing %s/%s models to: %s\n" % ('-'.join(typesL),'-'.join(typesR),("<stdout>" if args.stdout else outPath)))
			if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
				bio.log("ERROR: output file '%s' already exists\n" % outPath)
			else:
				outFile = (sys.stdout if args.stdout else open(outPath, 'w'))
				for data in bio.generateModelOutput(typesL, typesR):
					outFile.write("\t".join(str(d) for d in data) + "\n")
				if outFile != sys.stdout:
					outFile.close()
			#if output ok
		#if model ok
	#foreach model
	
#__main__

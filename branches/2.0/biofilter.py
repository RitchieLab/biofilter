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
	
	
	ver_maj,ver_min,ver_rev,ver_dev,ver_date = 2,0,0,'a6','2012-07-31'
	
	
	##################################################
	# private class data
	
	
	_schema = {
		'main': {
			
			'snp': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  rs INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'snp__rs': '(rs)',
				}
			}, #.main.snp
			
			
			'snp_alt': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  rs INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'snp_alt__rs': '(rs)',
				}
			}, #.main.snp_alt
			
			
			'locus': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'locus__pos': '(chr,pos)',
				}
			}, #.main.locus
			
			
			'locus_alt': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'locus_alt__pos': '(chr,pos)',
				}
			}, #.main.locus_alt
			
			
			'gene': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'gene__biopolymer': '(biopolymer_id)',
				}
			}, #.main.gene
			
			
			'gene_alt': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'gene_alt__biopolymer': '(biopolymer_id)',
				}
			}, #.main.gene_alt
			
			
			'region': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'region__chr_min': '(chr,posMin)',
					'region__chr_max': '(chr,posMax)',
				}
			}, #.main.region
			
			
			'region_alt': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'region_alt__chr_min': '(chr,posMin)',
					'region_alt__chr_max': '(chr,posMax)',
				}
			}, #.main.region_alt
			
			
			'region_zone': {
				'table': """
(
  region_rowid INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (chr,zone,region_rowid)
)
""",
				'index': {
					'region_zone__region': '(region_rowid)',
				}
			}, #.main.region_zone
			
			
			'region_zone_alt': {
				'table': """
(
  region_rowid INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (chr,zone,region_rowid)
)
""",
				'index': {
					'region_zone_alt__region': '(region_rowid)',
				}
			}, #.main.region_zone_alt
			
			
			'group': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  group_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'group__group_id': '(group_id)',
				}
			}, #.main.group
			
			
			'group_alt': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  group_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'group_alt__group_id': '(group_id)',
				}
			}, #.main.group_alt
			
			
			'source': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  source_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'source__source_id': '(source_id)',
				}
			}, #.main.source
			
			
			'source_alt': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(32) NOT NULL,
  source_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'source_alt__source_id': '(source_id)',
				}
			}, #.main.source_alt
			
		}, #.main
	} #_schema{}
	
	
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
		
		self._verbose = False
		self._logFile = sys.stderr
		self._logIndent = 0
		self._logHanging = False
		
		self._debug = False
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
		self._minModelScore = 1
		self._numModels = 100
		self._modelOrder = True
		
		self._tablesDeindexed = set()
		self._snpFilters = [0,0]
		self._locusFilters = [0,0]
		self._geneFilters = [0,0]
		self._regionFilters = [0,0]
		self._groupFilters = [0,0]
		self._sourceFilters = [0,0]
		
		# verify loki_db version (generate<X>NameStats() in 2.0.0-a4)
		if not loki_db.Database.checkMinimumVersion(2,0,0,'a4'):
			exit("ERROR: LOKI version 2.0.0-a4 or later required; found %s" % (loki_db.Database.getVersionString(),))
		
		# initialize instance database
		self._loki = loki_db.Database()
		self._loki.setLogger(self)
		self._loki.createDatabaseTables(self._schema['main'], 'main', None, True)
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
	
	
	def setDebug(self, debug=False):
		self._debug = debug
		self.log("debugging mode: %s\n" % ("ON" if debug else "OFF"))
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
	
	
	def prepareTableForUpdate(self, table):
		if table not in self._tablesDeindexed:
			self._tablesDeindexed.add(table)
			self._loki.dropDatabaseIndecies(self._schema['main'], 'main', table)
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, table):
		if table in self._tablesDeindexed:
			self._tablesDeindexed.remove(table)
			self._loki.createDatabaseIndecies(self._schema['main'], 'main', table)
			if table == "region":
				self.updateRegionZones(False)
			elif table == "region_alt":
				self.updateRegionZones(True)
	#prepareTableForQuery()
	
	
	def updateRegionZones(self, alt=False):
		self.log("calculating %sregion zone coverage ..." % ("alternate " if alt else ""))
		
		size = self._loki.getDatabaseSetting('zone_size')
		if not size:
			raise Exception("ERROR: could not determine database setting 'zone_size'")
		size = int(size)
		dbc = self._loki._db.cursor()
		
		# make sure all regions are correctly oriented
		dbc.execute("UPDATE `main`.`region%s` SET posMin = posMax, posMax = posMin WHERE posMin > posMax" % ("_alt" if alt else ""))
		
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
		self.prepareTableForQuery('region%s' % ("_alt" if alt else ""))
		self.prepareTableForUpdate('region_zone%s' % ("_alt" if alt else ""))
		dbc.execute("DELETE FROM `main`.`region_zone%s`" % ("_alt" if alt else ""))
		dbc.executemany(
			"INSERT OR IGNORE INTO `main`.`region_zone%s` (region_rowid,chr,zone) VALUES (?,?,?)" % ("_alt" if alt else ""),
			_zones(
				size,
				self._loki._db.cursor().execute("SELECT rowid,chr,posMin,posMax FROM `main`.`region%s`" % ("_alt" if alt else ""))
			)
		)
		
		# clean up
		self.prepareTableForQuery('region_zone%s' % ("_alt" if alt else ""))
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
	
	
	def unionSNPs(self, snps, alt=False):
		# snps=[ rs, ... ]
		self.log("adding %sSNP filter ..." % ("alternate " if alt else ""))
		self.prepareTableForUpdate('snp%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		
		# for some reason this process (create temp table, insert to temp table,
		# left join merge table while insert-selecting from temp to main table)
		# is ~30% faster than the equivalent one-query solution (left join merge
		# table while insert-selecting from literal "SELECT ? AS rs" subquery)
		dbc.execute("CREATE TEMP TABLE `temp`.`rs` (rs INTEGER PRIMARY KEY)")
		dbc.executemany("INSERT OR IGNORE INTO `temp`.`rs` (rs) VALUES (?)", itertools.izip(snps))
		sql = """
INSERT INTO `main`.`snp%s` (label,rs)
SELECT 'rs'||t_r.rs, COALESCE(d_sm.rsCurrent, t_r.rs)
FROM `temp`.`rs` AS t_r
LEFT JOIN `db`.`snp_merge` AS d_sm
  ON d_sm.rsMerged = t_r.rs
""" % ("_alt" if alt else "")
		dbc.execute(sql)
		numAdd = self._loki._db.changes()
		dbc.execute("DROP TABLE `temp`.`rs`")
		self.log(" OK: added %d RS#s\n" % (numAdd,))
		
		self._snpFilters[1 if alt else 0] += 1
	#unionSNPs()
	
	
	def intersectSNPs(self, snps, alt=False):
		# snps=[ rs, ... ]
		if not self._snpFilters[1 if alt else 0]:
			return self.unionSNPs(snps, alt)
		self.log("intersecting %sSNP filter ..." % ("alternate " if alt else ""))
		dbc = self._loki._db.cursor()
		
		self.prepareTableForQuery('snp%s' % ("_alt" if alt else ""))
		numBefore = max(row[0] for row in dbc.execute("SELECT COUNT() FROM `main`.`snp%s`" % ("_alt" if alt else "")))
		dbc.execute("CREATE TEMP TABLE `temp`.`rs` (rs INTEGER PRIMARY KEY)")
		dbc.executemany("INSERT INTO `temp`.`rs` (rs) VALUES (?)", itertools.izip(snps))
		sql = """
DELETE FROM `main`.`snp%s` WHERE rs NOT IN (
  SELECT m_s.rs
  FROM `temp`.`rs` AS t_r
  LEFT JOIN `db`.`snp_merge` AS d_sm
    ON d_sm.rsMerged = t_r.rs
  JOIN `main`.`snp%s` AS m_s
    ON m_s.rs = COALESCE(d_sm.rsCurrent, t_r.rs)
)
""" % (("_alt" if alt else ""),("_alt" if alt else ""))
		dbc.execute(sql)
		numDrop = self._loki._db.changes()
		dbc.execute("DROP TABLE `temp`.`rs`")
		self.log(" OK: kept %d RS#s (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._snpFilters[1 if alt else 0] += 1
	#intersectSNPs()
	
	
	def unionLoci(self, loci, alt=False):
		# loci=[ (label,chr,pos), ... ]
		self.log("adding %slocus filter ..." % ("alternate " if alt else ""))
		self.prepareTableForUpdate('locus%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		sql = "INSERT OR IGNORE INTO `main`.`locus%s` (label,chr,pos) VALUES (?,?,?); SELECT LAST_INSERT_ROWID()" % ("_alt" if alt else "")
		lastID = None
		numAdd = numNull = 0
		for row in dbc.executemany(sql, loci):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d loci (%d incomplete)\n" % (numAdd,numNull))
		self._locusFilters[1 if alt else 0] += 1
	#unionLoci()
	
	
	def intersectLoci(self, loci, alt=False):
		# loci=[ (label,chr,pos), ... ]
		if not self._locusFilters[1 if alt else 0]:
			return self.unionLoci(loci, alt)
		self.log("intersecting %slocus filter ..." % ("alternate " if alt else ""))
		self.prepareTableForQuery('locus%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`locus%s` SET flag = 0" % ("_alt" if alt else ""))
		numBefore = self._loki._db.changes()
		dbc.executemany("UPDATE `main`.`locus%s` SET flag = 1 WHERE (1 OR ?) AND chr = ? AND pos = ?" % ("_alt" if alt else ""), loci)
		dbc.execute("DELETE FROM `main`.`locus%s` WHERE flag = 0" % ("_alt" if alt else ""))
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d loci (%d dropped)\n" % (numBefore-numDrop,numDrop))
		self._locusFilters[1 if alt else 0] += 1
	#intersectLoci()
	
	
	##################################################
	# region/boundary input
	
	
	def unionGenes(self, names, alt=False):
		# names=[ name, ... ]
		self.log("adding %sgene filter ..." % ("alternate " if alt else ""))
		
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
		
		self.prepareTableForUpdate('gene%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		sql = "INSERT INTO `main`.`gene%s` (label,biopolymer_id) VALUES (?,?); SELECT 1" % ("_alt" if alt else "")
		maxMatch = (1 if self._geneStrict else None)
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		self.log(" OK: added %d genes (%d matched, %d ambiguous, %d unrecognized)\n" % (numAdd,tally['match'],tally['ambig'],tally['null']))
		self._geneFilters[1 if alt else 0] += 1
	#unionGenes()
	
	
	def intersectGenes(self, names, alt=False):
		# names=[ name, ... ]
		if not self._geneFilters[1 if alt else 0]:
			return self.unionGenes(names, alt)
		self.log("intersecting %sgene filter ..." % ("alternate " if alt else ""))
		
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
		
		self.prepareTableForQuery('gene%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`gene%s` SET flag = 0" % ("_alt" if alt else ""))
		numBefore = self._loki._db.changes()
		tally = dict()
		sql = "UPDATE `main`.`gene%s` SET flag = 1 WHERE (1 OR ?) AND biopolymer_id = ?" % ("_alt" if alt else "")
		maxMatch = (1 if self._geneStrict else None)
		dbc.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		dbc.execute("DELETE FROM `main`.`gene%s` WHERE flag = 0" % ("_alt" if alt else ""))
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d genes (%d dropped, %d ambiguous, %d unrecognized)\n" % (numBefore-numDrop,numDrop,tally['ambig'],tally['null']))
		self._geneFilters[1 if alt else 0] += 1
	#intersectGenes()
	
	
	def unionRegions(self, regions, alt=False):
		# regions=[ (label,chr,posMin,posMax), ... ]
		self.log("adding %sregion filter ..." % ("alternate " if alt else ""))
		
		self.prepareTableForUpdate('region%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		
		sql = "INSERT OR IGNORE INTO `main`.`region%s` (label,chr,posMin,posMax) VALUES (?,?,?,?); SELECT LAST_INSERT_ROWID()" % ("_alt" if alt else "")
		lastID = None
		numAdd = numNull = 0
		for row in dbc.executemany(sql, regions):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d regions (%d incomplete)\n" % (numAdd,numNull))
		self._regionFilters[1 if alt else 0] += 1
	#unionRegions()
	
	
	def intersectRegions(self, regions, alt=False):
		# regions=[ (label,chr,posMin,posMax), ... ]
		if not self._regionFilters[1 if alt else 0]:
			return self.unionRegions(regions, alt)
		
		self.log("intersecting %sregion filter ..." % ("alternate " if alt else ""))
		self.prepareTableForQuery('region%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`region%s` SET flag = 0" % ("_alt" if alt else ""))
		numBefore = self._loki._db.changes()
		dbc.executemany("UPDATE `main`.`region%s` SET flag = 1 WHERE (1 OR ?) AND chr = ? AND posMin = ? AND posMax = ?" % ("_alt" if alt else ""), regions)
		dbc.execute("DELETE FROM `main`.`region%s` WHERE flag = 0" % ("_alt" if alt else ""))
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d regions (%d dropped)\n" % (numBefore-numDrop,numDrop))
		self._regionFilters[1 if alt else 0] += 1
	#intersectRegions()
	
	
	##################################################
	# group input
	
	
	def unionGroups(self, names, gtype=None, alt=False):
		# names=[ name, ... ]
		self.log("adding %s%s filter ..." % (("alternate" if alt else ""),(gtype or "group")))
		
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
		
		self.prepareTableForUpdate('group%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		sql = "INSERT INTO `main`.`group%s` (label,group_id) VALUES (?,?); SELECT 1" % ("_alt" if alt else "")
		maxMatch = (1 if self._groupStrict else None)
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		self.log(" OK: added %d groups (%d matched, %d ambiguous, %d unrecognized)\n" % (
				numAdd,tally['match'],tally['ambig'],tally['null']
		))
		self._groupFilters[1 if alt else 0] += 1
	#unionGroups()
	
	
	def intersectGroups(self, names, gtype=None, alt=False):
		# names=[ name, ... ]
		if not self._groupFilters[1 if alt else 0]:
			return self.unionGroups(names, gtype, alt)
		self.log("intersecting %s%s filter ..." % (("alternate" if alt else ""),(gtype or "group")))
		
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
		
		self.prepareTableForQuery('group%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`group%s` SET flag = 0" % ("_alt" if alt else ""))
		numBefore = self._loki._db.changes()
		maxMatch = (1 if self._groupStrict else None)
		tally = dict()
		sql = "UPDATE `main`.`group%s` SET flag = 1 WHERE (1 OR ?) AND group_id = ?" % ("_alt" if alt else "")
		dbc.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		dbc.execute("DELETE FROM `main`.`group%s` WHERE flag = 0" % ("_alt" if alt else ""))
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d groups (%d dropped, %d ambiguous, %d unrecognized)\n" % (
				numBefore-numDrop,numDrop,tally['ambig'],tally['null']
		))
		self._groupFilters[1 if alt else 0] += 1
	#intersectGroups()
	
	
	##################################################
	# source input
	
	
	def unionSources(self, names, alt=False):
		# names=[ name, ... ]
		self.log("adding %ssource filter ..." % ("alternate " if alt else ""))
		
		self.prepareTableForUpdate('source%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		sql = "INSERT OR IGNORE INTO `main`.`source%s` (label,source_id) VALUES (?,?); SELECT LAST_INSERT_ROWID()" % ("_alt" if alt else "")
		lastID = None
		numAdd = numNull = 0
		for row in dbc.executemany(sql, self._loki.getSourceIDs(names).iteritems()):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d sources (%d unrecognized)\n" % (numAdd,numNull))
		self._sourceFilters[1 if alt else 0] += 1
	#unionSources()
	
	
	def intersectSources(self, names, alt=False):
		# names=[ name, ... ]
		if not self._sourceFilters[1 if alt else 0]:
			return self.unionSources(names, alt)
		self.log("intersecting %ssource filter ..." % ("alternate " if alt else ""))
		
		self.prepareTableForQuery('source%s' % ("_alt" if alt else ""))
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`source%s` SET flag = 0" % ("_alt" if alt else ""))
		numBefore = self._loki._db.changes()
		sql = "UPDATE `main`.`source%s` SET flag = 1 WHERE (1 OR ?) AND source_id = ?" % ("_alt" if alt else "")
		dbc.executemany(sql, self._loki.getSourceIDs(names).iteritems())
		dbc.execute("DELETE FROM `main`.`source%s` WHERE flag = 0" % ("_alt" if alt else ""))
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d sources (%d dropped)\n" % (numBefore-numDrop,numDrop))
		self._sourceFilters[1 if alt else 0] += 1
	#intersectSources()
	
	
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
	
	
	# define table aliases and assign them to actual tables: {alias:(db,table)}
	_queryAliasTables = {
		'mf_s'  : ('main','snp'),              # (label,rs)
		'mm_s'  : ('main','snp'),
		'ma_s'  : ('main','snp_alt'),
		'mf_l'  : ('main','locus'),            # (label,chr,pos)
		'mm_l'  : ('main','locus'),
		'ma_l'  : ('main','locus_alt'),
		'mf_rz' : ('main','region_zone'),      # (region_rowid,chr,zone)
		'mm_rz' : ('main','region_zone'),
		'ma_rz' : ('main','region_zone_alt'),
		'mf_r'  : ('main','region'),           # (label,chr,posMin,posMax)
		'mm_r'  : ('main','region'),
		'ma_r'  : ('main','region_alt'),
		'mf_bg' : ('main','gene'),             # (label,biopolymer_id)
		'mm_bg' : ('main','gene'),
		'ma_bg' : ('main','gene_alt'),
		'mf_g'  : ('main','group'),            # (label,group_id)
		'mm_g'  : ('main','group'),
		'ma_g'  : ('main','group_alt'),
		'mf_c'  : ('main','source'),           # (label,source_id)
		'mm_c'  : ('main','source'),
		'ma_c'  : ('main','source_alt'),
		'df_sl' : ('db','snp_locus'),          # (rs,chr,pos)
		'dm_sl' : ('db','snp_locus'),
		'df_bz' : ('db','biopolymer_zone'),    # (biopolymer_id,chr,zone)
		'dm_bz' : ('db','biopolymer_zone'),
		'df_br' : ('db','biopolymer_region'),  # (biopolymer_id,ldprofile_id,chr,posMin,posMax)
		'dm_br' : ('db','biopolymer_region'),
		'df_b'  : ('db','biopolymer'),         # (biopolymer_id,type_id,label)
		'dm_b'  : ('db','biopolymer'),
		'df_gb' : ('db','group_biopolymer'),   # (group_id,biopolymer_id,specificity,implication,quality)
		'dm_gb' : ('db','group_biopolymer'),
		'df_g'  : ('db','group'),              # (group_id,type_id,label,source_id)
		'dm_g'  : ('db','group'),
		'df_c'  : ('db','source'),             # (source_id,source)
		'dm_c'  : ('db','source'),
	}#class._queryAliasTables{}
	
	
	# define aliases which can be joined along a primary path: {alias:{join1,join2}}
	_queryAliasJoinPathEdges = {
		'mf_s'  : {'df_sl'},
		'mm_s'  : {'dm_sl'}
		        | {'ma_s' },
		'ma_s'  : {'dm_sl'}
		        | {'mm_s' },
		'mf_l'  : {'mf_rz','df_sl','df_bz'},
		'mm_l'  : {'mm_rz','dm_sl','dm_bz'}
		        | {'ma_rz','ma_l' },
		'ma_l'  : {'ma_rz','dm_sl','dm_bz'}
		        | {'mm_rz','mm_l' },
		'mf_rz' : {'mf_l' ,'mf_r' ,'df_sl','df_bz'},
		'mm_rz' : {'mm_l' ,'mm_r' ,'dm_sl','dm_bz'}
		        | {'ma_l' ,'ma_rz'},
		'ma_rz' : {'ma_l' ,'ma_r' ,'dm_sl','dm_bz'}
		        | {'mm_l' ,'mm_rz'},
		'mf_r'  : {'mf_rz'},
		'mm_r'  : {'mm_rz'},
		'ma_r'  : {'ma_rz'},
		'mf_bg' : {'df_br','df_b' ,'df_gb'},
		'mm_bg' : {'dm_br','dm_b' ,'dm_gb'}
		        | {'ma_bg'},
		'ma_bg' : {'dm_br','dm_b' ,'dm_gb'}
		        | {'mm_bg'},
		'mf_g'  : {'df_gb','df_g' },
		'mm_g'  : {'dm_gb','dm_g' }
		        | {'ma_g'},
		'ma_g'  : {'dm_gb','dm_g' }
		        | {'mm_g'},
		'mf_c'  : {'df_g' ,'df_c' },
		'mm_c'  : {'dm_g' ,'dm_c' }
		        | {'ma_c'},
		'ma_c'  : {'dm_g' ,'dm_c' }
		        | {'mm_c'},
		'df_sl' : {'mf_s' ,'mf_l' ,'mf_rz','df_bz'},
		'dm_sl' : {'mm_s' ,'mm_l' ,'mm_rz','dm_bz'}
		        | {'ma_s' ,'ma_l' ,'ma_rz'},
		'df_bz' : {'mf_l' ,'mf_rz','df_sl','df_br'},
		'dm_bz' : {'mm_l' ,'mm_rz','dm_sl','dm_br'}
		        | {'ma_l' ,'ma_rz'},
		'df_br' : {'mf_bg','df_bz','df_b' ,'df_gb'},
		'dm_br' : {'mm_bg','dm_bz','dm_b' ,'dm_gb'}
		        | {'ma_bg'},
		'df_b'  : {'mf_bg','df_br','df_gb'},
		'dm_b'  : {'mm_bg','dm_br','dm_gb'}
		        | {'ma_bg'},
		'df_gb' : {'mf_bg','mf_g' ,'df_br','df_b' ,'df_g'},
		'dm_gb' : {'mm_bg','mm_g' ,'dm_br','dm_b' ,'dm_g'}
		        | {'ma_bg','ma_g' },
		'df_g'  : {'mf_g' ,'mf_c' ,'df_gb','df_c' },
		'dm_g'  : {'mm_g' ,'mm_c' ,'dm_gb','dm_c' }
		        | {'ma_g' ,'ma_c' },
		'df_c'  : {'mf_c' ,'df_g' },
		'dm_c'  : {'mm_c' ,'dm_g' }
		        | {'ma_c' },
	}#class._queryAliasJoinPathEdges{}
	
	
	# define aliases which can be joined, but are not a primary path: {alias:{join1,join2}}
	_queryAliasJoinConditionEdges = {
		'mf_l'  : {'mf_r' ,'df_br'},
		'mm_l'  : {'mm_r' ,'ma_r' ,'dm_br'},
		'ma_l'  : {'mm_r' ,'ma_r' ,'dm_br'},
		'mf_r'  : {'mf_l' ,'df_sl','df_br'},
		'mm_r'  : {'mm_l' ,'ma_l' ,'ma_r' ,'dm_sl','dm_br'},
		'ma_r'  : {'mm_l' ,'ma_l' ,'mm_r' ,'dm_sl','dm_br'},
		'df_sl' : {'mf_r' ,'df_br'},
		'dm_sl' : {'mm_r' ,'ma_r' ,'dm_br'},
		'df_br' : {'mf_l' ,'mf_r' ,'df_sl'},
		'dm_br' : {'mm_l' ,'ma_l' ,'mm_r' ,'ma_r' ,'dm_sl'},
	}#class._queryAliasJoinConditionEdges{}
	
	
	# make sure all join path edges are symmetric and non-reflexive
	for a1 in _queryAliasJoinPathEdges:
		if a1 not in _queryAliasTables:
			raise Exception("internal struct error: table alias '%s' has join path edges but no table assignment" % (a1))
		for a2 in _queryAliasJoinPathEdges[a1]:
			if a1 == a2:
				raise Exception("internal struct error: table alias '%s' has a join path edge to itself" % (a1))
			elif a1 not in _queryAliasJoinPathEdges[a2]:
				raise Exception("internal struct error: table alias '%s' join path edge to '%s' is not symmetric" % (a1,a2))
	
	
	# make sure all join condition edges are symmetric, non-reflexive and non-redundant
	for a1 in _queryAliasJoinConditionEdges:
		if a1 not in _queryAliasTables:
			raise Exception("internal struct error: table alias '%s' has join condition edges but no table assignment" % (a1))
		for a2 in _queryAliasJoinConditionEdges[a1]:
			if a1 == a2:
				raise Exception("internal struct error: table alias '%s' has a join condition edge to itself" % (a1))
			elif a1 not in _queryAliasJoinConditionEdges[a2]:
				raise Exception("internal struct error: table alias '%s' join condition edge to '%s' is not symmetric" % (a1,a2))
			elif a1 in _queryAliasJoinPathEdges and a2 in _queryAliasJoinPathEdges[a1]:
				raise Exception("internal struct error: table alias '%s' join condition edge to '%s' duplicates join path edge" % (a1,a2))
	
	
	# define join constraints for each pair of tables which can be joined: {(db,table):{(db,table):{cond1,cond2}}}
	# Note that the SQLite optimizer will not use an index on a column
	# which is modified by an expression, even if the condition could
	# be rewritten otherwise (i.e. "colA = colB + 10" will not use an
	# index on colB).  To account for this, all conditions which include
	# expressions must be duplicated so that each operand column appears
	# unmodified (i.e. "colA = colB + 10" and also "colA - 10 = colB").
	_queryTableJoinConditions = {
		('main','snp'): {
			('main','snp_alt'): {
				"{L}.rs = {R}.rs",
			},
			('db','snp_locus'): {
				"{L}.rs = {R}.rs",
			},
		},
		
		('main','snp_alt'): {
			('db','snp_locus'): {
				"{L}.rs = {R}.rs",
			},
		},
		
		('main','locus'): {
			('main','locus_alt'): {
				"{L}.chr = {R}.chr",
				"{L}.pos = {R}.pos",
			},
			('main','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('main','region_zone_alt'): {
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
			('main','region_alt'): {
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
		
		('main','locus_alt'): {
			('main','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.pos >= (({R}.zone * {zoneSize}) - {rlTolerance})",
				"{L}.pos < ((({R}.zone + 1) * {zoneSize}) + {rlTolerance})",
				"(({L}.pos + {rlTolerance}) / {zoneSize}) >= {R}.zone",
				"(({L}.pos - {rlTolerance}) / {zoneSize}) <= {R}.zone",
			},
			('main','region_zone_alt'): {
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
			('main','region_alt'): {
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
			('main','region_zone_alt'): {
				"{L}.chr = {R}.chr",
				"{L}.zone = {R}.zone",
			},
			('main','region'): {
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
		
		('main','region_zone_alt'): {
			('main','region_alt'): {
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
			('main','region_alt'): {
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
		
		('main','region_alt'): {
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
			('main','gene_alt'): {
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
		
		('main','gene_alt'): {
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
			('main','group_alt'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group_biopolymer'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group'): {
				"{L}.group_id = {R}.group_id",
			},
		},
		
		('main','group_alt'): {
			('db','group_biopolymer'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group'): {
				"{L}.group_id = {R}.group_id",
			},
		},
		
		('main','source'): {
			('main','source_alt'): {
				"{L}.source_id = {R}.source_id",
			},
			('db','group'): {
				"{L}.source_id = {R}.source_id",
			},
			('db','source'): {
				"{L}.source_id = {R}.source_id",
			},
		},
		
		('main','source_alt'): {
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
	}#class._queryTableJoinConditions{}
	
	
	
	##################################################
	# filtering, annotation & modeling
	
	
	def generateResults(self, filterTypes, modelTypes=None):
		filterTypes = set(filterTypes)
		modelTypes = set(modelTypes or [])
		columns = [
			'f_rowid',
			'f_snp_label',
			'f_locus_label', 'f_locus_chr', 'f_locus_pos',
			'f_gene_label',
			'f_region_label', 'f_region_chr', 'f_region_posMin', 'f_region_posMax',
			'f_group_label',
			'f_source_label',
			'm_rowid',
			'm_snp_label',
			'm_locus_label', 'm_locus_chr', 'm_locus_pos',
			'm_gene_label',
			'm_region_label', 'm_region_chr', 'm_region_posMin', 'm_region_posMax',
			'm_group_label',
			'm_source_label',
			'sources',
			'groups'
		]
		
		# initialize query fragments
		sqlFRowID = set() # {expA,expB,...} => SELECT expA||'_'||expB... AS f_rowid, ...
		sqlMRowID = set() # {expA,expB,...} => SELECT expA||'_'||expB... AS m_rowid, ...
		sqlSelect = { c:"NULL" for c in columns } # {colA:expA,colB:expB,...} => SELECT ... expA AS colA, expB AS colB, ...
		sqlFrom = set() # {tblA,tblB,...} => FROM aliasTable[tblA] AS tblA, aliasTable[tblB] AS tblB, ...
		sqlWhere = set() # {expA,expB,...} => WHERE expA AND expB AND ...
		sqlGroup = list() # [expA,expB,...] => GROUP BY expA, expB, ...
		sqlHaving = set() # {expA,expB,...} => HAVING expA AND expB AND ...
		sqlOrder = list() # [expA,expB,...] => ORDER BY expA, expB, ...
		sqlLimit = None # l => LIMIT int(l)
		
		# include all filtering aliases needed to satisfy inputs
		if self._snpFilters[0]:
			sqlFrom.add('mf_s')
		if self._locusFilters[0]:
			sqlFrom.add('mf_l')
		if self._geneFilters[0]:
			sqlFrom.add('mf_bg')
		if self._regionFilters[0]:
			sqlFrom.add('mf_r')
		if self._groupFilters[0]:
			sqlFrom.add('mf_g')
		if self._sourceFilters[0]:
			sqlFrom.add('mf_c')
		
		# include all modeling aliases needed to satisfy inputs
		if modelTypes:
			if self._snpFilters[1]:
				sqlFrom.add('ma_s')
			if self._locusFilters[1]:
				sqlFrom.add('ma_l')
			if self._geneFilters[1]:
				sqlFrom.add('ma_bg')
			if self._regionFilters[1]:
				sqlFrom.add('ma_r')
			if self._groupFilters[1]:
				sqlFrom.add('ma_g')
			if self._sourceFilters[1]:
				sqlFrom.add('ma_c')
			
			if not self._altModelFilter:
				if self._snpFilters[0]:
					sqlFrom.add('mm_s')
				if self._locusFilters[0]:
					sqlFrom.add('mm_l')
				if self._geneFilters[0]:
					sqlFrom.add('mm_bg')
				if self._regionFilters[0]:
					sqlFrom.add('mm_r')
				if self._groupFilters[0]:
					sqlFrom.add('mm_g')
				if self._sourceFilters[0]:
					sqlFrom.add('mm_c')
		#if modelTypes
		
		# include all filtering aliases and columns needed to satisfy output
		if 'snps' in filterTypes:
			if 'mf_s' in sqlFrom:
				sqlFRowID.add("mf_s.rs")
				sqlSelect['f_snp_label'] = "mf_s.label"
			else:
				sqlFrom.add('df_sl')
				sqlFRowID.add("df_sl.rs")
				sqlSelect['f_snp_label'] = "'rs'||df_sl.rs"
		
		if 'loci' in filterTypes:
			if 'mf_l' in sqlFrom:
				sqlFRowID.add("mf_l.rowid")
				sqlSelect['f_locus_label'] = "mf_l.label"
				sqlSelect['f_locus_chr'] = "mf_l.chr"
				sqlSelect['f_locus_pos'] = "mf_l.pos"
			else:
				sqlFrom.add('df_sl')
				sqlFRowID.add("df_sl._ROWID_")
				sqlSelect['f_locus_label'] = "'rs'||df_sl.rs"
				sqlSelect['f_locus_chr'] = "df_sl.chr"
				sqlSelect['f_locus_pos'] = "df_sl.pos"
		
		if 'genes' in filterTypes:
			if 'mf_bg' in sqlFrom:
				sqlFRowID.add("mf_bg.biopolymer_id")
				sqlSelect['f_gene_label'] = "mf_bg.label"
			else:
				sqlFrom.add('df_b')
				sqlFRowID.add("df_b.biopolymer_id")
				sqlSelect['f_gene_label'] = "df_b.label"
		
		if 'regions' in filterTypes:
			if 'mf_r' in sqlFrom:
				sqlFRowID.add("mf_r.rowid")
				sqlSelect['f_region_label'] = "mf_r.label"
				sqlSelect['f_region_chr'] = "mf_r.chr"
				sqlSelect['f_region_posMin'] = "mf_r.posMin"
				sqlSelect['f_region_posMax'] = "mf_r.posMax"
			else:
				sqlFrom.add('df_b')
				sqlFrom.add('df_br')
				sqlFRowID.add("df_br._ROWID_")
				sqlSelect['f_region_label'] = "df_b.label"
				sqlSelect['f_region_chr'] = "df_br.chr"
				sqlSelect['f_region_posMin'] = "df_br.posMin"
				sqlSelect['f_region_posMax'] = "df_br.posMax"
		
		if 'groups' in filterTypes:
			if 'mf_g' in sqlFrom:
				sqlFRowID.add("mf_g.group_id")
				sqlSelect['f_group_label'] = "mf_g.label"
			else:
				sqlFrom.add('df_g')
				sqlFRowID.add("df_g.group_id")
				sqlSelect['f_group_label'] = "df_g.label"
		
		if 'sources' in filterTypes:
			if 'mf_c' in sqlFrom:
				sqlFRowID.add("mf_c.source_id")
				sqlSelect['f_source_label'] = "mf_c.label"
			else:
				sqlFrom.add('df_c')
				sqlFRowID.add("df_c.source_id")
				sqlSelect['f_source_label'] = "df_c.source"
		
		# include all modeling aliases and columns needed to satisfy output
		if 'snps' in modelTypes:
			if 'ma_s' in sqlFrom:
				sqlMRowID.add("ma_s.rs")
				sqlSelect['m_snp_label'] = "ma_s.label"
			elif ('mm_s' in sqlFrom) and not self._altModelFilter:
				sqlMRowID.add("mm_s.rs")
				sqlSelect['m_snp_label'] = "mm_s.label"
			else:
				sqlFrom.add('dm_sl')
				sqlMRowID.add("dm_sl.rs")
				sqlSelect['m_snp_label'] = "'rs'||dm_sl.rs"
		
		if 'loci' in modelTypes:
			if 'ma_l' in sqlFrom:
				sqlMRowID.add("ma_l.rowid")
				sqlSelect['m_locus_label'] = "ma_l.label"
				sqlSelect['m_locus_chr'] = "ma_l.chr"
				sqlSelect['m_locus_pos'] = "ma_l.pos"
			elif ('mm_l' in sqlFrom) and not self._altModelFilter:
				sqlMRowID.add("mm_l.rowid")
				sqlSelect['m_locus_label'] = "mm_l.label"
				sqlSelect['m_locus_chr'] = "mm_l.chr"
				sqlSelect['m_locus_pos'] = "mm_l.pos"
			else:
				sqlFrom.add('dm_sl')
				sqlMRowID.add("dm_sl._ROWID_")
				sqlSelect['m_locus_label'] = "'rs'||dm_sl.rs"
				sqlSelect['m_locus_chr'] = "dm_sl.chr"
				sqlSelect['m_locus_pos'] = "dm_sl.pos"
		
		if 'genes' in modelTypes:
			if 'ma_bg' in sqlFrom:
				sqlMRowID.add("ma_bg.biopolymer_id")
				sqlSelect['m_gene_label'] = "ma_bg.label"
			elif ('mm_bg' in sqlFrom) and not self._altModelFilter:
				sqlMRowID.add("mm_bg.biopolymer_id")
				sqlSelect['m_gene_label'] = "mm_bg.label"
			else:
				sqlFrom.add('dm_b')
				sqlMRowID.add("dm_b.biopolymer_id")
				sqlSelect['m_gene_label'] = "dm_b.label"
		
		if 'regions' in modelTypes:
			if 'ma_r' in sqlFrom:
				sqlMRowID.add("ma_r.rowid")
				sqlSelect['m_region_label'] = "ma_r.label"
				sqlSelect['m_region_chr'] = "ma_r.chr"
				sqlSelect['m_region_posMin'] = "ma_r.posMin"
				sqlSelect['m_region_posMax'] = "ma_r.posMax"
			elif ('mm_r' in sqlFrom) and not self._altModelFilter:
				sqlMRowID.add("mm_r.rowid")
				sqlSelect['m_region_label'] = "mm_r.label"
				sqlSelect['m_region_chr'] = "mm_r.chr"
				sqlSelect['m_region_posMin'] = "mm_r.posMin"
				sqlSelect['m_region_posMax'] = "mm_r.posMax"
			else:
				sqlFrom.add('dm_b')
				sqlFrom.add('dm_br')
				sqlMRowID.add("dm_br._ROWID_")
				sqlSelect['m_region_label'] = "dm_b.label"
				sqlSelect['m_region_chr'] = "dm_br.chr"
				sqlSelect['m_region_posMin'] = "dm_br.posMin"
				sqlSelect['m_region_posMax'] = "dm_br.posMax"
		
		if 'groups' in modelTypes:
			if 'ma_g' in sqlFrom:
				sqlMRowID.add("ma_g.group_id")
				sqlSelect['m_group_label'] = "ma_g.label"
			elif ('mm_g' in sqlFrom) and not self._altModelFilter:
				sqlMRowID.add("mm_g.group_id")
				sqlSelect['m_group_label'] = "mm_g.label"
			else:
				sqlFrom.add('dm_g')
				sqlMRowID.add("dm_g.group_id")
				sqlSelect['m_group_label'] = "dm_g.label"
		
		if 'sources' in modelTypes:
			if 'ma_c' in sqlFrom:
				sqlMRowID.add("ma_c.source_id")
				sqlSelect['m_source_label'] = "ma_c.label"
			elif ('mm_c' in sqlFrom) and not self._altModelFilter:
				sqlMRowID.add("mm_c.source_id")
				sqlSelect['m_source_label'] = "mm_c.label"
			else:
				sqlFrom.add('dm_c')
				sqlMRowID.add("dm_c.source_id")
				sqlSelect['m_source_label'] = "dm_c.source"
		
		# add scores and group/having/order for knowledge-supported models
		if modelTypes and self._supportedModels:
			sqlFrom.add('df_g')
			sqlSelect['sources'] = "COUNT(DISTINCT df_g.source_id)"
			sqlSelect['groups'] = "COUNT(DISTINCT df_g.group_id)"
			
			sqlFrom.add('df_gb')
			sqlFrom.add('dm_gb')
			sqlWhere.add("df_gb.group_id = dm_gb.group_id")
			if not self._monogenicModels:
				sqlWhere.add("df_gb.biopolymer_id != dm_gb.biopolymer_id")
			
			sqlGroup.extend(sqlFRowID)
			sqlGroup.extend(sqlMRowID)
			sqlHaving.add("sources >= %d" % self._minModelScore)
			if filterTypes == modelTypes:
				sqlHaving.add("f_rowid != m_rowid")
			if self._modelOrder:
				sqlOrder.extend(['sources DESC','groups DESC'])
		#if supportedModels
		
		# add model limit
		if modelTypes and self._numModels:
			sqlLimit = int(self._numModels)
		
		# generate all-pairs-shortest-paths
		paths = { a:{} for a in self._queryAliasJoinPathEdges }
		queue = collections.deque()
		for a0 in self._queryAliasJoinPathEdges:
			visited = { a0 }
			for a1 in self._queryAliasJoinPathEdges[a0]:
				queue.append( [a0,a1] )
				visited.add(a1)
			while queue:
				path = queue.popleft()
				if path[-1] not in paths[path[0]]:
					paths[path[0]][path[-1]] = set(path[1:-1])
				for a1 in self._queryAliasJoinPathEdges[path[-1]]:
					if a1 not in visited:
						visited.add(a1)
						queue.append( path+[a1] )
		
		# include all tables needed to bridge other included tables
		for a0 in paths:
			for a1 in paths[a0]:
				if (a0 in sqlFrom) and (a1 in sqlFrom):
					sqlFrom.update(paths[a0][a1])
		
		# fetch values to insert into conditions
		rlTolerance = self._regionLocusTolerance
		rmPercent = self._regionMatchPercent
		rmBases = self._regionMatchBases
		zoneSize = self._loki.getDatabaseSetting('zone_size')
		zoneSize = int(zoneSize) if zoneSize else None
		ldprofileID = self._loki.getLDProfileID(self._ldprofile)
		
		# add some general constraints for included tables
		if ('df_sl' in sqlFrom) and self._snpLociValidated:
			sqlWhere.add("df_sl.validated = 1")
		if ('dm_sl' in sqlFrom) and self._snpLociValidated:
			sqlWhere.add("dm_sl.validated = 1")
		if ('df_br' in sqlFrom):
			sqlWhere.add("df_br.ldprofile_id = {ldprofileID}".format(ldprofileID=ldprofileID))
		if ('dm_br' in sqlFrom):
			sqlWhere.add("dm_br.ldprofile_id = {ldprofileID}".format(ldprofileID=ldprofileID))
		if ('df_gb' in sqlFrom):
			sqlWhere.add("df_gb.biopolymer_id > 0")
			if self._knowledgeScoring == 'quality':
				sqlWhere.add("df_gb.quality {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			elif self._knowledgeScoring == 'implication':
				sqlWhere.add("df_gb.implication {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			else:
				sqlWhere.add("df_gb.specificity {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
		if ('dm_gb' in sqlFrom):
			sqlWhere.add("dm_gb.biopolymer_id > 0")
			if self._knowledgeScoring == 'quality':
				sqlWhere.add("dm_gb.quality {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			elif self._knowledgeScoring == 'implication':
				sqlWhere.add("dm_gb.implication {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			else:
				sqlWhere.add("dm_gb.specificity {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
		
		# add join constraints for included table pairs
		for a0 in self._queryAliasJoinPathEdges:
			for a1 in self._queryAliasJoinPathEdges[a0]:
				if (a0 in sqlFrom) and (a1 in sqlFrom):
					t0 = self._queryAliasTables[a0]
					t1 = self._queryAliasTables[a1]
					if (t0 in self._queryTableJoinConditions) and (t1 in self._queryTableJoinConditions[t0]):
						sqlWhere.update(c.format(
								L=a0, R=a1, rlTolerance=rlTolerance, rmPercent=rmPercent, rmBases=rmBases, zoneSize=zoneSize, ldprofileID=ldprofileID
						) for c in self._queryTableJoinConditions[t0][t1])
		for a0 in self._queryAliasJoinConditionEdges:
			for a1 in self._queryAliasJoinConditionEdges[a0]:
				if (a0 in sqlFrom) and (a1 in sqlFrom):
					t0 = self._queryAliasTables[a0]
					t1 = self._queryAliasTables[a1]
					if (t0 in self._queryTableJoinConditions) and (t1 in self._queryTableJoinConditions[t0]):
						sqlWhere.update(c.format(
								L=a0, R=a1, rlTolerance=rlTolerance, rmPercent=rmPercent, rmBases=rmBases, zoneSize=zoneSize, ldprofileID=ldprofileID
						) for c in self._queryTableJoinConditions[t0][t1])
		
		# make sure any included filter tables are indexed
		for a0 in sqlFrom:
			if self._queryAliasTables[a0][0] == 'main':
				self.prepareTableForQuery(self._queryAliasTables[a0][1])
		
		# assemble the pieces
		sql = "SELECT "
		sqlSelect['f_rowid'] = ("(" + ("||'_'||".join(sqlFRowID)) + ")") if sqlFRowID else "0"
		sqlSelect['m_rowid'] = ("(" + ("||'_'||".join(sqlMRowID)) + ")") if sqlMRowID else "0"
		sql += ",\n  ".join("{0} AS {1}".format(sqlSelect[c],c) for c in columns)
		sql += "\nFROM "
		sql += (",\n  ".join("`{0[0]}`.`{0[1]}` AS {1}".format(self._queryAliasTables[a],a) for a in sorted(sqlFrom))) if sqlFrom else "(SELECT 1)"
		if sqlWhere:
			sql += "\nWHERE "
			sql += "\n  AND ".join(sorted(sqlWhere))
		if sqlGroup:
			sql += "\nGROUP BY " + (", ".join(sqlGroup))
		if sqlHaving:
			sql += "\nHAVING "
			sql += "\n  AND ".join(sorted(sqlHaving))
		if sqlOrder:
			sql += "\nORDER BY " + (", ".join(sqlOrder))
		if sqlLimit:
			sql += "\nLIMIT %d" % sqlLimit
		
		if self._debug:
			self.log(sql+"\n")
			for row in self._loki._db.cursor().execute("EXPLAIN QUERY PLAN "+sql):
				self.log(str(row)+"\n")
			return
		
		# run, filter and return
		# The unique-row filtering could be done in SQL using GROUP BY, but that
		# often forces the optimizer to join the tables in the order of grouping
		# which might not be ideal.  It could also be done with DISTINCT, but
		# there's no way to specify that only a few columns really have to be
		# checked for distinctness because all the rest depend on those few.
		# So it ends up being fastest to do the duplicate filtering here, by
		# checking only the composite ROWID against a set of previous values.
		columnIndex = { columns[i]:i for i in xrange(len(columns)) }
		fidIndex = columnIndex['f_rowid']
		midIndex = columnIndex['m_rowid']
		resultIDs = set()
		if filterTypes == modelTypes:
			for row in self._loki._db.cursor().execute(sql):
				rid = (min(row[fidIndex],row[midIndex]),max(row[fidIndex],row[midIndex]))
				if rid not in resultIDs:
					resultIDs.add(rid)
					yield row
		else:
			for row in self._loki._db.cursor().execute(sql):
				rid = (row[fidIndex],row[midIndex])
				if rid not in resultIDs:
					resultIDs.add(rid)
					yield row
	#generateResults()
	
	
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
	
	parser.add_argument('--minimum-model-score', '--mms', type=int, metavar='score',
			help="minimum implication score for knowledge-supported models (default: 1)"
	)
	
	parser.add_argument('--num-models', '--nm', type=int, metavar='num',
			help="maximum number of models to generate, 0 for unlimited (default: 100)"
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
	
	parser.add_argument('--output', '-o', type=str, metavar=('type'), nargs='+', action='append', choices=['snps','loci','genes','regions','groups','sources'],
			help="data type(s) to filter and annotate, from 'snps', 'loci', 'genes', 'regions', 'groups' and 'sources'"
	)
	
	parser.add_argument('--model', '-m', type=str, metavar=('type'), nargs=2, action='append', choices=['snps','loci','genes','regions','groups','sources'],
			help="data types to model, from 'snps', 'loci', 'genes', 'regions', 'groups' and 'sources'"
	)
	
	
	parser.add_argument('--verbose', '-v', action='store_true',
			help="print warnings and log messages"
	)
	
	parser.add_argument('--debug', action='store_true',
			help="print extra debugging information"
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
	if args.debug:
		bio.setDebug(True)
	
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
			bio.intersectSNPs( bio.generateRSesFromText(snpList) )
	if args.snp_file:
		for snpFileList in args.snp_file:
			bio.intersectSNPs( bio.generateRSesFromRSFiles(snpFileList) )
	if args.alt_snp:
		for snpList in args.alt_snp:
			bio.intersectSNPs( bio.generateRSesFromText(snpList), True )
	if args.alt_snp_file:
		for snpFileList in args.alt_snp_file:
			bio.intersectSNPs( bio.generateRSesFromRSFiles(snpFileList), True )
	
	# apply locus filters
	if args.locus:
		for locusList in args.locus:
			bio.intersectLoci( bio.generateLociFromText(locusList) )
	if args.locus_file:
		for locusFileList in args.locus_file:
			bio.intersectLoci( bio.generateLociFromMapFiles(locusFileList) )
	if args.alt_locus:
		for locusList in args.alt_locus:
			bio.intersectLoci( bio.generateLociFromText(locusList), True )
	if args.alt_locus_file:
		for locusFileList in args.alt_locus_file:
			bio.intersectLoci( bio.generateLociFromMapFiles(locusFileList), True )
	
	# apply gene filters
	if args.gene:
		for geneList in args.gene:
			bio.intersectGenes( geneList )
	if args.gene_file:
		for geneFileList in args.gene_file:
			bio.intersectGenes( bio.generateNamesFromNameFiles(geneFileList) )
	if args.alt_gene:
		for geneList in args.alt_gene:
			bio.intersectGenes( geneList, True )
	if args.alt_gene_file:
		for geneFileList in args.alt_gene_file:
			bio.intersectGenes( bio.generateNamesFromNameFiles(geneFileList), True )
	
	# apply region filters
	if args.region:
		for regionList in args.region:
			bio.intersectRegions( bio.generateRegionsFromText(regionList) )
	if args.region_file:
		for regionFileList in args.region_file:
			bio.intersectRegions( bio.generateRegionsFromFiles(regionFileList) )
	if args.alt_region:
		for regionList in args.alt_region:
			bio.intersectRegions( bio.generateRegionsFromText(regionList), True )
	if args.alt_region_file:
		for regionFileList in args.alt_region_file:
			bio.intersectRegions( bio.generateRegionsFromFiles(regionFileList), True )
	
	# apply group filters
	if args.group:
		for groupList in args.group:
			bio.intersectGroups( groupList )
	if args.group_file:
		for groupFileList in args.group_file:
			bio.intersectGroups( bio.generateNamesFromNameFiles(groupFileList) )
	if args.alt_group:
		for groupList in args.alt_group:
			bio.intersectGroups( groupList, True )
	if args.alt_group_file:
		for groupFileList in args.alt_group_file:
			bio.intersectGroups( bio.generateNamesFromNameFiles(groupFileList), True )
	
	# apply source filters
	if args.source:
		for sourceList in args.source:
			bio.intersectSources( sourceList )
	if args.source_file:
		for sourceFileList in args.source_file:
			bio.intersectSources( bio.generateNamesFromNameFiles(sourceFileList) )
	if args.alt_source:
		for sourceList in args.alt_source:
			bio.intersectSources( sourceList, True )
	if args.alt_source_file:
		for sourceFileList in args.alt_source_file:
			bio.intersectSources( bio.generateNamesFromNameFiles(sourceFileList), True )
	
	# gene name stats
	if args.gene_name_stats:
		outPath = args.prefix + '.gene-names'
		bio.log("writing gene name statistics to %s ..." % ("<stdout>" if args.stdout else outPath))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			with (sys.stdout if args.stdout else open(outPath, 'w')) as outFile:
				outFile.write("#type\tnames\tunique\tambiguous\n")
				for row in bio.generateGeneNameStats():
					outFile.write("%s\t%s\t%s\t%s\n" % row)
			bio.log(" OK\n")
	#if gene-name-stats
	
	# group name stats
	if args.group_name_stats:
		outPath = args.prefix + '.group-names'
		bio.log("writing group name statistics to %s ..." % ("<stdout>" if args.stdout else outPath))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			with (sys.stdout if args.stdout else open(outPath, 'w')) as outFile:
				outFile.write("#type\tnames\tunique\tambiguous\n")
				for row in bio.generateGroupNameStats():
					outFile.write("%s\t%s\t%s\t%s\n" % row)
			bio.log(" OK\n")
	#if group-name-stats
	
	# filtering/annotation output
	for output in (args.output or []):
		outPath = args.prefix + '.' + '-'.join(output)
		bio.log("writing %s to %s ..." % ('-'.join(output),("<stdout>" if args.stdout else outPath)))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			headerList = list()
			formatList = list()
			
			# => (rowid, snp_label, locus_label,chr,pos, gene_label, region_label,chr,posMin,posMax, group_label, source_label)
			for outType in output:
				if outType == 'snps':
					headerList.extend(["snp"])
					formatList.extend(["{d[1]}"])
				elif outType == 'loci':
					headerList.extend(["chr","locus","pos"])
					formatList.extend(["{d[3]}","{d[2]}","{d[4]}"])
				elif outType == 'genes':
					headerList.extend(["gene"])
					formatList.extend(["{d[5]}"])
				elif outType == 'regions':
					headerList.extend(["chr","region","posMin","posMax"])
					formatList.extend(["{d[7]}","{d[6]}","{d[8]}","{d[9]}"])
				elif outType == 'groups':
					headerList.extend(["group"])
					formatList.extend(["{d[10]}"])
				elif outType == 'sources':
					headerList.extend(["source"])
					formatList.extend(["{d[11]}"])
			#foreach outType
			
			headerStr = "#" + "\t".join(headerList) + "\n"
			formatStr = "\t".join(formatList) + "\n"
			with (sys.stdout if args.stdout else open(outPath, 'w')) as outFile:
				outFile.write(headerStr)
				for data in bio.generateResults(output):
					outFile.write(formatStr.format(d=data))
			#with outFile
			bio.log(" OK\n")
		#if output ok
	#foreach output
	
	# modeling output
	for model in (args.model or []):
		outPath = args.prefix + '.' + '-'.join(model) + '.models'
		bio.log("writing %s models to %s ..." % ('-'.join(model),("<stdout>" if args.stdout else outPath)))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			headerList = list()
			formatList = list()
			
			# [0:11] => (f_rowid, f_snp_label, f_locus_label,chr,pos, f_gene_label, f_region_label,chr,posMin,posMax, f_group_label, f_source_label)
			if model[0] == 'snps':
				headerList.extend(["snp"])
				formatList.extend(["{d[1]}"])
			elif model[0] == 'loci':
				headerList.extend(["chr","locus","pos"])
				formatList.extend(["{d[3]}","{d[2]}","{d[4]}"])
			elif model[0] == 'genes':
				headerList.extend(["gene"])
				formatList.extend(["{d[5]}"])
			elif model[0] == 'regions':
				headerList.extend(["chr","region","posMin","posMax"])
				formatList.extend(["{d[7]}","{d[6]}","{d[8]}","{d[9]}"])
			elif model[0] == 'groups':
				headerList.extend(["group"])
				formatList.extend(["{d[10]}"])
			elif model[0] == 'sources':
				headerList.extend(["source"])
				formatList.extend(["{d[11]}"])
			
			# [0:11] => (m_rowid, m_snp_label, m_locus_label,chr,pos, m_gene_label, m_region_label,chr,posMin,posMax, m_group_label, m_source_label)
			if model[1] == 'snps':
				headerList.extend(["snp"])
				formatList.extend(["{d[13]}"])
			elif model[1] == 'loci':
				headerList.extend(["chr","locus","pos"])
				formatList.extend(["{d[15]}","{d[14]}","{d[16]}"])
			elif model[1] == 'genes':
				headerList.extend(["gene"])
				formatList.extend(["{d[17]}"])
			elif model[1] == 'regions':
				headerList.extend(["chr","region","posMin","posMax"])
				formatList.extend(["{d[19]}","{d[18]}","{d[20]}","{d[21]}"])
			elif model[1] == 'groups':
				headerList.extend(["group"])
				formatList.extend(["{d[22]}"])
			elif model[1] == 'sources':
				headerList.extend(["source"])
				formatList.extend(["{d[23]}"])
			
			if bio._supportedModels:
				headerList.extend(["score"])
				formatList.extend(["{d[24]}.{d[25]}"])
			
			headerStr = "#" + "\t".join(headerList) + "\n"
			formatStr = "\t".join(formatList) + "\n"
			with (sys.stdout if args.stdout else open(outPath, 'w')) as outFile:
				outFile.write(headerStr)
				for data in bio.generateResults(model[0:1], model[1:2]):
					outFile.write(formatStr.format(d=data))
			#with outFile
			bio.log(" OK\n")
		#if model ok
	#foreach model
	
#__main__

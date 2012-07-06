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
	
	
	ver_maj,ver_min,ver_rev,ver_dev,ver_date = 2,0,0,'a4','2012-06-29'
	
	
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
		self._geneNamespace = None
		self._groupNamespace = None
		self._ldprofile = ''
		
		self._tablesDeindexed = set()
		self._snpFilters = 0
		self._locusFilters = 0
		self._geneFilters = 0
		self._regionFilters = 0
		self._groupFilters = 0
		self._sourceFilters = 0
		
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
	
	
	def setRegionLocusTolerance(self, tolerance=0):
		self._regionLocusTolerance = int(tolerance)
		self.log("region-locus match tolerance: %d\n" % self._regionLocusTolerance)
	#setRegionLocusTolerance()
	
	
	def setLDProfile(self, ldprofile=''):
		self._ldprofile = str(ldprofile).strip()
		self.log("LD profile for region-locus matching: %s\n" % self._ldprofile)
	#setLDProfile()
	
	
	def setGeneNamespace(self, namespace=None):
		self._geneNamespace = None if (namespace == None) else str(namespace).strip()
		self.log("gene name type: %s\n" % ("<label>" if self._geneNamespace == None else (self._geneNamespace or "<any>")))
	#setGeneNamespace()
	
	
	def setGroupNamespace(self, namespace=None):
		self._groupNamespace = None if (namespace == None) else str(namespace).strip()
		self.log("group name type: %s\n" % ("<label>" if self._groupNamespace == None else (self._groupNamespace or "<any>")))
	#setGroupNamespace()
	
	
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
				self.updateRegionZones()
	#prepareTableForQuery()
	
	
	def updateRegionZones(self):
		self.log("calculating region zone coverage ...")
		
		size = self._loki.getDatabaseSetting('zone_size')
		if not size:
			raise Exception("ERROR: could not determine database setting 'zone_size'")
		size = int(size)
		dbc = self._loki._db.cursor()
		
		# make sure all regions are correctly oriented
		dbc.execute("UPDATE `main`.`region` SET posMin = posMax, posMax = posMin WHERE posMin > posMax")
		
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
		self.prepareTableForQuery('region')
		self.prepareTableForUpdate('region_zone')
		dbc.execute("DELETE FROM `main`.`region_zone`")
		dbc.executemany(
			"INSERT OR IGNORE INTO `main`.`region_zone` (region_rowid,chr,zone) VALUES (?,?,?)",
			_zones(
				size,
				self._loki._db.cursor().execute("SELECT rowid,chr,posMin,posMax FROM `main`.`region`")
			)
		)
		
		# clean up
		self.prepareTableForQuery('region_zone')
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
	
	
	def unionSNPs(self, snps):
		# snps=[ rs, ... ]
		self.log("adding SNP filter ...")
		self.prepareTableForUpdate('snp')
		dbc = self._loki._db.cursor()
		
		# for some reason this process (create temp table, insert to temp table,
		# left join merge table while insert-selecting from temp to main table)
		# is ~30% faster than the equivalent one-query solution (left join merge
		# table while insert-selecting from literal "SELECT ? AS rs" subquery)
		dbc.execute("CREATE TEMP TABLE `temp`.`rs` (rs INTEGER PRIMARY KEY)")
		dbc.executemany("INSERT OR IGNORE INTO `temp`.`rs` (rs) VALUES (?)", itertools.izip(snps))
		sql = """
INSERT INTO `main`.`snp` (label,rs)
SELECT 'rs'||t_r.rs, COALESCE(d_sm.rsCurrent, t_r.rs)
FROM `temp`.`rs` AS t_r
LEFT JOIN `db`.`snp_merge` AS d_sm
  ON d_sm.rsMerged = t_r.rs
"""
		dbc.execute(sql)
		numAdd = self._loki._db.changes()
		dbc.execute("DROP TABLE `temp`.`rs`")
		self.log(" OK: added %d RS#s\n" % (numAdd,))
		
		self._snpFilters += 1
	#unionSNPs()
	
	
	def intersectSNPs(self, snps):
		# snps=[ rs, ... ]
		if not self._snpFilters:
			return self.unionSNPs(snps)
		self.log("intersecting SNP filter ...")
		dbc = self._loki._db.cursor()
		
		self.prepareTableForQuery('snp')
		numBefore = max(row[0] for row in dbc.execute("SELECT COUNT() FROM `main`.`snp`"))
		dbc.execute("CREATE TEMP TABLE `temp`.`rs` (rs INTEGER PRIMARY KEY)")
		dbc.executemany("INSERT INTO `temp`.`rs` (rs) VALUES (?)", itertools.izip(snps))
		sql = """
DELETE FROM `main`.`snp` WHERE rs NOT IN (
  SELECT m_s.rs
  FROM `temp`.`rs` AS t_r
  LEFT JOIN `db`.`snp_merge` AS d_sm
    ON d_sm.rsMerged = t_r.rs
  JOIN `main`.`snp` AS m_s
    ON m_s.rs = COALESCE(d_sm.rsCurrent, t_r.rs)
)
"""
		dbc.execute(sql)
		numDrop = self._loki._db.changes()
		dbc.execute("DROP TABLE `temp`.`rs`")
		self.log(" OK: kept %d RS#s (%d dropped)\n" % (numBefore-numDrop,numDrop))
		
		self._snpFilters += 1
	#intersectSNPs()
	
	
	def unionLoci(self, loci):
		# loci=[ (label,chr,pos), ... ]
		self.log("adding locus filter ...")
		self.prepareTableForUpdate('locus')
		dbc = self._loki._db.cursor()
		sql = "INSERT OR IGNORE INTO `main`.`locus` (label,chr,pos) VALUES (?,?,?); SELECT LAST_INSERT_ROWID()"
		lastID = None
		numAdd = numNull = 0
		for row in dbc.executemany(sql, loci):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d loci (%d incomplete)\n" % (numAdd,numNull))
		self._locusFilters += 1
	#unionLoci()
	
	
	def intersectLoci(self, loci):
		# loci=[ (label,chr,pos), ... ]
		if not self._locusFilters:
			return self.unionLoci(loci)
		self.log("intersecting locus filter ...")
		self.prepareTableForQuery('locus')
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`locus` SET flag = 0")
		numBefore = self._loki._db.changes()
		dbc.executemany("UPDATE `main`.`locus` SET flag = 1 WHERE (1 OR ?) AND chr = ? AND pos = ?", loci)
		dbc.execute("DELETE FROM `main`.`locus` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d loci (%d dropped)\n" % (numBefore-numDrop,numDrop))
		self._locusFilters += 1
	#intersectLoci()
	
	
	##################################################
	# region/boundary input
	
	
	def unionGenes(self, names):
		# names=[ name, ... ]
		self.log("adding gene filter ...")
		
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
		
		self.prepareTableForUpdate('gene')
		dbc = self._loki._db.cursor()
		sql = "INSERT INTO `main`.`gene` (label,biopolymer_id) VALUES (?,?); SELECT 1"
		maxMatch = (1 if self._geneStrict else None)
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		self.log(" OK: added %d genes (%d matched, %d ambiguous, %d unrecognized)\n" % (numAdd,tally['match'],tally['ambig'],tally['null']))
		self._geneFilters += 1
	#unionGenes()
	
	
	def intersectGenes(self, names):
		# names=[ name, ... ]
		if not self._geneFilters:
			return self.unionGenes(names)
		self.log("intersecting gene filter ...")
		
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
		
		self.prepareTableForQuery('gene')
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`gene` SET flag = 0")
		numBefore = self._loki._db.changes()
		tally = dict()
		sql = "UPDATE `main`.`gene` SET flag = 1 WHERE (1 OR ?) AND biopolymer_id = ?"
		maxMatch = (1 if self._geneStrict else None)
		dbc.executemany(sql, self._loki.generateBiopolymerIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		dbc.execute("DELETE FROM `main`.`gene` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d genes (%d dropped, %d ambiguous, %d unrecognized)\n" % (numBefore-numDrop,numDrop,tally['ambig'],tally['null']))
		self._geneFilters += 1
	#intersectGenes()
	
	
	def unionRegions(self, regions):
		# regions=[ (label,chr,posMin,posMax), ... ]
		self.log("adding region filter ...")
		
		self.prepareTableForUpdate('region')
		dbc = self._loki._db.cursor()
		
		sql = "INSERT OR IGNORE INTO `main`.`region` (label,chr,posMin,posMax) VALUES (?,?,?,?); SELECT LAST_INSERT_ROWID()"
		lastID = None
		numAdd = numNull = 0
		for row in dbc.executemany(sql, regions):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d regions (%d incomplete)\n" % (numAdd,numNull))
		self._regionFilters += 1
	#unionRegions()
	
	
	def intersectRegions(self, regions):
		# regions=[ (label,chr,posMin,posMax), ... ]
		if not self._regionFilters:
			return self.unionRegions(regions)
		
		self.log("intersecting region filter ...")
		self.prepareTableForQuery('region')
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`region` SET flag = 0")
		numBefore = self._loki._db.changes()
		dbc.executemany("UPDATE `main`.`region` SET flag = 1 WHERE (1 OR ?) AND chr = ? AND posMin = ? AND posMax = ?", regions)
		dbc.execute("DELETE FROM `main`.`region` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d regions (%d dropped)\n" % (numBefore-numDrop,numDrop))
		self._regionFilters += 1
	#intersectRegions()
	
	
	##################################################
	# group input
	
	
	def unionGroups(self, names, gtype=None):
		# names=[ name, ... ]
		self.log("adding %s filter ..." % (gtype or "group"))
		
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
		
		self.prepareTableForUpdate('group')
		dbc = self._loki._db.cursor()
		sql = "INSERT INTO `main`.`group` (label,group_id) VALUES (?,?); SELECT 1"
		maxMatch = (1 if self._groupStrict else None)
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID)):
			numAdd += 1
		self.log(" OK: added %d groups (%d matched, %d ambiguous, %d unrecognized)\n" % (
				numAdd,tally['match'],tally['ambig'],tally['null']
		))
		self._groupFilters += 1
	#unionGroups()
	
	
	def intersectGroups(self, names, gtype=None):
		# names=[ name, ... ]
		if not self._groupFilters:
			return self.unionGroups(names, gtype)
		self.log("intersecting %s filter ..." % (gtype or "group"))
		
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
		
		self.prepareTableForQuery('group')
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`group` SET flag = 0")
		numBefore = self._loki._db.changes()
		maxMatch = (1 if self._groupStrict else None)
		tally = dict()
		sql = "UPDATE `main`.`group` SET flag = 1 WHERE (1 OR ?) AND group_id = ?"
		dbc.executemany(sql, self._loki.generateGroupIDsByName(names, maxMatch=maxMatch, tally=tally, namespaceID=namespaceID, typeID=typeID))
		dbc.execute("DELETE FROM `main`.`group` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d groups (%d dropped, %d ambiguous, %d unrecognized)\n" % (
				numBefore-numDrop,numDrop,tally['ambig'],tally['null']
		))
		self._groupFilters += 1
	#intersectGroups()
	
	
	##################################################
	# source input
	
	
	def unionSources(self, names):
		# names=[ name, ... ]
		self.log("adding source filter ...")
		
		self.prepareTableForUpdate('source')
		dbc = self._loki._db.cursor()
		sql = "INSERT OR IGNORE INTO `main`.`source` (label,source_id) VALUES (?,?); SELECT LAST_INSERT_ROWID()"
		lastID = None
		numAdd = numNull = 0
		for row in dbc.executemany(sql, self._loki.getSourceIDs(names).iteritems()):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d sources (%d unrecognized)\n" % (numAdd,numNull))
		self._sourceFilters += 1
	#unionSources()
	
	
	def intersectSources(self, names):
		# names=[ name, ... ]
		if not self._sourceFilters:
			return self.unionSources(names)
		self.log("intersecting source filter ...")
		
		self.prepareTableForQuery('source')
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`source` SET flag = 0")
		numBefore = self._loki._db.changes()
		sql = "UPDATE `main`.`source` SET flag = 1 WHERE (1 OR ?) AND source_id = ?"
		dbc.executemany(sql, self._loki.getSourceIDs(names).iteritems())
		dbc.execute("DELETE FROM `main`.`source` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d sources (%d dropped)\n" % (numBefore-numDrop,numDrop))
		self._sourceFilters += 1
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
			('main','snp'): {
				"{L}.rs = {R}.rs",
			},
			('db','snp_locus'): {
				"{L}.rs = {R}.rs",
			},
		},
		
		('main','locus'): {
			('main','locus'): {
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
			('main','region_zone'): {
				"{L}.chr = {R}.chr",
				"{L}.zone = {R}.zone",
			},
			('main','region'): {
				"{L}.region_rowid = {R}.rowid",
				# with the rowid match, these should all be guaranteed by self.updateRegionZones()
				"{L}.chr = {R}.chr",
				"(({L}.zone + 1) * {zoneSize}) > {R}.posMin",
				"({L}.zone * {zoneSize}) <= {R}.posMax",
				"{L}.zone >= ({R}.posMin / {zoneSize})",
				"{L}.zone <= ({R}.posMax / {zoneSize})",
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
			('main','region'): { #TODO: partial overlap
				"{L}.chr = {R}.chr",
				"{L}.posMin = {R}.posMin",
				"{L}.posMax = {R}.posMax",
			},
			('db','snp_locus'): {
				"{L}.chr = {R}.chr",
				"({L}.posMin - {rlTolerance}) <= {R}.pos",
				"({L}.posMax + {rlTolerance}) >= {R}.pos",
				"{L}.posMin <= ({R}.pos + {rlTolerance})",
				"{L}.posMax >= ({R}.pos - {rlTolerance})",
			},
			('db','biopolymer_region'): { #TODO: partial overlap
				"{L}.chr = {R}.chr",
				"{L}.posMin = {R}.posMin",
				"{L}.posMax = {R}.posMax",
			},
		},
		
		('main','gene'): {
			('main','gene'): {
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
		
		('main','group'): {
			('main','group'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group_biopolymer'): {
				"{L}.group_id = {R}.group_id",
			},
			('db','group'): {
				"{L}.group_id = {R}.group_id",
			},
		},
		
		('main','source'): {
			('main','source'): {
				"{L}.source_id = {R}.source_id",
			},
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
	
	
	
	def getQueryAliasJoins(self, aliasTables):
		pass
	#getQueryAliasJoins()
	
	
	##################################################
	# filtering & annotation
	
	
	def generateFilteredData(self, types):
		# initialize query fragments
		sqlSelect = [
			'rowid',
			'snp_label',
			'locus_label', 'locus_chr', 'locus_pos',
			'gene_label',
			'region_label', 'region_chr', 'region_posMin', 'region_posMax',
			'group_label',
			'source_label'
		]
		sqlRowID = set() # {expA,expB,...} => SELECT expA||'_'||expB... AS rowid, ...
		sqlColumn = { # {colA:expA,colB:expB,...} => SELECT ... expA AS colA, expB AS colB, ...
			'snp_label'     : "NULL",
			'locus_label'   : "NULL",
			'locus_chr'     : "NULL",
			'locus_pos'     : "NULL",
			'gene_label'    : "NULL",
			'region_label'  : "NULL",
			'region_chr'    : "NULL",
			'region_posMin' : "NULL",
			'region_posMax' : "NULL",
			'group_label'   : "NULL",
			'source_label'  : "NULL",
		}
		sqlFrom = set() # {tblA,tblB,...} => FROM aliasTable[tblA] AS tblA, aliasTable[tblB] AS tblB, ...
		sqlWhere = set() # {expA,expB,...} => WHERE expA AND expB AND ...
		sqlGroup = list() # [expA,expB,...] => GROUP BY expA, expB, ...
		
		# include all table aliases needed to satisfy input filters
		if self._snpFilters:
			sqlFrom.add('mf_s')
		if self._locusFilters:
			sqlFrom.add('mf_l')
		if self._geneFilters:
			sqlFrom.add('mf_bg')
		if self._regionFilters:
			sqlFrom.add('mf_r')
		if self._groupFilters:
			sqlFrom.add('mf_g')
		if self._sourceFilters:
			sqlFrom.add('mf_c')
		
		# include all table aliases and columns needed to satisfy output requests
		if 'snps' in types:
			if 'mf_s' in sqlFrom:
				sqlRowID.add("mf_s.rowid")
				sqlColumn['snp_label'] = "mf_s.label"
			else:
				sqlFrom.add('df_sl')
				sqlRowID.add("df_sl.rs")
				sqlColumn['snp_label'] = "'rs'||df_sl.rs"
		
		if 'loci' in types:
			if 'mf_l' in sqlFrom:
				sqlRowID.add("mf_l.rowid")
				sqlColumn['locus_label'] = "mf_l.label"
				sqlColumn['locus_chr'] = "mf_l.chr"
				sqlColumn['locus_pos'] = "mf_l.pos"
			else:
				sqlFrom.add('df_sl')
				sqlRowID.add("df_sl._ROWID_")
				sqlColumn['locus_label'] = "'rs'||df_sl.rs"
				sqlColumn['locus_chr'] = "df_sl.chr"
				sqlColumn['locus_pos'] = "df_sl.pos"
		
		if 'genes' in types:
			if 'mf_bg' in sqlFrom:
				sqlRowID.add("mf_bg._ROWID")
				sqlColumn['gene_label'] = "mf_bg.label"
			else:
				sqlFrom.add('df_b')
				sqlRowID.add("df_b.biopolymer_id")
				sqlColumn['gene_label'] = "df_b.label"
		
		if 'regions' in types:
			if 'mf_r' in sqlFrom:
				sqlRowID.add("mf_r.rowid")
				sqlColumn['region_label'] = "mf_r.label"
				sqlColumn['region_chr'] = "mf_r.chr"
				sqlColumn['region_posMin'] = "mf_r.posMin"
				sqlColumn['region_posMax'] = "mf_r.posMax"
			else:
				sqlFrom.add('df_b')
				sqlFrom.add('df_br')
				sqlRowID.add("df_br._ROWID_")
				sqlColumn['region_label'] = "df_b.label"
				sqlColumn['region_chr'] = "df_br.chr"
				sqlColumn['region_posMin'] = "df_br.posMin"
				sqlColumn['region_posMax'] = "df_br.posMax"
		
		if 'groups' in types:
			if 'mf_g' in sqlFrom:
				sqlRowID.add("mf_g.rowid")
				sqlColumn['group_label'] = "mf_g.label"
			else:
				sqlFrom.add('df_g')
				sqlRowID.add("df_g.group_id")
				sqlColumn['group_label'] = "df_g.label"
		
		if 'sources' in types:
			if 'mf_c' in sqlFrom:
				sqlRowID.add("mf_c.rowid")
				sqlColumn['source_label'] = "mf_c.label"
			else:
				sqlFrom.add('df_c')
				sqlRowID.add("df_c.source_id")
				sqlColumn['source_label'] = "df_c.source"
		
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
		
		# make sure any included filter tables are indexed
		for a in sqlFrom:
			if self._queryAliasTables[a][0] == 'main':
				self.prepareTableForQuery(self._queryAliasTables[a][1])
		
		# fetch values to insert into conditions
		rlTolerance = self._regionLocusTolerance
		zoneSize = self._loki.getDatabaseSetting('zone_size')
		zoneSize = int(zoneSize) if zoneSize else None
		ldprofileID = self._loki.getLDProfileID(self._ldprofile)
		
		# add some general constraints for included tables
		if ('df_sl' in sqlFrom) and self._snpLociValidated:
			sqlWhere.add("df_sl.validated = 1")
		if ('df_br' in sqlFrom):
			sqlWhere.add("df_br.ldprofile_id = {ldprofileID}".format(ldprofileID=ldprofileID))
		if ('df_gb' in sqlFrom):
			sqlWhere.add("df_gb.biopolymer_id > 0")
			if self._knowledgeScoring == 'quality':
				sqlWhere.add("df_gb.quality {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			elif self._knowledgeScoring == 'implication':
				sqlWhere.add("df.implication {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
			else:
				sqlWhere.add("df_gb.specificity {0}".format(">= 100" if self._knowledgeStrict else "> 0"))
		
		# add join constraints for included table pairs
		for a0 in self._queryAliasJoinPathEdges:
			if a0 in sqlFrom:
				for a1 in self._queryAliasJoinPathEdges[a0]:
					if a1 in sqlFrom:
						t0 = self._queryAliasTables[a0]
						t1 = self._queryAliasTables[a1]
						if (t0 in self._queryTableJoinConditions) and (t1 in self._queryTableJoinConditions[t0]):
							sqlWhere.update(c.format(
									L=a0, R=a1, rlTolerance=rlTolerance, zoneSize=zoneSize, ldprofileID=ldprofileID
							) for c in self._queryTableJoinConditions[t0][t1])
		
		# assemble the pieces
		sqlColumn['rowid'] = "(" + ("||'_'||".join(sqlRowID)) + ")"
		sql = "SELECT " + (",\n  ".join("{0} AS {1}".format(sqlColumn[c],c) for c in sqlSelect))
		sql += "\nFROM " + ((",\n  ".join("`{0[0]}`.`{0[1]}` AS {1}".format(self._queryAliasTables[a],a) for a in sqlFrom)) if sqlFrom else "(SELECT 1)")
		sql += "\nWHERE " + (("\n  AND ".join(sqlWhere)) if sqlWhere else "1")
		if sqlGroup:
			sql += "\nGROUP BY " + (", ".join(sqlGroup))
		
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
		rowIDs = set()
		for row in self._loki._db.cursor().execute(sql):
			if row[0] not in rowIDs:
				rowIDs.add(row[0])
				yield row
	#generateFilteredData()
	
	
	##################################################
	# model generation
	
	
	def outputLociModels(self, etype='gene', target=sys.stdout): #TODO
		typeID = self._loki.getTypeID(etype)
		if not typeID:
			sys.stderr.write("ERROR: unknown entity type '%s'\n" % etype)
			sys.exit(1)
		populationID = self._loki.getPopulationID(self._population)
		if not populationID:
			sys.stderr.write("ERROR: unknown population '%s'\n" % self._population)
			sys.exit(1)
		
		target.write(
				("#label1\tchr1\tpos1"
				+"\tlabel2\tchr2\tpos2"
				+"\troutes\tlinks\tsources"
				+"\n")
		)
		locus = {}
		modelRoutes = {}
		modelLinks = {}
		modelSources = {}
		sql = """
SELECT
  lA.locus_id AS locus1,
  COALESCE(lA.label, 'rs'||lA.rs, 'chr'||lA.chr||':'||lA.pos, '!#'||lA.locus_id) AS label1,
  COALESCE(lA.chr, 'NA') AS chr1,
  COALESCE(lA.pos, 'NA') AS pos1,
  lB.locus_id AS locus2,
  COALESCE(lB.label, 'rs'||lB.rs, 'chr'||lB.chr||':'||lB.pos, '!#'||lB.locus_id) AS label2,
  COALESCE(lB.chr, 'NA') AS chr2,
  COALESCE(lB.pos, 'NA') AS pos2,
  eX.entity_id AS link,
  elA.source_id AS source1,
  elB.source_id AS source2
FROM main.locus AS lA
JOIN db.entity_zone AS ezA
  ON ezA.population_id = :population_id
  AND ezA.chr = lA.chr
  AND ezA.zone >= (lA.pos - :expand) / 100000
  AND ezA.zone <= (lA.pos + :expand) / 100000
JOIN db.entity_region AS erA
  ON erA.entity_id = ezA.entity_id
  AND erA.population_id = :population_id
  AND erA.chr = lA.chr
  AND erA.posMin <= lA.pos + :expand
  AND erA.posMax >= lA.pos - :expand
JOIN db.entity AS eA
  ON eA.entity_id = erA.entity_id
  AND eA.type_id = :type_id
JOIN db.entity_link AS elA
  ON elA.entity_id = eA.entity_id
JOIN db.entity AS eX
  ON eX.entity_id = elA.related_entity_id
JOIN db.entity_link AS elB
  ON elB.entity_id = elA.related_entity_id
  AND elB.related_entity_id != elA.entity_id
JOIN db.entity AS eB
  ON eB.entity_id = elB.related_entity_id
  AND eB.type_id = :type_id
JOIN db.entity_region AS erB
  ON erB.entity_id = eB.entity_id
  AND erB.population_id = :population_id
JOIN main.locus AS lB
  ON lB.chr = erB.chr
  AND lB.pos >= erB.posMin - :expand
  AND lB.pos <= erB.posMax + :expand
"""
		for row in self._loki._dbc.execute(sql, { 'expand':self._expand, 'population_id':populationID, 'type_id':typeID }):
			if row[0] not in locus:
				locus[row[0]] = (row[0],row[1],row[2],row[3])
			if row[4] not in locus:
				locus[row[4]] = (row[4],row[5],row[6],row[7])
			model = (row[0],row[4])
			if model not in modelRoutes:
				modelRoutes[model] = 0
				modelLinks[model] = set()
				modelSources[model] = set()
			modelRoutes[model] += 1
			modelLinks[model].add(row[8])
			modelSources[model].add(row[9])
			modelSources[model].add(row[10])
		print "outputting ..."
		for model in modelRoutes:
			target.write(
				(
					"%s\t%d:%d\t"
					+"%s\t%d:%d\t"
					+"%d\t%d\t%d\n"
				) % (
					locus[model[0]][1],locus[model[0]][2],locus[model[0]][3],
					locus[model[1]][1],locus[model[1]][2],locus[model[1]][3],
					modelRoutes[model], len(modelLinks[model]), len(modelSources[model])
				)
			)
	#outputLociModels() #TODO
	
	
	def outputRegionModels(self, rtype='gene', target=sys.stdout): #TODO
		typeID = self._loki.getTypeID(rtype)
		if not typeID:
			sys.stderr.write("ERROR: unknown region type '%s'\n" % rtype)
			sys.exit(1)
		populationID = self._loki.getPopulationID(self._population)
		if not populationID:
			sys.stderr.write("ERROR: unknown population '%s'\n" % self._population)
			sys.exit(1)
		
		# map loci to known regions
		sys.stderr.write("identifying candidate %s regions ..." % rtype)
		sys.stderr.flush()
		self._loki.createDatabaseTables(self._schema['temp'], 'temp', 'region')
		self._loki._dbc.execute("""
INSERT INTO temp.`region` (label, region_id, type_id, chr, posMin, posMax)
SELECT r.label, r.region_id, r.type_id, rb.chr, rb.posMin, rb.posMax
FROM db.`region` AS r
JOIN db.`region_bound` AS rb
  ON rb.region_id = r.region_id
  AND rb.population_id = :population_id
JOIN main.`locus` AS l
  ON l.chr = rb.chr
  AND l.pos >= rb.posMin - :expand
  AND l.pos <= rb.posMax + :expand
WHERE r.type_id = :type_id
GROUP BY rb._rowid_
""", { 'expand':self._expand, 'population_id':populationID, 'type_id':typeID })
		self._loki.createDatabaseIndecies(self._schema['temp'], 'temp', 'region')
		for row in self._loki._dbc.execute("SELECT COUNT(1) FROM temp.region"):
			ttl = row[0]
		sys.stderr.write(" OK: %d regions\n" % ttl)
		
		# identify suitable groups # TODO: configurable max group size
		sys.stderr.write("identifying candidate %s groups ..." % rtype)
		sys.stderr.flush()
		self._loki.createDatabaseTables(self._schema['temp'], 'temp', 'group')
		self._loki._dbc.execute("""
INSERT OR IGNORE INTO temp.`group` (label, group_id, type_id)
SELECT g.label, g.group_id, g.type_id
FROM db.`group` AS g
JOIN db.`group_region` AS gr
  ON gr.group_id = g.group_id
JOIN db.`region` AS r
  ON r.region_id = gr.region_id
  AND r.type_id = :type_id
GROUP BY g.group_id
HAVING COUNT(DISTINCT r.region_id) <= 30
""", { 'type_id':typeID })
		self._loki.createDatabaseIndecies(self._schema['temp'], 'temp', 'group')
		for row in self._loki._dbc.execute("SELECT COUNT(1) FROM temp.`group`"):
			ttl = row[0]
		sys.stderr.write(" OK: %d groups\n" % ttl)
		
		# generate region models
		sys.stderr.write("generating %s-%s models ..." % (rtype,rtype))
		sys.stderr.flush()
		target.write(
			(
				"#%s1\tregion1\t"
				+"%s2\tregion2\t"
				+"routes\tgroups\tsources\n"
			) % (rtype,rtype)
		)
		sql = """
SELECT
  rA.label AS labelA,
  GROUP_CONCAT(DISTINCT trA.chr || ':' || trA.posMin || '-' || trA.posMax) AS regionA,
  rB.label AS labelB,
  GROUP_CONCAT(DISTINCT trB.chr || ':' || trB.posMin || '-' || trB.posMax) AS regionB,
  COUNT(1) AS routes,
  COUNT(DISTINCT tg.group_id) AS groups,
  COUNT(DISTINCT grA.source_id) AS sourcesA,
  COUNT(DISTINCT grB.source_id) AS sourcesB
FROM temp.`region` AS trA
JOIN db.`region` AS rA
  ON rA.region_id = trA.region_id
  AND rA.type_id = :type_id
JOIN db.`group_region` AS grA
  ON grA.region_id = rA.region_id
JOIN temp.`group` AS tg
  ON tg.group_id = grA.group_id
JOIN db.`group_region` AS grB
  ON grB.group_id = grA.group_id
  AND grB.region_id != grA.region_id
JOIN db.`region` AS rB
  ON rB.region_id = grB.region_id
  AND rB.type_id = :type_id
JOIN temp.`region` AS trB
  ON trB.region_id = rB.region_id
GROUP BY rA.region_id, rB.region_id
HAVING groups > 1 OR sourcesA > 1 OR sourcesB > 1
ORDER BY groups DESC, sourcesA DESC, sourcesB DESC
"""
		n = 0
		for row in self._loki._dbc.execute(sql, { 'expand':self._expand, 'population_id':populationID, 'type_id':typeID }):
			n += 1
			target.write(
				(
					"%s\t%s\t"
					+"%s\t%s\t"
					+"%d\t%d\t%d\t%d\n"
				) % row
			)
		sys.stderr.write(" OK: %d models\n" % n)
		self._loki.dropDatabaseTables(self._schema['temp'], 'temp', 'region')
		self._loki.dropDatabaseTables(self._schema['temp'], 'temp', 'group')
	#outputRegionModels() #TODO
	
	
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
	
	parser.add_argument('-k', '--knowledge', type=str, metavar='file',
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
	
	parser.add_argument('--region-locus-tolerance', type=str, metavar='bases',
			help="distance beyond the bounds of known regions where SNPs and loci should still be matched (default: 0)"
	)
	
	parser.add_argument('--ld-profile', type=str, metavar='profile',
			help="LD profile with which to match known regions to SNPs and loci (default: none)"
	)
	
	
	parser.add_argument('-s', '--snp', type=str, metavar=('rs#'), nargs='+', action='append',
			help="input SNPs, specified by RS#"
	)
	
	parser.add_argument('-S', '--snp-file', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input SNPs"
	)
	
	parser.add_argument('-l', '--locus', type=str, metavar=('locus'), nargs='+', action='append',
			help="input loci, specified by chromosome and position"
	)
	
	parser.add_argument('-L', '--locus-file', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input loci"
	)
	
	
	parser.add_argument('-g', '--gene', type=str, metavar=('name'), nargs='+', action='append',
			help="input genes, specified by name"
	)
	
	parser.add_argument('-G', '--gene-file', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input genes"
	)
	
	parser.add_argument('--gene-names', type=str, metavar='type', nargs='?', default=False,
			help="the type of the gene name(s) provided via --gene or --gene-file (default: primary labels)"
	)
	
	parser.add_argument('-r', '--region', type=str, metavar=('region'), nargs='+', action='append',
			help="input regions, specified by chromosome, start and stop positions"
	)
	
	parser.add_argument('-R', '--region-file', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input regions"
	)
	
	
	parser.add_argument('-u', '--group', type=str, metavar=('name'), nargs='+', action='append',
			help="input groups, specified by name"
	)
	
	parser.add_argument('-U', '--group-file', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input groups"
	)
	
	parser.add_argument('--group-names', type=str, metavar='type', nargs='?', default=False,
			help="the type of the group name(s) provided via --group or --group-file (default: primary labels)"
	)
	
	
	parser.add_argument('-c', '--source', type=str, metavar=('name'), nargs='+', action='append',
			help="input sources, specified by name"
	)
	
	parser.add_argument('-C', '--source-file', type=str, metavar=('file'), nargs='+', action='append',
			help="file(s) from which to load input sources"
	)
	
	
	parser.add_argument('-p', '--prefix', type=str, metavar='prefix', default='biofilter',
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
	
	parser.add_argument('-o', '--output', type=str, metavar=('type'), nargs='+', action='append', choices=['snps','loci','genes','regions','groups','sources'],
			help="data type(s) to filter and annotate, from 'snps', 'loci', 'genes', 'regions', 'groups' and 'sources'"
	)
	
	
	parser.add_argument('-v', '--verbose', action='store_true',
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
			if not os.path.samefile(os.getcwd(), os.path.dirname(__file__)):
				dbPath = os.path.join(os.path.dirname(__file__), args.knowledge)
				if not os.path.exists(dbPath):
					exit("ERROR: knowledge database file '%s' not found in '%s' or '%s'" % (args.knowledge, os.getcwd(), os.path.dirname(__file__)))
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
		bio.attachDatabaseFile(args.knowledge)
	
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
	
	if args.region_locus_tolerance:
		t = args.region_locus_tolerance.strip().upper()
		if t[-1:] == 'B':
			t = t[:-1]
		if t[-1] == 'K':
			t = long(t[:-1]) * 1000
		elif t[-1] == 'M':
			t = long(t[:-1]) * 1000 * 1000
		elif t[-1] == 'G':
			t = long(t[:-1]) * 1000 * 1000 * 1000
		else:
			t = long(t)
		bio.setRegionLocusTolerance(t)
	
	if args.ld_profile:
		bio.setLDProfile(args.ld_profile)
	
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
	
	# apply locus filters
	if args.locus:
		for locusList in args.locus:
			bio.intersectLoci( bio.generateLociFromText(locusList) )
	if args.locus_file:
		for locusFileList in args.locus_file:
			bio.intersectLoci( bio.generateLociFromMapFiles(locusFileList) )
	
	# apply gene filters
	if args.gene:
		for geneList in args.gene:
			bio.intersectGenes( geneList )
	if args.gene_file:
		for geneFileList in args.gene_file:
			bio.intersectGenes( bio.generateNamesFromNameFiles(geneFileList) )
	
	# apply region filters
	if args.region:
		for regionList in args.region:
			bio.intersectRegions( bio.generateRegionsFromText(regionList) )
	if args.region_file:
		for regionFileList in args.region_file:
			bio.intersectRegions( bio.generateRegionsFromFiles(regionFileList) )
	
	# apply group filters
	if args.group:
		for groupList in args.group:
			bio.intersectGroups( groupList )
	if args.group_file:
		for groupFileList in args.group_file:
			bio.intersectGroups( bio.generateNamesFromNameFiles(groupFileList) )
	
	# apply source filters
	if args.source:
		for sourceList in args.source:
			bio.intersectSources( sourceList )
	if args.source_file:
		for sourceFileList in args.source_file:
			bio.intersectSources( bio.generateNamesFromNameFiles(sourceFileList) )
	
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
	
	# output
	for output in (args.output or []):
		outPath = args.prefix + '.' + '-'.join(output)
		bio.log("writing %s to %s ..." % ('-'.join(output),("<stdout>" if args.stdout else outPath)))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			# generateFilteredData() yields (rowid, snp_label, locus_label,chr,pos, gene_label, region_label,chr,posMin,posMax, group_label, source_label)
			headerList = list()
			formatList = list()
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
				for data in bio.generateFilteredData(set(output)):
					outFile.write(formatStr.format(d=data))
			#with outFile
			bio.log(" OK\n")
		#if output ok
	#foreach output
	
#__main__

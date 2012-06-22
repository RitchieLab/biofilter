#!/usr/bin/env python

import argparse
import itertools
import os
import sys
import time

import loki_db


class Biofilter:
	
	
	##################################################
	# public class data
	
	
	ver_maj,ver_min,ver_rev,ver_date = 2,-1,620,'2012-06-20'
	
	
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
	# constructor
	
	
	def __init__(self):
		# initialize instance properties
		self._iwd = os.getcwd()
		self._verbose = False
		self._logFile = sys.stderr
		self._logIndent = 0
		self._logHanging = False
		self._debug = False
		self._expansion = 0
		self._ldprofile = 'n/a'
		self._geneNamespace = None
		self._groupNamespace = None
		
		self._tablesDeindexed = set()
		self._snpFilters = 0
		self._locusFilters = 0
		self._geneFilters = 0
		self._regionFilters = 0
		self._groupFilters = 0
		self._sourceFilters = 0
		
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
	
	
	def setExpansion(self, expansion=0):
		self._expansion = int(expansion)
		self.log("region boundary expansion: %d\n" % self._expansion)
	#setExpansion()
	
	
	def setLDProfile(self, ldprofile='n/a'):
		self._ldprofile = ldprofile.strip()
		self.log("LD profile for region boundary expansion: %s\n" % self._ldprofile)
	#setLDProfile()
	
	
	def setGeneNamespace(self, namespace=None):
		self._geneNamespace = namespace
		self.log("gene name type: %s\n" % ("<label>" if namespace == None else (namespace or "<any>")))
	#setGeneNamespace()
	
	
	def setGroupNamespace(self, namespace=None):
		self._groupNamespace = namespace
		self.log("group name type: %s\n" % ("<label>" if namespace == None else (namespace or "<any>")))
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
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateBiopolymerIDsByName(names, tally=tally, namespaceID=namespaceID, typeID=typeID)):
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
		dbc.executemany(sql, self._loki.generateBiopolymerIDsByName(names, tally=tally, namespaceID=namespaceID, typeID=typeID))
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
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateGroupIDsByName(names, tally=tally, namespaceID=namespaceID, typeID=typeID)):
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
		tally = dict()
		sql = "UPDATE `main`.`group` SET flag = 1 WHERE (1 OR ?) AND group_id = ?"
		dbc.executemany(sql, self._loki.generateGroupIDsByName(names, tally=tally, namespaceID=namespaceID, typeID=typeID))
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
	# filtering & annotation
	
	
	def generateFilteredData(self, snps=None, loci=None, genes=None, regions=None, groups=None, sources=None):
		# gather settings
		expansion = self._expansion
		zoneSize = self._loki.getDatabaseSetting('zone_size')
		zoneSize = int(zoneSize) if zoneSize else None
		ldprofileID = self._loki.getLDProfileID(self._ldprofile)
		
		# define unique aliases for all tables we might need
		aliasTable = {
			"m_s":  "`main`.`snp`",              # (label,rs)
			"m_l":  "`main`.`locus`",            # (label,chr,pos)
			"m_bg": "`main`.`gene`",             # (label,biopolymer_id)
			"m_r":  "`main`.`region`",           # (label,chr,posMin,posMax)
			"m_rz": "`main`.`region_zone`",      # (region_rowid,chr,zone)
			"m_g":  "`main`.`group`",            # (label,group_id)
			"m_c":  "`main`.`source`",           # (label,source_id)
			"d_sl": "`db`.`snp_locus`",          # (rs,chr,pos)
			"d_b":  "`db`.`biopolymer`",         # (biopolymer_id,type_id,label)
			"d_br": "`db`.`biopolymer_region`",  # (biopolymer_id,ldprofile_id,chr,posMin,posMax)
			"d_bz": "`db`.`biopolymer_zone`",    # (biopolymer_id,chr,zone)
			"d_g":  "`db`.`group`",              # (group_id,type_id,label,source_id)
			"d_gb": "`db`.`group_biopolymer`",   # (group_id,biopolymer_id,specificity,implication,quality)
			"d_c":  "`db`.`source`",             # (source_id,source)
		}
		
		# define intermediate tables necessary to join other pairs of tables
		joinTable = {
			("m_s" ,"m_l" ): {"d_sl"},
			("m_s" ,"m_bg"): {"d_sl","d_bz","d_br"},
			("m_s" ,"m_r" ): {"d_sl","m_rz"},
		#	("m_s" ,"m_rz"): {"d_sl"},
			("m_s" ,"m_g" ): {"d_sl","d_bz","d_br","d_gb"},
			("m_s" ,"m_c" ): {"d_sl","d_bz","d_br","d_gb","d_g"},
		#	("m_s" ,"d_sl"): {},
			("m_s" ,"d_b" ): {"d_sl","d_bz","d_br"},
			("m_s" ,"d_br"): {"d_sl","d_bz"},
		#	("m_s" ,"d_bz"): {"d_sl"},
			("m_s" ,"d_g" ): {"d_sl","d_bz","d_br","d_gb"},
		#	("m_s" ,"d_gb"): {"d_sl","d_bz","d_br"},
			("m_s" ,"d_c" ): {"d_sl","d_bz","d_br","d_gb","d_g"},
			
			("m_l" ,"m_bg"): {"d_bz","d_br"},
			("m_l" ,"m_r" ): {"m_rz"},
		#	("m_l" ,"m_rz"): {},
			("m_l" ,"m_g" ): {"d_bz","d_br","d_gb"},
			("m_l" ,"m_c" ): {"d_bz","d_br","d_gb","d_g"},
		#	("m_l" ,"d_sl"): {},
			("m_l" ,"d_b" ): {"d_bz","d_br"},
			("m_l" ,"d_br"): {"d_bz"},
		#	("m_l" ,"d_bz"): {},
			("m_l" ,"d_g" ): {"d_bz","d_br","d_gb"},
		#	("m_l" ,"d_gb"): {"d_bz","d_br"},
			("m_l" ,"d_c" ): {"d_bz","d_br","d_gb","d_g"},
			
			("m_bg","m_r" ): {"d_br"},
		#	("m_bg","m_rz"): {"d_bz"},
			("m_bg","m_g" ): {"d_gb"},
			("m_bg","m_c" ): {"d_gb","d_g"},
			("m_bg","d_sl"): {"d_bz","d_br"},
		#	("m_bg","d_b" ): {},
		#	("m_bg","d_br"): {},
		#	("m_bg","d_bz"): {},
			("m_bg","d_g" ): {"d_gb"},
		#	("m_bg","d_gb"): {},
			("m_bg","d_c" ): {"d_gb","d_g"},
			
		#	("m_r" ,"m_rz"): {},
			("m_r" ,"m_g" ): {"d_br","d_gb"},
			("m_r" ,"m_c" ): {"d_br","d_gb","d_g"},
			("m_r" ,"d_sl"): {"m_rz"},
			("m_r" ,"d_b" ): {"d_br"},
		#	("m_r" ,"d_br"): {},
		#	("m_r" ,"d_bz"): {},
			("m_r" ,"d_g" ): {"d_br","d_gb"},
		#	("m_r" ,"d_gb"): {"d_br"},
			("m_r" ,"d_c" ): {"d_br","d_gb","d_g"},
			
		#	("m_rz","m_g" ): {"m_r","d_br","d_gb"},
		#	("m_rz","m_c" ): {"m_r","d_br","d_gb","d_g"},
		#	("m_rz","d_sl"): {},
		#	("m_rz","d_b" ): {"d_bz"},
		#	("m_rz","d_br"): {"d_bz"},
		#	("m_rz","d_bz"): {},
		#	("m_rz","d_g" ): {"d_bz","d_gr"},
		#	("m_rz","d_gb"): {"d_bz"},
		#	("m_rz","d_c" ): {"d_bz","d_gr","d_g"},
			
			("m_g" ,"m_c" ): {"d_g"},
			("m_g" ,"d_sl"): {"d_gb","d_br","d_bz"},
			("m_g" ,"d_b" ): {"d_gb"},
			("m_g" ,"d_br"): {"d_gb"},
		#	("m_g" ,"d_bz"): {"d_gb"},
		#	("m_g" ,"d_g" ): {},
		#	("m_g" ,"d_gb"): {},
			("m_g" ,"d_c" ): {"d_g"},
			
			("m_c" ,"d_sl"): {"d_g","d_gb","d_br","d_bz"},
			("m_c" ,"d_b" ): {"d_g","d_gb"},
			("m_c" ,"d_br"): {"d_g","d_gb"},
		#	("m_c" ,"d_bz"): {"d_g","d_gb"},
		#	("m_c" ,"d_g" ): {},
		#	("m_c" ,"d_gb"): {"d_g"},
		#	("m_c" ,"d_c" ): {},
			
			("d_sl","d_b" ): {"d_bz","d_br"},
			("d_sl","d_br"): {"d_bz"},
		#	("d_sl","d_bz"): {},
			("d_sl","d_g" ): {"d_bz","d_br","d_gb"},
		#	("d_sl","d_gb"): {"d_bz","d_br"},
			("d_sl","d_c" ): {"d_bz","d_br","d_gb","d_g"},
			
		#	("d_b" ,"d_br"): {},
		#	("d_b" ,"d_bz"): {},
			("d_b" ,"d_g" ): {"d_gb"},
		#	("d_b" ,"d_gb"): {},
			("d_b" ,"d_c" ): {"d_gb","d_g"},
			
		#	("d_br","d_bz"): {},
			("d_br","d_g" ): {"d_gb"},
		#	("d_br","d_gb"): {},
			("d_br","d_c" ): {"d_gb","d_c"},
			
			("d_bz","d_g" ): {"d_gb"},
		#	("d_bz","d_gb"): {},
			("d_bz","d_c" ): {"d_gb","d_c"},
			
		#	("d_g", "d_gb"): {},
		#	("d_g", "d_c" ): {},
			
		#	("d_gb", "d_c" ): {},
		}
		
		# define general constraints for each table
		aliasWhere = {
			"d_br": {"d_br.ldprofile_id = {ldprofileID}"},
		}
		
		# define join constraints for each pair of tables;
		# Note that the SQLite optimizer will not use an index on a column
		# which is modified by an expression, even if the condition could
		# be rewritten otherwise (i.e. "colA = colB + 10" will not use an
		# index on colB).  To account for this, all conditions which include
		# expressions should be repeated once for each operand column to appear
		# unmodified (i.e. "colA = colB + 10" and also "colA - 10 = colB").
		joinWhere = {
			("m_s","d_sl"): {
				"m_s.rs = d_sl.rs"
			},
			("m_l","m_r"): {
				"m_l.chr = m_r.chr",
				"m_l.pos >= (m_r.posMin - {expansion})",
				"m_l.pos <= (m_r.posMax + {expansion})",
				"(m_l.pos + {expansion}) >= m_r.posMin",
				"(m_l.pos - {expansion}) <= m_r.posMax",
			},
			("m_l","m_rz"): {
				"m_l.chr = m_rz.chr",
				"m_l.pos >= ((m_rz.zone * {zoneSize}) - {expansion})",
				"m_l.pos < (((m_rz.zone + 1) * {zoneSize}) + {expansion})",
				"((m_l.pos + {expansion}) / {zoneSize}) >= m_rz.zone",
				"((m_l.pos - {expansion}) / {zoneSize}) <= m_rz.zone",
			},
			("m_l","d_sl"): {
				"m_l.chr = d_sl.chr",
				"m_l.pos = d_sl.pos",
			},
			("m_l","d_br"): {
				"m_l.chr = d_br.chr",
				"m_l.pos >= (d_br.posMin - {expansion})",
				"m_l.pos <= (d_br.posMax + {expansion})",
				"(m_l.pos + {expansion}) >= d_br.posMin",
				"(m_l.pos - {expansion}) <= d_br.posMax",
			},
			("m_l","d_bz"): {
				"m_l.chr = d_bz.chr",
				"m_l.pos >= ((d_bz.zone * {zoneSize}) - {expansion})",
				"m_l.pos < (((d_bz.zone + 1) * {zoneSize}) + {expansion})",
				"((m_l.pos + {expansion}) / {zoneSize}) >= d_bz.zone",
				"((m_l.pos - {expansion}) / {zoneSize}) <= d_bz.zone",
			},
			("m_bg","d_b"): {
				"m_bg.biopolymer_id = d_b.biopolymer_id",
			},
			("m_bg","d_br"): {
				"m_bg.biopolymer_id = d_br.biopolymer_id",
			},
			("m_bg","d_bz"): {
				"m_bg.biopolymer_id = d_bz.biopolymer_id",
			},
			("m_bg","d_gb"): {
				"m_bg.biopolymer_id = d_gb.biopolymer_id",
			},
			("m_r","m_rz"): {
				"m_r.rowid = m_rz.region_rowid",
				# these should all be guaranteed by self.updateRegionZones()
				"m_r.chr = m_rz.chr",
				"m_r.posMin < ((m_rz.zone + 1) * {zoneSize})",
				"m_r.posMax >= (m_rz.zone * {zoneSize})",
				"(m_r.posMin / {zoneSize}) <= m_rz.zone",
				"(m_r.posMax / {zoneSize}) >= m_rz.zone",
			},
			("m_r","d_sl"): {
				"m_r.chr = d_sl.chr",
				"(m_r.posMin - {expansion}) <= d_sl.pos",
				"(m_r.posMax + {expansion}) >= d_sl.pos",
				"m_r.posMin <= (d_sl.pos + {expansion})",
				"m_r.posMax >= (d_sl.pos - {expansion})",
			},
			("m_r","d_br"): {
				#TODO: match by overlap? more complited, but maybe more useful
				"m_r.chr = d_br.chr",
				"m_r.posMin = d_br.posMin",
				"m_r.posMax = d_br.posMax",
			},
			("m_r","d_bz"): {
				"m_r.chr = d_bz.chr",
				"m_r.posMin < ((d_bz.zone + 1) * {zoneSize})",
				"m_r.posMax >= (d_bz.zone * {zoneSize})",
				"(m_r.posMin / {zoneSize}) <= d_bz.zone",
				"(m_r.posMax / {zoneSize}) >= d_bz.zone",
			},
			("m_rz","d_sl"): {
				"m_rz.chr = d_sl.chr",
				"((m_rz.zone * {zoneSize}) - {expansion}) <= d_sl.pos",
				"(((m_rz.zone + 1) * {zoneSize}) + {expansion}) > d_sl.pos",
				"m_rz.zone <= ((d_sl.pos + {expansion}) / {zoneSize})",
				"m_rz.zone >= ((d_sl.pos - {expansion}) / {zoneSize})",
			},
			("m_rz","d_br"): {
				"m_rz.chr = d_br.chr",
				"m_rz.zone >= (d_br.posMin / {zoneSize})",
				"m_rz.zone <= (d_br.posMax / {zoneSize})",
				"((m_rz.zone + 1) * {zoneSize}) > d_br.posMin",
				"(m_rz.zone * {zoneSize}) <= d_br.posMax",
			},
			("m_rz","d_bz"): {
				"m_rz.chr = d_bz.chr",
				"m_rz.zone = d_bz.zone",
			},
			("m_g","d_g"): {
				"m_g.group_id = d_g.group_id",
			},
			("m_g","d_gb"): {
				"m_g.group_id = d_gb.group_id",
			},
			("m_c","d_g"): {
				"m_c.source_id = d_g.source_id",
			},
			("m_c","d_c"): {
				"m_c.source_id = d_c.source_id",
			},
			("d_sl","d_br"): {
				"d_sl.chr = d_br.chr",
				"d_sl.pos >= (d_br.posMin - {expansion})",
				"d_sl.pos <= (d_br.posMax + {expansion})",
				"(d_sl.pos + {expansion}) >= d_br.posMin",
				"(d_sl.pos - {expansion}) <= d_br.posMax",
			},
			("d_sl","d_bz"): {
				"d_sl.chr = d_bz.chr",
				"d_sl.pos >= ((d_bz.zone * {zoneSize}) - {expansion})",
				"d_sl.pos < (((d_bz.zone + 1) * {zoneSize}) + {expansion})",
				"((d_sl.pos + {expansion}) / {zoneSize}) >= d_bz.zone",
				"((d_sl.pos - {expansion}) / {zoneSize}) <= d_bz.zone",
			},
			("d_b","d_br"): {
				"d_b.biopolymer_id = d_br.biopolymer_id",
			},
			("d_b","d_bz"): {
				"d_b.biopolymer_id = d_bz.biopolymer_id",
			},
			("d_b","d_gb"): {
				"d_b.biopolymer_id = d_gb.biopolymer_id",
			},
			("d_br","d_bz"): {
				"d_br.biopolymer_id = d_bz.biopolymer_id",
				"d_br.chr = d_bz.chr",
				# these should all be guaranteed by loki.updateRegionZones()
				"d_br.posMin < ((d_bz.zone + 1) * {zoneSize})",
				"d_br.posMax >= (d_bz.zone * {zoneSize})",
				"(d_br.posMin / {zoneSize}) <= d_bz.zone",
				"(d_br.posMax / {zoneSize}) >= d_bz.zone",
			},
			("d_br","d_gb"): {
				"d_br.biopolymer_id = d_gb.biopolymer_id",
			},
			("d_bz","d_gb"): {
				"d_bz.biopolymer_id = d_gb.biopolymer_id",
			},
			("d_g","d_gb"): {
				"d_g.group_id = d_gb.group_id",
			},
			("d_g","d_c"): {
				"d_g.source_id = d_c.source_id",
			},
		} #joinWhere{}
		
		# initialize query fragments
		columns = [
				'rowid',
				'locus_label','locus_chr','locus_pos',
				'region_label','region_chr','region_posMin','region_posMax',
				'group_label',
				'source_label'
		]
		sqlSelect = { col:"NULL" for col in columns } # {A:a,B:a,...} => SELECT a AS A, b AS B, ...
		sqlSelect['rowid'] = "''"
		sqlFrom = set() # {a,b,...} => FROM aliasTable[a] AS a, aliasTable[b] AS b, ...
		sqlWhere = set() # {a,b,...} => WHERE a AND b AND ...
		sqlGroup = list() # [a,b,...] => GROUP BY a, b, ...
		
		# include all tables needed to satisfy input filters
		if self._snpFilters:
			sqlFrom.add("m_s")
		
		if self._locusFilters:
			sqlFrom.add("m_l")
		
		if self._geneFilters:
			sqlFrom.add("m_bg")
		
		if self._regionFilters:
			sqlFrom.add("m_r")
		
		if self._groupFilters:
			sqlFrom.add("m_g")
		
		if self._sourceFilters:
			sqlFrom.add("m_c")
		
		# include all tables and columns needed to satisfy output column requests
		if loci:
			if "m_l" in sqlFrom:
				sqlSelect['rowid'] += "||m_l.rowid||'_'"
				sqlSelect['locus_label'] = "m_l.label"
				sqlSelect['locus_chr'] = "m_l.chr"
				sqlSelect['locus_pos'] = "m_l.pos"
			elif "m_s" in sqlFrom:
				sqlFrom.add("d_sl")
				sqlSelect['rowid'] += "||d_sl._ROWID_||'_'"
				sqlSelect['locus_label'] = "m_s.label"
				sqlSelect['locus_chr'] = "d_sl.chr"
				sqlSelect['locus_pos'] = "d_sl.pos"
			else:
				sqlFrom.add("d_sl")
				sqlSelect['rowid'] += "||d_sl._ROWID_||'_'"
				sqlSelect['locus_label'] = "'rs'||d_sl.rs"
				sqlSelect['locus_chr'] = "d_sl.chr"
				sqlSelect['locus_pos'] = "d_sl.pos"
		elif snps:
			if "m_s" in sqlFrom:
				sqlSelect['rowid'] += "||m_s.rowid||'_'"
				sqlSelect['locus_label'] = "m_s.label"
			else:
				sqlFrom.add("d_sl")
				sqlSelect['rowid'] += "||d_sl.rs||'_'"
				sqlSelect['locus_label'] = "'rs'||d_sl.rs"
		
		if regions:
			if "m_r" in sqlFrom:
				sqlSelect['rowid'] += "||m_r.rowid||'_'"
				sqlSelect['region_label'] = "m_r.label"
				sqlSelect['region_chr'] = "m_r.chr"
				sqlSelect['region_posMin'] = "m_r.posMin"
				sqlSelect['region_posMax'] = "m_r.posMax"
			elif "m_bg" in sqlFrom:
				sqlFrom.add("d_br")
				sqlSelect['rowid'] += "||d_br._ROWID_||'_'"
				sqlSelect['region_label'] = "m_bg.label"
				sqlSelect['region_chr'] = "d_br.chr"
				sqlSelect['region_posMin'] = "d_br.posMin"
				sqlSelect['region_posMax'] = "d_br.posMax"
			else:
				sqlFrom.add("d_b")
				sqlFrom.add("d_br")
				sqlSelect['rowid'] += "||d_br._ROWID_||'_'"
				sqlSelect['region_label'] = "d_b.label"
				sqlSelect['region_chr'] = "d_br.chr"
				sqlSelect['region_posMin'] = "d_br.posMin"
				sqlSelect['region_posMax'] = "d_br.posMax"
		elif genes:
			if "m_bg" in sqlFrom:
				sqlSelect['rowid'] += "||m_bg._ROWID_||'_'"
				sqlSelect['region_label'] = "m_bg.label"
			else:
				sqlFrom.add("d_b")
				sqlSelect['rowid'] += "||d_b.biopolymer_id||'_'"
				sqlSelect['region_label'] = "d_b.label"
		
		if groups:
			if "m_g" in sqlFrom:
				sqlSelect['rowid'] += "||m_g.rowid||'_'"
				sqlSelect['group_label'] = "m_g.label"
			else:
				sqlFrom.add("d_g")
				sqlSelect['rowid'] += "||d_g.group_id||'_'"
				sqlSelect['group_label'] = "d_g.label"
		
		if sources:
			if "m_c" in sqlFrom:
				sqlSelect['rowid'] += "||m_c.rowid||'_'"
				sqlSelect['source_label'] = "m_c.label"
			else:
				sqlFrom.add("d_c")
				sqlSelect['rowid'] += "||d_c.source_id||'_'"
				sqlSelect['source_label'] = "d_c.source"
		
		# include all tables needed to bridge other included tables
		# (since rules can be interdependent, iterate until nothing changes)
		sizeFrom = None
		while sizeFrom != len(sqlFrom):
			sizeFrom = len(sqlFrom)
			
			for join in joinTable:
				if join[0] in sqlFrom and join[1] in sqlFrom:
					sqlFrom.update(joinTable[join])
		#while
		
		# decide which constraints need to be included
		for alias in aliasWhere:
			if alias in sqlFrom:
				sqlWhere.update(aliasWhere[alias])
		for join in joinWhere:
			if join[0] in sqlFrom and join[1] in sqlFrom:
				sqlWhere.update(joinWhere[join])
		
		# assemble the pieces
		sqlSelect['rowid'] = "("+sqlSelect['rowid']+")"
		sql = "SELECT "+(",\n  ".join(("%s AS %s" % (sqlSelect[col],col)) for col in columns))
		sql += "\nFROM "+(",\n  ".join(aliasTable[t]+" AS "+t for t in sqlFrom) if sqlFrom else "(SELECT 1)")
		sql += "\nWHERE "+("\n  AND ".join(sqlWhere) if sqlWhere else "1")
		if sqlGroup:
			sql += "\nGROUP BY "+(",".join(sqlGroup))
		if "{zoneSize}" in sql and not zoneSize:
			raise Exception("ERROR: knowledge database is missing 'zone_size' setting")
		if "{ldprofileID}" in sql and not ldprofileID:
			raise Exception("ERROR: unknown LD profile '%s'" % self._ldprofile)
		sql = sql.format(expansion=expansion, ldprofileID=ldprofileID, zoneSize=zoneSize)
		
		# make sure any filter tables are indexed
		if "m_s" in sqlFrom:
			self.prepareTableForQuery("snp")
		if "m_l" in sqlFrom:
			self.prepareTableForQuery("locus")
		if "m_bg" in sqlFrom:
			self.prepareTableForQuery("gene")
		if "m_r" in sqlFrom:
			self.prepareTableForQuery("region")
		if "m_rz" in sqlFrom:
			self.prepareTableForQuery("region_zone")
		if "m_g" in sqlFrom:
			self.prepareTableForQuery("group")
		if "m_c" in sqlFrom:
			self.prepareTableForQuery("source")
		
		if self._debug:
			self.log(sql+"\n")
			for row in self._loki._db.cursor().execute("EXPLAIN QUERY PLAN "+sql):
				self.log(str(row)+"\n")
		
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
	version = "Biofilter version %d.%d.%d (%s)" % (
			Biofilter.ver_maj,
			Biofilter.ver_min,
			Biofilter.ver_rev,
			Biofilter.ver_date
	)
	
	# define arguments
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description=version,
	)
	
	parser.add_argument('--version', action='version',
			version=version+"""
%9s version %d.%d.%d (%s)
%9s version %s
%9s version %s
""" % (
				"LOKI",
				loki_db.Database.ver_maj,
				loki_db.Database.ver_min,
				loki_db.Database.ver_rev,
				loki_db.Database.ver_date,
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
			help="attempt to 'prime' the knowledge database file by reading it one or more times,"
			+" hopefully causing the filesystem to cache it into main memory"
	)
	
	parser.add_argument('-x', '--expansion', type=str, metavar='num',
			help="amount by which to expand region boundaries when matching them to loci"
	)
	
	parser.add_argument('-l', '--ldprofile', type=str, metavar='profile',
			help="LD profile with which to expand region boundaries when matching them to loci"
	)
	
	parser.add_argument('-p', '--prefix', type=str, metavar='prefix', default='biofilter',
			help="prefix to use for all output filenames; may contain path components (default: 'biofilter')"
	)
	
	parser.add_argument('--stdout', action='store_true',
			help="return output directly on stdout rather than writing to any files"
	)
	
	parser.add_argument('--overwrite', action='store_true',
			help="overwrite any existing output files",
	)
	
	parser.add_argument('--debug', action='store_true',
			help="print extra debugging information"
	)
	
	
	parser.add_argument('-s', '--snp', type=str, metavar=('rs#'), nargs='+', action='append',
			help="a filtering set of SNPs, specified by RS#"
	)
	
	parser.add_argument('-S', '--snp-file', type=str, metavar=('file'), nargs='+', action='append',
			help="RS# file(s) from which to load a filtering set of SNPs"
	)
	
	parser.add_argument('-m', '--marker', type=str, metavar=('marker'), nargs='+', action='append',
			help="a filtering set of markers, specified by 'chr:pos' or 'chr:label:pos'"
	)
	
	parser.add_argument('-M', '--map-file', type=str, metavar=('file'), nargs='+', action='append',
			help=".map file(s) from which to load a filtering set of markers"
	)
	
	
	parser.add_argument('-g', '--gene', type=str, metavar=('name'), nargs='+', action='append',
			help="a filtering set of genes, specified by name"
	)
	
	parser.add_argument('-G', '--gene-file', type=str, metavar=('file'), nargs='+', action='append',
			help="name file(s) from which to load a filtering set of genes"
	)
	
	parser.add_argument('--gene-names', type=str, metavar='type',
			help="the type of the gene name(s) provided via --gene or --gene-file (default: primary labels)"
	)
	
	parser.add_argument('-r', '--region', type=str, metavar=('region'), nargs='+', action='append',
			help="a filtering set of regions, specified by 'chr:start:stop' or 'chr:label:start:stop'"
	)
	
	parser.add_argument('-R', '--region-file', type=str, metavar=('file'), nargs='+', action='append',
			help="region file(s) from which to load a filtering set of regions"
	)
	
	
	parser.add_argument('-u', '--group', type=str, metavar=('name'), nargs='+', action='append',
			help="a filtering set of groups, specified by name"
	)
	
	parser.add_argument('-U', '--group-file', type=str, metavar=('file'), nargs='+', action='append',
			help="name file(s) from which to load a filtering set of groups"
	)
	
	parser.add_argument('--group-names', type=str, metavar='type',
			help="the type of the group name(s) provided via --group or --group-file (default: primary labels)"
	)
	
	
	parser.add_argument('-c', '--source', type=str, metavar=('name'), nargs='+', action='append',
			help="a filtering set of knowledge sources, specified by name"
	)
	
	parser.add_argument('-C', '--source-file', type=str, metavar=('file'), nargs='+', action='append',
			help="name file(s) from which to load a filtering set of knowledge sources"
	)
	
	
	parser.add_argument('-o', '--output', type=str, metavar=('type'), nargs='+', action='append', choices=['snps','loci','genes','regions','groups','sources'],
			help="data type(s) to filter and annotate, from 'snps', 'loci', 'genes', 'regions', 'groups' and 'sources'"
	)
	
	parser.add_argument('-v', '--verbose', action='store_true',
			help="print warnings and log messages"
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
	if args.expansion:
		e = args.expansion.strip().upper()
		if e[-1:] == 'B':
			e = e[:-1]
		if e[-1] == 'K':
			e = long(e[:-1]) * 1000
		elif e[-1] == 'M':
			e = long(e[:-1]) * 1000 * 1000
		elif e[-1] == 'G':
			e = long(e[:-1]) * 1000 * 1000 * 1000
		else:
			e = long(e)
		bio.setExpansion(e)
	if args.ldprofile:
		bio.setLDProfile(args.ldprofile)
	if args.gene_names:
		bio.setGeneNamespace(args.gene_names or '')
	if args.group_names:
		bio.setGroupNamespace(args.group_names or '')
	
	# apply SNP filters
	if args.snp:
		for snpList in args.snp:
			bio.intersectSNPs( bio.generateRSesFromText(snpList) )
	if args.snp_file:
		for snpFileList in args.snp_file:
			bio.intersectSNPs( bio.generateRSesFromRSFiles(snpFileList) )
	
	# apply locus filters
	if args.marker:
		for markerList in args.marker:
			bio.intersectLoci( bio.generateLociFromText(markerList) )
	if args.map_file:
		for mapFileList in args.map_file:
			bio.intersectLoci( bio.generateLociFromMapFiles(mapFileList) )
	
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
	
	# output
	for output in (args.output or []):
		outPath = args.prefix + '.' + '-'.join(output)
		bio.log("writing %s to %s ..." % ('-'.join(output),("<stdout>" if args.stdout else outPath)))
		if (not args.stdout) and (not args.overwrite) and os.path.exists(outPath):
			bio.log("ERROR: output file '%s' already exists\n" % outPath)
		else:
			# generateFilteredData() yields (rowid, locus_label,chr,pos, region_label,chr,posMin,posMax, group_label, source_label)
			headerList = list()
			formatList = list()
			outS = outL = outBG = outR = outG = outC = False
			for outType in output:
				if outType == 'snps':
					outS = True
					headerList.extend(["snp"])
					formatList.extend(["{d[1]}"])
				elif outType == 'loci':
					outL = True
					headerList.extend(["chr","locus","pos"])
					formatList.extend(["{d[2]}","{d[1]}","{d[3]}"])
				elif outType == 'genes':
					outBG = True
					headerList.extend(["gene"])
					formatList.extend(["{d[4]}"])
				elif outType == 'regions':
					outR = True
					headerList.extend(["chr","region","posMin","posMax"])
					formatList.extend(["{d[5]}","{d[4]}","{d[6]}","{d[7]}"])
				elif outType == 'groups':
					outG = True
					headerList.extend(["group"])
					formatList.extend(["{d[8]}"])
				elif outType == 'sources':
					outC = True
					headerList.extend(["source"])
					formatList.extend(["{d[9]}"])
			#foreach outType
			headerStr = "#" + "\t".join(headerList) + "\n"
			formatStr = "\t".join(formatList) + "\n"
			with (sys.stdout if args.stdout else open(outPath, 'w')) as outFile:
				outFile.write(headerStr)
				for data in bio.generateFilteredData(snps=outS, loci=outL, genes=outBG, regions=outR, groups=outG, sources=outC):
					outFile.write(formatStr.format(d=data))
			#with outFile
			bio.log(" OK\n")
		#if output ok
	#foreach output
	
#__main__

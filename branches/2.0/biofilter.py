#!/usr/bin/env python

import argparse
import os
import sys

import loki_db


class Biofilter:
	
	
	# ##################################################
	# public class data
	
	
	ver_maj,ver_min,ver_rev,ver_date = 2,-1,605,'2012-06-05'
	
	
	# ##################################################
	# private class data
	
	
	_schema = {
		'main': {
			
			# ########## main.snp ##########
			'snp': {
				'table': """
(
  label VARCHAR(32) NOT NULL,
  rs INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'snp__rs': '(rs)',
				}
			}, #.main.snp
			
			# ########## main.locus ##########
			'locus': {
				'table': """
(
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
			
			# ########## main.region ##########
			'region': {
				'table': """
(
  label VARCHAR(32) NOT NULL,
  region_id INTEGER NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'region__region_id': '(region_id)',
				}
			}, #.main.region
			
			# ########## main.bound ##########
			'bound': {
				'table': """
(
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {
					'bound__posmin': '(chr,posMin,posMax)',
					'bound__posmax': '(chr,posMax,posMin)',
				}
			}, #.main.bound
			
			# ########## main.bound_zone ##########
			'bound_zone': {
				'table': """
(
  bound_rowid INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (chr,zone,bound_rowid)
)
""",
				'index': {
					'bound_zone__bound': '(bound_rowid)',
				}
			}, #.main.bound_zone
			
		}, #.main
	} #_schema{}
	
	
	# ##################################################
	# constructor
	
	
	def __init__(self):
		# initialize instance properties
		self._iwd = os.getcwd()
		self._verbose = False
		self._logFile = sys.stderr
		self._logIndent = 0
		self._logHanging = False
		self._expansion = 0
		self._population = 'n/a'
		self._geneNamespace = 'symbol'
		
		self._tablesDeindexed = set()
		self._snpFilters = 0
		self._locusFilters = 0
		self._regionFilters = 0
		self._boundFilters = 0
		
		# initialize instance database
		self._loki = loki_db.Database()
		self._loki.setLogger(self)
		self._loki.createDatabaseTables(self._schema['main'], 'main', '*', True)
	#__init__()
	
	
	# ##################################################
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
	
	
	# ##################################################
	# configuration
	
	
	def setExpansion(self, expansion=0):
		self._expansion = int(expansion)
		self.log("region boundary expansion: %d\n" % self._expansion)
	#setExpansion()
	
	
	def setPopulation(self, population='n/a'):
		self._population = population.strip()
		self.log("population LD profile for region boundary expansion: %s\n" % self._population)
	#setPopulation()
	
	
	def setGeneNamespace(self, namespace='symbol'):
		self._geneNamespace = namespace.strip()
		self.log("gene region name type: %s\n" % self._geneNamespace)
	#setGeneNamespace()
	
	
	# ##################################################
	# database management
	
	
	def attachDatabaseFile(self, dbFile):
		return self._loki.attachDatabaseFile(dbFile)
	#attachDatabaseFile()
	
	
	def prepareTableForUpdate(self, table):
		if table not in self._tablesDeindexed:
			self._tablesDeindexed.add(table)
			self._loki.dropDatabaseIndexes(self._schema['main'], 'main', table)
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, table):
		if table in self._tablesDeindexed:
			self._tablesDeindexed.remove(table)
			self._loki.createDatabaseIndexes(self._schema['main'], 'main', table)
			if table == "bound":
				self.updateBoundZones()
	#prepareTableForQuery()
	
	
	def updateBoundZones(self):
		self.log("calculating region zones ...")
		
		zoneSize = self._loki.getDatabaseSetting('region_zone_size')
		if not zoneSize:
			raise Exception("ERROR: could not determine database setting 'region_zone_size'")
		zoneSize = int(zoneSize)
		dbc = self._db.cursor()
		
		# make sure all regions are correctly oriented
		dbc.execute("UPDATE `main`.`bound` SET posMin = posMax, posMax = posMin WHERE posMin > posMax")
		
		# define zone generator
		def _zones(zoneSize, bounds):
			# bounds=[ (rowid,chr,posMin,posMax),... ]
			# yields:[ (rowid,chr,zone),... ]
			for b in bounds:
				for z in xrange(int(b[2])/zoneSize,(int(b[3])/zoneSize)+1):
					yield (b[0],b[1],z)
		#_zones()
		
		# feed all bounds through the zone generator
		self.prepareTableForUpdate('bound_zone')
		self.prepareTableForQuery('bound')
		dbc.execute("DELETE FROM `main`.`bound_zone`")
		dbc.executemany(
			"INSERT OR IGNORE INTO `main`.`bound_zone` (bound_rowid,chr,zone) VALUES (?,?,?)",
			_zones(
				zoneSize,
				self._db.cursor().execute("SELECT _ROWID_,chr,posMin,posMax FROM `main`.`bound`")
			)
		)
		
		# clean up
		self.prepareTableForQuery('bound_zone')
		self.log(" OK\n")
	#updateBoundZones()
	
	
	# ##################################################
	# input data parsers
	
	
	def generateRSesFromRSFiles(self, rsfiles):
		for path in rsfiles:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as rsfile:
				for line in rsfile:
					if line[0:1] == '#':
						pass
					elif line[0:2].upper() == 'RS':
						yield long(line[2:].rstrip())
					else:
						yield long(line.rstrip())
				#foreach line in rsfile
			#with rsfile
		#foreach rsfile
	#generateRSesFromRSFiles()
	
	
	def generateLociiFromMarkers(self, markers, separator=':'):
		for marker in markers:
			if marker == None:
				continue
			
			label = chm = pos = None
			cols = marker.split(separator)
			
			# parse line
			if len(cols) < 2:
				raise Exception("malformed marker '%s': expected 'chr:pos' or 'chr:label:pos'" % marker)
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
				raise Exception("malformed marker '%s': unknown chromosome '%s'" % (marker,chm))
			chm = self._loki.chr_num[chm]
			
			# parse and convert marker label
			if not label:
				label = 'chr%s:%s' % (self.loki.chr_name[chm], pos)
			#elif label[:2].upper() == 'RS' and label[2:].isdigit():
			#	rs = long(label[2:])
			
			# parse and convert position
			if pos == '-' or pos == 'NA':
				pos = None
			else:
				pos = long(pos)
			
			yield (label,chm,pos)
		#foreach marker
	#generateLociiFromMarkers()
	
	
	def generateLociiFromMapFiles(self, mapfiles):
		for path in mapfiles:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as mapfile:
				for locus in self.generateLociiFromMarkers(
						(line.rstrip() for line in mapfile if line[0:1] != '#'),
						"\t"
				):
					yield locus
				#foreach generated locus
			#with mapfile
		#foreach mapfile
	#generateLociiFromMapFiles()
	
	
	def generateNamesFromNameFiles(self, namefiles):
		for path in namefiles:
			with (sys.stdin if (path == '-' or not path) else open(path, 'rU')) as namefile:
				for line in namefile:
					if line[0:1] == '#':
						pass
					else:
						yield line.rstrip()
				#foreach line in namefile
			#with namefile
		#foreach namefile
	#generateNamesFromNameFiles()
	
	
	# ##################################################
	# snp/locus input
	
	
	def unionSNPs(self, snps):
		# snps=[ rs, ... ]
		self.log("adding SNP filter ...")
		self.prepareTableForUpdate('snp')
		dbc = self._loki._db.cursor()
		sql = "INSERT INTO `main`.`snp` (label,rs) VALUES ('rs'||?,?); SELECT 1"
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateCurrentRSesByRS(snps, tally=tally)):
			numAdd += 1
		self.log(" OK: added %d RS#s (%d matched, %d updated, %d ambiguous, %d unrecognized)\n" % (
				numAdd,tally['match'],tally['merge'],tally['ambig'],tally['null']
		))
		self._snpFilters += 1
	#unionSNPs()
	
	
	def intersectSNPs(self, snps):
		# snps=[ rs, ... ]
		if not self._snpFilters:
			return self.unionSNPs(snps)
		self.log("intersecting SNP filter ...")
		self.prepareTableForQuery('snp')
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`snp` SET flag = 0")
		numBefore = self._loki._db.changes()
		tally = dict()
		sql = "UPDATE `main`.`snp` SET flag = 1 WHERE (1 OR ?) AND rs = ?"
		dbc.executemany(sql, self._loki.generateCurrentRSesByRS(snps, tally=tally))
		dbc.execute("DELETE FROM `main`.`snp` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d RS#s (%d dropped, %d ambiguous, %d unrecognized)\n" % (
				numBefore-numDrop,numDrop,tally['ambig'],tally['null']
		))
		self._snpFilters += 1
	#intersectSNPs()
	
	
	def unionLocii(self, locii):
		# locii=[ (label,chr,pos), ... ]
		self.log("adding locus filter ...")
		self.prepareTableForUpdate('locus')
		dbc = self._loki._db.cursor()
		sql = "INSERT OR IGNORE INTO `main`.`locus` (label,chr,pos) VALUES (?,?,?); SELECT LAST_INSERT_ROWID()"
		lastID = None
		numAdd = numNull = 0
		for row in dbc.executemany(sql, locii):
			if lastID != row[0]:
				numAdd += 1
				lastID = row[0]
			else:
				numNull += 1
		self.log(" OK: added %d locii (%d incomplete)\n" % (numAdd,numNull))
		self._locusFilters += 1
	#unionLocii()
	
	
	def intersectLocii(self, locii):
		# locii=[ (label,chr,pos), ... ]
		if not self._locusFilters:
			return self.unionLocii(locii)
		self.log("intersecting locus filter ...")
		self.prepareTableForQuery('locus')
		dbc = self._loki._db.cursor()
		dbc.execute("UPDATE `main`.`locus` SET flag = 0")
		numBefore = self._loki._db.changes()
		dbc.executemany("UPDATE `main`.`locus` SET flag = 1 WHERE (1 OR ?) AND chr = ? AND pos = ?", locii)
		dbc.execute("DELETE FROM `main`.`locus` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d locii (%d dropped)\n" % (numBefore-numDrop,numDrop))
		self._locusFilters += 1
	#intersectLocii()
	
	
	# ##################################################
	# region/bound input
	
	
	def unionRegions(self, names, ntype=None, rtype='gene'):
		# names=[ rs, ... ]
		
		self.log("adding %s filter ..." % rtype)
		ntype = ntype or self._geneNamespace
		namespaceID = ntype and self._loki.getNamespaceID(ntype)
		if ntype and not namespaceID:
			raise Exception("ERROR: unknown name type '%s'" % ntype)
		typeID = self._loki.getTypeID(rtype)
		if not typeID:
			raise Exception("ERROR: unknown region type '%s'" % rtype)
		self.prepareTableForUpdate('region')
		dbc = self._loki._db.cursor()
		
		sql = "INSERT INTO `main`.`region` (label,region_id) VALUES (?,?); SELECT 1"
		tally = dict()
		numAdd = 0
		for row in dbc.executemany(sql, self._loki.generateRegionIDsByName(names, namespaceID=namespaceID, typeID=typeID, tally=tally)):
			numAdd += 1
		self.log(" OK: added %d %s regions (%d matched, %d ambiguous, %d unrecognized)\n" % (
				numAdd,rtype,tally['match'],tally['ambig'],tally['null']
		))
		self._regionFilters += 1
	#unionRegions()
	
	
	def intersectRegions(self, names, ntype=None, rtype='gene'):
		# names=[ rs, ... ]
		if not self._regionFilters:
			return self.unionRegions(names, ntype, rtype)
		
		self.log("intersecting %s filter ..." % rtype)
		ntype = ntype or self._geneNamespace
		namespaceID = ntype and self._loki.getNamespaceID(ntype)
		if ntype and not namespaceID:
			raise Exception("ERROR: unknown name type '%s'" % ntype)
		typeID = self._loki.getTypeID(rtype)
		if not typeID:
			raise Exception("ERROR: unknown region type '%s'" % rtype)
		self.prepareTableForQuery('region')
		dbc = self._loki._db.cursor()
		
		dbc.execute("UPDATE `main`.`region` SET flag = 0")
		numBefore = self._loki._db.changes()
		tally = dict()
		sql = "UPDATE `main`.`region` SET flag = 1 WHERE (1 OR ?) AND region_id = ?"
		dbc.executemany(sql, self._loki.generateRegionIDsByName(names, namespaceID=namespaceID, typeID=typeID, tally=tally))
		dbc.execute("DELETE FROM `main`.`region` WHERE flag = 0")
		numDrop = self._loki._db.changes()
		self.log(" OK: kept %d %s regions (%d dropped, %d ambiguous, %d unrecognized)\n" % (
				numBefore-numDrop,rtype,numDrop,tally['ambig'],tally['null']
		))
		self._regionFilters += 1
	#intersectRegions()
	
	
	def unionBounds(self, bounds):
		raise Exception("not implemented")
	#unionBounds()
	
	
	def intersectBounds(self, bounds):
		raise Exception("not implemented")
	#intersectBounds()
	
	
	# ##################################################
	# filtering
	
	
	def generateFilteredData(self, snps=None, regions=None):
		# gather settings
		expansion = self._expansion
		populationID = self._loki.getPopulationID(self._population)
		if not populationID:
			raise Exception("ERROR: unknown population '%s'" % self._population)
		zoneSize = self._loki.getDatabaseSetting('region_zone_size')
		if not zoneSize:
			raise Exception("ERROR: could not determine database setting 'region_zone_size'")
		zoneSize = int(zoneSize)
		
		# define unique aliases for all tables we might need
		aliasTable = {
			"ms":  "`main`.`snp`",
			"ml":  "`main`.`locus`",
			"mr":  "`main`.`region`",
			"mb":  "`main`.`bound`",
			"mbz": "`main`.`bound_zone`",
			"ds":  "`db`.`snp`",
			"dr":  "`db`.`region`",
			"drb": "`db`.`region_bound`",
			"drz": "`db`.`region_zone`",
			"dg":  "`db`.`group`",
			"dgg": "`db`.`group_group`",
			"dgr": "`db`.`group_region`",
		}
		
		# define general constraints for each table
		aliasWhere = {
			"drb": {"drb.population_id = {populationID}"},
			"drz": {"drz.population_id = {populationID}"},
		}
		
		# define join constraints for each pair of tables
		joinWhere = {
			("ms","ds"): {
				"ms.rs = ds.rs"
			},
			("ml","mb"): {
				"ml.chr = mb.chr",
				"ml.pos >= (mb.posMin - {expansion})",
				"ml.pos <= (mb.posMax + {expansion})",
				"(ml.pos + {expansion}) >= mb.posMin",
				"(ml.pos - {expansion}) <= mb.posMax",
			},
			("ml","mbz"): {
				"ml.chr = mbz.chr",
				"ml.pos >= ((mbz.zone * {zoneSize}) - {expansion})",
				"ml.pos < (((mbz.zone + 1) * {zoneSize}) + {expansion})",
				"((ml.pos + {expansion}) / {zoneSize}) >= mbz.zone",
				"((ml.pos - {expansion}) / {zoneSize}) <= mbz.zone",
			},
			("ml","ds"): {
				"ml.chr = ds.chr",
				"ml.pos = ds.pos",
			},
			("ml","drb"): {
				"ml.chr = drb.chr",
				"ml.pos >= (drb.posMin - {expansion})",
				"ml.pos <= (drb.posMax + {expansion})",
				"(ml.pos + {expansion}) >= drb.posMin",
				"(ml.pos - {expansion}) <= drb.posMax",
			},
			("ml","drz"): {
				"ml.chr = drz.chr",
				"ml.pos >= ((drz.zone * {zoneSize}) - {expansion})",
				"ml.pos < (((drz.zone + 1) * {zoneSize}) + {expansion})",
				"((ml.pos + {expansion}) / {zoneSize}) >= drz.zone",
				"((ml.pos - {expansion}) / {zoneSize}) <= drz.zone",
			},
			("mr","dr"): {
				"mr.region_id = dr.region_id",
			},
			("mr","drb"): {
				"mr.region_id = drb.region_id",
			},
			("mb","mbz"): {
				"mb._ROWID_ = mbz.bound_rowid",
				# these should all be guaranteed by self.updateBoundZones()
				"mb.chr = mbz.chr",
				"mb.posMin < ((mbz.zone + 1) * {zoneSize})",
				"mb.posMax >= (mbz.zone * {zoneSize})",
				"(mb.posMin / {zoneSize}) <= mbz.zone",
				"(mb.posMax / {zoneSize}) >= mbz.zone",
			},
			("mb","ds"): {
				"mb.chr = ds.chr",
				"(mb.posMin - {expansion}) <= ds.pos",
				"(mb.posMax + {expansion}) >= ds.pos",
				"mb.posMin <= (ds.pos + {expansion})",
				"mb.posMax >= (ds.pos - {expansion})",
			},
			("mb","drb"): {
				#TODO: match by overlap? more complited, but maybe more useful
				"mb.chr = drb.chr",
				"mb.posMin = drb.posMin",
				"mb.posMax = drb.posMax",
			},
			("mbz","ds"): {
				"mbz.chr = ds.chr",
				"((mbz.zone * {zoneSize}) - {expansion}) <= ds.pos",
				"(((mbz.zone + 1) * {zoneSize}) + {expansion}) > ds.pos",
				"mbz.zone <= ((ds.pos + {expansion}) / {zoneSize})",
				"mbz.zone >= ((ds.pos - {expansion}) / {zoneSize})",
			},
			("mbz","drb"): {
				#TODO
			},
			("mbz","drz"): {
				"mbz.chr = drz.chr",
				"mbz.zone = drz.zone",
			},
			("ds","drb"): {
				"ds.chr = drb.chr",
				"ds.pos >= (drb.posMin - {expansion})",
				"ds.pos <= (drb.posMax + {expansion})",
				"(ds.pos + {expansion}) >= drb.posMin",
				"(ds.pos - {expansion}) <= drb.posMax",
			},
			("ds","drz"): {
				"ds.chr = drz.chr",
				"ds.pos >= ((drz.zone * {zoneSize}) - {expansion})",
				"ds.pos < (((drz.zone + 1) * {zoneSize}) + {expansion})",
				"((ds.pos + {expansion}) / {zoneSize}) >= drz.zone",
				"((ds.pos - {expansion}) / {zoneSize}) <= drz.zone",
			},
			("dr","drb"): {
				"dr.region_id = drb.region_id",
			},
			("dr","drz"): {
				"dr.region_id = drz.region_id",
			},
			("drb","drz"): {
				"drb.region_id = drz.region_id",
				"drb.population_id = drz.population_id",
				"drb.chr = drz.chr",
				# these should all be guaranteed by loki.updateRegionZones()
				"drb.posMin < ((drz.zone + 1) * {zoneSize})",
				"drb.posMax >= (drz.zone * {zoneSize})",
				"(drb.posMin / {zoneSize}) <= drz.zone",
				"(drb.posMax / {zoneSize}) >= drz.zone",
			},
		} #joinWhere{}
		
		# initialize query fragments
		sqlSelect = list() # [a,b,...] => SELECT a, b, ...
		sqlFrom = set() # {a,b,...} => FROM aliasTable[a] AS a, aliasTable[b] AS b, ...
		sqlWhere = set() # {a,b,...} => WHERE a AND b AND ...
		sqlGroup = list() # [a,b,...] => GROUP BY a, b, ...
		
		# decide which tables need to be included
		if self._snpFilters:
			sqlFrom.add("ms")
			sqlFrom.add("ds")
		
		if self._locusFilters:
			sqlFrom.add("ml")
		
		if snps and not (self._snpFilters or self._locusFilters):
			sqlFrom.add("ds")
		
		if self._regionFilters:
			sqlFrom.add("mr")
			sqlFrom.add("dr")
			sqlFrom.add("drb")
		
		if self._boundFilters:
			sqlFrom.add("mb")
		
		if regions and not (self._regionFilters or self._boundFilters):
			sqlFrom.add("dr")
			sqlFrom.add("drb")
		
		if "mb" in sqlFrom and ("ml" in sqlFrom or "ds" in sqlFrom):
			sqlFrom.add("mbz")
		
		if "drb" in sqlFrom and ("ml" in sqlFrom or "ds" in sqlFrom):
			sqlFrom.add("drz")
		
		# decide which constraints need to be included
		for alias in aliasWhere:
			if alias in sqlFrom:
				sqlWhere.update(aliasWhere[alias])
		for join in joinWhere:
			if join[0] in sqlFrom and join[1] in sqlFrom:
				sqlWhere.update(joinWhere[join])
		
		# decide which columns to select for output
		if snps:
			if "ml" in sqlFrom:
				sqlSelect.append("ml.label")
				sqlSelect.append("ml.chr")
				sqlSelect.append("ml.pos")
				sqlGroup.append("ml._ROWID_")
			elif "ds" in sqlFrom:
				sqlSelect.append("'rs'||ds.rs AS label")
				sqlSelect.append("ds.chr")
				sqlSelect.append("ds.pos")
				sqlGroup.append("ds._ROWID_")
			else:
				raise Exception("ERROR: constructed query includes no snp tables")
		
		if regions:
			if "mb" in sqlFrom:
				sqlSelect.append("mb.label")
				sqlSelect.append("mb.chr")
				sqlSelect.append("mb.posMin")
				sqlSelect.append("mb.posMax")
				sqlGroup.append("mb._ROWID_")
			elif "dr" in sqlFrom and "drb" in sqlFrom:
				sqlSelect.append("dr.label")
				sqlSelect.append("drb.chr")
				sqlSelect.append("drb.posMin")
				sqlSelect.append("drb.posMax")
				sqlGroup.append("drb._ROWID_")
			else:
				raise Exception("ERROR: constructed query includes no region tables")
		
		# assemble the pieces
		sql = "SELECT\n  "+(",\n  ".join(sqlSelect) if sqlSelect else "1")
		sql += "\nFROM "+(",\n  ".join(aliasTable[t]+" AS "+t for t in sqlFrom) if sqlFrom else "(SELECT 1)")
		sql += "\nWHERE "+("\n  AND ".join(sqlWhere) if sqlWhere else "1")
		if sqlGroup:
			sql += "\nGROUP BY "+(",".join(sqlGroup))
		sql = sql.format(expansion=expansion, populationID=populationID, zoneSize=zoneSize)
		
		# make sure any filter tables are indexed
		if "ms" in sqlFrom:
			self.prepareTableForQuery("snp")
		if "ml" in sqlFrom:
			self.prepareTableForQuery("locus")
		if "mr" in sqlFrom:
			self.prepareTableForQuery("region")
		if "mb" in sqlFrom:
			self.prepareTableForQuery("bound")
		if "mbz" in sqlFrom:
			self.prepareTableForQuery("bound_zone")
		
		# run and return
		self.log(sql+"\n")
		for row in self._loki._db.cursor().execute("EXPLAIN QUERY PLAN "+sql):
			self.log(str(row)+"\n")
		for row in self._loki._db.cursor().execute(sql):
			yield row
	#generateFilteredData()
	
	
	def generateFilteredLocii(self):
		#TODO
		return self.generateVariants()
	#generateFilteredLocii()
	
	
	def generateFilteredRegions(self):
		#TODO
		return self.generateRegions()
	#generateFilteredRegions()
	
	
	# ##################################################
	# annotation
	
	
	def generateAnnotatedLociiRegions(self, locii, rtype='gene'):
		expansion = self._expansion
		populationID = self._loki.getPopulationID(self._population)
		if not populationID:
			raise Exception("ERROR: unknown population '%s'" % self._population)
		typeID = self._loki.getTypeID(rtype)
		if not typeID:
			raise Exception("ERROR: unknown region type '%s'" % rtype)
		zonesize = int(self._loki.getDatabaseSetting('region_zone_size'))
		if not zonesize:
			raise Exception("ERROR: missing 'region_zone_size' setting in knowledge database")
		
		# an expansion value might cause a locus near a zone boundary to map to
		# both neighboring zones, and therefore map to the same region twice;
		# to suppress this effect we use DISTINCT, but only when expansion > 0
		# since otherwise it isn't needed and adds a performance overhead
		sql = """
SELECT {distinct}
  i_l.label,
  i_l.rs,
  i_l.chr,
  i_l.pos,
  d_r.label,
  d_r.region_id,
  d_rb.posMin,
  d_rb.posMax
FROM (SELECT ? AS label, ? AS rs, ? AS chr, ? AS pos) AS i_l
JOIN `db`.`region_zone` AS d_rz
  ON d_rz.population_id = {populationID}
  AND d_rz.chr = i_l.chr
  AND d_rz.zone >= (i_l.pos - {expansion}) / {zonesize}
  AND d_rz.zone <= (i_l.pos + {expansion}) / {zonesize}
JOIN `db`.`region_bound` AS d_rb
  ON d_rb.region_id = d_rz.region_id
  AND d_rb.population_id = d_rz.population_id
  AND d_rb.chr = d_rz.chr
  AND d_rb.posMin <= (i_l.pos + {expansion})
  AND d_rb.posMax >= (i_l.pos - {expansion})
JOIN `db`.`region` AS d_r
  ON d_r.region_id = d_rb.region_id
  AND d_r.type_id = {typeID}
""".format(
				distinct=("DISTINCT" if expansion else ""),
				expansion=expansion,
				populationID=populationID,
				typeID=typeID,
				zonesize=zonesize
		)
		for row in self._loki._db.cursor().executemany(sql, locii):
			yield row
	#generateAnnotatedLociiRegions()
	
	
	# ##################################################
	# model generation
	
	
	def outputLociiModels(self, etype='gene', target=sys.stdout): #TODO
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
	#outputLociiModels() #TODO
	
	
	def outputRegionModels(self, rtype='gene', target=sys.stdout): #TODO
		typeID = self._loki.getTypeID(rtype)
		if not typeID:
			sys.stderr.write("ERROR: unknown region type '%s'\n" % rtype)
			sys.exit(1)
		populationID = self._loki.getPopulationID(self._population)
		if not populationID:
			sys.stderr.write("ERROR: unknown population '%s'\n" % self._population)
			sys.exit(1)
		
		# map locii to known regions
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
		self._loki.createDatabaseIndexes(self._schema['temp'], 'temp', 'region')
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
		self._loki.createDatabaseIndexes(self._schema['temp'], 'temp', 'group')
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
	
	parser.add_argument('-x', '--expansion', type=str, metavar='num',
			help="amount by which to expand region boundaries when matching them to locii"
	)
	
	parser.add_argument('-p', '--population', type=str, metavar='label',
			help="LD profile with which to expand region boundaries when matching them to locii"
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
			help="the type of the gene name(s) provided via --gene or --gene-file"
	)
	
	parser.add_argument('-r', '--region', type=str, metavar=('region'), nargs='+', action='append',
			help="a filtering set of regions, specified by 'chr:start:stop' or 'chr:label:start:stop'"
	)
	
	parser.add_argument('-R', '--region-file', type=str, metavar=('file'), nargs='+', action='append',
			help="region file(s) from which to load a filtering set of regions"
	)
	
	
	parser.add_argument('-o', '--output', type=str, metavar=('type'), nargs=1, action='append', choices=['snps','genes'],
			help="filtered data type to output, among 'snps' and 'genes'"
	)
	
	parser.add_argument('-a', '--annotate', type=str, metavar=('type','type'), nargs=2, action='append', choices=['snps','genes'],
			help="pair of data types to cross-reference and annotate, among 'snps' and 'genes'"
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
	
	# parse arguments
	args = parser.parse_args()
	bio = Biofilter()
	bio.setVerbose(args.verbose)
	if args.knowledge:
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
	if args.population:
		bio.setPopulation(args.population)
	if args.gene_names:
		bio.setGeneNamespace(args.gene_names)
	
	# apply SNP filters
	if args.snp:
		for snpList in args.snp:
			bio.intersectSNPs( (long(snp[2:]) if snp[0:2].upper() == 'RS' else long(snp)) for snp in snpList )
	if args.snp_file:
		for snpFileList in args.snp_file:
			bio.intersectSNPs( bio.generateRSesFromRSFiles(snpFileList) )
	
	# apply locus filters
	if args.marker:
		for markerList in args.marker:
			bio.intersectLocii( bio.generateLociiFromMarkers(markerList) )
	if args.map_file:
		for mapFileList in args.map_file:
			bio.intersectLocii( bio.generateLociiFromMapFiles(mapFileList) )
	
	# apply gene filters
	if args.gene:
		for geneList in args.gene:
			bio.intersectRegions( geneList )
	if args.gene_file:
		for geneFileList in args.gene_file:
			bio.intersectRegions( bio.generateNamesFromNameFiles(geneFileList) )
	
	# output
	if args.output:
		for o in args.output:
			if o[0] == 'snps':
				print "\t".join(("snp","chr","pos"))
				for data in bio.generateFilteredData(snps=True):
					print "\t".join(str(col) for col in data)
			elif o[0] == 'genes':
				print "\t".join(("gene","chr","posMin","posMax"))
				for data in bio.generateFilteredData(regions=True):
					print "\t".join(str(col) for col in data)
			else:
				print "%s output not implemented" % o
		#foreach output
	#if output
	
	# annotate
	if args.annotate:
		for a in args.annotate:
			if a[0] == 'snps' and a[1] == 'genes':
				print "\t".join(("snp","chr","pos","gene","chr","posMin","posMax"))
				for data in bio.generateFilteredData(snps=True, regions=True):
					print "\t".join(str(col) for col in data)
			else:
				print "%s annotation not implemented" % a
		#foreach annotate
	#if annotate
	
#__main__


"""
h	help
k	knowledge

s	SNPs (rs#)
m	markers (map)
g	genes (symbol)
r	regions (map)
?	groups (name)
?	networks (symbols)
c	sources (name)

p	population
x	expansion
o	output
"""

#!/usr/bin/env python

import argparse
import os
import sys

import loki_db


class Biofilter:
	
	
	# ##################################################
	# public class data
	
	
	ver_maj,ver_min,ver_rev,ver_date = 2,-1,601,'2012-06-01'
	
	
	# ##################################################
	# private class data
	
	
	_schema = {
		'main': {
			
			# ########## main.locus ##########
			'locus': {
				'table': """
(
  label VARCHAR(32) NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (label,chr,pos)
)
""",
				'index': {
					'locus__pos': '(chr,pos)',
				}
			}, #.main.locus
			
			# ########## main.snp ##########
			'snp': {
				'table': """
(
  rs INTEGER PRIMARY KEY NOT NULL,
  flag TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {}
			}, #.main.snp
			
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
		self._snpFilters = 0
		self._locusFilters = 0
		
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
	
	
	def attachDatabaseFile(self, dbFile):
		return self._loki.attachDatabaseFile(dbFile)
	#attachDatabaseFile()
	
	
	def setExpansion(self, expansion=0):
		self._expansion = int(expansion)
		self.log("region boundary expansion: %d\n" % self._expansion)
	#setExpansion()
	
	
	def setPopulation(self, population='n/a'):
		self._population = population.strip()
		self.log("population LD profile for region boundary expansion: %s\n" % self._population)
	#setPopulation()
	
	
	# ##################################################
	# input data parsers
	
	
	def generateRSesFromRSFiles(self, rsfiles):
		for path in rsfiles:
			with open(path, 'rU') as rsfile:
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
			with open(path, 'rU') as mapfile:
				for locus in self.generateLociiFromMarkers(
						(line.rstrip() for line in mapfile if line[0:1] != '#'),
						"\t"
				):
					yield locus
				#foreach generated locus
			#with mapfile
		#foreach mapfile
	#generateLociiFromMapFiles()
	
	
	# ##################################################
	# filter management
	
	
	def intersectSNPs(self, snps):
		dbc = self._loki._db.cursor()
		
		if self._snpFilters:
			dbc.execute("UPDATE `main`.`snp` SET flag = 0")
			dbc.executemany("UPDATE `main`.`snp` SET flag = 1 WHERE rs = ?", snps)
			dbc.execute("DELETE FROM `main`.`snp` WHERE flag = 0")
		else:
			self.log("adding SNP filter ...")
			rs = None
			total = found = 0
			for row in dbc.executemany("INSERT OR REPLACE INTO `main`.`snp` (rs) SELECT rs FROM `db`.`snp` WHERE rs = ?; SELECT LAST_INSERT_ROWID()", snps):
				total += 1
				if rs != row[0]:
					found += 1
					rs = row[0]
			self.log(" OK: added %d of %d RS#s (%d unrecognized)\n" % (found,total,total-found))
		
		self._snpFilters += 1
	#intersectSNPs()
	
	
	def intersectLocii(self, locii):
		dbc = self._loki._db.cursor()
		
		self.log("adding locus filter ...")
		if self._locusFilters:
			dbc.execute("UPDATE `main`.`locus` SET flag = 0")
			dbc.executemany("UPDATE `main`.`locus` SET flag = 1 WHERE (1 OR ?) AND chr = ? AND pos = ?", locii)
			dbc.execute("DELETE FROM `main`.`locus` WHERE flag = 0")
		else:
			dbc.executemany("INSERT OR IGNORE INTO `main`.`locus` (label,chr,pos) VALUES (?,?,?)", locii)
		self.log(" OK\n")
		
		self._locusFilters += 1
	#intersectLocii()
	
	
	# ##################################################
	# annotation
	
	
	def generateLocii(self):
		if self._snpFilters and self._locusFilters:
			sql = """
SELECT 'rs'||d_s.rs AS label, d_s.chr, d_s.pos
FROM `main`.`snp` AS m_s
JOIN `db`.`snp` AS d_s USING (rs)
UNION
SELECT label, chr, pos
FROM `main`.`locus` AS m_l
"""
		elif self._snpFilters:
			sql = """
SELECT 'rs'||d_s.rs AS label, d_s.chr, d_s.pos
FROM `main`.`snp` AS m_s
JOIN `db`.`snp` AS d_s USING (rs)
"""
		elif self._locusFilters:
			sql = """
SELECT label, chr, pos
FROM `main`.`locus` AS m_l
"""
		else:
			sql = """
SELECT 'rs'||d_s.rs AS label, d_s.chr, d_s.pos
FROM `db`.`snp` AS d_s
"""
		#if _snpFilters and/or _locusFilters
		
		for row in self._loki._db.cursor().execute(sql):
			yield row
	#generateLocii()
	
	
	def generateLociiRegions(self, locii, rtype='gene'):
		expansion = self._expansion
		populationID = self._loki.getPopulationID(self._population)
		typeID = self._loki.getTypeID(rtype)
		zonesize = int(self._loki.getDatabaseSetting('region_zone_size'))
		
		sql = """
SELECT DISTINCT i_l.label, i_l.chr, i_l.pos, d_r.region_id, d_r.label, d_rb.posMin, d_rb.posMax
FROM (SELECT ? AS label, ? AS chr, ? AS pos) AS i_l
JOIN `db`.`region_zone` AS d_rz
  ON d_rz.population_id = %d
  AND d_rz.chr = i_l.chr
  AND d_rz.zone >= (i_l.pos - %d) / %d
  AND d_rz.zone <= (i_l.pos + %d) / %d
JOIN `db`.`region_bound` AS d_rb
  ON d_rb.region_id = d_rz.region_id
  AND d_rb.population_id = d_rz.population_id
  AND d_rb.chr = d_rz.chr
  AND d_rb.posMin <= (i_l.pos + %d)
  AND d_rb.posMax >= (i_l.pos - %d)
JOIN `db`.`region` AS d_r
  ON d_r.region_id = d_rb.region_id
  AND d_r.type_id = %d
""" % (populationID,expansion,zonesize,expansion,zonesize,expansion,expansion,typeID)
		for row in self._loki._db.cursor().executemany(sql, locii):
			yield row
	#generateLociiRegions()
	
	
	def outputLociiRegions(self, rtype='gene', target=sys.stdout): #TODO
		typeID = self._loki.getTypeID(rtype)
		if not typeID:
			sys.stderr.write("ERROR: unknown region type '%s'\n" % rtype)
			sys.exit(1)
		populationID = self._loki.getPopulationID(self._population)
		if not populationID:
			sys.stderr.write("ERROR: unknown population '%s'\n" % self._population)
			sys.exit(1)
		
		target.write(
				("#chr\tlabel\tpos"
				+"\t%s_name.match\t%s_start.match\t%s_end.match"
				+"\t%s_name.upstream\t%s_start.upstream\t%s_end.upstream"
				+"\t%s_name.downstream\t%s_start.downstream\t%s_end.downstream"
				+"\n")
				% (rtype,rtype,rtype,rtype,rtype,rtype,rtype,rtype,rtype)
		)
		for row in self._loki._dbc.execute("""
SELECT
  COALESCE(lM.chr, 'NA') AS chr,
  COALESCE(
    lM.label,
    'rs' || lM.rs,
    'chr' || lM.chr || ':' || lM.pos,
    '!#' || lM.rowid
  ) AS label,
  COALESCE(lM.pos, 'NA') AS pos,
  lM.labelM,
  lM.posMinM,
  lM.posMaxM,
  COALESCE(rU.label, '') AS labelU,
  COALESCE(rbU.posMin, '') AS posMinU,
  COALESCE(rbU.posMax, '') AS posMaxU,
  COALESCE(rD.label, '') AS labelD,
  COALESCE(rbD.posMin, '') AS posMinD,
  COALESCE(rbD.posMax, '') AS posMaxD
FROM (
  SELECT
    l.rowid,
    l.label,
    l.rs,
    l.chr,
    l.pos,
    (CASE WHEN rM.region_id IS NULL THEN '' ELSE GROUP_CONCAT(DISTINCT rM.label) END) AS labelM,
    (CASE WHEN rM.region_id IS NULL THEN '' ELSE MIN(rbM.posMin) END) AS posMinM,
    (CASE WHEN rM.region_id IS NULL THEN '' ELSE MAX(rbM.posMax) END) AS posMaxM,
    (
      SELECT rbU._rowid_
      FROM db.region_bound AS rbU
      JOIN db.region AS rU USING (region_id)
      WHERE rbU.population_id = :population_id
        AND rbU.chr = l.chr
        AND rbU.posMax < l.pos - :expand
        AND rU.type_id = :type_id
      ORDER BY rbU.posMax DESC
      LIMIT 1
    ) AS rbU_rowid,
    (
      SELECT rbD._rowid_
      FROM db.region_bound AS rbD
      JOIN db.region AS rD USING (region_id)
      WHERE rbD.population_id = :population_id
        AND rbD.chr = l.chr
        AND rbD.posMin > l.pos + :expand
        AND rD.type_id = :type_id
      ORDER BY rbD.posMin ASC
      LIMIT 1
    ) AS rbD_rowid
  FROM main.locus AS l
  LEFT JOIN db.region_zone AS rzM
    ON rzM.population_id = :population_id
    AND rzM.chr = l.chr
    AND rzM.zone >= (l.pos - :expand) / 100000
    AND rzM.zone <= (l.pos + :expand) / 100000
  LEFT JOIN db.region_bound AS rbM
    ON rbM.region_id = rzM.region_id
    AND rbM.population_id = :population_id
    AND rbM.chr = l.chr
    AND rbM.posMin <= l.pos + :expand
    AND rbM.posMax >= l.pos - :expand
  LEFT JOIN db.region AS rM
    ON rM.region_id = rbM.region_id
    AND rM.type_id = :type_id
  GROUP BY l.rowid
) AS lM
LEFT JOIN db.region_bound AS rbU
  ON rbU._rowid_ = lM.rbU_rowid
LEFT JOIN db.region AS rU
  ON rU.region_id = rbU.region_id
LEFT JOIN db.region_bound AS rbD
  ON rbD._rowid_ = lM.rbD_rowid
LEFT JOIN db.region AS rD
  ON rD.region_id = rbD.region_id
""", { 'expand':self._expand, 'population_id':populationID, 'type_id':typeID }
		):
			target.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % row)
	#outputLociiRegions() #TODO
	
	
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
	
	parser.add_argument('-s', '--snp', type=str, metavar='rs#', nargs='+', action='append',
			help="a filtering set of SNPs, specified as RS#s"
	)
	
	parser.add_argument('-S', '--snpfile', type=str, metavar='file', nargs='+', action='append',
			help="RS# file(s) from which to load a filtering set of SNPs"
	)
	
	parser.add_argument('-m', '--marker', type=str, metavar='marker', nargs='+', action='append',
			help="a filtering set of markers, specified as 'chr:pos' or 'chr:label:pos'"
	)
	
	parser.add_argument('-M', '--mapfile', type=str, metavar='file', nargs='+', action='append',
			help=".map file(s) from which to load a filtering set of markers"
	)
	
	parser.add_argument('-x', '--expansion', type=str, metavar='num',
			help="amount by which to expand region boundaries when matching them to locii"
	)
	
	parser.add_argument('-p', '--population', type=str, metavar='label',
			help="LD profile with which to expand region boundaries when matching them to locii"
	)
	
	parser.add_argument('-o', '--output', type=str, metavar='format',
			help="output type"
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
	
	# apply SNP filters
	if args.snp:
		for snpList in args.snp:
			bio.intersectSNPs( (long(snp[2:]),) if snp[0:2].upper() == 'RS' else (long(snp),) for snp in snpList )
	if args.snpfile:
		for snpFileList in args.snpfile:
			bio.intersectSNPs( (rs,) for rs in bio.generateRSesFromRSFiles(snpFileList) )
	
	# apply locus filters
	if args.marker:
		for markerList in args.marker:
			bio.intersectLocii( bio.generateLociiFromMarkers(markerList) )
	if args.mapfile:
		for mapFileList in args.mapfile:
			bio.intersectLocii( bio.generateLociiFromMapFiles(mapFileList) )
	
	# output
	if args.output:
		o = args.output.strip().upper()
		if o == 'L':
			sys.stdout.write("#label\tchr\tpos\n")
			for line in bio.generateLocii():
				sys.stdout.write("%s\t%s\t%s\n" % line)
		elif o == 'L:R':
			sys.stdout.write("#label\tchr\tpos\tid\tgene\tstart\tstop\n")
			for line in bio.generateLociiRegions( (bio.generateLocii()) ):
				sys.stdout.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % line)
	
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

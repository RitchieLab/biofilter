#!/usr/bin/env python

import sys
import os
import argparse

import loki


class Biofilter:
	
	
	# ##################################################
	# public class data
	
	
	ver_maj,ver_min,ver_rev,ver_date = 0,0,4,'2012-03-15'
	
	
	# ##################################################
	# private class data
	
	
	_schema = {
		
		'main': {
			
			# ########## main.group ##########
			'group': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  group_id INTEGER,
  type_id TINYINT
)
""",
				'index': {
					'group__label': '(label)',
					'group__group_id': '(group_id)',
				}
			}, #.main.group
			
			# ########## main.locus ##########
			'locus': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  rs INTEGER,
  chr TINYINT,
  pos BIGINT
)
""",
				'index': {
					'locus__label': '(label)',
					'locus__rs': '(rs)',
					'locus__pos': '(chr,pos)',
				}
			}, #.main.locus
			
			# ########## main.region ##########
			'region': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  region_id INTEGER,
  type_id TINYINT,
  chr TINYINT,
  posMin BIGINT,
  posMax BIGINT
)
""",
				'index': {
					'region__label': '(label)',
					'region__region_id': '(region_id)',
					'region__posmin': '(chr,posMin)',
					'region__posmax': '(chr,posMax)',
				}
			}, #.main.region
			
			# ########## main.region_zone ##########
			'region_zone': {
				'table': """
(
  rowid INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (rowid,chr,zone)
)
""",
				'index': {
					'region_zone__zone': '(chr,zone)',
				}
			}, #.main.region_zone
			
		}, #.main
		
		
		'temp': {
			
			# ########## temp.group ##########
			'group': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  group_id INTEGER,
  type_id TINYINT
)
""",
				'index': {
					'group__label': '(label)',
					'group__group_id': '(group_id)',
				}
			}, #.temp.group
			
			# ########## temp.locus ##########
			'locus': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  rs INTEGER,
  chr TINYINT,
  pos BIGINT
)
""",
				'index': {
					'locus__label': '(label)',
					'locus__rs': '(rs)',
					'locus__pos': '(chr,pos)'
				}
			}, #.temp.locus
			
			# ########## temp.region ##########
			'region': {
				'table': """
(
  rowid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  label VARCHAR(64),
  region_id INTEGER,
  type_id TINYINT,
  chr TINYINT,
  posMin BIGINT,
  posMax BIGINT
)
""",
				'index': {
					'region__label': '(label)',
					'region__region_id': '(region_id)',
					'region__posmin': '(chr,posMin)',
					'region__posmax': '(chr,posMax)',
				}
			}, #.temp.region
			
			# ########## temp.region_zone ##########
			'region_zone': {
				'table': """
(
  rowid INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (rowid,chr,zone)
)
""",
				'index': {
					'region_zone__zone': '(chr,zone)'
				}
			}, #.temp.region_zone
			
			# ########## temp.rs ##########
			'rs': {
				'table': """
(
  rs INTEGER PRIMARY KEY NOT NULL
)
""",
				'index': {}
			}, #.temp.rs
			
		}, #.temp
	} #_schema{}
	
	
	# ##################################################
	# constructor
	
	
	def __init__(self):
		# initialize instance properties
		self._iwd = os.getcwd()
		self._expand = 0
		self._population = 'n/a'
		
		# initialize instance database
		self._loki = loki.Database()
		self._loki.createDatabaseObjects(self._schema['main'], 'main')
		self._loki.setVerbose(True)
	#__init__()
	
	
	# ##################################################
	# instance management
	
	
	def changeDirectory(self, path):
		try:
			os.chdir(self._iwd if path == "-" else path)
		except OSError as e:
			sys.exit("ERROR: %s" % e)
		sys.stderr.write("OK: %s\n" % os.getcwd())
	#changeDirectory()
	
	
	# ##################################################
	# input data parsers
	
	
	def generateLociiFromMarkers(self, markers, separator=':'):
		for marker in markers:
			if marker == None:
				continue
			
			label = rs = chm = pos = None
			cols = marker.split(separator)
			
			# parse line
			if len(cols) < 2:
				sys.exit("ERROR: malformed marker '%s', expected 'chr:pos' or 'chr:label:pos'" % marker)
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
				sys.exit("ERROR: malformed marker '%s', unknown chromosome" % marker)
			chm = self._loki.chr_num[chm]
			
			# parse and convert marker label
			if not label:
				label = 'chr%s:%s' % (self.loki.chr_name[chm], pos)
			elif label[:2].upper() == 'RS' and label[2:].isdigit():
				rs = long(label[2:])
			
			# parse and convert position
			if pos == '-' or pos == 'NA':
				pos = None
			else:
				pos = long(pos)
			
			yield (label,rs,chm,pos)
		#foreach marker
	#generateLociiFromMarkers()
	
	
	def getLociiFromMarkers(self, markers, separator=':'):
		return [ self.generateLociiFromMarkers(markers, separator) ]
	#getLociiFromMarkers()
	
	
	def generateLociiFromMapFiles(self, files):
		for filePtr in files:
			sys.stderr.write("processing '%s' ..." % filePtr.name)
			sys.stderr.flush()
			for locus in self.generateLociiFromMarkers(
					((line.rstrip() if (len(line) > 0 and line[0] != '#') else None) for line in filePtr),
					"\t"
			):
				yield locus
			sys.stderr.write(" OK: %d variants\n" % len(lines))
		#foreach file
	#generateLociiFromMapFiles()
	
	
	def getLociiFromMapFiles(self, files):
		return [ self.generateLociiFromMapFiles(files) ]
	#getLociiFromMapFiles()
	
	
	def generateSNPsFromRSFiles(self, files):
		for filePtr in files:
			sys.stderr.write("processing '%s' ...\n" % filePtr.name)
			n = 0
			for line in filePtr:
				if len(line) > 0 and line[0] != '#':
					n += 1
					yield long(line)
			#foreach line in file
			sys.stderr.write("... OK: %d SNPs\n" % n)
		#foreach file
	#generateSNPsFromRSFiles()
	
	
	def getSNPsFromRSFiles(self, files):
		return [ self.generateSNPsFromRSFiles(files) ]
	#getSNPsFromRSFiles()
	
	
	# ##################################################
	# working set management
	
	
	def addLocii(self, itrLocus):
		# itrLocus.next() => (label,rs,chr,pos)
		with self._loki:
			# load locii into temp table
			self._loki.createDatabaseTables(self._schema['temp'], 'temp', 'locus')
			self._loki._dbc.executemany("INSERT INTO temp.locus (label,rs,chr,pos) VALUES (?,?,?,?)", itrLocus)
			
			sys.stderr.write("adding locii to working set ...\n")
			
			# update merged rs#s
			lstUpdate = []
			for row in self._loki._dbc.execute("SELECT sm.rsCur, l.rowid FROM temp.locus AS l JOIN db.snp_merge AS sm ON sm.rsOld = l.rs"):
				lstUpdate.append( (row[0],row[1]) )
			self._loki._dbc.executemany("UPDATE temp.locus SET rs=? WHERE rowid=?", lstUpdate)
			numUpdate = len(lstUpdate)
			
			# load locii into the working set, using rs# to fill in missing chr/pos
			self._loki.dropDatabaseIndexes(self._schema['main'], 'main', 'locus')
			self._loki._dbc.execute("""
INSERT INTO main.locus (label,rs,chr,pos)
SELECT
  COALESCE(
    l.label,
    'rs' || l.rs,
    'chr' || COALESCE(l.chr,s.chr) || ':' || COALESCE(l.pos,s.pos)
  ) AS label,
  l.rs,
  COALESCE(l.chr,s.chr) AS chr,
  COALESCE(l.pos,s.pos) AS pos
FROM temp.locus AS l
LEFT JOIN db.snp AS s
  ON s.rs = l.rs
  AND (l.chr IS NULL OR s.chr = l.chr)
  AND (l.pos IS NULL OR s.pos = l.pos)
""")
			numAdd = self._loki._db.changes()
			self._loki.createDatabaseIndexes(self._schema['main'], 'main', 'locus')
			self._loki.dropDatabaseTables(self._schema['temp'], 'temp', 'locus')
			
			# print stats
			sys.stderr.write("... OK: %d locii added (%d rs#s updated)\n" % (numAdd,numUpdate))
			sys.stderr.write("verifying ...\n")
			sys.stderr.flush()
			for row in self._loki._dbc.execute("SELECT COUNT(1) FROM main.locus"):
				ttl = row[0]
			sys.stderr.write("... OK: %d variants in working set\n" % ttl)
		#with db transaction
	#addLocii()
	
	
	# ##################################################
	# annotation
	
	
	def outputLocii(self, target=sys.stdout):
		target.write("#chr\tlabel\tpos\n")
		# sqlite3's string concat operator is ||
		for row in self._loki._dbc.execute("""
SELECT
  COALESCE(chr, 'NA') AS chr,
  COALESCE(
    label,
    'rs' || rs,
    'chr' || chr || ':' || pos,
    '!#' || rowid
  ) AS label,
  COALESCE(pos, 'NA') AS pos
FROM main.locus
"""
		):
			target.write("%s\t%s\t%s\n" % row)
	#outputLocii()
	
	
	def outputLociiRegions(self, rtype='gene', target=sys.stdout):
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
	#outputLociiRegions()
	
	
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
	#outputLociiModels()
	
	
	def outputRegionModels(self, rtype='gene', target=sys.stdout):
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
	#outputRegionModels()
	
	
#Biofilter


class Biofilter_ArgParse(argparse.Action):
	def __call__(self, parser, namespace, values, option_string=None):
		setattr(namespace, 'action', True)


class Biofilter_ArgParse_Database(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> database %s\n" % values)
		warnings = namespace.biofilter._loki.attachDatabaseFile(values)
		if warnings != True:
			for msg in warnings:
				sys.stderr.write("WARNING: %s\n" % msg)


class Biofilter_ArgParse_ChDir(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> chdir %s\n" % values)
		namespace.biofilter.changeDirectory(values)


class Biofilter_ArgParse_Update(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> update %s\n" % values)
		namespace.biofilter._loki.updateDatabase(values)


class Biofilter_ArgParse_Marker(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> marker %s\n" % values)
		namespace.biofilter.addLocii(namespace.biofilter.generateLociiFromMarkers(values))


class Biofilter_ArgParse_MapFile(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> mapfile %s\n" % [k.name for k in values])
		namespace.biofilter.addLocii( namespace.biofilter.generateLociiFromMapFiles(values) )


class Biofilter_ArgParse_SNP(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> snp %s\n" % values)
		namespace.biofilter.addLocii( (None,snp,None,None) for snp in values )


class Biofilter_ArgParse_SNPFile(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> snpfile %s\n" % [k.name for k in values])
		namespace.biofilter.addLocii( (None,snp,None,None) for snp in namespace.biofilter.generateSNPsFromRSFiles(values) )


class Biofilter_ArgParse_Expand(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> expand %s\n" % values)
		if values[-1:].upper() == 'K':
			namespace.biofilter._expand = int(float(values[:-1]) * 1000)
		else:
			namespace.biofilter._expand = int(values)
		sys.stderr.write("OK: region boundary expansion set to %d\n" % namespace.biofilter._expand)


class Biofilter_ArgParse_Output(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> output %s\n" % values)
		if values == 'l':
			namespace.biofilter.outputLocii()
		elif values == 'l:r':
			namespace.biofilter.outputLociiRegions()
		elif values == 'l:l':
			namespace.biofilter.outputLociiModels()
		elif values == 'r:r':
			namespace.biofilter.outputRegionModels()


class Biofilter_ArgParse_Version(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write(
"""Biofilter version %d.%d.%d (%s)
     LOKI version %d.%d.%d (%s)
%9s version %s
%9s version %s
""" % (
			Biofilter.ver_maj, Biofilter.ver_min, Biofilter.ver_rev, Biofilter.ver_date,
			loki.Database.ver_maj, loki.Database.ver_min, loki.Database.ver_rev, loki.Database.ver_date,
			loki.Database.getDatabaseDriverName(), loki.Database.getDatabaseDriverVersion(),
			loki.Database.getDatabaseInterfaceName(), loki.Database.getDatabaseInterfaceVersion()
		))


class Biofilter_ArgParse_NotImplemented(Biofilter_ArgParse):
	def __call__(self, parser, namespace, values, option_string=None):
		Biofilter_ArgParse.__call__(self, parser, namespace, values, option_string)
		sys.stderr.write("> %s %s\n" % (self.dest, values))
		sys.stderr.write("NOT YET IMPLEMENTED\n")


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	
	parser.add_argument('-d', '--database',
			type=str, metavar='filename', action=Biofilter_ArgParse_Database,
			help="specify the database file to use"
	)
	
	parser.add_argument('--cd', '--chdir',
			type=str, metavar='pathname', action=Biofilter_ArgParse_ChDir,
			help="change the current working directory, from which relative paths to input and output files are resolved; "
			+"the special path '-' returns to the initial directory when the program was started"
	)
	
	parser.add_argument('-u', '--update',
			type=str, metavar='data', nargs='+', action=Biofilter_ArgParse_Update,
			help="update the database file by downloading and processing new source data of the specified type; "
			+"files will be downloaded into a 'loki_cache' subdirectory of the current working directory and left in place "
			+"afterwards, so that future updates can avoid re-downloading source data which has not changed"
	)
	
	parser.add_argument('-m', '--marker',
			type=str, metavar='marker', nargs='+', action=Biofilter_ArgParse_Marker,
			help="load variants into the working set by marker ('chr:pos' or 'chr:label:pos')"
	)
	parser.add_argument('-M', '--mapfile',
			type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_MapFile,
			help="load variants into the working set by reading markers from one or more .map files"
	)
	
	parser.add_argument('-s', '--snp',
			type=str, metavar='rs#', nargs='+', action=Biofilter_ArgParse_SNP,
			help="load variants into the working set by rs#"
	)
	parser.add_argument('-S', '--snpfile',
			type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_SNPFile,
			help="load variants into the working set by reading rs#s from one or more files"
	)
	
	#parser.add_argument('-r', '--region',
			#type=str, metavar='chr:pos-pos', nargs='+', action=Biofilter_ArgParse_NotImplemented,
			#help="load regions into the working set by locus range (chr:pos-pos)"
	#)
	#parser.add_argument('-R', '--regionfile',
			#type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_NotImplemented,
			#help="load regions into the working set by reading locus ranges from one or more files"
	#)
	
	#parser.add_argument('-g', '--gene',
			#type=str, metavar='alias/tag', nargs='+', action=Biofilter_ArgParse_Gene,
			#help="load regions into the working set by gene alias or special tag: "
			#+"':d' loads all known gene regions from the database; "
			#+"':v' loads gene regions from the database using the working set of variants; "
			#+"':c' clears all regions from the current working set"
	#)
	#parser.add_argument('-G', '--genefile',
			#type=argparse.FileType('r'), metavar='filename', nargs='+', action=Biofilter_ArgParse_NotImplemented,
			#help="load regions into the working set by reading gene aliases from one or more files"
	#)
	
	parser.add_argument('-x', '--expand',
			type=str, metavar='num', action=Biofilter_ArgParse_Expand,
			help="when matching region boundaries to locii, expand the boundaries by this amount; "
			+"the suffix 'k' multiplies the amount by 1000"
	)
	parser.add_argument('-p', '--population',
			type=str, metavar='label', action=Biofilter_ArgParse_NotImplemented,
			help="when matching region boundaries to locii, expand the boundaries according to the linkage disequilibrium calculations stored in the database"
	)
	
	parser.add_argument('-o', '--output',
			type=str, metavar='data', action=Biofilter_ArgParse_Output,
			help="outputs data from the working sets according to the requested type: "
			+"'v' lists all variants; "
			+"'v:dg' annotates variants against known genes"
	)
	
	parser.add_argument('--version', nargs=0, action=Biofilter_ArgParse_Version)
	
	ns = argparse.Namespace()
	ns.biofilter = Biofilter()
	args = parser.parse_args(namespace=ns)
	
	if not hasattr(args, 'action'):
		print "Biofilter version %d.%d.%d (%s)" % (Biofilter.ver_maj, Biofilter.ver_min, Biofilter.ver_rev, Biofilter.ver_date)
		print "     LOKI version %d.%d.%d (%s)" % (loki.Database.ver_maj, loki.Database.ver_min, loki.Database.ver_rev, loki.Database.ver_date)
		print
		parser.print_usage()
		print
		print "Use -h for details."
#__main__


"""
h	help
d	database
u	update

s	variants - rs#
m	variants - map
g	regions - genes
r	regions - map
t	groups - pathways
	groups - genesets

p	population
x	expansion
o	output
"""

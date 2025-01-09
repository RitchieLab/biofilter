#!/usr/bin/env python

import datetime
import apsw
import bisect
import itertools
import sys

##################################################
# Note on included docstring
# Code was created over 10+ years by several developers
# ChatGPT was used to generate docstring in June 2024 to help with legacy code interpretation
# Docstring has not been inspected line by line
##################################################

class Database(object):
	"""
	A class to interact with a SQLite database using APSW.

	Attributes:
		chr_num (dict): A dictionary mapping chromosome names and numbers.
		chr_name (dict): A dictionary mapping chromosome numbers to names.
		_schema (dict): A dictionary containing the schema definition for the database.
	"""	
	
	##################################################
	# class interrogation
	
	@classmethod
	def getVersionTuple(cls):
		"""
		Returns the version information of the database as a tuple.

		Returns:
			tuple: A tuple containing (major, minor, revision, dev, build, date).
		"""
		# tuple = (major,minor,revision,dev,build,date)
		# dev must be in ('a','b','rc','release') for lexicographic comparison
		return (2,2,5,'release','','2019-03-15')
	#getVersionTuple()
	
	
	@classmethod
	def getVersionString(cls):
		"""
		Returns the version information of the database as a formatted string.

		Returns:
			str: A formatted version string.
		"""
		v = list(cls.getVersionTuple())
		# tuple = (major,minor,revision,dev,build,date)
		# dev must be > 'rc' for releases for lexicographic comparison,
		# but we don't need to actually print 'release' in the version string
		v[3] = '' if v[3] > 'rc' else v[3]
		return "%d.%d.%d%s%s (%s)" % tuple(v)
	#getVersionString()
	
	
	@classmethod
	def getDatabaseDriverName(cls):
		"""
		Returns the name of the database driver.

		Returns:
			str: The database driver name.
		"""		
		return "SQLite"
	#getDatabaseDriverName()
	
	
	@classmethod
	def getDatabaseDriverVersion(cls):
		"""
		Returns the version of the SQLite library.

		Returns:
			str: The SQLite library version.
		"""
		return apsw.sqlitelibversion()
	#getDatabaseDriverVersion()
	
	
	@classmethod
	def getDatabaseInterfaceName(cls):
		"""
		Returns the name of the database interface.

		Returns:
			str: The database interface name.
		"""
		return "APSW"
	#getDatabaseInterfaceName()
	
	
	@classmethod
	def getDatabaseInterfaceVersion(cls):
		"""
		Returns the version of the APSW library.

		Returns:
			str: The APSW library version.
		"""
		return apsw.apswversion()
	#getDatabaseInterfaceVersion()
	
	
	##################################################
	# public class data
	
	
	# hardcode translations between chromosome numbers and textual tags
	chr_num = {}
	chr_name = {}
	cnum = 0
	for cname in ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','XY','MT'):
		cnum += 1
		chr_num[cnum] = cnum
		chr_num['%s' % cnum] = cnum
		chr_num[cname] = cnum
		chr_name[cnum] = cname
		chr_name['%s' % cnum] = cname
		chr_name[cname] = cname
	chr_num['M'] = chr_num['MT']
	chr_name['M'] = chr_name['MT']
	
	
	##################################################
	# private class data
	
	
	_schema = {
		'db': {
			##################################################
			# configuration tables
			
			
			'setting': {
				'table': """
(
  setting VARCHAR(32) PRIMARY KEY NOT NULL,
  value VARCHAR(256)
)
""",
				'data': [
					('schema','3'),
					('ucschg',None),
					('zone_size','100000'),
					('optimized','0'),
					('finalized','0'),
				],
				'index': {}
			}, #.db.setting
			
			
			##################################################
			# metadata tables
			
			
			'grch_ucschg': {
				'table': """
(
  grch INTEGER PRIMARY KEY,
  ucschg INTEGER NOT NULL
)
""",
				# translations known at time of writing are still provided,
				# but additional translations will also be fetched at update
				'data': [
					(34,16),
					(35,17),
					(36,18),
					(37,19),
					(38,38),
				],
				'index': {}
			}, #.db.grch_ucschg
			
			
			'ldprofile': {
				'table': """
(
  ldprofile_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  ldprofile VARCHAR(32) UNIQUE NOT NULL,
  description VARCHAR(128),
  metric VARCHAR(32),
  value DOUBLE
)
""",
				'index': {}
			}, #.db.ldprofile
			
			
			'namespace': {
				'table': """
(
  namespace_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  namespace VARCHAR(32) UNIQUE NOT NULL,
  polygenic TINYINT NOT NULL DEFAULT 0
)
""",
				'index': {}
			}, #.db.namespace
			
			
			'relationship': {
				'table': """
(
  relationship_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  relationship VARCHAR(32) UNIQUE NOT NULL
)
""",
				'index': {}
			}, #.db.relationship
			
			
			'role': {
				'table': """
(
  role_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  role VARCHAR(32) UNIQUE NOT NULL,
  description VARCHAR(128),
  coding TINYINT,
  exon TINYINT
)
""",
				'index': {}
			}, #.db.role
			
			
			'source': {
				'table': """
(
  source_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  source VARCHAR(32) UNIQUE NOT NULL,
  updated DATETIME,
  version VARCHAR(32),
  grch INTEGER,
  ucschg INTEGER,
  current_ucschg INTEGER
)
""",
				'index': {}
			}, #.db.source
			
			
			'source_option': {
				'table': """
(
  source_id TINYINT NOT NULL,
  option VARCHAR(32) NOT NULL,
  value VARCHAR(64),
  PRIMARY KEY (source_id, option)
)
""",
				'index': {}
			}, #.db.source_option
			
			
			'source_file': {
				'table': """
(
  source_id TINYINT NOT NULL,
  filename VARCHAR(256) NOT NULL,
  size BIGINT,
  modified DATETIME,
  md5 VARCHAR(64),
  PRIMARY KEY (source_id, filename)
)
""",
				'index': {}
			}, #.db.source_file
			
			
			'type': {
				'table': """
(
  type_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type VARCHAR(32) UNIQUE NOT NULL
)
""",
				'index': {}
			}, #.db.type

			'subtype': {
				'table': """
(
  subtype_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  subtype VARCHAR(32) UNIQUE NOT NULL
)
""",
				'index': {}
			}, #.db.subtype
			
			
			'warning': {
				'table': """
(
  warning_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  source_id TINYINT NOT NULL,
  warning VARCHAR(8192)
)
""",
				'index': {
					'warning__source': '(source_id)',
				}
			}, #.db.warning
			
			
			##################################################
			# snp tables
			
			
			'snp_merge': {
				'table': """
(
  rsMerged INTEGER NOT NULL,
  rsCurrent INTEGER NOT NULL,
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'snp_merge__merge_current': '(rsMerged,rsCurrent)',
				}
			}, #.db.snp_merge
			
			
			'snp_locus': { # all coordinates in LOKI are 1-based closed intervals
				'table': """
(
  rs INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  pos BIGINT NOT NULL,
  validated TINYINT NOT NULL,
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'snp_locus__rs_chr_pos': '(rs,chr,pos)',
					'snp_locus__chr_pos_rs': '(chr,pos,rs)',
					# a (validated,...) index would be nice but adds >1GB to the file size :/
					#'snp_locus__valid_chr_pos_rs': '(validated,chr,pos,rs)',
				}
			}, #.db.snp_locus
			
			
			'snp_entrez_role': {
				'table': """
(
  rs INTEGER NOT NULL,
  entrez_id INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'snp_entrez_role__rs_entrez_role': '(rs,entrez_id,role_id)',
				}
			}, #.db.snp_entrez_role
			
			
			'snp_biopolymer_role': {
				'table': """
(
  rs INTEGER NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'snp_biopolymer_role__rs_biopolymer_role': '(rs,biopolymer_id,role_id)',
					'snp_biopolymer_role__biopolymer_rs_role': '(biopolymer_id,rs,role_id)',
				}
			}, #.db.snp_biopolymer_role
			
			
			##################################################
			# biopolymer tables
			
			
			'biopolymer': {
				'table': """
(
  biopolymer_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type_id TINYINT NOT NULL,
  label VARCHAR(64) NOT NULL,
  description VARCHAR(256),
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'biopolymer__type': '(type_id)',
					'biopolymer__label_type': '(label,type_id)',
				}
			}, #.db.biopolymer
			
			
			'biopolymer_name': {
				'table': """
(
  biopolymer_id INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (biopolymer_id,namespace_id,name)
)
""",
				'index': {
					'biopolymer_name__name_namespace_biopolymer': '(name,namespace_id,biopolymer_id)',
				}
			}, #.db.biopolymer_name
			
			
			'biopolymer_name_name': {
				# PRIMARY KEY column order satisfies the need to GROUP BY new_namespace_id, new_name
				'table': """
(
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  type_id TINYINT NOT NULL,
  new_namespace_id INTEGER NOT NULL,
  new_name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (new_namespace_id,new_name,type_id,namespace_id,name)
)
""",
				'index': {}
			}, #.db.biopolymer_name_name
			
			
			'biopolymer_region': { # all coordinates in LOKI are 1-based closed intervals
				'table': """
(
  biopolymer_id INTEGER NOT NULL,
  ldprofile_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  posMin BIGINT NOT NULL,
  posMax BIGINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (biopolymer_id,ldprofile_id,chr,posMin,posMax)
)
""",
				'index': {
					'biopolymer_region__ldprofile_chr_min': '(ldprofile_id,chr,posMin)',
					'biopolymer_region__ldprofile_chr_max': '(ldprofile_id,chr,posMax)',
				}
			}, #.db.biopolymer_region
			
			
			'biopolymer_zone': {
				'table': """
(
  biopolymer_id INTEGER NOT NULL,
  chr TINYINT NOT NULL,
  zone INTEGER NOT NULL,
  PRIMARY KEY (biopolymer_id,chr,zone)
)
""",
				'index': {
					'biopolymer_zone__zone': '(chr,zone,biopolymer_id)',
				}
			}, #.db.biopolymer_zone
			
			
			##################################################
			# group tables
			
			
			'group': {
				'table': """
(
  group_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  type_id TINYINT NOT NULL,
  subtype_id TINYINT NOT NULL,
  label VARCHAR(64) NOT NULL,
  description VARCHAR(256),
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'group__type': '(type_id)',
					'group__subtype': '(subtype_id)',
					'group__label_type': '(label,type_id)',
				}
			}, #.db.group
			
			
			'group_name': {
				'table': """
(
  group_id INTEGER NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,namespace_id,name)
)
""",
				'index': {
					'group_name__name_namespace_group': '(name,namespace_id,group_id)',
					'group_name__source_name': '(source_id,name)',
				}
			}, #.db.group_name
			
			
			'group_group': {
				'table': """
(
  group_id INTEGER NOT NULL,
  related_group_id INTEGER NOT NULL,
  relationship_id SMALLINT NOT NULL,
  direction TINYINT NOT NULL,
  contains TINYINT,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,related_group_id,relationship_id,direction)
)
""",
				'index': {
					'group_group__related': '(related_group_id,group_id)',
				}
			}, #.db.group_group
			
			
			'group_biopolymer': {
				'table': """
(
  group_id INTEGER NOT NULL,
  biopolymer_id INTEGER NOT NULL,
  specificity TINYINT NOT NULL,
  implication TINYINT NOT NULL,
  quality TINYINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,biopolymer_id,source_id)
)
""",
				'index': {
					'group_biopolymer__biopolymer': '(biopolymer_id,group_id)',
				}
			}, #.db.group_biopolymer
			
			
			'group_member_name': {
				'table': """
(
  group_id INTEGER NOT NULL,
  member INTEGER NOT NULL,
  type_id TINYINT NOT NULL,
  namespace_id INTEGER NOT NULL,
  name VARCHAR(256) NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (group_id,member,type_id,namespace_id,name)
)
""",
				'index': {}
			}, #.db.group_member_name
			
			
			##################################################
			# gwas tables
			
			
			'gwas': { # all coordinates in LOKI are 1-based closed intervals
				'table': """
(
  gwas_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  rs INTEGER,
  chr TINYINT,
  pos BIGINT,
  trait VARCHAR(256) NOT NULL,
  snps VARCHAR(256),
  orbeta VARCHAR(8),
  allele95ci VARCHAR(16),
  riskAfreq VARCAHR(16),
  pubmed_id INTEGER,
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'gwas__rs': '(rs)',
					'gwas__chr_pos': '(chr,pos)',
				}
			}, #.db.gwas
			
			
			##################################################
			# liftover tables
			
			
			'chain': { # all coordinates in LOKI are 1-based closed intervals
				'table': """
(
  chain_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  old_ucschg INTEGER NOT NULL,
  old_chr TINYINT NOT NULL,
  old_start BIGINT NOT NULL,
  old_end BIGINT NOT NULL,
  new_ucschg INTEGER NOT NULL,
  new_chr TINYINT NOT NULL,
  new_start BIGINT NOT NULL,
  new_end BIGINT NOT NULL,
  score BIGINT NOT NULL,
  is_fwd TINYINT NOT NULL,
  source_id TINYINT NOT NULL
)
""",
				'index': {
					'chain__oldhg_newhg_chr': '(old_ucschg,new_ucschg,old_chr)',
				}
			}, #.db.chain
			
			
			'chain_data': { # all coordinates in LOKI are 1-based closed intervals
				'table': """
(
  chain_id INTEGER NOT NULL,
  old_start BIGINT NOT NULL,
  old_end BIGINT NOT NULL,
  new_start BIGINT NOT NULL,
  source_id TINYINT NOT NULL,
  PRIMARY KEY (chain_id,old_start)
)
""",
				'index': {
					'chain_data__end': '(chain_id,old_end)',
				}
			}, #.db.chain_data
			
		}, #.db
		
	} #_schema{}
	
	
	##################################################
	# constructor
	
	
	def __init__(self, dbFile=None, testing=False, updating=False, tempMem=False):
		"""
		Initializes a Database instance.

		Args:
			dbFile (str, optional): The database file to attach.
			testing (bool, optional): If True, runs in testing mode.
			updating (bool, optional): If True, runs in updating mode.
			tempMem (bool, optional): If True, uses memory for temporary storage.
		"""
		# initialize instance properties
		self._is_test = testing
		self._updating = updating
		self._verbose = True
		self._logger = None
		self._logFile = sys.stderr
		self._logIndent = 0
		self._logHanging = False
		self._db = apsw.Connection('')
		self._dbFile = None
		self._dbNew = None
		self._updater = None
		self._liftOverCache = dict() # { (from,to) : [] }
		
		self.configureDatabase(tempMem=tempMem)
		self.attachDatabaseFile(dbFile)
	#__init__()
	
	
	##################################################
	# context manager
	
	
	def __enter__(self):
		"""
		Enters the context manager.

		Returns:
			Connection: The APSW connection object.
		"""
		return self._db.__enter__()
	#__enter__()
	
	
	def __exit__(self, excType, excVal, traceback):
		"""
		Exits the context manager.

		Args:
			excType (type): Exception type.
			excVal (Exception): Exception value.
			traceback (traceback): Traceback object.

		Returns:
			bool: True if no exception occurred, otherwise False.
		"""
		return self._db.__exit__(excType, excVal, traceback)
	#__exit__()
	
	
	##################################################
	# logging
	
	
	def _checkTesting(self):
		"""
		Checks and updates the testing setting in the database.

		Returns:
			bool: True if testing settings match, otherwise False.
		"""
		now_test = self.getDatabaseSetting("testing")
		if now_test is None or bool(int(now_test)) == bool(self._is_test):
			self.setDatabaseSetting("testing", bool(self._is_test))
			return True
		else:
			return False
	# setTesting(is_test)
	
	
	def getVerbose(self):
		"""
		Gets the verbosity setting.

		Returns:
			bool: True if verbose logging is enabled, otherwise False.
		"""
		return self._verbose
	#getVerbose()
	
	
	def setVerbose(self, verbose=True):
		"""
		Sets the verbosity setting.

		Args:
			verbose (bool, optional): True to enable verbose logging, False to disable.
		"""
		self._verbose = verbose
	#setVerbose()
	
	
	def setLogger(self, logger=None):
		"""
		Sets the logger object.

		Args:
			logger (Logger, optional): The logger object.
		"""
		self._logger = logger
	#setLogger()
	
	
	def log(self, message=""):
		"""
		Logs a message to the configured logger or standard output with indentation.

		Args:
			message (str, optional): The message to log. Defaults to an empty string.

		Returns:
			int: The current indentation level.

		The function logs the message with appropriate indentation and handles line breaks.
		If a logger is set, it uses the logger to log the message. If verbose logging is enabled,
		it writes the message to the standard output with indentation.
		"""
		if message != "" and message != "\n":
			logtime = datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
			message = logtime + " " + message

		if self._logger:
			return self._logger.log(message)
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
		return self._logIndent
	#log()
	
	
	def logPush(self, message=None):
		"""
		Logs a message and increases the indentation level.

		Args:
			message (str, optional): The message to log. Defaults to None.

		Returns:
			int: The new indentation level.

		The function logs the message if provided and increases the indentation level for subsequent logs.
		If a logger is set, it uses the logger to log the message.
		"""

		if self._logger:
			return self._logger.logPush(message)
		if message:
			self.log(message)
		if self._logHanging:
			self.log("\n")
		self._logIndent += 1
		return self._logIndent
	#logPush()
	
	
	def logPop(self, message=None):
		"""
		Decreases the indentation level and logs a message.

		Args:
		message (str, optional): The message to log. Defaults to None.

		Returns:
		int: The new indentation level.

		The function decreases the indentation level and logs the message if provided.
		If a logger is set, it uses the logger to log the message.
		"""

		if self._logger:
			return self._logger.logPop(message)
		if self._logHanging:
			self.log("\n")
		self._logIndent = max(0, self._logIndent - 1)
		if message:
			self.log(message)
		return self._logIndent
	#logPop()
	
	
	##################################################
	# database management
	
	
	def getDatabaseMemoryUsage(self, resetPeak=False):
		"""
		Retrieves the current and peak memory usage of the database.

		Args:
			resetPeak (bool, optional): If True, resets the peak memory usage after retrieving it. Defaults to False.

		Returns:
			tuple: A tuple containing the current memory usage (int) and the peak memory usage (int) in bytes.
		"""
		return (apsw.memoryused(), apsw.memoryhighwater(resetPeak))
	#getDatabaseMemoryUsage()
	
	
	def getDatabaseMemoryLimit(self):
		"""
		Retrieves the current memory limit for the database.

		Returns:
			int: The current soft heap limit in bytes.
		"""
		return apsw.softheaplimit(-1)
	#getDatabaseMemoryLimit()
	
	
	def setDatabaseMemoryLimit(self, limit=0):
		"""
		Sets a new memory limit for the database.

		Args:
			limit (int, optional): The new memory limit in bytes. Defaults to 0, which sets no limit.
		"""
		apsw.softheaplimit(limit)
	#setDatabaseMemoryLimit()
	
	
	def configureDatabase(self, db=None, tempMem=False):
		"""
		Configures database settings for performance and behavior.

		Args:
			db (str, optional): The name of the database to configure. Defaults to None.
			tempMem (bool, optional): If True, configures the temporary storage to use memory. Defaults to False.

		The function sets various PRAGMA settings to optimize performance for typical usage scenarios.
		"""
		cursor = self._db.cursor()
		db = ("%s." % db) if db else ""
		
		# linux VFS doesn't usually report actual disk cluster size,
		# so sqlite ends up using 1KB pages by default; we prefer 4KB
		cursor.execute("PRAGMA %spage_size = 4096" % (db,))
		
		# cache_size is pages if positive, kibibytes if negative;
		# seems to only affect write performance
		cursor.execute("PRAGMA %scache_size = -65536" % (db,))
		
		# for typical read-only usage, synchronization behavior is moot anyway,
		# and while updating we're not that worried about a power failure
		# corrupting the database file since the user could just start the
		# update over from the beginning; so, we'll take the performance gain
		cursor.execute("PRAGMA %ssynchronous = OFF" % (db,))
		
		# the journal isn't that big, so keeping it in memory is faster; the
		# cost is that a system crash will corrupt the database rather than
		# leaving it recoverable with the on-disk journal (a program crash
		# should be fine since sqlite will rollback transactions before exiting)
		cursor.execute("PRAGMA %sjournal_mode = MEMORY" % (db,))
		
		# the temp store is used for all of sqlite's internal scratch space
		# needs, such as the TEMP database, indexing, etc; keeping it in memory
		# is much faster, but it can get quite large
		if tempMem and not db:
			cursor.execute("PRAGMA temp_store = MEMORY")
		
		# we want EXCLUSIVE while updating since the data shouldn't be read
		# until ready and we want the performance gain; for normal read usage,
		# NORMAL is better so multiple users can share a database file
		cursor.execute("PRAGMA %slocking_mode = %s" % (db,("EXCLUSIVE" if self._updating else "NORMAL")))
	#configureDatabase()
	
		
	def attachTempDatabase(self, db):
		"""
		Attaches a temporary database with the given name.

		Args:
			db (str): The name of the temporary database to attach.

		The function first detaches any existing temporary database with the same name, then attaches a new one.
		"""
		cursor = self._db.cursor()
		
		# detach the current db, if any
		try:
			cursor.execute("DETACH DATABASE `%s`" % db)
		except apsw.SQLError as e:
			if not str(e).startswith('SQLError: no such database: '):
				raise e
		
		# attach a new temp db
		cursor.execute("ATTACH DATABASE '' AS `%s`" % db)
		self.configureDatabase(db)
	#attachTempDatabase()
	
	
	def attachDatabaseFile(self, dbFile, quiet=False):
		"""
		Attaches a new database file and configures it.

		Args:
			dbFile (str): The path to the database file to attach.
			quiet (bool, optional): If True, suppresses log messages. Defaults to False.

		The function detaches any currently attached database file, then attaches the new one and configures it.
		It also establishes or audits the database schema.
		"""
		cursor = self._db.cursor()
		
		# detach the current db file, if any
		if self._dbFile and not quiet:
			self.log("unloading knowledge database file '%s' ..." % self._dbFile)
		try:
			cursor.execute("DETACH DATABASE `db`")
		except apsw.SQLError as e:
			if not str(e).startswith('SQLError: no such database: '):
				raise e
		if self._dbFile and not quiet:
			self.log("unloading knowledge database file completed\n")
		
		# reset db info
		self._dbFile = None
		self._dbNew = None
		
		# attach the new db file, if any
		if dbFile:
			if not quiet:
				self.logPush("loading knowledge database file '%s' ..." % dbFile)
			cursor.execute("ATTACH DATABASE ? AS `db`", (dbFile,))
			self._dbFile = dbFile
			self._dbNew = (0 == max(row[0] for row in cursor.execute("SELECT COUNT(1) FROM `db`.`sqlite_master`")))
			self.configureDatabase('db')
			
			# establish or audit database schema
			err_msg = ""
			with self._db:
				if self._dbNew:
					self.createDatabaseObjects(None, 'db')
					ok = True
				else:
					self.updateDatabaseSchema()
					ok = self.auditDatabaseObjects(None, 'db')
					if not ok:
						err_msg = "Audit of database failed"
				
				if ok and self._updating:
					ok = self._checkTesting()
					if not ok:
						err_msg = "Testing settings do not match loaded database"
			
			if ok:
				if not quiet:
					self.logPop("loading knowledge database file completed\n")
			else:
				self._dbFile = None
				self._dbNew = None
				cursor.execute("DETACH DATABASE `db`")
				if not quiet:
					self.logPop("... ERROR (" + err_msg + ")\n")
		#if new dbFile
	#attachDatabaseFile()
	
	
	def detachDatabaseFile(self, quiet=False):
		"""
		Detaches the currently attached database file.

		Args:
			quiet (bool, optional): If True, suppresses log messages. Defaults to False.

		Returns:
			None
		"""
		return self.attachDatabaseFile(None, quiet=quiet)
	#detachDatabaseFile()
	
	
	def testDatabaseWriteable(self):
		"""
		Tests if the current database file is writeable.

		Raises:
			Exception: If no database file is loaded or if the database is read-only.

		Returns:
			bool: True if the database file is writeable.
		"""
		if self._dbFile == None:
			raise Exception("ERROR: no knowledge database file is loaded")
		try:
			if self._db.readonly('db'):
				raise Exception("ERROR: knowledge database file cannot be modified")
		except AttributeError: # apsw.Connection.readonly() added in 3.7.11
			try:
				self._db.cursor().execute("UPDATE `db`.`setting` SET value = value")
			except apsw.ReadOnlyError:
				raise Exception("ERROR: knowledge database file cannot be modified")
		return True
	#testDatabaseWriteable()
	
	
	def createDatabaseObjects(self, schema, dbName, tblList=None, doTables=True, idxList=None, doIndecies=True):
		"""
		Creates tables and indices in the database based on the provided schema.

		Args:
			schema (dict): The schema definition for the database objects.
			dbName (str): The name of the database to create objects in.
			tblList (list, optional): List of tables to create. Defaults to None, which creates all tables in the schema.
			doTables (bool, optional): If True, creates tables. Defaults to True.
			idxList (list, optional): List of indices to create. Defaults to None, which creates all indices in the schema.
			doIndecies (bool, optional): If True, creates indices. Defaults to True.

		The function creates the specified tables and indices, inserting initial data if provided in the schema.
		"""
		cursor = self._db.cursor()
		schema = schema or self._schema[dbName]
		dbType = "TEMP " if (dbName == "temp") else ""
		if tblList and isinstance(tblList, str):
			tblList = (tblList,)
		if idxList and isinstance(idxList, str):
			idxList = (idxList,)
		for tblName in (tblList or schema.keys()):
			if doTables:
				cursor.execute("CREATE %sTABLE IF NOT EXISTS `%s`.`%s` %s" % (dbType, dbName, tblName, schema[tblName]['table']))
				if 'data' in schema[tblName] and schema[tblName]['data']:
					sql = "INSERT OR IGNORE INTO `%s`.`%s` VALUES (%s)" % (dbName, tblName, ("?,"*len(schema[tblName]['data'][0]))[:-1])
					# TODO: change how 'data' is defined so it can be tested without having to try inserting
					try:
						cursor.executemany(sql, schema[tblName]['data'])
					except apsw.ReadOnlyError:
						pass
			if doIndecies:
				for idxName in (idxList or schema[tblName]['index'].keys()):
					if idxName not in schema[tblName]['index']:
						raise Exception("ERROR: no definition for index '%s' on table '%s'" % (idxName,tblName))
					cursor.execute("CREATE INDEX IF NOT EXISTS `%s`.`%s` ON `%s` %s" % (dbName, idxName, tblName, schema[tblName]['index'][idxName]))
				#foreach idxName in idxList
				cursor.execute("ANALYZE `%s`.`%s`" % (dbName,tblName))
		#foreach tblName in tblList
		
		# this shouldn't be necessary since we don't manually modify the sqlite_stat* tables
		#if doIndecies:
		#	cursor.execute("ANALYZE `%s`.`sqlite_master`" % (dbName,))
	#createDatabaseObjects()
	
	
	def createDatabaseTables(self, schema, dbName, tblList, doIndecies=False):
		"""
		Creates tables in the database based on the provided schema.

		Args:
			schema (dict): The schema definition for the database objects.
			dbName (str): The name of the database to create tables in.
			tblList (list): List of tables to create.
			doIndecies (bool, optional): If True, creates indices. Defaults to False.

		The function creates the specified tables and optionally creates indices for them.
		"""
		return self.createDatabaseObjects(schema, dbName, tblList, True, None, doIndecies)
	#createDatabaseTables()
	
	
	def createDatabaseIndices(self, schema, dbName, tblList, doTables=False, idxList=None):
		"""
		Creates indices in the database based on the provided schema.

		Args:
			schema (dict): The schema definition for the database objects.
			dbName (str): The name of the database to create indices in.
			tblList (list): List of tables to create indices for.
			doTables (bool, optional): If True, creates tables as well. Defaults to False.
			idxList (list, optional): List of indices to create. Defaults to None, which creates all indices in the schema.

		The function creates the specified indices and optionally creates tables for them.
		"""
		return self.createDatabaseObjects(schema, dbName, tblList, doTables, idxList, True)
	#createDatabaseIndices()
	
	
	def dropDatabaseObjects(self, schema, dbName, tblList=None, doTables=True, idxList=None, doIndecies=True):
		"""
		Drops tables and indices in the database based on the provided schema.

		Args:
			schema (dict): The schema definition for the database objects.
			dbName (str): The name of the database to drop objects from.
			tblList (list, optional): List of tables to drop. Defaults to None, which drops all tables in the schema.
			doTables (bool, optional): If True, drops tables. Defaults to True.
			idxList (list, optional): List of indices to drop. Defaults to None, which drops all indices in the schema.
			doIndecies (bool, optional): If True, drops indices. Defaults to True.

		The function drops the specified tables and indices from the database.
		"""
		cursor = self._db.cursor()
		schema = schema or self._schema[dbName]
		if tblList and isinstance(tblList, str):
			tblList = (tblList,)
		if idxList and isinstance(idxList, str):
			idxList = (idxList,)
		for tblName in (tblList or schema.keys()):
			if doTables:
				cursor.execute("DROP TABLE IF EXISTS `%s`.`%s`" % (dbName, tblName))
			elif doIndecies:
				for idxName in (idxList or schema[tblName]['index'].keys()):
					cursor.execute("DROP INDEX IF EXISTS `%s`.`%s`" % (dbName, idxName))
				#foreach idxName in idxList
		#foreach tblName in tblList
	#dropDatabaseObjects()
	
	
	def dropDatabaseTables(self, schema, dbName, tblList):
		"""
		Drops tables in the database based on the provided schema.

		Args:
			schema (dict): The schema definition for the database objects.
			dbName (str): The name of the database to drop tables from.
			tblList (list): List of tables to drop.

		The function drops the specified tables from the database.
		"""
		return self.dropDatabaseObjects(schema, dbName, tblList, True, None, True)
	#dropDatabaseTables()
	
	
	def dropDatabaseIndices(self, schema, dbName, tblList, idxList=None):
		"""
		Drops indices in the database based on the provided schema.

		Args:
			schema (dict): The schema definition for the database objects.
			dbName (str): The name of the database to drop indices from.
			tblList (list): List of tables to drop indices for.
			idxList (list, optional): List of indices to drop. Defaults to None, which drops all indices in the schema.

		The function drops the specified indices from the database.
		"""
		return self.dropDatabaseObjects(schema, dbName, tblList, False, idxList, True)
	#dropDatabaseIndices()
	
	
	def updateDatabaseSchema(self):
		"""
		Updates the database schema to the latest version.

		The function checks the current schema version and applies necessary updates to bring it to the latest version.
		It logs the progress and results of each update step.

		Raises:
			Exception: If an error occurs during the schema update process.
		"""
		cursor = self._db.cursor()
		
		if self.getDatabaseSetting('schema',int) < 2:
			self.logPush("updating database schema to version 2 ...\n")
			updateMap = {
				'snp_merge'           : 'rsMerged,rsCurrent,source_id',
				'snp_locus'           : 'rs,chr,pos,validated,source_id',
				'snp_entrez_role'     : 'rs,entrez_id,role_id,source_id',
				'snp_biopolymer_role' : 'rs,biopolymer_id,role_id,source_id',
			}
			for tblName,tblColumns in updateMap.iteritems():
				self.log("%s ..." % (tblName,))
				cursor.execute("ALTER TABLE `db`.`%s` RENAME TO `___old_%s___`" % (tblName,tblName))
				self.createDatabaseTables(None, 'db', tblName)
				cursor.execute("INSERT INTO `db`.`%s` (%s) SELECT %s FROM `db`.`___old_%s___`" % (tblName,tblColumns,tblColumns,tblName))
				cursor.execute("DROP TABLE `db`.`___old_%s___`" % (tblName,))
				self.createDatabaseIndices(None, 'db', tblName)
				self.log(" OK\n")
			self.setDatabaseSetting('schema', 2)
			self.logPop("... OK\n")
		#schema<2
		
		if self.getDatabaseSetting('schema',int) < 3:
			self.log("updating database schema to version 3 ...")
			self.setDatabaseSetting('optimized', self.getDatabaseSetting('finalized',int))
			self.setDatabaseSetting('schema', 3)
			self.log(" OK\n")
		#schema<3
	#updateDatabaseSchema()
	
	
	def auditDatabaseObjects(self, schema, dbName, tblList=None, doTables=True, idxList=None, doIndecies=True, doRepair=True):
		"""
		Audits the database objects against the provided schema and repairs discrepancies if specified.

		Args:
			schema (dict, optional): The schema definition for the database objects. Defaults to None, which uses the internal schema.
			dbName (str): The name of the database to audit.
			tblList (list, optional): List of tables to audit. Defaults to None, which audits all tables in the schema.
			doTables (bool, optional): If True, audits tables. Defaults to True.
			idxList (list, optional): List of indices to audit. Defaults to None, which audits all indices in the schema.
			doIndecies (bool, optional): If True, audits indices. Defaults to True.
			doRepair (bool, optional): If True, repairs discrepancies. Defaults to True.

		Returns:
			bool: True if the audit is successful and all objects match the schema, False otherwise.

		The function fetches the current database schema, compares it with the provided schema, and repairs any discrepancies if specified.
		It logs warnings and errors for mismatches and repairs.
		"""		
		# fetch current schema
		cursor = self._db.cursor()
		current = dict()
		dbMaster = "`sqlite_temp_master`" if (dbName == "temp") else ("`%s`.`sqlite_master`" % (dbName,))
		sql = "SELECT tbl_name,type,name,COALESCE(sql,'') FROM %s WHERE type IN ('table','index')" % (dbMaster,)
		for row in cursor.execute(sql):
			tblName,objType,idxName,objDef = row
			if tblName not in current:
				current[tblName] = {'table':None, 'index':{}}
			if objType == 'table':
				current[tblName]['table'] = " ".join(objDef.strip().split())
			elif objType == 'index':
				current[tblName]['index'][idxName] = " ".join(objDef.strip().split())
		tblEmpty = dict()
		sql = None
		for tblName in current:
			tblEmpty[tblName] = True
			sql = "SELECT 1 FROM `%s`.`%s` LIMIT 1" % (dbName,tblName)
			for row in cursor.execute(sql):
				tblEmpty[tblName] = False
		# audit requested objects
		schema = schema or self._schema[dbName]
		if tblList and isinstance(tblList, str):
			tblList = (tblList,)
		if idxList and isinstance(idxList, str):
			idxList = (idxList,)
		ok = True
		for tblName in (tblList or schema.keys()):
			if doTables:
				if tblName in current:
					if current[tblName]['table'] == ("CREATE TABLE `%s` %s" % (tblName, " ".join(schema[tblName]['table'].strip().split()))):
						if 'data' in schema[tblName] and schema[tblName]['data']:
							sql = u"INSERT OR IGNORE INTO `%s`.`%s` VALUES (%s)" % (dbName, tblName, ("?,"*len(schema[tblName]['data'][0]))[:-1])
							# TODO: change how 'data' is defined so it can be tested without having to try inserting
							try:
								cursor.executemany(sql, schema[tblName]['data'])
							except apsw.ReadOnlyError:
								pass
					elif doRepair and tblEmpty[tblName]:
						self.log("WARNING: table '%s' schema mismatch -- repairing ..." % tblName)
						self.dropDatabaseTables(schema, dbName, tblName)
						self.createDatabaseTables(schema, dbName, tblName)
						current[tblName]['index'] = dict()
						self.log(" OK\n")
					elif doRepair:
						self.log("ERROR: table '%s' schema mismatch -- cannot repair\n" % tblName)
						ok = False
					else:
						self.log("ERROR: table '%s' schema mismatch\n" % tblName)
						ok = False
					#if definition match
				elif doRepair:
					self.log("WARNING: table '%s' is missing -- repairing ..." % tblName)
					self.createDatabaseTables(schema, dbName, tblName, doIndecies)
					self.log(" OK\n")
				else:
					self.log("ERROR: table '%s' is missing\n" % tblName)
					ok = False
				#if tblName in current
			#if doTables
			if doIndecies:
				for idxName in (idxList or schema[tblName]['index'].keys()):
					if (tblName not in current) and not (doTables and doRepair):
						self.log("ERROR: table '%s' is missing for index '%s'\n" % (tblName, idxName))
						ok = False
					elif tblName in current and idxName in current[tblName]['index']:
						if current[tblName]['index'][idxName] == ("CREATE INDEX `%s` ON `%s` %s" % (idxName, tblName, " ".join(schema[tblName]['index'][idxName].strip().split()))):
							pass
						elif doRepair:
							self.log("WARNING: index '%s' on table '%s' schema mismatch -- repairing ..." % (idxName, tblName))
							self.dropDatabaseIndices(schema, dbName, tblName, idxName)
							self.createDatabaseIndices(schema, dbName, tblName, False, idxName)
							self.log(" OK\n")
						else:
							self.log("ERROR: index '%s' on table '%s' schema mismatch\n" % (idxName, tblName))
							ok = False
						#if definition match
					elif doRepair:
						self.log("WARNING: index '%s' on table '%s' is missing -- repairing ..." % (idxName, tblName))
						self.createDatabaseIndices(schema, dbName, tblName, False, idxName)
						self.log(" OK\n")
					else:
						self.log("ERROR: index '%s' on table '%s' is missing\n" % (idxName, tblName))
						ok = False
					#if tblName,idxName in current
				#foreach idxName in idxList
			#if doIndecies
		#foreach tblName in tblList
		return ok
	#auditDatabaseObjects()
	
	
	def finalizeDatabase(self):
		"""
		Finalizes the database by discarding intermediate data and setting finalization flags.

		The function drops intermediate tables, recreates them, and sets the database settings to indicate that the database is finalized and not optimized.

		Returns:
			None
		"""
		self.log("discarding intermediate data ...")
		self.dropDatabaseTables(None, 'db', ('snp_entrez_role','biopolymer_name_name','group_member_name'))
		self.createDatabaseTables(None, 'db', ('snp_entrez_role','biopolymer_name_name','group_member_name'), True)
		self.log(" OK\n")
		self.setDatabaseSetting('finalized', 1)
		self.setDatabaseSetting('optimized', 0)
	#finalizeDatabase()
	
	
	def optimizeDatabase(self):
		"""
		Optimizes the database by updating optimizer statistics and compacting the database file.

		The function updates the database statistics for query optimization and compacts the database to free up space.

		Returns:
			None
		"""
		self._db.cursor().execute("ANALYZE `db`")
		self.log("updating optimizer statistics completed\n")
		self.defragmentDatabase()
		self.setDatabaseSetting('optimized', 1)
		self.log("compacting knowledge database file completed\n")
	#optimizeDatabase()
	
	
	def defragmentDatabase(self):
		"""
		Defragments the database to compact it and free up space.

		The function detaches the current database file, performs a VACUUM operation to compact it, and then re-attaches the database file.

		Returns:
			None
		"""
		# unfortunately sqlite's VACUUM doesn't work on attached databases,
		# so we have to detach, make a new direct connection, then re-attach
		if self._dbFile:
			dbFile = self._dbFile
			self.detachDatabaseFile(quiet=True)
			db = apsw.Connection(dbFile)
			db.cursor().execute("VACUUM")
			db.close()
			self.attachDatabaseFile(dbFile, quiet=True)
	#defragmentDatabase()
	
	
	def getDatabaseSetting(self, setting, type=None):
		"""
		Retrieves a specific setting value from the database.

		Args:
			setting (str): The name of the setting to retrieve.
			type (type, optional): The type to cast the setting value to. Defaults to None.

		Returns:
			The setting value, cast to the specified type if provided.
		"""		
		value = None
		if self._dbFile:
			for row in self._db.cursor().execute("SELECT value FROM `db`.`setting` WHERE setting = ?", (setting,)):
				value = row[0]
		if type:
			value = type(value) if (value != None) else type()
		return value
	#getDatabaseSetting()
	
	
	def setDatabaseSetting(self, setting, value):
		"""
		Sets a specific setting value in the database.

		Args:
			setting (str): The name of the setting to set.
			value: The value to set for the specified setting.

		Returns:
			None
		"""
		self._db.cursor().execute("INSERT OR REPLACE INTO `db`.`setting` (setting, value) VALUES (?, ?)", (setting,value))
	#setDatabaseSetting()
	
	
	def getSourceModules(self):
		"""
		Retrieves the source modules available for updating the database.

		If the updater is not already initialized, it imports and initializes the updater module.

		Returns:
			list: A list of available source modules.
		"""
		if not self._updater:
			import loki_modules.loki_updater as loki_updater
			self._updater = loki_updater.Updater(self, self._is_test)
		return self._updater.getSourceModules()
	#getSourceModules()
	
	
	def getSourceModuleVersions(self, sources=None):
		"""
		Retrieves the versions of the specified source modules.

		If the updater is not already initialized, it imports and initializes the updater module.

		Args:
			sources (list, optional): A list of source modules to get versions for. Defaults to None, which retrieves versions for all modules.

		Returns:
			dict: A dictionary mapping source modules to their versions.
		"""
		if not self._updater:
			import loki_modules.loki_updater as loki_updater
			self._updater = loki_updater.Updater(self, self._is_test)
		return self._updater.getSourceModuleVersions(sources)
	#getSourceModuleVersions()
	
	
	def getSourceModuleOptions(self, sources=None):
		"""
		Retrieves the options for the specified source modules.

		If the updater is not already initialized, it imports and initializes the updater module.

		Args:
			sources (list, optional): A list of source modules to get options for. Defaults to None, which retrieves options for all modules.

		Returns:
			dict: A dictionary mapping source modules to their options.
		"""
		if not self._updater:
			import loki_modules.loki_updater as loki_updater
			self._updater = loki_updater.Updater(self, self._is_test)
		return self._updater.getSourceModuleOptions(sources)
	#getSourceModuleOptions()
	
	
	def updateDatabase(self, sources=None, sourceOptions=None, cacheOnly=False, forceUpdate=False):
		"""
		Updates the database using the specified source modules and options.

		If the updater is not already initialized, it imports and initializes the updater module.

		Args:
			sources (list, optional): A list of source modules to update from. Defaults to None, which updates from all sources.
			sourceOptions (dict, optional): A dictionary of options for the source modules. Defaults to None.
			cacheOnly (bool, optional): If True, only updates the cache. Defaults to False.
			forceUpdate (bool, optional): If True, forces the update even if not necessary. Defaults to False.

		Returns:
			Any: The result of the update operation.

		Raises:
			Exception: If the database is finalized and cannot be updated.
		"""
		if self.getDatabaseSetting('finalized',int):
			raise Exception("ERROR: cannot update a finalized database")
		if not self._updater:
			import loki_modules.loki_updater as loki_updater
			self._updater = loki_updater.Updater(self, self._is_test)
		return self._updater.updateDatabase(sources, sourceOptions, cacheOnly, forceUpdate)
	#updateDatabase()
	
	
	def prepareTableForUpdate(self, table):
		"""
		Prepares a table for update by the updater.

		If the database is finalized, it raises an exception.

		Args:
			table (str): The name of the table to prepare for update.

		Returns:
			Any: The result of the preparation.

		Raises:
			Exception: If the database is finalized and cannot be updated.
		"""
		if self.getDatabaseSetting('finalized',int):
			raise Exception("ERROR: cannot update a finalized database")
		if self._updater:
			return self._updater.prepareTableForUpdate(table)
		return None
	#prepareTableForUpdate()
	
	
	def prepareTableForQuery(self, table):
		"""
		Prepares a table for query by the updater.

		Args:
			table (str): The name of the table to prepare for query.

		Returns:
			Any: The result of the preparation, or None if no updater is available.
		"""
		if self._updater:
			return self._updater.prepareTableForQuery(table)
		return None
	#prepareTableForQuery()
	
	
	##################################################
	# metadata retrieval
	
	
	def generateGRChByUCSChg(self, ucschg):
		"""
		Generates GRCh values based on a given UCSC chain identifier.

		Args:
			ucschg (str): The UCSC chain identifier.

		Returns:
			generator: A generator yielding GRCh values corresponding to the given UCSC chain identifier.
		"""
		return (row[0] for row in self._db.cursor().execute("SELECT grch FROM grch_ucschg WHERE ucschg = ?", (ucschg,)))
	#generateGRChByUCSChg()
	
	
	def getUCSChgByGRCh(self, grch):
		"""
		Retrieves the UCSC chain identifier for a given GRCh value.

		Args:
			grch (str): The GRCh value.

		Returns:
			str: The UCSC chain identifier corresponding to the given GRCh value, or None if not found.
		"""
		ucschg = None
		for row in self._db.cursor().execute("SELECT ucschg FROM grch_ucschg WHERE grch = ?", (grch,)):
			ucschg = row[0]
		return ucschg
	#getUCSChgByGRCh()
	
	
	def getLDProfileID(self, ldprofile):
		"""
		Retrieves the identifier for a given LD profile.

		Args:
			ldprofile (str): The LD profile name.

		Returns:
			int: The identifier of the LD profile, or None if not found.
		"""
		return self.getLDProfileIDs([ldprofile])[ldprofile]
	#getLDProfileID()
	
	
	def getLDProfileIDs(self, ldprofiles):
		"""
		Retrieves the identifiers for a list of LD profiles.

		Args:
			ldprofiles (list): A list of LD profile names.

		Returns:
			dict: A dictionary mapping LD profile names to their identifiers.
		"""
		if not self._dbFile:
			return { l:None for l in ldprofiles }
		sql = "SELECT i.ldprofile, l.ldprofile_id FROM (SELECT ? AS ldprofile) AS i LEFT JOIN `db`.`ldprofile` AS l ON LOWER(TRIM(l.ldprofile)) = LOWER(TRIM(i.ldprofile))"
		with self._db:
			ret = { row[0]:row[1] for row in self._db.cursor().executemany(sql, zip(ldprofiles)) }
		return ret
	#getLDProfileIDs()
	
	
	def getLDProfiles(self, ldprofiles=None):
		"""
		Retrieves detailed information about LD profiles.

		Args:
			ldprofiles (list, optional): A list of LD profile names. Defaults to None, which retrieves information for all profiles.

		Returns:
			dict: A dictionary mapping LD profile names to a tuple containing their identifier, description, metric, and value.
		"""
		if not self._dbFile:
			return { l:None for l in (ldprofiles or list()) }
		with self._db:
			if ldprofiles:
				sql = "SELECT i.ldprofile, l.ldprofile_id, l.description, l.metric, l.value FROM (SELECT ? AS ldprofile) AS i LEFT JOIN `db`.`ldprofile` AS l ON LOWER(TRIM(l.ldprofile)) = LOWER(TRIM(i.ldprofile))"
				ret = { row[0]:row[1:] for row in self._db.cursor().executemany(sql, zip(ldprofiles)) }
			else:
				sql = "SELECT l.ldprofile, l.ldprofile_id, l.description, l.metric, l.value FROM `db`.`ldprofile` AS l"
				ret = { row[0]:row[1:] for row in self._db.cursor().execute(sql) }
		return ret
	#getLDProfiles()
	
	
	def getNamespaceID(self, namespace):
		"""
		Retrieves the identifier for a given namespace.

		Args:
			namespace (str): The namespace name.

		Returns:
			int: The identifier of the namespace, or None if not found.
		"""
		return self.getNamespaceIDs([namespace])[namespace]
	#getNamespaceID()
	
	
	def getNamespaceIDs(self, namespaces):
		"""
		Retrieves the identifiers for a list of namespaces.

		Args:
			namespaces (list): A list of namespace names.

		Returns:
			dict: A dictionary mapping namespace names to their identifiers.
		"""
		if not self._dbFile:
			return { n:None for n in namespaces }
		sql = "SELECT i.namespace, n.namespace_id FROM (SELECT ? AS namespace) AS i LEFT JOIN `db`.`namespace` AS n ON n.namespace = LOWER(i.namespace)"
		with self._db:
			ret = { row[0]:row[1] for row in self._db.cursor().executemany(sql, zip(namespaces)) }
		return ret
	#getNamespaceIDs()
	
	
	def getRelationshipID(self, relationship):
		"""
		Retrieves the identifier for a given relationship.

		Args:
			relationship (str): The relationship name.

		Returns:
			int: The identifier of the relationship, or None if not found.
		"""
		return self.getRelationshipIDs([relationship])[relationship]
	#getRelationshipID()
	
	
	def getRelationshipIDs(self, relationships):
		"""
		Retrieves the identifiers for a list of relationships.

		Args:
			relationships (list): A list of relationship names.

		Returns:
			dict: A dictionary mapping relationship names to their identifiers.
		"""
		if not self._dbFile:
			return { r:None for r in relationships }
		sql = "SELECT i.relationship, r.relationship_id FROM (SELECT ? AS relationship) AS i LEFT JOIN `db`.`relationship` AS r ON r.relationship = LOWER(i.relationship)"
		with self._db:
			ret = { row[0]:row[1] for row in self._db.cursor().executemany(sql, zip(relationships)) }
		return ret
	#getRelationshipIDs()
	
	
	def getRoleID(self, role):
		"""
		Retrieves the identifier for a given role.

		Args:
			role (str): The role name.

		Returns:
			int: The identifier of the role, or None if not found.
		"""
		return self.getRoleIDs([role])[role]
	#getRoleID()
	
	
	def getRoleIDs(self, roles):
		"""
		Retrieves the identifiers for a list of roles.

		Args:
			roles (list): A list of role names.

		Returns:
			dict: A dictionary mapping role names to their identifiers.
		"""
		if not self._dbFile:
			return { r:None for r in roles }
		sql = "SELECT i.role, role_id FROM (SELECT ? AS role) AS i LEFT JOIN `db`.`role` AS r ON r.role = LOWER(i.role)"
		with self._db:
			ret = { row[0]:row[1] for row in self._db.cursor().executemany(sql, zip(roles)) }
		return ret
	#getRoleIDs()
	
	
	def getSourceID(self, source):
		"""
		Retrieves the identifier for a given data source.

		Args:
			source (str): The name of the data source.

		Returns:
			int: The identifier of the data source, or None if not found.
		"""
		return self.getSourceIDs([source])[source]
	#getSourceID()
	
	
	def getSourceIDs(self, sources=None):
		"""
		Retrieves the identifiers for a list of data sources.

		Args:
			sources (list, optional): A list of data source names. Defaults to None, which retrieves information for all sources.

		Returns:
			dict: A dictionary mapping data source names to their identifiers.
		"""
		if not self._dbFile:
			return { s:None for s in (sources or list()) }
		if sources:
			sql = "SELECT i.source, s.source_id FROM (SELECT ? AS source) AS i LEFT JOIN `db`.`source` AS s ON s.source = LOWER(i.source)"
			with self._db:
				ret = { row[0]:row[1] for row in self._db.cursor().executemany(sql, zip(sources)) }
		else:
			sql = "SELECT source, source_id FROM `db`.`source`"
			with self._db:
				ret = { row[0]:row[1] for row in self._db.cursor().execute(sql) }
		return ret
	#getSourceIDs()
	
	
	def getSourceIDVersion(self, sourceID):
		"""
		Retrieves the version of a data source given its identifier.

		Args:
			sourceID (int): The identifier of the data source.

		Returns:
			str: The version of the data source, or None if not found.
		"""
		sql = "SELECT version FROM `db`.`source` WHERE source_id = ?"
		ret = None
		with self._db:
			for row in self._db.cursor().execute(sql, (sourceID,)):
				ret = row[0]
		return ret
	#getSourceIDVersion()
	
	
	def getSourceIDOptions(self, sourceID):
		"""
		Retrieves the options associated with a data source given its identifier.

		Args:
			sourceID (int): The identifier of the data source.

		Returns:
			dict: A dictionary mapping option names to their values for the given data source.
		"""
		sql = "SELECT option, value FROM `db`.`source_option` WHERE source_id = ?"
		with self._db:
			ret = { row[0]:row[1] for row in self._db.cursor().execute(sql, (sourceID,)) }
		return ret
	#getSourceIDOptions()
	
	
	def getSourceIDFiles(self, sourceID):
		"""
		Retrieves information about files associated with a data source given its identifier.

		Args:
			sourceID (int): The identifier of the data source.

		Returns:
			dict: A dictionary mapping filenames to tuples containing their modified date, size, and md5 hash.
		"""
		sql = "SELECT filename, COALESCE(modified,''), COALESCE(size,''), COALESCE(md5,'') FROM `db`.`source_file` WHERE source_id = ?"
		with self._db:
			ret = { row[0]:tuple(row[1:]) for row in self._db.cursor().execute(sql, (sourceID,)) }
		return ret
	#getSourceIDFiles()
	
	
	def getTypeID(self, type):
		"""
		Retrieves the identifier for a given type.

		Args:
			type (str): The name of the type.

		Returns:
			int: The identifier of the type, or None if not found.
		"""
		return self.getTypeIDs([type])[type]
	#getTypeID()
	
	
	def getTypeIDs(self, types):
		"""
		Retrieves the identifiers for a list of types.

		Args:
			types (list): A list of type names.

		Returns:
			dict: A dictionary mapping type names to their identifiers.
		"""
		if not self._dbFile:
			return { t:None for t in types }
		sql = "SELECT i.type, t.type_id FROM (SELECT ? AS type) AS i LEFT JOIN `db`.`type` AS t ON t.type = LOWER(i.type)"
		with self._db:
			ret = { row[0]:row[1] for row in self._db.cursor().executemany(sql, zip(types)) }
		return ret
	#getTypeIDs()
	
	def getSubtypeID(self, subtype):
		"""
		Retrieves the identifier for a given subtype.

		Args:
			subtype (str): The name of the subtype.

		Returns:
			int: The identifier of the subtype, or None if not found.
		"""
		return self.getSubtypeIDs([subtype])[subtype]
	#getSubtypeID()
	
	
	def getSubtypeIDs(self, subtypes):
		"""
		Retrieves subtype IDs for given subtype names from the database.

		Args:
			subtypes (list): A list of subtype names.

		Returns:
			dict: A dictionary where keys are subtype names and values are their corresponding subtype IDs.
					If a subtype is not found in the database, its value in the dictionary will be None.
		"""
		if not self._dbFile:
			return { t:None for t in subtypes }
		sql = "SELECT i.subtype, t.subtype_id FROM (SELECT ? AS subtype) AS i LEFT JOIN `db`.`subtype` AS t ON t.subtype = LOWER(i.subtype)"
		with self._db:
			ret = { row[0]:row[1] for row in self._db.cursor().executemany(sql, zip(subtypes)) }
		return ret
	#getSubtypeIDs()
	
	##################################################
	# snp data retrieval
	
	
	def generateCurrentRSesByRSes(self, rses, tally=None):
		"""
		Generates current RS IDs by merging RS IDs from the database.

		Args:
			rses (list): A list of tuples, where each tuple contains (rsMerged, extra).
			tally (dict, optional): A dictionary to store tally counts for 'merge' and 'match'. Defaults to None.

		Yields:
			tuple: A tuple containing (rsMerged, extra, rsCurrent).
		"""
		# rses=[ (rsInput,extra), ... ]
		# tally=dict()
		# yield:[ (rsInput,extra,rsCurrent), ... ]
		sql = """
SELECT i.rsMerged, i.extra, COALESCE(sm.rsCurrent, i.rsMerged) AS rsCurrent
FROM (SELECT ? AS rsMerged, ? AS extra) AS i
LEFT JOIN `db`.`snp_merge` AS sm USING (rsMerged)
"""
		with self._db:
			if tally != None:
				numMerge = numMatch = 0
				for row in self._db.cursor().executemany(sql, rses):
					if row[2] != row[0]:
						numMerge += 1
					else:
						numMatch += 1
					yield row
				tally['merge'] = numMerge
				tally['match'] = numMatch
			else:
				for row in self._db.cursor().executemany(sql, rses):
					yield row
	#generateCurrentRSesByRSes()
	
	
	def generateSNPLociByRSes(self, rses, minMatch=1, maxMatch=1, validated=None, tally=None, errorCallback=None):
		"""
		Generates SNP loci by RS IDs from the database.

		Args:
			rses (list): A list of tuples, where each tuple contains (rs, extra).
			minMatch (int, optional): Minimum number of matches required. Defaults to 1.
			maxMatch (int, optional): Maximum number of matches allowed. Defaults to 1.
			validated (bool, optional): Flag to filter validated SNP loci. Defaults to None.
			tally (dict, optional): A dictionary to store tally counts for 'zero', 'one', and 'many'. Defaults to None.
			errorCallback (callable, optional): A callable function for error handling. Defaults to None.

		Yields:
			tuple: A tuple containing (rs, extra, chr, pos) for each SNP locus.
		"""
		# rses=[ (rs,extra), ... ]
		# tally=dict()
		# yield:[ (rs,extra,chr,pos), ... ]
		sql = """
SELECT i.rs, i.extra, sl.chr, sl.pos
FROM (SELECT ? AS rs, ? AS extra) AS i
LEFT JOIN `db`.`snp_locus` AS sl
  ON sl.rs = i.rs
ORDER BY sl.chr, sl.pos
"""
		if validated != None:
			sql += "  AND sl.validated = %d" % (1 if validated else 0)
		
		minMatch = int(minMatch) if (minMatch != None) else 0
		maxMatch = int(maxMatch) if (maxMatch != None) else None
		tag = matches = None
		n = numZero = numOne = numMany = 0
		with self._db:
			for row in itertools.chain(self._db.cursor().executemany(sql, rses), [(None,None,None,None)]):
				if tag != row[0:2]:
					if tag:
						if not matches:
							numZero += 1
						elif len(matches) == 1:
							numOne += 1
						else:
							numMany += 1
						
						if minMatch <= len(matches) <= (maxMatch if (maxMatch != None) else len(matches)):
							for match in (matches or [tag+(None,None)]):
								yield match
						elif errorCallback:
							errorCallback("\t".join((t or "") for t in tag), "%s match%s at index %d" % ((len(matches) or "no"),("" if len(matches) == 1 else "es"),n))
					tag = row[0:2]
					matches = list()
					n += 1
				if row[2] and row[3]:
					matches.append(row)
			#foreach row
		if tally != None:
			tally['zero'] = numZero
			tally['one']  = numOne
			tally['many'] = numMany
	#generateSNPLociByRSes()
	
	
	##################################################
	# biopolymer data retrieval
	
	
	def generateBiopolymersByIDs(self, ids):
		"""
		Generates biopolymers by their IDs from the database.

		Args:
			ids (list): A list of tuples, where each tuple contains (id, extra).

		Yields:
			tuple: A tuple containing (biopolymer_id, extra, type_id, label, description) for each biopolymer.
		"""
		# ids=[ (id,extra), ... ]
		# yield:[ (id,extra,type_id,label,description), ... ]
		sql = "SELECT biopolymer_id, ?2 AS extra, type_id, label, description FROM `db`.`biopolymer` WHERE biopolymer_id = ?1"
		return self._db.cursor().executemany(sql, ids)
	#generateBiopolymersByIDs()
	
	
	def _lookupBiopolymerIDs(self, typeID, identifiers, minMatch, maxMatch, tally, errorCallback):
		"""
		Looks up biopolymer IDs based on identifiers from the database.

		Args:
			typeID (int or Falseish): Type ID of the biopolymer, or Falseish for any type.
			identifiers (list): A list of tuples, where each tuple contains (namespace, name, extra).
			minMatch (int or Falseish): Minimum number of matches required, or Falseish for none.
			maxMatch (int or Falseish): Maximum number of matches allowed, or Falseish for none.
			tally (dict or None): A dictionary to store tally counts for 'zero', 'one', and 'many'. Defaults to None.
			errorCallback (callable): A callable function for error handling.

		Yields:
			tuple: A tuple containing (namespace, name, extra, id) for each matched biopolymer.
		"""
		# typeID=int or Falseish for any
		# identifiers=[ (namespace,name,extra), ... ]
		#   namespace='' or '*' for any, '-' for labels, '=' for biopolymer_id
		# minMatch=int or Falseish for none
		# maxMatch=int or Falseish for none
		# tally=dict() or None
		# errorCallback=callable(position,input,error)
		# yields (namespace,name,extra,id)
		
		sql = """
SELECT i.namespace, i.identifier, i.extra, COALESCE(bID.biopolymer_id,bLabel.biopolymer_id,bName.biopolymer_id) AS biopolymer_id
FROM (SELECT ?1 AS namespace, ?2 AS identifier, ?3 AS extra) AS i
LEFT JOIN `db`.`biopolymer` AS bID
  ON i.namespace = '='
  AND bID.biopolymer_id = 1*i.identifier
  AND ( ({0} IS NULL) OR (bID.type_id = {0}) )
LEFT JOIN `db`.`biopolymer` AS bLabel
  ON i.namespace = '-'
  AND bLabel.label = i.identifier
  AND ( ({0} IS NULL) OR (bLabel.type_id = {0}) )
LEFT JOIN `db`.`namespace` AS n
  ON i.namespace NOT IN ('=','-')
  AND n.namespace = COALESCE(NULLIF(NULLIF(LOWER(TRIM(i.namespace)),''),'*'),n.namespace)
LEFT JOIN `db`.`biopolymer_name` AS bn
  ON i.namespace NOT IN ('=','-')
  AND bn.name = i.identifier
  AND bn.namespace_id = n.namespace_id
LEFT JOIN `db`.`biopolymer` AS bName
  ON i.namespace NOT IN ('=','-')
  AND bName.biopolymer_id = bn.biopolymer_id
  AND ( ({0} IS NULL) OR (bName.type_id = {0}) )
""".format(int(typeID) if typeID else "NULL")
		
		minMatch = int(minMatch) if (minMatch != None) else 0
		maxMatch = int(maxMatch) if (maxMatch != None) else None
		tag = matches = None
		n = numZero = numOne = numMany = 0
		with self._db:
			for row in itertools.chain(self._db.cursor().executemany(sql, identifiers), [(None,None,None,None)]):
				if tag != row[0:3]:
					if tag:
						if not matches:
							numZero += 1
						elif len(matches) == 1:
							numOne += 1
						else:
							numMany += 1
						
						if minMatch <= len(matches) <= (maxMatch if (maxMatch != None) else len(matches)):
							for match in (matches or [tag+(None,)]):
								yield match
						elif errorCallback:
							errorCallback("\t".join((t or "") for t in tag), "%s match%s at index %d" % ((len(matches) or "no"),("" if len(matches) == 1 else "es"),n))
					tag = row[0:3]
					matches = set()
					n += 1
				if row[3]:
					matches.add(row)
			#foreach row
		if tally != None:
			tally['zero'] = numZero
			tally['one']  = numOne
			tally['many'] = numMany
	#_lookupBiopolymerIDs()
	
	
	def generateBiopolymerIDsByIdentifiers(self, identifiers, minMatch=1, maxMatch=1, tally=None, errorCallback=None):
		"""
		Retrieve biopolymer IDs based on identifiers such as namespace and name.

		Parameters:
		-----------
		identifiers : list of tuples
			Each tuple contains (namespace, name, extra).
		minMatch : int, optional
			Minimum number of matches allowed (default is 1).
		maxMatch : int, optional
			Maximum number of matches allowed (default is 1).
		tally : dict, optional
			Dictionary to store match counts (default is None).
		errorCallback : callable, optional
			Function to handle errors.

		Returns:
		--------
		Generator object yielding biopolymer IDs based on the given identifiers.
		"""
		# identifiers=[ (namespace,name,extra), ... ]
		return self._lookupBiopolymerIDs(None, identifiers, minMatch, maxMatch, tally, errorCallback)
	#generateBiopolymerIDsByIdentifiers()
	
	
	def generateTypedBiopolymerIDsByIdentifiers(self, typeID, identifiers, minMatch=1, maxMatch=1, tally=None, errorCallback=None):
		"""
		Retrieve biopolymer IDs based on identifiers with a specific type.

		Parameters:
		-----------
		typeID : int or None
			Specific type ID for filtering.
		identifiers : list of tuples
			Each tuple contains (namespace, name, extra).
		minMatch : int, optional
			Minimum number of matches allowed (default is 1).
		maxMatch : int, optional
			Maximum number of matches allowed (default is 1).
		tally : dict, optional
			Dictionary to store match counts (default is None).
		errorCallback : callable, optional
			Function to handle errors.

		Returns:
		--------
		Generator object yielding biopolymer IDs based on the given identifiers and type ID.
		"""
		# identifiers=[ (namespace,name,extra), ... ]
		return self._lookupBiopolymerIDs(typeID, identifiers, minMatch, maxMatch, tally, errorCallback)
	#generateTypedBiopolymerIDsByIdentifiers()
	
	
	def _searchBiopolymerIDs(self, typeID, texts):
		"""
		Helper method to perform text-based search for biopolymer IDs.

		Parameters:
		-----------
		typeID : int or None
			Specific type ID for filtering.
		texts : list of tuples
			Each tuple contains (text, extra).

		Yields:
		-------
		Tuples containing biopolymer IDs based on the given search criteria and type ID.
		"""
		# texts=[ (text,extra), ... ]
		# yields (extra,label,id)
		
		sql = """
SELECT ?2 AS extra, b.label, b.biopolymer_id
FROM `db`.`biopolymer` AS b
LEFT JOIN `db`.`biopolymer_name` AS bn USING (biopolymer_id)
WHERE
  (
    b.label LIKE '%'||?1||'%'
    OR b.description LIKE '%'||?1||'%'
    OR bn.name LIKE '%'||?1||'%'
  )
"""
		
		if typeID:
			sql += """
  AND b.type_id = %d
""" % typeID
		#if typeID
		
		sql += """
GROUP BY b.biopolymer_id
"""
		
		return self._db.cursor().executemany(sql, texts)
	#_searchBiopolymerIDs()
	
	
	def generateBiopolymerIDsBySearch(self, searches):
		"""
		Retrieve biopolymer IDs based on a text-based search.

		Parameters:
		-----------
		searches : list of tuples
			Each tuple contains (text, extra).

		Returns:
		--------
		Generator object yielding biopolymer IDs based on the given search criteria.
		"""
		# searches=[ (text,extra), ... ]
		return self._searchBiopolymerIDs(None, searches)
	#generateBiopolymerIDsBySearch()
	
	
	def generateTypedBiopolymerIDsBySearch(self, typeID, searches):
		"""
		Retrieve biopolymer IDs based on a text-based search with a specific type.

		Parameters:
		-----------
		typeID : int or None
			Specific type ID for filtering.
		searches : list of tuples
			Each tuple contains (text, extra).

		Returns:
		--------
		Generator object yielding biopolymer IDs based on the given search criteria and type ID.
		"""
		# searches=[ (text,extra), ... ]
		return self._searchBiopolymerIDs(typeID, searches)
	#generateTypedBiopolymerIDsBySearch()
	
	
	def generateBiopolymerNameStats(self, namespaceID=None, typeID=None):
		"""
		Generate statistics on biopolymer names, including counts of unique and ambiguous names.

		Parameters:
		-----------
		namespaceID : int or None, optional
			Optional namespace ID filter.
		typeID : int or None, optional
			Optional type ID filter.

		Yields:
		-------
		Tuples containing statistics for biopolymer names:
			- `namespace`: Name of the namespace.
			- `names`: Total number of names.
			- `unique`: Number of unique names.
			- `ambiguous`: Number of ambiguous names.
		"""
		sql = """
SELECT
  `namespace`,
  COUNT() AS `names`,
  SUM(CASE WHEN matches = 1 THEN 1 ELSE 0 END) AS `unique`,
  SUM(CASE WHEN matches > 1 THEN 1 ELSE 0 END) AS `ambiguous`
FROM (
  SELECT bn.namespace_id, bn.name, COUNT(DISTINCT bn.biopolymer_id) AS matches
  FROM `db`.`biopolymer_name` AS bn
"""
		
		if typeID:
			sql += """
  JOIN `db`.`biopolymer` AS b
    ON b.biopolymer_id = bn.biopolymer_id AND b.type_id = %d
""" % typeID
		
		if namespaceID:
			sql += """
  WHERE bn.namespace_id = %d
""" % namespaceID
		
		sql += """
  GROUP BY bn.namespace_id, bn.name
)
JOIN `db`.`namespace` AS n USING (namespace_id)
GROUP BY namespace_id
"""
		
		for row in self._db.cursor().execute(sql):
			yield row
	#generateBiopolymerNameStats()
	
	
	##################################################
	# group data retrieval
	
	
	def generateGroupsByIDs(self, ids):
		"""
		Retrieve groups based on provided group IDs.

		Parameters:
		-----------
		ids : list of tuples
			Each tuple contains (group_id, extra).

		Yields:
		-------
		Tuples containing group information:
			(group_id, extra, type_id, subtype_id, label, description)
		"""
		# ids=[ (id,extra), ... ]
		# yield:[ (id,extra,type_id,subtype_id,label,description), ... ]
		sql = "SELECT group_id, ?2 AS extra, type_id, subtype_id, label, description FROM `db`.`group` WHERE group_id = ?1"
		return self._db.cursor().executemany(sql, ids)
	#generateGroupsByIDs()
	
	
	def _lookupGroupIDs(self, typeID, identifiers, minMatch, maxMatch, tally, errorCallback):
		"""
		Helper method to look up group IDs based on identifiers.

		Parameters:
		-----------
		typeID : int or None
			Specific type ID for filtering.
		identifiers : list of tuples
			Each tuple contains (namespace, name, extra).
		minMatch : int or None
			Minimum number of matches allowed.
		maxMatch : int or None
			Maximum number of matches allowed.
		tally : dict or None
			Dictionary to store match counts.
		errorCallback : callable or None
			Function to handle errors.

		Yields:
		-------
		Tuples containing (namespace, name, extra, group_id).
		"""
		# typeID=int or Falseish for any
		# identifiers=[ (namespace,name,extra), ... ]
		#   namespace='' or '*' for any, '-' for labels, '=' for group_id
		# minMatch=int or Falseish for none
		# maxMatch=int or Falseish for none
		# tally=dict() or None
		# errorCallback=callable(input,error)
		# yields (namespace,name,extra,id)
		
		sql = """
SELECT i.namespace, i.identifier, i.extra, COALESCE(gID.group_id,gLabel.group_id,gName.group_id) AS group_id
FROM (SELECT ?1 AS namespace, ?2 AS identifier, ?3 AS extra) AS i
LEFT JOIN `db`.`group` AS gID
  ON i.namespace = '='
  AND gID.group_id = 1*i.identifier
  AND ( ({0} IS NULL) OR (gID.type_id = {0}) )
LEFT JOIN `db`.`group` AS gLabel
  ON i.namespace = '-'
  AND gLabel.label = i.identifier
  AND ( ({0} IS NULL) OR (gLabel.type_id = {0}) )
LEFT JOIN `db`.`namespace` AS n
  ON i.namespace NOT IN ('=','-')
  AND n.namespace = COALESCE(NULLIF(NULLIF(LOWER(TRIM(i.namespace)),''),'*'),n.namespace)
LEFT JOIN `db`.`group_name` AS gn
  ON i.namespace NOT IN ('=','-')
  AND gn.name = i.identifier
  AND gn.namespace_id = n.namespace_id
LEFT JOIN `db`.`group` AS gName
  ON i.namespace NOT IN ('=','-')
  AND gName.group_id = gn.group_id
  AND ( ({0} IS NULL) OR (gName.type_id = {0}) )
""".format(int(typeID) if typeID else "NULL")
		
		minMatch = int(minMatch) if (minMatch != None) else 0
		maxMatch = int(maxMatch) if (maxMatch != None) else None
		tag = matches = None
		n = numZero = numOne = numMany = 0
		with self._db:
			for row in itertools.chain(self._db.cursor().executemany(sql, identifiers), [(None,None,None,None)]):
				if tag != row[0:3]:
					if tag:
						if not matches:
							numZero += 1
						elif len(matches) == 1:
							numOne += 1
						else:
							numMany += 1
						
						if minMatch <= len(matches) <= (maxMatch if (maxMatch != None) else len(matches)):
							for match in (matches or [tag+(None,)]):
								yield match
						elif errorCallback:
							errorCallback("\t".join((t or "") for t in tag), "%s match%s at index %d" % ((len(matches) or "no"),("" if len(matches) == 1 else "es"),n))
					tag = row[0:3]
					matches = set()
					n += 1
				if row[3]:
					matches.add(row)
			#foreach row
		if tally != None:
			tally['zero'] = numZero
			tally['one']  = numOne
			tally['many'] = numMany
	#_lookupGroupIDs()
	
	
	def generateGroupIDsByIdentifiers(self, identifiers, minMatch=1, maxMatch=1, tally=None, errorCallback=None):
		"""
		Generate group IDs based on identifiers such as namespace and name.

		Parameters:
		-----------
		identifiers : list of tuples
			Each tuple contains (namespace, name, extra).
		minMatch : int, optional
			Minimum number of matches allowed (default is 1).
		maxMatch : int, optional
			Maximum number of matches allowed (default is 1).
		tally : dict, optional
			Dictionary to store match counts (default is None).
		errorCallback : callable, optional
			Function to handle errors.

		Yields:
		-------
		Tuples containing (namespace, name, extra, group_id).
		"""
		# identifiers=[ (namespace,name,extra), ... ]
		return self._lookupGroupIDs(None, identifiers, minMatch, maxMatch, tally, errorCallback)
	#generateGroupIDsByIdentifiers()
	
	
	def generateTypedGroupIDsByIdentifiers(self, typeID, identifiers, minMatch=1, maxMatch=1, tally=None, errorCallback=None):
		"""
		Generate group IDs based on identifiers with a specific type.

		Parameters:
		-----------
		typeID : int
			Specific type ID for filtering.
		identifiers : list of tuples
			Each tuple contains (namespace, name, extra).
		minMatch : int, optional
			Minimum number of matches allowed (default is 1).
		maxMatch : int, optional
			Maximum number of matches allowed (default is 1).
		tally : dict, optional
			Dictionary to store match counts (default is None).
		errorCallback : callable, optional
			Function to handle errors.

		Yields:
		-------
		Tuples containing (namespace, name, extra, group_id).
		"""

		# identifiers=[ (namespace,name,extra), ... ]
		return self._lookupGroupIDs(typeID, identifiers, minMatch, maxMatch, tally, errorCallback)
	#generateTypedGroupIDsByIdentifiers()
	
	
	def _searchGroupIDs(self, typeID, texts):
		"""
		Helper method to perform text-based search for group IDs.

		Parameters:
		-----------
		typeID : int or None
			Specific type ID for filtering.
		texts : list of tuples
			Each tuple contains (text, extra).

		Yields:
		-------
		Tuples containing group IDs based on the given search criteria and type ID.
		"""
		# texts=[ (text,extra), ... ]
		# yields (extra,label,id)
		
		sql = """
SELECT ?2 AS extra, g.label, g.group_id
FROM `db`.`group` AS g
LEFT JOIN `db`.`group_name` AS gn USING (group_id)
WHERE
  (
    g.label LIKE '%'||?1||'%'
    OR g.description LIKE '%'||?1||'%'
    OR gn.name LIKE '%'||?1||'%'
  )
"""
		
		if typeID:
			sql += """
  AND g.type_id = %d
""" % typeID
		#if typeID
		
		sql += """
GROUP BY g.group_id
"""
		
		return self._db.cursor().executemany(sql, texts)
	#_searchGroupIDs()
	
	
	def generateGroupIDsBySearch(self, searches):
		"""
		Retrieve group IDs based on a text-based search.

		Parameters:
		-----------
		searches : list of tuples
			Each tuple contains (text, extra).

		Yields:
		-------
		Tuples containing group IDs based on the given search criteria.
			(extra, label, group_id)
		"""
		# searches=[ (text,extra), ... ]
		return self._searchGroupIDs(None, searches)
	#generateGroupIDsBySearch()
	
	
	def generateTypedGroupIDsBySearch(self, typeID, searches):
		"""
		Retrieve group IDs based on a text-based search with a specific type.

		Parameters:
		-----------
		typeID : int
			Specific type ID for filtering.
		searches : list of tuples
			Each tuple contains (text, extra).

		Yields:
		-------
		Tuples containing group IDs based on the given search criteria and type ID.
			(extra, label, group_id)
		"""
		# searches=[ (text,extra), ... ]
		return self._searchGroupIDs(typeID, searches)
	#generateTypedGroupIDsBySearch()
	
	
	def generateGroupNameStats(self, namespaceID=None, typeID=None):
		"""
		Generate statistics on group names.

		Parameters:
		-----------
		namespaceID : int or None, optional
			Namespace ID for filtering (default is None).
		typeID : int or None, optional
			Specific type ID for filtering (default is None).

		Yields:
		-------
		Tuples containing statistics on group names:
			(namespace, names, unique, ambiguous)
		"""
		sql = """
SELECT
  `namespace`,
  COUNT() AS `names`,
  SUM(CASE WHEN matches = 1 THEN 1 ELSE 0 END) AS `unique`,
  SUM(CASE WHEN matches > 1 THEN 1 ELSE 0 END) AS `ambiguous`
FROM (
  SELECT gn.namespace_id, gn.name, COUNT(DISTINCT gn.group_id) AS matches
  FROM `db`.`group_name` AS gn
"""
		
		if typeID:
			sql += """
  JOIN `db`.`group` AS g
    ON g.group_id = gn.group_id AND g.type_id = %d
""" % typeID
		
		if namespaceID:
			sql += """
  WHERE gn.namespace_id = %d
""" % namespaceID
		
		sql += """
  GROUP BY gn.namespace_id, gn.name
)
JOIN `db`.`namespace` AS n USING (namespace_id)
GROUP BY namespace_id
"""
		
		for row in self._db.cursor().execute(sql):
			yield row
	#generateGroupNameStats()
	
	
	##################################################
	# liftover
	# 
	# originally from UCSC
	# reimplemented? in C++ for Biofilter 1.0 by Eric Torstenson
	# reimplemented again in Python by John Wallace
	
	
	def hasLiftOverChains(self, oldHG, newHG):
		"""
		Check if there are liftOver chains between old and new genome assemblies.

		Parameters:
		-----------
		oldHG : str
			Old genome assembly identifier.
		newHG : str
			New genome assembly identifier.

		Returns:
		--------
		int
			Number of liftOver chains found between old and new genome assemblies.
		"""
		sql = "SELECT COUNT() FROM `db`.`chain` WHERE old_ucschg = ? AND new_ucschg = ?"
		return max(row[0] for row in self._db.cursor().execute(sql, (oldHG, newHG)))
	#hasLiftOverChains()
	
	
	def _generateApplicableLiftOverChains(self, oldHG, newHG, chrom, start, end):
		"""
		Generate applicable liftOver chains for a specific region.

		Parameters:
		-----------
		oldHG : str
			Old genome assembly identifier.
		newHG : str
			New genome assembly identifier.
		chrom : str
			Chromosome name.
		start : int
			Start position of the region.
		end : int
			End position of the region.

		Yields:
		-------
		Tuples containing liftOver chain information for the given region.
			(chain_id, old_chr, score, old_start, old_end, new_start, is_fwd, new_chr, old_start, old_end, new_start)
		"""
		conv = (oldHG,newHG)
		if conv in self._liftOverCache:
			chains = self._liftOverCache[conv]
		else:
			chains = {'data':{}, 'keys':{}}
			sql = """
SELECT chain_id,
  c.old_chr, c.score, c.old_start, c.old_end, c.new_start, c.is_fwd, c.new_chr,
  cd.old_start, cd.old_end, cd.new_start
FROM `db`.`chain` AS c
JOIN `db`.`chain_data` AS cd USING (chain_id)
WHERE c.old_ucschg=? AND c.new_ucschg=?
ORDER BY c.old_chr, score DESC, cd.old_start
"""
			for row in self._db.cursor().execute(sql, conv):
				chain = (row[2], row[3], row[4], row[5], row[6], row[7], row[0])
				chr = row[1]
				
				if chr not in chains['data']:
					chains['data'][chr] = {chain: []}
					chains['keys'][chr] = [chain]
				elif chain not in chains['data'][chr]:
					chains['data'][chr][chain] = []
					chains['keys'][chr].append(chain)
				
				chains['data'][chr][chain].append( (row[8],row[9],row[10]) )
			#foreach row
			
			# Sort the chains by score
			for k in chains['keys']:
				chains['keys'][k].sort(reverse=True)
			
			self._liftOverCache[conv] = chains
		#if chains are cached
		
		for c in chains['keys'].get(chrom, []):
			# if the region overlaps the chain... (1-based, closed intervals)
			if start <= c[2] and end >= c[1]:
				data = chains['data'][chrom][c]
				idx = bisect.bisect(data, (start, sys.maxsize, sys.maxsize)) - 1
				while (idx < 0) or (data[idx][1] < start):
					idx = idx + 1
				while (idx < len(data)) and (data[idx][0] <= end):
					yield (c[-1], data[idx][0], data[idx][1], data[idx][2], c[4], c[5])
					idx = idx + 1
		#foreach chain
	#_generateApplicableLiftOverChains()
	
	
	def _liftOverRegionUsingChains(self, label, start, end, extra, first_seg, end_seg, total_mapped_sz):
		"""
		Map a region given the 1st and last segment as well as the total mapped size.

		Parameters:
		-----------
		label : str
			Label of the region.
		start : int
			Start position of the region.
		end : int
			End position of the region.
		extra : object
			Additional data associated with the region.
		first_seg : tuple
			First segment information.
		end_seg : tuple
			Last segment information.
		total_mapped_sz : int
			Total mapped size of the region.

		Returns:
		--------
		tuple or None
			Mapped region information if mapped successfully, otherwise None.
		"""
		mapped_reg = None
		
		# The front and end differences are the distances from the
		# beginning of the segment.
		
		# The front difference should be >= 0 and <= size of 1st segment
		front_diff = max(0, min(start - first_seg[1], first_seg[2] - first_seg[1]))
		
		# The end difference should be similar, but w/ last
		end_diff = max(0, min(end - end_seg[1], end_seg[2] - end_seg[1]))
		
		# Now, if we are moving forward, we add the difference
		# to the new_start, backward, we subtract
		# Also, at this point, if backward, swap start/end
		if first_seg[4]:
			new_start = first_seg[3] + front_diff
			new_end = end_seg[3] + end_diff
		else:
			new_start = end_seg[3] - end_diff
			new_end = first_seg[3] - front_diff
		
		# old_startHere, detect if we have mapped a sufficient fraction 
		# of the region.  liftOver uses a default of 95%
		mapped_size = total_mapped_sz - front_diff - (end_seg[2] - end_seg[1] + 1) + end_diff + 1
		
		if mapped_size / float(end - start + 1) >= 0.95: # TODO: configurable threshold?
			mapped_reg = (label, first_seg[5], new_start, new_end, extra)
		
		return mapped_reg
	#_liftOverRegionUsingChains()
	
	
	def generateLiftOverRegions(self, oldHG, newHG, regions, tally=None, errorCallback=None):
		"""
		Generate liftOver regions based on old and new genome assemblies.

		Parameters:
		-----------
		oldHG : str
			Old genome assembly identifier.
		newHG : str
			New genome assembly identifier.
		regions : iterable
			Iterable of regions to be lifted over, where each region is represented as a tuple
			(label, chr, posMin, posMax, extra).
		tally : dict or None, optional
			A dictionary to store the count of lifted and non-lifted regions (default is None).
		errorCallback : function or None, optional
			A callback function to handle errors for non-liftable regions (default is None).

		Yields:
		-------
		tuple
			Mapped regions in the format (label, chrom, new_start, new_end, extra).
		"""
		# regions=[ (label,chr,posMin,posMax,extra), ... ]
		oldHG = int(oldHG)
		newHG = int(newHG)
		numNull = numLift = 0
		for region in regions:
			label,chrom,start,end,extra = region
			
			if start > end:
				start,end = end,start
			is_region = (start != end)
			
			# find and apply chains
			mapped_reg = None
			curr_chain = None
			total_mapped_sz = 0
			first_seg = None
			end_seg = None
			for seg in self._generateApplicableLiftOverChains(oldHG, newHG, chrom, start, end):
				if curr_chain is None:
					curr_chain = seg[0]
					first_seg = seg
					end_seg = seg
					total_mapped_sz = seg[2] - seg[1] + 1
				elif seg[0] != curr_chain:
					mapped_reg = self._liftOverRegionUsingChains(label, start, end, extra, first_seg, end_seg, total_mapped_sz)
					if mapped_reg:
						break
					curr_chain = seg[0]
					first_seg = seg
					end_seg = seg
					total_mapped_sz = seg[2] - seg[1] + 1
				else:
					end_seg = seg
					total_mapped_sz = total_mapped_sz + seg[2] - seg[1] + 1
			
			if not mapped_reg and first_seg is not None:
				mapped_reg = self._liftOverRegionUsingChains(label, start, end, extra, first_seg, end_seg, total_mapped_sz)
			
			if mapped_reg:
				numLift += 1
				if not is_region:
					mapped_reg = (mapped_reg[0], mapped_reg[1], mapped_reg[2], mapped_reg[2], extra)
				yield mapped_reg
			else:
				numNull += 1
				if errorCallback:
					errorCallback(region)
		#foreach region
		
		if tally != None:
			tally['null'] = numNull
			tally['lift'] = numLift
	#generateLiftOverRegions()
	
	
	def generateLiftOverLoci(self, oldHG, newHG, loci, tally=None, errorCallback=None):
		"""
		Generate liftOver loci based on old and new genome assemblies.

		Parameters:
		-----------
		oldHG : str
			Old genome assembly identifier.
		newHG : str
			New genome assembly identifier.
		loci : iterable
			Iterable of loci to be lifted over, where each locus is represented as a tuple
			(label, chr, pos, extra).
		tally : dict or None, optional
			A dictionary to store the count of lifted and non-lifted loci (default is None).
		errorCallback : function or None, optional
			A callback function to handle errors for non-liftable loci (default is None).

		Returns:
		--------
		iterable
			Yields new loci in the format (label, chrom, new_pos, extra) for each successfully
			lifted locus.
		"""
		# loci=[ (label,chr,pos,extra), ... ]
		regions = ((l[0],l[1],l[2],l[2],l[3]) for l in loci)
		newloci = ((r[0],r[1],r[2],r[4]) for r in self.generateLiftOverRegions(oldHG, newHG, regions, tally, errorCallback))
		return newloci
	#generateLiftOverLoci()
	
	
#Database


# TODO: find a better place for this liftover testing code
"""
if __name__ == "__main__":
	inputFile = file(sys.argv[1])
	loki = Database(sys.argv[2])
	outputFile = file(sys.argv[3],'w')
	unmapFile = file(sys.argv[4],'w')
	oldHG = int(sys.argv[5]) if (len(sys.argv) > 5) else 18
	newHG = int(sys.argv[6]) if (len(sys.argv) > 6) else 19
	
	def generateInput():
		for line in inputFile:
			chrom,start,end = line.split()
			if chrom[:3].upper() in ('CHM','CHR'):
				chrom = chrom[3:]
			yield (None, loki.chr_num.get(chrom,-1), int(start), int(end))
	
	def errorCallback(region):
		print >> unmapFile, "chr"+loki.chr_name.get(region[1],'?'), region[2], region[3]
	
	for region in loki.generateLiftOverRegions(oldHG, newHG, generateInput(), errorCallback=errorCallback):
		print >> outputFile, "chr"+loki.chr_name.get(region[1],'?'), region[2], region[3]
"""
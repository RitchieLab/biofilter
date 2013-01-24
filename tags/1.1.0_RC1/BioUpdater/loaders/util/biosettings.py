'''
Created on May 7, 2010

@author: torstees
'''

import time, datetime, os.path, sys, struct
import sqlite3
import region_manager
import traceback #used to debug weird data in the database

def MakeAscii(str):
        s = ""
        for i in str:
                try:
                        i.encode("iso-8859-8")
                except UnicodeDecodeError:
                        i = " "
                        continue

                s += i
        return s

class BioSettings:
	'''
	This class represents the biosettings database. If we decide to migrate to another database, we should be able to 
	get away with just replacing this file.
	'''
	
	nextID = 100
	def NextID(self):
		BioSettings.nextID += 1
		#print "ASDFASDFASDF:: The Next ID is: ", BioSettings.nextID
		return BioSettings.nextID - 1
		
	def __init__(self, name = None):
		'''
		If name is None, we will create a name and start with a new database
		'''
		self.cwd			= os.getcwd()
		if name == None:
			name = self.BuildDbFilename("bio-settings")
		else:
			name = os.path.join(self.cwd, name)
		self.filename = name
		self.db = None
		self.snpRoles = dict()
		self.maxRoleID = 1
		self.aliasToID = dict()		#entrezID -> geneID
		self.regions = region_manager.RegionManager()
	
	def PurgeGroupData(self, groupTypeID):
		cur = self.db.cursor()
		#print "Purging all group data for type: ", groupTypeID
		cur.execute("DELETE FROM group_associations WHERE EXISTS (SELECT * FROM groups WHERE group_associations.group_id=groups.group_id AND groups.group_type_id=?)", (groupTypeID,))
		#print "DELETE FROM group_associations WHERE EXISTS (SELECT * FROM groups WHERE group_associations.group_id=groups.group_id AND groups.group_type_id=%s)" % (groupTypeID,)
		cur.execute("DELETE FROM group_relationships WHERE EXISTS (SELECT * FROM groups WHERE group_relationships.child_id=groups.group_id AND groups.group_type_id=?)", (groupTypeID,))
		#print "DELETE FROM group_relationships WHERE EXISTS (SELECT * FROM groups WHERE group_relationships.child_id=groups.group_id AND groups.group_type_id=%s)" % (groupTypeID)
		cur.execute("DELETE FROM groups WHERE group_type_id=?", (groupTypeID,))
		#print "DELETE FROM groups WHERE group_type_id=%s" % (groupTypeID)
		cur.execute("DELETE FROM group_type WHERE group_type_id=?", (groupTypeID,))
		#print "DELETE FROM group_type WHERE group_type_id=%s" % (groupTypeID)
		self.Commit()
		cur.execute("SELECT MAX(group_id) FROM groups")
		maxid = cur.fetchone()[0]
		if maxid and maxid > 100:
			BioSettings.nextID					= maxid  + 1
		else:
			BioSettings.nextID					= 100

	def CommitGroup(self, typeID, roleID, name, desc, timestamp):
		#print "CommitGroup"
		cur		= self.GetCursor()
		#print "DELETE FROM group_type WHERE group_type_id=%s" % typeID
		cur.execute("DELETE FROM group_type WHERE group_type_id=?", (typeID,))
		try:
			#print "INSERT INTO group_type (group_type_id, role_id, group_type, download_date) VALUES (%s,%s,%s)", (typeID, roleID, name, timestamp)
			cur.execute("INSERT INTO group_type (group_type_id, role_id, group_type, download_date) VALUES (?,?,?,?)", (typeID, roleID, name, timestamp))
			cur.execute("INSERT INTO groups (group_type_id, group_id, group_name, group_desc) VALUES (?,?,?,?)", (typeID, typeID, name, ""))
		except sqlite3.Error, e:
			pass
			#print "Insertion error: ", e[0]

	def CommitPathway(self, typeID, groupID, name, desc):
		cur		= self.GetCursor()
		#if groupID < 100:
		#	print>>sys.stderr, "\n----\nINSERT INTO groups (group_type_id, group_id, group_name, group_desc) VALUES (%s,%s,%s,%s)" % (typeID, groupID, name, desc)
		#	traceback.print_exc(file=sys.stderr)
		try:
			cur.execute("INSERT INTO groups (group_type_id, group_id, group_name, group_desc) VALUES (?,?,?,?)", (typeID, groupID, name, desc))
			#print "INSERT INTO groups (group_type_id, group_id, group_name, group_desc) VALUES (%s,%s,%s,%s)" % (typeID, groupID, name, desc)
		except sqlite3.Error, e:
			pass
			#print "INSERT INTO groups (group_type_id,group_id, group_name, group_desc) VALUES (%s,%s,%s,%s)" % (typeID, groupID, name, desc)
			#print "Insertion error: ", e[0]
			#self.Commit()
			#raise
			
	def AssociateGene(self, groupID, geneID):
		cur		= self.GetCursor()

		try:
			cur.execute("INSERT INTO group_associations (group_id, gene_id) VALUES (?,?)", (groupID, geneID))
		except sqlite3.Error, e:
			pass
			#print "SQL Error: INSERT INTO group_associations (group_id, gene_id) VALUES (?,?)", (groupID, geneID)
			#print e[0]
			#sys.exit(1)
		#print "INSERT INTO group_Associations (group_id, gene_id) VALUES (%s,%s)" % (groupID, geneID)
		
	def RelatePathways(self, parentID, childID, relationship, relationshipDesc):
		cur		= self.GetCursor()

		#print "INSERT INTO group_relationships (child_id, parent_id, relationship, relationship_description) VALUES (%s,%s,'%s','%s')" % (childID, parentID, relationship, relationshipDesc)
		#cur.execute("INSERT INTO group_relationships (child_id, parent_id, relationship, relationship_description) VALUES (?,?,?,?)", (childID, parentID, relationship, relationshipDesc))
		try:
			cur.execute("INSERT INTO group_relationships (child_id, parent_id, relationship, relationship_description) VALUES (?,?,?,?)", (childID, parentID, relationship, relationshipDesc))
		except sqlite3.Error, e:
			pass
			#print "INSERT INTO group_relationships (child_id, parent_id, relationship, relationship_description) VALUES (%s,%s,%s,%s)" % (childID, parentID, relationship, relationshipDesc)
			#print "Insertion error: ", e[0]
			#raise
	
	def AddAlias(self, alias, geneID, typeID, label="", desc=""):
		if alias not in self.aliasToID:
			self.aliasToID[alias] = geneID
			cur		= self.GetCursor()
			cur.execute("INSERT INTO region_alias (region_alias_type_id, alias, gene_id, alias_label, alias_desc) VALUES (?,?,?,?,?)", (typeID, alias, geneID, label, desc))
	
	def LoadAliases(self):
		self.regions.LoadRegionData(self.db)
		#self.regions.LoadAliasesFromDB(self.db)

	def BuildDbFilename(self, baseName, ext = "db"):
		d = datetime.datetime.now()
		return os.path.join(self.cwd, "%s-%s%02d%02d.%s" % (baseName, d.year, int(d.month), int(d.day), ext))

	def Commit(self):
		self.db.commit()
		
	def GetCursor(self):
		return self.db.cursor()

	def Empty(self, table):
		dest 				= self.db.cursor()
		dest.execute("DELETE FROM %s" % (table))
		
	def LoadRoles(self):
		self.snpRoles		= dict()
		c					= self.db.cursor()
		c.execute("SELECT id, role FROM snp_role")
		for row in c.fetchall():
			if row[0] > self.maxRoleID:
				self.maxRoleID = row[0]
			self.snpRoles[row[1]] = row[0]

	def GetRoleID(self, role):
		if role not in self.snpRoles:
			self.db.cursor().execute("INSERT INTO snp_role(id, role) VALUES (?,?)", (self.maxRoleID, role))
			self.snpRoles[role] = self.maxRoleID
			self.maxRoleID += 1
		return self.snpRoles[role]
	
	def GetGroupTimestamp(self, groupID):
		ts 					= None
		dest				= self.db.execute("SELECT download_date FROM group_type WHERE group_type_id=?", (groupID,))
		row 				= dest.fetchone()
		if row is not None:
			ts				= time.strptime(row[0].split()[0], "%Y-%m-%d")
		return ts
	
	def ResetDB(self):
		os.remove(self.filename)
		self.InitDB()

	def InitDB(self):
		self.db 			= sqlite3.connect(self.filename)
		dest				= self.db.cursor()
		dest.execute("CREATE TABLE IF NOT EXISTS versions (element VARCHAR(64), version VARCHAR(64))")
		#dest.execute('CREATE TABLE version (version_id INTEGER, ensembl_version INTEGER, hapmap_version INTEGER)')
		dest.execute('CREATE TABLE IF NOT EXISTS regions (gene_id INTEGER UNIQUE PRIMARY KEY, primary_name VARCHAR(128) UNIQUE, chrom VARCHAR(2), description TEXT)')
		dest.execute('CREATE TABLE IF NOT EXISTS populations(population_id INTEGER UNIQUE PRIMARY KEY, population_label VARCHAR(8), pop_ld_comment TEXT, pop_description TEXT)')
		dest.execute('CREATE TABLE IF NOT EXISTS region_bounds (gene_id INTEGER, population_id INTEGER, start INTEGER, end INTEGER)');
		dest.execute("CREATE TABLE IF NOT EXISTS region_alias_type (region_alias_type_id INTEGER UNIQUE PRIMARY KEY, region_alias_type_desc TEXT)")
		dest.execute("CREATE TABLE IF NOT EXISTS region_alias (region_alias_type_id INTEGER, alias VARCHAR(64), gene_id INTEGER, gene_count INTEGER, UNIQUE(gene_id, alias, region_alias_type_id))")
		dest.execute('CREATE TABLE IF NOT EXISTS chromosomes (chromosome VARCHAR(2) UNIQUE, length INTEGER)')
		dest.execute('CREATE TABLE IF NOT EXISTS group_type(group_type_id INTEGER UNIQUE, group_type VARCHAR(64) UNIQUE, role_id INTEGER, download_date DATE)')
		dest.execute('CREATE TABLE IF NOT EXISTS groups(group_type_id INTEGER, group_id INTEGER PRIMARY KEY AUTOINCREMENT, group_name VARCHAR(32) UNIQUE, group_desc TEXT)')
		dest.execute('CREATE TABLE IF NOT EXISTS group_associations(group_id INTEGER, gene_id INTEGER, UNIQUE(group_id, gene_id))')
		dest.execute('CREATE TABLE IF NOT EXISTS group_relationships(child_id INTEGER, parent_id INTEGER, relationship INTEGER, relationship_description TEXT, UNIQUE(child_id, parent_id, relationship))')
		dest.execute('CREATE INDEX IF NOT EXISTS group_relationships_idx ON group_relationships(child_id ASC, parent_id ASC)')
		dest.execute('CREATE TABLE IF NOT EXISTS literature(group_type_id INTEGER, gene_id INTEGER, note TEXT, UNIQUE(group_type_id, gene_id))')
		dest.execute('CREATE TABLE IF NOT EXISTS snp_role (id INTEGER, role VARCHAR)')
		dest.execute("CREATE TABLE IF NOT EXISTS rs_merged (merged_rs_id INTEGER, new_rs_id INTEGER, build INTEGER, current_rs_id INTEGER, expired BOOLEAN);")
		dest.execute("CREATE TABLE IF NOT EXISTS group_role(role_id INTEGER PRIMARY KEY, group_role VARCHAR(64))")
		dest.execute("CREATE TABLE IF NOT EXISTS chain_files(chrom_id INTEGER, orig_assembly INTEGER, chain_data TEXT)")
		dest.execute("CREATE TABLE IF NOT EXISTS build_versions(build VARCHAR(32), orig_assembly INTEGER)")
		dest.execute("CREATE TABLE IF NOT EXISTS dataset_groups(group_type_id INTEGER, chromosome INTEGER, location INTEGER, name VARCHAR(64),score FLOAT, UNIQUE(group_type_id, chromosome, name))")
		#dest.execute("CREATE TABLE IF NOT EXISTS rs_expired (rs_id INTEGER)")
		
		dest.execute("INSERT INTO group_role VALUES (1, 'Disease Independent')")
		dest.execute("INSERT INTO group_role VALUES (2, 'Disease Dependent')")
		dest.execute("INSERT INTO group_role VALUES (3, 'SNP Collection')")
		dest.execute("INSERT INTO group_role VALUES (4, 'Gene Collection')")

		dest.execute("DELETE FROM build_versions")
		dest.execute("INSERT INTO build_versions VALUES ('36.1', 18)")
		dest.execute("INSERT INTO build_versions VALUES ('36', 18)")
		dest.execute("INSERT INTO build_versions VALUES ('35', 17)")
		dest.execute("INSERT INTO build_versions VALUES ('34', 16)")
		dest.execute("INSERT INTO build_versions VALUES ('b36', 18)")
		dest.execute("INSERT INTO build_versions VALUES ('b35', 17)")
		dest.execute("INSERT INTO build_versions VALUES ('b34', 16)")

		self.Commit()
		dest.close()

	def SetVersion(self, name, version):
		dest = self.db.cursor()
		dest.execute("DELETE FROM versions WHERE element=?", (name,))
		dest.execute("INSERT INTO versions VALUES (?,?)", (name, version))

	def OpenDB(self):
		#print "DB: ", self.filename
		if not os.path.isfile(self.filename):
			self.InitDB()
		else:
			self.db = sqlite3.connect(self.filename)
			c = self.db.cursor()
			nextID = 99
			c.execute("SELECT MAX(group_id) FROM groups")
			row = c.fetchone()
			if row[0] != None:
				nextID = row[0]
			BioSettings.nextID = nextID + 1
		return self.db.cursor()
	
	

	
	

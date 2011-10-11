#!/usr/bin/env python

'''
Created on May 19, 2010

@author: torstees
'''


import os, time, struct, sys, MySQLdb, sqlite3
import bioloader, settings
from bioloader import Pathway
import biosettings

class ReactomeEntity:
	"""Generic reactome entity that can be represent either a reaction, event, pathway, etc"""
	def __init__(self, entityType, entityID, entityName, description, groupTypeID):
		"""Type is a string (BlackBoxEvent, Pathway, etc) and entityID is the DB_ID associated with the entity"""
		self.type 		= entityType	#BlackBoxEvent, Pathway, etc
		self.id			= entityID		#DB_ID - This is a unique integer over the entire DB
		self.name		= entityName	#Common name (i.e. REACT_78)
		self.desc		= description	#English summary of the entity
		self.children 	= set()			#Children - 
		self.genes		= set()			#Set of genes associated directly with this node
		self.groupID	= 0				#this will be set during commit
		self.parents	= set()
		self.childLinks		= []
		self.groupTypeID = groupTypeID	#DB Key for group Type (reaction/pathway)

	def AddChild(self, entity, table):
		"""Add a child to the local system. This will be used to parse out all genes that are "contained". For a reaction,
			this shouldn't be anything but genes"""
		if entity.name == self.name:
			print "Why are we adding ourself as a child? (", self.name, " ", self.type, " ", self.id, " : ", entity.name, " ", entity.type, " ", entity.id, ")"
		else:
			self.children.add(entity)
		self.childLinks.append((entity.name, table))

	def AddParent(self, entity):
		self.parents.add(entity)
		
	def AddGene(self, gene_id):
		"""Adds a gene to the gene_list associated directly with this entity."""
		print "Adding gene (%s) to group (%s)" % (gene_id, self.name)
		self.genes.add(gene_id)

	def GetGenes(self, visited, entityLookup):
		gene_list = set(self.genes)
		
		#if self.type in ["Reaction", "Pathway"]:
		for child in self.children:
			gene_list = gene_list.union(entityLookup[child.id].GetGenes(visited, entityLookup))
		return gene_list

	def GetSelectiveGenes(self, visited, entityLookup):
		gene_list = set(self.genes)
		
		for child in self.children:
			if (entityLookup[child.id].type not in ["Reaction","Pathway"]):
				gene_list = gene_list.union(entityLookup[child.id].GetGenes(visited, entityLookup))
		return gene_list

	def Commit(self, cursor, typeID, groupID, entityLookup):
		self.groupID = groupID
		doContinue = False

		#if self.type != "Pathway" and self.type != "Reaction":
		#	return groupID

		genes = self.GetSelectiveGenes(set(), entityLookup)
		try:
			cursor.execute("INSERT INTO groups VALUES (?,?,?,?)", (self.groupTypeID, groupID, self.name, "%s-%s" % (self.type, self.desc)))
			doContinue=True
		except sqlite3.Error, e:
				print e[0], typeID, self.groupID, self.name, self.desc

		if doContinue:
			if len(genes) == 0:
				print "No genes associated with group %s (%s) of type %s" % (self.name, groupID, self.type)
			for gene in genes:
				try:
					cursor.execute("INSERT INTO group_associations VALUES (?,?)", (self.groupID, gene))
					print "---", self.groupID, gene, self.name, self.type
				except sqlite3.Error, e:
						print e[0], typeID, self.groupID, gene
		if doContinue:
			return groupID +1
		else:
			return groupID

	def CommitRelationships(self, cursor, entityLookup):
		"""This has to be done after all commits are finished, since the group assignment is done at that point"""
		if self.type != "Pathway" and self.type != "Reaction":
			return
		
		if len(self.GetGenes(set(), entityLookup)) == 0:
			return
		
		if len(self.parents) == 0:
			try:					
				type = self.groupTypeID + 1
				if self.type=="Reaction":
					type+=1
				print "CommitRelationships: %s (%s) %s->%s" % (self.name, self.type, self.groupID, type)
				cursor.execute("INSERT INTO group_relationships VALUES (?,?,?,?)", (self.groupID, type, self.type, self.type))
				print "INSERT INTO group_relationships VALUES (%s,%s,%s,%s)" % (self.groupID, type, self.type, self.type)
			except sqlite3.Error, e:
				print e[0], self.groupID, self.groupID
		for parent in self.parents:
			try:
				print "CommitRelationships: %s (%s) %s->%s" % (self.name, self.type, self.groupID, entityLookup[parent.id].groupID)
				cursor.execute("INSERT INTO group_relationships VALUES (?,?,?,?)", (self.groupID, entityLookup[parent.id].groupID, self.type, entityLookup[parent.id].type))
			except sqlite3.Error, e:
				print e[0], self.groupID, entityLookup[parent.id].groupID

	def Print(self, indentionLevel, visited):
		"""Display contents. indentionLevel allows N tabs, visited allows us to avoid recursive loops""" 
		s=''
		for i in range(indentionLevel):
			s+="-"
#		if self.id in visited:
#			print "^-%s(*%s*, %s, %s [%s])" %(s, self.id, self.name, self.desc, self.type)
#			return
		visited.add(self.id)
		print "%s(*%s*, %s, %s [%s])" %(s, self.id, self.name, self.desc, self.type)
		print "Genes: ", self.genes
				
		for child in self.children:
			child.Print(indentionLevel+1, visited)
	

class ReactomeLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=9):
		bioloader.BioLoader.__init__(self, biosettings, id)
		biosettings.LoadAliases()
		self.entityLookup						= dict()

	def RefreshDatabase(self, force=True):
		localFilename = self.FetchViaHTTP("http://www.reactome.org/download/current/sql.gz")
		localFilename = self._ExtractGZ(localFilename)

		timestamp					= time.localtime(os.path.getmtime(localFilename))
		if force or self.CheckTimestampAgainstServer(timestamp, self.groupID):
			print "mysql -h rogue -u torstees -p'SMOJ2010' -e \"DROP DATABASE IF EXISTS reactome; CREATE DATABASE reactome;\""
			os.system("mysql -h rogue -u torstees -p'SMOJ2010' -e \"DROP DATABASE IF EXISTS reactome; CREATE DATABASE reactome;\"")
	
			print "cat %s | mysql -h rogue -u torstees -p'SMOJ2010' reactome" % (localFilename)
			os.system("cat %s | mysql -h rogue -u torstees -p'SMOJ2010' reactome" % (localFilename))

	def LoadSimpleBaseEntity(self, dbCursor, tableName, typeID):
		"""Loads entities which don't have matching DatabaseObject entries...."""
		sql = """SELECT DB_ID FROM reactome.%s""" % tableName
		dbCursor.execute(sql)
		rows = dbCursor.fetchall()
		if len(rows) == 0:
			print tableName, "still returns nothing....there is something wrong with our datasource"
			return
		for row in rows:
			if row[0] in self.entityLookup:
				#print "We have a duplicate in our entity IDs!", row[0]
				pass
			else:
				name = tableName + str(row[0])
				newEntity = ReactomeEntity(tableName, row[0], name, name, self.groupID)
				self.entityLookup[row[0]] = newEntity
		
	def LoadEWAS(self, cursor):
		"""Entity w Accession Seq table is different from most of the others, and has it's links built in (1 to 1 relationship, apparently)"""
		cursor.execute("SELECT DB_ID, referenceEntity FROM reactome.EntityWithAccessionedSequence")
		rows = cursor.fetchall()
		associationsMade = 0
		for row in rows:
			dbID = row[0]
			entityID = row[1]
			if entityID in self.entityLookup:
				name = "EWAS." + str(dbID)
				self.entityLookup[dbID] = ReactomeEntity("EntityWithAccessionSequence", dbID, name, name, self.groupID)
				self.entityLookup[dbID].AddChild(self.entityLookup[entityID], "EWAS")
				self.entityLookup[entityID].AddParent(self.entityLookup[dbID])
				associationsMade+=1
		report = ' ' * (10-len(str(associationsMade))) + str(associationsMade) + " out of "  + ' ' * (10-len(str(len(rows)))) + str(len(rows))
		print "%s associations made -- EWAS " %(report)

	def LoadCatalystActivities(self, cursor):
		"""Catalyst Activity table is different from most of the others, and has it's links built in (1 to 1 relationship, apparently)"""
		cursor.execute("SELECT DB_ID, physicalEntity FROM reactome.CatalystActivity")
		rows = cursor.fetchall()
		associationsMade = 0
		for row in rows:
			dbID = row[0]
			entityID = row[1]
			if entityID in self.entityLookup:
				name = "Catalyst Activity." + str(dbID)
				self.entityLookup[dbID] = ReactomeEntity("CatalystActivity", dbID, name, name, self.groupID)
				self.entityLookup[dbID].AddChild(self.entityLookup[entityID], "CatalystActivity")
				self.entityLookup[entityID].AddParent(self.entityLookup[dbID])
				associationsMade+=1
		report = ' ' * (10-len(str(associationsMade))) + str(associationsMade) + " out of "  + ' ' * (10-len(str(len(rows)))) + str(len(rows))
		print "%s associations made -- CatalystActivity " %(report)

	def LoadEWAS(self, cursor):
		"""Entity w Accession Seq table is different from most of the others, and has it's links built in (1 to 1 relationship, apparently)"""
		cursor.execute("SELECT DB_ID, referenceEntity FROM reactome.EntityWithAccessionedSequence")
		rows = cursor.fetchall()
		associationsMade = 0
		for row in rows:
			dbID = row[0]
			entityID = row[1]
			if entityID in self.entityLookup:
				name = "EWAS." + str(dbID)
				self.entityLookup[dbID] = ReactomeEntity("EntityWithAccessionSequence", dbID, name, name, self.groupID)
				self.entityLookup[dbID].AddChild(self.entityLookup[entityID], "EWAS")
				self.entityLookup[entityID].AddParent(self.entityLookup[dbID])
				associationsMade+=1
		report = ' ' * (10-len(str(associationsMade))) + str(associationsMade) + " out of "  + ' ' * (10-len(str(len(rows)))) + str(len(rows))
		print "%s associations made -- EWAS " %(report)

	def LoadCatalystActivities(self, cursor):
		"""Catalyst Activity table is different from most of the others, and has it's links built in (1 to 1 relationship, apparently)"""
		cursor.execute("SELECT DB_ID, physicalEntity FROM reactome.CatalystActivity")
		rows = cursor.fetchall()
		associationsMade = 0
		for row in rows:
			dbID = row[0]
			entityID = row[1]
			if entityID in self.entityLookup:
				name = "Catalyst Activity." + str(dbID)
				self.entityLookup[dbID] = ReactomeEntity("CatalystActivity", dbID, name, name, self.groupID)
				self.entityLookup[dbID].AddChild(self.entityLookup[entityID], "CatalystActivity")
				self.entityLookup[entityID].AddParent(self.entityLookup[dbID])
				associationsMade+=1
		report = ' ' * (10-len(str(associationsMade))) + str(associationsMade) + " out of "  + ' ' * (10-len(str(len(rows)))) + str(len(rows))
		print "%s associations made -- CatalystActivity " %(report)
		
	def LoadAssociation(self, dbCursor, tableName):
		sql = "SELECT * FROM reactome.%s" % tableName
		dbCursor.execute(sql)
		rows = dbCursor.fetchall()

		associationsMade = 0
		for row in rows:
				dbID = int(row[0])
				if dbID in self.entityLookup:
#					if row[2] in self.dbIDGenes:
#						self.entityLookup[dbID].AddGene(self.dbIDGenes[row[2]])
#						print "Gene Association ", dbID, " -> ", self.dbIDGenes[row[2]]
#					else:
					if row[2] in self.entityLookup:
						self.entityLookup[dbID].AddChild(self.entityLookup[row[2]], tableName)
						self.entityLookup[row[2]].AddParent(self.entityLookup[dbID])
						associationsMade+=1
		report = ' ' * (10-len(str(associationsMade))) + str(associationsMade) + " out of "  + ' ' * (10-len(str(len(rows)))) + str(len(rows))
		print "%s associations made -- %s " %(report, tableName)		

	def LoadReferencePeptideToGene(self, cursor):
		sql = """SELECT DISTINCT a.DB_ID, c.identifier , d.name
				FROM 
					reactome.EntityWithAccessionedSequence a 
					INNER JOIN reactome.ReferenceGeneProduct_2_referenceGene b ON a.referenceEntity=b.DB_ID 
					INNER JOIN reactome.ReferenceEntity c ON b.referenceGene=c.DB_ID
					INNER JOIN reactome.ReferenceDatabase_2_name d ON c.referenceDatabase=d.DB_ID
				WHERE d.name='Entrez Gene'"""

		# 
		cursor.execute(sql)
		rows=cursor.fetchall()
		rowcount = 0
		for row in rows:
			rowcount+=1
			dbID = row[0]
			entrezID = int(row[1])
			superDebug = False
			if dbID in self.entityLookup:
				if entrezID in self.biosettings.regions.genes:
					gene							= self.biosettings.regions.genes[entrezID]
					print "This is the gene we found: ", gene.Print(sys.stderr)
					print "Not skipping geneID: ", entrezID, "-->", gene.geneID, " -- ", row
					self.entityLookup[dbID].AddGene(gene.geneID)
				else:
					print "Skipping geneID: ", entrezID, " (", len(self.biosettings.regions.genes), ") genes"
			else:
				if superDebug:
					print "Can't find, ", dbID
		
	def LoadBaseEntity(self, dbCursor, tableName):
		"""Loads entities and populates the entity structure accordingly"""
		sql = """SELECT a.DB_ID, b._displayName AS description,
						c.identifier AS name, b._class AS type
					FROM reactome.%s a
						INNER JOIN reactome.DatabaseObject b ON a.DB_ID=b.DB_ID
						INNER JOIN reactome.StableIdentifier c ON b.stableIdentifier=c.DB_ID""" % (tableName)
		dbCursor.execute(sql)
		rows = dbCursor.fetchall()

		if len(rows) == 0:
			print tableName, "...............Returned an empty set. Trying a simpler query"
			self.LoadSimpleBaseEntity(dbCursor, tableName)
		for row in rows:
			
			if row[0] in self.entityLookup:
				#print "We have a duplicate in our entity IDs!", row[0]
				pass
			else:
				newEntity = ReactomeEntity(row[3], row[0], row[2], row[1], self.groupID)
				self.entityLookup[row[0]] = newEntity



	def Load(self, sourceDB):
		"""Extract data from the database, sourceDB"""
		cwd 					= os.getcwd()
		os.system("mkdir -p reactome")
		os.chdir("reactome")

		c=sourceDB.ensembl.cursor()
		self.LoadBaseEntity(c, "Pathway")
		self.LoadBaseEntity(c, "BlackBoxEvent")
		self.LoadBaseEntity(c, "ReactionlikeEvent")
		self.LoadBaseEntity(c, "EntityWithAccessionedSequence")
		#self.LoadBaseEntity(c, "ReferenceGeneProduct")
		self.LoadBaseEntity(c, "Complex")
		self.LoadBaseEntity(c, "DefinedSet")
		self.LoadCatalystActivities(c)
		self.LoadEWAS(c)
		self.LoadAssociation(c, "Pathway_2_hasEvent")
		self.LoadAssociation(c, "BlackBoxEvent_2_hasEvent")
		self.LoadAssociation(c, "ReactionlikeEvent_2_hasMember")
		self.LoadAssociation(c, "Complex_2_hasComponent")
		self.LoadAssociation(c, "EntitySet_2_hasMember")
		self.LoadAssociation(c, "CatalystActivity_2_activeUnit")
		self.LoadAssociation(c, "ReactionlikeEvent_2_input")
		self.LoadAssociation(c, "ReactionlikeEvent_2_output")
		self.LoadAssociation(c, "ReactionlikeEvent_2_catalystActivity")
		self.LoadAssociation(c, "ReactionlikeEvent_2_requiredInputComponent")
		self.LoadAssociation(c, "ReactionlikeEvent_2_hasMember")
		#self.LoadAssociation(c, "ReactionlikeEvent_2_")
		self.LoadReferencePeptideToGene(c)
		os.chdir(cwd)

	def Commit(self):
		self.biosettings.PurgeGroupData(self.groupID)
		timestamp					= time.localtime(time.time())
		self.biosettings.CommitGroup(self.groupID, 1, "Reactome", "Reactome Groups", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		self.biosettings.CommitPathway(self.groupID, self.groupID+1, "Pathway", "Reactome Pathway")
		self.biosettings.CommitPathway(self.groupID, self.groupID+2, "Reaction", "Reactome Reaction")
		self.biosettings.RelatePathways(self.groupID, self.groupID+1, "is a", "member")
		self.biosettings.RelatePathways(self.groupID, self.groupID+2, "is a", "member")
		dbCursor 					= self.biosettings.GetCursor()
		for entity in self.entityLookup:
			rEntity = self.entityLookup[entity]
			groupID = rEntity.Commit(dbCursor, self.groupID, self.biosettings.NextID(), self.entityLookup)
		
		for entity in self.entityLookup:
			self.entityLookup[entity].CommitRelationships(dbCursor, self.entityLookup)
		return groupID
	
if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
		
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= ReactomeLoader(bioDB)
	ensembl					= load_ensembl.EnsemblLoader(bioDB, 2)
	ensembl.ConnectToEnsemblDB()
	#loader.RefreshDatabase()
	#ensembl					= MySQLdb.connect ("rogue", "torstees", "SMOJ2010", db="reactome")
	loader.Load(ensembl)
	loader.Commit()
	bioDB.Commit()
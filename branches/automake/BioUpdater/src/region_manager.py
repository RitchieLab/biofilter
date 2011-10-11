'''
Created on May 18, 2010

@author: torstees
'''

import os, time, struct, sys
import bioloader, settings, sqlite3
import MySQLdb

aliasTypeIDs			= { 1:"Ensembl", 13:"Entrez ID", 1300:"Entrez Gene", 1301:"Entrez History", 2000:"Uniprot", 2200:"Uniprot/SWISSPROT", 2:"Protein Accession ID", 3:"mRNA Accession ID",  11:"NCBI Ensembl" }

class GenomicRegion:
	nextID						= 2000000000
	def NextID(self):
		nextid					= GenomicRegion.nextID
		GenomicRegion.nextID 	+= 1
		return nextid
	
	def Reset(self):
		GenomicRegion.nextID	= 100

	def AddAlias(self, typeID, alias):
		if typeID == 1:
			self.ensemblID = alias
		elif typeID == 13:
			self.geneID		= alias
		

	def __init__(self, chrom):
		self.primaryName		= ''
		self.geneID				= 0
		self.chrom				= chrom
		self.start				= 0
		self.stop				= 0
		self.desc				= ""
		self.doCommit			= False					# This basically is used to avoid recommiting the data when we just loaded it from the database
		self.hgnc				= 0
		self.ensemblID			= ""
		
	def InitViaEnsembl(self, geneID, label, ensemblID, start, stop, description):
		self.geneID				= geneID
		self.ensemblID			= ensemblID
		self.start				= start
		self.stop				= stop
		self.primaryName		= label
		self.description		= description
		
		
	def InitStub(self, geneID, accID, proteinAcc):
		self.geneID				= geneID
		self.description		= "%s %s" % (accID, proteinAcc)
		

	def InitViaEntrez(self, geneID, start, stop):
		self.geneID				= geneID
		self.start				= start
		self.stop				= stop
		self.primaryName 		= "%s" % geneID
		
	def UpdateFromEntrez(self, primaryName, ensemblID, hgnc, chromosome, mapPosition, desc):
		self.ensemblID			= ensemblID
		self.chrom				= chromosome
		self.desc				= desc
		self.primaryName		= primaryName
		self.hgnc				= hgnc

	def Print(self, file):
		print>>file, "Gene(%s): %s (%s) %s" % (self.geneID, self.primaryName, self.ensemblID, self.desc)

	def Commit(self, cursor, aliases):
		#if self.primaryName in aliases[1300] and len(aliases[1300][self.primaryName]) > 1:
		#	print "Resetting primary name, due to bad stuff: %s" % (self.primaryName)
		#	self.primaryName = self.geneID
		try:
			cursor.execute("INSERT INTO regions VALUES (?, ?, ?, ?)", (self.geneID, self.primaryName, self.chrom, self.desc))
			cursor.execute("INSERT INTO region_bounds VALUES (?,0,?,?)", (self.geneID, self.start, self.stop))
		except sqlite3.Error, e:
			print "Unable to insert %s:%s (%s) into Regions table -- %s" % (self.geneID, self.primaryName, aliases[1300][self.primaryName], e.args[0])

		try:
			cursor.execute("INSERT INTO region_alias (region_alias_type_id, alias, gene_id, gene_count) VALUES (?, ?, ?, ?)", (1300, self.primaryName, self.geneID, 1))
		except:
			print "Unable to add alias %s to gene, %s : %s"% (self.primaryName, self.geneID, e.args[0])

class RegionManager:
	def __init__(self):
		self.genes				= dict()	#entrez -> genomic region
		self.aliases			= dict()	#alias -> [geneID] This allows us to record how many aliases have been found foa  given object
		self.InitAliases()
		self.missingEnsemblIDs	= set()		#set of ensembl IDs that were requested but not found
		self.doCommit 			= True
		self.primaryNames		= dict()	#primaryName -> geneID
		self.historic			= dict()
		self.stubs				= dict()
	
	def AddEntrezHistory(self, oldID, newID):
		"""tracking historic changes. "-" as newID indicates that the alias is dead"""
		oldID					= int(oldID)
		if int(oldID) in self.historic:
			print "Duplicate historic SNP: %s -> %s \t\t%s -> %s" % (oldID, self.historic[int(oldID)], oldID, newID)
		self.historic[int(oldID)] = newID
		
	def InitAliases(self):
		"""Just a way to initialize the aliases sets"""
		for type in aliasTypeIDs:
			if type in self.aliases:
				print "Dropping Alaises: %s"% (len(self.aliases[type]))
			self.aliases[type] = dict()
	def AddEntrezStub(self, geneID, accID, proteinAcc):
		if accID[:2] == "NT":
			gene								= GenomicRegion("??")
			gene.InitStub(geneID, accID, proteinAcc)
			self.stubs[geneID]					= gene

	def AddEntrezGene(self, entrezID, accID, start, stop, strand, proteinAcc, mRNAaccID):
		if accID[:2] == "NC":
			gene								= GenomicRegion("??")
			gene.InitViaEntrez(entrezID, start, stop)
			self.genes[entrezID] 				= gene
			self.AddAlias(13,"%s" % entrezID, gene.geneID)
			self.AddAlias(2, "%s" % proteinAcc, gene.geneID)
			self.AddAlias(3, "%s"% mRNAaccID, gene.geneID)
			
		elif accID[:2] == "NT":
			#print "Trying to add NT"
			if entrezID not in self.genes:
				print>>sys.stderr, "We have an NC type ID without a preceeding NT: Entrez: %s    Acc ID: %s" % (entrezID, accID)
		else:
			print "What? No way to id this one! %s" % (accID)

	def PrintStubReport(self):
		for item in self.stubs:
			print "%s - %s " % (item.geneID, item.desc)

	def UpdateEntrezGene(self, entrezID, primaryName, aliasList, ensemblID, hgnc, chromosome, mapPosition, desc):
		if entrezID in self.genes:
			self.genes[entrezID].UpdateFromEntrez(primaryName, ensemblID, hgnc, chromosome, mapPosition, desc)
			if primaryName not in self.primaryNames:
				self.primaryNames[primaryName]	= set()
			
			#print "%s:%s:%s:%s:%s" % (chromosome, entrezID, primaryName, ensemblID, desc)
			self.primaryNames[primaryName].add(int(entrezID))
			self.AddAlias(11, ensemblID, entrezID)
			self.AddAlias(1300, primaryName, entrezID)
			for alias in aliasList:
				if not alias.isdigit():
					self.AddAlias(1300, alias, entrezID)
		else:
			return False
			print "Skipping UpdateEntrezGene due to absence in gene list: %s,%s,%s,%s" % (entrezID, primaryName, aliasList, ensemblID)
		return True
	def AddPseudoRegion(self, chrom, label, ensemblID, start, stop, description):
		gene							= GenomicRegion(chrom)
		geneID							= gene.NextID()
		#These are coming from Ensembl, and we don't want primary name collisions to occur because of
		#ensembl...they are too generous with their naming scheme...so, if it's in the primary name
		#then we will drop that gene name altogether
		if label in self.primaryNames:
			label						= "%s" % geneID
		gene.InitViaEnsembl(geneID, label, ensemblID, start, stop, description)
		if label not in self.primaryNames:
			self.primaryNames[label]	= set()
		self.primaryNames[label].add(geneID)
		
		self.genes[geneID] 			= gene
			
				#print "------Added new region: ", chrom, gID, label, ensemblID
			#else:
				#print "------Updated Region:   ", chrom, gID, label, ensemblID
			#print "%s:%s:%s:%s:%s:%s:%s" % (chrom, gID, label, ensemblID, start, stop, description)
		self.AddAlias(1, ensemblID, geneID)
		return geneID
		
	def AddRegion(self, chrom, gID, label, ensemblID, start, stop, description):
		geneID 								= int(gID)
		rValue = False
		if geneID > 0:
			if geneID not in self.genes:
				rValue							= True

				#These are coming from Ensembl, and we don't want primary name collisions to occur because of
				#ensembl...they are too generous with their naming scheme...so, if it's in the primary name
				#then we will drop that gene name altogether
				if label in self.primaryNames:
					label						= "%s" % geneID
				if label not in self.primaryNames:
					self.primaryNames[label]	= set()
				self.primaryNames[label].add(geneID)

				
				gene							= GenomicRegion(chrom)
				gene.InitViaEnsembl(geneID, label, ensemblID, start, stop, description)
				self.genes[geneID] 			= gene
				
				
				#print "------Added new region: ", chrom, gID, label, ensemblID
			#else:
				#print "------Updated Region:   ", chrom, gID, label, ensemblID
			#print "%s:%s:%s:%s:%s:%s:%s" % (chrom, gID, label, ensemblID, start, stop, description)
			self.AddAlias(1, ensemblID, geneID)
			self.AddAlias(1300, label, geneID)
			self.AddAlias(13, "%s" % geneID, geneID)
		else:
			print "------Refusing to add region: ", chrom, gID, label, ensemblID
			#print "%s, %s, %s, %s, %s, %s (%s)" % (chrom, gene.geneID, ensemblID, start, stop, description, len(self.genes))
		return rValue

	def AddEnsemblID(self, geneID, ensemblID):
		if geneID in self.genes and len(ensemblID.strip()) > 0:
			self.AddAlias(1, ensemblID, geneID)
			self.genes[geneID].ensemblID = ensemblID

	def AddAlias(self, aliasTypeID, alias, geneID):
		alias = alias.strip()
		if alias == "-" or len(alias) == 0:
			return
		if len(alias) > 0 and int(geneID) in self.genes:
			self.genes[int(geneID)].AddAlias(aliasTypeID, alias)
			if alias not in self.aliases[aliasTypeID]:
				self.aliases[aliasTypeID][alias] = set()
			self.aliases[aliasTypeID][alias].add(int(geneID))
		else:
			print "Skipping alias: %s -> %s" % (geneID, alias)

	def AddAliasToEnsemblID(self, alias, ensemblID, type):
		if ensemblID in self.genes:
			geneID = self.genes[ensemblID].geneID
			self.AddAlias(type, alias, geneID)
		else:
			self.missingEnsemblIDs.add((ensemblID, alias))
	
	def AliasToGeneID(self, aliases, aliasTypeID = 0):
		if aliasTypeID > 0:
			for alias in aliases:
				if alias in self.aliases[aliasTypeID]:
					print "%s --> %s" % (alias, self.aliases[aliasTypeID][alias])
					return list(self.aliases[aliasTypeID][alias])
				else:
					#print "Trying to find alias: %s in set of %s aliases." % (",".join(aliases), len(self.aliases[aliasTypeID]))
					return []
		for type in self.aliases:
			for alias in aliases:
				if alias in self.aliases[type]:
					print "%s - %s ==> %s" % (aliases, alias, self.aliases[type][alias])
					return list(self.aliases[type][alias])
		return []

	
	def LoadGenesOnChromosome(self, chromosome, c):
		csel = "c"+str(chromosome)+"_%"
		c.execute("""
SELECT DISTINCT  
    e.dbprimary_acc,
    c.name AS chromosome, 
    d.seq_region_start,
    d.seq_region_end,
    b.stable_id,
    e.display_label,
   	e.external_db_id,
	e.description

FROM 
    ensembl.gene AS a
    INNER JOIN ensembl.gene_stable_id AS b ON (a.gene_id=b.gene_id)
    INNER JOIN ensembl.seq_region c ON a.seq_region_id = c.seq_region_id
    INNER JOIN ensembl.transcript d ON a.gene_id=d.gene_id
    INNER JOIN ensembl.object_xref x ON d.canonical_translation_id=x.ensembl_id
    INNER JOIN ensembl.xref e ON x.xref_id=e.xref_id

WHERE 
    x.ensembl_object_type='Translation'
    AND c.name = %s
    AND e.external_db_id IN (2000,2200,1300)
    AND coord_system_id=%s
ORDER BY e.external_db_id, e.dbprimary_acc""", (chromosome, 2))


		
		rows = c.fetchall()
		geneCount					= 0
		stubCount					= 0
		ensemblUpdate				= 0
		totalStubs					= len(self.stubs)
		totalGenes					= len(self.genes)
		totalSkipped				= 0
		totalUpdates				= 0
		for row in rows:
			#print "|".join(["%s" % r for r in row])
			#print "--> %s" % ("|".join(["%s" % r for r in row]))
			name					= row[0]
			chr						= row[1]
			start					= int(row[2])
			end						= int(row[3])
			stableID				= row[4]
			desc					= row[7]
			label					= row[5]
			dbID					= int(row[6])
			
			keywords				= []
			if desc:
				keywords				= [w.lower() for w in desc.split()]
			
			ensGenes				= self.AliasToGeneID([stableID])
			
			if dbID != 1300 or "similar" not in keywords:
				if len(ensGenes) == 0:
					id = 0
					if dbID == 1300:
						id = int(name)
						if name in self.genes:
							ensemblUpdate +=1 
						else:
							if self.AddRegion(chr, name, label, stableID, start, end, desc) :
								if name in self.stubs:
									stubCount+=1
									del self.stubs[name]
								else:
									geneCount += 1
							else:
								totalSkipped+=1
					else:
						id = self.AddPseudoRegion(chr, label, stableID, start, end, desc)
						geneCount += 1
						self.AddAlias(dbID, name, id)
					self.AddAlias(1, stableID, id)
				else:
					geneIDs			= self.AliasToGeneID([stableID])
					for id in geneIDs:
						self.AddAlias(dbID, name, id)

				
	
		print "Chromosome :  %s" % (chromosome)
		print "\tNew Genes: %s (out of %s)" % (geneCount, totalGenes)
		print "\tStubs IDd: %s (out of %s)" % (stubCount, totalStubs)
		print "\tEnsembl  : %s" % (ensemblUpdate)
		print "\tUpdated  : %s" % (totalUpdates)
		print "\tSkipped  : %s" % (totalSkipped)

	def LoadGenesOnChromosome2(self, chromosome, c):
		csel = "c"+str(chromosome)+"_%"
		c.execute("""
SELECT DISTINCT  
    GROUP_CONCAT(DISTINCT IF(e.external_db_id=1300,e.dbprimary_acc,'') SEPARATOR ' ') AS entrez_id,
    c.name AS chromosome, 
    d.seq_region_start,
    d.seq_region_end,
    b.stable_id,
    e.description,
    e.display_label,
	GROUP_CONCAT(DISTINCT IF(e.external_db_id=2000,e.dbprimary_acc,'') SEPARATOR ' ') AS trembl_id,
	GROUP_CONCAT(DISTINCT IF(e.external_db_id=2200,e.dbprimary_acc,'') SEPARATOR ' ') AS swissprot_id
FROM 
    ensembl.gene AS a
    INNER JOIN ensembl.gene_stable_id AS b ON (a.gene_id=b.gene_id)
    INNER JOIN ensembl.seq_region c ON a.seq_region_id = c.seq_region_id
    INNER JOIN ensembl.transcript d ON a.gene_id=d.gene_id
    INNER JOIN ensembl.object_xref x ON d.canonical_translation_id=x.ensembl_id
    INNER JOIN ensembl.xref e ON x.xref_id=e.xref_id

WHERE 
    x.ensembl_object_type='Translation'
    AND c.name = %s
    AND e.external_db_id IN (2000,2200,1300)
    AND coord_system_id=%s
GROUP BY b.gene_id""", (chromosome, 2))


		
		rows = c.fetchall()
		geneCount					= 0
		stubCount					= 0
		ensemblUpdate				= 0
		totalStubs					= len(self.stubs)
		totalGenes					= len(self.genes)
		totalSkipped				= 0
		totalUpdates				= 0
		for row in rows:
			#print "--> %s" % ("|".join(["%s" % r for r in row]))
			entrezIDs				= [int(r) for r in row[0].split()]
			uniprotIDs				= [r.strip() for r in row[7].split()]
			swissprotIDs			= [r.strip() for r in row[8].split()]
			chr						= row[1]
			start					= int(row[2])
			end						= int(row[3])
			stableID				= row[4]
			desc					= row[5]
			label					= row[6]
			
			keywords				= []
			if desc:
				keywords				= desc.split()
			
			#Lets ignore the things that are "Kind of Like whatever"
			if len(keywords) > 0 and keywords[0].lower() == "similar":
				totalSkipped += 1
				stableID = "-"
				#print "Skipping ensembl ID due to description contents: %s" % (",".join(["%s" % r for r in row]))
			for entrez in entrezIDs:
				if entrez in self.stubs:
					if self.AddRegion(chr, entrez, label, stableID, start, end, desc):
						stubCount += 1
					else:
						totalSkipped += 1
					del self.stubs[entrez]
				else:
					if entrez in self.genes:
						ensemblUpdate += 1
						self.AddAlias(1, stableID, entrez)
					else:
						if self.AddRegion(chr, entrez, label, stableID, start, end, desc):
							geneCount += 1
						else:
							totalUpdates += 1
				if len(swissprotIDs) > 20 or len(uniprotIDs) > 20:
					print "%s\t%s\t%s\t%s" % (",".join(["%s" % e for e in entrezIDs]), stableID,",".join(uniprotIDs), ",".join(swissprotIDs))
				for a in swissprotIDs:
					self.AddAlias(2200, a, entrez)
				for u in uniprotIDs:
					self.AddAlias(2000, u, entrez)
			if len(entrezIDs) == 0:
				id = self.AddPseudoRegion(chr, label, stableID, start, end, desc)
				for a in swissprotIDs:
					self.AddAlias(2200, a, id)
				for u in uniprotIDs:
					self.AddAlias(2000, u, id)
				
	
		print "Chromosome :  %s" % (chromosome)
		print "\tNew Genes: %s (out of %s)" % (geneCount, totalGenes)
		print "\tStubs IDd: %s (out of %s)" % (stubCount, totalStubs)
		print "\tEnsembl  : %s" % (ensemblUpdate)
		print "\tUpdated  : %s" % (totalUpdates)
		print "\tSkipped  : %s" % (totalSkipped)
	def LoadAliasesFromEnsembl(self, ensembl):
		self.InitAliases()
		cur = ensembl.cursor()
		cur.execute("SET GLOBAL group_concat_max_len=4096")
		for aliasTypeID in aliasTypeIDs:
			if aliasTypeID != 1:
				cur.execute("""
					SELECT DISTINCT  d.gene_id, e.stable_id, a.dbprimary_acc, a.display_label, a.description
					FROM (SELECT dbprimary_acc, display_label, description, ensembl.xref.xref_id 
								FROM ensembl.xref
								WHERE external_db_id=%s) AS a
					NATURAL JOIN ensembl.object_xref AS b
					INNER JOIN ensembl.translation AS c ON b.ensembl_id=c.translation_id
					INNER JOIN ensembl.transcript AS d ON c.transcript_id=d.transcript_id
					INNER JOIN ensembl.gene_stable_id as e ON e.gene_id=d.gene_id""", (aliasTypeID, ))
				for row in cur.fetchall():
					self.AddAliasToEnsemblID(row[2], row[1], aliasTypeID)
					if aliasTypeID == 2000:
						primaryAcc	= row[3][:len(row[3])-6]
						self.AddAliasToEnsemblID(primaryAcc, row[1], aliasTypeID)
					elif aliasTypeID != 2200:
						self.AddAliasToEnsemblID(row[3], row[1], aliasTypeID)
		print "# Total number of ensembl IDs recorded: %s" % (len(self.genes))
		print "# Ensembl IDs unable to be matched:     %s" % (len(self.missingEnsemblIDs))
		
		if len(self.missingEnsemblIDs) > 0:
			file = open("missing-ensemblIDs.txt", "w")
			print>>file, "Missing Ensembl IDs are those referenced in some other place (generally in the alias lookup) but weren't found in our set of genes."
			print>>file, "From what I've seen, these match weird things like cloned genes or gene families, which don't get picked up on the queries."
			print>>file, "These are recorded here in case we ever need to determine exactly how much data we aren't keeping for one reason or another"
			for id in self.missingEnsemblIDs:
				print>>file, id[0], id[1]
			file.close()
		self.doCommit							= True
		
	def LoadRegionData(self, db):
		if len(self.genes) == 0:
			c										= db.cursor()
			c.execute("SELECT * FROM regions")
			for row in c.fetchall():
				#print row
				gene								= GenomicRegion(row[2])
				gene.id								= row[0]
				gene.primaryName					= row[1]
				gene.desc							= row[3]
				gene.commit							= False
				self.genes[gene.id]					= gene
		self.LoadAliasesFromDB(db)

	def LoadAliasesFromDB(self, db):
		if len(self.aliases[1300]) == 0:
			print "Attempting to load aliases from sqlite database"
			cursor									= db.cursor()
			
			cursor.execute("SELECT alias, gene_id, region_alias_type_id FROM region_alias")
			for row in cursor.fetchall():
				#print row
				self.AddAlias(row[2], row[0], row[1])
			self.doCommit							= False
			
			cursor.execute("SELECT MAX(gene_id) FROM regions")
			GenomicRegion.nextID = 1
			nextID = cursor.fetchone()[0]
			if nextID:
				GenomicRegion.nextID = nextID + 1
			for type in self.aliases:
				print "%s aliases loaded from DB %s" % (len(self.aliases[type]), type)
			
		else:
			print "Aliases already loaded"
		
		cursor										= db.cursor()
			
	def RunTest(self, geneName, aliasType):
		geneIDs										= self.AliasToGeneID([geneName])
		if len(geneIDs) == 0:
			print "Unable to match %s" % (geneName)
			for alias in self.aliases[aliasType]:
				if alias[:2] == geneName[:2]:
					print "%s : %s" % (alias, self.aliases[aliasType])
			
	def Commit(self, dest):
		if self.doCommit:
			cur = dest.cursor()
			
			cur.execute("DELETE FROM region_alias_type")
			cur.execute("DELETE FROM region_alias")
			cur.execute("DELETE FROM regions")
			cur.execute("DELETE FROM region_bounds")
			cur.execute("DELETE FROM populations")
			
			print "Commiting data to database"
			#alias types
			cur.execute("INSERT INTO populations(population_id, population_label, pop_ld_comment, pop_description) VALUES (0, 'NO-LD', 'No LD', 'Gene Boundaries represent those described by entrez gene')")
			
			print "%s Alias Types" % (len(aliasTypeIDs))
			for aliasType in aliasTypeIDs:
				cur.execute("INSERT INTO region_alias_type(region_alias_type_id, region_alias_type_desc) VALUES (?, ?)", (aliasType, aliasTypeIDs[aliasType]))
			
			redirectedGeneCount				= 0
			print "%s Genes" % (len(self.genes))
			#This takes care of regions and region_bounds
			for gene in self.genes:
				gene = int(gene)
				if gene not in self.historic:
					primaryName				= self.genes[gene].primaryName
					if primaryName in self.primaryNames and len(self.primaryNames[primaryName]) > 1:
						if self.genes[gene].hgnc == 0:
							print "Resetting primary name (%s : %s) due to collision: Other values (%s)" % (primaryName, gene, self.primaryNames[primaryName])
							self.genes[gene].primaryName = self.genes[gene].geneID
					self.genes[gene].Commit(dest, self.aliases)
				else:
					redirectedGeneCount += 1
			print "Skipping commit of %s genes due to it's presence as a redirected gene ID" % (redirectedGeneCount)

			for oldID in self.historic:
				newID = self.historic[oldID]
				if newID != "-":
					#print "History: %s -> %s" % (oldID, newID)
					self.AddAlias(1301, "%s" % oldID, "%s" % (int(newID)))
					
			#And now, for the aliases
			for aType in self.aliases:
				aliasType = aType
				aliasesCommited				= 0
				
				primaryOverlaps				= 0
				notsingularEnsembl			= 0
				for alias in self.aliases[aType]:
					if alias not in self.primaryNames:
						genes = self.aliases[aType][alias]
						
						#if aliasType != 1 or len(genes) == 1:
						#print "%s - %s" % (len(genes), alias)
						geneCount = len(genes - set(self.historic.keys()))

						#we don't want to rely on entrez for ensembl IDs, if they are ambiguous
						if aType != 11 or len(genes) == 1:
							if aType == 11:
								aliasType			= 1
							for gid in genes:
								geneID			= gid
								#print "%s : %s -> %s %s" % (aliasType, alias, geneID, geneCount)
								try:
									if geneID in self.historic:
										geneID = self.historic[geneID]
									if geneID=="-":
										print "WTF? ", aliasType, alias, geneID, geneCount, gid
									elif geneCount == 0:
										print "ACK! ", aliasType, alias, geneID, geneCount, gid
									
									#In some cases, the primary name is the same as other aliases...we can't afford to have that sort of indirection, since we are trying to identify genes based 
									#on their name
									if alias not in self.primaryNames or self.primaryNames[alias] == geneID:
										cur.execute("INSERT INTO region_alias (region_alias_type_id, alias, gene_id, gene_count) VALUES (?, ?, ?, ?)", (aliasType, alias, geneID, geneCount))
										#if alias in self.primaryNames:
										#	print "+Inserting alias: %s %s (%s)" % (alias, geneID, self.primaryNames[alias].geneID)
										#else:
										#	print "+Inserting alias: %s %s" % (alias, geneID)
										aliasesCommited += 1
									#else:
									#	print "-Skipping insertion of alias, due to alias/primary collision: %s %s (%s)" % (alias, geneID, self.primaryNames[alias].geneID)
								except sqlite3.Error, e:
									if gid != geneID:
										print "Failed attempt to insert an alias (%s, %s) : %s\n -- %s" % (alias, geneID, e[0], " ".join(["%s" % g for g in genes]))
								#pass

						#else:
						#	notsingularEnsembl += 1
					else:
						primaryOverlaps += 1
				print "Comitting Aliases: %s : %s (%s, %s) out of %s" % (aliasType, aliasesCommited, notsingularEnsembl, primaryOverlaps, len(self.aliases[aliasType]))

			dest.commit()
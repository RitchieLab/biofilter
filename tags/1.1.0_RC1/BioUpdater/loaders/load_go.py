#!/usr/bin/env python

'''
Created on Jun 7, 2010

@author: torstees
'''
import os, time, struct, sys
from util import bioloader, settings, biosettings
#import bioloader.Pathway as Pathway


class OntologyTerm:
	"""Used to parse the ontology files. There will be all possible terms loaded, even though many might not apply to humans"""
	def __init__(self):
		self.term_id = ''	#The database we'll write our contents to
		self.parents = dict()#goID->(function description, relationship)
		self.namespace = ''	#GO Namespace ... not sure if we care about this
		self.name = ''	#GO name
		self.description = ''	#Long description of this entity

	def ParseLine(self, line):
		"""Parses a single line, extracting necessary details-returns false if the line is "[Term]" """
		# id:			-> This is the local goID
		# name:			-> name
		# namespace:	-> namespace 
		# def:			-> description
		# is_a			-> relationship
		# relationship: part_of|regulates|positively_regulates|negatively_regulates		-> relationship
		#print line
		words = line.rstrip().split(':', 1)
		if words[0] == "id":
			self.term_id 	= words[1].strip()
		elif words[0] == "name":
			self.name		= words[1].strip()
		elif words[0] == "namespace":
			self.namespace 	= words[1].strip()
		elif words[0] == "def":
			self.description = words[1].strip()
		elif words[0] == "is_a":
			elements = words[1].lstrip().split("!", 1)
			self.parents[elements[0].strip()] = (words[0].strip(), elements[1].strip())
			#print "%s IS_A %s = (%s, %s)" % (self.term_id, elements[0], words[0],  elements[1].strip())
		elif words[0] == "relationship":
			elements = words[1].lstrip().split(" ", 3)
			self.parents[elements[1].strip()] = (elements[0].strip(), elements[3].strip())
			#print "%s RELATIONSHIP %s = %s,%s" % (self.term_id, elements[1], elements[0], elements[3])
		elif words[0].strip() == "[Term]":
			return False
		return True

	def ReadEntity(self, file):
		"""Read in the details until we reach a line with [Term] in it, which indicates the next entity can be found"""
		continueReading = True
		try:
			line = file.next()
			while self.ParseLine(line):
				line = file.next()
		except StopIteration, e:
			continueReading = False
		return continueReading


class GoTerm:
	"""Storage for all details associated with a single GO Term within our area of interest (Human GOA)"""
	def __init__(self, term_id, groupID):
		self.associations = set()			#set of (gene_id, protein)
		self.brokenAssociations = set()			#set of details that can't be matched back to entrezgene IDs
		self.parents = dict()			#(parent_id, relationship, description)
		self.badParents = dict()
		self.term_id = term_id		#(GO:0000010) ID
		self.description = ''
		self.name = ''
		self.namespace = ''
		self.groupID = groupID

	def AddAssociation(self, gene_id, protein):
		self.associations.add((gene_id, protein))
		
	def AddUnknownAssociation(self, gene_name, protein):
		self.brokenAssociations.add((gene_name, protein))
		
	def ParseOntology(self, ontology, groups):
		"""Extract relevant information from the ontology entity (including group_ids associated with the varios parents"""
		if self.term_id != ontology.term_id:
			print "!!!!!!!!!!!!!!!!!!!!!! Term IDs don't match!"
			exit(0)
		self.name = ontology.name
		self.naemspace = ontology.namespace
		self.description = ontology.description
		
		for parent in ontology.parents:
			if parent in groups:
				#print "Child: %s\tParent: %s" % (self.term_id, parent)
				#group_id -> (name, desc)
				self.parents[groups[parent].groupID] = ontology.parents[parent]
			else:
				self.badParents = parent

	def Commit(self, groupTypeID, biosettings):
		#First, we'll add ourselves to the groups table
		pathway							= bioloader.Pathway(groupTypeID, self.groupID, self.term_id, self.description)
		#Next, add our associations to the association table
		for association in self.associations:
			pathway.AddGene(association[0])
			
		#finally, we can add the various groups. If we have no parents, we can set ourselves as our own parent (since we don't have any root)
		
		parentCount = len(self.parents)
		for parent in self.parents:
			biosettings.RelatePathways(parent, self.groupID, self.parents[parent][0], self.parents[parent][1])
		if parentCount == 0:
			biosettings.RelatePathways(groupTypeID, self.groupID, "", "")
		pathway.Commit(biosettings)
		

class GoLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=1):
		bioloader.BioLoader.__init__(self, biosettings, id)
		biosettings.LoadAliases()
		self.terms					= dict() #term_id -> GoTerm

	def ParseOntologies(self, filename):
		#print "Loading GO Ontologies"
		reader = open(filename, "r")
		#relationship = OntologyTerm()
		#print "Reading ontology file, ", filename
		doContinue = True
		entityCount = 0
		while doContinue:
			goGroup = OntologyTerm()
			doContinue = goGroup.ReadEntity(reader)
			if len(goGroup.term_id) > 0 and goGroup.term_id in self.terms:
				self.terms[goGroup.term_id].ParseOntology(goGroup, self.terms)
			entityCount+=1
		
		#print "\r....Completed...", entityCount, " Terms found."		
	
	def LoadGeneAssociation(self, line):
		words = line .split('\t')
		if words[0].strip().lower() in ['uniprotkb/swiss-prot', 'uniprotkb'] and words[12].strip().lower() == "taxon:9606":
			uniprotID = words[1]
			term_id = words[4]
			protein = words[9]
			geneLabel = words[2]
			evidence = words[6]
			aliases	= words[10].split("|")
			gene_id = 'NO_ID'
			failedInsertions = 0
			
			if evidence == "IEA":
				return
			if term_id not in self.terms:
				self.terms[term_id] = GoTerm(term_id, self.biosettings.NextID())
			geneIDs					= self.biosettings.regions.AliasToGeneID([uniprotID])
			
			if len(geneIDs) != 1:
				geneIDs					= self.biosettings.regions.AliasToGeneID([geneLabel])
				
			if len(geneIDs) != 1:
				for alias in aliases:
					geneIDs				= self.biosettings.regions.AliasToGeneID([alias])
				
			#print "%s -> %s " % (geneLabel, ",".join(["%s" % g for g in geneIDs]))

			if len(geneIDs) == 1:
				gene_id = geneIDs[0]
				self.terms[term_id].AddAssociation(gene_id, protein)
			else:
				self.terms[term_id].AddUnknownAssociation(geneLabel, protein)
				if len(geneIDs) == 0:
					#print "-----(%s, %s)\tUnable to recognize gene (%s)" % (geneLabel, term_id, words[12].strip())
					failedInsertions+=1
				elif len(geneIDs) != 1:
					failedInsertions+=1
					#print "-----(%s,%s)\tUnable to disambiguate gene ID (%s distinct ids) " % (geneLabel, term_id, len(geneIDs))
		
	def Commit(self):
		for term in self.terms:
			self.terms[term].Commit(self.groupID, self.biosettings)
		self.biosettings.Commit()
		
	def Load(self, force=True):
		os.system("rm -rf gene_ontology")
		cwd 					= os.getcwd()
		os.system("mkdir -p go")
		os.chdir("go")
		gene_assoc 				= self.FetchViaHTTP("ftp://ftp.geneontology.org/pub/go/gene-associations/gene_association.goa_human.gz")
		gene_assoc				= self._ExtractGZ(gene_assoc)
		
		obo_file				= self.FetchViaHTTP("ftp://ftp.geneontology.org/pub/go/ontology/obo_format_1_2/gene_ontology.1_2.obo")
		#obo_file				= self._ExtractGZ(obo_file)
		
		for line in open(gene_assoc):
			self.LoadGeneAssociation(line)
		self.ParseOntologies(obo_file)
		
		timestamp					= time.localtime(time.time())
		
		self.biosettings.PurgeGroupData(self.groupID)
		self.biosettings.CommitGroup(self.groupID, 1, "GO", "GO", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		
		self.Commit()
		os.chdir(cwd)


if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
		
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= GoLoader(bioDB)
	loader.Load()

	bioDB.Commit()

#!/usr/bin/env python

'''
Created on Jun 8, 2010

@author: torstees
'''

import os, time, struct, sys, csv
import bioloader, settings
from bioloader import Pathway
import biosettings


class PFamMetaGroup(bioloader.Pathway):
	def __init__(self, groupTypeID, groupID, name, desc):
		bioloader.Pathway.__init__(self, groupTypeID, groupID, name, desc)
		self.pathways			= dict()		#This is a link to actual pathway objects
	
	def AddAssociation(self, groupID, gene):
		if groupID in self.children:
			self.pathways[groupID].AddGene(gene)
	
	#pfamA				ftp://ftp.sanger.ac.uk/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz
	#Field name -> column translations:	 (Zero based index)
	#pfamA_acc		Column 1				--> Name - can be used to map genes from the other file
	#description	Column 4				--> Probably part of the description
	#domain			Column 8			--> This is used to pick which "Group" we are in. Breaking each of the sub groups apart
	#comment		Column 9				--> Probably part of the description
	def AddGroup(self, fields, groupID):
		if groupID not in self.children:
			newGroup				= PFamMetaGroup(self.groupTypeID, groupID, fields[1], biosettings.MakeAscii(fields[9]))
			self.children[groupID] 	= self.name
			self.pathways[groupID]  = newGroup
	
	def Commit(self, biosettings):
		biosettings.CommitPathway(self.groupTypeID, self.groupID, self.name, self.desc)
		for pathway in self.pathways:
			self.pathways[pathway].Commit(biosettings)
		print "PFam Group: %s\tGene Count: %s" % (self.name, len(self.genes))
		for gene in self.genes:
			biosettings.AssociateGene(self.groupID, gene)
		for child in self.children:
			biosettings.RelatePathways(self.groupID, child, self.children[child], "")


class PFamLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=4):
		bioloader.BioLoader.__init__(self, biosettings, id, "pfam")
		biosettings.LoadAliases()
		self.associations 		= dict()
		self.submetas			= dict()
		submetaNames			= ["Domain","Family","Motif","Repeat"]
		subID					= id+1
		self.pfLookup			= dict()
		for name in submetaNames:
			self.submetas[name] = PFamMetaGroup(id, subID, name, "PFam:%s" % name)
			subID += 1
	
	def Commit(self):
		for name in self.submetas:
			self.biosettings.RelatePathways(self.groupID, self.submetas[name].groupID, self.submetas[name].name, "")
			self.submetas[name].Commit(self.biosettings)		#Do local stuff
		self.biosettings.Commit()
	
	#we have to have uniProt gene IDs loaded as well as a group of items indexed by the pfam_acc id
	
	#seq_info.txt		ftp://ftp.sanger.ac.uk/pub/databases/Pfam/current_release/database_files/seq_info.txt.gz
	#Field Name -> Column Translations:
	#pfamA_acc		Column 0
	#pfamseq_acc	Column 6
	def LoadAssociations(self, filename):
		print "Loading associations from file, ", filename
		errReport					= open("%s.errors" % (filename), "w")
		reader = csv.reader(open(filename,"rU"), delimiter='\t', quotechar="'")
		for cleanWords in reader:
			if (cleanWords[0] in self.pfLookup):
				groupID				= self.pfLookup[cleanWords[0]]
				assoc 				= self.biosettings.regions.AliasToGeneID([cleanWords[6]])
				if len(assoc) == 1:
					assocID 		= list(assoc)[0]
					for meta in self.submetas:
						print>>errReport, "-- %s -> %s (%s : %s)" % (groupID, assocID, cleanWords[0], cleanWords[6])
						self.submetas[meta].AddAssociation(groupID, assocID)
				else:
					print>>errReport, "-- %s -> Unknown ID: %s (%s)" % (cleanWords[0], cleanWords[6], assoc)
			else:
				print>>errReport, "-- %s -> Unknown Group" % (cleanWords[0])
	
	def LoadGroupData(self, filename):
		reader = csv.reader(open(filename), delimiter='\t', quotechar="'", escapechar="\\")
		for cleanWords in reader:
			#print cleanWords[0:8]
			#They keep adding columns:/
			metaGroupName = cleanWords[8]
			id						= self.biosettings.NextID()
			self.pfLookup[cleanWords[1]] = id
			self.submetas[metaGroupName].AddGroup(cleanWords, id)
	
	def Load(self, force=True):
		os.system("rm -rf pfam")
		cwd 					= os.getcwd()
		os.system("mkdir -p pfam")
		os.chdir("pfam")
		famA 				= self.FetchViaHTTP("ftp://ftp.sanger.ac.uk/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz")
		#famA				= "pfamA.txt.gz"
		famA				= self._ExtractGZ(famA)
		
		seqFile				= self.FetchViaHTTP("ftp://ftp.sanger.ac.uk/pub/databases/Pfam/current_release/database_files/seq_info.txt.gz")
		#seqFile				= "seq_info.txt.gz"
		seqFile				= self._ExtractGZ(seqFile)
		os.system("dos2unix %s" % (seqFile))
		
		self.biosettings.PurgeGroupData(self.groupID)
		for submeta in self.submetas:
			self.biosettings.PurgeGroupData(self.submetas[submeta].groupID)
		self.LoadGroupData(famA)
		self.LoadAssociations(seqFile)
		
		timestamp					= time.localtime(time.time())
		
		self.biosettings.CommitGroup(self.groupID, 1, "PFam", "PFam", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		
		self.Commit()
		os.chdir(cwd)


if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
	
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= PFamLoader(bioDB)
	loader.Load()
	
	bioDB.Commit()

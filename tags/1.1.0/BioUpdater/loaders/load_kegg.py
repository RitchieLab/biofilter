#!/usr/bin/env python


'''
Created on May 18, 2010

@author: torstees
'''


import os, time, struct, sys
#from SOAPpy import WSDL

from suds.client import Client

from util import bioloader, settings, biosettings


import logging
logging.basicConfig(level=logging.INFO)



class KeggLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=2):
		bioloader.BioLoader.__init__(self, biosettings, id, "KEGG")
		biosettings.LoadAliases()
		
		self.srv = Client("http://soap.genome.jp/KEGG.wsdl").service
		#self.srv = WSDL.Proxy("http://soap.genome.jp/KEGG.wsdl")

	def Load(self, force=True):
		os.system("rm -rf kegg")
		
		doContinue = False
		
		while not doContinue:	
			#try
			remotePathways = self.srv.list_pathways('hsa')
			doContinue = True
			#except

		timestamp = time.localtime(time.time())
		
		self.biosettings.PurgeGroupData(self.groupID)
		self.biosettings.CommitGroup(self.groupID, 1, "KEGG", "KEGG", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		
		for pathway in remotePathways:
			self.LoadPathway(self.biosettings.NextID(), pathway)
		self.biosettings.Commit()

	def LoadPathway(self, groupID, remotePathway):
		definition = remotePathway['definition'][0]
		entryID = remotePathway['entry_id'][0]	
		
		#print remotePathway	
		
		genes = set()			# geneIDs identified with pathway
		failedInsertions = 0
		geneCount = 0
		#print "\n\nPathway(%s): %s - %s" % (groupID, entryID, definition)
		
		#because this could timeout, let's make sure it doesn't just faile
		doContinue = False
		
		while not doContinue:
			#try
			remoteGeneList = self.srv.get_genes_by_pathway(pathway_id=entryID)		
			
			doContinue = True
			for geneHSA in remoteGeneList:
				gene = geneHSA[4:]
				geneIDs = self.biosettings.regions.AliasToGeneID([gene])
				if len(geneIDs) == 0:
					#print "-----(%s, %s)\tUnable to recognize gene" % (gene, geneHSA)
					failedInsertions+=1
				elif len(geneIDs) != 1:
					failedInsertions+=1
					#print "-----(%s,%s)\tUnable to disambiguate gene ID (%s distinct ids) " % (gene, geneHSA, len(geneIDs))
				else:
					genes.add(geneIDs[0])
			
		
		if len(genes) > 0:
			pathway	= bioloader.Pathway(self.groupID, groupID, entryID[5:], definition)
			for geneID in genes:
				pathway.AddGene(geneID)
				geneCount+=1
			pathway.Commit(self.biosettings)
			self.biosettings.RelatePathways(self.groupID, groupID, "", "")
		#print "Genes Identified: %s\nGenes unable to be inserted: %s\nTotal genes associated : %s" % (geneCount, failedInsertions, len(genes))

if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename = sys.argv[1]
		
	bioDB = biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader = KeggLoader(bioDB)
	loader.Load()

	bioDB.Commit()

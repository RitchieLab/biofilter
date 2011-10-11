#!/usr/bin/env python


'''
Created on May 14, 2010

@author: torstees
'''

import os, time, struct, sys
import bioloader, settings
import biosettings



class PharmGKBLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=15):
		bioloader.BioLoader.__init__(self, biosettings, id)
		biosettings.LoadAliases()
		self.kb						= bioloader.KnowledgeBase(id, "PharGKB", "Pharmacogenomics Knowledge Base")
		self.pathways				= []
		
	def Load(self, force=True):
		os.system("rm -rf pharmgkb")
		cwd 					= os.getcwd()
		os.system("mkdir -p pharmgkb")
		os.chdir("pharmgkb")

		localFilename = self.FetchViaHTTP("http://www.pharmgkb.org/commonFileDownload.action?filename=pathways-tsv.zip")
		os.system("unzip %s" % (localFilename))
		
		dataFiles					= []
		filename					= "pathways.tsv"
		#if force or self.CheckTimestampAgainstServer(timestamp, self.groupID):
		self.biosettings.PurgeGroupData(self.groupID)
		#self.biosettings.CommitGroup(self.groupID, "PharmGKB", "PharmGKB", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))

		self.LoadPathways(filename)
		
		os.chdir(cwd)

	

	def LoadPathways(self, filename):
		curPathway = None
		for line in open(filename):
			words 					= line.split(":")
			if len(words) > 1:
				newID				= self.biosettings.NextID()
				if curPathway:
					self.pathways.append(curPathway)
				curPathway 			= bioloader.Pathway(self.groupID, newID, words[0].strip(), words[1].strip())
				self.kb.AssociatePathways(self.groupID, newID, "")
			else:
				words				= line.split()
				if len(words) > 1 and words[0] == "Gene":
					geneIDs			= self.biosettings.regions.AliasToGeneID([words[2].strip()])
					if len(geneIDs) == 1:
						curPathway.AddGene(geneIDs[0])
		if curPathway:
			self.pathways.append(curPathway)

	def Commit(self):
		print "ASDFASDFASDFASDFASDFASD"
		timestamp					= time.localtime(os.path.getmtime("pharmgkb/pathways.tsv"))
		self.kb.Commit(self.biosettings, time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		for pathway in self.pathways:
			pathway.Commit(self.biosettings)

if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
		
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= PharmGKBLoader(bioDB)
	loader.Load(False)

	bioDB.Commit()
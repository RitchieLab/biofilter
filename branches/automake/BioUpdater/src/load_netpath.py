#!/usr/bin/env python


'''
Created on May 14, 2010

@author: torstees
'''

import os, time, struct, sys
import bioloader, settings
import biosettings



class NetPathLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=3):
		bioloader.BioLoader.__init__(self, biosettings, id)
		biosettings.LoadAliases()

	def Load(self, force=True):
		os.system("rm -rf netpath")
		cwd 					= os.getcwd()
		os.system("mkdir -p netpath")
		os.chdir("netpath")

		localFilename = self.FetchViaHTTP("http://www.netpath.org/data/batch/NetPath_GeneReg_TSV.zip")
		os.system("unzip %s -d netpath" % (localFilename))
		
		dataFiles					= []
		files						= os.listdir('netpath')
		for filename in files:
			dataFiles.append(os.path.join("netpath", filename))
		print "%s files identified" % (len(dataFiles))
		timestamp					= time.localtime(os.path.getmtime(localFilename))
		#if force or self.CheckTimestampAgainstServer(timestamp, self.groupID):
		self.biosettings.PurgeGroupData(self.groupID)
		self.biosettings.CommitGroup(self.groupID, 1, "NetPath", "NetPath", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		for file in dataFiles:
			self.LoadFile(self.biosettings.NextID(), file)

		os.chdir(cwd)

	def LoadFile(self, groupID, filename):
		print filename
		elements					= filename.split("_")
		name						= "_".join(elements[:2])
		
		elements					= name.split("/")
		if len(elements) > 1:
			name = elements[1]
		
		print "Pathway Name:	    ", name
		pathway						= bioloader.Pathway(self.groupID, groupID, name, filename)
		
		lineCount					= 0
		geneCount					= 0
		genesMissed					= 0
		
		

		
		for line in open(filename):
			words					= line.strip().split()
			if lineCount > 0:
				geneID				= 0
				geneName			= words[2].strip()
				entrezID			= words[3].strip()
				geneIDs				= self.biosettings.regions.AliasToGeneID([entrezID, geneName])
				if len(geneIDs) > 0:
					if len(geneIDs) == 1:
						pathway.AddGene(geneIDs.pop())
						geneCount+=1
					else:
						print "-----(%s,%s)\tUnable to disambiguate gene ID (%s distinct ids) " % (geneName, entrezID, len(geneIDs))
				else:
					print "-----(%s,%s)\tUnable to recognize gene" % (entrezID, geneName)
					genesMissed+=1
			lineCount+=1
			
		print "Lines in File: %s\nGenes Identified: %s\nGenes Unable to be Identified: %s" % (lineCount, geneCount, genesMissed)
		pathway.Commit(self.biosettings)
		self.biosettings.RelatePathways(self.groupID, groupID, "", "")
if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
		
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= NetPathLoader(bioDB)
	loader.Load(False)

	bioDB.Commit()
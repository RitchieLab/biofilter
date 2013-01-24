#!/usr/bin/env python

'''
Created on May 17, 2010

@author: torstees
'''


import os, time, struct, sys
from util import bioloader, settings, biosettings


class EntrezGeneHistory(bioloader.BioLoader):
	def __init__(self, biosettings):
		bioloader.BioLoader.__init__(self, biosettings, 0, "ncbi")
		biosettings.LoadAliases()
		self.newAliasList 			= dict()
		
	
	def AppendAlias(self, alias, id):
		if alias not in self.newAliasList:
			self.newAliasList[alias] = set()
		self.newAliasList[alias].add(id)

	
	def Load(self, force=True):
		os.system("rm -rf gene_history")

		localFilename 				= self.FetchViaHTTP("ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_history.gz")
		localFilename  				= self._ExtractFilename(localFilename)
		
		print localFilename
		timestamp					= time.localtime(os.path.getmtime(localFilename))
		
		lineCount					= 0
		for line in open(localFilename):
			if lineCount > 0:
				words = line.strip().split()
				
				#ignore any but humans
				if words[0] == "9606":
					newID			= words[1]
					if newID != "-":
						oldID		= words[2]
						oldGene		= words[3].strip()
						
						if newID in self.biosettings.aliasToID:
							newID = self.biosettings.aliasToID[newID]
							self.AppendAlias(oldID, (newID, oldGene))
							self.AppendAlias(oldGene, (newID, oldGene))
						else:
							print "Unable to find entrez gene ID: %s in local gene lookup." % (newID)
			lineCount+=1
		for alias in self.newAliasList:
			aliases = self.newAliasList[alias]
			if len(aliases) == 1:
				for a in aliases:
					self.biosettings.AddAlias(alias, a[0], 1300, a[1], "Entrez Gene History File")
				else:
					msg		= "Too many endpoints for an alias assignment: %s -> " % (alias)
					for a in aliases:
						msg = "%s %s" % (msg, a[0])
			
		
if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
		
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= EntrezGeneHistory(bioDB)
	loader.Load()

	bioDB.Commit()
	

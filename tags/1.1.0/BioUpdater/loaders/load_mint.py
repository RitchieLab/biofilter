#!/usr/bin/env python

'''
Created on Jun 8, 2010

@author: torstees

The data must be downloaded by hand. My account is:
etorstenson
WW48k

ftp://mint.bio.uniroma2.it/pub/release/mitab26/current/*-mint-human-binary.mitab26.txt
'''

import os, time, struct, sys, csv
from util import bioloader, settings, biosettings

def BuildLookup(word, colSep="|", keySep=":"):
	cols = word.split(colSep)
	lookup					= dict()
	
	for col in cols:
		words				= col.split(keySep)
		lookup[words[0]]	= words[1]
	return lookup

class MintLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=13):
		bioloader.BioLoader.__init__(self, biosettings, id)
		biosettings.LoadAliases()

		self.missingUni				= set()
		self.ambiguousUni			= set()
		
		self.observedIDs			= set()		# Just to see if there are any groups that are more than pairs
	
	def Load(self, force=True):
		cwd 					= os.getcwd()
		os.system("mkdir -p mint")
		os.chdir("mint")
		self.OpenFTP("mint.bio.uniroma2.it")
		responses				= self.ListFtpFiles("pub/release/mitab26/current/*-mint-human-binary.mitab26.txt")
		filename				= responses[0]
		
		v						= int(time.time())

		#v 						= filename.split("/")[-1].split(".")[0][18:len(filename)-4]
		self.biosettings.SetVersion("mint", v)
		#print "\tDownloading file, ", filename
		filename	 				= self.FetchViaHTTP("ftp://mint.bio.uniroma2.it/pub/release/mitab26/current/%s" % filename)

		timestamp					= time.localtime(time.time())
		
		self.biosettings.PurgeGroupData(self.groupID)
		self.biosettings.CommitGroup(self.groupID, 1, "MINT", "MINT", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		
		success						= 0
		failures					= 0
		for line in open(filename):
			if self.LoadAssociation(line):
				success += 1
			else:
				failures += 1

		#print "Missing UniProt IDs:"
		#print "\t%s" % (",".join(self.missingUni))
		#print "Ambiguous UniProt IDs:"
		#print "\t%s" % (",".join(self.ambiguousUni))
		#print "DIP Groups Committed: %s" % (success)
		#print "DIP Groups Failed: %s" % (failures)

		self.biosettings.Commit()
		os.chdir(cwd)
			
	def LoadAssociation(self, line):
		cols							= line.split("\t")
		success							= False
		if len(cols[0].split(":")) > 1:
			idA							= cols[0].split(":")		#type:id 
			idB							= cols[1].split(":")
			geneA						= None
			geneB						= None
			acceptableTypes				= ["uniprotkb"]
			
			taxA						= cols[9].split(":")[-1].strip()
			taxB						= cols[10].split(":")[-1].strip()
			idLookup					= BuildLookup(cols[13])
			if taxA == taxB and taxA == "9606(Homo sapiens)":
				#we can also attempt to id a gene using columns 4 & 5
				if idA[0] in acceptableTypes:
					ids						= self.biosettings.regions.AliasToGeneID([idA[1]])
					if len(ids) == 1:
						geneA				= list(ids)[0]
					elif len(ids) == 0:
						self.missingUni		= idA[1]
					else:
						self.ambiguousUni	= idA[1]
				if idB[0] in acceptableTypes:
					ids						= self.biosettings.regions.AliasToGeneID([idB[1]])
					if len(ids) == 1:
						geneB				= list(ids)[0]
					elif len(ids) == 0:
						self.missingUni		= idB[1]
					else:
						self.ambiguousUni	= idB[1]
				if geneA != None and geneB != None and geneA != geneB:
					groupID			= self.biosettings.NextID()
					if idLookup["mint"] in self.observedIDs:
						print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
						print thisdoesnt.exit
					self.observedIDs.add(idLookup["mint"])
					pathway			= bioloader.Pathway(self.groupID, groupID, idLookup["mint"], "")
					pathway.AddGene(geneA)
					pathway.AddGene(geneB)
					pathway.Commit(self.biosettings)
					self.biosettings.RelatePathways(self.groupID, groupID, "mint", "")
					success			= True
			#else:
			#	print "Unknown tax ID: ", taxA, taxB
		return success
	def Commit(self):
		self.biosettings.Commit()

if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
		
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= MintLoader(bioDB)
	loader.Load()

	bioDB.Commit()

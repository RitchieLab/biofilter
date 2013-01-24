#!/usr/bin/env python

'''
Created on Jun 8, 2010

@author: torstees

The data must be downloaded by hand. My account is:
etorstenson
WW48k

http://thebiogrid.org/downloads/archives/Release%20Archive/BIOGRID-3.0.65/BIOGRID-ORGANISM-3.0.65.tab2.zip
'''

import os, time, struct, sys, csv, glob
from util import bioloader, settings, biosettings

def BuildLookup(word, colSep="|", keySep=":"):
	cols = word.split(colSep)
	lookup					= dict()
	
	for col in cols:
		words				= col.split(keySep)
		lookup[words[0]]	= words[1]
	return lookup

class BioGridLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=14):
		bioloader.BioLoader.__init__(self, biosettings, id, "MINT")
		biosettings.LoadAliases()

		self.missing				= set()
		self.ambiguous				= set()
		
		self.observedIDs			= set()		# Just to see if there are any groups that are more than pairs
	def Load(self, force=True):
		archive						= self.FetchViaHTTP("http://thebiogrid.org/downloads/archives/Release%20Archive/BIOGRID-3.0.65/BIOGRID-ORGANISM-3.0.65.tab2.zip")
		self._ExtractFilename(archive)
		#print "Trying to glob....", "%s/*Homo_sapiens*.tab2.txt" % (archive[:len(archive)-4])
		filename					= glob.glob("*Homo_sapiens*.tab2.txt" )
		if len(filename) > 0:
			timestamp					= time.localtime(time.time())
			v							= int(time.time())
	
			#v 						= filename.split("/")[-1].split(".")[0][18:len(filename)-4]
			self.biosettings.SetVersion("biogrid", v)
			
			self.biosettings.PurgeGroupData(self.groupID)
			self.biosettings.CommitGroup(self.groupID, 1, "BioGrid", "BioGrid", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
			
			success						= 0
			failures					= 0
			#print "Loading BioGrid Data from file, %s" % filename[0]
			for line in open(filename[0]):
				if self.LoadAssociation(line):
					success += 1
				else:
					failures += 1
	
			#print "Missing Entrez IDs:"
			#print "\t%s" % (",".join(["%s"%s for s in self.missing]))
			#print "Ambiguous Entrez IDs:"
			#print "\t%s" % (",".join(["%s"%s for s in self.ambiguous]))
			#print "DIP Groups Committed: %s" % (success)
			#print "DIP Groups Failed: %s" % (failures)
	
			self.biosettings.Commit()
		else:
			print>>sys.stderr, "Unable to find any files. Did the file get downloaded?"
			
	def LoadAssociation(self, line):
		#header line
		if line[0] == "#":
			return False
		cols							= line.split("\t")
		success							= False
		geneA							= int(cols[1])
		geneB							= int(cols[2])
		name							= "biogrid:%s" % (cols[0])
		cantContinue					= False
		taxA						= cols[15].strip()
		taxB						= cols[16].strip()
		
		
		if taxA == taxB and taxA == "9606":
			if geneA not in self.biosettings.regions.genes:
				self.missing.add(geneA)
				cantContinue				= True
			if geneB not in self.biosettings.regions.genes:
				self.missing.add(geneB)
				cantContinue				= True
		
			if not cantContinue:
				groupID			= self.biosettings.NextID()
				pathway			= bioloader.Pathway(self.groupID, groupID, name, "")
				pathway.AddGene(geneA)
				pathway.AddGene(geneB)
				pathway.Commit(self.biosettings)
				self.biosettings.RelatePathways(self.groupID, groupID, cols[12].strip(), "")
				success			= True
			#else:
			#	print "Unknown tax ID: ", taxA, taxB
		else:
			pass
			#print "Unable to find one or more genes (%s, %s) in %s total genes" % (geneA, geneB, len(self.biosettings.regions.genes))
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

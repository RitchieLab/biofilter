#!/usr/bin/env python

'''
Created on Oct 18, 2010

@author: Eric Torstenson

The data must be downloaded by hand. My account is:
etorstenson
WW48k

http://www.hprd.org/download

Unfortunately, this isn't available via automation-so we'll have to manually extract the files when a new version is released. Each time, you provide basic contact details and agree to the restrictions.
'''

import os, time, struct, sys, csv
from util import bioloader, settings, biosettings
from bioloader import Pathway


class DIPLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=12):
		bioloader.BioLoader.__init__(self, biosettings, id)
		biosettings.LoadAliases()
		self.ambiguousUni			= set()				#Keep up with ambiguous UniProt IDs that were encountered
		self.missingUni				= set()				#Keep up with missing UniProt IDs that were encountered

	def Load(self, force=True):
		#os.system("rm -rf dip/*")
		cwd 					= os.getcwd()
		#os.system("mkdir -p dip")
		os.chdir("dip")
		#self.OpenFTP("ftp.ensembl.org", "etorstenson", "WW48k")
		#filename			= self.FTPFile("2009/tab25/Hsapi20091230.txt.gz")
		#DIP have explicitly made it impossible to automate. So, this data might not be as fresh as the 
		#more reasonable sites
		filename			= "Hsapi20091230.txt" #self.FetchViaHTTP("http://dip.doe-mbi.ucla.edu/dip/File.cgi?FN=2009/tab25/Hsapi20091230.txt.gz")
		#filename	 				= self.FetchViaHTTP("ftp://etorstenson:F5bXRV8s@dip.doe-mbi.ucla.edu/2009/tab25/Hsapi20091230.txt.gz")
		#filename					= self._ExtractGZ(filename)
		print "Local filename: %s" % (filename)
		timestamp					= time.localtime(time.time())
		
		self.biosettings.PurgeGroupData(self.groupID)
		self.biosettings.CommitGroup(self.groupID, "DIP", "DIP", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		
		success						= 0
		failures					= 0
		for line in open(filename):
			if self.LoadAssociation(line):
				success += 1
			else:
				failures += 1

		print "Missing UniProt IDs:"
		print "\t%s" % (",".join(self.missingUni))
		print "Ambiguous UniProt IDs:"
		print "\t%s" % (",".join(self.ambiguousUni))
		print "DIP Groups Committed: %s" % (success)
		print "DIP Groups Failed: %s" % (failures)

		self.biosettings.Commit()
		os.chdir(cwd)

	def LoadAssociation(self, line):
		cols						= line.split("\t")
		words						= cols[0].split("|")
		dipA						= words[0].strip()
		uniA						= words[len(words)-1][10:]
		
		words						= cols[1].split("|")
		dipB						= words[0].strip()
		uniB						= words[len(words)-1][10:]
		taxA						= cols[9][6:]
		taxB						= cols[10][6:]
		comment						= cols[6]
		name						= cols[13]
		success						= False
		if taxA == taxB:
			idsA					= self.biosettings.regions.AliasToGeneID([uniA])
			idsB					= self.biosettings.regions.AliasToGeneID([uniB])
			if len(idsA) == 1 and len(idsB) == 1:
				geneA				= list(idsA)[0]
				geneB				= list(idsB)[0]
				
				if geneA != geneB:
					groupID			= self.biosettings.NextID()
					pathway			= Pathway(self.groupID, groupID, name, comment)
					pathway.AddGene(geneA)
					pathway.AddGene(geneB)
					pathway.Commit(self.biosettings)
					self.biosettings.RelatePathways(self.groupID, groupID, "PFAM", "")
					success			= True
			else:
				if len(idsA) == 0:
					self.missingUni.add(uniA)
				else:
					self.ambiguousUni.add(uniA)
				if len(idsB) == 0:
					self.missingUni.add(uniB)
				else:
					self.ambiguousUni.add(uniB)
		else:
			print >> sys.stdout, "Misaligned Taxonomy (%s), %s %s  : %s" % (name, taxA, taxB, comment)
		return success
	def Commit(self):
		self.biosettings.Commit()

if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
		
	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= DIPLoader(bioDB)
	loader.Load()

	bioDB.Commit()

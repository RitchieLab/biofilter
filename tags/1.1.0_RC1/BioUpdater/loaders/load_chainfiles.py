#!/usr/bin/env python
from util import bioloader, biosettings
import sys, os
#For now, we don't really know how to id the local build...so,
#we'll stick with UCSC's numbers

targetFiles = [
	"http://hgdownload.cse.ucsc.edu/goldenPath/hg16/liftOver/hg16ToHg19.over.chain.gz",
	"http://hgdownload.cse.ucsc.edu/goldenPath/hg17/liftOver/hg17ToHg19.over.chain.gz",
	"http://hgdownload.cse.ucsc.edu/goldenPath/hg18/liftOver/hg18ToHg19.over.chain.gz"
]

class ChainLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=16):
		bioloader.BioLoader.__init__(self, biosettings, id)

	def Load(self, force=True):
		cwd 					= os.getcwd()
		try:
			os.mkdir("hg-chains")
		#You can't tell it it's OK if the directory exists, so we'll just ignore the exception
		except:
			pass
		os.chdir("hg-chains")
		dbCursor 					= self.biosettings.GetCursor()
		lkup = {"X":"23", "Y":"24", "XY":"25", "M":"26"}

		for file in targetFiles:
			filename					= self.FetchViaHTTP(file)
			filename					= self._ExtractGZ(filename)
			#print "filename: ", filename
			origAssembly			= filename.split("To")[0][2:]
			f							= open(filename).read()
			chains					= f.split("chain")
			for chain in chains:
				if len(chain) > 0:
					#Grab the first line and pull out the original chromosome
					line					= "chain " + chain.split("\n")[0]
					words					= line.split()

					chr					= words[2][3:]

					if chr in ["X", "Y", "XY", "M"]:
						chr = lkup[chr]

					if len(chr.split("_")) == 1:
						#print "INSERT INTO chain_files VALUES (%s, %s, '%s')" % (chr, origAssembly, "chain "+chain)
						dbCursor.execute("INSERT INTO chain_files VALUES (%s, %s, '%s')" % (chr, origAssembly, "chain "+chain))
		os.chdir(cwd)

	def Commit(self):
		pass
if __name__ == "__main__":
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]

	bioDB					= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	loader					= ChainLoader(bioDB)
	loader.Load()

	bioDB.Commit()

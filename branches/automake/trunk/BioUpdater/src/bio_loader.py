#!/usr/bin/env python
'''
Created on Jun 10, 2010

@author: torstees
'''
import load_chainfiles
import biosettings, ncbi_loader, load_go, load_kegg, load_netpath, load_reactome
import load_pfam, load_ensembl, load_dip, load_mint, load_biogrid, load_pharmgkb
import sys, os

ensembl					= None
loadables				= ["snps", "genes", "go", "kegg", "reactome", "netpath", "pfam", "dip", "biogrid", "mint", "pharmgkb", "chainfiles"]
allgroups				= ["go", "kegg", "reactome", "netpath", "pfam", "dip", "biogrid", "mint", "pharmgkb", "chainfiles"]
def GetEnsembl(bioDB, refreshEnsembl):
	global ensembl
	if ensembl == None:
		ensembl					= load_ensembl.EnsemblLoader(bioDB, 2)
		if refreshEnsembl:
			print "Refreshing"
			ensembl.RefreshEnsemblDatabase()
		else:
			print "Not refreshing"
		ensembl.ConnectToEnsemblDB()
	else:
		print "--Ensembl Reused"
	return ensembl

def LoadKB(dbFilename, kbLoads, doReset = False, refreshEnsembl = False):
	global loadables
	chromosomes = ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT')
	bioDB					= biosettings.BioSettings(dbFilename)
	bioDB.OpenDB()
	
	if doReset:
		bioDB.ResetDB()

	os.system("mkdir -p download")
	os.chdir("download")

	#variations 				= bioDB.BuildDbFilename(bioDB.filename, "var")
	ncbiLoader				= ncbi_loader.NCBI_Loader(bioDB)

	if kbLoads[0] == "ALL":
		kbLoads = allgroups

	for kb in kbLoads:
		kb 					= kb.strip().lower()
		if kb == "snps":
			ncbiLoader.InitLog("snps.log")
			cwd				= os.getcwd()
			ensembl			= GetEnsembl(bioDB, refreshEnsembl)
			os.chdir("NCBI")
			#ncbiLoader.UpdateSNPs(chromosomes, "variations", ensembl)
			os.chdir(cwd)
			ensembl.InitVariations("variations", chromosomes)
			ncbiLoader.CloseLog()
		elif kb == "genes":
			ncbiLoader.InitLog("genes.log")
			ensembl			= GetEnsembl(bioDB, refreshEnsembl)
			#ensembl.ConnectToEnsemblDB(bioDB)
			ncbiLoader.UpdateGenes(ensembl, chromosomes)
			ncbiLoader.CloseLog()
		elif kb == "go":
			loader					= load_go.GoLoader(bioDB)
			loader.InitLog()
			loader.Load()
			loader.CloseLog()
		elif kb == "kegg":
			loader					= load_kegg.KeggLoader(bioDB)
			loader.InitLog()
			loader.Load()
			loader.CloseLog()
		elif kb == "reactome":
			loader					= load_reactome.ReactomeLoader(bioDB)
			loader.InitLog()
			ensembl					= GetEnsembl(bioDB, refreshEnsembl)
			loader.Load(ensembl)
			loader.Commit()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "netpath":
			loader					= load_netpath.NetPathLoader(bioDB)
			loader.InitLog()
			loader.Load(False)
			loader.CloseLog()
		elif kb == "pfam":
			loader					= load_pfam.PFamLoader(bioDB)
			loader.InitLog()
			loader.Load()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "dip":
			loader					= load_dip.DIPLoader(bioDB)
			loader.InitLog()
			loader.Load()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "mint":
			loader					= load_mint.MintLoader(bioDB)
			loader.InitLog()
			loader.Load()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "biogrid":	
			loader					= load_biogrid.BioGridLoader(bioDB)
			loader.InitLog()
			loader.Load()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "pharmgkb":
			print "Loading PharmGKB"
			loader					= load_pharmgkb.PharmGKBLoader(bioDB)
			loader.InitLog()
			loader.Load()
			print "Attempting to Commit the data"
			loader.Commit()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "chainfiles":
			print "Loading Chain files"
			loader					= load_chainfiles.ChainLoader(bioDB)
			loader.InitLog()
			loader.Load()
			loader.Commit()
			bioDB.Commit()
			loader.Commit()
		else:
			print>>sys.stderr, "Unknown kb name: %s. Options include: %s" % (kb, ",".join(loadables))
		
			
def RunCommands(configFilename):
	global loadables
	dbFilename 					= None
	kbLoads						= []
	doReset						= False
	refreshEnsembl				= os.getenv("REFRESH_ENSEMBL", "FALSE") == "TRUE"
	if configFilename == "ALL":
		kbLoads = loadables 
		doReset					= True
	elif configFilename == "GROUPS":
		kbLoads = ["genes"] + loadables
	else:
		for line in open(configFilename):
			words 					= line.strip().split()
			if words[0].strip().lower() == "db_name":
				dbFilename 			= words[1].strip()
			elif words[0].strip().lower() == "reload":
				kbLoads.append(words[1].strip().lower())
			elif words[0].strip().lower() == "db_reset":
				doReset				= True
			elif words[0].strip().lower() == "refresh_ensembl":
				refreshEnsembl		= True

	if refreshEnsembl:
		print "Refreshing Ensembl"
	else:
		print "Reusing Ensembl DB"
	LoadKB(dbFilename, kbLoads, doReset, refreshEnsembl)

if __name__ == '__main__':
	if len(sys.argv) > 1:
		if sys.argv[1].strip().lower() == "load":
			LoadKB(sys.argv[2], sys.argv[3:])
		else:
			RunCommands(sys.argv[1])
	else:
		print>>sys.stderr, """
bio_loader will parse a configuration file as guidance to populating or updating a biofilter database.

To perform a complete fresh build, users can substitute ALL for configuration filename.


Configuration File Contents:

DB_NAME Name			-- This can be left unset, and will default to one that contains the date
DB_RESET 				-- Basically drop and recreate all tables...all data will be lost
LOAD db_filename OPTION
Available Options include:
	SNPS				-- Load SNP data from ncbi
	GENES				-- Load Gene data from ncbi
	GO					-- Load GO data
	KEGG				-- Load KEGG data
	NETPATH				-- Load NetPath data
	REACTOME			-- Load Reactome data
	PFAM				-- Load PFam data

Optionally, users can specify as the first argument, LOAD, immediately followed by the name
of the database, then followed by one or more knowledge bases from the list above to be loaded. 
These can include genes and SNPs, however, the database will not be reset as is the case in ALL.
"""
		
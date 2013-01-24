#!/usr/bin/env python
'''
Created on Jun 10, 2010

@author: torstees
'''
from loaders import load_chainfiles
import loaders.util.biosettings as biosettings
from loaders import ncbi_loader, load_go, load_kegg, load_netpath, load_reactome
from loaders import load_pfam, load_ensembl, load_dip, load_mint, load_biogrid, load_pharmgkb
import sys, os
import dbsettings

import sqlite3, time, struct

ensembl = None
loadables = ["snps", "genes", "go", "kegg", "reactome", "netpath", "pfam", "dip", "biogrid", "mint", "pharmgkb", "chainfiles"]
allgroups = ["genes", "go", "kegg", "reactome", "netpath", "pfam", "dip", "biogrid", "mint", "pharmgkb", "chainfiles"]

def GetEnsembl(bioDB, refreshEnsembl, db_set):
	global ensembl
	if ensembl == None:
		ensembl = load_ensembl.EnsemblLoader(bioDB, db_set, 0)
		if refreshEnsembl:
			print "Refreshing Ensembl Database"
			ensembl.RefreshEnsemblDatabase()
		else:
			pass
			#print "Not refreshing"
		ensembl.ConnectToEnsemblDB()
	else:
		pass
		#print "--Ensembl Reused"
	return ensembl

def LoadKB(dbFilename, kbLoads, doReset = False, refreshEnsembl = False):
	global loadables	
	
	if kbLoads[0] == "TEST":
		LoadTest(dbFilename)
		return
	
	chromosomes = ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT')
	bioDB = biosettings.BioSettings(dbFilename)
	bioDB.OpenDB()
	db_set = dbsettings.DBSettings()
	
	if doReset:
		bioDB.ResetDB()

	os.system("mkdir -p download")
	os.chdir("download")

	#variations 				= bioDB.BuildDbFilename(bioDB.filename, "var")
	ncbiLoader = ncbi_loader.NCBI_Loader(bioDB)

	if kbLoads[0] == "ALL":
		kbLoads = loadables
		ensembl = GetEnsembl(bioDB, True, db_set)
	
	for kb in kbLoads:
		kb = kb.strip().lower()
		if kb == "snps":
			print "Loading SNPs"
			ncbiLoader.InitLog("snps.log")
			cwd = os.getcwd()
			ensembl = GetEnsembl(bioDB, refreshEnsembl, db_set)
			os.system("mkdir -p NCBI")
			os.chdir("NCBI")
			#ncbiLoader.UpdateSNPs(chromosomes, "variations", ensembl)
			os.chdir(cwd)
			ensembl.InitVariations("variations", chromosomes)
			ncbiLoader.CloseLog()
		elif kb == "genes":
			print "Loading Genes"
			ncbiLoader.InitLog("genes.log")
			ensembl = GetEnsembl(bioDB, refreshEnsembl, db_set)
			#ensembl.ConnectToEnsemblDB(bioDB)
			ncbiLoader.UpdateGenes(ensembl, chromosomes)
			ncbiLoader.CloseLog()
		elif kb == "go":
			print "Loading GO"
			loader = load_go.GoLoader(bioDB)
			loader.InitLog()
			loader.Load()
			loader.CloseLog()
		elif kb == "kegg":
			print "Loading KEGG"
			loader = load_kegg.KeggLoader(bioDB)
			loader.InitLog()
			loader.Load()
			loader.CloseLog()
		elif kb == "reactome":
			print "Loading Reactome"
			loader = load_reactome.ReactomeLoader(bioDB, db_set)
			loader.InitLog()
			ensembl = GetEnsembl(bioDB, refreshEnsembl, db_set)
			if refreshEnsembl:
				loader.RefreshDatabase()
			
			loader.Load(ensembl)
			loader.Commit()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "netpath":
			print "Loading Netpath"
			loader = load_netpath.NetPathLoader(bioDB)
			loader.InitLog()
			loader.Load(False)
			loader.CloseLog()
		elif kb == "pfam":
			print "Loading PFAM"
			loader = load_pfam.PFamLoader(bioDB)
			loader.InitLog()
			loader.Load()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "dip":
			print "Loading DIP -- DISABLED"
			loader = load_dip.DIPLoader(bioDB)
			loader.InitLog()
			#loader.Load()
			#bioDB.Commit()
			loader.CloseLog()
		elif kb == "mint":
			print "Loading MINT"
			loader = load_mint.MintLoader(bioDB)
			loader.InitLog()
			loader.Load()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "biogrid":	
			print "Loading Biogrid"
			loader = load_biogrid.BioGridLoader(bioDB)
			loader.InitLog()
			loader.Load()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "pharmgkb":
			print "Loading PharmGKB"
			loader = load_pharmgkb.PharmGKBLoader(bioDB)
			loader.InitLog()
			loader.Load()
			#print "Attempting to Commit the data"
			loader.Commit()
			bioDB.Commit()
			loader.CloseLog()
		elif kb == "chainfiles":
			print "Loading Chain files"
			loader = load_chainfiles.ChainLoader(bioDB)
			loader.InitLog()
			loader.Load()
			loader.Commit()
			bioDB.Commit()
			loader.Commit()
		else:
			print>>sys.stderr, "Unknown kb name: %s. Options include: %s" % (kb, ",".join(loadables))
		
def LoadTest(dbFilename):
	"""
	Loads a test database using fake data
	"""
	
	# For now, just delete any files in the way, and create empty files
	os.system("rm -f variations variations.txt " + dbFilename)
	# Construct the tables.  This is the ONLY time I'll use biosettings
	biodb = biosettings.BioSettings(dbFilename)
	biodb.InitDB()
	
	# get a cursor to the database:
	db = sqlite3.connect(dbFilename)
	c = db.cursor()
	
	gene_ids = [1, 2, 3, 4]

	# Add in the default population
	c.execute("INSERT INTO populations VALUES (0, 'NO-LD', 'default population', 'default');")
	c.execute("INSERT INTO populations VALUES (1, 'PLUS5', 'Shifted populations', 'regions shifted +5');")
	
	# Add in region alias types
	c.execute("INSERT INTO region_alias_type VALUES (10, 'G# gene names')")
	c.execute("INSERT INTO region_alias_type VALUES (11, 'R# gene names')")
	c.execute("INSERT INTO region_alias_type VALUES (12, 'Ambiguous gene names')")
	
	# Now, add in some genes:
	for g in gene_ids:
		c.execute("INSERT INTO regions VALUES (%d, 'G%d', '1', 'Gene %d');" % (g,g,g))
		c.execute("INSERT INTO region_bounds VALUES (%d, 0, %d, %d);" % (g, 2*(g-1)*10 + 5, 2*(g-1)*10 + 15))
		c.execute("INSERT INTO region_bounds VALUES (%d, 1, %d, %d);" % (g, 2*(g-1)*10 + 10, 2*(g-1)*10 + 20))
		c.execute("INSERT INTO region_alias VALUES (10, 'G%d', %d, 1);" % (g,g))
		c.execute("INSERT INTO region_alias VALUES (11, 'R%d', %d, 1);" % (g,g))

	# Add in genes 5 and 6
	c.execute("INSERT INTO regions VALUES (%d, 'G%d', '1', 'Gene %d');" % (5,5,5))
	c.execute("INSERT INTO regions VALUES (%d, 'G%d', '2', 'Gene %d');" % (6,6,6))
	c.execute("INSERT INTO region_bounds VALUES (%d, 0, %d, %d);" % (5,30,50))
	c.execute("INSERT INTO region_bounds VALUES (%d, 1, %d, %d);" % (5,35,55))
	c.execute("INSERT INTO region_bounds VALUES (%d, 0, %d, %d);" % (6,10,20))
	c.execute("INSERT INTO region_bounds VALUES (%d, 1, %d, %d);" % (6,15,25))
	c.execute("INSERT INTO region_alias VALUES (10, 'G%d', %d, 1);" % (5,5))
	c.execute("INSERT INTO region_alias VALUES (11, 'R%d', %d, 1);" % (5,5))
	c.execute("INSERT INTO region_alias VALUES (10, 'G%d', %d, 1);" % (6,6))
	c.execute("INSERT INTO region_alias VALUES (11, 'R%d', %d, 1);" % (6,6))
	
	# Add in ambiguous gene alias
	c.execute("INSERT INTO region_alias VALUES (12, 'G23', 2, 2);")
	c.execute("INSERT INTO region_alias VALUES (12, 'G23', 3, 2);")
	
	# OK, add in a source or two for pathways
	c.execute("INSERT INTO group_type (group_type_id, group_type, role_id) VALUES (1, 'S1', 1);")
	c.execute("INSERT INTO group_type (group_type_id, group_type, role_id) VALUES (2, 'S2', 1);")
	
	# Add a few pathways
	# Add the parent "meta-groups"
	c.execute("INSERT INTO groups VALUES (1, 1, 'S1', 'Src 1 Metagroup');")
	c.execute("INSERT INTO groups VALUES (2, 2, 'S2', 'Src 2 Metagroup');")
	c.execute("INSERT INTO groups VALUES (1, 101, 'P1', 'Pathway 1 (Src 1)');")
	c.execute("INSERT INTO groups VALUES (1, 102, 'P2', 'Pathway 2 (Src 1)');")
	c.execute("INSERT INTO groups VALUES (2, 201, 'P3', 'Pathway 3 (Src 2)');")
	c.execute("INSERT INTO groups VALUES (2, 202, 'P4', 'Pathway 4 (== P1) (Src 2)');")
	
	# Add the appropriate relationships among pathways
	c.execute("INSERT INTO group_relationships VALUES (101, 1, 0, 'P1 from S1');")
	c.execute("INSERT INTO group_relationships VALUES (102, 1, 0, 'P2 from S1');")
	c.execute("INSERT INTO group_relationships VALUES (201, 2, 0, 'P3 from S2');")
	c.execute("INSERT INTO group_relationships VALUES (202, 2, 0, 'P4 from S2');")
	c.execute("INSERT INTO group_relationships VALUES (102, 101, 0, 'P1 parent of P2');")
	
	
	# Add some relationships between pathways and genes
	c.execute("INSERT INTO group_associations VALUES (101,1);")
	c.execute("INSERT INTO group_associations VALUES (101,2);")
	c.execute("INSERT INTO group_associations VALUES (201,2);")
	c.execute("INSERT INTO group_associations VALUES (201,3);")
	c.execute("INSERT INTO group_associations VALUES (102,4);")
	c.execute("INSERT INTO group_associations VALUES (202,1);")
	c.execute("INSERT INTO group_associations VALUES (202,2);")
	
	# Add the build as "37"
	c.execute("INSERT INTO versions VALUES ('build','37');")
	
	# Now, add in some SNPs
	# SNPs will occur every 7 positions and will be numbered sequentially
	# starting from 1.
	# The role will be determined by their "oddness", with odd SNPs
	# (rs1, rs3,...) being exons and even SNPs being introns
	c.execute("INSERT INTO snp_role VALUES (1, 'Exon');")
	c.execute("INSERT INTO snp_role VALUES (2, 'Intron');")
	
	c.execute("INSERT INTO versions VALUES ('variations', 'variations-test');")
	
	db.commit()
	
	snp_ids_1 = [x + 1 for x in range((20*max(gene_ids))/7)]
		
	# open up the variations file
	f = file("variations-test", "wb")
	
	# Write the file header
	f.write(struct.pack('I', int(time.time())))
	
	# Write the chromosome header
	f.write('1 ')
	f.write(struct.pack('II', len(snp_ids_1), max(snp_ids_1)*7))
	
	# Write each SNP
	for s in snp_ids_1:
		f.write(struct.pack('III', s, 7*s, (s+1) % 2 + 1))
	
	snp_ids_2 = [21 + x for x in range(4)]

	# Write the chromosome header
	f.write('2 ')
	f.write(struct.pack('II', len(snp_ids_2), (max(snp_ids_2)-20)*7))
	
	# Write each SNP
	for s in snp_ids_2:
		f.write(struct.pack('III', s, 7*(s-20), (s+1) % 2 + 1))
	
	f.close()
	
	os.system("touch variations-test.txt")
			
def RunCommands(configFilename):
	global loadables
	dbFilename = None
	kbLoads = []
	doReset = False
	refreshEnsembl = os.getenv("REFRESH_ENSEMBL", "FALSE") == "TRUE"
	if configFilename == "ALL":
		kbLoads = loadables 
		#doReset = True
	elif configFilename == "GROUPS":
		kbLoads = ["genes"] + loadables
	else:
		for line in open(configFilename):
			words = line.strip().split()
			if words[0].strip().lower() == "db_name":
				dbFilename = words[1].strip()
			elif words[0].strip().lower() == "reload":
				kbLoads.append(words[1].strip().lower())
			elif words[0].strip().lower() == "db_reset":
				doReset = True
			elif words[0].strip().lower() == "refresh_ensembl":
				refreshEnsembl = True

	#if refreshEnsembl:
	#	print "Refreshing Ensembl"
	#else:
	#	print "Reusing Ensembl DB"
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
		

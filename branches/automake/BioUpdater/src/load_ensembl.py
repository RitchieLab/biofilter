#!/usr/bin/env python

"""
Created on May 7, 2010

@author: torstees
"""
import os, time, struct, sys, datetime
import bioloader, settings, sqlite3
import MySQLdb
import region_manager


class EnsemblLoader(bioloader.BioLoader):
	def __init__(self, biosettings, coord, rebuildDatabase = False):
		bioloader.BioLoader.__init__(self, biosettings, 0)
		self.coordinate			= coord
		self.rebuildDatabase	= rebuildDatabase
		self.ensembl			= None
		self.roles				= dict()
	
	def ConnectToEnsemblDB(self, host = "badger", user="atf3", password="patO9FTPXUV0JonedaLe1COO"):
		self.ensembl		= MySQLdb.connect (host, user, password, db="ritchie_ensembl")
	
	def RefreshEnsemblDatabase(self):
		#TODO: fix ensembl refresh with configurable database settings
		pass
		"""
		print "Refreshing Ensembl Database"
		cwd 					= os.getcwd()
		os.system("mkdir -p ensembl")
		os.chdir("ensembl")
		self.OpenFTP("ftp.ensembl.org")
		
		trunkPath			= "pub/" + self.FtpGetLast("pub/release-*") + "/mysql/"
		varPath				= trunkPath + self.ListFtpFiles("%shomo_sapiens_variation_*" % trunkPath)[0].split()[-1]
		corePath			= trunkPath + self.ListFtpFiles("%shomo_sapiens_core_*" % trunkPath)[0].split()[-1]
		pieces				= varPath.split("_")
		self.version		= pieces[len(pieces)-2]
		self.ncbiVersion	= pieces[-1]
		
		print "Ensembl Version: %s\tNCBI Version: %s" % (self.version, self.ncbiVersion)
		
		variationFiles		= self.ListFtpFiles("%s/var*.txt.gz" % varPath)
		varSQL				= self.FTPFile(varPath + "/" + self.ListFtpFiles("%s/homo_sapiens_var*.sql.gz" % varPath)[0])
		os.system("mkdir -p variation")
		os.system("mkdir -p core")
		
		for file in variationFiles:
			localFile		= self.FTPFile("%s/%s"% (varPath, file))
			#print localFile
			os.system("mv %s variation" % (localFile))
		
		coreFiles			= self.ListFtpFiles("%s/[egostux]*.txt.gz" % corePath)
		coreSQL				= self.FTPFile(corePath + "/" + self.ListFtpFiles("%s/homo_sapiens_core*.sql.gz" % corePath)[0])
		
		for file in coreFiles:
			localFile		= self.FTPFile("%s/%s"% (corePath, file))
			os.system("mv %s core" % (localFile))
		
		print "mysql -h rogue -u torstees -p'SMOJ2010' -e \"DROP DATABASE IF EXISTS ensembl; CREATE DATABASE ensembl;\""
		os.system("mysql -h rogue -u torstees -p'SMOJ2010' -e \"DROP DATABASE IF EXISTS ensembl; CREATE DATABASE ensembl;\"")
		print "mysql -h rogue -u torstees -p'SMOJ2010' -e \"DROP DATABASE IF EXISTS variation; CREATE DATABASE variation;\""
		os.system("mysql -h rogue -u torstees -p'SMOJ2010' -e \"DROP DATABASE IF EXISTS variation; CREATE DATABASE variation;\"")
		
		print "cat %s | mysql -h rogue -u torstees -p'SMOJ2010' ensembl" % (coreSQL)
		os.system("cat %s | mysql -h rogue -u torstees -p'SMOJ2010' ensembl" % (coreSQL))
		print "cat %s | mysql -h rogue -u torstees -p'SMOJ2010' variation" % (varSQL)
		os.system("cat %s | mysql -h rogue -u torstees -p'SMOJ2010' variation" % (varSQL))
		
		print "mysqlimport -u root ensembl -h rogue -u torstees -p'SMOJ2010' -L core/*.txt"
		os.system("mysqlimport -u root ensembl -h rogue -u torstees -p'SMOJ2010' -L core/*.txt")
		print "mysqlimport -u root variation -h rogue -u torstees -p'SMOJ2010' -L variation/*.txt"
		os.system("mysqlimport -u root variation -h rogue -u torstees -p'SMOJ2010' -L variation/*.txt")
		
		os.system("mkdir -p processed; mv *.sql core variation processed")
		os.system("mysql -h rogue -u torstees -p'SMOG2010' -e \"CREATE TABLE IF NOT EXISTS ensembl.biodb_versions (element VARCHAR(64), version VARCHAR(64))\"")
		os.system("mysql -h rogue -u torstees -p'SMOG2010' -e \"DROP FROM TABKE ensembl.biodb_versions; INSERT INTO ensembl.biodb_versions VALUES ('build','%s'); INSERT INTO ensembl.biodb_versions VALUES ('ncbi', '%s');\"" % (self.ncbiVersion, self.version))
		self.biosettings.SetVersion("build", self.ncbiVersion)
		self.biosettings.SetVersion("ensembl", self.version)
		
		os.chdir(cwd)
		"""
	
	def GrabEnsemblVersionDetails(self):
		c= self.ensembl.cursor()
		c.execute("SELECT version FROM biodb_versions WHERE element='build'")
		row = c.fetchone()
		self.ncbiVersion = row[0]
		
		c.execute("SELECT version FROM biodb_versions WHERE element='enembl'")
		row = c.fetchone()
		self.version     = row[0]
		self.biosettings.SetVersion("build", self.ncbiVersion)
		self.biosettings.SetVersion("ensembl", self.version)
	
	def LoadRegionsFromEnsembl(self, chromosomes):
		self.genes								= dict()
		for chrom in chromosomes:
			self.biosettings.regions.LoadGenesOnChromosome(chrom, self.ensembl.cursor())
		#self.LoadRegionAliasesFromEnsembl()
	
	def LoadRegionAliasesFromEnsembl(self):
		self.biosettings.regions.LoadAliasesFromEnsembl(self.ensembl)
		self.biosettings.regions.Commit(self.biosettings.db)
	
	def InitVariations(self, filename, chromosomes):
		v						= int(time.time())
		d = datetime.datetime.now()
		filename 				= "%s-ens.%s%02d%02d" % (filename, d.year, int(d.month), int(d.day))
		print "Creating Variations File: %s" % (filename)
		self.biosettings.SetVersion("variations", filename)
		variations 				= open(filename, "wb")
		snpLog					= open("%s.txt" % (filename), "w")
		
		self.roles								= dict()
		print "Variation Filename: ", filename
		file 				= open(filename, "wb")
		#print>>sys.stderr, "Gathering Variation Data"
		
		file.write(struct.pack('I', v))
		for chrom in chromosomes:
			self.WriteChromosome(file, chrom, snpLog)
		
		c= self.biosettings.db.cursor()
		for role in self.roles:
			c.execute("INSERT INTO snp_role (id, role) VALUES (?,?)", (self.roles[role], role))
		return v
	
	def CommitRoles(self):
		c= self.biosettings.db.cursor()
		for role in self.roles:
			c.execute("INSERT INTO snp_role (id, role) VALUES (?,?)", (self.roles[role], role))
	
	def GetRoleID(self, role):
		if role not in self.roles:
			self.roles[role] = len(self.roles) + 1
		return self.roles[role]
	
	def LoadSnpRoles(self, chrom):
		cursor						= self.ensembl.cursor()
		roles						= dict()
		cursor.execute("""
SELECT 
	a.name as rs_id,
	seq_region_start AS position, 
	b.consequence_type as role
FROM 
	variation a INNER JOIN 
	variation_feature b ON (a.variation_id = b.variation_id) INNER JOIN 
	(SELECT * FROM seq_region WHERE coord_system_id=%s AND name=%s) c ON (b.seq_region_id = c.seq_region_id)
WHERE a.name LIKE %s""", (self.coordinate, chrom, 'rs%'))
		for row in cursor.fetchall():
			roleID									= self.GetRoleID(row[2])
			roles[row[1]] = (int(row[0][2:]), roleID)
		return roles	
	
	def WriteChromosome(self, file, chrom, textFile):
		cursor = self.ensembl.cursor()
		#outCur = dest.OpenDB()
		offset = 0
		
		print>> sys.stderr, """
SELECT
  COUNT(a.name),
  c.name AS chromosome,
  MAX(seq_region_start) AS position
FROM variation a
INNER JOIN variation_feature b
  ON a.variation_id = b.variation_id
INNER JOIN seq_region c
  ON b.seq_region_id = c.seq_region_id
WHERE c.coord_system_id=%s
  AND a.name REGEXP '%s'
  AND c.name=%s
GROUP BY c.name
""" % (self.coordinate, '^rs[0-9]+$', chrom)
		
		#Table of Contents for the variations
		cursor.execute("""
SELECT
  COUNT(a.name),
  c.name AS chromosome,
  MAX(seq_region_start) AS position
FROM variation a
INNER JOIN variation_feature b
  ON a.variation_id = b.variation_id
INNER JOIN seq_region c
  ON b.seq_region_id = c.seq_region_id
WHERE c.coord_system_id=%s
  AND a.name REGEXP %s
  AND c.name=%s
GROUP BY c.name
""", (self.coordinate, '^rs[0-9]+$', chrom))
		row = cursor.fetchone()
		chr = row[1]
		if (len(chr) > 1):
			file.write(chr[:2])
		else:
			file.write(chr)
			file.write(' ')
		file.write(struct.pack('II', row[0], row[2]))
		#snpCount = row[0]
		totalSNPs = 0
		# Iterate over the return according to the step increments
		cursor.execute("""
SELECT
  a.name as rs_id,
  seq_region_start AS position,
  b.consequence_type as role
FROM variation a
INNER JOIN variation_feature b
  ON a.variation_id = b.variation_id
INNER JOIN (
  SELECT *
  FROM seq_region
  WHERE coord_system_id=%s
    AND name=%s
) c
  ON b.seq_region_id = c.seq_region_id
WHERE a.name REGEXP %s
""", (self.coordinate, chrom, '^rs[0-9]+$'))
		offset += cursor.rowcount
		row = cursor.fetchone()
		snpCount = 0
		while (row):
			roleID									= self.GetRoleID(row[2]);
			#print "%s\t%s\t%s" % (chrom, row[0], row[1])
			file.write(struct.pack('III', (int)(row[0][2:]), row[1], roleID))
			print>>textFile, "%s\t%s\t%s\t%s" % (chr, int(row[0][2:]), row[1], roleID)
			#print>>textFile, "%s %s %s" % (row[0][2:], row[1], roleID)
			row=cursor.fetchone()
			snpCount+=1
			totalSNPs+=1
		print "Chromosome ", chrom, " completed. ", totalSNPs, " total variations written to file."
		
		self.biosettings.Commit()
		cursor.close()


if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
	biodb 					= settings.InitDB(filename)
	ensembl					= EnsemblLoader(biodb, 2)
	ensembl.ConnectToEnsemblDB()
	chromosomes = ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT')
	filename 				= biodb.BuildDbFilename("bio-settings", "var")
	ensembl.InitVariations(filename, chromosomes)
	ensembl.LoadRegionsFromEnsembl(chromosomes)

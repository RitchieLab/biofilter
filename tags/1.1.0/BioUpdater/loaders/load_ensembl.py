#!/usr/bin/env python

"""
Created on May 7, 2010

@author: torstees
"""
import os, time, struct, sys, datetime, sqlite3
from util import bioloader, settings, region_manager
import MySQLdb

import subprocess
import re
import shlex

class EnsemblLoader(bioloader.BioLoader):
	def __init__(self, biosettings, db_set, coord, rebuildDatabase = False):
		bioloader.BioLoader.__init__(self, biosettings, 0, dbsettings_in=db_set)
		self.coordinate = coord
		self.rebuildDatabase = rebuildDatabase
		self.ensembl = None
		self.roles = dict()
	
	def ConnectToEnsemblDB(self, host = None, user=None, password=None):
		if host is None:
			host = self.db_settings.db_host
		if user is None:
			user = self.db_settings.db_user
		if password is None:
			password = self.db_settings.db_pass
		self.ensembl = MySQLdb.connect(host, user, password, db=self.db_settings.db_name)
	
	def RefreshEnsemblDatabase(self):
		#TODO: fix ensembl refresh with configurable database settings
		#pass
		
		#print "Refreshing Ensembl Database"
		cwd = os.getcwd()
		os.system("mkdir -p ensembl")
		os.chdir("ensembl")
		self.OpenFTP("ftp.ensembl.org")
		
		core_prefix = "ens"
		variation_prefix = "var"
		
		pass_cmd = ""
		if self.db_settings.db_pass:
			pass_cmd = " -p'" + self.db_settings.db_pass + "' "
			
		mysql_opts = "-h " + self.db_settings.db_host + " -u " + self.db_settings.db_user + pass_cmd + " " + self.db_settings.db_name
		
		trunkPath = "pub/" + self.FtpGetLast("pub/release-*") + "/mysql/"
		varPath = trunkPath + self.ListFtpFiles("%shomo_sapiens_variation_*" % trunkPath)[0].split()[-1]
		corePath = trunkPath + self.ListFtpFiles("%shomo_sapiens_core_*" % trunkPath)[0].split()[-1]
		pieces = varPath.split("_")
		self.version = pieces[len(pieces)-2]
		self.ncbiVersion = pieces[-1]
		
		#print "Ensembl Version: %s\tNCBI Version: %s" % (self.version, self.ncbiVersion)
		
		
		variationFiles = self.ListFtpFiles("%s/var*.txt.gz" % varPath)
		variationFiles.extend(self.ListFtpFiles("%s/seq*.txt.gz" % varPath))
		varSQL = self.FTPFile(varPath + "/" + self.ListFtpFiles("%s/homo_sapiens_var*.sql.gz" % varPath)[0])
		os.system("mkdir -p variation")
		os.system("mkdir -p core")
		
		for v_file in variationFiles:
			localFile = self.FTPFile("%s/%s"% (varPath, v_file), variation_prefix + "_")
			os.system("mv %s variation" % (localFile,))
		
		coreFiles = self.ListFtpFiles("%s/[egostux]*.txt.gz" % corePath)
		coreSQL = self.FTPFile(corePath + "/" + self.ListFtpFiles("%s/homo_sapiens_core*.sql.gz" % corePath)[0])
		
		for c_file in coreFiles:
			localFile = self.FTPFile("%s/%s"% (corePath, c_file), core_prefix + "_")
			os.system("mv %s core/" % (localFile,))
		

		# We're going to fix up the SQL to prefix the table names appropriately
		table_re = re.compile(r"CREATE TABLE `([^`]*)`", re.IGNORECASE)
		view_re = re.compile(r"(CREATE.*?VIEW\s*`)([^`]*)(`)", re.IGNORECASE)
		tn_re = re.compile(r"(`)([^`]*)(`\.`)", re.IGNORECASE)
		tn2_re = re.compile(r"((?:from)\s*`)([^`]*)(`)", re.IGNORECASE)
		def repl_view(prefix, mo): return "DROP VIEW IF EXISTS `%s_%s`;\n%s%s_%s%s" % (prefix, mo.group(2), mo.group(1), prefix, mo.group(2), mo.group(3))
		def repl_tables(prefix, mo): return "DROP TABLE IF EXISTS `%s_%s`;\nCREATE TABLE `%s_%s`" % (prefix, mo.group(1), prefix, mo.group(1))
		def repl_table_name(prefix, mo): return "%s%s_%s%s" % (mo.group(1), prefix, mo.group(2), mo.group(3))
		
		varSQLf = file(varSQL,'r')
		varSQL_str = varSQLf.read()
		varSQLf.close()
		varSQL_str_fix = table_re.sub(lambda x,p=variation_prefix: repl_tables(p,x), varSQL_str)
		varSQL_str_fix = tn_re.sub(lambda x,p=variation_prefix: repl_table_name(p,x), varSQL_str_fix)
		varSQL_str_fix = tn2_re.sub(lambda x,p=variation_prefix: repl_table_name(p,x), varSQL_str_fix)
		varSQL_str_fix = view_re.sub(lambda x,p=variation_prefix: repl_view(p,x), varSQL_str_fix)

		coreSQLf = file(coreSQL,'r')
		coreSQL_str = coreSQLf.read()
		coreSQLf.close()
		coreSQL_str_fix = table_re.sub(lambda x,p=core_prefix: repl_tables(p,x), coreSQL_str)
		coreSQL_str_fix = tn_re.sub(lambda x,p=core_prefix: repl_table_name(p,x), coreSQL_str_fix)
		coreSQL_str_fix = tn2_re.sub(lambda x,p=core_prefix: repl_table_name(p,x), coreSQL_str_fix)		
		coreSQL_str_fix = view_re.sub(lambda x,p=core_prefix: repl_view(p,x), coreSQL_str_fix)
			
		mysql_proc = subprocess.Popen(shlex.split("mysql " + mysql_opts), stdin=subprocess.PIPE)
		mysql_proc.communicate(varSQL_str_fix)
		mysql_proc = subprocess.Popen(shlex.split("mysql " + mysql_opts), stdin=subprocess.PIPE)
		mysql_proc.communicate(coreSQL_str_fix)	
		
		#os.system("cat %s | mysql " % (varSQL) + mysql_opts)
		
		#print "cat %s | mysql -h rogue -u torstees -p'SMOJ2010' ensembl" % (coreSQL)
		#os.system("cat %s | mysql " % (coreSQL) + mysql_opts)
		
		os.system("mysqlimport -L " + mysql_opts + " core/*.txt")
		os.system("mysqlimport -L " + mysql_opts + " variation/*.txt")
				
		#os.system("mkdir -p processed; mv *.sql core variation processed")
		
		os.system("mysql " + mysql_opts + " -e \"DROP TABLE IF EXISTS " + self.db_settings.db_name + ".biodb_versions; CREATE TABLE " + self.db_settings.db_name + ".biodb_versions (element VARCHAR(64), version VARCHAR(64)); INSERT INTO " + self.db_settings.db_name + ".biodb_versions VALUES ('ensembl','%s'); INSERT INTO " % (self.version) + self.db_settings.db_name + ".biodb_versions VALUES ('build', '%s');\"" % (self.ncbiVersion))
		self.biosettings.SetVersion("build", self.ncbiVersion)
		self.biosettings.SetVersion("ensembl", self.version)
		
		
		os.chdir(cwd)
		
	
	def GrabEnsemblVersionDetails(self):
		c= self.ensembl.cursor()
		c.execute("SELECT version FROM biodb_versions WHERE element='build'")
		row = c.fetchone()
		self.ncbiVersion = row[0]
		
		c.execute("SELECT version FROM biodb_versions WHERE element='enembl'")
		row = c.fetchone()
		self.version = row[0]
		self.biosettings.SetVersion("build", self.ncbiVersion)
		self.biosettings.SetVersion("ensembl", self.version)
	
	def LoadRegionsFromEnsembl(self, chromosomes):
		self.genes = dict()
		for chrom in chromosomes:
			self.biosettings.regions.LoadGenesOnChromosome(chrom, self.ensembl.cursor())
		#self.LoadRegionAliasesFromEnsembl()
	
	def LoadRegionAliasesFromEnsembl(self):
		self.biosettings.regions.LoadAliasesFromEnsembl(self.ensembl)
		self.biosettings.regions.Commit(self.biosettings.db)
	
	def InitVariations(self, filename, chromosomes):
		v = int(time.time())
		d = datetime.datetime.now()
		#filename = "%s-ens.%s%02d%02d" % (filename, d.year, int(d.month), int(d.day))
		#print "Creating Variations File: %s" % (filename)
		self.biosettings.SetVersion("variations", filename)
		f = open(os.path.join("..", filename), "wb")
		snpLog = open(os.path.join("..", filename) + ".txt", "w")
		
		self.roles = dict()
		#print "Variation Filename: ", filename
		#print>>sys.stderr, "Gathering Variation Data"
		
		f.write(struct.pack('I', v))
		for chrom in chromosomes:
			self.WriteChromosome(f, chrom, snpLog)
		
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
		cursor = self.ensembl.cursor()
		roles = dict()
		cursor.execute("""
SELECT 
	a.name as rs_id,
	seq_region_start AS position, 
	b.consequence_type as role
FROM 
	var_variation a INNER JOIN 
	var_variation_feature b ON (a.ariation_id = b.variation_id) INNER JOIN 
	(SELECT * FROM var_seq_region WHERE coord_system_id=%s AND name=%s) c ON (b.seq_region_id = c.seq_region_id)
WHERE a.name LIKE %s""", (self.coordinate, chrom, 'rs%'))
		for row in cursor.fetchall():
			roleID									= self.GetRoleID(row[2])
			roles[row[1]] = (int(row[0][2:]), roleID)
		return roles	
	
	def WriteChromosome(self, f, chrom, textFile):
		print "\tFinding Variants for chomosome %s." % (chrom,),
		cursor = self.ensembl.cursor()
		#outCur = dest.OpenDB()
		offset = 0

		#Table of Contents for the variations
		cursor.execute("""
SELECT
  COUNT(a.name),
  c.name AS chromosome,
  MAX(seq_region_start) AS position
FROM var_variation a
INNER JOIN var_variation_feature b
  ON a.variation_id = b.variation_id
INNER JOIN var_seq_region c
  ON b.seq_region_id = c.seq_region_id
WHERE c.coord_system_id=%s
  AND a.name REGEXP %s
  AND c.name=%s
GROUP BY c.name
""", (self.coordinate, '^rs[0-9]+$', chrom))
		row = cursor.fetchone()
		totalSNPs = 0
		if row:
			chr = row[1]
			if (len(chr) > 1):
				f.write(chr[:2])
			else:
				f.write(chr)
				f.write(' ')
			f.write(struct.pack('II', row[0], row[2]))
			#snpCount = row[0]

			# Iterate over the return according to the step increments
			cursor.execute("""
SELECT
  a.name as rs_id,
  seq_region_start AS position,
  b.consequence_type as role
FROM var_variation a
INNER JOIN var_variation_feature b
  ON a.variation_id = b.variation_id
INNER JOIN (
  SELECT *
  FROM var_seq_region
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
				roleID = self.GetRoleID(row[2]);
				#print "%s\t%s\t%s" % (chrom, row[0], row[1])
				f.write(struct.pack('III', (int)(row[0][2:]), row[1], roleID))
				#print>>textFile, "%s\t%s\t%s\t%s" % (chr, int(row[0][2:]), row[1], roleID)
				#print>>textFile, "%s %s %s" % (row[0][2:], row[1], roleID)
				row=cursor.fetchone()
				snpCount+=1
				totalSNPs+=1
		print "Complete;", totalSNPs, " total variations written to file."
		
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

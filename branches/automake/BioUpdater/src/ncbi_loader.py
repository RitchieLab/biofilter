#!/usr/bin/env python

'''
Created on May 10, 2010

@author: torstees
'''

import bioloader, settings, struct
import os, sys, time
import load_ensembl

class Variant:
	"""Storage for SNP data during chromosome loading"""
	def __init__(self, rsid, pos):
		self.rsid					= rsid
		if rsid[:2].lower == "rs":
			self.rsid				= int(rsid[2:])
		self.pos					= int(pos)


class NCBI_Loader(bioloader.BioLoader):
	def __init__(self, biosettings):
		#We don't have a group ID for this, so we'll just use 0
		bioloader.BioLoader.__init__(self, biosettings, 0, "ncbi")
		self.roles					= dict()

	def GetRoleID(self, role):
		if role not in self.roles:
			self.roles[role] = len(self.roles) + 1
		return self.roles[role]

	def ParseGene2Refseq(self, filename):
		linecount					= 0
		validGeneCount				= 0
		for line in open(filename):
			if linecount > 0:
				words 					= line.split()
				if len(words) > 0 and words[0] == '9606':
					geneID				= int(words[1])
					accID				= words[7]
					mRNAaccID			= words[3]
					proteinAcc			= words[5].split(".")[0]

					#we only want the NCs...not the NTs
					if accID[:2] == "NC":
						start			= int(words[9])
						stop			= int(words[10])
						source			= words[12]
						strand			= words[11]
						self.biosettings.regions.AddEntrezGene(geneID, accID, start, stop, strand, proteinAcc, mRNAaccID)
						validGeneCount += 1
					else:
						#We'll try to pull these items out of ensembl
						self.biosettings.regions.AddEntrezStub(geneID, accID, proteinAcc)
			linecount+=1
		print "%s - %s" % (validGeneCount, filename)

	def ParseGeneinfo(self, geneinfo):
		linecount 					= 0
		for line in open(geneinfo):
			words					= line.split("\t")
			
			if len(words) > 0 and words[0].strip() == "9606":
				entrezID			= int(words[1])
				
				primaryName			= words[2]
				aliasList			= []
				if words[2] != "-":
					aliasList			= words[4].split("|")

				ensemblID			= ""
				hgnc				= 0
				foundHGNC			= "HGNC" in aliasList
				if words[5] != "-":
					alternateIDs		= words[5].lower().split("|")
					for id in alternateIDs:
						pieces			= id.split(":")
						if pieces[0] == "ensembl":
							ensemblID	= pieces[1].strip().upper()
						elif pieces[0] == "hgnc":
							hgnc		= pieces[1]
							foundHGNC	= True
				chromosome			= words[6]
				mapPosition			= words[7]
				desc				= words[8]
				
				#This should protect us from using names that aren't 
				if not self.biosettings.regions.UpdateEntrezGene(entrezID, primaryName, aliasList, ensemblID, hgnc, chromosome, mapPosition, desc):
					pass	#print " <->", line.strip()

	def DownloadUniprot(self, force=True):
		uniprotIDs					= self.FetchViaHTTP("ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/idmapping_selected.tab.gz")
		uniprotIDs					= self._ExtractGZ(uniprotIDs)
		count						= 0
		unidentifiables				= 0
		for line in open(uniprotIDs):
			words									= line.strip().split("\t")
			
			
			if len(words) > 13:
				if words[13] == "9606":
					uniID							= words[0]
					geneID							= 0
					
					geneIDs							= []
					geneNames						= [word.strip().replace("_HUMAN", "") for word in words[1].split(";")]
					entrezIDs						= [word.strip() for word in words[2]]
					errorMessage					= ""
					
					#if they don't have a direct mapping to entrez gene ID, then we can try to get there via their ensembl ID
					if len(entrezIDs) == 0:
						if len(geneNames) > 0:
							geneIDs					= self.biosettings.regions.AliasToGeneID(geneNames, 1300)
						errorMessage = "* No Entrez ID and gene name=%s" % (",".join(geneNames))
						#if len(words) > 19:
						#	ensIDs						= [id.strip().upper() for id in words[19].strip().split(";")]
						#	if len(ensIDs) == 1 and len(geneIDs) == 0:
						#		geneIDs				= self.biosettings.regions.AliasToGeneID([ensIDs[0].strip()], 1)
						#	if len(geneIDs) == 0:
						#		errorMessage += "\n* Ensembl IDs: %s" % (",".join(ensIDs))
								
						if len(geneIDs) == 0 and len(words) > 18:
							otherIDs				= [word.strip().split(".")[0] for word in words[18].strip().split(";")]
							if len(otherIDs) == 1:
								geneIDs				= self.biosettings.regions.AliasToGeneID(otherIDs)
							if len(geneIDs) == 0:
								errorMessage += "\n* Other Names: %s" % (",".join(otherIDs))
					else:
						geneIDs						= [int(word.strip()) for word in words[2].split(";")]
					#apparently a single protein can mapto more than one gene. At least, this can be observed in uniprot's website and can be observed in their flat files as well
					if len(geneIDs) > 0:
						for id in geneIDs:
							self.biosettings.regions.AddAlias(2000, uniID.strip(), geneID)
						count+=1
					else:
						#these are probably only identified by unigene or some other non-entrez database. 
						#when I look them up at ensembl, I am getting either nothing, or I get a non-human species. so, if they don't
						#have entrez nor ensembl, it seems safe to assume they aren't useful to us. 
						unidentifiables += 1
						print "Ambiguous Uniprot ID found: \t\t%s" % (line)
						print " -- Error: %s" % (errorMessage)
						#print "--> ", geneIDs
		print "%s Uniprot IDs found" % (count)
		print "%s Unidentifiable IDs found" % (unidentifiables)
	def LoadUniProt(self, force=True):
		localFilename				= self.FetchViaHTTP("ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_refseq_uniprotkb_collab.gz")
		localFilename				= self._ExtractGZ(localFilename)
		
		lineCount					= 0
		for line in open(localFilename):
			if lineCount > 0:
				words					= line.strip().split("\t")
				if len(words) > 0:
					protAccIDs			= [ word.strip() for word in  words[0].split(".")]
					uniProt				= words[1].strip()
					
					geneIDs				= self.biosettings.regions.AliasToGeneID(protAccIDs, 2)
					for geneID in geneIDs:	
						self.biosettings.regions.AddAlias(2000, uniProt, geneID)
			lineCount+=1

	def LoadGeneHistory(self, force=True):
		#os.system("rm -rf gene_history")

		localFilename 				= self.FetchViaHTTP("ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_history.gz")
		localFilename				= self._ExtractGZ(localFilename)
		
		historicMods		= 0
		lineCount					= 0
		for line in open(localFilename):
			if lineCount > 0:
				words = line.strip().split()
				
				#ignore any but humans
				if len(words) > 0 and words[0] == "9606":
					newID			= words[1]
					oldID			= int(words[2])
					self.biosettings.regions.AddEntrezHistory(oldID, newID)
					historicMods += 1
			lineCount+=1
		print "%s Historic Entrez Genes Identified" % (historicMods)
		
	def ParseGene2Ensembl(self, filename):
		linecount					= 0
		for line in open(filename):
			if linecount > 0:
				words = line.split("\t")
				if len(words)==7:
					taxID			= words[0]
					if taxID == "9606":
						geneID			= int(words[1])
						ensemblID		= words[2]
						self.biosettings.regions.AddEnsemblID(geneID, ensemblID)
			linecount+=1
	
	def LoadGenes(self):
		refseq		 				= self.FetchViaHTTP("ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz")
		refseq						= self._ExtractGZ(refseq)
		geneinfo					= self.FetchViaHTTP("ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz")
		geneinfo					= self._ExtractGZ(geneinfo)
		#gene2ensembl				= self.FetchViaHTTP("ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2ensembl.gz")
		#gene2ensembl				= self._ExtractGZ(gene2ensembl)
		print "Gene To Refseq: %s" % (refseq)
		print "Gene Info:      %s" % (geneinfo)
		self.ParseGene2Refseq(refseq)
		self.ParseGeneinfo(geneinfo)
		#self.ParseGene2Ensembl(gene2ensembl)
	def LoadSnpMerge(self):
		arch = "ftp://ftp.ncbi.nih.gov/snp/database/organism_data/human_9606/RsMergeArch.bcp.gz"
		arch						= self.FetchViaHTTP(arch)
		arch						= self._ExtractGZ(arch)
		#history files represent snps that are "history". At this point, we really don't care if they are expired. 
		#history = "ftp://ftp.ncbi.nih.gov/snp/database/organism_data/human_9606/SNPHistory.bcp.gz"
		#history						= self.fetchViaHTTP(history)
		#history						= self._ExtractGZ(history)
		cur							= self.biosettings.db.cursor()
		for line in open(arch):
			words 					= line.split("\t")
			if len(words) > 5:
				#for now, we don't really care if a SNP is dead....
				cur.execute("INSERT INTO rs_merged VALUES (?, ?, ?, ?, ?)", (words[0].strip(), words[1].strip(), words[2].strip(), words[6].strip(), False))
		self.biosettings.db.commit()	
		
	def LoadChromosome(self, chrom, dest, snpLog, roles):
		chrFile 					= self.FetchViaHTTP("ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/chr_rpts/chr_%s.txt.gz" % chrom)
		chrFile						= self._ExtractGZ(chrFile)
		print chrFile
		file 						= open(chrFile)
		snps						= []
		maxPosition					= 0
		unvalidatedSNPs				= 0
		for line in file:
			line = line.strip()
			words = line.split("\t")
			if len(words) >= 20:
				if words[-1] == "GRCh37":
					if int(words[16]) > 0:
						rsid			= words[0]
						position		= words[11]
						
						#print "rs%s %s \t%s" % (rsid, position, ",".join(words))
						if position.strip() != "":
							snps.append(Variant(rsid, position))
							if int(position) > maxPosition:
								maxPosition = int(position)
					else:
						unvalidatedSNPs += 1
				#else:
					#print "Skipping %s\t-\t%s" % (words[-1], words)
			#else:
				#print "Line too short, skipping: ", line
		if (len(chrom) > 1):
			dest.write(chrom[:2])
		else:
			dest.write(chrom[0])
			dest.write(' ')
		
		dest.write(struct.pack('II', len(snps), maxPosition))
		withRoles					= 0
		withoutRoles				= 0
		for snp in snps:
			roleID					= '0'						#self.GetRoleID(row[2]);
			if snp.pos in roles:
				roleID				= roles[snp.pos][1]
				if int(roles[snp.pos][0]) != int(snp.rsid):
					withoutRoles += 1
					print "Ensembl and RefSEQ out of date: %s:%s ->  ( rs%s , rs%s )"% (chrom, snp.pos, snp.rsid, roles[snp.pos][0])
				else:
					withRoles += 1
			dest.write(struct.pack('III', int(snp.rsid), int(snp.pos), int(roleID)))
			print>>snpLog, "%s\t%s\t%s\t%s" % (chrom, snp.rsid, snp.pos, roleID)

		print>>sys.stderr, "%s SNPs (%s not in ensembl) added for chromosome: %s (%s unvalidated SNPs were ignored)" % (withRoles, withoutRoles, chrom, unvalidatedSNPs)
		#for role in self.roles:
		#	c.execute("INSERT INTO snp_role (id, role) VALUES (?,?)", (self.roles[role], role))


	def UpdateGenes(self, ensembl, chromosomes):
		#now the genes
		#self.OpenFTP("ftp.ncbi.nlm.nih.gov")
		#self.LoadGeneHistory()
		self.LoadGenes()
		
		ensembl.LoadRegionsFromEnsembl(chromosomes)

		
		#self.LoadUniProt()
		#self.DownloadUniprot()
		self.biosettings.regions.Commit(self.biosettings.db)
		
	def UpdateSNPs(self, chromosomes, filename, ensembl):
		self.OpenFTP("ftp.ncbi.nih.gov")
		v						= int(time.time())
		filename 				= "%s.%s" % (filename, v)
		print "Creating Variations File: %s" % (filename)
		self.biosettings.SetVersion("variations", filename)
		variations 				= open(filename, "wb")
		snpLog					= open("%s.txt" % (filename), "w")
		responses				= self.ListFtpFiles("snp/organisms/human_9606/*human_96*")
		filename				= responses[0]
		
		variations.write(struct.pack('I', v))

		v 						= filename.split("/")[-1].split(".")[0][18:len(filename)-4]
		self.biosettings.SetVersion("ncbi", v)

		print "--%s\nVersion: %s" % (filename.split("/")[0], v)
		#Load up the SNP data
		for chrom in chromosomes:
			snpRoles			= ensembl.LoadSnpRoles(chrom)
			self.LoadChromosome(chrom, variations, snpLog, snpRoles)
		ensembl.CommitRoles()
		variations.close()
		self.LoadSnpMerge()
		
if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
	biodb 					= settings.InitDB(filename)
	os.chdir("NCBI")
	#ensembl					= EnsemblLoader(biodb, 2)
	#ensembl.ConnectToEnsemblDB()
	#chromosomes = ('Y','MT')
	chromosomes = ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT')
	#variations 				= biodb.BuildDbFilename("bio-settings", "var")

	
	
	ensembl					= load_ensembl.EnsemblLoader(biodb, 2)
	ensembl.ConnectToEnsemblDB()
	ncbiLoader				= NCBI_Loader(biodb)
	ncbiLoader.UpdateSNPs(chromosomes, "variations", ensembl)
	ncbiLoader.UpdateGenes(ensembl, chromosomes)

		

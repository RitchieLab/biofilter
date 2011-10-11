#!/usr/bin/env python


'''
Created on May 14, 2010

@author: torstees
'''

import os, time, struct, sys
import bioloader, settings
import biosettings, subprocess
import csv

#http://lethain.com/entry/2009/jan/22/handling-very-large-csv-and-xml-files-in-python/
csv.field_size_limit(1000000000)				#Required to ensure that the long lines don't cause an error

globalIDs = dict()								# Converts familiar names with integer IDs

relationshipsFile					= open("observed_relationships.csv", "w")

class GkbEntity:
	def __init__(self, biosettings, type):
		self.biosettings			= biosettings
		self.associations			= dict()			# Class => objects
		self.id						= -1
		self.alreadyCommitted	= []
		self.name					= ""
		self.acc						= ""
		self.type					= type

	def GetID(self):
		if self.id == -1:
			self.id = biosettings.NextID()
		return self.id

	def CommitToDB(self, kb, parentGroup):
		if kb.groupTypeID not in self.alreadyCommitted:
			self.alreadyCommitted.append(kb.groupTypeID)
			curPathway 			= bioloader.Pathway(kb.groupTypeID, self.GetID(), self.acc, "%s (%s)" % (self.name, self.type))
			parentGroup.AddAssociation(curPathway.groupID, "")
			#kb.AddPathway(curPathway)
			#curPathway.AssociatePathways(group_id, self.id, self.type)

			if "Drug" in self.associations:
				for item in self.associations["Drug"]:
					#print item
					#print >> sys.stderr, kb.groupTypeID, "-> ", item.name
					curPathway.AddAssociation(item.GetID(), "Drug")
					item.CommitToDB(kb, curPathway)

			if "Disease" in self.associations:
				for item in self.associations["Disease"]:
					#print >> sys.stderr, kb.groupTypeID, "-> ", item.name
					curPathway.AddAssociation(item.GetID(), "Disease")
					item.CommitToDB(kb, curPathway)

			if "Gene" in self.associations:
				#print "Saving genes to the database, %s in total" % (len(self.associations["Gene"]))
				for item in self.associations["Gene"]:
					#print >> sys.stderr, curPathway.groupID, " + ", item.name
					item.AddToPathway(curPathway)
					
			curPathway.Commit(self.biosettings)


	def AssociateOther(self, other):
		if self.id > 0 and other.id > 0 and self.id != other.id:
			sType = (other.__class__.__name__).split(".")[-1]
			if sType == "Gene":
				print >> relationshipsFile, "%s,%s,%s,%s" % (self.id, other.id, self.name, other.name)
			if sType not in self.associations:
				self.associations[sType] = []
			self.associations[sType].append(other)


class Gene(GkbEntity):
	def __init__(self, biosettings):
		GkbEntity.__init__(self, biosettings, "Gene")
		self.acc			= ""
		self.entrez		= ""
		self.ensID		= ""
		self.unip		= ""
		self.symbol		= ""

	#We are assuming that genes were taken care of already
	def Commit(self, kb, group_id):
		pass

	def AddToPathway(self, pathway):
		if self.id > 0:
			pathway.AddGene(self.id)
	
	def ParseLine(self, words):
		self.acc		= words[0]
		self.entrez		= words[1]
		self.ensID		= words[2]
		self.unip		= words[3]
		self.name		= words[4]
		self.symbol		= words[5]

		self.id			= -1

		if len(self.entrez) > 0:
			print "Looking for Entrez: ", self.entrez
			aliases		= self.biosettings.regions.AliasToGeneID([self.entrez])
			if len(aliases) > 0:
				self.id		= aliases[0]
		elif len(self.symbol) > 0:
			print "Looking for Symbol: ", self.symbol
			aliases		= self.biosettings.regions.AliasToGeneID([self.symbol])
			if len(aliases) > 0:
				self.id     = aliases[0]
		else:
			sys.exit("Empty entrez gene ID. Other options include: %s " % (",".join(words[0:6])))

		print >> relationshipsFile, "%s, %s, %s, %s [%s] %s" % (self.id, self.acc, self.name, self.symbol, aliases, self.entrez)
class Drug(GkbEntity):
	def __init__(self, biosettings):
		GkbEntity.__init__(self, biosettings, "Drug")
		self.acc			= ""
		self.altName	= ""
		self.drugtype	= ""
		self.genes		= []
		self.drugs		= []
		self.diseases	= []

	def ParseLine(self, words):
		self.acc		= words[0]
		self.name		= words[1]
		self.altName	= words[2]
		self.drugtype	= words[3]
		self.id			= self.biosettings.NextID()

	def Commit(self, kb, parentGroup):
		self.CommitToDB(kb, parentGroup)

class Disease(GkbEntity):
	def __init__(self, biosettings):
		GkbEntity.__init__(self, biosettings, "Disease")
		self.acc			= ""
		self.altNames	= []

	def ParseLine(self, words):
		self.acc		= words[0]
		self.name		= words[1]

		#print >> sys.stderr, words[2]
		for row in csv.reader(words[2], delimiter=",", quotechar='"'):
			#print row
			self.altNames.append(" ".join(row))

		#print>>sys.stderr, "\t", "\n\t".join(self.altNames)
		self.id				= self.biosettings.NextID()

	def Commit(self, kb, parentGroup):
		self.CommitToDB(kb, parentGroup)

class PharmGKBLoader(bioloader.BioLoader):
	def __init__(self, biosettings, id=15):
		bioloader.BioLoader.__init__(self, biosettings, id)

		self.biosettings.PurgeGroupData(id)

		biosettings.LoadAliases()
		self.kb						= bioloader.KnowledgeBase(id, "PharmGKB", "Pharmacogenomics Knowledge Base")
		self.pathwayGroups		= bioloader.Pathway(self.kb.groupTypeID, id+1, "Pathways", "PharmGKB Pathways")
		self.kb.AddPathway(self.pathwayGroups, "PharmGKB Pathway")
		self.pathways				= []
		#self.diseaseGroups		= bioloader.KnowledgeBase(id+1, "PharmGKB-Diseases", "Pharmacogenomics Knowledge Base (Disease)")
		self.diseaseGroups		= bioloader.Pathway(self.kb.groupTypeID, id+2, "Diseases", "PharmGKB Disease Associations")
		self.kb.AddPathway(self.diseaseGroups, "PharmGKB Disease")
		#self.drugGroups			= bioloader.KnowledgeBase(id+2, "PharmGKB-Drugs", "Pharmacogenomics Knowledge Base (Drug)")
		self.drugGroups			= bioloader.Pathway(self.kb.groupTypeID, id+3, "Drugs", "PharmGKB Drug Associations")
		self.kb.AddPathway(self.drugGroups, "PharmGKB Drug")
		self.gkbentities			= dict()
		self.gkbentities["Gene"]	= dict()
		self.gkbentities["Drug"]	= dict()
		self.gkbentities["Disease"]= dict()


	def GetItem(self, identifier):
		type, acc = identifier.split(":")
		return self.gkbentities[type][acc]


	def DontDownloadFiles(self, filenames):
		files = dict()

		files["pathways-tsv.zip"]		= "archives/pathways.tsv"
		files["diseases.zip"]			= "archives/diseases.tsv"
		files["genes.zip"]				= "archives/genes.tsv"
		files["drugs.zip"]				= "archives/drugs.tsv"
		files["relationships.zip"]		= "archives/relationships.tsv"

		return files
	
	def DownloadFiles(self, filenames):
		files = dict()
		os.system("mkdir -p archives")
		for file in filenames:
			archive = self.FetchViaHTTP("http://www.pharmgkb.org/commonFileDownload.action?filename=%s" % (file))
			#print "The file we are trying to get..", file, archive
			output = subprocess.Popen(["unzip", "-o", "%s" % archive], stdout=subprocess.PIPE).communicate()[0]
			#print >> sys.stderr, output
			os.system("mv %s archives/" % (archive))
			print output
			files[file] = output.split("\n")[1].split(":")[1].strip()
		return files

	def LoadDrugData(self, filename):
		print >> sys.stderr, filename
		reader = csv.reader(open(filename), delimiter='\t', quotechar="'")
		for words in reader:
			if words[0] not in ["PharmGKB Accession Id"]:
				drug = Drug(self.biosettings)
				drug.ParseLine(words)
				self.gkbentities["Drug"][drug.acc] = drug
		os.system("mv %s archives" % (filename))

	def LoadDiseaseData(self, filename):
		reader = csv.reader(open(filename), delimiter='\t', quotechar="'")
		for words in reader:
			if words[0] not in ["PharmGKB Accession Id"]:
				#print >> sys.stderr, words
				disease = Disease(self.biosettings)
				disease.ParseLine(words)
				self.gkbentities["Disease"][disease.acc] = disease
		print "%s Diseases Loaded." % (len(self.gkbentities["Disease"]))
		os.system("mv %s archives" % (filename))

	def LoadGeneData(self, filename):
		reader = csv.reader(open(filename), delimiter='\t', quotechar="'")
		for words in reader:
			if words[0] not in ["PharmGKB Accession Id"]:
				gene = Gene(self.biosettings)
				gene.ParseLine(words)
				self.gkbentities["Gene"][gene.acc] = gene
		os.system("mv %s archives" % (filename))


	def LoadRelationships(self, filename):
		for line in open(filename):
			words				= line.split("\t")
			if words[0] not in ["Entity1_id"]:
				try:
					left				= self.GetItem(words[0])
					right				= self.GetItem(words[2])
					left.AssociateOther(right)
					right.AssociateOther(left)
				except:
					print "Missing data: ", line
	def Load(self, force=True):
		#os.system("rm -rf pharmgkb")
		cwd 					= os.getcwd()
		os.system("mkdir -p pharmgkb")
		os.chdir("pharmgkb")


		files = self.DownloadFiles(["pathways-tsv.zip", "genes.zip", "diseases.zip", "drugs.zip", "relationships.zip"])
		#files = self.DontDownloadFiles(["pathways-tsv.zip", "genes.zip", "diseases.zip", "drugs.zip", "relationships.zip"])
		#localFilename = self.FetchViaHTTP("http://www.pharmgkb.org/commonFileDownload.action?filename=pathways-tsv.zip")

		#os.system("unzip %s" % (localFilename))
		
		dataFiles					= []
		filename					= "pathways.tsv"

		#if force or self.CheckTimestampAgainstServer(timestamp, self.groupID):
		self.biosettings.PurgeGroupData(self.groupID)
		#self.biosettings.CommitGroup(self.groupID, "PharmGKB", "PharmGKB", time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		self.LoadPathways(files["pathways-tsv.zip"])

		#Disease Related
		#self.biosettings.PurgeGroupData(self.diseaseGroups.groupTypeID)
		#Drug Related
		#self.biosettings.PurgeGroupData(self.drugGroups.groupTypeID)

		self.LoadGeneData(files["genes.zip"])
		self.LoadDrugData(files["drugs.zip"])
		self.LoadDiseaseData(files["diseases.zip"])

		self.LoadRelationships(files["relationships.zip"])

		os.chdir(cwd)

	def LoadPathways(self, filename):
		curPathway = None
		for line in open(filename):
			words 					= line.split(":")
			if len(words) > 1:
				newID					= self.biosettings.NextID()
				if curPathway:
					self.pathways.append(curPathway)
				curPathway 			= bioloader.Pathway(self.groupID, newID, words[0].strip(), words[1].strip())
				self.pathwayGroups.AddAssociation(curPathway.groupID, "Pathway")
				#self.kb.AssociatePathways(self.groupID, newID, "Member")
			else:
				words					= line.split()
				if len(words) > 1 and words[0] == "Gene":
					geneIDs			= self.biosettings.regions.AliasToGeneID([words[2].strip()])
					if len(geneIDs) == 1:
						curPathway.AddGene(geneIDs[0])

		if curPathway:
			self.pathways.append(curPathway)
		os.system("mv %s archives" % (filename))
	def Commit(self):
		timestamp					= time.localtime(os.path.getmtime("pharmgkb/archives/pathways.tsv"))
		self.kb.Commit(self.biosettings, time.strftime("%Y-%M-%d %H:%M:%S", timestamp))
		for pathway in self.pathways:
			pathway.Commit(self.biosettings)
		self.pathwayGroups.Commit(self.biosettings)

		for disease in self.gkbentities["Disease"]:
			self.gkbentities["Disease"][disease].Commit(self.kb, self.diseaseGroups)
		self.diseaseGroups.Commit(self.biosettings)

		for drug in self.gkbentities["Drug"]:
			self.gkbentities["Drug"][drug].Commit(self.kb, self.drugGroups)
		self.drugGroups.Commit(self.biosettings)
		self.biosettings.db.commit()
if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename						= sys.argv[1]


	bioDB								= biosettings.BioSettings(filename)
	bioDB.OpenDB()
	print "Initializing PharmGKB"
	loader							= PharmGKBLoader(bioDB)
	print "Loading data"
	loader.Load(False)
	print "Load Completed"
	loader.Commit()
	print "Commit Completed"
	bioDB.Commit()
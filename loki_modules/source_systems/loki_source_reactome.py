#!/usr/bin/env python

import collections
import itertools
import zipfile
from loki_modules import loki_source


class Source_reactome(loki_source.Source):
	
	
	@classmethod
	def getVersionString(cls):
		return '2.1 (2015-01-23)'
	#getVersionString()
	
	
	def download(self, options, path):
		# download the latest source files
		self.downloadFilesFromHTTP('www.reactome.org', {
			path+'/ReactomePathways.txt'                        : '/download/current/ReactomePathways.txt',
			path+'/ReactomePathwaysRelation.txt'                : '/download/current/ReactomePathwaysRelation.txt',
			path+'/ReactomePathways.gmt.zip'                    : '/download/current/ReactomePathways.gmt.zip',
			path+'/UniProt2Reactome.txt'                        : '/download/current/UniProt2Reactome.txt',
			path+'/Ensembl2Reactome.txt'                        : '/download/current/Ensembl2Reactome.txt',
		#	path+'/homo_sapiens.interactions.txt.gz'            : '/download/current/homo_sapiens.interactions.txt.gz',
		#	path+'/gene_association.reactome'                   : '/download/current/gene_association.reactome',
		})

		return [
			path+'/ReactomePathways.txt',
			path+'/ReactomePathwaysRelation.txt',
			path+'/ReactomePathways.gmt.zip',
			path+'/UniProt2Reactome.txt',
			path+'/Ensembl2Reactome.txt'
		]
	#download()
	
	
	def update(self, options, path):
		# clear out all old data from this source
		self.log("deleting old records from the database ...\n")
		self.deleteAll()
		self.log("deleting old records from the database completed\n")
		
		# get or create the required metadata records
		namespaceID = self.addNamespaces([
			('symbol',       0),
			('entrez_gid',   0),
			('ensembl_gid',  0),
			('ensembl_pid',  1),
			('uniprot_pid',  1),
			('pathway',      0),
			('reactome_id',  0),
		])
		relationshipID = self.addRelationships([
			('',),
		])
		typeID = self.addTypes([
			('gene',),
			('pathway',),
		])
		# subtypeID = self.addSubtypes([
		# 	('-',),
		# ])
		
		# initialize storage
		numPath = 0
		reactPath = dict()
		pathReact = dict()
		listRelationships = list()
		numAssoc = 0
		nsAssoc = {
			'symbol'      : { 'path':set(), 'react':set() },
			'entrez_gid'  : { 'path':set(), 'react':set() },
			'ensembl_gid' : { 'path':set(), 'react':set() },
			'ensembl_pid' : { 'path':set(), 'react':set() },
			'uniprot_pid' : { 'path':set(), 'react':set() },
		}
		
		# process pathways
		# <react_id>\t<description>\t<species>
		self.log("processing pathways ...\n")
		numNewPath = 0
		numMismatch = 0
		with open(path+'/ReactomePathways.txt', 'r') as pathFile:
			# no header
			for line in pathFile:
				words = line.rstrip().split("\t")
				if line.startswith('#') or (len(words) < 3) or (words[2] != "Homo sapiens"):
					continue
				reactID = words[0]
				pathway = words[1]
				
				if reactID not in reactPath:
					numNewPath += 1
					reactPath[reactID] = pathway
					pathReact[pathway] = reactID
				elif reactPath[reactID] != pathway:
					numMismatch += 1
			#for line in pathFile
		#with pathFile
		self.log("processing pathways completed: %d pathways (%d mismatches)\n" % (numNewPath,numMismatch))
		numPath += numNewPath
		
		# process pathway relationships
		# <parent>\t<child>
		self.log("processing pathway hierarchy ...\n")
		numRelations = 0
		with open(path+'/ReactomePathwaysRelation.txt', 'r') as relFile:
			# no header
			for line in relFile:
				words = line.rstrip().split("\t")
				if line.startswith('#') or (len(words) < 2):
					continue
				
				numRelations += 1
				listRelationships.append( (words[0],words[1]) )
		#with relFile
		self.log("processing pathway hierarchy completed: %d relationships\n" % (numRelations,))
		
		# process gene sets
		# <description>\t"Reactome Pathway"\t<symbol1>\t<symbol2>...
		self.log("verifying gene set archive ...\n")
		numNewPath = 0
		numNewAssoc = 0
		with zipfile.ZipFile(path+'/ReactomePathways.gmt.zip','r') as geneZip:
			err = geneZip.testzip()
			if err:
				self.log(" ERROR\n")
				raise Exception("CRC failed for %s\n" % err)
			self.log("verifying gene set archive completed\n")
			self.log("processing gene sets ...\n")
			for info in geneZip.infolist():
				# there should be only one file in the archive, but just in case..
				if info.filename == 'ReactomePathways.gmt':
					geneFile = geneZip.open(info,'r')
					for line in geneFile:
						words = line.decode('latin-1').rstrip().split("\t")
						if line.decode().startswith('#') or (len(words) < 3) or (words[1] != "Reactome Pathway"):
							continue
						pathway = words[0]
						
						if pathway not in pathReact:
							numPath += 1
							numNewPath += 1
							reactID = "REACT_unknown_%d" % (numPath,)
							pathReact[pathway] = reactID
							reactPath[reactID] = pathway
						
						for n in range(2, len(words)):
							numAssoc += 1
							numNewAssoc += 1
							nsAssoc['symbol']['path'].add( (pathway,numAssoc,words[n]) )
						#foreach gene symbol
					#foreach line in geneFile
					geneFile.close()
				#if file ok
			#foreach file in geneZip
			self.log("processing gene sets completed: %d associations (%d new pathways)\n" % (numNewAssoc,numNewPath))
		#with geneZip
		
		# TODO: ChEBI or miRBase mappings?
		
		# process ensembl mappings (to lowest reactome pathway, not parents)
		# http://www.reactome.org/download/mapping.README.txt
		# <mapID>\t<reactID>\t<url>\t<pathway>\t<evidence>\t<species>
		self.log("processing ensembl associations ...\n")
		numNewPath = 0
		numMismatch = 0
		numNewAssoc = 0
		with open(path+'/Ensembl2Reactome.txt', 'r') as assocFile:
			for line in assocFile:
				words = line.rstrip().split("\t")
				if line.startswith('#') or (len(words) < 6) or (words[5] != "Homo sapiens"):
					continue
				ensemblID = words[0]
				reactID = words[1]
				pathway = words[3]
				
				if ensemblID.startswith('ENSG'):
					ns = 'ensembl_gid'
				elif ensemblID.startswith('ENSP'):
					ns = 'ensembl_pid'
				else:
					continue
				
				if reactID not in reactPath:
					numPath += 1
					numNewPath += 1
					reactPath[reactID] = pathway
					pathReact[pathway] = reactID
				elif reactPath[reactID] != pathway:
					numMismatch += 1
					continue
				
				numAssoc += 1
				numNewAssoc += 1
				nsAssoc[ns]['path'].add( (pathway,numAssoc,ensemblID) )
			#foreach line in assocFile
		#with assocFile
		self.log("processing ensembl associations completed: %d associations (%d new pathways, %d mismatches)\n" % (numNewAssoc,numNewPath,numMismatch))
		
		# process uniprot mappings (to lowest reactome pathway, not parents)
		# http://www.reactome.org/download/mapping.README.txt
		# <mapID>\t<reactID>\t<url>\t<pathway>\t<evidence>\t<species>
		self.log("processing uniprot associations ...\n")
		numNewPath = 0
		numMismatch = 0
		numNewAssoc = 0
		with open(path+'/UniProt2Reactome.txt', 'r') as assocFile:
			for line in assocFile:
				words = line.rstrip().split("\t")
				if line.startswith('#') or (len(words) < 6) or (words[5] != "Homo sapiens"):
					continue
				uniprotPID = words[0]
				reactID = words[1]
				pathway = words[3]
				
				if reactID not in reactPath:
					numPath += 1
					numNewPath += 1
					reactPath[reactID] = pathway
					pathReact[pathway] = reactID
				elif reactPath[reactID] != pathway:
					numMismatch += 1
					continue
				
				numAssoc += 1
				numNewAssoc += 1
				nsAssoc['uniprot_pid']['path'].add( (pathway,numAssoc,uniprotPID) )
			#foreach line in assocFile
		#with assocFile
		self.log("processing uniprot associations completed: %d associations (%d new pathways, %d mismatches)\n" % (numNewAssoc,numNewPath,numMismatch))
		numPath += numNewPath
		numAssoc += numNewAssoc
		
		# TODO: process interaction associations?
		
		if False:
			tally = collections.defaultdict(int)
			# http://www.reactome.org/download/interactions.README.txt
			# <uniprot>\t<ensembl>\t<entrez>\t<uniprot>\t<ensembl>\t<entrez>\t<reacttype>\t<reactID>["<->"<reactID>]\t<pubmedIDs>
			self.log("processing protein interactions ...\n")
			numNewPath = 0
			numNewAssoc = 0
			iaFile = self.zfile(path+'/homo_sapiens.interactions.txt.gz') #TODO:context manager,iterator
			for line in iaFile:
				words = line.decode('latin-1').rstrip().split("\t")
				if line.decode().startswith('#') or (len(words) < 8):
					continue
				uniprotP1 = words[0][8:]  if words[0].startswith('UniProt:')     else None
				ensemblG1 = words[1][8:]  if words[1].startswith('ENSEMBL:ENSG') else None
				ensemblP1 = words[1][8:]  if words[1].startswith('ENSEMBL:ENSP') else None
				entrezG1  = words[2][12:] if words[2].startswith('Entrez Gene:') else None
				uniprotP2 = words[3][8:]  if words[3].startswith('UniProt:')     else None
				ensemblG2 = words[4][8:]  if words[4].startswith('ENSEMBL:ENSG') else None
				ensemblP2 = words[4][8:]  if words[4].startswith('ENSEMBL:ENSP') else None
				entrezG2  = words[5][12:] if words[5].startswith('Entrez Gene:') else None
				reacttype = words[6]
				reactIDs = words[7].split('<->')
				reactID1 = reactIDs[0].split('.',1)[0]
				reactID2 = reactIDs[1].split('.',1)[0] if (len(reactIDs) > 1) else None
				reactID2 = reactID2 if (reactID2 != reactID1) else None
				
				# if reacttype is "direct_complex" or "indirect_complex",
				# the interactors are in the same group (or supergroup)
				# and only one REACTOME pathway will be given in column 8;
				# if reacttype is "reaction" or "neighbouring_reaction",
				# they are not in the same group but interact anyway, but
				# there will still be only one pathway named in column 8
				# for "reaction" (and two for "neighboring_reaction")
				
				if (not reactID1):
					tally['no react 1'] += 1
				elif (reactID1 not in reactPath):
					tally['no such react 1'] += 1
				else:
					in1 = of1 = 0
					if (uniprotP1):
						in1 += max(((alias == uniprotP1) and (pathway == reactPath[reactID1])) for pathway,_,alias in nsAssoc['uniprot_pid']['path'])
						of1 += 1
					if (ensemblG1):
						in1 += max(((alias == ensemblG1) and (pathway == reactPath[reactID1])) for pathway,_,alias in nsAssoc['ensembl_gid']['path'])
						of1 += 1
					if (ensemblP1):
						in1 += max(((alias == ensemblP1) and (pathway == reactPath[reactID1])) for pathway,_,alias in nsAssoc['ensembl_pid']['path'])
						of1 += 1
					tally['%d/%d protein 1 in react 1' % (in1,of1)] += 1
					
					in2 = of2 = 0
					if (uniprotP2):
						in2 += max(((alias == uniprotP2) and (pathway == reactPath[reactID1])) for pathway,_,alias in nsAssoc['uniprot_pid']['path'])
						of2 += 1
					if (ensemblG2):
						in2 += max(((alias == ensemblG2) and (pathway == reactPath[reactID1])) for pathway,_,alias in nsAssoc['ensembl_gid']['path'])
						of2 += 1
					if (ensemblP2):
						in2 += max(((alias == ensemblP2) and (pathway == reactPath[reactID1])) for pathway,_,alias in nsAssoc['ensembl_pid']['path'])
						of2 += 1
					tally['%d/%d protein 2 in react 1' % (in2,of2)] += 1
				#if reactID1
				
				if (not reactID2):
					tally['no react 2'] += 1
				elif (reactID2 not in reactPath):
					tally['no such react 2'] += 1
				else:
					in1 = of1 = 0
					if (uniprotP1):
						in1 += max(((alias == uniprotP1) and (pathway == reactPath[reactID2])) for pathway,_,alias in nsAssoc['uniprot_pid']['path'])
						of1 += 1
					if (ensemblG1):
						in1 += max(((alias == ensemblG1) and (pathway == reactPath[reactID2])) for pathway,_,alias in nsAssoc['ensembl_gid']['path'])
						of1 += 1
					if (ensemblP1):
						in1 += max(((alias == ensemblP1) and (pathway == reactPath[reactID2])) for pathway,_,alias in nsAssoc['ensembl_pid']['path'])
						of1 += 1
					tally['%d/%d protein 1 in react 1' % (in1,of1)] += 1
					
					in2 = of2 = 0
					if (uniprotP2):
						in2 += max(((alias == uniprotP2) and (pathway == reactPath[reactID2])) for pathway,_,alias in nsAssoc['uniprot_pid']['path'])
						of2 += 1
					if (ensemblG2):
						in2 += max(((alias == ensemblG2) and (pathway == reactPath[reactID2])) for pathway,_,alias in nsAssoc['ensembl_gid']['path'])
						of2 += 1
					if (ensemblP2):
						in2 += max(((alias == ensemblP2) and (pathway == reactPath[reactID2])) for pathway,_,alias in nsAssoc['ensembl_pid']['path'])
						of2 += 1
					tally['%d/%d protein 2 in react 1' % (in2,of2)] += 1
				#if reactID1
			#foreach line in iaFile
			self.log("processing protein interactions completed: %d associations (%d new pathways)\n" % (numNewAssoc,numNewPath))
			numPath += numNewPath
			numAssoc += numNewAssoc
			for k,v in tally.items():
				print(k, v)
		#TODO
		
		# store pathways
		self.log("writing pathways to the database ...\n")
		listReact = list(reactPath.keys())
		listGID = self.addTypedGroups(typeID['pathway'], ((reactID, reactPath[reactID]) for reactID in listReact))
		reactGID = dict(zip(listReact, listGID))
		self.log("writing pathways to the database completed\n")
		
		# store pathway names
		self.log("writing pathway names to the database ...\n")
		self.addGroupNamespacedNames(namespaceID['reactome_id'], ((gid,reactID) for reactID,gid in reactGID.items()))
		self.addGroupNamespacedNames(namespaceID['pathway'], ((gid,reactPath[reactID]) for reactID,gid in reactGID.items()))
		self.log("writing pathway names to the database completed\n")
		
		# store pathway relationships
		self.log("writing pathway relationships to the database ...\n")
		self.addGroupParentRelationships( (reactGID[parentID],reactGID[childID],relationshipID['']) for parentID,childID in listRelationships if ((parentID in reactGID) and (childID in reactGID)) )
		self.log("writing pathway relationships to the database completed\n")
		
		# store gene associations
		self.log("writing gene associations to the database ...\n")
		for ns in nsAssoc:
			self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID[ns], ((reactGID[reactID],num,name) for reactID,num,name in nsAssoc[ns]['react']))
			self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID[ns], ((reactGID[pathReact[path]],num,name) for path,num,name in nsAssoc[ns]['path']))
		self.log("writing gene associations to the database completed\n")
	#update()
	
#Source_reactome
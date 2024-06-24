#!/usr/bin/env python

import collections
import re
import apsw
from sh import gunzip
from loki import loki_source


class Source_disgenet(loki_source.Source):
        
        
        @classmethod
        def getVersionString(cls):
                return '1.0 (2023-08-08)'
        #getVersionString()
        
        
        def download(self, options):
                # download the latest source files
                self.downloadFilesFromHTTP('disgenet.org', {
                        'disgenet_2020.db.gz': '/static/disgenet_ap1/files/sqlite_downloads/current/disgenet_2020.db.gz',          
                })
        #download()
        
        
        def update(self, options):
                # clear out all old data from this source
                self.log("deleting old records from the database ...")
                self.deleteAll()
                self.log(" OK\n")
                
                # get or create the required metadata records
                namespaceID = self.addNamespaces([
			('disgenet_id',  0),
                        ('entrez_gid', 0),
                        ('disease', 0)
                ])
                typeID = self.addTypes([
			('disease',),
                        ('gene',),
		])
                subtypeID = self.addSubtypes([
			('-',),
		])
                
                # process disgenet sqlite file
                self.log("processing diseases ...")
                gunzip('disgenet_2020.db.gz')
                diseases = {}
                diseaseClass = {}
                con = apsw.Connection('disgenet_2020.db')
                cur = con.cursor()
                comm = 'select diseaseClassNID,diseaseClassName from diseaseClass'
                cur.execute(comm)
                diseaseClass = {diseaseclass[0]:diseaseclass[1].strip() for diseaseclass in cur.fetchall()}
                comm = 'SELECT a.diseaseId,a.diseaseName,b.diseaseClassNID FROM diseaseAttributes a LEFT JOIN disease2class b ON a.diseaseNID=b.diseaseNID order by a.diseaseNID'
                cur.execute(comm)
                diseases = {disease[0]:[disease[1],disease[2]] for disease in cur.fetchall()}
		#foreach line in diseaseFile
                self.log(" OK: %d disease\n" % (len(diseases),))
                
                # store diseases
                self.log("writing diseases to the database ...")
                listSubtype = self.addSubtypes([(val,)for val in set(diseaseClass.values())])
                listGroup = diseases.keys()
                listAID = self.addTypedGroups(typeID['disease'], ((subtypeID['-'] if diseases[diseaseID][1] is None else listSubtype[diseaseClass[diseases[diseaseID][1]]],diseases[diseaseID][0],None) for diseaseID in listGroup))
                groupAID = dict(zip(listGroup,listAID))
                self.log(" OK\n")

                # store diseases names
                self.log("writing diseases names to the database ...")
                self.addGroupNamespacedNames(namespaceID['disgenet_id'], ((groupAID[diseaseID],diseaseID) for diseaseID in listGroup))
                self.addGroupNamespacedNames(namespaceID['disease'], ((groupAID[diseaseID],diseases[diseaseID][0]) for diseaseID in listGroup))
                diseases = None
                diseaseClass  = None
                self.log(" OK\n")

                # process disgenet disease identifiers
                self.log("processing diseases identifiers ...")                
                diseaseGene = set()
                comm = 'SELECT b.geneId,c.diseaseId FROM geneDiseaseNetwork a LEFT JOIN geneAttributes b ON a.geneNID=b.geneNID LEFT JOIN diseaseAttributes c ON a.diseaseNID=c.diseaseNID ORDER BY c.diseaseId'
                cur.execute(comm)
                diseaseGeneResult = cur.fetchall()
                con.close()
                numAssoc = 0
                for pair in diseaseGeneResult:
                        if pair[1] in listGroup:
                                numAssoc += 1
                                diseaseGene.add( (groupAID[pair[1]],numAssoc,pair[0]) )
                self.log(" OK: %d diseases and gene pairs\n" % (len(diseaseGene),))

                # store gaad disease identifiers
                self.log("writing diseases and gene pairs to the database ...")
                self.addGroupMemberTypedNamespacedNames(typeID['gene'], namespaceID['entrez_gid'], diseaseGene)
                diseaseGene = None
                self.log(" OK\n")

        #update()
        
#Source_go

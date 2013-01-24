#!/usr/bin/env python
'''
Created on May 7, 2010

@author: torstees
'''

import sys
import biosettings

ncbi			= "37a"				#NCBU Build associated with our SNP and gene information
hapmap			= 27				#Hapmap version from which we extracted our LD boundaries
ensembl			= 56				#Version of Ensembl being used
ensemblCoord	= 2					#Coordinate version used by ensembl

queryLimit		= 500000

chromosomes = ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT')


biodb			= None				#This should be instantiated in the Initialization function, which
									#should only be called once
									
def InitDB(filename = None):
	"""
	By default, it will reuse the database already loaded-however, it will instantiate a new one, 
	if there hasn't been one loaded so far.
	"""
	global biodb
	if biodb == None:
		biodb = biosettings.BioSettings(filename)
		biodb.InitDB()
	return biodb



if __name__ == '__main__':
	filename = None
	if len(sys.argv) > 1:
		filename 			= sys.argv[1]
	biodb 					= InitDB()
	cur 					= biodb.OpenDB()
	biodb.SetVersion("ncbi", ncbi)
	biodb.SetVersion("ensembl", ensembl)
	biodb.SetVersion("hapmap", hapmap)
	
	biodb.Commit()
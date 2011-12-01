'''
Created on May 7, 2010

@author: torstees
'''
import os, subprocess, time, ftplib
import cStringIO
import sys

skipFTP								= False


class KnowledgeBase:
	def __init__(self, groupTypeID, name, desc):
		self.groupTypeID			= groupTypeID
		self.name					= name
		self.desc					= desc
		self.pathways				= dict()				#pathway ID -> pathway
		os.system("mkdir -p %s" % (name))
		#os.chdir(name)
		self.rootPathway							= Pathway(self.groupTypeID, self.groupTypeID, self.name, self.desc)
		self.AddPathway(self.rootPathway)
	
	def __del__(self):
		pass
		#os.chdir("..")
	
	def AssociatePathways(self, parentID, childID, relationship):
		#print "AssociatePathways: %s x %s  -- %s" % (parentID, childID, relationship)
		self.pathways[parentID].AddAssociation(childID, relationship)
	
	def AddPathway(self, pathway, relationship=""):
		#print "AddPathway: %s" % (pathway.groupID)
		self.rootPathway.AddAssociation(pathway.groupID, relationship)
		#self.pathways[pathway.groupID] = pathway
	
	def Commit(self, biosettings, timestamp, roleID=1):
		#print "Committing KB"
		biosettings.CommitGroup(self.groupTypeID, roleID, self.name, self.desc, timestamp)
		self.rootPathway.Commit(biosettings, roleID)
		for pathway in self.pathways:
			self.pathways[pathway].Commit(biosettings)
		biosettings.Commit()


class Pathway:
	def __init__(self, groupTypeID, groupID, name, desc):
		self.groupTypeID 			= groupTypeID
		self.groupID				= groupID
		self.name					= name
		self.desc					= desc					
		self.genes					= set()					#geneID
		self.children				= dict()				#child ID -> relationship
	
	def AddGene(self, geneID):
		#print>>sys.stderr,  geneID, self.genes
		self.genes.add(geneID)
	
	def Commit(self, biosettings, roleID = 1):
		biosettings.CommitPathway(self.groupTypeID, self.groupID, self.name, self.desc)
		#for gene in self.genes:
		#	biosettings.AssociateGene(self.groupID, gene)
		for child in self.children:
			biosettings.RelatePathways(self.groupID, child, self.children[child], "")
		for child in self.genes:
			biosettings.AssociateGene(self.groupID, child)
	
	def AddAssociation(self, childID, relationship):
		#print "AddAssociation(%s,%s)"%(childID, relationship)
		self.children[childID] 		= relationship


class BioLoader:
	'''
	classdocs
	'''
	
	def __init__(self, biosettings, groupID = 0, localSubdir= "."):
		"""Most group related things will assume that groupID is not zero. If it is, they should be skipped"""
		self.ftp				= None		#Local FTP Server, in case we want our classes downloading via FTP
		self.biosettings		= biosettings
		self.groupID			= groupID
		self.localdir			= localSubdir
		os.system("mkdir -p %s" % localSubdir)
		self.localFiles			= []
	
	def InitLog(self, logfilename = None):
		if logfilename == None:
			logfilename = "%s.log"% (self.__module__)
		self.std				= sys.stdout
		self.sck				= open(logfilename, 'w')
		print>>sys.stderr, "Log created: %s" % (logfilename)
		sys.stdout				= self.sck
	
	def CloseLog(self):
		sys.stdout				= self.std
		self.sck.close()
	
	def _ExtractGZ(self, filename):
		newFilename = filename[:len(filename)-3]
		process					= subprocess.Popen("gunzip -c %s > %s" % (filename, newFilename), shell=True )
		process.wait()
		return newFilename
	
	def _Extract(self, filename, command):
		results 				= cStringIO.StringIO()
		print command % filename
		process					= subprocess.Popen(command % filename, stdout=subprocess.PIPE, shell=True)
		process.wait()
		output, error 			= process.communicate()
		
		if output == "":
			output = os.path.splitext(filename)[0]
		return output
	
	def _ParseFTPTimestamp(self, line):
		months= {"Jan":"01", "Feb":"02", "Mar":"03", "Apr":"04", "May":"05", "Jun":"06", "Jul":"07", "Aug":"08", "Sep":"09", "Oct":"10", "Nov":"11", "Dec":"12"}
		words = line.split()
		if ":" in words[7]:
			words[7] = time.strftime("%Y", time.gmtime())
		date = "-".join((words[7], months[words[5]], words[6]))
		self.remoteTimestamp = time.strptime(date, "%Y-%m-%d")
	
	def FetchViaHTTP(self, filename):
		print>>sys.stderr, "Fetching %s" % filename
		#os.system("wget -Nq %s" % (filename))
		os.system("curl -OL %s" % (filename))
		localFilename = filename.split("/")[len(filename.split("/"))-1]
		print>>sys.stderr, "Local filename: %s "% (localFilename)
		os.system("chmod 666 %s" % (localFilename))
		return localFilename
	
	def FtpGetLast(self, prefix):
		lines = []
		self.ftp.retrlines("LIST %s" % (prefix), lines.append)
		line = lines[len(lines) - 1].split()
		return line[len(line)-1]
	
	def GetGeneIDFromEntrez(self, aliases):
		"""Takes a set of synonyms and returns the first matched geneID."""
		geneID					= None
		
		for alias in aliases:
			if geneID is None:
				if alias in self.biosettings.aliasToID:
					geneID		= self.biosettings.aliasToID[alias]
		return geneID
	
	def ListFtpFiles(self, expression, dir = None):
		responses				= []
		self.ftp.dir(expression, responses.append)
		
		fileList				= []
		for response in responses:
			fileList.append(response.split()[8])
		return fileList
	
	def EvaluateTimestamp(self, filename):
		remoteTS				= self.FTP_Timestamp(filename)
		localTS					= self.biosettings.GetGroupTimeStamp(self.groupID)
		return localTS is None or localTS < remoteTS
	
	def FTP_Timestamp(self, filename):
		try:
			self.ftp.dir(filename, self._ParseFTPTimestamp)
		except ftplib.error_perm, e:
			print e
			print filename
		return self.remoteTimestamp
	
	def _ExtractFilename(self, filename):
		ext						= os.path.splitext(filename)[-1]
		
		if ext == ".tgz":
			localFilename		= self._Extract(filename, "tar -zxvf %s")
		elif ext == ".zip":
			localFilename		= self._Extract(filename, "unzip %s")
		elif ext == ".gz":
			localFilename		= self._ExtractGZ(filename)
		else:
			print "Unable to understand how to extract (%s) files from: %s" % (ext, filename)
		return localFilename
	
	def FTPFile(self, filename):
		localFilename 			= os.path.basename(filename)
		if self.localdir != ".":
			localFilename 		= os.path.join(self.localdir, localFilename)
			
		ext						= os.path.splitext(localFilename)[-1]
		
		remoteTS				= self.FTP_Timestamp(filename)
		downloadFile			= True
		try:
			localTS				= os.path.getctime(localFilename)
			downloadFile		= localTS < time.mktime(remoteTS)
			
			#print downloadFile, " ", localTS, " ", time.mktime(remoteTS)
		except os.error, e:
			pass
		
		if downloadFile:
			print "RETR %s (-> %s)" % (filename, localFilename)
			

			self.ftp.retrbinary('RETR %s' % filename, open(localFilename, 'wb').write)
		else:
			print "-> %s (Skipping Download)" % (localFilename)
			
		if ext == ".tgz":
			localFilename		= self._Extract(localFilename, "tar -zxvf %s")
		elif ext == ".zip":
			localFilename		= self._Extract(localFilename, "unzip %s")
		elif ext == ".gz":
			localFilename		= self._ExtractGZ(localFilename)
		else:
			print "Unable to understand how to extract (%s) files from: %s" % (ext, localFilename)
		self.localFiles.append(localFilename)

		return localFilename
	
	def OpenFTP(self, url, u="anonymous", p="anonymous@"):
		isDone					= False
		
		while not isDone:
			try:
				print "Opening FTP site: %s" % (url)
				self.ftp = ftplib.FTP(url)
				self.ftp.login(u, p)
				isDone			= True
			except ftplib.socket.error, e:
				pass

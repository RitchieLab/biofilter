//
// C++ Interface: snpmanager
//
// Description: 
//
//
// Author: Eric Torstenson <torstees@torstensonx.mc.vanderbilt.edu>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//
#ifndef BIOFILTERSNPMANAGER_H
#define BIOFILTERSNPMANAGER_H

#include <string>
#include <set>
#include <map>
#include <string>
#include "utility/strings.h"
namespace Biofilter {

struct SNP_Details {
	int chromosome;
	uint rsID;
	uint position;
	SNP_Details() : chromosome(0), rsID(0), position(0) { }

	bool operator<(const SNP_Details& other) const {
		if (chromosome == other.chromosome) 
			return position < other.position;
		else
			return chromosome < other.chromosome;
	}
};

typedef std::set<uint> SNPSet;
typedef std::set<SNP_Details> SnpDetailsCollection;

class Chromosome {
public:
	Chromosome(const char *label, uint offset);
	~Chromosome();
	int CollectSnps(uint left, uint right, SNPSet& bag);
	uint AddSNP(uint position, uint rsID);
	SNP_Details GetDetails(uint pos);
	bool operator<(const Chromosome& other) const;

	void PrintSNPs(std::ostream& os);
	void WriteMarkerInfo(std::ostream& os);
	void GetRsIDs(std::set<uint>& positions, std::set<uint>& rsIDs);
protected:
	uint offset;							///< Offset from start of genome
	std::string label;						///< Chromosome label (1, X, MT, etc)
	std::map<uint, uint> snps;				///< position->rsID
};


/**
	@Brief Repository for all SNP data
	@author Eric Torstenson <torstees@torstensonx.mc.vanderbilt.edu>
*/
class SnpManager{
public:
    SnpManager();
    ~SnpManager();
	int GetSNPs(const char *chrom, uint start, uint stop, SNPSet& snps);
	void GetDetails(SNPSet& snps, SnpDetailsCollection& details);
	SNP_Details GetDetails(uint pos);					///< Returns details for a snp at genomic position, pos
	int GetSNPs(uint rsID, SNPSet& snps);			///< Adds all SNPs at rs, rsID, to the set (position)
	uint InitSNPs(std::set<uint>& snps, const char *fn = NULL);
	uint InitSNPs(std::vector<uint>& snps, const char *fn = NULL);
	uint InitSNPs(std::vector<uint>& snps, int chromosome, const char *filename);
	uint GetRSID(uint pos);
	void PrintSNPs(std::ostream& os);
	void WriteMarkerInfo(std::ostream& os);

	void Purge();
protected:
	std::string filename;
	std::multimap<uint, uint> snps;						///< rsID -> position
	std::map<uint, Chromosome*> posLookup;				///< The chromosomes hold data mapped by position
	std::map<int, Chromosome*> chrLookup;				///< Lookup based on chromosome name
};






}

#endif

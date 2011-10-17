//
// C++ Implementation: snpmanager
//
// Description: 
//
//
// Author: Eric Torstenson <torstees@torstensonx.mc.vanderbilt.edu>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//
#include "snpmanager.h"
#include "kbentity.h"
#include <fstream>
#include <iostream>

using namespace std;
using namespace Utility;

namespace Biofilter {

Chromosome::Chromosome(const char *label, uint offset) : offset(offset), label(label) { }
Chromosome::~Chromosome() { }
int Chromosome::CollectSnps(uint left, uint right, SNPSet& bag) {
	std::map<uint, uint>::iterator itr = snps.lower_bound(left);
	std::map<uint, uint>::iterator end = snps.upper_bound(right);
	int count = 0;
	while (itr != end) {
		count++; 
		bag.insert(itr++->first + offset);
	}

	return count;
}
SNP_Details Chromosome::GetDetails(uint pos) {
	SNP_Details details;
	if (snps.find(pos - offset) != snps.end()) {
			details.chromosome = Utility::ChromToInt(label.c_str());
			details.position = pos - offset;
			details.rsID = snps[details.position];
	}
	else
		cerr<<"\n?? "<<label<<" "<<pos<<" ( "<<pos-offset<<" )\n";
	return details;
}

void Chromosome::GetRsIDs(set<uint>& positions, set<uint>& rsIDs) {
	set<uint>::iterator itr = positions.begin();
	set<uint>::iterator end = positions.end();

	while (itr != end) {
		uint pos = *itr;
		if (snps.find(pos) != snps.end())
			rsIDs.insert(snps[pos]);
		itr++;
	}
}


bool Chromosome::operator<(const Chromosome& other) const {
	return offset < other.offset;
}
uint Chromosome::AddSNP(uint position, uint rsID) {
//cerr<<"+ "<<label<<" ( "<<offset<<" ) "<<position<<" rs"<<rsID<<" ( "<<position+offset<<" )\n";
	snps[position] = rsID;
	return position+offset;
}


void Chromosome::WriteMarkerInfo(ostream& os) {
	std::map<uint, uint>::iterator itr = snps.begin();
	std::map<uint, uint>::iterator end = snps.end();
	
	while (itr != end) {
		os<<"rs"<<itr->second<<"\t"<<itr->first<<"\t"<<label<<"\n";
		itr++;
	}
}

void Chromosome::PrintSNPs(ostream& os) {
	std::map<uint, uint>::iterator itr = snps.begin();
	std::map<uint, uint>::iterator end = snps.end();
	
	//This function doesn't work right yet. I don't have the gene information stored that way right now
	while (itr != end) {
		os<<"?@?@?@?@?@?@"<<"rs"<<itr->second<<"\t"<<itr->first<<"\t"<<label<<"\n";
		itr++;
	}
}





SnpManager::SnpManager() : filename("variations.bn") {	}
SnpManager::~SnpManager() { 
	Purge();
}


void SnpManager::Purge() {
	std::map<uint, Chromosome*>::iterator itr = posLookup.begin();
	std::map<uint, Chromosome*>::iterator end = posLookup.end();
	while (itr != end) {
		delete itr++->second;
	}
	posLookup.clear();
	chrLookup.clear();
	snps.clear();
}

int SnpManager::GetSNPs(const char *chromLabel, uint start, uint stop, SNPSet& snps) {
	int count = 0;
	int chrom = ChromToInt(chromLabel);
	if (chrLookup.find(chrom) != chrLookup.end()) 
		count = chrLookup[chrom]->CollectSnps(start, stop, snps);
	return count;
}
void SnpManager::GetDetails(SNPSet& snps, SnpDetailsCollection& details) {
	SNPSet::iterator itr = snps.begin();
	SNPSet::iterator end = snps.end();

	while (itr != end )
		details.insert(GetDetails(*itr++));
}
SNP_Details SnpManager::GetDetails(uint pos) {
	assert(posLookup.lower_bound(pos) != posLookup.end());
	return posLookup.lower_bound(pos)->second->GetDetails(pos);
}


uint SnpManager::InitSNPs(std::vector<uint>& snpSource, int chromosome, const char *filename) {
	ifstream file(filename, ios::binary);
	if (!file.good()) {
		cerr<<"Unable to open file, "<<filename<<". Aborting\n";
		exit(1);
	}
	set<uint>snps;
	snps.insert(snpSource.begin(), snpSource.end());
	uint count = 0;
	uint offset = 0;
	uint idx = 0;
	while (file.good()) {
		char label[3];
		file.read(label, 2);
		label[2]='\0';
		int snpCount=0, maxPosition=0;
		file.read((char*)&snpCount, 4);
		file.read((char*)&maxPosition, 4);

		int chrom = ChromToInt(label);
		if (chrom != chromosome) {
			//Skip the rest of this chromosome
			file.seekg(snpCount*8, ios::cur);
		}
		else {
			Chromosome *newChrom = new Chromosome(label, offset);
			offset+= maxPosition;
			chrLookup[chrom] = newChrom;
			posLookup[offset] = newChrom;
			if (file.good()) {
				cerr<<".";cerr.flush();
				for (int i=0; i<snpCount; i++) {
					int rs=0, pos=0;
					file.read((char*)&rs, 4);
					file.read((char*)&pos, 4);
		
					if (snps.size() == 0 || (rs > 0 && snps.find(rs) != snps.end())) {
						idx=newChrom->AddSNP(pos, rs);
						this->snps.insert(pair<uint, uint>(rs, idx));
						count++;
					}
				}
			}
			//Break out of the while loop, since we've successfully loaded the contents of the chromsome
			break;
		}
	}
	Knowledge::KbEntity::snpManager = this;
	return count;
}


uint SnpManager::InitSNPs(std::vector<uint>& snpSource, const char *fn) {
	set<uint>snps;
	snps.insert(snpSource.begin(), snpSource.end());
	if (fn)
		filename = fn;
	ifstream file(filename.c_str(), ios::binary);
	if (!file.good()) {
		cerr<<"Unable to open file, "<<filename<<". Aborting\n";
		exit(1);
	}
	uint count = 0;
	uint offset = 0;
	uint idx = 0;
	while (file.good()) {
		char label[3];
		file.read(label, 2);
		label[2]='\0';
		int snpCount=0, maxPosition=0;
		file.read((char*)&snpCount, 4);
		file.read((char*)&maxPosition, 4);

		int chrom = ChromToInt(label);
		Chromosome *newChrom = new Chromosome(label, offset);
		offset+= maxPosition;
		chrLookup[chrom] = newChrom;
		posLookup[offset] = newChrom;
		if (file.good()) {
			cerr<<".";cerr.flush();
			for (int i=0; i<snpCount; i++) {
				int rs=0, pos=0;
				file.read((char*)&rs, 4);
				file.read((char*)&pos, 4);
	
				if (snps.size() == 0 || (rs > 0 && snps.find(rs) != snps.end())) {
					idx=newChrom->AddSNP(pos, rs);
					this->snps.insert(pair<uint, uint>(rs, idx));
					count++;
				}
			}
		}
	}
	Knowledge::KbEntity::snpManager = this;
	assert(this->snps.size() == count);
	return count;
}

uint SnpManager::InitSNPs(std::set<uint>& snps, const char *fn) {
	if (fn)
		filename = fn;
	ifstream file(filename.c_str(), ios::binary);
	if (!file.good()) {
		cerr<<"Unable to open file, "<<filename<<". Aborting\n";
		exit(1);
	}
	uint count = 0;
	uint offset = 0;
	uint idx = 0;
	while (file.good()) {
		char label[3];
		file.read(label, 2);
		label[2]='\0';
		int snpCount=0, maxPosition=0;
		file.read((char*)&snpCount, 4);
		file.read((char*)&maxPosition, 4);

		int chrom = ChromToInt(label);
		Chromosome *newChrom = new Chromosome(label, offset);
		offset+= maxPosition;
		chrLookup[chrom] = newChrom;
		posLookup[offset] = newChrom;
		if (file.good()) {
			cerr<<".";cerr.flush();
			for (int i=0; i<snpCount; i++) {
				int rs=0, pos=0;
				file.read((char*)&rs, 4);
				file.read((char*)&pos, 4);

				if (snps.size() == 0 || (rs > 0 && snps.find(rs) != snps.end())) {
					idx=newChrom->AddSNP(pos, rs);
					this->snps.insert(pair<uint, uint>(rs, idx));
					count++;
				}
			}
		}
	}
	Knowledge::KbEntity::snpManager = this;
	assert(this->snps.size() == count);
	return count;
}

void SnpManager::WriteMarkerInfo(ostream& os) {
	std::map<uint, Chromosome*>::iterator itr = posLookup.begin();
	std::map<uint, Chromosome*>::iterator end = posLookup.end();

	while (itr != end) 
		itr++->second->WriteMarkerInfo(os);
}

void SnpManager::PrintSNPs(ostream& os) {
	std::map<uint, Chromosome*>::iterator itr = posLookup.begin();
	std::map<uint, Chromosome*>::iterator end = posLookup.end();

	while (itr != end) 
		itr++->second->PrintSNPs(os);
}
int SnpManager::GetSNPs(uint rsID, SNPSet& stache) {
	int count = 0;
	std::multimap<uint, uint>::iterator itr = snps.lower_bound(rsID);
	if (itr != snps.end()) {
		std::multimap<uint, uint>::iterator end = snps.upper_bound(rsID);
		while (itr != end) {
			count++; 
			uint pos = (itr++)->second;
			stache.insert(pos);
		}
	}
	return count;
}

}

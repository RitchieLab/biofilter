/* 
 * File:   region.h
 * Author: torstees
 *
 * Basic entity that relates SNPs to groups
 *
 * Created on March 7, 2011, 2:36 PM
 */


#ifndef KREGION_H
#define	KREGION_H

#include <string>
#include <map>
#include "def.h"
#include "utility/strings.h"
#include "snpsnpmodel.h"
#include "snpdataset.h"

#include <set>

using std::map;
using std::set;
using std::string;
//#include "genegenemodel.h"

namespace Knowledge {

class GroupManager;

class Region {
public:
	Region();
	Region(const char *name, uint id);
	Region(const char *name, uint id, uint start, uint stop);
	Region(const char *name, uint id, uint effStart, uint effEnd, uint trueStart, uint trueEnd);
	Region(const Region& orig);
	virtual ~Region() {}

	/**
	 * Checks for presence of a SNP within the gene (assumes that the SNP list is populated)
    * @param snp
    * @return
    */
	bool IsPresent(uint snp);

	/**
	 * Adds a SNP to the snp set
    * @param snp
    */
	void AddSNP(uint snp);

	void AddSNPs(const Utility::IdCollection& snps);

	float ImplicationIndex(Region& other);

	/**
	 * Adds one or more meta type IDs to the meta type collection
	 */
	void AddMetaIDs(const GroupManager* gm, const Utility::IdCollection& ids);
	void AddMetaID(const GroupManager* gm, uint id);

	uint GenerateModels(SnpSnpModel::Collection& models, Region& other, float ii = 0.0);
	uint GenerateModels(SnpSnpModel::Collection& models, Utility::IdCollection& otherSnps, float ii);

	void WriteToArchive(std::ostream& os, const char *sep);
	void WriteToArchiveBinary(std::ostream& os);

	void GenerateRandomModels(uint count, SnpSnpModel::Collection& models, Region& other, float ii=0.0);
	void GenerateRandomModels(uint count, SnpSnpModel::Collection& models, Utility::IdCollection& otherSnps, float ii);

	std::string GetAliasString(const char *sep);
	std::string GetSnpString(const char *sep, SnpDataset& dataset);

	void ListGroupAssociations(std::ostream& os, uint tabCount, SnpDataset& snps);
	/**
	 * Parses comma separated list of aliases and adds them to the region aliases
	 */
	void AddAliases(const char *aliases);

	uint CountDDCapable();

	uint SnpCount() const;

	std::string DescribeRelationship(uint location);


	std::string name;					///< Primary name
	std::string enID;					///< Ensembl ID
	char chrom;							///< Chromosome index (numeric index)
	uint id;								///< DB key
	uint effStart;						///< effective lower bounding position
	uint effEnd;						///< effective upper bounding position
	uint trueStart;					///< true lower bounding position
	uint trueEnd;						///< true upper bounding position
	static float DuplicateDD_Weight;

	Utility::StringArray aliases;

	Utility::IdCollection snps;				///< SNPs contained within the gene
private:

	// mapping of type->src->groups
	std::map<const GroupManager*, set<uint> > groups;

};



inline
Region::Region() : name(""), id(0), effStart(0), effEnd(0), trueStart(0), trueEnd(0) { }

inline
Region::Region(const char *name, uint id) : name(name), id(id), effStart(0), effEnd(0), trueStart(0), trueEnd(0) { }

inline
Region::Region(const Region& orig) : name(orig.name), chrom(orig.chrom), id(orig.id), effStart(orig.effStart), effEnd(orig.effEnd), trueStart(orig.trueStart), trueEnd(orig.trueEnd), aliases(orig.aliases), snps(orig.snps), groups(orig.groups) { }

inline
Region::Region(const char *name, uint id, uint start, uint stop) : name(name), id(id), effStart(start), effEnd(stop), trueStart(start), trueEnd(stop) { }

inline
Region::Region(const char *name, uint id, uint effStart, uint effEnd, uint trueStart, uint trueEnd) : name(name), id(id), effStart(effStart), effEnd(effEnd), trueStart(trueStart), trueEnd(trueEnd) { }

inline
bool Region::IsPresent(uint snp) {
	return snps.find(snp) != snps.end();
}

inline
void Region::AddSNPs(const Utility::IdCollection& snps) {
	this->snps.insert(snps.begin(), snps.end());
}

inline
void Region::AddSNP(uint snp) {
	snps.insert(snp);
}

inline
uint Region::SnpCount() const {
	return snps.size();
}

inline
void Region::AddMetaIDs(const GroupManager* gm, const Utility::IdCollection& ids) {
	groups[gm].insert(ids.begin(), ids.end());
}

inline
void Region::AddMetaID(const GroupManager* gm, uint id) {
	groups[gm].insert(id);
}

inline
void Region::AddAliases(const char *aliases) {
	Utility::StringArray strings = Utility::Split(aliases, ",");
	this->aliases.insert(this->aliases.end(), strings.begin(), strings.end());
}

}

#endif	/* REGION_H */


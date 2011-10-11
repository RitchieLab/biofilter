/* 
 * File:   binapplication.h
 * Author: torstees
 *
 * Created on June 22, 2011, 10:35 AM
 * 
 * One of the big TODO list things would be to integrate the two 
 * forms of SNPs: Biofilter and BioBin. Right now, we have two
 * very different approaches to SNPs. For the biofilter, we only
 * need a way to recognize names and associate them with a basepair
 * and chromosome. However, for biobin, we need to maintain alleles
 * and provide the ability to perform genotype conversion and 
 * some other stuff. So, the Locus object is much more complex. 
 * 
 * Most likely it's just a matter of moving the biobin Locus class
 * to someplace common and changing the biofilter code to use it...
 * 
 */

#ifndef BINAPPLICATION_H
#define	BINAPPLICATION_H

#include "binmanager.h"
#include "biofilter/application.h"
#include "dataimporter.h"
#include "individual.h"
#include <utility>

namespace BioBin {
	
class BinApplication : public Biofilter::Application {
public:
	BinApplication();
	virtual ~BinApplication();
	void SetReportPrefix(const char *pref);
	std::pair<uint, uint> LoadVcfFile(std::string& filename, std::string& genomicBuild, Knowledge::SnpDataset& lostSnps);

	/**
	 * Return the individuals that have been loaded 
    * @return
    */
	const std::vector<Individual> &Individuals();

	/**
	 * Returns the number of SNPs that might contribute to a given bin
    * @param hits
    */
	void GetMaxBinHits(std::vector<uint>& hits);

	void GetBinContributors(std::vector<std::vector<uint> >& contributors);

	/**
	 * Returns an array matching each of the bin names
    * @return
    */
	void GetBinNames(Utility::StringArray& names);

	/**
	 * Returns the region object at a given index
    * @param idx
    * @return
    */
	const Knowledge::Region& GetRegion(uint idx);

	Utility::StringArray phenotypeFilenames;			///< The file to be used to load phenotype values

	void ApplyPhenotypes();

	Utility::Locus &Locus(uint idx);

	/**
	 * returns lookup region index -> snp index
	 */
	void GenerateBinContentLookup(std::multimap<uint, uint>& binContents);
	std::map<uint, uint> GetBinLookup();
private:
	std::map<char, BinManager> binData;		///< Used to build and parse data into bins and genotypes
						///< Help to extract genotype data from vcf files
	std::vector<Individual> individuals;	///< This represents the actual data from the vcf files
	std::set<uint> binnable;					///< Just a map into the regions that created the entries
	std::map<uint, uint> binIndex;
	//std::vector<Utility::Locus> loci;		///< The loci associated with the dataset
	//Knowledge::SnpDataset loci;			/// This is now dataset
};

inline
void BinApplication::SetReportPrefix(const char *pref) {
	if (strcmp(pref, "") == 0)
		reportPrefix = "biobin";
	else
		reportPrefix = pref;
}

inline
void BinApplication::GetMaxBinHits(std::vector<uint>& hits) {
	hits = std::vector<uint>(binnable.size()+1, 0);
	
	std::map<char, BinManager>::iterator itr = binData.begin();
	std::map<char, BinManager>::iterator end = binData.end();
	while (itr != end) {
		itr->second.CountBinContributors(hits);
		itr++;
	}
}

inline
Utility::Locus& BinApplication::Locus(uint idx) {
	return dataset[idx];
}
inline
std::map<uint, uint> BinApplication::GetBinLookup() {
	return binIndex;
}
inline
const std::vector<Individual>& BinApplication::Individuals() {
	return individuals;
}

inline
void BinApplication::GetBinNames(Utility::StringArray& names) {
	std::set<uint>::iterator itr = binnable.begin();
	std::set<uint>::iterator end = binnable.end();

	while (itr!=end) 
		names.push_back(regions[*itr++].name);
}

inline
const Knowledge::Region& BinApplication::GetRegion(uint idx) {
	return regions[idx];
}


inline
void BinApplication::GetBinContributors(std::vector<std::vector<uint> >& contributors) {
	contributors = std::vector<std::vector<uint> >(binnable.size()+1);		///< Don't forget bin 0 which is everything else....
	std::map<char, BinManager>::iterator itr = binData.begin();
	std::map<char, BinManager>::iterator end = binData.end();
	while (itr != end) {
		itr->second.BuildContributorList(contributors);
		itr++;
	}

}

}
#endif	/* BINAPPLICATION_H */


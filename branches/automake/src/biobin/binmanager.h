/* 
 * File:   binmanager.h
 * Author: torstees
 * 
 * From this standpoint, a bin is simply the smallest set of contiguous regions
 * on a single chromosome. The ID associated with a bin might actually be common
 * across multiple map entries, but the entries in the region container must
 * all be contiguous. If there are issues translating the introns/exons into
 * contiguous regions....we are in trouble....
 * 
 * Bin Manager is responsible for building up the bins for a given 
 * range within a single chromosome. The idea here is to allow these
 * objects to be able to work in parallel in the event the application
 * is migrated to the cluster. For serial purposes, we can just have an 
 * array of binmanagers, one for each chromosome
 * 
 * Each bin manager will be responsible for a contiguous region within 
 * it's chromosome (this might be the entire chromosome or a small fraction).
 * If a bin is confined to a particular region, it's actual boundaries might
 * be modified slightly to the right as a result of a bin that "hangs" beyond
 * the termination point of the manager's target region. In this case, the "end"
 * variable will be modified accordingly. However, bins that start past the
 * original "end" point will not be added. Those are assumed to become part
 * of the neighboring manager. This does mean that a small number of snps
 * will be considered by multiple managers, but it will allow parallel instantiation
 * to occur, rather than requiring them to be initialized serial.  
 * 
 * BP Boundaries are offsets from the beginning of the chromosome and the 
 * end might be -1, which means to the end of the chromosome (0 would suffice
 * for beginning...)
 * 
 * *************************************************************************
 * 
 * Genotype data format
 * Once we pull data through the manager, it is assumed that the genotypes
 * will go into a compact char array (compact in the sense that there is
 * only those genotypes that aren't represented by binned variants). 
 * 
 * Binned data is presumably implemented as an array of integer values
 * 
 * *************************************************************************
 * 
 * Building Bins
 * 
 * mafCutoff		-- This static member is used to set a common threshold for 
 * determining which SNPs are to be binned. 
 * 
 * The chromosome is first initialized with the base pair boundaries within the 
 * chromosome (beginning and end). 
 * 
 * It will then be passed a single RegionContainer, which 
 * represents all possible bins for the chromosome and a vector containing
 * allele frequencies:bp location. We'll scan this structure for the relevant
 * index boundaries (these will be the start and end points for the individual's
 * genotype data) and use the frequencies to determine which loci are binnable
 * associate them with the appropriate binIDs.
 * 
 * The manager will collect a set of bin IDs associated with all SNPs that meet
 * the mafCutoff and store a multimap: binID->locus index and return a set of binIDs
 * 
 * Any locus whose maf is greater than the allele frequency that falls within 
 * the range of the manager will be recorded in genotypeOffsets, which will 
 * later be used to identify raw_index -> compact index    
 * 
 * *************************************************************************
 * 
 * Realignment
 * 
 * Realignment allows us to replace binIDs with bin indexes so that we can
 * be most efficient in using the bin results that come from the individual's
 * parsing. This should be performed as soon as the empty bins are purged and
 * the final data layout is completed. This function simply takes a single
 * map containing binID->binIndex. 
 * 
 * In addition to this, we'll also be updating the genotypeOffsets to actually
 * include the compact genotype index
 * 
 * *************************************************************************
 * 
 * Parsing individuals
 * 
 * This function takes the vector of genotypes, pointer to genotype array and
 * returns a map binIndex->variant count. It then walks through the relevant
 * indices. Where a locus is associated with a bin, we'll increment the bin's
 * variant count. If it's a genotype, we'll record that value in the genotype
 * array. 
 * 
 * Parsing assumptions:
 * The SNP data is packaged as characters, which are some sort of encoding...
 * I'm assuming that 128 distinct encodings is sufficient. The dataset
 * should be responsible for translating these encoded values back into 
 * something meaningful....
 * 
 * For variants, 0/1 is assumed to be "Non Variant"/"Variant"
 * 
 * Created on June 7, 2011, 12:44 PM
 */

#ifndef BINMANAGER_H
#define	BINMANAGER_H

#include "individual.h"
#include <vector>
#include "knowledge/regioncontainer.h"
#include "dataimporter.h"
#include "knowledge/regionmanagerdb.h"
#include "knowledge/snpdataset.h"

namespace BioBin {
class BinManager {
public:
	BinManager();
	
	BinManager(char chrom, uint begin = 0, uint end = -1);
	
	BinManager(const BinManager& orig);
	
	virtual ~BinManager();
	
	/**
	 * Initializes the bin lookup structures and returns Ids and indexes associated
	 * with binnable and genotype loci
	 * @param indexOffset The number of snps previously observed
    * @param regionData This is all regions on the local chromosome
    * @param freqs The frequencies and BP locations associated with the loci on local chromosome
    * @param binnable (return) The ids associated with bins from this region
    * @param genotypes (return) The indexes associated with genotypes from this region
    */
	void InitBins(uint indexOffset, 
			Knowledge::RegionContainer& regionData, 
			const std::vector<Utility::Locus>& freqs, 
			std::set<uint>& binnable, 
			std::set<uint>& genotypes,
			std::set<uint>& intronic);
	
	void InitBin(uint i, 
				Knowledge::RegionContainer& regionData, 
				const std::vector<Utility::Locus>& freqs, 
				std::set<uint>& binnable, 
				std::set<uint>& genotypes, 
				std::set<uint>& intronic);
	void RealignBins(std::map<uint, uint>& binIndex);
	
	/**
	 * Adjust the internal mapping to genotypes using this conversion.
	 * We have to do this separately from the bins, since bins are sort of "Global" 
	 * in scope, whereas the genotypes are related to a single region
	 * 
	 * It is imperative that the same region is queried at once for genotypes
	 * as was used to initialize the genotype indexes...otherwise there will 
	 * be confusion as to where to find any given genotype
    * @param genotypeIndexConversion
    */
	void RealignGenotypes(std::map<uint, uint>& genotypeIndexConversion);
	//void ParseIndividual(std::vector<char>& genotypes, std::map<uint, uint>& binCounts, std::vector<char>& genotypes);
	
	/**
	 * Passes a list of genotypes (for all people at a given SNP) and returns the bin IDs 
    * @param snpIndex which SNP we are referring to
    * @param genotypes the original data from the vcf files
    * @param hits Vector containing the individual indexes where a variation occured
    * @param genotypes This is where we'll write genotype data
    * @return set of bin indexes for which this SNP applies
    */
	std::set<uint> ParseSNP(uint snpIndex, std::vector<char>& genotypes, std::vector<Individual>& data);
	
	void DescribeLocus(uint snpIndex, std::ostream& os, Knowledge::RegionManagerDB& regions, Knowledge::SnpDataset& snps);

	void CountBinContributors(std::vector<uint>& contributorCounts);
	void BuildContributorList(std::vector<std::vector<uint> >& contributors);

	static float mafCutoff;								///< Max maf to produce result in a bin
	char variantEncoding;								///< Match this to indicate a variant is present
	
	char	chromosome;										///< Indicate which chromosome we are on
	uint bpStart;											///< Region starting position
	uint bpStop;											///< Official stopping point
	uint effStop;											///< This is the effective stop...in case there are regions that hang over the edge
protected:
	std::pair<uint, uint> genotypeBoundaries;		///< index boundaries into genotype array

	std::set<uint> intergenicRareVariants;			///< SNP indexes not associated with genes
	std::multimap<uint, uint> binLookup;			///< snp_index -> bin index, [bin index...]
	/** 
	 * bin ID -> genotype index(es)
	 */
	//std::map<uint, std::set<uint> > binLookup;
	
	/**
	 * This is a little wasteful, but it allows me to more quickly
	 * hash out the genotypes into their raw locations rather than having to 
	 * maintain a list of genotype indexes and binnables to allow for deciding 
	 * which ones apply where. 
	 * 
	 * Basically, index -> offset, which is the integer offset of the array
	 */
	std::map<uint, uint> genotypeOffsets;			///< gt Index -> genotype data offsets.  
	

};

inline
BinManager::BinManager() : chromosome(char(0)), bpStart(0), bpStop(-1), effStop(-1) { }

inline
BinManager::BinManager(char chrom, uint begin, uint end) : chromosome(chrom), bpStart(begin), bpStop(end), effStop(end) {}

inline
BinManager::BinManager(const BinManager& orig) : 
		chromosome(orig.chromosome), 
		bpStart(orig.bpStart), 
		bpStop(orig.bpStop), 
		effStop(orig.effStop),
 	   genotypeBoundaries(orig.genotypeBoundaries),
		binLookup(orig.binLookup) { }

inline
BinManager::~BinManager() { }


inline
void BinManager::BuildContributorList(std::vector<std::vector<uint> >& contributors) {
	std::multimap<uint, uint>::iterator itr = binLookup.begin();
	std::multimap<uint, uint>::iterator end = binLookup.end();

	while (itr != end) {
		contributors[itr->second].push_back(itr->first);
		itr++;
	}
}

inline
void BinManager::CountBinContributors(std::vector<uint>& contributorCounts) {
	std::multimap<uint, uint>::iterator itr = binLookup.begin();
	std::multimap<uint, uint>::iterator end = binLookup.end();

	contributorCounts[0]+=intergenicRareVariants.size();
	while (itr != end) {
		contributorCounts[itr->second]++;
		itr++;
	}
}

}	// BioBin
#endif	/* BINMANAGER_H */


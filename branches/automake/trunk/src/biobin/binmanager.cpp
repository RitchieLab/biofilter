/* 
 * File:   binmanager.cpp
 * Author: torstees
 * 
 * Created on June 7, 2011, 12:44 PM
 */

#include "binmanager.h"
#include <sstream>

namespace BioBin {
	
float BinManager::mafCutoff = 0.05;

void BinManager::InitBin(uint i, 
				Knowledge::RegionContainer& regionData, 
				const std::vector<Utility::Locus>& freqs, 
				std::set<uint>& binnable, 
				std::set<uint>& genotypes, 
				std::set<uint>& intronic) { 
	const Utility::Locus& l = freqs[i];
	if (l.chrom == chromosome) {
		if (l.pos > bpStart) {
			if (l.pos < effStop) {
				genotypeBoundaries.second++;
				
				if (l.MinorAlleleFreq() < mafCutoff) {
					std::set<Knowledge::RegionContainer::Region> regionIDs;
					if (regionData.GetRegionCoverage(l.pos, regionIDs)) {
						std::set<Knowledge::RegionContainer::Region>::iterator regItr = regionIDs.begin();
						std::set<Knowledge::RegionContainer::Region>::iterator regEnd = regionIDs.end();

						while (regItr != regEnd) {
							// We don't include regions that start before 
							// the start of the manager's region, nor the bins that
							// start between the effective end and the original end
							if (regItr->lBound <= bpStop && regItr->lBound >= bpStart) {
								binLookup.insert(std::make_pair(i, regItr->index));

								if (regItr->rBound > effStop)
									effStop = regItr->rBound;
								binnable.insert(regItr->index);			///< This is actually the gene's ID
							}
							regItr++;
						}
					}
					else {
						// We'll denote variants that lie outside a gene as index -1
						intronic.insert(i);
					}
				}
				else {
					genotypes.insert(i);
					genotypeOffsets[i] = i;					// Right now, put a placeholder for the genotype indexes
				}
			}	
		} else {
			genotypeBoundaries.first = ++genotypeBoundaries.second;
		}	
	}
}


void BinManager::InitBins(uint i, 
				Knowledge::RegionContainer& regionData, 
				const std::vector<Utility::Locus>& freqs, 
				std::set<uint>& binnable, 
				std::set<uint>& genotypes, 
				std::set<uint>& intronic) {
	std::vector<Utility::Locus>::const_iterator itr = freqs.begin();
	std::vector<Utility::Locus>::const_iterator end = freqs.end();
	
	genotypeBoundaries											= std::make_pair(i, i);
	std::set<Knowledge::RegionContainer::Region> regionIDs;
	
	while (itr != end) {
		const Utility::Locus& l = *itr;
		if (l.chrom == chromosome) {
			if (l.pos > bpStart) {
				if (l.pos < effStop) {
					genotypeBoundaries.second++;
					if (l.MinorAlleleFreq() < mafCutoff) {
						regionIDs.clear();
						if (regionData.GetRegionCoverage(l.pos, regionIDs)) {
							std::set<Knowledge::RegionContainer::Region>::iterator regItr = regionIDs.begin();
							std::set<Knowledge::RegionContainer::Region>::iterator regEnd = regionIDs.end();

							while (regItr != regEnd) {
								// We don't include regions that start before 
								// the start of the manager's region, nor the bins that
								// start between the effective end and the original end
								if (regItr->lBound <= bpStop && regItr->lBound >= bpStart) {
									binLookup.insert(std::make_pair(i, regItr->index));

									if (regItr->rBound > effStop)
										effStop = regItr->rBound;
									binnable.insert(regItr->index);			///< This is actually the gene's ID
								}
								regItr++;
							}
						}
						else {
							// We'll denote variants that lie outside a gene as index -1
							intronic.insert(i);
						}
					}
					else {
						genotypes.insert(i);
						genotypeOffsets[i] = i;					// Right now, put a placeholder for the genotype indexes
					}
				}	
			} else {
				genotypeBoundaries.first = ++genotypeBoundaries.second;
			}
		}
		itr++;
		i++;
	}
	intergenicRareVariants.insert(intronic.begin(), intronic.end());
}


void BinManager::RealignGenotypes(std::map<uint, uint>& genotypeIndexConversion) {
	std::map<uint, uint>::iterator itr = genotypeOffsets.begin();
	std::map<uint, uint>::iterator end = genotypeOffsets.end();
	
	while (itr != end) {
		// For this one, the second part was only a placeholder and can be overwritten
		itr->second = genotypeIndexConversion[itr->first];
		itr++;
	}	
}

void BinManager::RealignBins(std::map<uint, uint>& regIndexConversion) {
	//Basically, we'll recreate the binLookup to use the itr->second stuff and replace the
	//original. 

	std::multimap<uint, uint> originalLookup = binLookup;
	binLookup.clear();
	
	std::multimap<uint, uint>::iterator bItr = originalLookup.begin();
	std::multimap<uint, uint>::iterator bEnd = originalLookup.end();

	while (bItr != bEnd) {
		binLookup.insert(std::make_pair(bItr->first, regIndexConversion[bItr->second]));
		bItr++;
	}
	


}

void BinManager::DescribeLocus(uint snpIndex, std::ostream& os, Knowledge::RegionManagerDB& regions, Knowledge::SnpDataset& snps) {
	bool isRareIntron = intergenicRareVariants.find(snpIndex) != intergenicRareVariants.end();
	
	if (isRareIntron) {
		os<<"Rare Variant,\n";
	} else {
		std::multimap<uint, uint>::iterator binend = binLookup.end();
		std::multimap<uint, uint>::iterator itr = binLookup.lower_bound(snpIndex);
		std::multimap<uint, uint>::iterator last = binLookup.upper_bound(snpIndex);
		std::stringstream ss;

		Utility::IdCollection regionIdx;
		snps.GetRegionCoverage(snpIndex, regionIdx);
		Utility::StringArray regionNames;
		Utility::IdCollection::iterator ritr = regionIdx.begin();
		Utility::IdCollection::iterator rend = regionIdx.end();
		while (ritr != rend) 
			regionNames.push_back(regions[*ritr++].name);
		
		if (binLookup.find(snpIndex) == binend) {
			os<<"Variant,";
		} else {
			os<<"Rare Variant,";
		}
		os<<Utility::Join(regionNames, ":")<<"\n";
	}
}

std::set<uint> BinManager::ParseSNP(uint snpIndex, std::vector<char>& genotypes, std::vector<Individual>& data) {
	bool isRareIntron = intergenicRareVariants.find(snpIndex) != intergenicRareVariants.end();
	
	std::multimap<uint, uint>::iterator binend = binLookup.end();
	uint count = genotypes.size();
	for (uint i=0; i<count; i++) {
		if (isRareIntron) {
			if ((int)genotypes[i] > -1)
				data[i].binData[0]+=genotypes[i];
		}
		else {
			std::multimap<uint, uint>::iterator itr = binLookup.find(snpIndex);
			std::multimap<uint, uint>::iterator first = binLookup.lower_bound(snpIndex);
			std::multimap<uint, uint>::iterator last = binLookup.upper_bound(snpIndex);

			if (itr != binend) {
				while (first != last) {
					if ((int)genotypes[i] > -1)
						data[i].binData[first->second]+=genotypes[i];
					first++;
				}
			} else 
				data[i].genotypes.SetGenotype(genotypeOffsets[snpIndex], genotypes[i]);
		}
	}

	
	std::set<uint> bins;
	std::multimap<uint, uint>::iterator itr = binLookup.lower_bound(snpIndex);
	std::multimap<uint, uint>::iterator end = binLookup.upper_bound(snpIndex);
	
	while (itr != end) 
		bins.insert(itr++->second);

	

	return bins;
}




} // BioBin


/* 
 * File:   binapplication.cpp
 * Author: torstees
 * 
 * Created on June 22, 2011, 10:35 AM
 */

#include "binapplication.h"

namespace BioBin {

BinApplication::BinApplication() {
}


BinApplication::~BinApplication() {
}

std::pair<uint, uint> BinApplication::LoadVcfFile(std::string& filename, std::string& genomicBuild, Knowledge::SnpDataset& lostSnps) {
	std::set<uint> genotypes;
	Utility::StringArray emptyChromosomes;
	
	std::vector<Utility::Locus> genotypeableLoci;							///< Used to record the entire locus array
	std::vector<Utility::Locus> complexLoci;									///< Used to record all loci with more than 2 distinct allele possibilities
	DataImporter vcfimporter;
	uint locusCount    = 0;
	uint genotypeCount = 0;
	std::cerr<<"Loading VCF Data\n";
	std::cerr<<"Chrom\tLoci\tCommon\tRare\tIntergenic Rare\n";
	if (vcfimporter.Open(filename.c_str(), (char)0)) {
		std::vector<Utility::Locus> locusArray;
		vcfimporter.GetAllAlleleFrequencies(locusArray);
		
		std::map<char, std::vector<int> > locusRemap;
		//const std::vector<Utility::Locus>& locusArray = vcfimporter.GetLoci();
		LiftOver::ConverterDB cnv;
		int chainCount = cnv.LoadFromDB(genomicBuild.c_str(), sociDB);
		if (chainCount > 0) {
			std::string conversionLog = this->AddReport("lift-over", "tsv", "SNPs that were lifted over to new build which differed dramatically or changed chromosome");
			std::ofstream cnvLog(conversionLog.c_str());
			cnvLog<<"RSID,Chrom(Orig),Pos(Orig),Chrom(New),Pos(New)\n";
			
			std::multimap<Utility::Locus, Utility::Locus> converted;
			std::multimap<Utility::Locus, Utility::Locus>::iterator missing;
			cnv.ConvertDataset(locusArray, converted);
			std::vector<Utility::Locus>::iterator itr = locusArray.begin();
			std::vector<Utility::Locus>::iterator end = locusArray.end();

			uint i=0;
			uint validLocus = 0;
			std::stringstream missingSNPs;
			while (itr != end) {
				Utility::Locus &orig = *itr;
				
				if (converted.find(orig) != missing) {
					std::multimap<Utility::Locus, Utility::Locus>::iterator first = converted.lower_bound(orig);
					std::multimap<Utility::Locus, Utility::Locus>::iterator last  = converted.upper_bound(orig);
					
					if (converted.count(orig) != 1)
						std::cerr<<"It was observed that there are multiple hits returned by convert dataset: "<<orig.RSID()<<" has "<<converted.count(orig)<<" counterparts.\n";
					while (first != last) {
						if (first->second.pos == 0)
							missingSNPs<<first->first.RSID()<<"\t"<<Utility::ChromFromInt(first->first.chrom)<<"\t"<<first->first.pos<<"\n";
						else {
							if (first->second.chrom > 0) {
								if (first->first.Chrom() != first->second.Chrom() || ((float)abs((float)first->first.pos - (float)first->second.pos)/(float)first->first.pos)> 0.01)
									cnvLog<<first->first.RSID()<<"\t"
										<<first->first.Chrom()<<"\t"
										 <<first->first.pos<<"\t"
										 <<first->second.Chrom()<<"\t"
										 <<first->second.pos<<"\t"
										 <<first->second.RSID()<<"\n";
								dataset.AddSNP(first->second.chrom, first->second.pos, first->second.RSID().c_str());
								locusRemap[first->second.chrom].push_back(validLocus++);
								locusArray[i] = first->second;
							}							
						}
						first++;
					}
				}
				itr++;
				i++;
			}
			if (missingSNPs.str().length() > 0) {
				std::string filename = AddReport("missing-snps", "txt", "SNPs that were dropped during build conversion");
				std::ofstream file(filename.c_str());
				file<<missingSNPs.str();
			}
		} else {
			std::vector<Utility::Locus>::iterator itr = locusArray.begin();
			std::vector<Utility::Locus>::iterator end = locusArray.end();
			
			uint i=0;
			while (itr != end) {
				locusRemap[itr->chrom].push_back(i);
				dataset.AddSNP(itr->chrom, itr->pos, itr->RSID().c_str());
				dataset.LoadData(locusArray, 37);
				i++;
				itr++;
			}
		}
		std::cerr<<"Locus Remap[0].size() = "<<locusRemap[0].size()<<"\n";
		for (char c=0; c<26; c++) {
			std::map<uint, uint> genotypeLocusIndex;						///< Used to configure the genotype conversion 
			std::set<uint> localBinnable;
			std::set<uint> intergenicRegions;									///< This is used for reporting purposes...the binmanager keeps track of it internally
			//vcfimporter.SetChromosome(c);
			
			binData[c] = BinManager(c+1);

			Knowledge::RegionContainer bins;
			regions.BuildRegionSegments(c+1, bins);
			genotypes.clear();
			//std::vector<Utility::Locus>& loci				= lociByChrom[c+1];
			//const std::vector<Utility::Locus>& loci = vcfimporter.GetAlleleFrequencies();
			//binData[c].InitBins(locusCount, bins, loci, localBinnable, genotypes, intergenicRegions);
			BinManager &bmgr = binData[c];
			std::vector<int>::iterator lrItr = locusRemap[c+1].begin();
			std::vector<int>::iterator lrEnd = locusRemap[c+1].end();
			while (lrItr != lrEnd) {
				bmgr.InitBin(*lrItr, bins, locusArray, localBinnable, genotypes, intergenicRegions);
				lrItr++;
			}
			
			
			// Record the indexes we'll use for each true genotype
			std::set<uint>::iterator gitr = genotypes.begin();
			std::set<uint>::iterator gend = genotypes.end();
			
			while (gitr != gend)  {
				GenotypeStorage::alleleCount.push_back(locusArray[*gitr].alleles.size());
				genotypeLocusIndex[*gitr++] = genotypeCount++;
			}
			
			//This should fix the genotypes to use the correct index
			binData[c].RealignGenotypes(genotypeLocusIndex);

			if (localBinnable.size() > 0 || locusRemap[c+1].size() > 0)
				std::cerr<<vcfimporter.chromosomeNames[c]<<"\t"
						<<locusRemap[c+1].size()<<"\t"
						<<genotypes.size()<<"\t"
						<<locusRemap[c+1].size()-genotypes.size()-intergenicRegions.size()<<"\t"
						<<intergenicRegions.size()<<"\n";
			else {
				emptyChromosomes.push_back(vcfimporter.chromosomeNames[c]);
			}
			binnable.insert(localBinnable.begin(), localBinnable.end());
			locusCount += locusRemap[c+1].size();
		}
		
		//std::vector<uint> genotypeIndexes(genotypeCount, 0);
		//std::vector<uint> binIndexes(binnable.size(), 0);
		
		vcfimporter.Close();
		
		//Fix the bins. We want to eliminate the bins that are empty, so we aren't
		//carrying around lots of empty columns in our array
		std::set<uint>::iterator itr = binnable.begin();
		std::set<uint>::iterator end = binnable.end();
		//std::map<uint, uint> binIndex;
		
		//bin 0 is currently used to indicate intronic snps
		uint i = 1;
		while (itr != end) {
			binIndex[*itr] = i++;
			itr++;
		}
		std::map<uint, uint>::iterator notInBinIndexes = binIndex.end();
		
		/*
		Utility::StringArray binnableGenes;
		GetBinNames(binnableGenes);

		if (binnableGenes.size() < 50 && binnableGenes.size() > 0)
			std::cerr<<"Binnables: "<<Utility::Join(binnableGenes, ",")<<"\n";
		*/		
		for (char c=0; c<25; c++) 
			binData[c].RealignBins(binIndex);
				
		//At this point, our bin managers should be ready to correctly parse genotype data
		vcfimporter.Open(filename.c_str(), -1);
		

		//Parsing SNPs is a little difference. We are going to march from top 
		//to bottom-since we don't have to parse them according to chromosome
		
		
		Utility::StringArray individualIDs = vcfimporter.GetIndividualIDs();
		Utility::StringArray::iterator iitr = individualIDs.begin();
		Utility::StringArray::iterator iend = individualIDs.end();
		
		uint individualCount = individualIDs.size();
		individuals = std::vector<Individual>(individualCount);
		i=0;
		while (iitr != iend) {
			individuals[i++].Init(*iitr, genotypeCount, binnable.size());
			iitr++;
		}		
		
		std::string ofn = AddReport("locus", "csv", "Locus Description");
		std::ofstream locusFile(ofn.c_str());
		locusFile<<"Chromosome,bp loc,all 1,freq(1),all(2),freq(2),type,gene\n";
		std::vector<Utility::Locus>::const_iterator litr = locusArray.begin();
		std::vector<Utility::Locus>::const_iterator lend = locusArray.end();
		i=0;
		uint locusCount = dataset.Size();
		while (litr != lend && i<locusCount) {
			litr->Print(locusFile, ",");
			binData[litr->chrom-1].DescribeLocus(i++, locusFile, regions, dataset);
			litr++;
		}
		locusFile.close();
		
		i=0;
		litr = locusArray.begin();
		
		while (litr != lend && i<locusCount) {
			std::vector<char> genotypes(individualCount, (char)-1);
			if (litr->chrom > 0) {
				vcfimporter.ParseSNP(i, genotypes);
				binData[litr->chrom-1].ParseSNP(i, genotypes, individuals);
			}
			i++;
			litr++;
		}
		
		//We should have binned data and gentypes sorted out
		ApplyPhenotypes();
		
		
		// OK, now for the silliness. We are just going to march
		// through the regions to count how many people are in each
		// bin and print out the gene ID, boundaries and counts
		// This will eventually become a real report, but we need to
		// do stuff really quick and dirty, so here it is....
		std::vector<uint> binCounts(dataset.Size()-genotypeCount+1, 0);
		std::vector<Individual>::iterator indItr = individuals.begin();
		std::vector<Individual>::iterator indEnd = individuals.end();
		
		while (indItr != indEnd) 
			indItr++->ApplyBinCounts(binCounts);
		ofn = AddReport("bins", "csv", "Bin Descriptions");
		std::ofstream binreport(ofn.c_str());
		std::cout<<"\n\n"<<std::setw(10)<<"Idx"<<std::setw(15)<<"Region ID"<<std::setw(20)<<"Region"<<std::setw(10)<<"Chrom."<<std::setw(10)<<"Start"<<std::setw(10)<<"Stop"<<std::setw(10)<<"Bin Count"<<"\n";
		binreport<<"Idx,Region,Chrom.,Start,Stop,Bin Count\n";
		std::cout
			 <<setw(10)<<"-"<<setw(15)<<"-"
			 <<std::setw(20)<<"Intergenic"
			 <<setw(10)<<"-"
			 <<std::setw(10)<<"-"
			 <<std::setw(10)<<"-"
			 <<std::setw(10)<<binCounts[0]<<"\n";
		binreport<<"0,-,Intergenic,-,-,"<<binCounts[0]<<"\n";
		
		uint regionCount = regions.Size();
		//uint bin = 0;
		for (uint i=0; i<regionCount; i++) {
			Knowledge::Region& r = regions[i];
			if (binIndex.find(i) != notInBinIndexes) {
				std::cout
					 <<setw(10)<<i<<setw(15)<<r.id
					 <<std::setw(20)<<r.name
					 <<setw(10)<<Utility::ChromFromInt(r.chrom-1)
					 <<std::setw(10)<<r.effStart
					 <<std::setw(10)<<r.effEnd
					 <<std::setw(10)<<binCounts[binIndex[i]]<<"\n";
				binreport<<binIndex[i]<<","<<r.name<<","<<Utility::ChromFromInt(r.chrom-1)<<","<<r.effStart<<","<<r.effEnd<<","<<binCounts[binIndex[i]]<<"\n";
			}
		}
		//std::cerr<<"Binnable Indexes: "<<Utility::Join(binnable, ",")<<"\n";
			
	}
	std::cerr<<Utility::Join(emptyChromosomes, ",")<<" were not found in the file, "<<filename<<".\n";

	
	return std::make_pair(binnable.size(), genotypeCount);
}

void BinApplication::ApplyPhenotypes() {
	Utility::StringArray::iterator itr = phenotypeFilenames.begin();
	Utility::StringArray::iterator end = phenotypeFilenames.end();
	std::map<std::string, std::string> phenotypeLookup;
	while (itr != end) {
		Utility::StringArray ids;
		std::string contents = Utility::LoadContents(itr->c_str());
		ids = Utility::Split(contents.c_str(), "\n");
		Utility::StringArray::iterator id = ids.begin();
		Utility::StringArray::iterator kvend = ids.end();

		while (id != kvend) {
			Utility::StringArray kv = Utility::Split(id->c_str());
			if (kv.size() > 1)
				phenotypeLookup[kv[0]] = kv[1];
			id++;
		}
		itr++;
	}
	std::map<std::string, std::string>::iterator indNotFound = phenotypeLookup.end();
	std::vector<Individual>::iterator indItr = individuals.begin();
	std::vector<Individual>::iterator indEnd = individuals.end();
	while (indItr != indEnd) {
		if (phenotypeLookup.find(indItr->indID) != indNotFound)
			indItr->status = atof(phenotypeLookup[indItr->indID].c_str());
		indItr++;
	}
}


void BinApplication::GenerateBinContentLookup(std::multimap<uint, uint>& binContents) {
	binContents.clear();
	std::vector<std::vector<uint> > contributors;
	GetBinContributors(contributors);
	
	{
		//uint i=0;
		std::vector<std::vector<uint> >::iterator itr = contributors.begin();
		std::vector<std::vector<uint> >::iterator end = contributors.end();
		while (itr != end) {
			itr++;
		}
	}
	
	
	std::map<uint, uint>::iterator itr = binIndex.begin();
	std::map<uint, uint>::iterator end = binIndex.end();
	while (itr != end) {
		std::vector<uint>& contribs = contributors[itr->second];
		std::vector<uint>::iterator citr = contribs.begin();
		std::vector<uint>::iterator cend = contribs.end();
		while (citr != cend) {
			binContents.insert(std::make_pair(itr->first, *(citr)));
			if ((*citr) > dataset.Size())
				std::cerr<<" Oversized Index ("<<dataset.Size()<<"):\t"<<regions[itr->first].id<<"\t"<<regions[itr->first].name<<"\t"<<*citr<<"\n";
			citr++;
		}
		itr++;
	}
}
}

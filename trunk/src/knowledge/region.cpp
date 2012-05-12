/* 
 * File:   region.cpp
 * Author: torstees
 * 
 * Created on March 7, 2011, 2:36 PM
 */
#include <iostream>
#include <fstream>
#include "region.h"
#include "groupmanager.h"

namespace Knowledge {

float Region::DuplicateDD_Weight = 0.0;

float Region::ImplicationIndex(Region& other)  {

	float ii = 0;

	map<const GroupManager*, set<uint> >::const_iterator itr = groups.begin();
	map<const GroupManager*, set<uint> >::const_iterator end = groups.end();

	set<uint> common;

	while(itr != end){
		map<const GroupManager*, set<uint> >::const_iterator o_itr = other.groups.find((*itr).first);
		if(o_itr != other.groups.end()){
			common.clear();
			set_intersection((*itr).second.begin(), (*itr).second.end(),
					(*o_itr).second.begin(), (*o_itr).second.end(),
					inserter(common, common.begin()));

			ii += (common.size() > 0) + (common.size() > 0)*((*itr).first->diseaseDependent() ? 1 : DuplicateDD_Weight)*(common.size() - 1);
		}
		++itr;
	}

	return ii;
}

uint Region::GenerateModels(SnpSnpModel::Collection& models, Region& other, float ii) {
	if (ii == 0.0)
		ii = ImplicationIndex(other);
	return GenerateModels(models, other.snps, ii);
}

uint Region::GenerateModels(SnpSnpModel::Collection& models, Utility::IdCollection& otherSnps, float ii) {
	Utility::IdCollection left;
	Utility::IdCollection right;

	uint count = models.size();

	set_difference(snps.begin(), snps.end(), otherSnps.begin(), otherSnps.end(), inserter(left, left.begin()));
	set_difference(otherSnps.begin(), otherSnps.end(), snps.begin(), snps.end(), inserter(right, right.begin()));

	Utility::IdCollection::iterator lItr	= left.begin();
	Utility::IdCollection::iterator lEnd	= left.end();

	Utility::IdCollection::iterator rItr	= right.begin();
	Utility::IdCollection::iterator rEnd	= right.end();

	while (lItr != lEnd) {
		rItr								= right.begin();
		while (rItr != rEnd) {
			models.insert(SnpSnpModel(*lItr, *rItr++, ii));
		}
		lItr++;
	}
	return models.size()  - count;
}
void Region::GenerateRandomModels(uint count, SnpSnpModel::Collection& models, Region& other, float ii) {
	if (ii == 0.0)
		ii = ImplicationIndex(other);
	GenerateRandomModels(count, models, other.snps, ii);
}

std::string Region::DescribeRelationship(uint location) {
	if (location >= trueStart && location <= trueEnd)
		return "Interior";
	if (location >= effStart && location <= effEnd)
		return "Flanking";
	if (location < effStart || location > effEnd)
		return "Exterior";
	return "Unknown";
}

void Region::GenerateRandomModels(uint count, SnpSnpModel::Collection& models, Utility::IdCollection& otherSnps, float ii) {
	Utility::IdCollection left;
	Utility::IdCollection right;
	uint target		= models.size() + count;

	set_difference(snps.begin(), snps.end(), otherSnps.begin(), otherSnps.end(), inserter(left, left.begin()));
	set_difference(otherSnps.begin(), otherSnps.end(), snps.begin(), snps.end(), inserter(right, right.begin()));

	std::vector<uint> lpool;
	std::vector<uint> rpool;
	lpool.insert(lpool.end(), left.begin(), left.end());
	rpool.insert(rpool.end(), right.begin(), right.end());

	uint lcount = lpool.size();
	uint rcount = rpool.size();

	while (models.size() < target) {
		uint lIdx = rand() % lcount;
		uint rIdx = rand() % rcount;
		models.insert(SnpSnpModel(lpool[lIdx], rpool[rIdx], ii));
	}

}

std::string Region::GetAliasString(const char *sep) {
	return Utility::Join(aliases, sep);
}
std::string Region::GetSnpString(const char *sep, SnpDataset& dataset) {
	Utility::StringArray snpList;

	Utility::IdCollection::iterator itr = snps.begin();
	Utility::IdCollection::iterator end = snps.end();

	while (itr != end)
		snpList.push_back(dataset[*itr++].RSID());

	return Utility::Join(snpList, sep);
}


void Region::WriteToArchiveBinary(std::ostream& os) {
	Utility::StringArray ids;
	os<<name<<"\n";
	os.write((char*)&trueStart, 4);
	os.write((char*)&trueEnd, 4);
	os.write((char*)&effStart, 4);
	os.write((char*)&effEnd, 4);
//	for (MetaGroup::Type i=MetaGroup::DiseaseIndependent; i<MetaGroup::MetaGroupCount; i++) {
//		uint count = groups[i].size();
//		os.write((char*)&count, 4);
//		Utility::IdCollection::iterator itr = groups[i].begin();
//		Utility::IdCollection::iterator end = groups[i].end();
//
//		while (itr != end) {
//			os.write((char*)&(*itr++), 4);
//		}
//	}
	os<<Utility::Join(aliases, "|")<<"\n";
	uint count = snps.size();
	os.write((char*)&count, 4);
	Utility::IdCollection::iterator itr = snps.begin();
	Utility::IdCollection::iterator end = snps.end();

	while (itr != end) {
		os.write((char*)&*itr++, 4);
	}
}


void Region::WriteToArchive(std::ostream& os, const char *sep) {
	Utility::StringArray ids;

	os	<<name<<sep
		<<trueStart<<sep
		<<trueEnd<<sep
		<<effStart<<sep
		<<effEnd<<sep;

	// output the groups

	map<const GroupManager*, set<uint> >::const_iterator itr = groups.begin();
	map<const GroupManager*, set<uint> >::const_iterator end = groups.end();

	while(itr != end){

		set<uint>::const_iterator g_itr = (*itr).second.begin();
		set<uint>::const_iterator g_end = (*itr).second.end();
		while(g_itr != g_end){
			os << ((*itr).first->diseaseDependent() ? "~" : "!") << (*(*itr).first)[*g_itr].id;
			++g_itr;
		}

		++itr;
		if(itr != end){
			os << "|";
		}

	}

	os 	<<sep<<Utility::Join(aliases, "|")<<sep
		<<Utility::Join<std::set<uint> >(snps, "|")<<"\n";

}


void Region::ListGroupAssociations(std::ostream& os, uint tabCount, SnpDataset& snps) {
	os<<std::string(tabCount, '\t')<<name<<" (";
	Utility::IdCollection::iterator itr = this->snps.begin();
	Utility::IdCollection::iterator end = this->snps.end();

	while (itr != end) {
		os<<snps[*itr++].RSID()<<" ";
	}
	os<<")\n";
}

uint Region::CountDDCapable() {
	uint num_dd = 0;

	map<const GroupManager*, set<uint> >::const_iterator itr = groups.begin();
	map<const GroupManager*, set<uint> >::const_iterator end = groups.end();
	while (itr != end){
		num_dd += (*itr).first->diseaseDependent();
	}

	return num_dd;
}

}


#ifdef TEST_APP

#include <gtest/gtest.h>

using namespace Knowledge;

TEST(RegionTest, BasicTest) {
	Region reg1("reg1", 1);
	reg1.AddSNPs(Utility::ToSet<uint>("1,2,3", ","));
	Utility::IdCollection ids;
	ids.insert(1);				// 1 -> 1
	ids.insert(3);				// 1 -> 3
	reg1.AddMetaIDs(MetaGroup::DiseaseIndependent, ids);
	ids.clear();
	ids.insert(2);				// 2 -> 2
	reg1.AddMetaIDs(MetaGroup::DiseaseDependent, ids);

	EXPECT_EQ("reg1", reg1.name);
	EXPECT_EQ((uint)1, reg1.id);
	EXPECT_TRUE(reg1.IsPresent(1));
	EXPECT_TRUE(reg1.IsPresent(2));
	EXPECT_TRUE(reg1.IsPresent(3));
	EXPECT_FALSE(reg1.IsPresent(4));

	Region reg2("reg2", 2);
	reg2.AddSNP(2);
	reg2.AddSNP(4);
	reg2.AddSNP(5);
	reg2.AddSNP(6);

	ids.clear();
	ids.insert(1);				// 1 -> 1
	reg2.AddMetaIDs(MetaGroup::DiseaseIndependent, ids);

	ids.clear();
	ids.insert(2);				// 2 -> 2
	ids.insert(4);				// 2 -> 4
	reg2.AddMetaIDs(MetaGroup::DiseaseDependent, ids);

	Region::DuplicateDD_Weight = 0.25;
	SnpSnpModel::Collection models;
	reg1.GenerateModels(models, reg2);
	EXPECT_EQ((uint)6, models.size());

	SnpSnpModel::Collection::iterator itr = models.begin();
	SnpSnpModel::Collection::iterator end = models.end();

	EXPECT_EQ(1, (*itr)[0]);
	EXPECT_EQ(4, (*itr)[1]);
	EXPECT_EQ(2.5, itr++->ImplicationIndex());

	EXPECT_EQ(1, (*itr)[0]);
	EXPECT_EQ(5, (*itr)[1]);
	EXPECT_EQ(2.5, itr++->ImplicationIndex());

	EXPECT_EQ(1, (*itr)[0]);
	EXPECT_EQ(6, (*itr)[1]);
	EXPECT_EQ(2.5, itr++->ImplicationIndex());

	EXPECT_EQ(3, (*itr)[0]);
	EXPECT_EQ(4, (*itr)[1]);
	EXPECT_EQ(2.5, itr++->ImplicationIndex());

	EXPECT_EQ(3, (*itr)[0]);
	EXPECT_EQ(5, (*itr)[1]);
	EXPECT_EQ(2.5, itr++->ImplicationIndex());

	EXPECT_EQ(3, (*itr)[0]);
	EXPECT_EQ(6, (*itr)[1]);
	EXPECT_EQ(2.5, itr++->ImplicationIndex());

	Region::DuplicateDD_Weight = 0.0;
}

TEST(RegionTest, TextArchive) {
	Region reg1("reg1", 1, 0, 100, 25, 75);
	reg1.AddSNP(1);
	reg1.AddSNP(2);
	reg1.AddSNP(3);
	Utility::IdCollection ids;
	ids.insert(1);				// 1 -> 1
	ids.insert(3);				// 1 -> 3
	reg1.AddMetaIDs(MetaGroup::DiseaseIndependent, ids);
	ids.clear();
	ids.insert(2);				// 2 -> 2
	reg1.AddMetaIDs(MetaGroup::DiseaseDependent, ids);
	reg1.AddAliases("r1,1,region1");

	Region reg2("reg2", 2, 80, 180, 110, 165);
	reg2.AddSNP(3);
	reg2.AddSNP(4);
	reg2.AddSNP(5);
	reg2.AddSNP(8);
	reg2.AddSNP(9);
	ids.clear();
	ids.insert(1);
	reg2.AddMetaIDs(MetaGroup::DiseaseIndependent, ids);
	ids.clear();
	ids.insert(4);
	ids.insert(5);
	reg2.AddMetaIDs(MetaGroup::DiseaseDependent, ids);
	reg2.AddAliases("r2,2");

	std::ofstream file("regions-test.txt");
	reg1.WriteToArchive(file, "\t");
	reg2.WriteToArchive(file, "\t");
	file.close();

	std::ifstream infile("regions-test.txt");
	Region r1, r2;
	r1.LoadFromArchive(infile, "\t");
	r2.LoadFromArchive(infile, "\t");

	EXPECT_EQ("reg1", r1.name);
	EXPECT_TRUE(r1.IsPresent(1));
	EXPECT_TRUE(r1.IsPresent(2));
	EXPECT_TRUE(r1.IsPresent(3));
	EXPECT_FALSE(r1.IsPresent(4));
	EXPECT_EQ("r1:1:region1", Utility::Join(r1.aliases, ":"));
	EXPECT_EQ(3, r1.aliases.size());
	EXPECT_EQ(2, r1.groups[MetaGroup::DiseaseIndependent].size());
	EXPECT_EQ(1, r1.groups[MetaGroup::DiseaseDependent].size());
	EXPECT_EQ(1, *(r1.groups[MetaGroup::DiseaseIndependent].begin()));
	EXPECT_EQ(2, *(r1.groups[2].begin()));

	EXPECT_EQ("reg2", r2.name);
	EXPECT_TRUE(r2.IsPresent(3));
	EXPECT_TRUE(r2.IsPresent(4));
	EXPECT_TRUE(r2.IsPresent(5));
	EXPECT_TRUE(r2.IsPresent(8));
	EXPECT_FALSE(r2.IsPresent(1));
	EXPECT_EQ("r2=2", Utility::Join(r2.aliases, "="));
	EXPECT_EQ(2, r2.aliases.size());
	EXPECT_EQ(1, r2.groups[MetaGroup::DiseaseIndependent].size());
	EXPECT_EQ(2, r2.groups[MetaGroup::DiseaseDependent].size());
	EXPECT_EQ(1, *(r2.groups[MetaGroup::DiseaseIndependent].begin()));
	EXPECT_EQ(4, *(r2.groups[MetaGroup::DiseaseDependent].begin()));

	remove("regions-test.txt");
}




TEST(RegionTest, BinaryArchive) {
	Region reg1("reg1", 1, 0, 100, 25, 75);
	reg1.AddSNP(1);
	reg1.AddSNP(2);
	reg1.AddSNP(3);
	Utility::IdCollection ids;
	ids.insert(1);				// 1 -> 1
	ids.insert(3);				// 1 -> 3
	reg1.AddMetaIDs(MetaGroup::DiseaseIndependent, ids);
	ids.clear();
	ids.insert(2);				// 2 -> 2
	reg1.AddMetaIDs(MetaGroup::DiseaseDependent, ids);
	reg1.AddAliases("r1,1,region1");

	Region reg2("reg2", MetaGroup::DiseaseDependent, 80, 180, 110, 165);
	reg2.AddSNP(3);
	reg2.AddSNP(4);
	reg2.AddSNP(5);
	reg2.AddSNP(8);
	reg2.AddSNP(9);
	ids.clear();
	ids.insert(1);
	reg2.AddMetaIDs(MetaGroup::DiseaseIndependent, ids);
	ids.clear();
	ids.insert(4);
	ids.insert(5);
	reg2.AddMetaIDs(MetaGroup::DiseaseDependent, ids);
	reg2.AddAliases("r2,2");

	std::ofstream file("regions-test.bin", std::ios::binary);
	reg1.WriteToArchiveBinary(file);
	reg2.WriteToArchiveBinary(file);
	file.close();

	std::ifstream infile("regions-test.bin", std::ios::binary);
	Region r1, r2;
	r1.LoadFromArchiveBinary(infile);
	r2.LoadFromArchiveBinary(infile);

	EXPECT_EQ("reg1", r1.name);
	EXPECT_TRUE(r1.IsPresent(1));
	EXPECT_TRUE(r1.IsPresent(2));
	EXPECT_TRUE(r1.IsPresent(3));
	EXPECT_FALSE(r1.IsPresent(4));
	EXPECT_EQ("r1:1:region1", Utility::Join(r1.aliases, ":"));
	EXPECT_EQ(3, r1.aliases.size());
	EXPECT_EQ(2, r1.groups[MetaGroup::DiseaseIndependent].size());
	EXPECT_EQ(1, r1.groups[MetaGroup::DiseaseDependent].size());
	EXPECT_EQ(1, *(r1.groups[MetaGroup::DiseaseIndependent].begin()));
	EXPECT_EQ(2, *(r1.groups[MetaGroup::DiseaseDependent].begin()));

	EXPECT_EQ("reg2", r2.name);
	EXPECT_TRUE(r2.IsPresent(3));
	EXPECT_TRUE(r2.IsPresent(4));
	EXPECT_TRUE(r2.IsPresent(5));
	EXPECT_TRUE(r2.IsPresent(8));
	EXPECT_FALSE(r2.IsPresent(1));
	EXPECT_EQ("r2=2", Utility::Join(r2.aliases, "="));
	EXPECT_EQ(2, r2.aliases.size());
	EXPECT_EQ(1, r2.groups[MetaGroup::DiseaseIndependent].size());
	EXPECT_EQ(2, r2.groups[MetaGroup::DiseaseDependent].size());
	EXPECT_EQ(1, *(r2.groups[MetaGroup::DiseaseIndependent].begin()));
	EXPECT_EQ(4, *(r2.groups[MetaGroup::DiseaseDependent].begin()));

	remove("regions-test.bin");
}


TEST(RegionTest, Aliases) {
	Region reg1("reg1", 1);
	reg1.AddAliases("Region 1,b9028,asdf,asd");

	EXPECT_EQ(4, reg1.aliases.size());
	EXPECT_EQ("Region 1", reg1.aliases[0]);
	EXPECT_EQ("b9028", reg1.aliases[1]);
	EXPECT_EQ("asdf", reg1.aliases[2]);

	Utility::StringArray strings;
	strings.push_back("1");
	strings.push_back("2");
	strings.push_back("3");
	EXPECT_EQ("1", strings[0]);
	EXPECT_EQ("2", strings[1]);
	EXPECT_EQ("3", strings[2]);

	strings.clear();
	strings = Utility::Split("Region 1,b9028,asdf,b9028", ",");
	EXPECT_EQ("Region 1", strings[0]);
	EXPECT_EQ("b9028", strings[1]);
	EXPECT_EQ("asdf", strings[2]);
	EXPECT_EQ("b9028", strings[3]);


}

#endif // TEST_APP

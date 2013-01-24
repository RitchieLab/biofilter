/* 
 * File:   ldSplineImporter.h
 * Author: torstees
 *
 * Created on September 17, 2010, 1:50 PM
 */

#ifndef LDSPLINEIMPORTER_H
#define	LDSPLINEIMPORTER_H

#include "ldspline/ldspline.h"
using namespace Spline;

#include <sqlite3.h>
#include <string>
#include <map>
#include <vector>
#include <iostream>
#include <iomanip>

using std::string;
using std::map;
using std::vector;
using std::cerr;

class LdSplineImporter {
private:

	struct PopulationSpline {
		string name;					///< CEU/JPT/etc
		string desc;					///< comment to help inform users who might not be familiar with the 3 letter names
		string filename;			///< The filename associated with the splines

		string GetPopulationName(const string& statType, float value) const{
			std::stringstream ss;
			ss<<name<<"-"<<statType<<std::setiosflags(std::ios::fixed|std::ios::showpoint)<<std::setprecision(2)<<value;
			return ss.str();
		}
		PopulationSpline(std::string name, std::string desc, std::string filename) : name(name), desc(desc), filename(filename) {}
	};

	struct RegionBoundary {
		int geneID;
		int lower;
		int upper;
		string chrom;
		RegionBoundary(int geneID, std::string chrom, int lower, int upper) : geneID(geneID), lower(lower), upper(upper), chrom(chrom) {}
	};

public:

	LdSplineImporter(const string& config_fn, const string& db_fn);

	LdSplineImporter(const string& config_fn, sqlite3 *db_conn);

	// Load the population given a DB connection
	void loadPops();

	~LdSplineImporter();


private:
	/**
	 * @brief Parse configuration
    * @param filename
	 *
	 * Example:
	 * rs 0.9 0.8 0.6
	 * dp 0.9 0.8 0.6
	 * CEU /path/to/ceu.ldspline Descriptive note about CEU population
	 * JPT /path/to/jpg.ldspline Descriptive note about the population
	 * ...
    */
	void LoadConfiguration(const char *filename);
	void ProcessLD(LocusLookup& chr, const PopulationSpline& sp, const map<string, int>& popIDs);
	void LoadGenes(const string& chrom);
	void InitPopulationIDs(std::map<std::string, int>& popIDs,
			const PopulationSpline& sp, const string& type, const vector<float>& stats);

	std::vector<PopulationSpline> splines;			///<population -> ldspline filename
	std::vector<float> dp;								///<The various DPrime values we are splining on
	std::vector<float> rs;								///<The various RSquared values we are splining on
	std::vector<RegionBoundary> regions;

	// Database connection
	sqlite3* _db;
	bool _self_open;
	bool _write_db;
	string dbFilename;

	// DB processing functions
	static int parseGenes(void*, int, char**, char**);
	static int parseSingleInt(void*, int, char**, char**);

};


#endif	/* LDSPLINEIMPORTER_H */


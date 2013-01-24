/* 
 * File:   ldsplineimporter.cpp
 * Author: torstees
 * 
 * Created on September 17, 2010, 1:50 PM
 */
#include "ldsplineimporter.h"
#include "ldspline/ldspline.h"
#include <fstream>

#include <boost/filesystem.hpp>
#include <sys/stat.h>
#include <sstream>

#include <utility>

using std::pair;
using std::stringstream;

LdSplineImporter::LdSplineImporter(const string& fn, const string& db_fn) :
		_self_open(true), _write_db(false) {
	LoadConfiguration(fn.c_str());

	dbFilename = db_fn;
	boost::filesystem::path dbPath = boost::filesystem::path(db_fn);
	bool fileFound = false;
	if (boost::filesystem::is_regular_file(dbPath)) {
		fileFound = true;
	} else {
#ifdef DATA_DIR
		if (dbPath.is_relative()) {
			dbPath = (boost::filesystem::path(std::string(DATA_DIR))/=(dbPath));
			if (boost::filesystem::is_regular_file(dbPath)) {
				fileFound=true;
				dbFilename=dbPath.string();
			}
		}
#endif
	}

	if (!fileFound){
		throw std::runtime_error("DB File not found");
	}

	// At this point, try to get write permissions, if needed

	// BEGIN Non-portable code!!!
	struct stat results;
	bool throw_err = false;

	// If we do not currently have write access
	if (stat(dbPath.c_str(), &results)) {
		throw_err = true;
	} else if (!(results.st_mode & S_IWUSR)) {
		//set the write access
		if (!chmod(dbPath.c_str(), results.st_mode | S_IWUSR)) {
			//Whoo-hoo, it's writeable!
			_write_db = true;
		} else {
			throw_err = true;
			//Uh-oh, can't set the write bit.  Time to throw an error!
		}
	} // Hidden else means that we found that write bit was already set; noop

	// END Non-portable code
	if (throw_err) {
		throw std::runtime_error("Cannot write to Database");
	}

	// Create the sqlite3 db object
	sqlite3_open(dbFilename.c_str(), &_db);
}

LdSplineImporter::LdSplineImporter(const string& fn, sqlite3 *db_conn) :
		_db(db_conn), _self_open(false), _write_db(false) {
	LoadConfiguration(fn.c_str());
}

LdSplineImporter::~LdSplineImporter() {
	if (_self_open) {
		sqlite3_close(_db);
	}

	// If we set the write bit, it's now time to unset it
	if(_write_db){
		struct stat results;
		if(!stat(dbFilename.c_str(), &results)){
			chmod(dbFilename.c_str(), results.st_mode & (~S_IWUSR));
		}
	}
}

void LdSplineImporter::loadPops() {

	vector<PopulationSpline>::const_iterator spItr = splines.begin();
	vector<PopulationSpline>::const_iterator spEnd = splines.end();

	while (spItr != spEnd) {
		map<string, int> popIDs;
		InitPopulationIDs(popIDs, *spItr, "DP", dp);
		InitPopulationIDs(popIDs, *spItr, "RS", rs);

		LdSpline ldspline;
		ldspline.OpenBinary(spItr->filename.c_str());

		map<string, LocusLookup> chromosomes =
				ldspline.GetChromosomes();
		map<string, LocusLookup>::iterator chr = chromosomes.begin();
		map<string, LocusLookup>::iterator end = chromosomes.end();

		while (chr != end) {
			LoadGenes(chr->second.Chromosome());
			ProcessLD(chr->second, *spItr, popIDs);
			chr->second.Release();
			chr++;
		}
		spItr++;
	}

}

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
void LdSplineImporter::LoadConfiguration(const char *filename) {
	std::ifstream file(filename);
	while (file.good() && !file.eof()) {
		char line[4096];
		file.getline(line, 4096);

		std::stringstream ss(line);

		std::istream_iterator<std::string> itr(ss);

		std::vector<std::string> tokens(itr,
				std::istream_iterator<std::string>());

		if (tokens.size() > 0) {
			if (tokens[0] == "rs" || tokens[0] == "RS") {
				std::vector<std::string>::iterator values = tokens.begin();
				std::vector<std::string>::iterator tokenEnd = tokens.end();
				while (++values != tokenEnd) {
					rs.push_back(atof(values->c_str()));
					cerr << "RS: " << *values << "\t" << rs[rs.size() - 1]
							<< "\n";

				}
				cerr << rs.size() << "Total RS values to be used.";
			} else if (tokens[0] == "dp" || tokens[0] == "DP") {
				std::vector<std::string>::iterator values = tokens.begin();
				std::vector<std::string>::iterator tokenEnd = tokens.end();
				while (++values != tokenEnd) {
					dp.push_back(atof(values->c_str()));
					cerr << "DP: " << *values << "\t" << dp[dp.size() - 1]
							<< "\n";
				}
				cerr << dp.size() << "Total DP values to be used.";
			} else {
				if (tokens[0][0] != '#') {
					std::stringstream ss(line);
					std::string pop, popFilename, word;
					ss >> pop >> popFilename;

					std::stringstream desc;
					int k=-1;
					while (!ss.eof()) {
						ss >> word;
						if(++k){
							desc << " ";
						}
						desc << word;
					}

					splines.push_back(
							PopulationSpline(pop, desc.str(), popFilename));
				}
			}
		}
	}
}

void LdSplineImporter::ProcessLD(LocusLookup& chr,
		const PopulationSpline& sp, const map<std::string, int>& popIDs) {

	vector<RegionBoundary>::const_iterator regItr = regions.begin();
	vector<RegionBoundary>::const_iterator regEnd = regions.end();

	cerr << chr.Chromosome() << "(";
	cerr.flush();
	map<std::string, int>::const_iterator pi = popIDs.begin();
	map<std::string, int>::const_iterator pe = popIDs.end();

	while (pi != pe) {
		cerr << pi->first << " ";
		cerr.flush();
		pi++;
	}

	int incCount = 0;

	while (regItr != regEnd) {
		int lower = regItr->lower, upper = regItr->upper;
		//cerr<<"\t--"<<regItr->geneID<<"\n";
		vector<float>::const_iterator vItr = dp.begin();
		vector<float>::const_iterator vEnd = dp.end();

		while (vItr != vEnd) {
			map<string, int>::const_iterator pop_itr = popIDs.find(
					sp.GetPopulationName("DP", *vItr));
			if (pop_itr != popIDs.end()) {
				int popID = (*pop_itr).second;

				pair<int, int> bounds = chr.GetRangeBoundariesDP(lower, upper,
						*vItr);
				if (bounds.first != lower || bounds.second != upper) {
					incCount++;
					stringstream query_ss;
					query_ss << "UPDATE region_bounds SET start="
							<< bounds.first << ", end=" << bounds.second
							<< " WHERE gene_id=" << regItr->geneID
							<< " AND population_id=" << popID << ";";

					sqlite3_exec(_db, query_ss.str().c_str(), NULL, NULL, NULL);
				}
			}
			vItr++;
		}

		vItr = rs.begin();
		vEnd = rs.end();
		while (vItr != vEnd) {
			map<string, int>::const_iterator pop_itr = popIDs.find(
					sp.GetPopulationName("RS", *vItr));
			if (pop_itr != popIDs.end()) {
				int popID = (*pop_itr).second;
				pair<int, int> bounds = chr.GetRangeBoundariesRS(lower, upper,
						*vItr);
				if (bounds.first != lower || bounds.second != upper) {
					incCount++;
					stringstream query_ss;
					query_ss << "UPDATE region_bounds SET start="
							<< bounds.first << ", end=" << bounds.second
							<< "WHERE gene_id=" << regItr->geneID
							<< " AND population_id=" << popID << ";";

					sqlite3_exec(_db, query_ss.str().c_str(), NULL, NULL, NULL);
				}
			}
			vItr++;
		}

		regItr++;

	}
	cerr << ")\t" << incCount << "\n";
}

void LdSplineImporter::InitPopulationIDs(map<string, int>& popIDs,
		const PopulationSpline& sp, const string& type, const vector<float>& stats) {

	vector<float>::const_iterator sItr = stats.begin();
	vector<float>::const_iterator sEnd = stats.end();

	sItr = stats.begin();
	while (sItr != sEnd) {
		cerr << "Initializing Population: " << type << ", " << *sItr << "\t"
				<< stats.size() << "\n";
		string popName = sp.GetPopulationName(type, *sItr);

		string pop_query = "SELECT population_id FROM populations where population_label='"+popName+"';";
		int popID = -1;

		sqlite3_exec(_db, pop_query.c_str(), parseSingleInt, &popID, NULL);

		if (popID > 0) {
			cerr << "Clearing out all bounds associated with population"
					<< popID << " (" << popName << ")\n";

			stringstream del_ss;
			del_ss << "DELETE FROM populations WHERE population_id=" << popID <<"; ";
			del_ss << "DELETE FROM region_bounds WHERE population_id=" << popID <<";";

			sqlite3_exec(_db, del_ss.str().c_str(), NULL, NULL, NULL);

		} else {
			pop_query = "SELECT MAX(population_id) FROM populations;";
			sqlite3_exec(_db, pop_query.c_str(), parseSingleInt, &popID, NULL);
			popID++;
		}

		stringstream pop_ins_ss;
		pop_ins_ss << "INSERT INTO populations VALUES (" << popID << ",'"
				<< popName << "','" << sp.desc << "','"
				<< sp.desc << " with " << type << " cutoff " << *sItr << "');";

		sqlite3_exec(_db, pop_ins_ss.str().c_str(), NULL, NULL, NULL);

		stringstream bds_ins_ss;
		bds_ins_ss << "INSERT INTO region_bounds SELECT gene_id, " << popID
				<< ", start, end FROM region_bounds WHERE population_id=0";

		sqlite3_exec(_db, bds_ins_ss.str().c_str(), NULL, NULL, NULL);

		popIDs[popName] = popID;
		sItr++;
	}
}

void LdSplineImporter::LoadGenes(const string& chrom) {

	stringstream query_ss;
	query_ss << "SELECT gene_id, chrom, start, end FROM regions NATURAL JOIN "
			<< "region_bounds WHERE population_id=0 AND chrom='" << chrom
			<< "' ORDER BY start;";

	regions.clear();
	sqlite3_exec(_db, query_ss.str().c_str(), parseGenes, &regions, NULL);

	cerr << "Total Regions: " << regions.size() << "\n";
}

int LdSplineImporter::parseGenes(void* obj, int n_cols, char** col_vals, char** col_names){
	if(n_cols != 4){
		return 2;
	}
	vector<RegionBoundary>* result = (vector<RegionBoundary>*) obj;
	result->push_back(RegionBoundary(atoi(col_vals[0]), col_vals[1], atoi(col_vals[2]), atoi(col_vals[3])));
	return 0;

}

int LdSplineImporter::parseSingleInt(void* pop_id, int n_cols, char** col_vals, char** col_names){
	if(n_cols !=  1){
		return 2;
	}

	int* result = (int*) pop_id;
	(*result) = atoi(col_vals[0]);
	return 0;
}

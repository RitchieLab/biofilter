#include <iostream>
#ifdef USE_MPI
#include <mpi.h>
#endif

#include "main.h"
#include "timestamp.h"
#include "utility/exception.h"
#include "config.h"

#include <algorithm>

#include <boost/filesystem.hpp>

namespace Biofilter {


void Main::LoadConfiguration(const char *cfgFilename) {
	cfg.SetValue("REPORT_PREFIX", Utility::ExtractBaseFilename(cfgFilename));
	cfg.Parse(cfgFilename);
}


void Main::InitRegionData() {
	Utility::StringArray missingAliases;
	Utility::StringArray aliasList;

	app.LoadRegionData(cfg.GetLine("POPULATION").c_str(), missingAliases, aliasList);
}

void Main::InitGroupData() {
	//User defined groups
	Utility::StringArray udGroups;
	cfg.GetLines("ADD_GROUP", udGroups);

	//Any specialized searches are defined here
	Utility::IdCollection ids = Utility::ToSet<uint>(cfg.GetLine("INCLUDE_GROUPS").c_str(), ", \t");

	cfg.LoadFileContents("INCLUDE_GROUP_FILE", ids);

	//Now, let's do the same for names
	Utility::StringArray groups = Utility::Split(cfg.GetLine("INCLUDE_GROUP_NAMES").c_str(), ", \t");
	cfg.LoadFileContents("INCLUDE_GROUP_NAME_FILE", groups);
	app.LoadGroupDataByName(udGroups, groups, ids);

}

void Main::RunCommands() {
	app.InitBiofilter(cfg.GetLine("SETTINGS_DB").c_str(), !silentRun, _write_db);
	std::string genomicBuild = cfg.GetString("GENOMIC_BUILD");
	if (genomicBuild != "") {
		app.LoadBuildConverter(genomicBuild.c_str());
	}
	switch (action) {
		case BiofilterAction::SetVariationFilename:
		{
			app.SetVariationFilename(cfg.GetLine("VARIATIONS_FILENAME").c_str());
			return;
		}
		case BiofilterAction::RunGeneCoverage: {
			Utility::StringArray rsList;
			cfg.GetLines("COVERAGE_RS", rsList);
			Utility::StringArray mapList;
			cfg.GetLines("COVERAGE_MAP", mapList);
			std::string geneFile = cfg.GetLine("GENE_COVERAGE");
			app.GeneCoverage(rsList, mapList, geneFile.c_str(), cfg.GetLine("POPULATION").c_str());
			return;
		}
		case BiofilterAction::ListGroups:
			{
				std::vector<std::string> keywords;
				std::string s = cfg.GetLine("GROUP_SEARCH_CRITERIA");
				std::transform(s.begin(), s.end(), s.begin(), (int(*)(int)) std::toupper);
				if (s != "ALL")
					keywords = Utility::Split(s.c_str(), ",");
				
				app.ListGroupIDs(std::cout, keywords);
			} return;
		case BiofilterAction::ListPopulationIDs:
			app.ListPopulationIDs(std::cout);
			return;
		case BiofilterAction::ListGenes: {
			std::string s = cfg.GetLine("GENE_COVERAGE");
			Utility::StringArray aliasList;
			if (s != "ALL")
				aliasList = Utility::Split(s.c_str(), ",");
			Utility::StringArray aliasTypeList;
			s = cfg.GetLine("ALIAS_TYPES");
			if (s != "ALL")
				aliasTypeList = Utility::Split(s.c_str(), ",");

			app.ListGenes(std::cout, aliasList, aliasTypeList);
			return;
		}
		case BiofilterAction::ImportLdSplines: {
			std::string s = cfg.GetLine("LD_CONFIGURATION");
			app.LoadLdSpline(s.c_str());
			return;
		}
		case BiofilterAction::ListMetaGroups:
			{
/*				InitGroupData();
				app.ListMetaGroups(cout);
*/			}
			return;
		default: {}
	}
	
	//Tasks that run before SNPs load (not sure what those would be)
	cfg.RunTasks(0);

	LoadSNPs();

	Utility::StringArray genes;
	std::string geneFilename = cfg.GetLine("GENE_COVERAGE");
	if (geneFilename != "")
		cfg.LoadFileContents("GENE_COVERAGE", genes);
	/**
	 * Do the SNP oriented stuff here
    */
	cfg.RunTasks(1);

	InitRegionData();
	std::multimap<uint, uint> geneLookup = app.BuildSnpGeneMap();

	cfg.RunTasks(2);

	InitGroupData();

	cfg.RunTasks(3);

	//We need to make sure that there is one or more tasks at level four before we generate the models
	if (cfg.CountTasks(4)) {
		app.ProduceModels(std::cout);
		cfg.RunTasks(4);
	}
}



bool Main::ParseCmdLine(int argc, char **argv) {

	//Test the DB connection
#ifdef USE_MPI
	MPI::Init(argc, argv);
#endif
	if (argc < 2) {
		PrintHelp();
		return false;
	}
	int i=1;
	cfg.Init();
	if (argv[1][0] != '-')
		LoadConfiguration(argv[i++]);
	//Work out any other cmd line arguments
	for (; i<argc && i>0;) {
		i=ParseCmd(i, argc, argv);
	}
	cfg.ExecuteConfiguration(&app);
	app.SetReportPrefix(cfg.GetLine("REPORT_PREFIX").c_str());

	if (action == BiofilterAction::ParseError) {
		return false;
	}
	if (action == BiofilterAction::PrintSampleConfig) {
		PrintBanner();
		std::cout<<"#Biofilter configuration file\n";
		std::cout<<"#\n#\n#This file was generated by " << PACKAGE_STRING << "\n";
		std::cout<<"#\n#Users can change these parameters to meet their needs.\n";
		std::cout<<"#Please see the manual for more information about the different parameters and their options.\n";
		cfg.Write(std::cout);
		return false;
	}

	if (!silentRun)
		cfg.ReportConfiguration(std::cerr);

	return true;
}

void Main::PrintBanner()  {
	if (!silentRun) {
		std::cerr<<PACKAGE_STRING<<"\n";
#ifdef USE_MPI
		std::cerr<<"* This application is compiled to run on parallel computing systems using MPI\n";
#else
		std::cerr<<"* (serial)\n";
#endif
		std::cerr<<"\nMarylyn Ritchie, William Bush and Eric Torstenson\nPlease forward any comments or errors to " << PACKAGE_BUGREPORT <<"\n\n";
	}
}

void Main::PrintHelp() {
	silentRun = false;
	PrintBanner();
#ifdef USE_MPI
	std::cerr<<"usage: biofilter <configuration file> [ [command] ...] [ [parameter] ...]\n";
#else
	std::cerr<<"usage: biofilter <configuration file> [OPTIONS]\n";
#endif
	std::cerr<<"\nbiofilter is a standalone application for use in investigating possible SNP associations\n"
	           "\tin a set of data which, through biological knowledge, might be worth investigating\n";
	std::cerr<<"Optional Commands Include:\n";
	std::cerr<<"\t-S [--sample-config]                       -- Print sample configuration to std-out\n";
	std::cerr<<"\t--report-gene-coverage                     -- Reports the number of markers in each gene in the \n"
	         <<"\t                                              given gene list\n";
	std::cerr<<"\t-G [--groups] <label|ALL>                  -- Prints the groups from the LOKI database matching the given \n"
			 <<"\t                                              comma-separated criteria.\n";
	std::cerr<<"\t--genes <label|ALL> <label|ALL>            -- Prints the genes from the LOKI database mathing the given \n"
			 <<"\t                                              comma-separated criteria and type.\n";
	std::cerr<<"\t-P [--list-populations]                    -- Lists all available Population based LD boundary options\n";

	//LD-SPLINE import here!

	std::cerr<<"\nOptional Parameters Include:\n";
	std::cerr<<"\t--DB <filename>                            -- Uses the given file as the LOKI database\n";
	std::cerr<<"\t--list-genes                               -- Lists all genes that are covered by at least one SNP\n";
	std::cerr<<"\t--marker-info                              -- Reports each SNP and it's position/chromosome\n"
		     <<"\t                                              in a format acceptable by haploview\n";
	std::cerr<<"\t-b [--binary] <yes/no>                     -- Overrides binary setting in configuration file\n";
	std::cerr<<"\t-D [--detailed]                            -- Adds extra details to output reports\n";
	std::cerr<<"\t--cov-rs  <filename>                       -- Add a platform to coverage report list (Using RSIDs)\n";
	std::cerr<<"\t--cov-map <filename>                       -- Add a platform to coverage report list (Using BP Locations)\n";
	std::cerr<<"\t-d [--add-group] <filename>                -- Adds a meta group containing data from the given file\n";
	std::cerr<<"\t-g [--gene-file] <filename|ALL>            -- File containing one or more gene alias (or ALL) to be used\n"
	         <<"\t                                              in conjunction with gene reports\n";
	std::cerr<<"\t--snp-report                               -- Reports all genes each SNP is found in (from genes listed \n"
	         <<"\t                                              in file or all known to biofilter)\n";
	std::cerr<<"\t--map-snps-to-gene                         -- Reports all genes each SNP is found along with information\n"
	         <<"\t                                              describing the SNPs relationship to that gene (INTERIOR, etc)\n";
	std::cerr<<"\t-B [--build] <label>                       -- Define the build associated with map files (35, 36, 37)\n";
	std::cerr<<"--PREFIX <label>                             -- Set the report prefix.\n";
	std::cerr<<"\t-s [--snps] <filename>                     -- Override the snp source file on the commandline\n";
	std::cerr<<"\t-p [--set-population] <label>              -- Override the configurations population setting (NO-LD, CEUDP1.0, etc)\n";
	std::cerr<<"\t--gene-boundary <integer>                  -- Extends a gene by the given number of base pairs (NO-LD population only)\n";
	std::cerr<<"\t-v [--variants] <filename>                 -- Override the map source file (this takes precedence over --snps\n";
	std::cerr<<"\t-W [--write-models] <float> <integer>      -- Writes gene/gene model list to files limitted to those with given minimum\n"
	         <<"\t                                              implication or greater with a given maximum number of snp-snp models\n";
	std::cerr<<"\t-X [--export-snp-models] <float> <integer> -- Writes SNP/SNP Models to file. This assumes a pre-existing \n"
	         <<"\t                                              gene-gene model file \n";




// The options below are either old or unknown
//
//	std::cerr<<"\t--filter-by-genes gene-list-filename       -- Lists gene name and rsid for each SNP inside each gene.\n"
//	         <<"\t                                              gene names and rsids can both appear multiple times.\n";
//	std::cerr<<"\t--inject-gene-information analysis-results chrom-col rs-col gene-list  \n"
//	         <<"\t                                              Injects gene(s) at the end of the CSV file and writes the\n"
//	         <<"\t                                              combined data to a new file.\n";
//
//	std::cerr<<"\t--genes alias_list alias_type              -- Lists all genes present in the database that match one of the comma \n"
//	         <<"\t                                              separated. Either or both can also be ALL, which will show them all. \n";
//	std::cerr<<"\t--model-report  model-list-filename        -- Generates a report containing the Genes and groups\n"
//	         <<"\t                                              associated with each two snp model listed in the file\n";
//
//
//
//
//
//	std::cerr<<"\t-m [--show-models]                         -- Writes contents of model file to screen in human\n"
//	         <<"\t                                              readable form\n";
//	std::cerr<<"\t-l [--load-ld] <ld filename>               -- Loads LD information from the file, filename, and\n"
//	         <<"\t                                              adjusts the gene boundaries accordingly\n";
//
//
//	std::cerr<<"\t-G [--list-groups] [criteria]              -- Adds group search criteria and produces a list of\n"
//	         <<"\t                                               group IDs that match the criteria\n";
//	std::cerr<<"\t-h [--html-reports] yes/no                 -- Turns HTML Reporting on/off\n";
//	std::cerr<<"\t-q [--quiet]                               -- Silences general output during processing. Reports and errors are still produced\n";
//
//
//
//	std::cerr<<"\t--optimize                                 -- Updates internal structures to allow faster access. This\n"
//	         <<"\t                                              is usually done prior to release\n";
//	std::cerr<<"\t--strip-optimization                       -- Strips the optimization out (this is helpful to allow data\n"
//	         <<"\t                                              imports to run more quickly) \n";
//	std::cerr<<"\t--ldspline ldconfig                        -- Imports LD-Spline variations using ldconfig as a guide\n";
//	std::cerr<<"\t--fix-variations var-filename-path         -- Sets the path (and filename) to the appropriate variation file.\n";
//	std::cerr<<"\t                                              This should only be done if the file needs to be moved to a new location.\n";
}

void Main::LoadSNPs() {
	std::string snpFilename = cfg.GetLine("MAP_SOURCE");
	uint snpsLoaded = 0;
	if (snpFilename.size() > 0) {
		Knowledge::SnpDataset lostSnps;
		std::string genomicBuild = cfg.GetLine("GENOMIC_BUILD");
		snpsLoaded = app.LoadMapData(snpFilename.c_str(), genomicBuild.c_str(), lostSnps);
		std::cerr<<"Map Source Loaded: "<<snpsLoaded<<" snps loaded. \n";
		std::cerr<<lostSnps.Size()<<" SNPs were not able to be found in the variations database.\n";
		//TODO Right now, I'm not sure it matters about these lost SNPs. What is important are the ones
		//that are lost because of genomic build translation. Otherwise, the only real loss is some
		//annotation.
	} else {
		//Load RS IDs into a string array (1 on each line)
		std::set<std::string> lostSnps;
		snpFilename = cfg.GetLine("RS_SOURCE");
		if (snpFilename.size() > 0) {
			app.LoadSnpsSource(snpFilename.c_str(), lostSnps);

			if (lostSnps.size() > 0) {
				std::string lostSnpFilename = app.AddReport("missing-snps", "txt", "SNPs missing from variations file");
				std::ofstream file(lostSnpFilename.c_str());
				file<<"The following SNPs were unable to be found in the variations file:\n\t"<<Utility::Join(lostSnps, "\n\t")<<"\n";
			}
		}
	}
}

void Main::InitGroups() {

}

int Main::SetConfigValue(int nextCmd, int argc, const char *var, const char *val, const char *err) {
	if (nextCmd < argc) 
		cfg.SetValue(var, val);
	else {
		action = BiofilterAction::ParseError;
		std::cerr<<err<<"\n";
		return -1;
	}
	return nextCmd + 1;
}

int Main::ParseCmd(int curr, int argc, char **argv) {
	int nextCmd = curr+1;
	if (strcmp(argv[curr], "-h")==0 || strcmp(argv[curr], "--help")==0){
		PrintHelp();
		action = BiofilterAction::ParseError;
		return -1;
	}
	if (strcmp(argv[curr], "-S")==0 || strcmp(argv[curr], "--sample-config")==0) {
		action = BiofilterAction::PrintSampleConfig;
		return nextCmd;
	}
	if (strcmp(argv[curr], "--DB")==0)
		return SetConfigValue(nextCmd, argc, "SETTINGS_DB", argv[nextCmd], "--DB must be followed by a database filename");
	if (strcmp(argv[curr], "--marker-info")==0) {
		cfg.SetValue("MARKER_INFO_REPORT", "ON");
		return nextCmd;
	}
	if (strcmp(argv[curr], "-b")==0 || strcmp(argv[curr], "--binary")==0)
		return SetConfigValue(nextCmd, argc, "BINARY_MODEL_ARCHIVE", argv[nextCmd], "--binary must be followed by Yes/No");
	if (strcmp(argv[curr], "-P")==0 || strcmp(argv[curr], "--list-populations")==0) {
		action = BiofilterAction::ListPopulationIDs;
		return nextCmd;
	}
	if (strcmp(argv[curr], "-D") == 0 || strcmp(argv[curr], "--detailed")==0) {
		cfg.SetValue("DETAILED_REPORTS", "ON");
		return nextCmd;
	}
	if (strcmp(argv[curr], "--report-gene-coverage")==0) {
		action = BiofilterAction::RunGeneCoverage;
		return nextCmd;
	}
	if (strcmp(argv[curr], "--cov-rs")==0) {
		if (nextCmd < argc)
			cfg.AppendValue("COVERAGE_RS", argv[nextCmd]);
		else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--cov-rs must be followed by a filename containing RS Numbers representing a platform\n";
			return -1;
		}
		return nextCmd + 1;

	}
	if (strcmp(argv[curr], "--cov-map")==0) {
		if (nextCmd < argc)
			cfg.AppendValue("COVERAGE_MAP", argv[nextCmd]);
		else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--cov-map must be followed by a filename containing map entries representing a platform\n";
			return -1;
		}
		return nextCmd + 1;
	}
	if (strcmp(argv[curr], "-d")==0 || strcmp(argv[curr], "--add-group")==0) {
		if (nextCmd < argc) {
			cfg.AppendValue("ADD_GROUP", argv[nextCmd++]);
		}
		else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--add-group must be followed by a filename\n";
			return -1;
		}
		return nextCmd;
	}
	if (strcmp(argv[curr], "-g")==0 || strcmp(argv[curr], "--gene-file")==0) {
		if (nextCmd < argc) {
			cfg.SetValue("GENE_COVERAGE", argv[nextCmd++]);
		}
		else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--gene-file must be followed by a filename containing a list of genes.\n";
			return -1;
		}
		return nextCmd;
	}
	if (strcmp(argv[curr], "--list-genes")==0)  {
		cfg.SetValue("GENE_REPORT", "ON");
		return nextCmd;
	}
	if (strcmp(argv[curr], "--snp-report")==0)  {
		cfg.SetValue("SNP_REPORT", "ON");
		return nextCmd;
	}
	if (strcmp(argv[curr], "--map-snps-to-gene")==0)  {
		cfg.SetValue("SNP_GENE_MAP", "ON");
		return nextCmd;
	}
	if (strcmp(argv[curr], "-G")==0 || strcmp(argv[curr], "--groups")==0) {
		if (argc > nextCmd) {
			silentRun = true;				// At this point, we don't care about the other stuff
			cfg.SetValue("LIST_GROUPS_FROM_DB", "ON");
			cfg.SetValue("GROUP_SEARCH_CRITERIA", argv[nextCmd++]);
			action = BiofilterAction::ListGroups;
			return -1;
		}
		else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--groups must include search criterion or ALL (to list all groups).\n";
			return -1;
		}
	}
	if (strcmp(argv[curr], "--genes")==0) {
		if (argc > nextCmd + 1) {
			silentRun = true;				// At this point, we don't care about the other stuff
			cfg.SetValue("LIST_GENES_FROM_DB", "ON");
			cfg.SetValue("GENE_COVERAGE", argv[nextCmd++]);
			cfg.SetValue("ALIAS_TYPES", argv[nextCmd++]);
			action = BiofilterAction::ListGenes;
			return -1;
		}
		else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--genes must include genes (comma separated) and alias type (comma separated). Either can be replaced by ALL.\n";
			return -1;
		}

	}
	if (strcmp(argv[curr], "-B")==0 || strcmp(argv[curr], "--build")==0) {
		if (nextCmd < argc) {
			cfg.SetValue("GENOMIC_BUILD", argv[nextCmd++]);
			return nextCmd;
		} else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--build must be followed by an appropriate build number (35, 36, etc.)\n";
			return -1;
		}
	}
	if (strcmp(argv[curr], "--PREFIX")==0)
		return SetConfigValue(nextCmd, argc, "REPORT_PREFIX", argv[nextCmd], "--PREFIX must be followed by prefix to be prepended to the generated filenames");
	if (strcmp(argv[curr], "-s")==0 || strcmp(argv[curr], "--snps")==0)
		return SetConfigValue(nextCmd, argc, "RS_SOURCE", argv[nextCmd], "--snps must be followed by the name of a file containing RS Numbers to describe the target dataset");
	if (strcmp(argv[curr], "-p")==0 || strcmp(argv[curr], "--set-population")==0)
		return SetConfigValue(nextCmd, argc, "POPULATION", argv[nextCmd], "--set-population must be followed by name population you wish to use");
	if (strcmp(argv[curr], "--gene-boundary")==0)
		return SetConfigValue(nextCmd, argc, "GENE_BOUNDARY_EXTENSION", argv[nextCmd], "--gene-boundary must be followed by an integer describing the number of bases");
	if (strcmp(argv[curr], "-v")==0 || strcmp(argv[curr], "--variants")==0) {
		if (nextCmd < argc && argv[nextCmd][0] != '-') {
			cfg.SetValue("MAP_SOURCE", argv[nextCmd++]);
			return nextCmd;
		} else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--variants requires the variant file.\n";
			return -1;
		}
	}
	if (strcmp(argv[curr], "--ldspline")==0) {
		action = BiofilterAction::ImportLdSplines;
		if (argc > nextCmd) {
			_write_db = true;
			cfg.SetValue("LD_CONFIGURATION", argv[nextCmd++]);
			return nextCmd;
		} else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--import-ld must be followed by the import configuration.\n";
			return -1;			
		}
		
	}
	if (strcmp(argv[curr], "-W")==0 || strcmp(argv[curr], "--write-models")==0) {
		//action = BiofilterAction::ProduceModels;
		if (nextCmd < argc-1 && argv[nextCmd][0] != '-') {
			cfg.SetValue("EXPORT_GENE_MODELS", "YES");
			cfg.SetValue("MINIMUM_IMPLICATION_INDEX", argv[nextCmd++]);
			cfg.SetValue("MAX_SNP_MODEL_COUNT", argv[nextCmd++]);
			return nextCmd;
		} else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--write-models requires implication index followed by the max SNP/SNP model count\n";
			return -1;
		}
	}
	if (strcmp(argv[curr], "-X")==0 || strcmp(argv[curr], "--export-snp-models")==0) {
		//action = BiofilterAction::ProduceModels;
		if (nextCmd < argc-1 && argv[nextCmd][0] != '-') {
			cfg.SetValue("EXPORT_SNP_MODELS", "YES");
			cfg.SetValue("MINIMUM_IMPLICATION_INDEX", argv[nextCmd++]);
			cfg.SetValue("MAX_SNP_MODEL_COUNT", argv[nextCmd++]);
			return nextCmd;
		} else {
			action = BiofilterAction::ParseError;
			std::cerr<<"--export-snp-models requires implication index followed by max SNP/SNP model count\n";
			return -1;
		}

	}
	action = BiofilterAction::ParseError;
	std::cerr<<"Unrecognized parameter: "<<argv[curr]<<"\n";
	return -1;
}



}


int main(int argc, char *argv[]) {
	std::string cfgFilename;

	Biofilter::Main *app = new Biofilter::Main();					///<The application object

	int retval = 0;

	if (!app->ParseCmdLine(argc, argv)) {
		delete app;
		exit(1);
	}
	//Performs any commands
	try {
		app->RunCommands();
	}
	catch (Utility::Exception::General& e) {
		Biofilter::Application::errorExit = true;
		std::cerr<<"\nError: \t"<<e.GetErrorMessage()<<" Unable to continue.\n";
		retval = 1;
	}

	delete app;

	return retval;

}

//
// C++ Implementation: biofilter
//
// Description: 
//
//
// Author: Eric Torstenson <torstees@torstensonx.mc.vanderbilt.edu>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//
#include "biofilter.h"
#include <iostream>
#include "timestamp.h"
#include "ldcorrection.h"
#include <iomanip>
#include "genegenemodelreader.h"
#include "utility/strings.h"

using namespace std;
using namespace Utility;

namespace Biofilter {

Biofilter::Biofilter() : detailedCoverage(false), doLoadRegionAliases(false), doWriteModelCounts(false), ldConfiguration("") {
	action = BiofilterAction::NoAction;
}


Biofilter::~Biofilter()
{
	cout<<bioApp.GetReportLog()<<reportLog.str();
}


void Biofilter::PrintBanner()  {
	cout<<"biofilter "<<APPMAJOR<<"."<<APPMINOR<<"."<<APPBUGFIX<<" ("<<BUILD_NUMBER<<") "<<BUILD_TYPE<<"  "<<BUILD_DATE<<"\n";
#ifdef USE_MPI
	cout<<"* This application is compiled to run on parallel computing systems using MPI\n";
##else
	cout<<"* (serial)\n";
#endif
	cout<<"\nMarylyn Ritchie, William Bush and Eric Torstenson\nPlease forward any comments or errors to biofilter@chgr.mc.vanderbilt.edu\n\n";
}

void Biofilter::PrintHelp() {
	PrintBanner();
#ifdef USE_MPI
	cerr<<"usage: biofilter <configuration file> [ [command] ...] [ [parameter] ...]\n";
#else
	cerr<<"usage: biofilter <configuration file> \n";
#endif
	cerr<<"\nbiofilter is a standalone application for use in investigating possible SNP associations\n"
			"\tin a set of data which, through biological knowledge, might be worth investigating\n"; 
	cerr<<"Optional Commands Include:\n";
	cerr<<"\t-S [--sample-config]                       -- Print sample configuration to std-out\n";
	cerr<<"\t--report-gene-coverage gene-list-filename  -- Reports the snp count for the genes in genelist \n"
		<<"\t                                              for the snps in snp-source\n";
	cerr<<"\t--marker-info                              -- Reports each SNP and it's position/chromosome\n"
		<<"\t                                              in a format acceptable by haploview\n";
	cerr<<"\t--model-report  model-list-filename        -- Generates a report containing the Genes and groups\n"
		<<"\t                                              associated with each two snp model listed in the file\n";
	cerr<<"\nOptional Parameters Include:\n";
	cerr<<"\t-s [--snps] <snps filename>                -- Override the snp source file ont he commandline\n";
	cerr<<"\t-C [--coverage] <snps filename>            -- Add a file to coverage report list\n";
	cerr<<"\t-D [--detailed-coverage]                   -- (used with -C) adds extra details to coverage report\n";
	cerr<<"\t-X (--export-snp-models)	[model count] [min impl index]\n"
		<<"\t                                           -- Writes Snp-Snp Models to file. This assumes a pre-existing gene-gene model file \n";
	cerr<<"\t-W [--write-models] [min implication index] [model count]\n"
		<<"\t                                           -- Writes gene-gene models to file. Arguments are optional and produce snp-snp models\n";
	cerr<<"\t-m [--show-models] <model filename>        -- Writes contents of model file to screen in human\n"
		<<"\t                                              readable form\n";
	cerr<<"\t-l [--load-ld] <model filename>            -- Loads LD information from the file, filename, and\n"
		<<"\t                                              adjusts the gene boundaries accordingly\n";
	cerr<<"\t-d [--disease-dependent] <filename>        -- Adds a meta group containing data from the file, filename\n";
	cerr<<"\t-G [--list-groups] [criteria]              -- Adds group search criteria and produces a list of\n"
		<<"\t                                              group IDs that match the criteria\n";
	cerr<<"\t-P [--list-populations]                    -- Lists all available Population based LD boundary options\n";
	cerr<<"\t-h [--html-reports] yes/no                 -- Turns HTML Reporting on/off\n";
	cerr<<"\t-b [--binary] yes/no						-- Overrides binary setting in configuration file\n";
	cerr<<"\t--optimize                                 -- Updates internal structures to allow faster access. This\n"
		<<"\t                                              is usually done prior to release\n";
	cerr<<"\t--strip-optimization                       -- Strips the optimization out (this is helpful to allow data\n"
		<<"\t                                              imports to run more quickly) \n";
}


int Biofilter::ParseCmd(int curr, int argc, char **argv) {
	int nextCmd = curr+1;
	if (strcmp(argv[curr], "--report-gene-coverage") == 0) 
		if (nextCmd < argc) {
			cfg.SetValue("GENE_COVERAGE", argv[nextCmd++]);
			action = BiofilterAction::RunGeneCoverage;
		}
		else {
			action = BiofilterAction::ParseError;
			cerr<<"--report-gene-coverage must be followed the genelist filename\n";
			return -1;
		}
	else if (strcmp(argv[curr], "--snp-report") == 0)
		cfg.SetValue("SNP_REPORT", "YES");

		//action = BiofilterAction::ProduceSnpReport;
	else if (strcmp(argv[curr], "--model-report") == 0) {
		action = BiofilterAction::RunModelReport;
		if (nextCmd < argc)
			cfg.SetValue("SNPS_SOURCE", argv[nextCmd++]);
		else {
			action = BiofilterAction::ParseError;
			cerr<<"--model-report must be followed by the model file\n";
			return -1;
		}
	}
	else if (strcmp(argv[curr], "--list-associations") == 0)
		action = BiofilterAction::ListAssociations;
	else if (strcmp(argv[curr], "--graph-associations") == 0)
		action = BiofilterAction::GraphAssociations;
	else if (strcmp(argv[curr], "-D")==0 || strcmp(argv[curr], "--detailed-coverage")==0) 
		detailedCoverage = true;
	else if (strcmp(argv[curr], "-C")==0 || strcmp(argv[curr], "--coverage")==0) {
		if (nextCmd < argc)
			cfg.AppendValue("COVERAGE_SNPS", argv[nextCmd++]);
		else {
			action = BiofilterAction::ParseError;
			cerr<<"-C (--coverage) must be followed by a snp filename\n";
			return -1;
		}
	}
	else if (strcmp(argv[curr], "-G")==0 || strcmp(argv[curr], "--list-groups")==0) {
		action = BiofilterAction::ListGroups;
		if (nextCmd < argc) {
			cfg.AppendValue("GROUP_SEARCH_CRITERIA", argv[nextCmd++]);
		}
	}
	else if (strcmp(argv[curr], "-P")==0 || strcmp(argv[curr], "--list-population-ids")==0) {
		action = BiofilterAction::ListPopulationIDs;
	}
	else if (strcmp(argv[curr], "-d")==0 || strcmp(argv[curr], "--disease-dependent")==0) {
		if (nextCmd < argc)
			cfg.AppendValue("DISEASE_DEPENDENT", argv[nextCmd++]);
		else {
			action = BiofilterAction::ParseError;
			cerr<<"--disease-dependent must be followed by a filename\n";
			return -1;
		}
	}
	else if (strcmp(argv[curr], "-l")==0 || strcmp(argv[curr], "--load-ld")==0) {
		if (nextCmd < argc) {
			ldConfiguration = argv[nextCmd++];
		}
		else {
			action = BiofilterAction::ParseError;
			cerr<<"-l (--load-ld) must be followed by a ld configuration filename\n";
			return -1;
		}
	}
	else if (strcmp(argv[curr], "-b")==0 || strcmp(argv[curr], "--binary")==0) {
		if (nextCmd < argc)
			cfg.SetValue("BINARY_MODEL_ARCHIVE", argv[nextCmd++]);
		else {
			action = BiofilterAction::ParseError;
			cerr<<"-b (--binary) must be followed by an option: YES/NO\n";
			return -1;
		}
	}
	else if (strcmp(argv[curr], "-h")==0 || strcmp(argv[curr], "--html-reports")==0) {
		if (nextCmd < argc) 
			cfg.SetValue("HTML_REPORTS", argv[nextCmd++]);
		else {
			action = BiofilterAction::ParseError;
			cerr<<"-h (--html-reports) must be followed by an option: YES/NO\n";
			return -1;
		}
	}		
	else if (strcmp(argv[curr], "-S")==0 || strcmp(argv[curr], "--sample-config")==0)
		action = BiofilterAction::PrintSampleConfig;
	else if (strcmp(argv[curr], "-p")==0 || strcmp(argv[curr], "--print-count-estimates")==0)
		doWriteModelCounts = true;
	else if (strcmp(argv[curr], "-x")==0 || strcmp(argv[curr], "--export-snp-models")==0) {
		if (nextCmd < argc-1) {
			cerr<<"EXPORT MODELS\n";
			action = BiofilterAction::ExportSnpModels;
			cfg.SetValue("MINIMUM_IMPLICATION_INDEX", argv[nextCmd++]);
			cfg.SetValue("MAX_SNP_MODEL_COUNT", argv[nextCmd++]);
		}
		else {
			action = BiofilterAction::ParseError;
			cerr<<"-X (--export-snp-models) must be followed by 2 parameters: \n\t[minimum implication index] [max snp-snp model count]\n";
			return -1;
		}
	}
	else if (strcmp(argv[curr], "-W")==0 || strcmp(argv[curr], "--write-models")==0) {
		action = BiofilterAction::ProduceModels;
		if (curr < argc - 1 && argv[curr+1][0] != '-') {
			if (nextCmd < argc -1) {
				cfg.SetValue("EXPORT_SNP_MODELS", "YES");
				cfg.SetValue("MINIMUM_IMPLICATION_INDEX", argv[nextCmd++]);
				cfg.SetValue("MAX_SNP_MODEL_COUNT", argv[nextCmd++]);
			}
		}
	}
	else if (strcmp(argv[curr], "-m")==0 || strcmp(argv[curr], "--show-models")==0) {
		if (nextCmd < argc) {
			action = BiofilterAction::ListModels;
			cfg.SetValue("MODEL_FILENAME", argv[nextCmd++]);
		}
		else {
			action = BiofilterAction::ParseError;
			cerr<<"-m (--show-models) must be followed by a filename\n";
			return -1;
		}
	}
	else if (strcmp(argv[curr], "--marker-info")==0) 
		action = BiofilterAction::RunMarkerInfo;
	else if (strcmp(argv[curr], "--strip-optimization") ==0)
		action = BiofilterAction::StripOptimization;
	else if (strcmp(argv[curr], "--optimize")==0) {
		action = BiofilterAction::Optimize;
	}
	else if (strcmp(argv[curr], "-s")==0 || strcmp(argv[curr], "--snps" )==0)
		if (nextCmd < argc)
			cfg.SetValue("SNPS_SOURCE", argv[nextCmd++]);
		else {
			action = BiofilterAction::ParseError;
			cerr<<"-s (snps) must be followed by the snps filename\n";
			return -1;
		}
	else {
		action = BiofilterAction::ParseError;
		cerr<<"Unknown argument: "<<argv[curr]<<"\n";
		return -1;
	}

	return nextCmd;
}

bool Biofilter::ParseCmdLine(int argc, char **argv) {

	//Test the DB connection
#ifdef USE_MPI
	MPI::Init(argc, argv);
#endif
	if (argc < 2) {
		PrintHelp();
		return false;
	}
	int i=1;
	if (argv[1][0] != '-')
		LoadConfiguration(argv[i++]);
	//Work out any other cmd line arguments
	for (; i<argc && i>0;) {
		i=ParseCmd(i, argc, argv);
	}
	if (action == BiofilterAction::ParseError) {
		return false;
	}
	if (action == BiofilterAction::PrintSampleConfig) {
		cfg.Init();
		cfg.Write(cout);
		return false;
	}
	bioApp.SetReportPrefix(cfg.GetString("REPORT_PREFIX").c_str());
	bioApp.UseHtmlReports(cfg.GetBoolean("HTML_REPORTS"));
	bioApp.InitBiofilter(cfg.GetLine("SETTINGS_DB").c_str());

	doLoadRegionAliases = cfg.GetBoolean("LOAD_ALL_ALIASES");
	
	cfg.ReportConfiguration(cout);

	return true;
}

AppConfiguration *Biofilter::LoadConfiguration(const char *cfgFilename) {
	cfg.Init();
	cfg.SetValue("REPORT_PREFIX", ExtractBaseFilename(cfgFilename));
	if (cfgFilename) 
		cfg.Parse(cfgFilename);
	cfg.ExecuteConfiguration();
	if (cfgFilename)
		configFilename=cfgFilename;
	else 
		configFilename="";
	return &cfg;
}

vector<uint> Biofilter::LoadSNPs() {
	string snpFilename = cfg.GetLine("SNPS_SOURCE");
	set<uint> snps;
	vector<uint> snpList;
	if (snpFilename != "ALL") {
		ifstream file(snpFilename.c_str());
		if (!file.good()) {
			cerr<<"SNP data source, "<<snpFilename<<", appears unreadable. Unable to continue.\n";
			exit(1);
		}
		while (file.good()) {
			size_t rsID = 0;
			string id;
			file>>id;
			if (id.find("r") != string::npos || id.find("R") != string::npos)
				id=id.erase(0,2);
			rsID = atoi(id.c_str());
			if (rsID > 0) {
				snpList.push_back(rsID);
			}
		}
		cout<<"\n"<<setw(35)<<snpFilename<<" : "<<snpList.size()<<" SNPs ";cout.flush();
	}
	vector<int> inclusions;
	string cleanReport = GetReportFilename("snp-cleanup");

	bioApp.CleanRSIDs(snpList, cleanReport.c_str());
	reportLog<<setw(45)<<right<<"SNP Cleanup Report: "<<cleanReport<<"\n";

	snps.insert(snpList.begin(), snpList.end());
	//0 is used for missing SNPs. We don't want those
	snps.erase(0);
	//snpsRecorded can be larger than snps due to the fact that there might be more than a single SNP with the same RS number (we record them both)
	int snpsRecorded = bioApp.InitSNPs(snps, cfg.GetLine("VARIATION_FILENAME").c_str());
	cout<<" ("<<snpsRecorded<<" matches in our database )\n";


	return snpList;
}

void Biofilter::InitGroupData() {
	vector<uint> inclusions;
	vector<string> groupInclusions;

	//Let's set up disease independant
	vector<string> diseaseIndependent;
	cfg.GetLines("DISEASE_DEPENDENT", diseaseIndependent);
	vector<string>::iterator lineItr 	= diseaseIndependent.begin();
	vector<string>::iterator linesEnd 	= diseaseIndependent.end();
	while (lineItr != linesEnd) 
		bioApp.AddUserDefinedGroup(lineItr++->c_str());

	cfg.GetLines("INCLUDE_GROUPS", groupInclusions);

	string groupFilename = cfg.GetString("INCLUDE_GROUP_FILE");
	if (groupFilename.length() > 0) {
		Utility::FileToArray conv;
		Utility::LineParser lp;
		lp.Parse(groupFilename.c_str(), &conv, false);
		groupInclusions.insert(groupInclusions.end(), conv.strings.begin(), conv.strings.end());
	}

	lineItr = groupInclusions.begin();
	linesEnd = groupInclusions.end();

	while (lineItr!=linesEnd) {
		stringstream ss(*lineItr);
		while (!ss.eof()) {
			string group = "";
			ss>>group;
			if (group!="") {
				inclusions.push_back(atoi(group.c_str()));
			}
		}
		lineItr++;
	}



	string regionAlias = cfg.GetLine("PREFERRED_ALIAS");
	string groupReport = GetReportFilename("dd-contents");
	ofstream gr(groupReport.c_str());
	bioApp.LoadGroupData(cfg.GetInteger("MAX_GENE_COUNT"), inclusions, gr, cfg.GetLine("POPULATION").c_str(), regionAlias.c_str());

	//bioApp.PrintSNPs(cout);
}



void Biofilter::DetailGeneCoverage() {
	string db = cfg.GetLine("SETTINGS_DB");
	string genes = cfg.GetLine("GENE_COVERAGE");
	vector<string> snpFiles;
	if (!cfg.GetLines("COVERAGE_SNPS", snpFiles))
		cerr<<"Unable to find coverage files!\n";

	ifstream file(genes.c_str());
	vector<string> genelist;
	string geneInc = "";
	if (file.good()) {
		while (!file.eof()) {
			string gene="";
			file>>gene;
			if (gene.length() > 0) {
				genelist.push_back(gene);
				if (geneInc.length() > 0)
					geneInc += ", ";
				geneInc += "'" + gene + "'";
			}
		}
		bioApp.LoadRegions(geneInc, cfg.GetLine("POPULATION").c_str());
		bioApp.DetailCoverage(genelist, snpFiles, detailedCoverage);	
	}
	else {
		cerr<<"A problem was encountered opening file, "<<genes<<"\n";
		exit(1);
	}
}


void Biofilter::RunModelReport() {
/**
	Reporting::ModelReport *report;
	bool htmlReports = cfg.GetBoolean("HTML_REPORTS");
	if (htmlReports) {
		string filename = cfg.GetLine("REPORT_PREFIX") + "-model-report.html";
		report = new Reporting::ModelReportHTML(filename.c_str());
	}
	else
		report = new Reporting::ModelReport(cout);

	ifstream file(cfg.GetLine("SNPS_SOURCE").c_str());

	vector<uint> snps;
	while (file.good()) {
		string rs1, rs2;
		file>>rs1>>rs2;
		if (rs1.find("r") != string::npos || rs1.find("R") != string::npos) 
			rs1.erase(0,2);
		if (rs2.find("r") != string::npos || rs2.find("R") != string::npos) 
			rs2.erase(0,2);
		uint snp1 = atoi(rs1.c_str());
		uint snp2 = atoi(rs2.c_str());

		if (snp1 > 0 && snp2 > 0) {
			snps.push_back(snp1);
			snps.push_back(snp2);
			report->AddModel(snp1, snp2);
		}
	}

	//I'm worried that this is way too much logic for this function....
	//Maybe some of this should be done at the actual application level
	uint snpsLoaded = bioApp.InitSNPs(snps, cfg.GetLine("VARIATION_FILENAME").c_str());
	cerr<<"SNPS Loaded. "<<snps.size()<<" -> "<<snpsLoaded<<"\n";
	InitGroupData();
	bioApp.RunReport(report);
	delete report;
	 */
}
std::string Biofilter::GetReportFilename(const char *extension) {
	string prefix = cfg.GetLine("REPORT_PREFIX");
	string joint = ".";
	if (extension[0] == '-' || extension[0]=='.' || extension[0]=='_')
		joint = "";
	string filename = prefix + joint + string(extension);
	return filename;
}
void Biofilter::RunCommands() {
	bioApp.SetReportPrefix(cfg.GetLine("REPORT_PREFIX").c_str());
	switch (action) {
		case BiofilterAction::PrintSampleConfig:
		{
			cfg.Write(cout);
			return;
		}
		case BiofilterAction::Optimize:
		{
			cerr<<"Optimizing\n";
			bioApp.PerformOptimization();
			return;
		}
		case BiofilterAction::StripOptimization:
			bioApp.StripOptimization();
			return;
		case BiofilterAction::ListGroups:
			{
				vector<string> keywords;
				cfg.GetLines("GROUP_SEARCH_CRITERIA", keywords);
				bioApp.ListGroupIDs(keywords);
			} return;
		case BiofilterAction::ListPopulationIDs:
			bioApp.ListPopulationIDs();
			return;
		case BiofilterAction::ListMetaGroups:
			{
				InitGroupData();
				bioApp.ListMetaGroups(cout);
			}
			return;
		case BiofilterAction::ListModels:
			{
				//InitGroupData();
				string filename = GetReportFilename(".gene-gene");
				string geneFilename = GetReportFilename(".genes");
				bool binaryArchive = cfg.GetBoolean("BINARY_MODEL_ARCHIVE");
				GeneGeneModelReader modelArchive(geneFilename.c_str(), filename.c_str(), binaryArchive);
				GeneGeneModelReader::iterator itr = modelArchive.begin();
				std::map<float, uint> counts;

				SnpModelCollection modelCollection;

				//This will consume ~2 gigabytes
				while (itr.GetModels(modelCollection, 10000000, 1) > 0) {
					SnpModelCollection::iterator sItr = modelCollection.begin();
					SnpModelCollection::iterator sEnd = modelCollection.end();

					while (sItr != sEnd) {
						SnpSnpModel* m = *sItr++;
						float score = m->ImplicationIndex();
						if (counts.find(score)==counts.end())
							counts[score] = 1;
						else
							counts[score]++;
						m->Write(cout, false);
						delete m;
					}
				}
				std::map<float, uint>::iterator citr = counts.begin();
				std::map<float, uint>::iterator cend = counts.end();
				cout<<"Model Generation Completed:\n"
					<<"Impl.\n"
					<<"Index\tCount\n";
				while (citr != cend) {
					cout<<setprecision(2)<<citr->first<<"\t"<<citr->second<<"\n";
					citr++;
				}
				//cerr<<"!!!!!!Skipping model report for now\n";
				//bioApp.ReportOnModels(filename.c_str());
			}
			return;
		case BiofilterAction::ExportSnpModels:
			{
				string filename = GetReportFilename("gene-gene");
				string geneFilename = GetReportFilename(".genes");
				string snpModelFilename = GetReportFilename(".snpsnp");
				bool binaryArchive = cfg.GetBoolean("BINARY_MODEL_ARCHIVE");
				uint minImplicationIndex = cfg.GetInteger("MINIMUM_IMPLICATION_INDEX");
				uint maxSnpModelCount = cfg.GetInteger("MAX_SNP_MODEL_COUNT");
				GeneGeneModelReader modelArchive(geneFilename.c_str(), filename.c_str(), binaryArchive);

				std::map<float, uint> counts = modelArchive.ArchiveSnpModels(snpModelFilename.c_str(), maxSnpModelCount, minImplicationIndex, binaryArchive);

				std::map<float, uint>::iterator citr = counts.begin();
				std::map<float, uint>::iterator cend = counts.end();
				cout<<"Model Generation Completed:\n"
					<<"Impl.\n"
					<<"Index\tCount\n";
				while (citr != cend) {
					cout<<setprecision(2)<<citr->first<<"\t"<<citr->second<<"\n";
					citr++;
				}
				reportLog<<setw(45)<<right<<"Snp Models: "<<snpModelFilename.c_str()<<"\n";

				//cerr<<"!!!!!!Skipping model report for now\n";
				//bioApp.ReportOnModels(filename.c_str());
				return;
			}
		case BiofilterAction::RunModelReport:
			RunModelReport();
			return;
		default:
			{}
	}

	//This is a special case, and shouldn't be done with anything else!
	if (ldConfiguration.length() > 0) {
		bioApp.ImportLD(ldConfiguration.c_str(), cfg.GetLine("VARIATION_FILENAME").c_str());
		return;
	}
	
	//The rest of these needs this to be done before they begin
	vector<uint> snps = LoadSNPs();
	switch (action) {
		case BiofilterAction::RunMarkerInfo:
			bioApp.WriteMarkerInfo(cout);
			return;
		case BiofilterAction::RunGeneCoverage:
			DetailGeneCoverage();
			return;
		default:
			{}
	}
	
	InitGroupData();
	if (doLoadRegionAliases)
		bioApp.LoadRegionAliases();
	int maxGeneCount = cfg.GetInteger("MAX_GENE_COUNT");

	if (doWriteModelCounts) {
		int maxGeneCount = cfg.GetInteger("MAX_GENE_COUNT");
		bioApp.SummarizeModelCounts(maxGeneCount);
	}
	if (cfg.GetBoolean("SNP_REPORT")) {
		bool writeHtml = cfg.GetBoolean("HTML_REPORTS");
		if (writeHtml) {
			string snpMissingFilename = GetReportFilename("_nogenes.txt");
			ofstream missing(snpMissingFilename.c_str());
			string snpReportFilename = GetReportFilename("_SNP_Report.html");
			ofstream file(snpReportFilename.c_str());
			bioApp.SnpReport(file, missing, snps, writeHtml);
			reportLog<<setw(45)<<right<<"SNP Report : "<<snpReportFilename<<"\n";
		}
		else {
			string snpReportFilename = GetReportFilename("_SNP_Report.txt");
			ofstream file(snpReportFilename.c_str());
			bioApp.SnpReport(cout, cout, snps, writeHtml);
		}
	}

	if (cfg.GetBoolean("ASSOCIATION_REPORT")) {
		bioApp.ListPresentAssociations(maxGeneCount);
	}

	if (cfg.GetBoolean("ASSOCIATION_GRAPH")) {
		bioApp.GraphPresentAssociations(GetReportFilename("dot").c_str(), maxGeneCount);
	}

	if (action == BiofilterAction::ProduceModels) {
		char tmpname[128];
		strcpy(tmpname, "modelsXXXXXX");
		int initBufferSize = cfg.GetInteger("MODEL_BUFFER_INIT");
		int maxBufferSize = cfg.GetInteger("MODEL_BUFFER_MAX");
		bool binaryArchive = cfg.GetBoolean("BINARY_MODEL_ARCHIVE");
		GeneGeneModelArchive repo(tmpname, initBufferSize, maxBufferSize, binaryArchive);

		//	ModelRepository repo(tmpname, initBufferSize, maxBufferSize);
		int maxGeneCount = cfg.GetInteger("MAX_GENE_COUNT");
		string geneGeneReport = GetReportFilename("gene-gene");
		string summaryReport  = GetReportFilename("-model-summary.txt");
		ofstream file(summaryReport.c_str());
		bioApp.ProduceModels(repo, file, maxGeneCount);
 		string geneFilename = GetReportFilename("genes");
		std::map<float, uint> counts = repo.Archive(geneFilename.c_str(), geneGeneReport.c_str());
		std::map<float, uint>::iterator itr = counts.begin();
		std::map<float, uint>::iterator end = counts.end();
		reportLog<<setw(45)<<right<<"Gene-Gene Model Summary: "<<summaryReport<<"\n";
		cout<<"Gene-Gene Model Summary (Snp-Snp Model Estimates)\n";
		cout<<setw(20)<<"Impl. Idx "<<setw(20)<<right<<"Count"<<"\n";
		cout<<setw(20)<<"-------------"<<setw(20)<<right<<"---------"<<"\n";
		while (itr != end) {
			cout<<setw(20)<<setprecision(2)<<itr->first<<setw(20)<<right<<itr->second<<"\n";
			itr++;
		}
		reportLog<<setw(45)<<right<<"Gene-Gene Models: "<<geneGeneReport<<"\n";
	}
	if (cfg.GetBoolean("EXPORT_SNP_MODELS")) {
		string filename = GetReportFilename("gene-gene");
		string geneFilename = GetReportFilename("genes");
		string snpModelFilename = GetReportFilename("snpsnp");
		bool binaryArchive = cfg.GetBoolean("BINARY_MODEL_ARCHIVE");
		uint minImplicationIndex = cfg.GetInteger("MINIMUM_IMPLICATION_INDEX");
		uint maxSnpModelCount = cfg.GetInteger("MAX_SNP_MODEL_COUNT");
		GeneGeneModelReader modelArchive(geneFilename.c_str(), filename.c_str(), binaryArchive);

		std::map<float, uint> counts = modelArchive.ArchiveSnpModels(snpModelFilename.c_str(), maxSnpModelCount, minImplicationIndex, binaryArchive);

		std::map<float, uint>::iterator citr = counts.begin();
		std::map<float, uint>::iterator cend = counts.end();
		cout<<"\nSnp-Snp Model Generation Summary:\n"
			<<setw(20)<<"Impl."<<"\n"
			<<setw(20)<<right<<"Index "<<setw(20)<<"Count"<<"\n";
		cout<<setw(20)<<"-------------"<<setw(20)<<right<<"---------"<<"\n";
		while (citr != cend) {
			cout<<setw(20)<<setprecision(2)<<citr->first<<setw(20)<<citr->second<<"\n";
			citr++;
		}
		reportLog<<setw(45)<<right<<"Snp Models: "<<snpModelFilename.c_str()<<"\n";

	}
	cout<<"\n";

}



string Biofilter::GetReportPrefix() {
	string prefix = cfg.GetLine("REPORT_PREFIX");
	if (prefix == "")
		prefix = configFilename;
	return prefix;
}

}
int main(int argc, char *argv[])	{
	string cfgFilename;

	Biofilter::Biofilter app;					///<The application object
	if (!app.ParseCmdLine(argc, argv))
		exit(1);
	//Performs any commands
	app.RunCommands();
	
  	return EXIT_SUCCESS;
}

#!/usr/bin/env python
import os
import re
import gc
import csv
import logging
import psutil
import time
import urllib.request as urllib2
from multiprocessing import cpu_count, Manager
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from loki_modules import loki_source
from loki_mixins import SourceUtilMixin


# Classe principal que inicia o multiprocessamento
class Source_dbsnp(loki_source.Source):

    _chmList = (
        # "1",
        # "2",
        # "3",
        # "4",
        # "5",
        # "6",
        # "7",
        # "8",
        # "9",
        # "10",
        # "11",
        # "12",
        # "13",
        # "14",
        # "15",
        # "16",
        # "17",
        # "18",
        # "19",
        # "20",
        # "21",
        "22",
        # "X",
        # "Y",
        # "PAR",
        # "MT",
    )
    _grcBuild = None

    def _identifyLatestSNPContig(self, filenames):
        bestbuild = 0
        bestfile = list()
        for filename in filenames:
            # foreach file in path
            if int(filename[0]) > bestbuild:
                bestfile.append(filename[0])
                bestfile.append(filename[1].split(".bcp.gz")[0])
                bestbuild = int(filename[0])
        return bestfile

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    @classmethod
    def getOptions(cls):
        return {
            "unvalidated": "[yes|no]  --  store SNP loci which have not been validated (default: yes)",  # noqa E501
            "suspect": "[yes|no]  --  store SNP loci which are suspect (default: no)",  # http://www.ncbi.nlm.nih.gov/projects/SNP/docs/rs_attributes.html#suspect  # noqa E501
            "withdrawn": "[yes|no]  --  store SNP loci which have been withdrawn (default: no)",  # noqa E501
            "loci": "[all|validated]  --  store all or only validated SNP loci (default: validat`dddded)",  # noqa E501
            "merges": "[yes|no]  --  process and store RS# merge history (default: yes)",  # noqa E501
            "roles": "[yes|no]  --  process and store SNP roles (default: no)",  # noqa E501
        }

    def validateOptions(self, options):
        options.setdefault("unvalidated", "yes")
        options.setdefault("suspect", "no")
        options.setdefault("withdrawn", "no")
        options.setdefault("merges", "yes")
        options.setdefault("roles", "no")
        for o, v in options.items():
            v = v.strip().lower()
            if o in ("unvalidated", "suspect", "withdrawn", "merges", "roles"):
                if "yes".startswith(v):
                    v = "yes"
                elif "no".startswith(v):
                    v = "no"
                else:
                    return "%s must be 'yes' or 'no'" % o
            else:
                return "unknown option '%s'" % o
            options[o] = v
        return True

    def download(self, options, path):
        # define a callback to identify the latest SNPContigLocusId file
        def remFilesCallback(ftp, path):
            remFiles = dict()
            for chm in self._chmList:
                remFiles[path + "/chr_" + chm + ".txt.gz"] = (
                    "/snp/organisms/human_9606/chr_rpts/chr_%s.txt.gz" % chm
                )

            if options["merges"] == "yes":
                remFiles[path + "/RsMergeArch.bcp.gz"] = (
                    "/snp/organisms/human_9606/database/organism_data/RsMergeArch.bcp.gz"  # noqa E501
                )

            if options.get["roles"] == "yes":
                remFiles[path + "/SnpFunctionCode.bcp.gz"] = (
                    "/snp/organisms/database/shared_data/SnpFunctionCode.bcp.gz"  # noqa E501
                )
                urlpath = "/snp/organisms/human_9606/database/organism_data"
                ftp.cwd(urlpath)
                bestfile = self._identifyLatestSNPContig(ftp.nlst())

                if bestfile:
                    remFiles[bestfile] = "%s/%s" % (urlpath, bestfile)

            return remFiles

        remFiles = dict()
        for chm in self._chmList:
            remFiles[path + "/chr_%s.txt.gz" % chm] = (
                "/snp/organisms/human_9606/chr_rpts/chr_%s.txt.gz" % chm
            )

        if options["merges"] == "yes":
            remFiles[path + "/RsMergeArch.bcp.gz"] = (
                "/snp/organisms/human_9606/database/organism_data/RsMergeArch.bcp.gz"  # noqa E501
            )
        if options["roles"] == "yes":
            remFiles[path + "/SnpFunctionCode.bcp.gz"] = (
                "/snp/organisms/database/shared_data/SnpFunctionCode.bcp.gz"
            )
            urlfolderpath = "/snp/organisms/human_9606/database/organism_data"
            urlpath = urllib2.urlopen(
                "https://ftp.ncbi.nih.gov" + urlfolderpath
            )  # noqa E501
            string = urlpath.read().decode("utf-8")
            onlyfiles = list(
                set(
                    re.findall(
                        r"b([0-9]+)_SNPContigLocusId_(.*)\.bcp\.gz", string
                    )  # noqa E501
                )  # noqa E501
            )
            bestfile = self._identifyLatestSNPContig(onlyfiles)
            bestfilename = (
                "b"
                + bestfile[0]
                + "_SNPContigLocusId_"
                + bestfile[1]
                + ".bcp.gz"  # noqa E501
            )
            if bestfile:
                remFiles[path + "/" + bestfilename] = "%s/%s" % (
                    urlfolderpath,
                    bestfilename,
                )

        # download the latest source files
        self.downloadFilesFromHTTP("ftp.ncbi.nih.gov", remFiles)

        return list(remFiles.keys())

    def update(self, options, path):
        """
        Transformation Rule to dbSNP
        """
        start_time = time.time()
        process_memory = psutil.Process()
        memory_bef = process_memory.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"dbSNP - Inicial memory {memory_bef:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            "dbSNP - Starting deletion of old records from the database ...",
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()  # will drop by Source ID
        self.log(
            "dbSNP - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # process merge report (no header!)
        # NOTE: Temp desativado pra deploment.
        # if 3 == 4:
        if options.get("merges", "yes") == "yes":
            """/* from human_9606_table.sql.gz */
            CREATE TABLE [RsMergeArch]
            (
            [rsHigh] [int] NULL ,
            [rsLow] [int] NULL ,
            [build_id] [int] NULL ,
            [orien] [tinyint] NULL ,
            [create_time] [datetime] NOT NULL ,
            [last_updated_time] [datetime] NOT NULL ,
            [rsCurrent] [int] NULL ,
            [orien2Current] [tinyint] NULL ,
            [comment] [varchar](255) NULL
            )
            """
            self.log("processing SNP merge records ...\n")
            mergeFile = self.zfile(
                path + "/RsMergeArch.bcp.gz"
            )  # TODO:context manager,iterator
            numMerge = 0
            setMerge = set()
            for line in mergeFile:
                words = line.split("\t")
                if not (len(words) > 6 and words[0] and words[6]):
                    continue
                rsOld = int(words[0])
                # rsNew = int(words[1])
                rsCur = int(words[6])

                setMerge.add((rsOld, rsCur))

                # write to the database after each 2.5 million,
                # to keep memory usage down
                if len(setMerge) >= 2500000:
                    numMerge += len(setMerge)
                    self.log(
                        "processing SNP merge records: ~%1.1f million so far\n"
                        % (numMerge / 1000000.0)
                    )  # TODO: time estimate
                    self.log("writing SNP merge records to the database ...\n")
                    # SNP_MERGE TABLE: [reMerged, rsCurrent, source_id]
                    self.addSNPMerges(setMerge)
                    setMerge = set()
                    self.log(
                        "writing SNP merge records to the database completed\n"
                    )  # noqa E501
            # foreach line in mergeFile
            numMerge += len(setMerge)
            self.log(
                "processing SNP merge records completed: ~%d merged RS#s\n"
                % numMerge  # noqa E501
            )
            if setMerge:
                self.log("writing SNP merge records to the database ...\n")
                self.addSNPMerges(setMerge) # Talves nao reciar o indice aqui
                self.log(
                    "writing SNP merge records to the database completed\n"
                )  # noqa E501
            setMerge = None
        # if merges

        # process SNP role function codes
        # NOTE: Temp desativado pra deploment.
        # if 3 == 4:
        if options.get("roles", "no") == "yes":
            """/* from dbSNP_main_table.sql.gz */
            CREATE TABLE [SnpFunctionCode]
            (
            [code] [tinyint] NOT NULL ,
            [abbrev] [varchar](20) NOT NULL ,
            [descrip] [varchar](255) NOT NULL ,
            [create_time] [smalldatetime] NOT NULL ,
            [top_level_class] [char](5) NOT NULL ,
            [is_coding] [tinyint] NOT NULL ,
            [is_exon] [bit] NULL ,
            [var_prop_effect_code] [int] NULL ,
            [var_prop_gene_loc_code] [int] NULL ,
            [SO_id] [varchar](32) NULL
            )
            """
            self.log("processing SNP role codes ...\n")
            roleID = dict()
            codeFile = self.zfile(path + "/SnpFunctionCode.bcp.gz")
            for line in codeFile:
                words = line.split("\t")
                code = int(words[0])
                name = words[1]
                desc = words[2]
                coding = (
                    int(words[5])
                    if (len(words) > 5 and words[5] != "")
                    else None  # noqa E501
                )  # noqa E501
                exon = (
                    int(words[6])
                    if (len(words) > 6 and words[6] != "")
                    else None  # noqa E501
                )  # noqa E501

                roleID[code] = self.addRole(name, desc, coding, exon)
            # foreach line in codeFile
            self.log(
                "processing SNP role codes completed: %d codes\n" % len(roleID)
            )  # noqa E501

            # process SNP roles
            """ /* from human_9606_table.sql.gz */
            CREATE TABLE [b137_SNPContigLocusId]
            (
            [snp_id] [int] NULL ,
            [contig_acc] [varchar](32) NOT NULL ,
            [contig_ver] [tinyint] NULL ,
            [asn_from] [int] NULL ,
            [asn_to] [int] NULL ,
            [locus_id] [int] NULL ,
            [locus_symbol] [varchar](64) NULL ,
            [mrna_acc] [varchar](32) NOT NULL ,
            [mrna_ver] [smallint] NOT NULL ,
            [protein_acc] [varchar](32) NULL ,
            [protein_ver] [smallint] NULL ,
            [fxn_class] [int] NULL ,
            [reading_frame] [int] NULL ,
            [allele] [varchar](255) NULL ,
            [residue] [varchar](1000) NULL ,
            [aa_position] [int] NULL ,
            [build_id] [varchar](4) NOT NULL ,
            [ctg_id] [int] NULL ,
            [mrna_start] [int] NULL ,
            [mrna_stop] [int] NULL ,
            [codon] [varchar](1000) NULL ,
            [protRes] [char](3) NULL ,
            [contig_gi] [int] NULL ,
            [mrna_gi] [int] NULL ,
            [mrna_orien] [tinyint] NULL ,
            [cp_mrna_ver] [int] NULL ,
            [cp_mrna_gi] [int] NULL ,
            [verComp] [int] NULL
            )
            """
            self.log("processing SNP roles ...\n")
            setRole = set()
            numRole = numOrphan = numInc = 0
            setOrphan = set()
            funcFile = self.zfile(
                list(
                    filter(
                        re.compile(
                            r"b([0-9]+)_SNPContigLocusId_(.*)\.bcp\.gz"
                        ).match,  # noqa E501
                        os.listdir(path),
                    )
                )[0]
            )
            for line in funcFile:
                words = list(w.strip() for w in line.split("\t"))
                rs = int(words[0]) if words[0] else None
                entrez = int(words[5]) if words[5] else None
                # genesymbol = words[6]
                code = int(words[11]) if words[11] else None

                if rs and entrez and code:
                    try:
                        setRole.add((rs, entrez, roleID[code]))
                    except KeyError:
                        setOrphan.add(code)
                        numOrphan += 1
                else:
                    numInc += 1

                # write to the database after each 2.5 million,
                # to keep memory usage down
                if len(setRole) >= 2500000:
                    numRole += len(setRole)
                    self.log(
                        "processing SNP roles: ~%1.1f million so far\n"
                        % (numRole / 1000000.0)
                    )  # TODO: time estimate
                    self.log("writing SNP roles to the database ...\n")
                    self.addSNPEntrezRoles(setRole)
                    setRole = set()
                    self.log("writing SNP roles to the database completed\n")

            roleID = None
            # foreach line in funcFile
            numRole += len(setRole)
            self.log(
                "processing SNP roles completed: ~%d roles\n" % (numRole,)
            )  # noqa E501
            if setRole:
                self.log("writing SNP roles to the database ...\n")
                self.addSNPEntrezRoles(setRole)
                self.log("writing SNP roles to the database completed\n")
            setRole = None

            # warn about orphans
            if setOrphan:
                self.log(
                    "WARNING: %d roles (%d codes) unrecognized\n"
                    % (numOrphan, len(setOrphan))
                )
            if numInc:
                self.log("WARNING: %d roles incomplete\n" % (numInc,))
            setOrphan = None
        # if roles

        # process chromosome report files
        # dbSNP chromosome reports use 1-based coordinates since b125,
        # according to:
        #   http://www.ncbi.nlm.nih.gov/books/NBK44414/#Reports.the_xml_dump_for_build_126_has_a
        # This matches LOKI's convention.
        reBuild = re.compile("GRCh([0-9]+)")
        includeUnvalidated = options["unvalidated"] == "yes"
        includeSuspect = options["suspect"] == "yes"
        includeWithdrawn = options["withdrawn"] == "yes"

        # Ensures that `multiprocessing` is safe even when called externally
        try:
            multiprocessing.set_start_method("spawn", force=True)
        except RuntimeError:
            pass

        # Define cores to use [TODO create a parameter]
        num_workers = min(cpu_count() - 2, len(self._chmList))
        self.log(
            f"dbSNP - Setup {num_workers} workers to process the data",
            level=logging.INFO,
            indent=2,
        )

        # PROCESS PHASE
        # ==========================
        # 📌 Process and save results in temp csv

        manager = Manager()
        queue = manager.Queue()
        processed_files = {}
        result = {}

        self.log(
            "dbSNP - Starting workers in Multiprocessing mode",
            level=logging.INFO,
            indent=2,
        )

        # Run the processing inside a safe Executor
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(
                    run_processing_worker,
                    chromosome,
                    reBuild,
                    includeUnvalidated,
                    includeSuspect,
                    includeWithdrawn,
                    path,
                    queue,
                    self._grcBuild,
                )  #
                for chromosome in self._chmList
            ]

            # Wait for all processes to finish
            for future in futures:
                future.result()

        for _ in self._chmList:
            result = queue.get()
            processed_files.update(result)

        process_time = time.time()
        process_finish = (process_time - start_time) / 60  # time in minutes
        self.log(
            f"dbSNP - Workers completed the processing in {process_finish:.2f} minutes.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )

        # INGESTION PHASE
        # ==========================
        ingestion_start = time.time()
        v_indent = 4
        self.log(
            "dbSNP - Starting ingestion data fase",
            level=logging.INFO,
            indent=2,
        )

        self.log(
            "dbSNP - Setup batch size to 1M records",
            level=logging.INFO,
            indent=2,
        )
        batch_size = 1000000  # TODO create a parameter to set batch size

        for key, (output_file, msn) in processed_files.items():
            self.log(
                f"dbSNP - Starting the ingesting to chromosome {key}",
                level=logging.INFO,
                indent=v_indent,
            )

            with open(output_file, mode="r") as f:
                reader = csv.reader(f)
                next(reader)  # Pular cabeçalho

                buffer = []
                if key == "PAR":
                    buffer_X = []
                    buffer_Y = []
                    for row in reader:
                        # buffer.append(row)
                        if row[0] == "X":
                            buffer_X.append(row[1:])
                        elif row[0] == "Y":
                            buffer_Y.append(row[1:])
                    if buffer_X:
                        self.addChromosomeSNPLoci(self._loki.chr_num["X"], buffer_X)  # noqa E501
                    if buffer_Y:
                        self.addChromosomeSNPLoci(self._loki.chr_num["Y"], buffer_Y)  # noqa E501
                # Other files different from PAR
                else:
                    for row in reader:
                        buffer.append(row)

                        if len(buffer) >= batch_size:
                            self.addChromosomeSNPLoci(self._loki.chr_num[key], buffer)  # noqa E501
                            buffer.clear()

                    if buffer:
                        self.addChromosomeSNPLoci(self._loki.chr_num[key], buffer)  # noqa E501

            buffer.clear()
            os.remove(output_file)  # 🔥 Drop temp csv files

            self.log(
                f"dbSNP - Removed temp files to {key} chromosome data",
                level=logging.INFO,
                indent=v_indent,
            )
            end_time = time.time()
            ingestion_stop = (end_time - ingestion_start) / 60
            self.log(
                f"dbSNP - 🎯 Ingestion for chromosome {key} completed in {ingestion_stop:.2f} minutes.",  # noqa E501
                level=logging.INFO,
                indent=v_indent,
            )

        # store source metadata
        self.setSourceBuilds(self._grcBuild, None)

        # Finalize the process
        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process_memory.memory_info().rss / (1024 * 1024)  # MB
        self.log(
            f"dbSNP - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_bef:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"dbSNP - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )

    # update(🎯)


# Função que inicia um Worker
def run_processing_worker(
    chromosome,
    reBuild,
    includeUnvalidated,
    includeSuspect,
    includeWithdrawn,
    path,
    queue,
    grcBuild,
):

    # Pass the task to the worker
    worker = ProcessingWorker(
        chromosome,
        reBuild,
        includeUnvalidated,
        includeSuspect,
        includeWithdrawn,
        path,
        queue,
        grcBuild,
    )
    worker.run()


# Worker que processa cada cromossomo
class ProcessingWorker(SourceUtilMixin):
    def __init__(
        self,
        chromosome,
        reBuild,
        includeUnvalidated,
        includeSuspect,
        includeWithdrawn,
        path,
        queue,
        grcBuild,
    ):
        super().__init__()
        self.chrom = chromosome
        self.reBuild = reBuild
        self.includeUnvalidated = includeUnvalidated
        self.includeSuspect = includeSuspect
        self.includeWithdrawn = includeWithdrawn
        self.path = path
        self.queue = queue
        self._grcBuild = grcBuild

    def run(self):
        v_msn = "n/a"
        v_indent = " " * 40

        try:
            print(f"{v_indent}🧬 Starting Worker to chrom {self.chrom}")  # noqa E501

            # Open the file
            filename = f"{self.path}/chr_{self.chrom}.txt.gz"
            chmFile = self.zfile(filename)

            # Check if the headers are aligned as expected
            hearders_expected = [
                "dbSNP Chromosome Report",
                "Refer to ftp://ftp.ncbi.nlm.nih.gov/snp/00readme for documentation on tabular data below",  # noqa E501
                "",
                "rs#\tmap\tsnp\tchr\tctg\ttotal\tchr\tctg\tctg\tctg\tctg\tchr\tlocal\tavg\ts.e.\tmax\tvali-\tgeno-\tlink\torig\tupd\tref-\tsus-\tclin\tallele\tgmaf",  # noqa E501
                "\twgt\ttype\thits\thits\thits\t\tacc\tver\tID\tpos\tpos\tloci\thet\thet\tprob\tdated\ttypes\touts\tbuild\tbuild\talt\tpect\tsig.\torigin",  # noqa E501
                "",
                "",
            ]

            try:
                headers = [chmFile.__next__().rstrip() for _ in range(7)]
                if not headers == hearders_expected:
                    v_msn = f"unrecognized file headers to {filename}"
            except StopIteration:
                v_msn = f"File {self.filename} is empty or corrupted."
                return

            setBadBuild, setBadVers, setBadFilter, setBadChr = (
                set(),
                set(),
                set(),
                set(),
            )  # noqa E501
            record_count, interations = 0, 0
            data_buffer = []
            batch_size = 1000000  # TODO create a parameter to set batch size

            output_file = f"{self.path}/output_chr_{self.chrom}.csv"

            # Criar CSV e definir cabeçalho
            with open(output_file, mode="w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                if self.chrom == "PAR":
                    writer.writerow(["chrom", "rs", "position", "validated"])
                else:
                    writer.writerow(["rs", "position", "validated"])

                for line in chmFile:
                    words = line.split("\t")
                    try:
                        rs = int(words[0].strip())
                        withdrawn = int(words[2].strip()) > 0
                        chm = words[6].strip()
                        pos = int(words[11].strip())
                        validated = int(words[16].strip()) > 0
                        build = self.reBuild.search(words[21])
                        suspect = int(words[22].strip()) > 0
                    except (ValueError, IndexError):
                        continue  # Ignorar linhas malformadas

                    if not build:
                        setBadBuild.add(rs)
                    elif self._grcBuild and self._grcBuild != build.group(1):
                        setBadVers.add(rs)
                    elif not (validated or self.includeUnvalidated):
                        setBadFilter.add(rs)
                    elif suspect and not self.includeSuspect:
                        setBadFilter.add(rs)
                    elif withdrawn and not self.includeWithdrawn:
                        setBadFilter.add(rs)
                    elif (self.chrom != "PAR") and (chm != self.chrom):
                        setBadChr.add(rs)
                    elif self.chrom == "PAR" and chm not in {"X", "Y"}:
                        setBadChr.add(rs)
                    else:
                        if self.chrom == "PAR":
                            data_buffer.append([chm, rs, pos, validated])
                        else:
                            data_buffer.append([rs, pos, validated])

                    if len(data_buffer) >= batch_size:
                        interations += 1
                        writer.writerows(data_buffer)
                        print(
                            f"{v_indent}  Worker {self.chrom} - {interations} saved {len(data_buffer)/ 1000000:.1f}M SNPs in buffer"  # noqa E501
                        )  # noqa E501
                        record_count += len(data_buffer)
                        data_buffer.clear()

                # 🔥 Escreve o que restou no buffer (último batch)
                if data_buffer:
                    writer.writerows(data_buffer)
                    print(
                        f"{v_indent}  Worker {self.chrom} - last saved {len(data_buffer)/ 1000000:.1f}M SNPs in buffer"  # noqa E501
                    )  # noqa E501
                    record_count += len(data_buffer)
                    data_buffer.clear()

                print(
                    f"{v_indent}  Worker {self.chrom} processed {record_count / 1000000:.1f}M SNPs"  # noqa E501
                )  # noqa E501

            # """   Loga avisos sobre dados não processados."""
            if setBadBuild:
                print(
                    f"{v_indent}  ⚠️ WARNING: {len(setBadBuild)} SNPs not mapped to any GRCh build"  # noqa E501
                )  # noqa E501
            if setBadVers:
                print(
                    f"{v_indent}  ⚠️ WARNING: {len(setBadVers)} SNPs mapped to different GRCh versions"  # noqa E501
                )  # noqa E501
            if setBadFilter:
                print(
                    f"{v_indent}  ⚠️ WARNING: {len(setBadFilter)} SNPs skipped (unvalidated, suspect, withdrawn)"  # noqa E501
                )  # noqa E501
            if setBadChr:
                print(
                    f"{v_indent}  ⚠️ WARNING: {len(setBadChr)} SNPs on mismatching chromosome"  # noqa E501
                )  # noqa E501

            print(f"{v_indent}🎯 Worker {self.chrom} completed.")

            # Enviar o arquivo gerado para a fila
            self.queue.put({self.chrom: [output_file, v_msn]})

        except Exception as e:
            print(f"{v_indent}🔥Error: {e}")
            self.queue.put({self.chrom: [output_file, e]})

        finally:
            # Clean up memory
            if "chmFile" in locals() and chmFile:
                chmFile.close()
                del chmFile
            gc.collect()

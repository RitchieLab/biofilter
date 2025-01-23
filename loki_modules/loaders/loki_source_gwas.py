import os
import re
import logging
import psutil
import time
from loki_modules import loki_source


class Source_gwas(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):
        self.downloadFilesFromHTTP(
            "www.ebi.ac.uk",
            {
                path
                + "/gwas_catalog_v1.0-associations.tsv": "/gwas/api/search/downloads/full"  # noqa E501
            },
            alwaysDownload=True,
        )

        return [path + "/gwas_catalog_v1.0-associations.tsv"]

    def update(self, options, path):
        # clear out all old data from this source
        start_time = time.time()
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"GWAS - Starting Data Ingestion (inicial memory {memory_before:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            "GWAS - Starting deletion of old records from the database ...",
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()
        self.log(
            "GWAS - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # process gwas cataog
        # the catalog uses dbSNP positions from b132,
        # which should already be 1-based
        self.log(
            "GWAS - Starting the processing of GWAS catalog annotations ...",
            level=logging.INFO,
            indent=2,
        )  # noqa E501
        reRS = re.compile("rs([0-9]+)", re.I)
        # reChrPos = re.compile("(?:^|[^_])chr([0-9XYMT]+)[:_]([0-9]+)", re.I)
        reSNP = re.compile(
            "(?:^|[^_])(?:chr([0-9XYMT]+)[:_]([0-9]+)|rs([0-9]+))", re.I
        )  # noqa E501
        listNone = [None]
        numInc = numInvalid = 0
        setGwas = set()
        if os.path.exists(path + "/gwas_catalog_v1.0-associations.tsv"):
            with open(
                path + "/gwas_catalog_v1.0-associations.tsv", "r"
            ) as gwasFile:  # noqa E501
                header = next(gwasFile).rstrip()
                cols = list(w.strip() for w in header.split("\t"))
                try:
                    colPubmedID = cols.index("PUBMEDID")
                    colTrait = cols.index("DISEASE/TRAIT")
                    colChm = cols.index("CHR_ID")
                    colPos = cols.index("CHR_POS")
                    colAlleles = cols.index("STRONGEST SNP-RISK ALLELE")
                    colSNPs = cols.index("SNPS")
                    colRAF = cols.index("RISK ALLELE FREQUENCY")
                    colORBeta = cols.index("OR or BETA")
                    col95CI = cols.index("95% CI (TEXT)")
                except ValueError as e:
                    self.log_exception(e)
                    msn_error = f"Error GWAS Header Processing: {str(e)}"
                    self._loki.addWarning(self._sourceID, msn_error)
                    raise Exception("unrecognized file header: %s" % str(e))

                lx = 1
                for line in gwasFile:
                    lx += 1
                    line = line.rstrip("\r\n")
                    words = list(w.strip() for w in line.split("\t"))
                    if len(words) <= col95CI:
                        # blank line at the end is normal
                        if (len(words) > 1) or words[0]:
                            numInc += 1
                        continue
                    elif (" x " in words[colPos]) or (" x " in words[colSNPs]):
                        # GWAS interaction pairs are not yet supported in LOKI
                        numInvalid += 1
                        continue
                    pubmedID = (
                        int(words[colPubmedID]) if words[colPubmedID] else None
                    )  # noqa E501
                    trait = words[colTrait]
                    listChm = (
                        words[colChm].split(";") if words[colChm] else list()
                    )  # noqa E501
                    listPos = (
                        words[colPos].split(";") if words[colPos] else list()
                    )  # noqa E501
                    snps = (
                        words[colSNPs]
                        if words[colAlleles].endswith("aplotype")
                        else words[colAlleles]
                    )
                    listSNPs = reSNP.findall(snps)
                    riskAfreq = words[colRAF]
                    orBeta = words[colORBeta]
                    allele95ci = words[col95CI]
                    if (len(listChm) == len(listPos) == 0) and (
                        len(listSNPs) > 0
                    ):  # noqa E501
                        listChm = listPos = list(
                            None for i in range(len(listSNPs))
                        )  # noqa E501
                    if (
                        (len(listChm) == len(listPos))
                        and (len(listChm) > 0)
                        and (len(listSNPs) == 0)
                    ):
                        listSNPs = list(
                            (None, None, None) for i in range(len(listChm))
                        )  # noqa E501
                    if len(listChm) == len(listPos) == len(listSNPs):
                        for i in range(len(listSNPs)):
                            rs = (
                                int(listSNPs[i][2]) if listSNPs[i][2] else None
                            )  # noqa E501
                            chm = self._loki.chr_num.get(
                                listChm[i]
                            ) or self._loki.chr_num.get(listSNPs[i][0])
                            pos = (
                                int(listPos[i])
                                if listPos[i]
                                else (
                                    int(listSNPs[i][1])
                                    if listSNPs[i][1]
                                    else None  # noqa E501
                                )
                            )
                            setGwas.add(
                                (
                                    rs,
                                    chm,
                                    pos,
                                    trait,
                                    snps,
                                    orBeta,
                                    allele95ci,
                                    riskAfreq,
                                    pubmedID,
                                )
                            )
                    elif len(listChm) == len(listPos):
                        for i in range(len(listChm)):
                            rs = None
                            chm = self._loki.chr_num.get(listChm[i])
                            pos = int(listPos[i]) if listPos[i] else None
                            setGwas.add(
                                (
                                    rs,
                                    chm,
                                    pos,
                                    trait,
                                    snps,
                                    orBeta,
                                    allele95ci,
                                    riskAfreq,
                                    pubmedID,
                                )
                            )
                        for i in range(len(listSNPs)):
                            rs = (
                                int(listSNPs[i][2]) if listSNPs[i][2] else None
                            )  # noqa E501
                            chm = self._loki.chr_num.get(listSNPs[i][0])
                            pos = (
                                int(listSNPs[i][1]) if listSNPs[i][1] else None
                            )  # noqa E501
                            setGwas.add(
                                (
                                    rs,
                                    chm,
                                    pos,
                                    trait,
                                    snps,
                                    orBeta,
                                    allele95ci,
                                    riskAfreq,
                                    pubmedID,
                                )
                            )
                    else:
                        numInvalid += 1
                # foreach line
            # with gwasFile
        else:
            with open(path + "/gwascatalog.txt", "r") as gwasFile:
                header = next(gwasFile).rstrip()
                if header.startswith(
                    "Date Added to Catalog\tPUBMEDID\tFirst Author\tDate\tJournal\tLink\tStudy\tDisease/Trait\tInitial Sample Size\tReplication Sample Size\tRegion\tChr_id\tChr_pos\tReported Gene(s)\tMapped_gene\tUpstream_gene_id\tDownstream_gene_id\tSnp_gene_ids\tUpstream_gene_distance\tDownstream_gene_distance\tStrongest SNP-Risk Allele\tSNPs\tMerged\tSnp_id_current\tContext\tIntergenic\tRisk Allele Frequency\tp-Value\tPvalue_mlog\tp-Value (text)\tOR or beta\t95% CI (text)\t"  # noqa E501
                ):  # "Platform [SNPs passing QC]\tCNV"
                    pass
                elif header.startswith(
                    "Date Added to Catalog\tPUBMEDID\tFirst Author\tDate\tJournal\tLink\tStudy\tDisease/Trait\tInitial Sample Description\tReplication Sample Description\tRegion\tChr_id\tChr_pos\tReported Gene(s)\tMapped_gene\tUpstream_gene_id\tDownstream_gene_id\tSnp_gene_ids\tUpstream_gene_distance\tDownstream_gene_distance\tStrongest SNP-Risk Allele\tSNPs\tMerged\tSnp_id_current\tContext\tIntergenic\tRisk Allele Frequency\tp-Value\tPvalue_mlog\tp-Value (text)\tOR or beta\t95% CI (text)\t"  # noqa E501
                ):  # "Platform [SNPs passing QC]\tCNV"
                    pass
                else:
                    self.log(
                        "GWAS - Error on GWAS catalog header: %s" % header,
                        level=logging.ERROR,
                        indent=2,
                    )  # noqa E501
                    msn_error = "Error GWAS unrecognized header"
                    self._loki.addWarning(self._sourceID, msn_error)
                    raise Exception("unrecognized file header")

                for line in gwasFile:
                    line = line.rstrip("\r\n")
                    words = list(
                        w.strip() for w in line.decode("latin-1").split("\t")
                    )  # noqa E501
                    if len(words) <= 31:
                        # blank line at the end is normal
                        if (len(words) > 1) or words[0]:
                            numInc += 1
                        continue
                    chm = (
                        self._loki.chr_num[words[11]]
                        if (words[11] in self._loki.chr_num)
                        else None
                    )
                    pos = int(words[12]) if words[12] else None
                    trait = words[7]
                    snps = (
                        words[21]
                        if words[20].endswith("aplotype")
                        else words[20]  # noqa E501
                    )
                    rses = (
                        list(int(rs[2:]) for rs in reRS.findall(snps))
                        or listNone  # noqa E501
                    )
                    orBeta = words[30]
                    allele95ci = words[31]
                    riskAfreq = words[26]
                    pubmedID = int(words[1]) if words[1] else None
                    for rs in rses:
                        setGwas.add(
                            (
                                rs,
                                chm,
                                pos,
                                trait,
                                snps,
                                orBeta,
                                allele95ci,
                                riskAfreq,
                                pubmedID,
                            )
                        )
                # foreach line
            # with gwasFile
        # if path
        self.log(
            "GWAS - Processing GWAS catalog annotations completed: %d entries (%d incomplete, %d invalid)"  # noqa E501
            % (len(setGwas), numInc, numInvalid),
            level=logging.INFO,
            indent=2,
        )
        if setGwas:
            self.log(
                "GWAS - Starting the writing GWAS catalog annotations to the database ...",  # noqa E501
                level=logging.INFO,
                indent=2,
            )  # noqa E501
            self.addGWASAnnotations(setGwas)
            self.log(
                "Writing GWAS catalog annotations to the database completed",
                level=logging.INFO,
                indent=2,
            )  # noqa E501

        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process.memory_info().rss / (1024 * 1024)  # mem in MB
        self.log(
            f"GWAS - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_before:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"GWAS - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )

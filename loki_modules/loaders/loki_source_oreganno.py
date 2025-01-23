import itertools
import logging
import psutil
import time
import os
from loki_modules import loki_source


class Source_oreganno(loki_source.Source):

    _remHost = "hgdownload.cse.ucsc.edu"
    _remPath = "/goldenPath/hg19/database/"

    _remFiles = [
        "oreganno.txt.gz",
        "oregannoAttr.txt.gz",
        "oregannoLink.txt.gz",
    ]  # noqa E501

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):

        self.downloadFilesFromHTTP(
            self._remHost,
            dict(((path + "/" + f, self._remPath + f) for f in self._remFiles)),  # noqa E501
        )

        return [os.path.join(path, f) for f in self._remFiles]

    def update(self, options, path):
        start_time = time.time()
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"Oreganno - Starting Data Ingestion (inicial memory {memory_before:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            "Oreganno - Starting deletion of old records from the database ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()
        self.log(
            "Oreganno - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # Add the 'oreganno' namespace
        ns = self.addNamespace("oreganno")

        # Add the ensembl and entrez namespaces
        external_ns = self.addNamespaces(
            [
                ("symbol", 0),
                ("entrez_gid", 0),
                ("ensembl_gid", 0),
            ]
        )

        # Add the types of Regions
        typeids = self.addTypes([("regulatory_region",), ("tfbs",), ("gene",)])

        # Add the types of groups
        group_typeid = self.addType("regulatory_group")

        # Add the role for regulatory
        snp_roleid = self.addRole(
            "regulatory", "OregAnno Regulatory Polymorphism", 1, 0
        )

        # Get the default population ID
        ldprofile_id = self.addLDProfile("", "no LD adjustment")

        # build dict of gene id->oreganno id and a dict of
        # oreganno id->entrez id and oreganno id->ensembl id
        oreg_gene = {}
        oreg_tfbs = {}
        oreg_snp = {}
        link_f = self.zfile("oregannoLink.txt.gz") # NOTE Parei aqui. Vai dar erro
        entrez_ns = external_ns["entrez_gid"]
        ensembl_ns = external_ns["ensembl_gid"]
        symbol_ns = external_ns["symbol"]

        self.log(
            "Oreganno - Starting the parsing external links ...",
            level=logging.INFO,
            indent=2,
        )
        for lx in link_f:
            fields = lx.split()
            if fields[1] == "Gene":
                oreg_id = fields[0]
                if fields[2] in ("EnsemblGene", "EnsemblId"):
                    gene_id = fields[3].split(",")[
                        -1
                    ]  # used to be "Homo_sapiens,ENSG123" but now just "ENSG123"  # noqa E501
                    oreg_gene.setdefault(oreg_id, {})[ensembl_ns] = gene_id
                elif fields[2] in ("EntrezGene", "NCBIGene"):
                    gene_id = fields[3]
                    oreg_gene.setdefault(oreg_id, {})[entrez_ns] = gene_id
            elif fields[1] == "TFbs":
                oreg_id = fields[0]
                if fields[2] in ("EnsemblGene", "EnsemblId"):
                    gene_id = fields[3].split(",")[
                        -1
                    ]  # used to be "Homo_sapiens,ENSG123" but now just "ENSG123"  # noqa E501
                    oreg_tfbs.setdefault(oreg_id, {})[ensembl_ns] = gene_id
                elif fields[2] in ("EntrezGene", "NCBIGene"):
                    gene_id = fields[3]
                    oreg_tfbs.setdefault(oreg_id, {})[entrez_ns] = gene_id
            elif fields[1] == "ExtLink" and fields[2] == "dbSNP":
                # Just store the RS# (no leading "rs")
                oreg_snp[fields[0]] = fields[3][2:]
        # for l
        self.log(
            "Oreganno - Parsing external links: %d genes, %d TFBs, %d SNPs"
            % (len(oreg_gene), len(oreg_tfbs), len(oreg_snp)),
            level=logging.INFO,
            indent=2,
        )

        # Now, create a dict of oreganno id->type
        oreganno_type = {}
        self.log(
            "Oreganno - Starting the parsing region attributes ... ",
            level=logging.INFO,
            indent=2,
        )
        attr_f = self.zfile("oregannoAttr.txt.gz")
        for lx in attr_f:
            fields = lx.split("\t")
            if fields[1] == "type":
                oreganno_type[fields[0]] = fields[2]
            elif fields[1] == "Gene":
                oreg_gene.setdefault(fields[0], {})[symbol_ns] = fields[2]
            elif fields[1] == "TFbs":
                oreg_tfbs.setdefault(fields[0], {})[symbol_ns] = fields[2]
        # for l
        self.log(
            "Oreganno - Parsing region attributes: %d genes, %d TFBs"
            % (len(oreg_gene), len(oreg_tfbs)),
            level=logging.INFO,
            indent=2,
        )  # noqa E501

        # OK, now parse the actual regions themselves
        region_f = self.zfile("oreganno.txt.gz")
        oreganno_roles = []
        oreganno_regions = []
        oreganno_bounds = []
        oreganno_groups = {}
        oreganno_types = {}

        self.log(
            "Oreganno - Starting the parsing regulatory regions ... ",
            level=logging.INFO,
            indent=2,
        )  # noqa E501
        snps_unmapped = 0
        for lx in region_f:
            fields = lx.split()
            chrom = self._loki.chr_num.get(fields[1][3:])
            start = int(fields[2]) + 1
            stop = int(fields[3])
            oreg_id = fields[4]
            oreg_type = oreganno_type[
                oreg_id
            ].upper()  # used to be CAPS, now Title Case
            if chrom and oreg_type == "REGULATORY POLYMORPHISM":
                entrez_id = oreg_gene.get(oreg_id, {}).get(entrez_ns)
                rsid = oreg_snp.get(oreg_id)
                if entrez_id and rsid:
                    oreganno_roles.append((int(rsid), entrez_id, snp_roleid))
                else:
                    snps_unmapped += 1
            elif chrom and (
                oreg_type == "REGULATORY REGION"
                or oreg_type == "TRANSCRIPTION FACTOR BINDING SITE"
            ):
                gene_symbol = oreg_gene.get(oreg_id, {}).get(symbol_ns)
                if not gene_symbol:
                    gene_symbol = oreg_tfbs.get(oreg_id, {}).get(symbol_ns)

                if gene_symbol:
                    oreganno_groups.setdefault(gene_symbol, []).append(oreg_id)

                if oreg_type == "REGULATORY REGION":
                    oreg_typeid = typeids["regulatory_region"]
                else:
                    oreg_typeid = typeids["tfbs"]

                oreganno_types[oreg_id] = oreg_typeid
                oreganno_regions.append((oreg_typeid, oreg_id, ""))
                oreganno_bounds.append((chrom, start, stop))
            # if chrom and oreg_type
        # for l
        self.log(
            "Oreganno - Parsing regulatory regions (%d regions found, %d SNPs found, %d SNPs unmapped)"  # noqa E501
            % (len(oreganno_regions), len(oreganno_roles), snps_unmapped),
            level=logging.INFO,
            indent=2,
        )

        self.log(
            "Oreganno - Starting the writing to database ... ",
            level=logging.INFO,
            indent=2,
        )  # noqa E501
        self.addSNPEntrezRoles(oreganno_roles)
        reg_ids = self.addBiopolymers(oreganno_regions)
        self.addBiopolymerNamespacedNames(
            ns,
            (
                (reg_ids[i], oreganno_regions[i][1])
                for i in range(len(reg_ids))  # noqa E501
            ),  # noqa E501
        )
        bound_gen = zip(((r,) for r in reg_ids), oreganno_bounds)
        self.addBiopolymerLDProfileRegions(
            ldprofile_id, ((itertools.chain(*c) for c in bound_gen))
        )

        # Now, add the regulation groups
        oreg_genes = list(oreganno_groups.keys())
        oreg_gids = self.addTypedGroups(
            group_typeid,
            (
                ("regulatory_%s" % k, "OregAnno Regulation of %s" % k)
                for k in oreg_genes
            ),
        )
        self.addGroupNamespacedNames(
            ns, zip(oreg_gids, ("regulatory_%s" % k for k in oreg_genes))
        )

        group_membership = []
        for i in range(len(oreg_gids)):
            gid = oreg_gids[i]
            gene_key = oreg_genes[i]
            gene_member = set()
            tfbs_member = {}
            member_num = 2
            for oreg_id in oreganno_groups[gene_key]:
                member_num += 1
                group_membership.append(
                    (
                        gid,
                        member_num,
                        oreganno_types.get(oreg_id, 0),
                        ns,
                        oreg_id,
                    )  # noqa E501
                )
                for external_nsid, external_val in oreg_gene.get(
                    oreg_id, {}
                ).items():  # noqa E501
                    gene_member.add(
                        (gid, 1, typeids["gene"], external_nsid, external_val)
                    )

                member_num += 1
                for external_nsid, external_val in oreg_tfbs.get(
                    oreg_id, {}
                ).items():  # noqa E501
                    tfbs_member.setdefault(external_nsid, {})[
                        external_val
                    ] = member_num  # noqa E501

            group_membership.extend(gene_member)
            for ext_ns, d in tfbs_member.items():
                for sym, mn in d.items():
                    group_membership.append(
                        (gid, mn, typeids["gene"], ext_ns, sym)
                    )  # noqa E501

        self.addGroupMemberNames(group_membership)

        self.log(
            "Oreganno - Writing to database completed",
            level=logging.INFO,
            indent=2,
        )  # noqa E501

        # store source metadata
        self.setSourceBuilds(None, 19)
        # TODO: check for latest FTP path rather than hardcoded
        # /goldenPath/hg19/database/

        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process.memory_info().rss / (1024 * 1024)  # mem in MB
        self.log(
            f"Oreganno - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_before:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"Oreganno - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )

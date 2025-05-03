import re
import ast
import pandas as pd
from biofilter.db.models.omics_models import (
    Gene,
    GeneGroup,
    GeneGroupMembership,
    LocusGroup,
    LocusType,
    GenomicRegion,
    GeneLocation,
    OmicStatus,
)  # noqa: E501


class GeneQueryMixin:

    def get_or_create_locus_group(self, name: str):
        """
        Retrieves an existing LocusGroup by name or creates a new one.

        Args:
            row (dict-like): A row containing 'locus_group' field.

        Returns:
            LocusGroup or None
        """
        if not name or not isinstance(name, str):
            return None

        name_clean = name.strip()
        if not name_clean:
            return None

        group = (
            self.session.query(LocusGroup).filter_by(name=name_clean).first()
        )  # noqa: E501
        if group:
            return group

        # Create new LocusGroup
        locus_group = LocusGroup(name=name_clean)
        self.session.add(locus_group)
        self.session.flush()  # commits later in batch
        msg = f"LocusGroup '{name_clean}' created"
        self.logger.log(msg, "DEBUG")
        return locus_group

    def get_or_create_locus_type(self, name: str):
        """
        Retrieves an existing LocusType by name or creates a new one.

        Args:
            row (dict-like): A row containing 'locus_type' field.

        Returns:
            LocusType or None
        """
        if not name or not isinstance(name, str):
            return None

        name_clean = name.strip()
        if not name_clean:
            return None

        locus_type = (
            self.session.query(LocusType).filter_by(name=name_clean).first()
        )  # noqa: E501
        if locus_type:
            return locus_type

        # Create new LocusType
        locus_type = LocusType(name=name_clean)
        self.session.add(locus_type)
        self.session.flush()  # commits later in batch
        self.logger.log(f"Created new LocusType: {name_clean}", "DEBUG")
        return locus_type

    def get_or_create_genomic_region(
        self,
        label: str,
        chromosome: str = None,
        start: int = None,
        end: int = None,
        description: str = None,
    ):
        """
        Returns an existing GenomicRegion by label, or creates a new one.
        """
        if not label or not isinstance(label, str):
            return None

        label_clean = label.strip()
        if not label_clean:
            return None

        region = (
            self.session.query(GenomicRegion)
            .filter_by(label=label_clean)
            .first()  # noqa: E501
        )  # noqa: E501
        if region:
            return region

        region = GenomicRegion(
            label=label_clean,
            chromosome=chromosome,
            start=start,
            end=end,
            description=description,
        )
        self.session.add(region)
        self.session.flush()
        msg = f"GenomicRegion '{label_clean}' created"
        self.logger.log(msg, "DEBUG")
        return region

    def get_or_create_gene_location(
        self,
        gene: Gene,
        chromosome: str = None,
        start: int = None,
        end: int = None,
        strand: str = None,
        region: GenomicRegion = None,
        assembly: str = "GRCh38",
        data_source_id: int = None,
    ):
        """
        GET or Create a location entry for the associated Gene.

        Returns:
            GeneLocation instance
        """
        if not gene:
            msg = "⚠️ Gene Location invalid: Gene not provided"
            self.logger.log(msg, "WARNING")
            return None

        # Check if the location already exists
        existing_location = (
            self.session.query(GeneLocation)
            .filter_by(
                gene_id=gene.id,
                chromosome=chromosome,
                start=start,
                end=end,
                strand=strand,
                region_id=region.id if region else None,
                assembly=assembly,
                data_source_id=data_source_id,
            )
            .first()
        )

        if existing_location:
            # msg = f"♻️ GeneLocation already exists for Gene '{gene.id}' on chromosome {chromosome}"  # noqa: E501
            # self.logger.log(msg, "DEBUG")
            return existing_location

        # Create new if it does not exist
        location = GeneLocation(
            gene_id=gene.id,
            chromosome=chromosome,
            start=start,
            end=end,
            strand=strand,
            region_id=region.id if region else None,
            assembly=assembly,
            data_source_id=data_source_id,
        )

        self.session.add(location)
        self.session.commit()

        msg = f"📌 GeneLocation created for Gene '{gene.id}' on chromosome {chromosome}"
        self.logger.log(msg, "DEBUG")

        return location

    def get_status_id(self, name: str) -> int:
        status = self.session.query(OmicStatus).filter_by(name=name).first()
        if not status:
            raise ValueError(f"OmicStatus '{name}' not found.")
        return status.id

    def get_or_create_gene(
        self,
        symbol: str,
        hgnc_status: str = None,
        hgnc_id: str = None,
        entrez_id: str = None,
        ensembl_id: str = None,
        entity_id: int = None,
        data_source_id: int = None,
        locus_group=None,
        locus_type=None,
        gene_group_names: list = None,
    ):
        """
        Creates or retrieves a gene based on unique identifiers (hgnc_id,
        entrez_id or entity_id). Also manages linking with GeneGroup and
        Memberships.
        """

        conflict_flag = False

        if not symbol:
            msg = f"⚠️ Gene {hgnc_id} ignored: empty symbol"
            self.logger.log(msg, "WARNING")
            return None

        # Normaliza os IDs
        hgnc_id, entrez_id, ensembl_id = (
            self.conflict_mgr.normalize_gene_identifiers(  # noqa: E501
                hgnc_id, entrez_id, ensembl_id
            )
        )

        # Check Conflict
        result = self.conflict_mgr.detect_gene_conflict(
            hgnc_id=hgnc_id,
            entrez_id=entrez_id,
            ensembl_id=ensembl_id,
            entity_id=entity_id,
            symbol=symbol,
            data_source_id=data_source_id,
        )

        # Gene in conflict
        if result == "CONFLICT":
            conflict_flag = True
            status_id = self.get_status_id("conflict")

        # Gene already exists
        elif result:
            return result, conflict_flag

        # New gene
        else:
            status_id = self.get_status_id("active")

        gene = Gene(
            # symbol=symbol,
            omic_status_id=status_id,
            hgnc_status=hgnc_status,
            entity_id=entity_id,
            hgnc_id=hgnc_id,
            entrez_id=entrez_id,
            ensembl_id=ensembl_id,
            data_source_id=data_source_id,
            locus_group=locus_group,
            locus_type=locus_type,
        )
        self.session.add(gene)
        self.session.flush()
        msg = f"🧬 New Gene '{symbol}' created"
        self.logger.log(msg, "INFO")

        # Association with GeneGroup
        group_objs = []
        if gene_group_names:
            for group_name in gene_group_names:
                if not group_name:
                    continue
                group = (
                    self.session.query(GeneGroup)
                    .filter_by(name=group_name.strip())
                    .first()
                )
                if not group:
                    group = GeneGroup(name=group_name.strip())
                    self.session.add(group)
                    self.session.flush()
                    msg = f"🧩 GeneGroup '{group_name}' created"
                    self.logger.log(msg, "DEBUG")
                group_objs.append(group)

        # Link Genes and Groups
        existing_links = {
            g.group_id
            for g in self.session.query(GeneGroupMembership).filter_by(
                gene_id=gene.id
            )  # noqa: E501
        }

        new_links = 0
        for group in group_objs:
            if group.id not in existing_links:
                membership = GeneGroupMembership(
                    gene_id=gene.id, group_id=group.id
                )  # noqa: E501
                self.session.add(membership)
                new_links += 1

        self.session.commit()
        msg = f"Gene '{symbol}' linked with {len(group_objs)} group(s), {new_links} new links added"  # noqa: E501
        self.logger.log(msg, "INFO")

        return gene, conflict_flag

    # def create_gene_location(
    #     self,
    #     gene: Gene,
    #     chromosome: str = None,
    #     start: int = None,
    #     end: int = None,
    #     strand: str = None,
    #     region: GenomicRegion = None,
    #     assembly: str = "GRCh38",
    #     data_source_id: int = None,
    # ):
    #     """
    #     Create a location entry for the associated Gene.

    #     Returns:
    #         GeneLocation instance
    #     """
    #     if not gene:
    #         msg = "⚠️ Gene Location invalid: Gene not provided"
    #         self.logger.log(msg, "WARNING")
    #         return None

    #     location = GeneLocation(  # BUG: GeneLocation should be created only once         # noqa: E501
    #         gene_id=gene.id,
    #         chromosome=chromosome,
    #         start=start,
    #         end=end,
    #         strand=strand,
    #         region_id=region.id if region else None,
    #         assembly=assembly,
    #         data_source_id=data_source_id,
    #     )

    #     self.session.add(location)
    #     self.session.commit()

    #     msg = f"📌 GeneLocation created for Gene '{gene.id}' on chromosome {chromosome}"  # noqa E501
    #     self.logger.log(msg, "DEBUG")

    #     return location

    def parse_gene_groups(self, group_data) -> list:
        """
        Normalization of the gene_group field to a list of strings.

        Args:
            group_data: Can be a string (literal list or single value), a real
                        list, None, or missing values like pd.NA.

        Returns:
            List of group names as cleaned strings.
        """

        # First, if it's None directly
        if group_data is None:
            return []

        # Treatment of missing values
        if isinstance(group_data, list):
            return [
                g.strip()
                for g in group_data
                if isinstance(g, str) and g.strip()  # noqa: E501
            ]  # noqa: E501

        # Treatment of clearly null or empty values
        if group_data is None or pd.isna(group_data):
            return []

        # Treatment of empty string
        if isinstance(group_data, str) and group_data.strip() == "":
            return []

        # Treatment of string that repres a list (ex: "['GroupA', 'GroupB']")
        if isinstance(group_data, str):
            if group_data.strip() == "":
                return []
            try:
                parsed = ast.literal_eval(group_data)
                return parsed if isinstance(parsed, list) else [parsed]
            except (ValueError, SyntaxError):
                clean = group_data.strip()
                return [clean] if clean else []

        # Treatment of lists
        if isinstance(group_data, list):
            return [
                g.strip()
                for g in group_data
                if isinstance(g, str) and g.strip()  # noqa: E501
            ]  # noqa: E501

        # Converts other types to string
        return [str(group_data).strip()]

    def extract_chromosome(self, location_sortable):
        if pd.isna(location_sortable) or not location_sortable:
            return None

        match = re.match(r"^([0-9XYMT]+)", str(location_sortable).upper())
        if match:
            return match.group(1)
        return None

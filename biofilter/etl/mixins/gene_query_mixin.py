import re
import ast
import pandas as pd

from biofilter.db.models import (
    GeneMaster,
    GeneGroup,
    GeneGroupMembership,
    GeneLocusGroup,
    GeneLocusType,
    # GeneGenomicRegion,
    # GeneLocation,
)  # noqa: E501

from biofilter.utils.utilities import as_list


class GeneQueryMixin:

    def get_or_create_locus_group(
        self,
        name: str,
        data_source_id: int = None,
        package_id: int = None,
    ):
        """
        Retrieves an existing LocusGroup by name or creates a new one.

        Args:
            row (dict-like): A row containing 'locus_group' field.

        Returns:
            LocusGroup or None
        """

        if not name or not isinstance(name, str):
            return None, True

        try:
            name_clean = name.strip()
            if not name_clean:
                return None, True

            group = (
                self.session.query(GeneLocusGroup)
                .filter_by(name=name_clean)
                .first()  # noqa: E501
            )  # noqa: E501
            if group:
                return group, True

            # Create new LocusGroup
            locus_group = GeneLocusGroup(
                name=name_clean,
                data_source_id=data_source_id,
                etl_package_id=package_id,
            )
            self.session.add(locus_group)
            self.session.flush()  # commits later in batch
            msg = f"LocusGroup '{name_clean}' created"
            self.logger.log(msg, "DEBUG")
            return locus_group, True

        except Exception as e:
            self.session.rollback()
            msg = f"⚠️  Error in Locus Group insert, error: '{e}'"
            self.logger.log(msg, "DEBUG")
            return None, False

    def get_or_create_locus_type(
        self,
        name: str,
        data_source_id: int = None,
        package_id: int = None,
    ):
        """
        Retrieves an existing LocusType by name or creates a new one.

        Args:
            row (dict-like): A row containing 'locus_type' field.

        Returns:
            LocusType or None
        """
        if not name or not isinstance(name, str):
            return None, True

        try:
            name_clean = name.strip()
            if not name_clean:
                return None, True

            locus_type = (
                self.session.query(GeneLocusType)
                .filter_by(name=name_clean)
                .first()  # noqa E501
            )  # noqa: E501
            if locus_type:
                return locus_type, True

            # Create new LocusType
            locus_type = GeneLocusType(
                name=name_clean,
                data_source_id=data_source_id,
                etl_package_id=package_id,
            )
            self.session.add(locus_type)
            self.session.flush()  # commits later in batch
            self.logger.log(f"Created new LocusType: {name_clean}", "DEBUG")
            return locus_type, True

        except Exception as e:
            self.session.rollback()
            msg = f"⚠️  Error in Locus Type insert, error: '{e}'"
            self.logger.log(msg, "DEBUG")
            return None, False

    # def get_or_create_genomic_region(
    #     self,
    #     label: str,
    #     chromosome: str = None,
    #     start: int = None,
    #     end: int = None,
    #     data_source_id: int = None,
    #     package_id: int = None,
    # ):
    #     """
    #     Returns an existing GenomicRegion by label, or creates a new one.
    #     """
    #     if not label or not isinstance(label, str):
    #         return None, True

    #     label_clean = label.strip()
    #     if not label_clean:
    #         return None, True

    #     try:
    #         region = (
    #             self.session.query(GeneGenomicRegion)
    #             .filter_by(label=label_clean)
    #             .first()  # noqa: E501
    #         )  # noqa: E501
    #         if region:
    #             return region, True

    #         region = GeneGenomicRegion(
    #             label=label_clean,
    #             chromosome=chromosome,
    #             start_pos=start,
    #             end_pos=end,
    #             description="",
    #             data_source_id=data_source_id,
    #             etl_package_id=package_id,
    #         )
    #         self.session.add(region)
    #         self.session.flush()
    #         msg = f"GenomicRegion '{label_clean}' created"
    #         self.logger.log(msg, "DEBUG")
    #         return region, True

    #     except Exception as e:
    #         self.session.rollback()
    #         msg = f"⚠️  Error in Genomic Region insert, error: {e}"
    #         self.logger.log(msg, "DEBUG")
    #         return None, False

    # def get_or_create_gene_location(
    #     self,
    #     gene: GeneMaster,
    #     chromosome: str = None,
    #     start: int = None,
    #     end: int = None,
    #     strand: str = None,
    #     region: GeneGenomicRegion = None,
    #     assembly: str = "GRCh38",  # TODO: Fix it?
    #     data_source_id: int = None,
    #     package_id: int = None,
    # ):
    #     """
    #     GET or Create a location entry for the associated Gene.

    #     Returns:
    #         GeneLocation instance
    #     """
    #     if not gene:
    #         msg = "⚠️  Gene Location invalid: Gene not provided"
    #         self.logger.log(msg, "WARNING")
    #         return None, True

    #     # Check if the location already exists
    #     existing_location = (
    #         self.session.query(GeneLocation)
    #         .filter_by(
    #             gene_id=gene.id,
    #             chromosome=chromosome,
    #             start_pos=start,
    #             end_pos=end,
    #             strand=strand,
    #             region_id=region.id if region else None,
    #             assembly=assembly,
    #             data_source_id=data_source_id,
    #         )
    #         .first()
    #     )

    #     if existing_location:
    #         return existing_location, True

    #     try:
    #         # Create new if it does not exist
    #         location = GeneLocation(
    #             gene_id=gene.id,
    #             chromosome=chromosome,
    #             start_pos=start,
    #             end_pos=end,
    #             strand=strand,
    #             region_id=region.id if region else None,
    #             assembly=assembly,
    #             data_source_id=data_source_id,
    #             etl_package_id=package_id,
    #         )

    #         self.session.add(location)
    #         self.session.commit()

    #         msg = f"📌 GeneLocation created for Gene '{gene.id}' on chromosome {chromosome}"  # noqa E501
    #         self.logger.log(msg, "DEBUG")

    #         return location, True

    #     except Exception as e:
    #         self.session.rollback()
    #         msg = f"⚠️  Error in Gene Location insert, error: '{e}'"
    #         self.logger.log(msg, "WARNING")
    #         return None, False

    def get_or_create_gene(
        self,
        status_id: int,
        symbol: str,
        hgnc_status: str = None,
        entity_id: int = None,
        chromosome: str = None,
        data_source_id: int = None,
        locus_group=None,
        locus_type=None,
        gene_group_names: list = None,
        package_id: int = None,
    ):
        """
        Creates or retrieves a gene based on unique identifiers (hgnc_id,
        entrez_id or entity_id). Also manages linking with GeneGroup and
        Memberships.
        """

        conflict_flag = False

        if not symbol:
            msg = f"⚠️ Gene {symbol} ignored: empty symbol"
            self.logger.log(msg, "WARNING")
            return None, conflict_flag, True

        # TODO: 🚧 Conflict was desable after schame changes in 3.0.1 🚧
        # # Normaliza os IDs
        # hgnc_id, entrez_id, ensembl_id = (
        #     self.conflict_mgr.normalize_gene_identifiers(  # noqa: E501
        #         hgnc_id, entrez_id, ensembl_id
        #     )
        # )

        # # Check Conflict
        # result = self.conflict_mgr.detect_gene_conflict(
        #     hgnc_id=hgnc_id,
        #     entrez_id=entrez_id,
        #     ensembl_id=ensembl_id,
        #     entity_id=entity_id,
        #     symbol=symbol,
        #     data_source_id=data_source_id,
        # )

        # # Gene in conflict
        # if result == "CONFLICT":
        #     conflict_flag = True
        #     status_id = self.get_status_id("conflict")

        # # Gene already exists
        # elif result:
        #     return result, conflict_flag

        # Check if Gene Master exist
        query = self.session.query(GeneMaster).filter_by(
            entity_id=entity_id,
        )
        gene = query.first()
        if gene:
            return gene, conflict_flag, True

        try:
            gene = GeneMaster(
                omic_status_id=status_id,
                hgnc_status=hgnc_status,
                entity_id=entity_id,
                symbol=symbol,
                chromosome=chromosome,
                data_source_id=data_source_id,
                gene_locus_group=locus_group,
                gene_locus_type=locus_type,
                etl_package_id=package_id,
            )
            self.session.add(gene)
            self.session.flush()
            msg = f"🧬 New Gene '{symbol}' created"
            self.logger.log(msg, "DEBUG")

        except Exception as e:
            self.session.rollback()
            msg = f"⚠️  Error in Gene insert, error: '{e}'"
            self.logger.log(msg, "WARNING")
            return None, conflict_flag, False

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
                    try:
                        group = GeneGroup(
                            name=group_name.strip(),
                            data_source_id=data_source_id,
                            etl_package_id=package_id,
                        )
                        self.session.add(group)
                        self.session.flush()
                        msg = f"🧩 GeneGroup '{group_name}' created"
                        self.logger.log(msg, "DEBUG")
                    except Exception as e:
                        self.session.rollback()
                        msg = f"⚠️  Error in Gene group insert, error: '{e}'"
                        self.logger.log(msg, "WARNING")
                        return None, conflict_flag, False

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
                try:
                    membership = GeneGroupMembership(
                        gene_id=gene.id,
                        group_id=group.id,
                        data_source_id=data_source_id,
                        etl_package_id=package_id,
                    )  # noqa: E501
                    self.session.add(membership)
                    new_links += 1
                except Exception as e:
                    self.session.rollback()
                    msg = f"⚠️  Error in Gene Group MemberShip insert, error: {e}"  # noqa E501
                    self.logger.log(msg, "WARNING")
                    return None, conflict_flag, False

        try:
            self.session.commit()
            msg = f"Gene '{symbol}' linked with {len(group_objs)} group(s), {new_links} new links added"  # noqa: E501
            self.logger.log(msg, "DEBUG")
        except Exception as e:
            self.session.rollback()
            msg = f"⚠️  Error in Commit {symbol} Gene, error: '{e}'"
            self.logger.log(msg, "WARNING")
            return None, conflict_flag, False

        return gene, conflict_flag, True

    def parse_gene_groups(self, group_data) -> list:
        """
        Normalization of the gene_group field to a list of strings.

        Args:
            group_data: Can be a string (literal list or single value), a real
                        list, None, or missing values like pd.NA.

        Returns:
            List of group names as cleaned strings.
        """

        # When read from PARQUET we receives as array object data
        group_data = as_list(group_data)

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

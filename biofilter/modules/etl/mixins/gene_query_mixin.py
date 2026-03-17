import ast
import re

import pandas as pd

from biofilter.modules.db.models import (  # noqa: E501
    GeneGroup,
    GeneGroupMembership,
    GeneLocusGroup,
    GeneLocusType,
    GeneMaster,
)
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

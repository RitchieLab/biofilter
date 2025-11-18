import os
import csv
import json
import time  # DEBUG MODE
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.db.models import (
    OmicStatus,
    DiseaseGroup,
    DiseaseGroupMembership,
    DiseaseMaster
)  # noqa E501


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.use_conflict_csv = use_conflict_csv
        self.conflict_mgr = ConflictManager(session, logger)

        # DTP versioning
        self.dtp_name = "dtp_mondo"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Download mondo.json from MONDO PURL and store it locally.
        Also computes a file hash to track content versioning.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(msg, "INFO")

        try:
            # Check compatibility
            self.check_compatibility()

            source_url = self.data_source.source_url  # note: MONDO file URL
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            file_path = os.path.join(landing_path, "mondo.json")

            # Download the file with streaming (big file!)
            msg = f"⬇️  Fetching MONDO JSON from: {source_url}"
            self.logger.log(msg, "INFO")

            with requests.get(source_url, stream=True) as r:
                if r.status_code != 200:
                    msg = f"Failed to fetch MONDO: {r.status_code}"
                    self.logger.log(msg, "ERROR")
                    return False, msg, None

                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            # Compute file hash
            current_hash = compute_file_hash(file_path)

            msg = f"✅ MONDO file downloaded to {file_path}"
            self.logger.log(msg, "INFO")

            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None



    def _normalize_mondo_id(self, node_id: str) -> str:
        """
        Convert MONDO IRIs to compact form (MONDO:0000722).
        """
        if not node_id:
            return None
        if node_id.startswith("http://purl.obolibrary.org/obo/MONDO_"):
            return node_id.replace("http://purl.obolibrary.org/obo/MONDO_", "MONDO:")
        return node_id  # keep as-is if already compact or different ontology


    # def _normalize_id(self, uri: str):
    #     """
    #     Normalize an ontology URI into (prefix, code).
    #     Examples:
    #         http://purl.obolibrary.org/obo/MONDO_0009168 -> ("MONDO", "0009168")
    #         http://identifiers.org/hgnc/20105 -> ("HGNC", "20105")
    #         http://purl.obolibrary.org/obo/CHEBI_15705 -> ("CHEBI", "15705")
    #     """
    #     if not uri:
    #         return None, None

    #     # MONDO/CHEBI/etc. often use underscore separator
    #     if "obo/" in uri:
    #         token = uri.split("/")[-1]  # MONDO_0009168
    #         if "_" in token:
    #             prefix, code = token.split("_", 1)
    #             return prefix, code

    #     # identifiers.org URIs (e.g., hgnc/20105)
    #     if "identifiers.org" in uri:
    #         parts = uri.split("/")
    #         prefix = parts[-2].upper()
    #         code = parts[-1]
    #         return prefix, code

    #     return None, None
    def _normalize_id(self, raw_id: str):
        """
        Normalize MONDO relationship IDs into (prefix, code, alias_value).
        Returns (prefix, code, alias_value) or (None, None, None) if not parsable.
        """

        if not raw_id:
            return None, None, None

        # Case 1: already in PREFIX:CODE
        if ":" in raw_id and not raw_id.startswith("http"):
            prefix, code = raw_id.split(":", 1)
            return prefix.upper(), code, f"{prefix.upper()}:{code}"

        # Case 2: OBO PURL (e.g. http://purl.obolibrary.org/obo/MONDO_0002107)
        if "purl.obolibrary.org/obo/" in raw_id:
            last = raw_id.split("/")[-1]  # MONDO_0002107
            if "_" in last:
                prefix, code = last.split("_", 1)
                return prefix.upper(), code, f"{prefix.upper()}:{code}"

        # Case 3: identifiers.org (e.g. http://identifiers.org/hgnc/21638)
        if "identifiers.org/" in raw_id:
            parts = raw_id.split("/")
            if len(parts) >= 2:
                prefix = parts[-2]
                code = parts[-1]
                return prefix.upper(), code, f"{prefix.upper()}:{code}"

        # Case 4: fallback (take after last # or /)
        if "#" in raw_id:
            code = raw_id.split("#")[-1]
            return "MONDO", code.upper(), f"MONDO:{code.upper()}"
        if "/" in raw_id:
            code = raw_id.split("/")[-1]
            return "MONDO", code.upper(), f"MONDO:{code.upper()}"

        return None, None, None


    def _map_entity_group(self, prefix: str):
        """
        Map ontology prefix to Biofilter3R entity group.
        Extend as needed.
        """
        mapping = {
            "MONDO": "diseases",
            "HGNC": "genes",
            "NCBIGENE": "genes",
            "CHEBI": "chemicals",
            "UBERON": "anatomy",
            "SO": "sequenceontology",
            # TODO: Add more types (need to be link with EntityGroup)
        }
        return mapping.get(prefix.upper(), "Unknown")


    def _map_predicate(self, pred_uri: str):
        """
        Map predicate URI to simplified relation type.
        """
        mapping = {
            "is_a": "is_a",
            "http://purl.obolibrary.org/obo/RO_0004003": "Disease_has_disruption",
            "http://purl.obolibrary.org/obo/RO_0004026": "located_in",
            "http://purl.obolibrary.org/obo/RO_0002162": "in_taxon",
            # Add more as needed
        }
        return mapping.get(pred_uri, pred_uri)  # fallback = raw


    def _transform_edges(self, mondo, output_path):
        """
        Extract and normalize MONDO edges into CSV for Biofilter3R.
        """
        edges = mondo.get("graphs", [])[0].get("edges", [])
        rel_file = output_path / "entity_relations.csv"

        with open(rel_file, "w", newline="") as rf:
            writer = csv.writer(rf)
            writer.writerow([
                "entity1_id", "entity1_group", "entity1_prefix", "entity1_alias",
                "entity2_id", "entity2_group", "entity2_prefix", "entity2_alias",
                "relation_type", "data_source"
            ])

            for e in edges:
                subj = e.get("sub")
                obj = e.get("obj")
                pred = e.get("pred")

                prefix1, code1, alias1 = self._normalize_id(subj)
                prefix2, code2, alias2 = self._normalize_id(obj)

                if not alias1 or not alias2:
                    continue  # skip if malformed

                if not prefix1 or not code1 or not prefix2 or not code2:
                    continue  # skip if malformed

                group1 = self._map_entity_group(prefix1)
                group2 = self._map_entity_group(prefix2)
                relation_type = self._map_predicate(pred)

                writer.writerow([
                    code1, group1, prefix1, alias1,
                    code2, group2, prefix2, alias2,
                    relation_type, "MONDO"
                ])

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):
        """
        Transform MONDO JSON into master_data + relationships
        """

        msg = f"⚙️ Starting transform of {self.data_source.name}..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        if self.debug_mode:
            start_total = time.time()

        try:
            input_path = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            output_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )
            output_path.mkdir(parents=True, exist_ok=True)

            # Input file path
            input_file = input_path / "mondo.json"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg

            # Load JSON (obo graph format)
            with open(input_file, "r") as f:
                mondo = json.load(f)

            nodes = mondo.get("graphs", [])[0].get("nodes", [])
            edges = mondo.get("graphs", [])[0].get("edges", [])

            # --- MASTER DATA ---
            master_records = []
            try:
                for n in nodes:
                    raw_id = n.get("id")
                    node_id = self._normalize_mondo_id(raw_id)

                    # skip non-MONDO diseases
                    if not node_id or not node_id.startswith("MONDO:"):
                        continue

                    description = None
                    if "definition" in n.get("meta", {}):
                        description = n["meta"]["definition"].get("val")

                    synonyms = [s.get("val") for s in n.get("meta", {}).get("synonyms", [])]
                    xrefs = [x.get("val") for x in n.get("meta", {}).get("xrefs", [])]
                    subsets = [
                        s.replace("http://purl.obolibrary.org/obo/mondo#", "")
                        for s in n.get("meta", {}).get("subsets", [])
                    ]

                    master_records.append({
                        "mondo_id": node_id,
                        "label": n.get("lbl"),
                        "description": description,
                        "iri": n.get("iri"),
                        "is_obsolete": n.get("meta", {}).get("deprecated", False),
                        "synonyms": synonyms,
                        "xrefs": xrefs,
                        "subsets": subsets,
                    })
            except Exception as e:
                print(e)

            df_master = pd.DataFrame(master_records)
            # MONDO:0000001 is Dummy/Root Disease
            df_master = df_master[df_master["mondo_id"] != "MONDO:0000001"]

            # --- RELATIONSHIPS ---
            rel_records = []
            for e in edges:
                subj, obj, pred = e.get("sub"), e.get("obj"), e.get("pred")
                prefix1, code1, alias1 = self._normalize_id(subj)
                prefix2, code2, alias2 = self._normalize_id(obj)

                if not prefix1 or not code1 or not prefix2 or not code2:
                    continue

                group1 = self._map_entity_group(prefix1)
                group2 = self._map_entity_group(prefix2)
                relation_type = self._map_predicate(pred)

                rel_records.append({
                    # "entity1_id": code1,
                    "term1_group": group1,
                    "term1_prefix": prefix1,
                    "term1_code": alias1,
                    # "entity2_id": code2,
                    "term2_group": group2,
                    "term2_prefix": prefix2,
                    "term2_code": alias2,
                    "relation_type": relation_type,
                })

            df_rels = pd.DataFrame(rel_records)

            # Save both
            df_master.to_parquet(output_path / "master_data.parquet", index=False)  # noqa E501
            df_rels.to_parquet(output_path / "relationship_data.parquet", index=False)  # noqa E501

            if self.debug_mode:
                df_master.to_csv(output_path / "master_data.csv", index=False)
                df_rels.to_csv(output_path / "relationship_data.csv", index=False)
                end_time = time.time() - start_total
                msg = str(
                    f"processed {len(df_master)} records - and {len(df_rels)} relationships /  Time Total: {end_time:.2f}s |"  # noqa E501
                )  # noqa E501
                self.logger.log(msg, "DEBUG")

            msg = f"✅ MONDO transformed into master_data + relationships at {output_path}"  # noqa E501
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        """
        Load MONDO diseases into DiseaseMaster and related tables.

        Inputs:
            - disease_master.csv (or parquet) from transform()
            - optionally a pre-loaded DataFrame (df_override)
        """

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",
        )

        # CHECK COMPARTIBILITY
        self.check_compatibility()

        # VARIABLES TO LOAD PROCESS
        if self.debug_mode:
            start_total = time.time()

        # Setting variables
        total_diseases = 0
        total_warnings = 0

        # ALIASES MAP FROM PROCESS DATA FIELDS
        self.alias_schema = {
            "mondo_id": ("code", "MONDO", True),
            "label": ("label", "MONDO", None),
            "synonyms": ("synonyms", "MONDO", None),
        }

        # READ PROCESSED DATA TO LOAD
        try:
            # Check if processed dir was set
            if not processed_dir:
                msg = "⚠️  processed_dir MUST be provided."
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR

            processed_path = os.path.join(
                processed_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            processed_file_name = processed_path + "/master_data.parquet"

            if not os.path.exists(processed_file_name):
                msg = f"⚠️  File not found: {processed_file_name}"
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR

            df = pd.read_parquet(processed_file_name, engine="pyarrow")

            if df.empty:
                msg = "DataFrame is empty."
                self.logger.log(msg, "ERROR")
                return False, msg

            df.fillna("", inplace=True)

        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # GET ENTITY GROUP ID AND OMICS STATUS
        try:
            self.get_entity_group("Diseases")
        except Exception as e:
            msg = f"Error on DTP to get Entity Group: {e}"
            return False, msg  # ⧮ Leaving with ERROR

        try:
            statuses = (
                self.session.query(OmicStatus)
                .filter(OmicStatus.name.in_(["active", "deactive"]))
                .all()
            )
            status_map = {s.name: s for s in statuses}
        except Exception as e:
            msg = f"❌ Error on DTP to get OmicStatus: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Validate
        if "active" not in status_map:
            msg = "⚠️ OmicStatus 'active' not found."
            self.logger.log(msg, "ERROR")
            return False, msg
        if "deactive" not in status_map:
            msg = "⚠️ OmicStatus 'deactive' not found."
            self.logger.log(msg, "ERROR")
            return False, msg

        # Usage
        # disease_status_active = status_map["active"]
        # disease_status_deactive = status_map["deactive"]

        # Set DB and drop indexes
        try:
            # self.db_write_mode()
            self.drop_indexes(self.get_disease_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # Clean Data Source
        df["description"] = df["description"].fillna("")
        df["iri"] = df["iri"].fillna("")
        # df["synonyms"] = df["synonyms"].fillna("")

        # --- Build Disease Groups (subsets) ---
        all_subsets = set()
        for subs in df["subsets"].dropna():
            if isinstance(subs, (list, np.ndarray)):
                all_subsets.update(subs)

        subset_map = {}
        for subset in all_subsets:
            group = (
                self.session.query(DiseaseGroup)
                .filter_by(name=subset, data_source_id=self.data_source.id)
                .first()
            )
            if not group:
                group = DiseaseGroup(
                    name=subset,
                    description=f"Subset tag from MONDO: {subset}",
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
                self.session.add(group)
                self.session.flush()
            subset_map[subset] = group.id

        try:
            # Interaction to each Disease Entry
            for _, row in df.iterrows():

                if self.debug_mode:
                    row_time = time.time() - start_total

                # CANONICAL PROTEIN
                # Add or Get Entity for Canonical protein
                disease_master = row["mondo_id"]
                if not disease_master or disease_master == "MONDO:0000001":
                    # skip root/dummy
                    msg = f"Disease Master not found in row: {row}"
                    self.logger.log(msg, "WARNING")
                    continue

                # --- ALIASES STRUCTURE ---
                # Create a dict of Aliases
                alias_dict = self.build_alias(row)
                # Only primary Name
                is_primary_alias = next(
                    (a for a in alias_dict if a.get("is_primary")), None
                )
                # Only Aliases Names
                not_primary_alias = [
                    a for a in alias_dict if a != is_primary_alias
                ]  # noqa E501

                # --- Extend with Xrefs ----
                # TODO: Talvez aqui vou precisar manter o Prefix:codigo?
                xrefs = row.get("xrefs", [])
                # Normalize to Python list
                if isinstance(xrefs, np.ndarray):
                    xrefs = xrefs.tolist()
                elif not isinstance(xrefs, list):
                    xrefs = []
                if isinstance(xrefs, list):
                    for x in xrefs:
                        try:
                            prefix, code = x.split(":", 1)
                        except ValueError:
                            prefix, code = None, x

                        if prefix and code:
                            not_primary_alias.append({
                                "alias_value": code,
                                "alias_type": "code",
                                "xref_source": prefix,
                                "alias_norm": code.lower(),
                                "is_primary": False,
                            })
                
                # Drop Alias Invalids
                # not_primary_alias = [alias for alias in not_primary_alias if alias.get("xref_source") != "ICD9"]

                # --- Determine OmicStatus ---
                omic_status_id = (
                    status_map["deactive"].id if row.get("is_obsolete") else status_map["active"].id
                )
                is_active_entity = False if row.get("is_obsolete") else True

                # --- Create Entity ---
                entity_id, _ = self.get_or_create_entity(
                    name=is_primary_alias["alias_value"],
                    group_id=self.entity_group,
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    alias_type=is_primary_alias["alias_type"],
                    xref_source=is_primary_alias["xref_source"],
                    alias_norm=is_primary_alias["alias_norm"],
                    is_active=is_active_entity,
                )

                # --- Entity Names ---
                self.get_or_create_entity_name(
                    group_id=self.entity_group,
                    entity_id=entity_id,
                    aliases=not_primary_alias,
                    is_active=is_active_entity,
                    data_source_id=self.data_source.id,  # noqa E501
                    package_id=self.package.id,
                )

                # --- Disease Master ---
                disease_master_obj = (
                    self.session.query(DiseaseMaster)
                    .filter_by(disease_id=disease_master, data_source_id=self.data_source.id)
                    .first()
                )

                if not disease_master_obj:
                    disease_master_obj = DiseaseMaster(
                        disease_id=disease_master,
                        label=row.get("label"),
                        description=row.get("description"),
                        omic_status_id=omic_status_id,
                        entity_id=entity_id,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    )
                    self.session.add(disease_master_obj)
                    self.session.flush()

                total_diseases += 1

                # --- Disease Subset Links ---
                subsets = row.get("subsets", [])
                if isinstance(subsets, np.ndarray):
                    subsets = subsets.tolist()
                elif not isinstance(subsets, list):
                    subsets = []

                for subset in subsets:
                    group_id = subset_map.get(subset)
                    if group_id:
                        link = (
                            self.session.query(DiseaseGroupMembership)
                            .filter_by(disease_id=disease_master_obj.id, group_id=group_id)
                            .first()
                        )
                        if not link:
                            self.session.add(
                                DiseaseGroupMembership(
                                    disease_id=disease_master_obj.id,
                                    group_id=group_id,
                                    data_source_id=self.data_source.id,
                                    etl_package_id=self.package.id,
                                )
                            )

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(self.get_protein_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"📥 Total Pathways: {total_diseases}"  # noqa E501  # noqa E501
        self.logger.log(msg, "INFO")

        return True, msg

import os
import time  # DEBUG MODE
import zipfile
from itertools import product
from pathlib import Path

import pandas as pd
import requests

from biofilter.modules.db.models import (  # noqa E501
    EntityAlias,
    EntityGroup,
    EntityRelationship,
    EntityRelationshipType,
)
from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.utils.file_hash import compute_file_hash


class DTP(DTPBase):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        db=None,
    ):  # noqa: E501
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db

        # DTP versioning
        self.dtp_name = "dtp_biogrid"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "4.0.0"

    # -------------------------------------------------------------------------
    #                            EXTRACT METHOD
    # -------------------------------------------------------------------------
    def extract(self, raw_dir: str):
        """
        Download data from the BioGRID and stores it locally.
        Also computes a file hash to track content versioning.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(
            msg,
            "INFO",  # noqa: E501
        )  # noqa: E501

        try:
            # Check Compartibility
            self.check_compatibility()

            source_url = self.data_source.source_url

            # Landing directory
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            # NOTE: We are getting from Current Version folder,
            #       but the file name fix the version
            file_path = os.path.join(
                landing_path, "BIOGRID-ALL-LATEST.mitab.zip"
            )  # noqa E501

            # Download file
            msg = f"⬇️  Downloading file from: {source_url} ..."
            self.logger.log(msg, "INFO")

            response = requests.get(source_url, stream=True)
            if response.status_code != 200:
                msg = f"❌ Failed to fetch data from BioGRID: {response.status_code}"  # noqa E501
                self.logger.log(msg, "ERROR")
                return False, msg, None

            # Write file in binary mode
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Compute hash
            current_hash = compute_file_hash(file_path)

            msg = f"✅ File downloaded to {file_path}"
            self.logger.log(msg, "INFO")

            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # -------------------------------------------------------------------------
    #                            TRANSFORM METHOD
    # -------------------------------------------------------------------------
    def transform(self, raw_dir: str, processed_dir: str):

        msg = f"🔧 Transforming the {self.data_source.name} data ..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        if self.debug_mode:
            start_total = time.time()

        # Check if raw_dir and processed_dir are provided
        try:
            # Define input/output base paths
            input_path = (
                Path(raw_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501
            output_path = (
                Path(processed_dir)
                / self.data_source.source_system.name
                / self.data_source.name
            )  # noqa E501

            # Ensure output directory exists
            output_path.mkdir(parents=True, exist_ok=True)

            # Input file path
            input_file = input_path / "BIOGRID-ALL-LATEST.mitab.zip"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                # self.logger.log(msg, "ERROR")
                return False, msg

            # Output files paths
            output_file_master = output_path / "relationship_data"

            # Delete existing files if they exist (both .csv and .parquet)
            for f in [output_file_master]:
                for ext in [".csv", ".parquet"]:
                    target_file = f.with_suffix(ext)
                    if target_file.exists():
                        target_file.unlink()
                        # if self.debug_mode:
                        self.logger.log(
                            f"🗑️  Removed existing file: {target_file}", "DEBUG"
                        )  # noqa E501

        except Exception as e:
            msg = f"❌ Error constructing paths: {str(e)}"
            # self.logger.log(msg, "ERROR")
            return False, msg

        try:
            # if self.debug_mode:
            msg = "Reading BioGRID MITAB file..."
            self.logger.log(msg, "DEBUG")

            required_columns = [
                "Alt IDs Interactor A",
                "Alt IDs Interactor B",
                "Interaction Identifiers",
                "Interaction Detection Method",
                "Interaction Types",
                "Taxid Interactor A",
                "Taxid Interactor B",
            ]
            idx_alt_a = 0
            idx_alt_b = 1
            idx_interaction_id = 2
            idx_interaction_method = 3
            idx_interaction_type = 4

            # Internal Functions to transform row
            def _extract_ids(field, prefix):
                if pd.isna(field):
                    return []
                parts = str(field).split("|")
                return [
                    p.split(":")[-1].strip()
                    for p in parts
                    if p.lower().startswith(prefix.lower())
                ]

            def _parse_biogrid_line(row):
                relations = []

                # Core identifiers
                alt_ids_a = row[idx_alt_a]
                alt_ids_b = row[idx_alt_b]
                gene_a = _extract_ids(alt_ids_a, "entrez gene/locuslink")
                gene_b = _extract_ids(alt_ids_b, "entrez gene/locuslink")
                prot_a = _extract_ids(alt_ids_a, "uniprot")
                prot_b = _extract_ids(alt_ids_b, "uniprot")
                chem_a = _extract_ids(alt_ids_a, "chebi") + _extract_ids(
                    alt_ids_a, "pubchem"
                )
                chem_b = _extract_ids(alt_ids_b, "chebi") + _extract_ids(
                    alt_ids_b, "pubchem"
                )

                # Metadata
                interaction_id = str(row[idx_interaction_id]).split("|")[0]
                interaction_method = (
                    str(row[idx_interaction_method])
                    .split("(")[-1]
                    .replace(")", "")
                )
                interaction_type = (
                    str(row[idx_interaction_type])
                    .split("(")[-1]
                    .replace(")", "")
                )

                # (1) Gene–Gene
                for g1, g2 in product(gene_a, gene_b):
                    relations.append(
                        (
                            "Genes",
                            "ENTREZ",
                            g1,
                            "Genes",
                            "ENTREZ",
                            g2,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )
                # (2) Gene–Protein / Protein–Gene
                for g, p in product(gene_a, prot_b):
                    relations.append(
                        (
                            "Genes",
                            "ENTREZ",
                            g,
                            "Proteins",
                            "UNIPROT",
                            p,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )
                for p, g in product(prot_a, gene_b):
                    relations.append(
                        (
                            "Proteins",
                            "UNIPROT",
                            p,
                            "Genes",
                            "ENTREZ",
                            g,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )
                # (3) Protein–Protein
                for p1, p2 in product(prot_a, prot_b):
                    relations.append(
                        (
                            "Proteins",
                            "UNIPROT",
                            p1,
                            "Proteins",
                            "UNIPROT",
                            p2,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )
                # (4) Protein–Chemical / Chemical–Protein
                for p, c in product(prot_a, chem_b):
                    relations.append(
                        (
                            "Proteins",
                            "UNIPROT",
                            p,
                            "Chemicals",
                            "CHEBI",
                            c,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )
                for c, p in product(chem_a, prot_b):
                    relations.append(
                        (
                            "Chemicals",
                            "CHEBI",
                            c,
                            "Proteins",
                            "UNIPROT",
                            p,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )
                # (5) Genes–Chemical / Chemical–Gene
                for g, c in product(gene_a, chem_b):
                    relations.append(
                        (
                            "Genes",
                            "ENTREZ",
                            g,
                            "Chemicals",
                            "CHEBI",
                            c,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )
                for c, g in product(chem_a, gene_b):
                    relations.append(
                        (
                            "Chemicals",
                            "CHEBI",
                            c,
                            "Genes",
                            "ENTREZ",
                            g,
                            interaction_id,
                            interaction_method,
                            interaction_type,
                        )
                    )

                return relations

            # Open the ZIP and locate the MITAB file inside
            total_rows = 0
            total_human_interactions = 0
            expanded = []
            with zipfile.ZipFile(input_file, "r") as z:
                inner_files = [f for f in z.namelist() if f.endswith(".mitab.txt")]
                if not inner_files:
                    msg = "❌ No MITAB file found inside the ZIP archive."
                    return False, msg

                mitab_name = inner_files[0]
                with z.open(mitab_name) as f:
                    chunks = pd.read_csv(
                        f,
                        sep="\t",
                        low_memory=False,
                        usecols=required_columns,
                        chunksize=100_000,
                    )
                    for chunk in chunks:
                        total_rows += len(chunk)
                        chunk = chunk[
                            (chunk["Taxid Interactor A"] == "taxid:9606")
                            & (chunk["Taxid Interactor B"] == "taxid:9606")
                        ]
                        total_human_interactions += len(chunk)
                        # Keep tuple position aligned with required_columns order.
                        for row in chunk[required_columns].itertuples(
                            index=False, name=None
                        ):
                            expanded.extend(_parse_biogrid_line(row))

            msg = f"✅ Loaded MITAB file with {total_rows} rows"
            self.logger.log(msg, "DEBUG")
            msg = (
                "🧬 Filtered for Homo sapiens (taxid:9606) → "
                f"{total_human_interactions} interactions"
            )
            self.logger.log(msg, "DEBUG")

            # Convert to DataFrame
            df_expanded = pd.DataFrame(
                expanded,
                columns=[
                    "group_a",
                    "source_a",
                    "value_a",
                    "group_b",
                    "source_b",
                    "value_b",
                    "interaction_id",
                    "interaction_method",
                    "interaction_type",
                ],
            )

            # Drop duplicates and export
            df_expanded = df_expanded.drop_duplicates()
            df_expanded.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )

            if self.debug_mode:
                df_expanded.to_csv(output_file_master.with_suffix(".csv"), index=False)
                end_time = time.time() - start_total
                msg = str(
                    f"processed {len(df_expanded)} records / Time Total: {end_time:.2f}s |"  # noqa E501
                )  # noqa E501
                self.logger.log(msg, "DEBUG")

            msg = (
                "Transform completed successfully with "
                f"{total_human_interactions} interactions."
            )
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transform phase: {str(e)}"
            # self.logger.log(msg, "ERROR")
            return False, msg

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------
    def load(self, processed_dir=None):
        """
        Loads BioGRID relationships into the database.
        Matches Entities by alias and creates EntityRelationship rows.
        """

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",  # noqa E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_relationships = 0
        total_warnings = 0

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
            processed_file_name = processed_path + "/relationship_data.parquet"

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
            self.logger.log(msg, "DEBUG")
            return False, msg  # ⧮ Leaving with ERROR

        # ----= MAPPING FIELDS =----
        # --------------------------

        # 1. Map EntityGroup IDs
        try:
            group_map = {
                "Genes": self.session.query(EntityGroup)
                .filter_by(name="Genes")
                .first()
                .id,
                "Proteins": self.session.query(EntityGroup)
                .filter_by(name="Proteins")
                .first()
                .id,
                "Chemicals": self.session.query(EntityGroup)
                .filter_by(name="Chemicals")
                .first()
                .id,
            }
            df["entity_1_group_id"] = df["group_a"].map(group_map)
            df["entity_2_group_id"] = df["group_b"].map(group_map)
        except Exception as e:
            msg = f"⚠️  Failed to map Entity Group data: {e}"
            self.logger.log(msg, "DEBUG")
            return False, msg  # ⧮ Leaving with ERROR

        # 2. Map Entity IDs
        # 2.1 Genes Maps
        try:
            genes = (
                df.loc[df["group_a"].eq("Genes"), "value_a"].unique().tolist()
                + df.loc[df["group_b"].eq("Genes"), "value_b"].unique().tolist()
            )

            gene_aliases = (
                self.session.query(EntityAlias.alias_value, EntityAlias.entity_id)
                .filter(EntityAlias.group_id == group_map["Genes"])
                .filter(EntityAlias.alias_type == "symbol")
                .filter(EntityAlias.is_primary.is_(True))
                .filter(EntityAlias.alias_value.in_(genes))
                .all()
            )
            df_gene_map = pd.DataFrame(
                gene_aliases, columns=["alias_value", "entity_id"]
            )
            df_gene_map["group_name"] = "Genes"
            df_gene_map["source_name"] = "ENTREZ"
        except Exception as e:
            msg = f"⚠️  Failed to map Genes Entity data: {e}"
            self.logger.log(msg, "DEBUG")
            return False, msg  # ⧮ Leaving with ERROR

        # 2.2 Proteins Maps
        try:
            proteins = (
                df.loc[df["group_a"].eq("Proteins"), "value_a"].unique().tolist()
                + df.loc[df["group_b"].eq("Proteins"), "value_b"].unique().tolist()
            )

            protein_aliases = (
                self.session.query(EntityAlias.alias_value, EntityAlias.entity_id)
                .filter(EntityAlias.group_id == group_map["Proteins"])
                .filter(EntityAlias.alias_type != "name")  # evita nomes descritivos
                .filter(EntityAlias.alias_value.in_(proteins))
                .all()
            )
            df_protein_map = pd.DataFrame(
                protein_aliases, columns=["alias_value", "entity_id"]
            )
            df_protein_map["group_name"] = "Proteins"
            df_protein_map["source_name"] = "UNIPROT"
        except Exception as e:
            msg = f"⚠️  Failed to map Proteins Entity data: {e}"
            self.logger.log(msg, "DEBUG")
            return False, msg  # ⧮ Leaving with ERROR

        # 2.3 Chemicals Maps
        try:
            chems = (
                df.loc[df["group_a"].eq("Chemicals"), "value_a"].unique().tolist()
                + df.loc[df["group_b"].eq("Chemicals"), "value_b"].unique().tolist()
            )
            chem_aliases = (
                self.session.query(EntityAlias.alias_value, EntityAlias.entity_id)
                .filter(EntityAlias.group_id == group_map["Chemicals"])
                .filter(EntityAlias.alias_type != "formula")
                .filter(EntityAlias.alias_value.in_(chems))
                .all()
            )
            df_chem_map = pd.DataFrame(
                chem_aliases, columns=["alias_value", "entity_id"]
            )
            df_chem_map["group_name"] = "Chemicals"
            df_chem_map["source_name"] = "CHEBI"
        except Exception as e:
            msg = f"⚠️  Failed to map Chemicals Entity data: {e}"
            self.logger.log(msg, "DEBUG")
            return False, msg  # ⧮ Leaving with ERROR

        # 2.4 Merge all Entities IDs
        map_dict = {}
        for d in [df_gene_map, df_protein_map, df_chem_map]:
            for row in d.itertuples(index=False):
                map_dict[(row.group_name, row.source_name, row.alias_value)] = (
                    row.entity_id
                )
        df["key_a"] = list(zip(df["group_a"], df["source_a"], df["value_a"]))
        df["key_b"] = list(zip(df["group_b"], df["source_b"], df["value_b"]))
        df["entity_1_id"] = df["key_a"].map(map_dict)
        df["entity_2_id"] = df["key_b"].map(map_dict)
        df.drop(columns=["key_a", "key_b"], inplace=True)

        # 3. Map Relationship Type ID
        # TODO: Improve Relation types
        rel_type = (
            self.session.query(EntityRelationshipType)
            .filter_by(code="interacts_with")
            .first()
        )
        if rel_type is None:
            msg = "⚠️  Relationship type 'interacts_with' not found."
            self.logger.log(msg, "ERROR")
            return False, msg

        # ----= CLEAN DATA =----
        # Slitting in two dfs (to load and with missing ID to check)
        try:

            df_resolved = df.dropna(subset=["entity_1_id", "entity_2_id"]).copy()
            df_missing = df[df["entity_1_id"].isna() | df["entity_2_id"].isna()].copy()
            # Covert to INT after filter pd.NAN
            df_resolved["entity_1_id"] = df_resolved["entity_1_id"].astype(int)
            df_resolved["entity_2_id"] = df_resolved["entity_2_id"].astype(int)

            # Save records without Master Data / Entity
            missing_file = processed_path + "/biogrid_missing_aliases.csv"
            df_missing.to_csv(missing_file, index=False)
            self.logger.log(f"Saved unresolved aliases to {missing_file}", "WARNING")
        except Exception as e:
            msg = f"⚠️  Failed to keep only valid ID to load: {e}"
            self.logger.log(msg, "DEBUG")
            return False, msg  # ⧮ Leaving with ERROR

        # ----= CHECK PREVIOUS DATA IN DB =----
        # --------------------------------------

        def normalize_pair(a, b):
            """Returns (min, max) tuple to make the relationship symmetric."""
            return (a, b) if a <= b else (b, a)

        try:
            msg = "🔍 Checking existing BioGRID relationships..."
            self.logger.log(msg, "DEBUG")

            # NOTE: We are consider all same Interaction Type
            existing = (
                self.session.query(
                    EntityRelationship.entity_1_id,
                    EntityRelationship.entity_2_id,
                    # EntityRelationship.relationship_type_id,
                )
                .filter(EntityRelationship.data_source_id == self.data_source.id)
                .all()
            )
            existing_set = {
                normalize_pair(r.entity_1_id, r.entity_2_id) for r in existing
            }
            msg = f"📊 Found {len(existing_set):,} existing relationships"
            self.logger.log(msg, "DEBUG")

            # Create key to match
            df_resolved["rel_key"] = [
                normalize_pair(a, b)
                for a, b in zip(df_resolved["entity_1_id"], df_resolved["entity_2_id"])
            ]
            # Drop all records previous loaded
            df_new = df_resolved[~df_resolved["rel_key"].isin(existing_set)].copy()
            self.logger.log(f"🧮 {len(df_new):,} new relationships to insert", "INFO")

            if df_new.empty:
                self.logger.log("No new relationships to insert — skipping.", "INFO")
                return True, "No new relationships"
        except Exception as e:
            msg = f"Failed to keep only new interactions: {e}"
            self.logger.log(msg, "DEBUG")
            return False, msg  # ⧮ Leaving with ERROR

        # ----= INSERT DATA IN DB =----
        # --------------------------------------

        # Drop Indexes
        try:
            self.drop_indexes(self.get_entity_relationship_index_specs)
        except Exception as e:
            self.session.rollback()
            msg = f"❌ Error when delete old relationships: {e}"
            return False, msg

        # Create Bulk
        chunk_size = 10_000  # 🔧 ajuste conforme o desempenho do servidor
        total = len(df_new)
        insert_error = None

        # Build relationships in chunks
        rels = []
        for i, row in enumerate(df_new.itertuples(index=False), start=1):
            rels.append(
                EntityRelationship(
                    entity_1_id=int(row.entity_1_id),
                    entity_1_group_id=int(row.entity_1_group_id),
                    entity_2_id=int(row.entity_2_id),
                    entity_2_group_id=int(row.entity_2_group_id),
                    relationship_type_id=rel_type.id if rel_type else None,
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )
            )

            # 🚀 Quando atingir o chunk_size ou o fim
            if i % chunk_size == 0 or i == total:
                chunk_number = ((i - 1) // chunk_size) + 1
                try:
                    self.session.bulk_save_objects(rels, return_defaults=False)
                    self.session.commit()
                    total_relationships += len(rels)
                    self.logger.log(
                        f"💾 Inserted chunk {chunk_number} ({len(rels):,} records)",
                        "DEBUG",
                    )

                except Exception as e:
                    self.session.rollback()
                    insert_error = (
                        "⚠️ Error inserting chunk "
                        f"{chunk_number} ending at record {i:,}: {str(e)}"
                    )
                    self.logger.log(insert_error, "ERROR")
                    rels.clear()
                    break
                rels.clear()  # libera memória

        # Create Index
        try:
            self.create_indexes(self.get_entity_relationship_index_specs)
        except Exception as e:
            idx_msg = f"⚠️ Failed to restore DB indexes: {e}"
            self.logger.log(idx_msg, "WARNING")
            if not insert_error:
                insert_error = idx_msg

        if insert_error:
            return False, insert_error

        msg = f"📥 Total BioGRID Relationships: {total_relationships}"
        return True, msg

import os
import re
import requests
import pandas as pd
from pathlib import Path
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.db.models import GOMaster, GORelation


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

        # DTP versioning
        self.dtp_name = "dtp_go"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Download GO data in OBO format and store it locally.
        Also computes a file hash to track content versioning.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."

        self.logger.log(
            msg,
            "INFO",
        )

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url

        try:
            # Prepare download path
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            file_path = os.path.join(landing_path, "geneontology.obo")

            # Download the OBO file
            msg = f"⬇️  Fetching OBO from URL: {source_url} ..."
            self.logger.log(msg, "INFO")

            headers = {
                "Accept": "application/x-obo"
            }  # Optional, GO responds with OBO anyway
            response = requests.get(source_url, headers=headers)

            if response.status_code != 200:
                msg = f"Failed to fetch data from GO: {response.status_code}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(response.text)

            # Compute hash
            current_hash = compute_file_hash(file_path)

            msg = f"✅ GO file downloaded to {file_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):
        """
        Transform the downloaded GO OBO file into structured CSV and Parquet.
        Extracts GO terms and their relationships.
        """

        msg = f"⚙️  Starting transformation of {self.data_source.name} data..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

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
            input_file = input_path / "geneontology.obo"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg

            # Output files paths
            output_file_master = output_path / "master_data"
            output_file_relations = output_path / "relations_data"

            # Delete existing files if they exist (both .csv and .parquet)
            for f in [output_file_master, output_file_relations]:
                for ext in [".csv", ".parquet"]:
                    target_file = f.with_suffix(ext)
                    if target_file.exists():
                        target_file.unlink()
                        self.logger.log(
                            f"🗑️  Removed existing file: {target_file}", "INFO"
                        )  # noqa E501

        except Exception as e:
            msg = f"❌ Error constructing paths: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Process source file to Biofilter format
        try:
            terms = []
            relations = []
            current = {}

            with open(input_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()

                    if line == "[Term]":
                        # New term starts, save the previous one if exists
                        if current.get("go_id"):
                            terms.append(current)
                        current = {}  # Reset for the new term

                    elif line == "[Typedef]":
                        break

                    elif line.startswith("id: "):
                        current["go_id"] = line[4:]

                    elif line.startswith("name: "):
                        current["name"] = line[6:]

                    elif line.startswith("namespace: "):
                        current["namespace"] = line[10:]

                    elif line.startswith("def: "):
                        current["definition"] = line[5:].split('" [')[0]

                    elif line.startswith("is_obsolete: "):
                        current["is_obsolete"] = line[13:] == "true"

                    elif line.startswith("replaced_by: "):
                        current["replaced_by"] = line[13:]

                    elif line.startswith("consider: "):
                        current.setdefault("consider", []).append(line[10:])

                    elif line.startswith("alt_id: "):
                        current.setdefault("alt_ids", []).append(line[8:])

                    elif line.startswith("synonym: "):
                        # Extract name between quotes (")
                        match = re.search(r'"(.+?)"', line)
                        if match:
                            current.setdefault("synonyms", []).append(
                                match.group(1)
                            )  # noqa: E501

                    elif line.startswith("xref: "):
                        xref_raw = line[6:]
                        # If it has a label, extract the name between quotes as well # noqa: E501
                        xref_parts = xref_raw.split(' "')
                        xref_id = xref_parts[0].strip()
                        current.setdefault("xrefs", []).append(xref_id)

                    # Hierarchical relations
                    elif line.startswith("is_a: "):
                        child_id = line[6:].split(" !")[0]
                        if current.get("go_id"):
                            relations.append(
                                {
                                    "parent_id": current["go_id"],
                                    "child_id": child_id,
                                    "relation_type": "is_a",
                                }
                            )

            # Save the last term if it exists
            if current.get("go_id"):
                terms.append(current)
            df_terms = pd.DataFrame(terms)

            df_terms["is_obsolete"] = (
                df_terms["is_obsolete"]
                .where(pd.notna(df_terms["is_obsolete"]), False)
                .astype(bool)
            )

            # Compond Fields as Lists
            for col in ["alt_ids", "consider", "synonyms", "xrefs"]:
                df_terms[col] = df_terms[col].apply(
                    lambda x: x if isinstance(x, list) else (None if pd.isna(x) else [x])  # noqa E501
                )

            df_rel = pd.DataFrame(relations)

            # SAVE FILES
            df_terms.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )  # noqa: E501
            df_rel.to_parquet(
                output_file_relations.with_suffix(".parquet"), index=False
            )  # noqa: E501

            if self.debug_mode:
                df_terms.to_csv(
                    output_file_master.with_suffix(".csv"), index=False
                )  # noqa: E501
                df_rel.to_csv(
                    output_file_relations.with_suffix(".csv"), index=False
                )  # noqa: E501

            self.logger.log("✅ GO terms and relations transformed.", "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):

        msg = "📥 Loading Gene Ontology data into the database..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        total_terms = 0
        total_warnings = 0
        skipped = 0

        # ALIASES MAP FROM PROCESS DATA FIELDS
        self.alias_schema = {
            "go_id": ("code", "GO", True),
            # "name": ("name", "GO", None),
            # "synonyms": ("synonym", "GO", None),
            "alt_ids": ("code", "GO", None),
            # "replaced_by": ("xref", "GO", None),
            # "consider": ("xref", "GO", None),
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

            # Setting files names
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

        # SET DB AND DROP INDEXES
        try:
            self.db_write_mode()
            self.drop_indexes(self.get_go_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # GET ENTITY GROUP ID AND OMICS STATUS
        try:
            self.get_entity_group("Gene Ontology")
        except Exception as e:
            msg = f"Error on DTP to get Entity Group: {e}"
            return False, msg  # ⧮ Leaving with ERROR

        # RUN LOAD BY ROW
        try:
            for _, row in df.iterrows():
                go_id = row.get("go_id")
                name = row.get("name")
                is_obsolete = str(row.get("is_obsolete", "")).strip().lower() == "true"  # noqa E501

                if go_id == "GO:0000050":
                    pass

                # Skip entries with missing required fields
                if not go_id or not name:
                    skipped += 1
                    continue

                # Skip obsolete terms entirely (optional — configurable)
                if is_obsolete:
                    skipped += 1
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

                # Add or Get Entity
                entity_id, _ = self.get_or_create_entity(
                    name=is_primary_alias["alias_value"],
                    group_id=self.entity_group,
                    data_source_id=self.data_source.id,
                    package_id=self.package.id,
                    alias_type=is_primary_alias["alias_type"],
                    xref_source=is_primary_alias["xref_source"],
                    alias_norm=is_primary_alias["alias_norm"],
                    is_active=True,
                )

                self.get_or_create_entity_name(
                    group_id=self.entity_group,
                    entity_id=entity_id,
                    aliases=not_primary_alias,
                    is_active=True,
                    data_source_id=self.data_source.id,  # noqa E501
                    package_id=self.package.id,
                )
                # # Additional names from synonyms, alt_ids, etc.
                # names = set()

                # # Alt IDs
                # if row.get("alt_ids"):
                #     for alt in row["alt_ids"].split(";"):
                #         alt = alt.strip()
                #         if alt:
                #             names.add(alt)

                # # Save all names for the entity
                # for name in names:
                #     self.get_or_create_entity_name(
                #         entity_id, name, data_source_id=self.data_source.id
                #     )

                # Add GO term to GOMaster if it doesn't exist
                go_master = (
                    self.session.query(GOMaster).filter_by(go_id=go_id).first()
                )  # noqa: E501
                if not go_master:
                    go_master = GOMaster(
                        go_id=go_id,
                        entity_id=entity_id,
                        name=row.get("name", "").strip(),
                        namespace=row.get("namespace", "").strip(),
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    )
                    self.session.add(go_master)
                    self.session.flush()

                total_terms += 1

            msg = f"📥 Total GO terms loaded: {total_terms}"
            self.logger.log(msg, "INFO")
            # return total_terms, True, msg

        except Exception as e:
            msg = f"❌ ETL load failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return 0, False, msg

        # 🔁 Load GO relations (is_a / part_of / regulates)
        try:
            rel_path = processed_path / "relations_data.parquet"
            df_rel = pd.read_parquet(rel_path, engine="pyarrow").fillna("")

            total_relations = 0
            for _, row in df_rel.iterrows():
                child_go = row["child_id"].strip()
                parent_go = row["parent_id"].strip()
                rel_type = row.get("relation_type", "is_a").strip()

                # Search GOMaster for both sides
                child = (
                    self.session.query(GOMaster)
                    .filter_by(go_id=child_go)
                    .first()  # noqa: E501
                )
                parent = (
                    self.session.query(GOMaster)
                    .filter_by(go_id=parent_go)
                    .first()  # noqa: E501
                )

                if not child or not parent:
                    msg = (
                        f"⚠️ Skipping relation {child_go} -> {parent_go}: "
                        "GOMaster not found."
                    )
                    self.logger.log(
                        msg,
                        "WARNING",
                    )
                    continue

                # Duplicates check
                existing = (
                    self.session.query(GORelation)
                    .filter_by(
                        child_id=child.id,
                        parent_id=parent.id,
                        relation_type=rel_type,
                    )
                    .first()
                )
                if existing:
                    continue

                # Create new relation
                relation = GORelation(
                    parent_id=parent.id,
                    child_id=child.id,
                    relation_type=rel_type,  # noqa E501
                    data_source_id=self.data_source.id,
                    etl_package_id=self.package.id,
                )  # noqa: E501
                self.session.add(relation)
                total_relations += 1

        except FileNotFoundError as e:
            msg = f"⚠️ Relations file not found: {str(e)}", "WARNING"
            self.logger.log(msg, "ERROR")
            return False, msg

        except Exception as e:
            msg = f"❌ ETL load failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(self.get_go_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"🔗 Total GO relations loaded: {total_relations}"
        self.logger.log(msg, "INFO")

        return True, msg

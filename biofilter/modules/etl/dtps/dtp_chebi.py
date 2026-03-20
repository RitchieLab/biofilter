import os
import time  # DEBUG MODE
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from biofilter.modules.db.models import (  # ChemicalData,; noqa E501
    ChemicalMaster,
    OmicStatus,
)
from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.modules.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.utils.file_hash import compute_file_hash


class DTP(DTPBase, EntityQueryMixin, GeneQueryMixin):
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
        self.dtp_name = "dtp_chebi"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "4.0.0"

    # -------------------------------------------------------------------------
    #                            EXTRACT METHOD
    # -------------------------------------------------------------------------
    def extract(self, raw_dir: str):
        """
        Download flat_files from ChEBI FTP
        (compounds.tsv.gz, chemical_data.tsv.gz).
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(msg, "INFO")

        try:
            # Check compatibility
            self.check_compatibility()

            # source_url = self.data_source.source_url
            # Donwload more files to extract data
            base_url = "https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/"
            files = [
                "compounds.tsv.gz",
                "chemical_data.tsv.gz",
                "secondary_ids.tsv.gz",
                "database_accession.tsv.gz",
                "source.tsv.gz",
            ]  # noqa E501

            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)

            downloaded_files = []
            for f in files:
                url = base_url + f
                out_file = landing_path + "/" + f
                try:
                    self.logger.log(f"⬇️ Downloading {url} ...", "INFO")
                    r = requests.get(url, stream=True)
                    r.raise_for_status()
                    with open(out_file, "wb") as handle:
                        for chunk in r.iter_content(chunk_size=8192):
                            handle.write(chunk)
                    downloaded_files.append(str(out_file))
                except Exception as e:
                    msg = f"❌ Failed to download {f}: {e}"
                    self.logger.log(msg, "ERROR")
                    return False, msg, None

            # Compute file hash to Compound File
            file_path = landing_path + "/compounds.tsv.gz"
            current_hash = compute_file_hash(file_path)

            msg = f"✅ CheBI files downloaded to {landing_path}"
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
        """
        Parse compounds.tsv.gz + chemical_data.tsv.gz into master and data
        parquet files.
        """

        # NOTE: Nao vamos processar nesse momento os arquivos relations, pois
        # sao relacionamentos entre Chemicals e nao sera o nosso foco nesse
        # momento

        msg = f"⚙️  Starting transform of {self.data_source.name}..."
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

            # --- Compounds ---
            input_file = input_path / "compounds.tsv.gz"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            compounds = pd.read_csv(input_file, sep="\t", compression="gzip")
            compounds = compounds.rename(
                columns={"chebi_accession": "chebi_id", "name": "label"}
            )

            # --- Chemical Data ---
            input_file = input_path / "chemical_data.tsv.gz"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            chemical_data = pd.read_csv(input_file, sep="\t", compression="gzip")  # noqa E501

            # Join on compound_id vs CHEBI:XXX
            compounds["compound_id"] = (
                compounds["chebi_id"].str.replace("CHEBI:", "").astype(int)
            )
            merged = pd.merge(compounds, chemical_data, how="left", on="compound_id")  # noqa E501

            status_map = {1: "active", 3: "active", 9: "active", 4: "deactive"}

            merged["omic_status"] = (
                merged["status_id_x"].map(status_map).fillna("deactive")
            )

            # --- Secondary IDs Data -
            # NOTE: Temos o arquivo Names.tsv que podemos expandir para
            # Alias se necessario.
            input_file = input_path / "secondary_ids.tsv.gz"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            df_secondary = pd.read_csv(input_file, sep="\t", compression="gzip")  # noqa E501
            # Padronizar para CHEBI:xxxx
            df_secondary["secondary_id"] = (
                df_secondary["secondary_id"].astype(str).apply(lambda x: f"CHEBI:{x}")  # noqa E501
            )
            # Agrupar por compound_id
            df_secondary_grouped = (
                df_secondary.groupby("compound_id")["secondary_id"]
                .apply(list)
                .reset_index()
            )
            # Merge com o merged principal
            merged = merged.merge(df_secondary_grouped, how="left", on="compound_id")  # noqa E501

            # Renomear coluna
            merged.rename(columns={"secondary_id": "secondary_ids"}, inplace=True)  # noqa E501
            merged.rename(columns={"status_id_x": "status_id"}, inplace=True)

            # --- Xrefs IDs Data -
            # NOTE: Iremos ler outros codigos
            input_file = input_path / "database_accession.tsv.gz"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            df_xref = pd.read_csv(input_file, sep="\t", compression="gzip")
            # Padronizar para CHEBI:xxxx
            df_xref["chem_id"] = (
                df_xref["compound_id"].astype(str).apply(lambda x: f"CHEBI:{x}")  # noqa E501
            )
            """
            id	    compound_id	    accession_number	type	        status_id	source_id  # noqa E501
            9	    3	            C06147	            MANUAL_X_REF	3	        45  # noqa E501
            97743	7	            663435	            REGISTRY_NUMBER	1	        33  # noqa E501
            97747	7	            4229885	            REGISTRY_NUMBER	1	        10  # noqa E501
            """

            # ler os Sources (no database_accession esta com o ID)
            input_file = input_path / "source.tsv.gz"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg
            df_source = pd.read_csv(input_file, sep="\t", compression="gzip")
            """
            id	name	                url	                                    prefix	description  # noqa E501
            1	Agricola	            https://europepmc.org/abstract/AGR/*	agr	  # noqa E501
            2	Alan Wood's Pesticides	https://bioregistry.io/pesticides:*	    pesticides	  # noqa E501
            """
            # --- Merge df_xref com df_source para pegar os nomes dos bancos ---  # noqa E501
            df_xref = df_xref.merge(
                df_source[["id", "name", "prefix"]],
                left_on="source_id",
                right_on="id",
                how="left",
            )

            # Criar colunas auxiliares
            df_xref["alias_value"] = df_xref["accession_number"].astype(str)
            df_xref["alias_type"] = "code"
            df_xref["xref_source"] = df_xref["name"].fillna("Unknown")

            # Montar dicionários
            df_xref["alias_dict"] = df_xref.apply(
                lambda row: {
                    "alias_value": row["alias_value"],
                    "alias_type": row["alias_type"],
                    "xref_source": row["xref_source"],
                    "alias_norm": row["alias_value"].lower(),
                    "is_primary": False,
                },
                axis=1,
            )

            # Agrupar por CHEBI:xxx → lista de dicionários
            df_xref_grouped = (
                df_xref.groupby("chem_id")["alias_dict"]
                .apply(list)
                .reset_index()
                .rename(columns={"alias_dict": "aliases_extra"})
            )

            # Merge no df principal
            merged = merged.merge(
                df_xref_grouped, how="left", left_on="chebi_id", right_on="chem_id"  # noqa E501
            )

            # Se não houver aliases_extra, garantir lista vazia
            merged["aliases_extra"] = merged["aliases_extra"].apply(
                lambda x: x if isinstance(x, list) else []
            )

            # Select only useful cols
            merged = merged[
                [
                    "chebi_id",
                    "omic_status",
                    "label",
                    "definition",
                    "source",
                    "ascii_name",
                    "status_id",
                    "formula",
                    "charge",
                    "mass",
                    "monoisotopic_mass",
                    "structure_id",
                    "is_autogenerated",
                    "secondary_ids",
                    "aliases_extra",
                ]
            ]

            # Save one master file
            merged.to_parquet(output_path / "master_data.parquet", index=False)

            if self.debug_mode:
                merged.to_csv(output_path / "master_data.csv", index=False)
                end_time = time.time() - start_total
                msg = str(
                    f"processed {len(merged)} records / Time Total: {end_time:.2f}s |"  # noqa E501
                )  # noqa E501
                self.logger.log(msg, "DEBUG")

            msg = f"✅ CheBI transformed into Compounds + Data at {output_path}"  # noqa E501
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

    # -------------------------------------------------------------------------
    #                            LOAD METHOD
    # -------------------------------------------------------------------------
    def load(self, processed_dir=None):
        """
        Load transformed CheBI compounds into Biofilter3R schema.
        """

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",
        )

        # CHECK COMPARTIBILITY
        self.check_compatibility()

        # VARIABLES TO LOAD PROCESS
        # if self.debug_mode:
        #     start_total = time.time()

        # Setting variables
        total_chemicals = 0
        total_warnings = 0

        # ALIASES MAP FROM PROCESS DATA FIELDS
        self.alias_schema = {
            "chebi_id": ("code", "CheBI", True),
            "ascii_name": ("label", "CheBI", None),
            "secondary_ids": ("code", "CheBI", None),
            "formula": ("formula", "CheBI", None),
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

            # Avoid filling numeric columns with string values (""),
            # which triggers pandas FutureWarning for incompatible dtypes.
            text_cols = df.select_dtypes(include=["object", "string"]).columns
            if len(text_cols) > 0:
                df[text_cols] = df[text_cols].fillna("")
            before_dedup = len(df)
            df = df.drop_duplicates(subset=["chebi_id"], keep="first")
            dropped = before_dedup - len(df)
            if dropped > 0:
                self.logger.log(
                    f"ℹ️ Dropped {dropped} duplicate ChEBI rows before load.",
                    "INFO",
                )

        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # GET ENTITY GROUP ID AND OMICS STATUS
        try:
            self.get_entity_group("Chemicals")
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

        # Set DB and drop indexes
        try:
            self.db_write_mode()
            self.drop_indexes(self.get_chemical_index_specs)
            self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            total_warnings += 1
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        # Clean Data Source
        df["definition"] = df["definition"].fillna("")
        df["ascii_name"] = df["ascii_name"].fillna("")
        # df["label"] = df["label"].fillna("")

        # Numeric normalization
        numeric_fields = ["charge", "mass", "monoisotopic_mass", "structure_id"]  # noqa E501
        # for col in numeric_fields:
        #     df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in numeric_fields:
            df[col] = pd.to_numeric(df[col], errors="coerce").replace({np.nan: None})  # noqa E501

        # Boolean normalization
        df["is_autogenerated"] = (
            df["is_autogenerated"]
            .astype(str)
            .str.upper()
            .replace(
                {"TRUE": True, "FALSE": False, "NAN": None, "NONE": None, "": None}  # noqa E501
            )
        )

        try:
            existing_chemical_ids = {
                cid
                for (cid,) in (
                    self.session.query(ChemicalMaster.chemical_id)
                    .filter(ChemicalMaster.data_source_id == self.data_source.id)  # noqa E501
                    .all()
                )
            }

            failed_records = []

            for row in df.itertuples(index=False):
                row_dict = {}
                try:
                    row_dict = row._asdict()
                    chebi_id = row_dict.get("chebi_id")
                    if not chebi_id:
                        continue

                    # --- Aliases ---
                    alias_dict = self.build_alias(row_dict)
                    is_primary_alias = next(
                        (a for a in alias_dict if a.get("is_primary")), None
                    )
                    not_primary_alias = [a for a in alias_dict if a != is_primary_alias]  # noqa E501

                    aliases_extra = row_dict.get("aliases_extra", [])
                    if isinstance(aliases_extra, np.ndarray):
                        aliases_extra = aliases_extra.tolist()
                    if not isinstance(aliases_extra, list):
                        aliases_extra = []

                    for alias in aliases_extra:
                        if alias.get("alias_value") and alias.get("xref_source"):  # noqa E501
                            not_primary_alias.append(alias)

                    # Drop Alias Invalids
                    not_primary_alias = [
                        alias
                        for alias in not_primary_alias
                        if alias.get("xref_source") != "PubMed"
                    ]

                    # --- Status ---
                    status_id = row_dict.get("status_id")
                    if status_id == 4:
                        omic_status_id = status_map["deactive"].id
                        is_active_entity = False
                    else:
                        omic_status_id = status_map["active"].id
                        is_active_entity = True

                    # --- Entity ---
                    entity_id, _ = self.get_or_create_entity(
                        name=is_primary_alias["alias_value"],
                        group_id=self.entity_group,
                        data_source_id=self.data_source.id,
                        package_id=self.package.id,
                        alias_type=is_primary_alias["alias_type"],
                        xref_source=is_primary_alias["xref_source"],
                        alias_norm=is_primary_alias["alias_norm"],
                        is_active=is_active_entity,
                        auto_commit=False,
                    )

                    # if not _:
                    #     print(f"Jump --> {chebi_id}")
                    #     continue

                    # --- Entity Aliases ---
                    self.get_or_create_entity_name(
                        group_id=self.entity_group,
                        entity_id=entity_id,
                        aliases=not_primary_alias,
                        is_active=is_active_entity,
                        data_source_id=self.data_source.id,
                        package_id=self.package.id,
                        auto_commit=False,
                    )

                    # --- Chemical Master ---
                    if chebi_id not in existing_chemical_ids:
                        chem_master = ChemicalMaster(
                            chemical_id=chebi_id,
                            # name=row.get("ascii_name"),
                            name=self.guard_description(
                                row_dict.get("ascii_name")
                            ),
                            definition=row_dict.get("definition"),
                            # ascii_name=row.get("ascii_name"),
                            # ascii_name=None,
                            omic_status_id=omic_status_id,
                            formula=row_dict.get("formula"),
                            charge=row_dict.get("charge"),
                            mass=row_dict.get("mass"),
                            monoisotopic_mass=row_dict.get(
                                "monoisotopic_mass"
                            ),
                            structure_id=row_dict.get("structure_id"),
                            is_autogenerated=row_dict.get("is_autogenerated"),
                            entity_id=entity_id,
                            data_source_id=self.data_source.id,
                            etl_package_id=self.package.id,
                        )
                        self.session.add(chem_master)
                        existing_chemical_ids.add(chebi_id)

                    # Single commit per row (instead of multiple commits inside
                    # helper methods) to reduce transaction overhead.
                    self.session.commit()

                    total_chemicals += 1

                except Exception as inner_e:
                    self.session.rollback()
                    failed_records.append((row_dict.get("chebi_id"), str(inner_e)))  # noqa E501
                    self.logger.log(
                        f"⚠️ Skipped {row_dict.get('chebi_id')} due to error: {inner_e}",  # noqa E501
                        "WARNING",
                    )

        except Exception as e:
            self.session.rollback()
            msg = f"❌ Critical error during load: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Set DB to Read Mode and Create Index
        try:
            self.create_indexes(self.get_chemical_index_specs)
            self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            total_warnings += 1
            msg = f"Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"📥 Total Chemicals processed: {total_chemicals}"  # noqa E501
        self.logger.log(msg, "INFO")

        return True, msg

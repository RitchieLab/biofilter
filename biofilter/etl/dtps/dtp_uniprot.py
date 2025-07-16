import os
import requests
from pathlib import Path
import pandas as pd
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.db.models.entity_models import EntityGroup
from biofilter.etl.mixins.base_dtp import DTPBase

from biofilter.db.models.protein_models import (
    ProteinMaster,
    ProteinEntity,
    ProteinPfam,
    ProteinPfamLink,
)  # noqa E501

import xml.etree.ElementTree as ET


def get_text(element, path, ns, default=""):
    tag = element.find(path, ns)
    return tag.text if tag is not None else default


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        datasource=None,
        etl_process=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.data_source = datasource
        self.etl_process = etl_process
        self.session = session
        self.use_conflict_csv = use_conflict_csv

        # DTP versioning
        self.dtp_name = "dtp_uniprot"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "3.0.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Download UniProt data in XML format and store it locally.
        Also computes a file hash to track content versioning.
        """
        self.logger.log(
            f"⬇️  Starting extraction of {self.data_source.name} data...",
            "INFO",
        )

        # Check Compartibility
        self.check_compatibility()

        msg = ""
        source_url = self.data_source.source_url
        if force_steps:
            last_hash = ""
            msg = "Ignoring hash check, forcing download"
            self.logger.log(msg, "WARNING")
        else:
            last_hash = self.etl_process.raw_data_hash

        try:
            # Prepare download path
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)
            file_path = os.path.join(landing_path, "proteins.xml")

            # Download the XML file
            msg = f"⬇️  Fetching XML from URL: {source_url} ..."
            self.logger.log(msg, "INFO")

            headers = {
                "Accept": "application/xml"
            }  # Optional, UniProt responds with XML anyway
            response = requests.get(source_url, headers=headers)

            if response.status_code != 200:
                msg = f"Failed to fetch data from UniProt: {response.status_code}"  # noqa: E501
                self.logger.log(msg, "ERROR")
                return False, msg, None

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(response.text)

            # Compute hash
            current_hash = compute_file_hash(file_path)
            if current_hash == last_hash:
                msg = f"No change detected in {file_path}"
                self.logger.log(msg, "INFO")
                return False, msg, current_hash

            msg = f"✅ UniProt file downloaded to {file_path}"
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
        Transforms the xml data from Uniprot in two output files:
            - master_data.csv
            - relationship_data.csv.
        """

        msg = f"🔧 Transforming the {self.data_source.name} data ..."
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
            input_file = input_path / "proteins.xml"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return None, False, msg

            # Output files paths
            output_file_master = output_path / "master_data"
            output_file_relationship = output_path / "relationship_data"

            # Delete existing files if they exist (both .csv and .parquet)
            for f in [output_file_master, output_file_relationship]:
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
            return None, False, msg

        # Parse XML and extract data
        ns = {"up": "http://uniprot.org/uniprot"}
        data = []

        try:
            root = ET.parse(input_file).getroot()

            for entry in root.findall("up:entry", ns):
                row = {}
                accessions = entry.findall("up:accession", ns)
                row["uniprot_id"] = accessions[0].text if accessions else ""
                row["secondary_ids"] = "|".join(a.text for a in accessions[1:])

                row["uniprot_name"] = get_text(entry, "up:name", ns)
                row["gene_symbol"] = get_text(
                    entry, "up:gene/up:name[@type='primary']", ns
                )
                row["full_name"] = get_text(
                    entry, "up:protein/up:recommendedName/up:fullName", ns
                )
                row["ec_number"] = get_text(
                    entry, "up:protein/up:recommendedName/up:ecNumber", ns
                )

                row["organism"] = get_text(
                    entry, "up:organism/up:name[@type='scientific']", ns
                )
                db_ref = entry.find(
                    "up:organism/up:dbReference[@type='NCBI Taxonomy']", ns
                )
                row["tax_id"] = (
                    db_ref.attrib["id"] if db_ref is not None else ""
                )  # noqa: E501

                row["function"] = self._get_comment_text(entry, "function", ns)
                row["location"] = self._get_subcellular_locations(entry, ns)
                row["tissue"] = self._get_comment_text(
                    entry, "tissue specificity", ns
                )  # noqa: E501
                row["pseudogene_note"] = self._get_comment_text(
                    entry, "caution", ns
                )  # noqa: E501

                row["go_terms"] = self._get_db_ids(entry, "GO", ns)
                row["kegg"] = self._get_db_id(entry, "KEGG", ns)
                row["hgnc"] = self._get_db_id(entry, "HGNC", ns)
                row["refseq"] = self._get_db_id(entry, "RefSeq", ns)

                seq_tag = entry.find("up:sequence", ns)
                row["protein_length"] = (
                    seq_tag.attrib["length"] if seq_tag is not None else ""
                )

                row["isoforms"] = self._get_isoform_ids(entry, ns)
                row["pfam_ids"] = self._get_pfam_ids(entry, ns)

                data.append(row)

            # DataFrame with fulldata results
            df = pd.DataFrame(data)

            # Split Master Data and Relationship Data
            master_cols = [
                "uniprot_id",
                "secondary_ids",
                "uniprot_name",
                "gene_symbol",
                "full_name",
                "ec_number",
                "organism",
                "tax_id",
                "function",
                "location",
                "tissue",
                "pseudogene_note",
                "protein_length",
                "isoforms",
                "pfam_ids",
            ]  # noqa: E501

            # Write master_data.csv
            master_df = df[master_cols]
            master_df.to_csv(
                output_file_master.with_suffix(".csv"), index=False
            )  # noqa: E501
            master_df.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )

            msg = f"✅ UniProt master data written with {len(df)} records)"
            self.logger.log(msg, "INFO")

            # Create long-form Relationship data file
            link_rows = []

            for row in df.itertuples():
                source_id = row.uniprot_id

                if row.go_terms:
                    for go in str(row.go_terms).split(";"):
                        link_rows.append(
                            {
                                "source_id": source_id,
                                "target_id": go.strip(),
                                "source_type": "Proteomics",
                                "target_type": "GO",
                                # "relation_type": "associated_with",
                                "relation_type": "part_of",
                            }
                        )

                if row.kegg:
                    for k in str(row.kegg).split(";"):
                        link_rows.append(
                            {
                                "source_id": source_id,
                                "target_id": k.strip(),
                                "source_type": "Proteomics",
                                "target_type": "Pathways",
                                "relation_type": "in_pathway",
                            }
                        )

                if row.hgnc:
                    for h in str(row.hgnc).split(";"):
                        link_rows.append(
                            {
                                "source_id": source_id,
                                "target_id": h.strip(),
                                "source_type": "Proteomics",
                                "target_type": "Genes",
                                "relation_type": "encodes",
                            }
                        )

                if row.refseq:
                    for r in str(row.refseq).split(";"):
                        link_rows.append(
                            {
                                "source_id": source_id,
                                "target_id": r.strip(),
                                "source_type": "Proteomics",
                                "target_type": "transcript",
                                "relation_type": "has_transcript",
                            }
                        )

                # if row.pfam_ids:
                #     for pf in str(row.pfam_ids).split(";"):
                #         link_rows.append(
                #             {
                #                 "source_id": source_id,
                #                 "target_id": pf.strip(),
                #                 "source_type": "Proteomics",
                #                 "target_type": "pfam",
                #                 "relation_type": "contains_domain",
                #             }
                #         )

            # Write links.csv
            links_df = pd.DataFrame(link_rows)

            # Save links data in both CSV and Parquet formats
            links_df.to_csv(
                output_file_relationship.with_suffix(".csv"), index=False
            )  # noqa: E501
            links_df.to_parquet(
                output_file_relationship.with_suffix(".parquet"), index=False
            )  # noqa: E501

            self.logger.log(
                f"✅ UniProt links written with {len(link_rows)} links)", "INFO"
            )  # noqa: E501

            msg = f"✅ Finished transforming {self.data_source.name} data."
            return None, True, msg

        except Exception as e:
            msg = f"❌ ETL transform failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

    def _get_comment_text(self, entry, type_, ns):
        comment = entry.find(f"up:comment[@type='{type_}']", ns)
        if comment is not None:
            text = comment.find("up:text", ns)
            return text.text if text is not None else ""
        return ""

    def _get_subcellular_locations(self, entry, ns):
        locs = entry.findall(
            "up:comment[@type='subcellular location']/up:subcellularLocation/up:location",  # noqa: E501
            ns,
        )
        return "|".join([loc.text for loc in locs if loc is not None])

    def _get_db_ids(self, entry, db_type, ns):
        return "|".join(
            [
                db.attrib["id"]
                for db in entry.findall(
                    f"up:dbReference[@type='{db_type}']", ns
                )  # noqa: E501
                if "id" in db.attrib
            ]
        )

    def _get_db_id(self, entry, db_type, ns):
        db = entry.find(f"up:dbReference[@type='{db_type}']", ns)
        return db.attrib["id"] if db is not None else ""

    def _get_isoform_ids(self, entry, ns):
        isoform_ids = []
        alt_products = entry.find(
            "up:comment[@type='alternative products']", ns
        )  # noqa: E501
        if alt_products is not None:
            for iso in alt_products.findall("up:isoform", ns):
                iso_id = iso.find("up:id", ns)
                if iso_id is not None:
                    isoform_ids.append(iso_id.text)
        return "|".join(isoform_ids)

    def _get_pfam_ids(self, entry, ns):
        pfams = [
            db.attrib["id"]
            for db in entry.findall("up:dbReference[@type='Pfam']", ns)
            if "id" in db.attrib
        ]
        return "|".join(pfams)

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):

        msg = f"📥 Loading {self.data_source.name} data into the database..."
        self.logger.log(
            msg,
            "INFO",
        )

        # Check Compartibility
        self.check_compatibility()

        total_proteins = 0
        total_isoforms = 0
        load_status = False

        data_source_id = self.data_source.id

        if df is None:
            if not processed_dir:
                msg = "Either 'df' or 'processed_path' must be provided."
                self.logger.log(msg, "ERROR")
                return total_proteins, load_status, msg

            processed_path = self.get_path(processed_dir)
            processed_data = str(processed_path / "master_data.parquet")

            if not os.path.exists(processed_data):
                msg = f"File not found: {processed_data}"
                self.logger.log(msg, "ERROR")
                return total_proteins, load_status, msg

            self.logger.log(
                f"📥 Reading data in chunks from {processed_data}", "INFO"
            )  # noqa E501

            # df = pd.read_csv(processed_data, dtype=str)
            df = pd.read_parquet(processed_data, engine="pyarrow")

        # Check if DataFrame is empty
        if df.empty:
            msg = "DataFrame is empty."
            self.logger.log(msg, "ERROR")
            return total_proteins, load_status, msg

        df["isoforms"] = df["isoforms"].fillna("")
        df["pfam_ids"] = df["pfam_ids"].fillna("")
        df["secondary_ids"] = df["secondary_ids"].fillna("")

        # Get Entity Group ID
        if not hasattr(self, "entity_group") or self.entity_group is None:
            group = (
                self.session.query(EntityGroup)
                .filter_by(name="Proteomics")
                .first()  # noqa: E501
            )  # noqa: E501
            if not group:
                msg = "EntityGroup 'Proteomics' not found in the database."
                self.logger.log(msg, "ERROR")
                return total_proteins, load_status
                # raise ValueError(msg)
            self.entity_group = group.id
            msg = f"EntityGroup ID for 'Proteomics' is {self.entity_group}"
            self.logger.log(msg, "DEBUG")

        try:
            # Interaction to each UnitProt entry
            for _, row in df.iterrows():

                # CANONICAL PROTEIN
                # Add or Get Entity for Canonical protein
                protein_master = row["uniprot_id"]
                # Skip if no protein master ID
                if not protein_master:
                    msg = f"Protein Master not found in row: {row}"
                    self.logger.log(msg, "WARNING")
                    continue
                # Add or Get ProteinMaster
                entity_id, _ = self.get_or_create_entity(
                    name=protein_master,
                    group_id=self.entity_group,
                    data_source_id=self.data_source.id,
                )

                # Add all possible aliases for this protein
                possible_names = set()
                # Principal Name (Uniprot_name)
                if row.get("uniprot_name"):
                    possible_names.add(row["uniprot_name"].strip())
                # Full Name (full_name)
                if row.get("full_name"):
                    possible_names.add(row["full_name"].strip())
                # Secondary IDs (can be multiple, separated by "|")
                if row.get("secondary_ids"):
                    for sid in row["secondary_ids"].split("|"):
                        sid = sid.strip()
                        if sid:
                            possible_names.add(sid)
                # Record all found names
                for name in possible_names:
                    self.get_or_create_entity_name(
                        entity_id, name, data_source_id=self.data_source.id
                    )

                # Create Protein Master object (This is the Canonical Protein)
                protein_master_obj = (
                    self.session.query(ProteinMaster)
                    .filter_by(
                        protein_id=protein_master,
                        data_source_id=data_source_id,  # noqa: E501
                    )  # noqa: E501
                    .first()
                )
                if not protein_master_obj:
                    protein_master_obj = ProteinMaster(
                        protein_id=protein_master,
                        function=row.get("function"),
                        location=row.get("location"),
                        tissue_expression=row.get("tissue"),
                        pseudogene_note=row.get("pseudogene_note"),
                        data_source_id=self.data_source.id,
                    )
                    self.session.add(protein_master_obj)
                    self.session.flush()  # be sure to protein is generated

                # ProteinEntity for Canonical Protein
                protein_entity_obj = (
                    self.session.query(ProteinEntity)
                    .filter_by(
                        protein_master_id=protein_master_obj.id,
                        entity_id=entity_id,
                    )
                    .first()
                )
                if not protein_entity_obj:
                    protein_entity = ProteinEntity(
                        entity_id=entity_id,
                        protein_master_id=protein_master_obj.id,
                        is_isoform=False,
                        data_source_id=self.data_source.id,
                    )
                    self.session.add(protein_entity)

                # Canonical Protein and Pfam Links
                pfam_ids = row.get("pfam_ids", "").strip()
                if pfam_ids:
                    for pfam_acc in pfam_ids.split("|"):
                        pfam_acc = pfam_acc.strip()
                        if not pfam_acc:
                            continue
                        # Get Pfam object
                        pfam = (
                            self.session.query(ProteinPfam)
                            .filter_by(pfam_acc=pfam_acc)
                            .first()
                        )
                        if not pfam:
                            msg = f"⚠️ PFAM accession '{pfam_acc}' not found in ProteinPfam table"  # noqa E501
                            self.logger.log(msg, "WARNING")
                            continue
                        # Check if PFAM accession exists in ProteinPfam table
                        protein_pfam_link = (
                            self.session.query(ProteinPfamLink)
                            .filter_by(
                                protein_master_id=protein_master_obj.id,
                                pfam_id=pfam.id,
                            )
                            .first()
                        )
                        if not protein_pfam_link:
                            pfam_link = ProteinPfamLink(
                                protein_master_id=protein_master_obj.id,
                                pfam_id=pfam.id,
                                data_source_id=self.data_source.id,
                            )
                            self.session.add(pfam_link)

                # ISOFORM PROTEIN
                isoform_ids = []
                isoform_str = row.get("isoforms", "").strip()
                # isoform_str = str(row.get("isoforms") or "").strip()
                if isoform_str:
                    for isoform_acc in isoform_str.split("|"):
                        isoform_acc = isoform_acc.strip()
                        if not isoform_acc:
                            continue

                        isoform_entity_id, _ = self.get_or_create_entity(
                            name=isoform_acc,
                            group_id=self.entity_group,
                            data_source_id=self.data_source.id,
                        )

                        # Registrar ProteinEntity com is_isoform=True
                        protein_entity_obj = (
                            self.session.query(ProteinEntity)
                            .filter_by(
                                protein_master_id=protein_master_obj.id,
                                entity_id=isoform_entity_id,
                            )
                            .first()
                        )
                        if not protein_entity_obj:
                            isoform_entity = ProteinEntity(
                                entity_id=isoform_entity_id,
                                protein_master_id=protein_master_obj.id,
                                is_isoform=True,
                                isoform_accession=isoform_acc,
                                data_source_id=self.data_source.id,
                            )
                            self.session.add(isoform_entity)
                            isoform_ids.append(isoform_entity_id)

                        total_isoforms += 1

                total_proteins += 1

            msg = f"📥 Total Proteins: {total_proteins} | Total Isoforms: {total_isoforms}"  # noqa E501
            self.logger.log(msg, "INFO")
            return total_proteins, True, msg

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return 0, False, msg

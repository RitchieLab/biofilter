import os
import ast
import json
import requests
import pandas as pd
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
# from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.db.models.entity_models import EntityGroup
# from biofilter.db.models.curation_models import (
#     CurationConflict,
#     ConflictStatus,
# )  # noqa E501
# from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase

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
            file_path = os.path.join(landing_path, "master_data.xml")

            # Download the XML file
            msg = f"⬇️  Fetching XML from URL: {source_url} ..."
            self.logger.log(msg, "INFO")

            headers = {"Accept": "application/xml"}  # Optional, UniProt responds with XML anyway
            response = requests.get(source_url, headers=headers)

            if response.status_code != 200:
                msg = f"Failed to fetch data from UniProt: {response.status_code}"
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

    # def transform(self, raw_path, processed_path):
    # def transform(self, raw_dir: str, processed_dir: str):
    def transform(self, raw_path, processed_path):
        self.logger.log(f"🔧 Transforming the {self.data_source.name} data ...", "INFO")

        # INPUT FILE
        # input_file = self.get_raw_file(raw_path, filename="master_data.xml")
        input_file = self.get_raw_file(raw_path)
        from pathlib import Path  # BUG
        input_file = Path("biofilter_data/raw/UniPort/uniprot/master_data.xml")  # BUG

        if not input_file.exists():
            msg = f"❌ Input file not found: {input_file}"
            self.logger.log(msg, "ERROR")
            return None, False, msg

        # OUTPUT DIR
        output_dir = self.get_path(processed_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "proteins.csv"

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
                row["gene_symbol"] = get_text(entry, "up:gene/up:name[@type='primary']", ns)
                row["full_name"] = get_text(entry, "up:protein/up:recommendedName/up:fullName", ns)
                row["ec_number"] = get_text(entry, "up:protein/up:recommendedName/up:ecNumber", ns)

                row["organism"] = get_text(entry, "up:organism/up:name[@type='scientific']", ns)
                db_ref = entry.find("up:organism/up:dbReference[@type='NCBI Taxonomy']", ns)
                row["tax_id"] = db_ref.attrib["id"] if db_ref is not None else ""

                row["function"] = self._get_comment_text(entry, "function", ns)
                row["location"] = self._get_subcellular_locations(entry, ns)
                row["tissue"] = self._get_comment_text(entry, "tissue specificity", ns)
                row["pseudogene_note"] = self._get_comment_text(entry, "caution", ns)

                row["go_terms"] = self._get_db_ids(entry, "GO", ns)
                row["kegg"] = self._get_db_id(entry, "KEGG", ns)
                row["hgnc"] = self._get_db_id(entry, "HGNC", ns)
                row["refseq"] = self._get_db_id(entry, "RefSeq", ns)

                seq_tag = entry.find("up:sequence", ns)
                row["protein_length"] = seq_tag.attrib["length"] if seq_tag is not None else ""

                row["isoforms"] = self._get_isoform_ids(entry, ns)
                row["pfam_ids"] = self._get_pfam_ids(entry, ns)

                data.append(row)

            df = pd.DataFrame(data)
            df.to_csv(output_file, index=False)

            msg = f"✅ UniProt transformation completed: {len(df)} entries written to {output_file}"
            self.logger.log(msg, "INFO")
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
        locs = entry.findall("up:comment[@type='subcellular location']/up:subcellularLocation/up:location", ns)
        return "|".join([loc.text for loc in locs if loc is not None])

    def _get_db_ids(self, entry, db_type, ns):
        return "|".join([
            db.attrib["id"]
            for db in entry.findall(f"up:dbReference[@type='{db_type}']", ns)
            if "id" in db.attrib
        ])

    def _get_db_id(self, entry, db_type, ns):
        db = entry.find(f"up:dbReference[@type='{db_type}']", ns)
        return db.attrib["id"] if db is not None else ""
    
    def _get_isoform_ids(self, entry, ns):
        isoform_ids = []
        alt_products = entry.find("up:comment[@type='alternative products']", ns)
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

        msg = "NOT IMPLEMENTED"
        self.logger.log(msg, "ERROR")

        return 0, False, msg

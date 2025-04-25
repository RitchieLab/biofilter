import os
import bz2
import json
import pandas as pd
from pathlib import Path
# from typing import Optional

from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase


class DTP(DTPBase):
    def __init__(
        self,
        logger=None,
        datasource=None,
        etl_process=None,
        session=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.datasource = datasource
        self.etl_process = etl_process
        self.session = session
        self.use_conflict_csv = use_conflict_csv
        self.conflict_mgr = ConflictManager(session, logger)

    def extract(self, raw_dir: str, source_url: str, last_hash: str):
        """
        Downloads the file from the dbSNP JSON release and stores it locally
        only if it doesn't exist or if the MD5 has changed.
        """

        try:
            message = ""

            # Landing path
            landing_path = os.path.join(
                raw_dir,
                self.datasource.source_system.name,
                self.datasource.name,
            )

            # Get hash from remote md5 file
            url_md5 = f"{source_url}.md5"
            remote_hash = self.get_md5_from_url_file(url_md5)
            if not remote_hash:
                msg = f"Failed to retrieve MD5 from {url_md5}"
                self.logger.log(msg, "WARNING")

            # Compare remote hash and last processed hash
            if remote_hash == last_hash:
                message = f"File already downloaded and hash matches: {last_hash}"      # noqa: E501
                self.logger.log(message, "INFO")
                return True, message, remote_hash

            # Download the file
            status, message = self.http_download(source_url, landing_path)

            if not status:
                self.logger.log(message, "ERROR")
                return False, message, remote_hash

            return True, message, remote_hash

        except Exception as e:
            message = f"❌ ETL extract failed: {str(e)}"
            self.logger.log(message, "ERROR")
            return False, message, None

    # 🚧 🚜 In developing
    def transform(self, raw_path, processed_path):

        # INPUT DATA
        input_file = self.get_raw_file(raw_path)
        if not input_file.exists():
            msg = f"❌ Input file not found: {input_file}."
            msg += " Consider running the extract() step or checking the source URL."               # noqa: E501
            self.logger.log(msg, "ERROR")
            return False, msg

        # OUTPUT DATA
        output_dir = self.get_path(processed_path)

        # RUN TRANSFORM
        status = self.transform_dbsnp_to_parquet(input_file, output_dir)

        transform_df = pd.DataFrame([None])  # Run in Parquet Files
        message = "Transform completed successfully."

        return transform_df, status, message

    # 🚧 No developed yet
    def load(self, df=None, processed_path=None):
        print(df)
        print(processed_path)
        return True

    # Extra methods
    def transform_dbsnp_to_parquet(
        self,
        input_file: Path,
        output_dir: Path,
        assembly_filter: str = "GRCh38"
    ) -> None:  # noqa: E501
        """
        Transforms a dbSNP JSON file (bz2) into structured Parquet files
        aligned with Biofilter 3R's variant models.

        Parameters:
        - input_file: Path to the .json.bz2 dbSNP file
        - output_dir: Directory where Parquet files will be saved
        - assembly_filter: (default: GRCh38)
        """
        variants = []
        locations = []
        # annotations = []
        # hgvs = []

        # https://www.ncbi.nlm.nih.gov/snp/rs268

        # https://ftp.ncbi.nih.gov/snp/organisms/human_9606/chr_rpts/ (Antigo)

        with bz2.open(input_file, "rt", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                rs_id = f"rs{record['refsnp_id']}"

                primary_data = record.get("primary_snapshot_data", {})

                variant_type = primary_data.get("variant_type", "NA")

                variants.append({
                    "rs_id": rs_id,
                    "variant_type": variant_type.upper(),
                    "length": 0
                })

                locations.append({
                    "rs_id": rs_id,
                    "chromosome": 1,
                    "position": 1,
                    "reference_allele": "A",
                    "alternate_allele": "G"
                })

                # annotations.append({
                #     "rs_id": rs_id,
                #     "effect": "",
                #     "phenotype": "",
                #     "clinical_significance": ""
                # })

                # hgvs.append({
                #     "rs_id": rs_id,
                #     "notation": "",
                #     "level": ""
                # })

        output_dir.mkdir(parents=True, exist_ok=True)

        pd.DataFrame(variants).to_parquet(output_dir / "variants.parquet", index=False)                 # noqa: E501
        pd.DataFrame(locations).to_parquet(output_dir / "variant_locations.parquet", index=False)       # noqa: E501
        # pd.DataFrame(annotations).to_parquet(output_dir / "variant_annotations.parquet", index=False)   # noqa: E501
        # pd.DataFrame(hgvs).to_parquet(output_dir / "variant_hgvs.parquet", index=False)                 # noqa: E501
        # print(f"✅ Transform complete. Files written to: {output_dir}")                               # noqa: E501

        return True

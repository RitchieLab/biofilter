import os
import gzip
import shutil
import requests
import pandas as pd
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.db.models.protein_models import ProteinPfam


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
        self.dtp_name = "dtp_pfam"
        self.dtp_version = "1.0.0"
        self.compatible_schema_min = "3.0.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str, force_steps: bool):
        """
        Download pfamA.txt.gz from the FTP server and extract it locally.
        Also computes a file hash to track content versioning.
        """

        msg = f"⬇️ Starting extraction of {self.data_source.name} data..."

        self.logger.log(
            msg,
            "INFO",  # noqa: E501
        )  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        source_url = self.data_source.source_url
        if force_steps:
            last_hash = ""
            msg = "Ignoring hash check, forcing download"
            self.logger.log(msg, "WARNING")
        else:
            last_hash = self.etl_process.raw_data_hash

        try:
            # Create destination directory
            landing_path = os.path.join(
                raw_dir,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(landing_path, exist_ok=True)

            # Prepare file paths
            gz_path = os.path.join(landing_path, "pfamA.txt.gz")
            txt_path = os.path.join(landing_path, "pfamA.txt")

            # Download the file
            msg = f"⬇️  Fetching gzipped file from: {source_url}"
            self.logger.log(msg, "INFO")

            response = requests.get(source_url, stream=True)

            if response.status_code != 200:
                msg = f"❌ Failed to fetch data: {response.status_code}"
                self.logger.log(msg, "ERROR")
                return False, msg, None

            with open(gz_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Extract the gz file
            self.logger.log(f"🗜️  Unzipping to: {txt_path}", "INFO")
            with gzip.open(gz_path, "rb") as f_in, open(txt_path, "wb") as f_out:  # noqa: E501
                shutil.copyfileobj(f_in, f_out)

            # Compute and compare hash
            current_hash = compute_file_hash(txt_path)
            if current_hash == last_hash:
                msg = f"⚠️  No changes detected in {txt_path}"
                self.logger.log(msg, "INFO")
                return False, msg, current_hash

            # Drop descompressed gz file
            os.remove(txt_path)

            msg = f"✅ File downloaded and extracted to {txt_path}"
            self.logger.log(msg, "INFO")
            return True, msg, current_hash

        except Exception as e:
            msg = f"❌ Exception during extract: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg, None

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_path, processed_path):

        self.logger.log(
            f"🔧 Transforming the {self.data_source.name} data ...", "INFO"
        )  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

        msg = ""
        try:
            # json_file = os.path.join(raw_path, "hgnc", "hgnc_data.json")
            landing_path = os.path.join(
                raw_path,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            processed_path = os.path.join(
                processed_path,
                self.data_source.source_system.name,
                self.data_source.name,
            )
            os.makedirs(processed_path, exist_ok=True)

            txt_file = os.path.join(landing_path, "pfamA.txt.gz")
            csv_file = os.path.join(processed_path, "master_data.csv")

            # Check if the txt file exists
            if not os.path.exists(txt_file):
                msg = f"File not found: {txt_file}"
                return None, False, msg

            # Create output directory if it doesn't exist
            # os.makedirs(os.path.dirname(csv_file), exist_ok=True)

            # Remove CSV file if it exists
            if os.path.exists(csv_file):
                os.remove(csv_file)
                self.logger.log(
                    f"⚠️ Previous CSV file deleted: {csv_file}", "DEBUG"
                )  # noqa: E501

            # define column names
            columns = [
                "pfam_acc",  # accession (ex: PF00001)
                "pfam_id",  # domain ID (ex: 7tm_1)
                "none_column",  # Column 2(C) no data
                "description",
                "clan_acc",  # accession clan (ex: CL0192)
                "source_database",  # DB Source (ex: Prosite)
                "type",  # domain or family
                "long_description",
            ]

            # Read only first N columns matching `columns`
            df = pd.read_csv(
                txt_file,
                sep="\t",
                header=None,
                usecols=range(len(columns)),
                names=columns,
                dtype=str,
                compression="gzip",
            )

            df.drop(columns=["none_column"], inplace=True)
            df["source_database"] = "Pfam"

            # Save DataFrame to CSV
            df.to_csv(csv_file, index=False)  # DUBUG propose only!
            df.to_parquet(csv_file.replace(".csv", ".parquet"), index=False)

            self.logger.log(
                f"✅ PFam data transformed and saved at {csv_file}", "INFO"
            )  # noqa: E501

            return df, True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            return None, False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, df=None, processed_dir=None, chunk_size=100_000):
        self.logger.log(
            f"📥 Loading {self.data_source.name} data into the database...",
            "INFO"  # noqa: E501
        )

        # Check Compartibility
        self.check_compatibility()

        total_pfam = 0
        load_status = False
        msg = ""

        data_source_id = self.data_source.id

        try:
            if df is None:
                if not processed_dir:
                    msg = "Either 'df' or 'processed_path' must be provided."
                    self.logger.log(msg, "ERROR")
                    return total_pfam, load_status, msg

                processed_path = self.get_path(processed_dir)
                processed_data = str(processed_path / "master_data.csv")

                if not os.path.exists(processed_data):
                    msg = f"File not found: {processed_data}"
                    self.logger.log(msg, "ERROR")
                    return total_pfam, load_status, msg

                self.logger.log(
                    f"📥 Reading data from {processed_data}",
                    "INFO"  # noqa: E501
                )
                df = pd.read_csv(processed_data, dtype=str)

            new_entries = []
            for row in df.itertuples(index=False):
                existing = (
                    self.session.query(ProteinPfam)
                    .filter_by(pfam_acc=row.pfam_acc)
                    .first()
                )

                if not existing:
                    new_entries.append(
                        ProteinPfam(
                            pfam_acc=row.pfam_acc,
                            pfam_id=row.pfam_id,
                            description=row.description,
                            clan_acc=row.clan_acc,
                            source_database=row.source_database,
                            type=row.type,
                            long_description=row.long_description,
                            data_source_id=data_source_id,
                        )
                    )

            if new_entries:
                self.session.bulk_save_objects(new_entries)
                self.session.commit()
                total_pfam = len(new_entries)

            msg = f"✅ New Pfam loaded: {total_pfam}"
            self.logger.log(msg, "INFO")
            return total_pfam, True, msg

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return 0, False, msg

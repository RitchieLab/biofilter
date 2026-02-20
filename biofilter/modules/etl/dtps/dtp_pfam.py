import os
import gzip
import shutil
import requests
import pandas as pd
from pathlib import Path
from biofilter.utils.file_hash import compute_file_hash
from biofilter.modules.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.modules.etl.mixins.base_dtp import DTPBase
from biofilter.modules.db.models.model_proteins import ProteinPfam


class DTP(DTPBase, EntityQueryMixin):
    def __init__(
        self,
        logger=None,
        debug_mode=False,
        datasource=None,
        package=None,
        session=None,
        db=None,
        use_conflict_csv=False,
    ):  # noqa: E501
        self.logger = logger
        self.debug_mode = debug_mode
        self.data_source = datasource
        self.package = package
        self.session = session
        self.db = db
        self.use_conflict_csv = use_conflict_csv

        # DTP versioning
        self.dtp_name = "dtp_pfam"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "0.0.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
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
            with gzip.open(gz_path, "rb") as f_in, open(
                txt_path, "wb"
            ) as f_out:  # noqa: E501
                shutil.copyfileobj(f_in, f_out)

            # Compute and compare hash
            current_hash = compute_file_hash(txt_path)

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
    def transform(self, raw_dir: str, processed_dir: str):

        self.logger.log(
            f"🔧 Transforming the {self.data_source.name} data ...", "INFO"
        )  # noqa: E501

        # Check Compartibility
        self.check_compatibility()

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

            txt_file = os.path.join(input_path, "pfamA.txt.gz")
            csv_file = os.path.join(output_path, "master_data.csv")

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

            # SAVE FILES
            if self.debug_mode:
                df.to_csv(csv_file, index=False)

            df.to_parquet(csv_file.replace(".csv", ".parquet"), index=False)

            msg = f"✅ PFam data transformed and saved at {csv_file}", "INFO"
            self.logger.log(msg, "INFO")

            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            return False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):

        msg = "📥 Loading Protein Family data into the database..."
        self.logger.log(msg, "INFO")

        # Check Compartibility
        self.check_compatibility()

        total_pfam = 0
        total_warnings = 0

        # ALIASES MAP FROM PROCESS DATA FIELDS
        # --> No Apply Here

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
        # --> No Apply Here

        # RUN LOAD BY ROW
        try:
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
                            # description=row.description,
                            description=self.guard_description(row.description),
                            clan_acc=row.clan_acc,
                            source_database=row.source_database,
                            type=row.type,
                            # long_description=row.long_description,
                            long_description=self.guard_description(
                                row.long_description
                            ),
                            data_source_id=self.data_source.id,
                            etl_package_id=self.package.id,
                        )
                    )

            if new_entries:
                self.session.bulk_save_objects(new_entries)
                self.session.commit()
                total_pfam = len(new_entries)

        except Exception as e:
            msg = f"❌ ETL load_relations failed: {str(e)}"
            self.logger.log(msg, "ERROR")
            return False, msg

        if total_warnings != 0:
            msg = f"{total_warnings} warning to analysis in log file"
            self.logger.log(msg, "WARNING")

        msg = f"✅ New Pfam loaded: {total_pfam}"
        self.logger.log(msg, "INFO")

        return True, msg

import os
import gzip
import requests
import pandas as pd
from pathlib import Path
from biofilter.utils.file_hash import compute_file_hash
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.etl.conflict_manager import ConflictManager
from biofilter.etl.mixins.base_dtp import DTPBase
from biofilter.db.models import (
    GeneMaster,
    GeneLocation,
)  # noqa E501


class DTP(DTPBase, EntityQueryMixin, GeneQueryMixin):
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
        self.dtp_name = "dtp_gene_hgnc"
        self.dtp_version = "1.1.0"
        self.compatible_schema_min = "3.1.0"
        self.compatible_schema_max = "4.0.0"

    # ⬇️  --------------------------  ⬇️
    # ⬇️  ------ EXTRACT FASE ------  ⬇️
    # ⬇️  --------------------------  ⬇️
    def extract(self, raw_dir: str):
        """
        Download data from the HGNC API and stores it locally.
        Also computes a file hash to track content versioning.
        """

        msg = f"⬇️  Starting extraction of {self.data_source.name} data..."
        self.logger.log(msg, "INFO")

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
                landing_path, "Homo_sapiens.GRCh38.115.chr.gff3.gz"
            )  # noqa E501

            # Download GFF3 file (binary)
            msg = f"⬇️  Downloading GFF3 file from: {source_url} ..."
            self.logger.log(msg, "INFO")

            response = requests.get(source_url, stream=True)
            if response.status_code != 200:
                msg = f"❌ Failed to fetch data from Ensembl: {response.status_code}"  # noqa E501
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

    # ⚙️  ----------------------------  ⚙️
    # ⚙️  ------ TRANSFORM FASE ------  ⚙️
    # ⚙️  ----------------------------  ⚙️
    def transform(self, raw_dir: str, processed_dir: str):

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
            input_file = input_path / "Homo_sapiens.GRCh38.115.chr.gff3.gz"
            if not input_file.exists():
                msg = f"❌ Input file not found: {input_file}"
                self.logger.log(msg, "ERROR")
                return False, msg

            # Output files paths
            output_file_master = output_path / "master_data"

            # Delete existing files if they exist (both .csv and .parquet)
            for f in [output_file_master]:
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

        try:
            # Parse GFF3 lines with "gene" type
            records = []
            with gzip.open(input_file, "rt") as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    cols = line.strip().split("\t")
                    if len(cols) < 9:
                        continue
                    # if cols[2] != "gene":
                    #     continue

                    (
                        chrom,
                        source,
                        feature_type,
                        start,
                        end,
                        score,
                        strand,
                        phase,
                        attributes,
                    ) = cols  # noqa E501

                    # Parse attributes field (e.g. ID=ENSG00000223972;Name=DDX11L1;biotype=transcribed_unprocessed_pseudogene)  # noqa E501
                    attr_dict = dict()
                    for entry in attributes.split(";"):
                        if "=" in entry:
                            key, value = entry.split("=", 1)
                            attr_dict[key.strip()] = value.strip()

                    # ⚠️ Skip rows that are not genes
                    if not attr_dict.get("ID", "").startswith("gene:"):
                        continue

                    record = {
                        "gene_id": attr_dict.get("ID"),
                        "gene_symbol": attr_dict.get("Name"),
                        "biotype": attr_dict.get("biotype"),
                        "chromosome": chrom,
                        "start": int(start),
                        "end": int(end),
                        "strand": strand,
                        "source": source,
                    }
                    records.append(record)

            df = pd.DataFrame(records)

            if self.debug_mode:
                df.to_csv(output_file_master.with_suffix(".csv"), index=False)
            df.to_parquet(
                output_file_master.with_suffix(".parquet"), index=False
            )  # noqa E501

            msg = f"✅ GFF3 gene data transformed and saved at {output_file_master}"  # noqa E501
            self.logger.log(msg, "INFO")
            return True, msg

        except Exception as e:
            msg = f"❌ Error during transformation: {e}"
            return False, msg

    # 📥  ------------------------ 📥
    # 📥  ------ LOAD FASE ------  📥
    # 📥  ------------------------ 📥
    def load(self, processed_dir=None):
        """
        TODO: CREATE DOCSTRING
        """

        msg = f"📥 Loading {self.data_source.name} data into the database..."

        self.logger.log(
            msg,
            "INFO",
        )

        # CHECK COMPARTIBILITY
        self.check_compatibility()

        created = 0
        updated = 0
        skipped = 0

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

            # Read Processed Gene Master Data
            if not os.path.exists(processed_file_name):
                msg = f"⚠️  File not found: {processed_file_name}"
                self.logger.log(msg, "ERROR")
                return False, msg  # ⧮ Leaving with ERROR
            df = pd.read_parquet(processed_file_name, engine="pyarrow")

            required_columns = {
                "gene_symbol",
                "chromosome",
                "start",
                "end",
                "strand",
            }  # noqa E501
            missing = required_columns - set(df.columns)
            if missing:
                msg = f"❌ Missing columns in DataFrame: {missing}"
                self.logger.log(msg, "ERROR")
                return False, msg

            # Drop rows without gene symbol (or empty strings)
            initial_rows = len(df)
            df = df.dropna(subset=["gene_symbol"])
            df = df[df["gene_symbol"].str.strip() != ""]
            removed = initial_rows - len(df)
            if removed > 0:
                self.logger.log(
                    f"ℹ️ Dropped {removed} rows with missing gene_symbol", "DEBUG"  # noqa E501
                )  # noqa E501

            if df.empty:
                msg = "⚠️ All rows were removed after dropping missing gene_symbol."  # noqa E501
                self.logger.log(msg, "WARNING")
                return False, msg

        except Exception as e:
            msg = f"⚠️  Failed to try read data: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg  # ⧮ Leaving with ERROR

        # SET DB AND DROP INDEXES
        try:
            self.db_write_mode()
            # self.drop_indexes(self.get_gene_index_specs)
            # self.drop_indexes(self.get_entity_index_specs)
        except Exception as e:
            msg = f"⚠️  Failed to switch DB to write mode or drop indexes: {e}"
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        try:

            # Load once the gene index to avoid repeated queries
            # gene_index = {
            #     (g.symbol, g.chromosome): g
            #     for g in self.session.query(GeneMaster).all()
            # }
            gene_index = {
                (g.symbol.upper(), g.chromosome): g
                for g in self.session.query(GeneMaster).all()
            }

            # NTERACTION WITH EACH MASTER DATA ROW
            # Row = Ensembl Gene (Process only Genes with Symbols)
            for _, row in df.iterrows():

                # key = (row["gene_symbol"], row["chromosome"])
                key = (row["gene_symbol"].upper(), row["chromosome"])
                gene = gene_index.get(key)

                if not gene:
                    self.logger.log(f"⚠️ Gene not found: {key}", "WARNING")
                    skipped += 1
                    continue

                # Check if location already exists
                location = (
                    self.session.query(GeneLocation)
                    .filter_by(gene_id=gene.id, assembly="GRCh38")
                    .first()
                )

                if location:
                    # Update
                    location.start_pos = row["start"]
                    location.end_pos = row["end"]
                    location.strand = row["strand"]
                    location.chromosome = row["chromosome"]
                    # Keep first Data Source and Package for now
                    updated += 1
                else:
                    # Create
                    location = GeneLocation(
                        gene_id=gene.id,
                        assembly="GRCh38",
                        start_pos=row["start"],
                        end_pos=row["end"],
                        strand=row["strand"],
                        chromosome=row["chromosome"],
                        region_id=None,
                        data_source_id=self.data_source.id,
                        etl_package_id=self.package.id,
                    )
                    self.session.add(location)
                    created += 1

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            msg = f"❌ Error during loading GeneLocation: {e}"
            self.logger.log(msg, "ERROR")
            return False, msg

        # Set DB to Read Mode and Create Index
        try:
            # self.create_indexes(self.get_gene_index_specs)
            # self.create_indexes(self.get_entity_index_specs)
            self.db_read_mode()
        except Exception as e:
            msg = f"⚠️  Failed to switch DB to read mode or create indexes: {e}"  # noqa E501
            self.logger.log(msg, "WARNING")
            return False, msg  # ⧮ Leaving with ERROR

        #  ---> LOAD FINISHED WITH SUCCESS
        msg = f"✅ GeneLocation loaded: {created} created, {updated} updated, {skipped} skipped"  # noqa E501
        self.logger.log(msg, "INFO")

        return True, msg

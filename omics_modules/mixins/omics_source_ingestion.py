import apsw


class SourceIngestionMixin:
    """
    Mixin for fast data ingestion using APSW.
    """

    def deleteAll(self):
        """
        Deletes all records for the current dataset_id from the specified
        tables.
        """
        tables = [
            "snpmerges",
            "snps",
        ]

        try:
            with self._apsw_db:
                cursor = self._apsw_db.cursor()

                for table in tables:
                    sql = f"DELETE FROM {table} WHERE source = ?"
                    cursor.execute(sql, (self.datasource_id,))

            self.logger.log("[INFO] All related records deleted successfully.")

        except apsw.ConstraintError as e:
            self.logger.log(
                f"[WARNING] Constraint error: {e}", level="WARNING"
            )  # noqa E501
        except Exception as e:
            self.logger.log(f"[ERROR] Deletion failed: {e}", level="ERROR")

    # Add records to the snpmerges table
    def add_snpmerges(self, load_data):
        sql = (
            "INSERT OR IGNORE INTO snpmerges "
            "(rs_source, rs_current, valid, source) "
            "VALUES (?, ?, ?, ?)"
        )
        try:
            with self._apsw_db:
                cursor = self._apsw_db.cursor()
                cursor.executemany(sql, load_data)  # Bulk insert

            self.logger.log(f"[INFO] Inserted {len(load_data)} SNP records.")

        except apsw.ConstraintError as e:
            self.logger.log(
                f"[WARNING] Constraint error: {e}", level="WARNING"
            )  # noqa E501
        except Exception as e:
            self.logger.log(f"[ERROR] Insert failed: {e}", level="ERROR")

    # Add records to the snps table
    def add_snps(self, load_data):
        sql = (
            "INSERT OR IGNORE INTO snps "
            "(chromosome, rs_source, position, valid, source) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        try:
            with self._apsw_db:
                cursor = self._apsw_db.cursor()
                cursor.executemany(sql, load_data)  # Bulk insert

            self.logger.log(f"[INFO] Inserted {len(load_data)} SNP records.")

        except apsw.ConstraintError as e:
            self.logger.log(f"[WARNING] Constraint error: {e}", level="WARNING")
        except Exception as e:
            self.logger.log(f"[ERROR] Insert failed: {e}", level="ERROR")

#     __tablename__ = "snps"
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     rs_source = Column(Integer, nullable=True, index=True)  # Ex: 9411893
#     rs_current = Column(Integer, unique=True, index=True, nullable=False)  # Ex: 3007669
#     chromosome = Column(SmallInteger, nullable=False)  # Ex: 1-22, 23 (X), 24 (Y), 25 (MT)
#     position = Column(Integer, nullable=False)  # SNP position in the chromosome
#     reference_allele = Column(String, nullable=False)  # Ref Allele (ex: "A", "GTC", "")
#     alternate_allele = Column(String, nullable=False)  # Alt Allele (ex: "G", "", "TCG")
#     variation_type = Column(String, nullable=False)  # "SNP", "Insertion", "Deletion"
#     build_source = Column(String, nullable=True)  # Ex: "GRCh38"
#     valid = Column(Boolean, default=True)  # Indication if this SNP is currently valid
#     source = Column(String, nullable=True)  # Ex: "dbSNP", "1000 Genomes"


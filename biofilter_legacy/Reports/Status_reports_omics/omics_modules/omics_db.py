from logger import Logger
from mixins import DBConfigMixin


class Database(DBConfigMixin):
    """
    Manages database connection and applies performance optimizations.
    """

    def __init__(self, dbFile=None, updating=False, temp_mem=False):  # noqa: E501
        """
        Initializes the db connection, integrity check, and optimizations.

        Args:
            dbFile (str): Path to the SQLite database file.
            updating (bool): If True, optimizes for bulk updates.
            temp_mem (bool): If True, enables in-memory temp storage.
        """
        # initialize instance properties
        self._updating = updating
        self._temp_mem = temp_mem
        self._verbose = True
        self._dbFile = dbFile
        if isinstance(dbFile, tuple) or isinstance(dbFile, list):
            self._dbFile = dbFile[0]
        self._updater = None
        self._dbURL = f"sqlite:///{self._dbFile}"
        self.logger = Logger()

        # Ensure database exists and is valid
        self.initialize_database()

        self.logger.log("[INFO] Database connection established.")

    def get_session(self):
        """
        Creates a new database session.
        """
        return self.SessionLocal()

    # CONTEXT MANAGER START
    def __enter__(self):
        self.session = self.get_session()
        return self.session

    # CONTEXT MANAGER END
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.session.rollback()
            self.logger.log(f"[ERROR] Error : {exc_value}", level="ERROR")
        else:
            self.session.commit()
        self.session.close()
        self.logger.log("[INFO] Finalized database session.")


# 🛠️ HOW TO USE IT:
# 📌 Criando e Conectando ao Banco
# db = OmicsDB()
# session = biofilter.db.get_session()

# 📌 Saída esperada no log se o banco não existir:
# [INFO] Banco de dados não encontrado. Criando novo...
# [INFO] Banco de dados criado com sucesso!
# [INFO] Conexão com o banco de dados estabelecida.

# 📌 Saída esperada se o banco existir e estiver íntegro:
# [INFO] Banco de dados encontrado. Verificando integridade...
# [INFO] Banco de dados íntegro e compatível.
# [INFO] Conexão com o banco de dados estabelecida.

# 📌 Saída esperada se o banco estiver corrompido ou incompatível:
# [INFO] Banco de dados encontrado. Verificando integridade...
# [ERROR] Banco de dados corrompido ou incompatível!
# Exception: Banco de dados corrompido ou incompatível com os modelos.

# 📌 Criando e Usando uma Sessão com with
# with OmicsDB() as session:
#     result = session.execute("SELECT COUNT(*) FROM genes")
#     print(result.scalar())  # Número total de genes no banco

# 📌 Executando um ETL Otimizado
# db = OmicsDB(updating=True)

# # Drop indexes before bulk insert
# biofilter.db.drop_indexes()

# # Perform bulk insert (Example)
# with biofilter.db.get_session() as session:
#     session.bulk_insert_mappings(SNP, [
#         {"rs_current": 12345, "chromosome": 1, "position": 1000000, "reference_allele": "A", "alternate_allele": "T"},  # noqa: E501
#         {"rs_current": 67890, "chromosome": 2, "position": 2000000, "reference_allele": "G", "alternate_allele": "C"},  # noqa: E501
#     ])
#     session.commit()

# # Recreate indexes after insert
# biofilter.db.recreate_indexes()

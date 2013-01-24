import os

# Old settings
# host = badger
# user = atf3
# pass = patO9FTPXUV0JonedaLe1COO
# name = ritchie_ensembl
class DBSettings:
	
	def __init__(self):
		self.db_host = "localhost"
		self.db_user = "root"
		self.db_pass = ""
		self.db_name = "LOKI"

		# PSU specific settings
		self.db_host = "ritchiedb.rcc.psu.edu"
		self.db_user = "loki_test"
		self.db_name = "loki_test"
		self.db_pass = "44NTx7NeEMyVrtdc"

		# Get these from the environment if they exist
		self.db_host = os.environ.get("DB_HOST", self.db_host)
		self.db_user = os.environ.get("DB_USER", self.db_user)
		self.db_pass = os.environ.get("DB_PASS", self.db_pass)
		self.db_name = os.environ.get("DB_NAME", self.db_name)


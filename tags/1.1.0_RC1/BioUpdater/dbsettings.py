import os

class DBSettings:
	
	def __init__(self):

		# Define the host, user, password and database here
		self.db_host = "localhost"
		self.db_user = "root"
		self.db_pass = ""
		self.db_name = "LOKI"

		# Alternatively, you can use the environment variables:
		# DB_HOST DB_USER DB_PASS and DB_NAME
		# environment variables trump the variables above
		self.db_host = os.environ.get("DB_HOST", self.db_host)
		self.db_user = os.environ.get("DB_USER", self.db_user)
		self.db_pass = os.environ.get("DB_PASS", self.db_pass)
		self.db_name = os.environ.get("DB_NAME", self.db_name)


import sqlite3

conn = sqlite3.connect(".db")
conn.execute("VACUUM;")
conn.close()

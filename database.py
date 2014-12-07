import MySQLdb

class Database:
	# database config
	conn   = ''
	cursor = ''

	# host 	 = "localhost"
	# username = "root"
	# password = "root"
	# database = "papernet"

	def __init__(self, host, username, password, database):
		# self.host = host
		# self.username = username
		# self.password = password
		# self.database = database
		
		self.conn = MySQLdb.connect(host, username, password, database)
		self.cursor = self.conn.cursor()

	def __del__(self):
		if self.cursor:
			self.cursor.close()
		if self.conn:
			self.conn.commit()
			self.conn.close()

	def executeSQL(self, sql):
		self.cursor.execute(sql)
		return self.cursor.fetchall()

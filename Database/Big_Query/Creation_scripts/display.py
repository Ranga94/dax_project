import mysql.connector

conn = mysql.connector.connect(host = "mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project", database = "dax_project", user = "igenie_readwrite", password = "igenie")
if conn_is_connected():
	print('Connected to database')
cursor = conn.cursor()
cursor.execute("select * from PARAM_TWITTER_KEYWORDS")
row = cursor.fetchall()
for row in rows: 
	print(row)
cursor.close()
conn.close()
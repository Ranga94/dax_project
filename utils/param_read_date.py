import mysql.connector

class SQL:
    def __init__(self):
        pass

    def insert_dates(self, from_date, to_date):
        conn = mysql.connector.connect(host="mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project",
                                       database="dax_project", user="igenie_readwrite", password="igenie")
        if conn.is_connected():
            # self.sql_connect = True
            print("Connected to MySQL database")
            cursor = conn.cursor()
            str = "Insert into dax_project.PARAM_READ_DATE (FROM_DATE, TO_DATE) VALUES ('{}','{}')".format(from_date,
                                                                                                           to_date)
            cursor.execute(str)
            conn.commit()
            cursor.close()
            conn.close()

    def get_dates(self):
        conn = mysql.connector.connect(host="mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project",
                                       database="dax_project", user="igenie_readwrite", password="igenie")
        if conn.is_connected():
            print("Connected to MySQL database")
            cursor = conn.cursor()
            cursor.execute("SELECT FROM_DATE from dax_project.PARAM_READ_DATE ORDER BY FROM_DATE DESC LIMIT 1")
            from_date = cursor.fetchall()
            cursor.execute("SELECT TO_DATE from dax_project.PARAM_READ_DATE ORDER BY TO_DATE DESC LIMIT 1")
            to_date = cursor.fetchall()
            return from_date, to_date


s = SQL()
s.insert_dates("2017-12-20 00:00:00", "2017-12-21 00:00:00")
a = s.get_dates()
print(a)
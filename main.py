import pyodbc
from DBConnector import DBConnector

database = 'Survey_Sample_A19'
username = 'sa'
password = '.'
server = 'localhost'

s = DBConnector(db_server=server, dbname=database, db_username=username, db_password=password)
print(isinstance(s, DBConnector))

s.selectBestDBDriverAvailable()
s.open()
row = s.conduit.cursor().execute("select * from Question").fetchone()
if row:
    print(row)


print(dir(s))

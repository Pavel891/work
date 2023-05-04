import psycopg2 as ps2
import pymssql as _mssql

conn_postgres = ps2.connect(dbname='postgres',
                            user='postgres',
                            password='12349876',
                            host='localhost',
                            port='5433')

# server = "sql-test"
# user = "sa"
# password = "sa"
# database = "upd_e"

conn_mssql = _mssql.connect(server = "sql-test", # type: ignore
                            user = "sa",
                            password = "sa",
                            database = "upd_e",
                            charset='cp1251')
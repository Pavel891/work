import os, sys; 
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath( __file__ ))))
import pymssql as _mssql
from config import cfg

def conn_mssql(station):
    db_name = cfg.ms_dbname.get(station)
    conn_ms = _mssql.connect(server = cfg.ms_srv, # type: ignore
                            user = cfg.ms_user,
                            password = cfg.ms_password,
                            database = db_name,
                            charset = cfg.ms_charset)

    return conn_ms
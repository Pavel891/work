import os, sys; 
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath( __file__ ))))
import psycopg2
import psycopg2.pool
from config import cfg

conn_postgres = psycopg2.pool.SimpleConnectionPool(cfg.pg_srv, cfg.pg_max_clients, 
                                                   dbname = cfg.pg_dbname,
                                                   user = cfg.pg_user,
                                                   password = cfg.pg_password,
                                                   host = cfg.pg_host,
                                                   port = cfg.pg_port)
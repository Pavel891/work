"""
Сервер работающи с БД закрытой сети.
main.py - многопоточный сервер, определяет клиентов и распределяет запросы.
upload_daily_... - функции для работы с клиентом диспетчера
pgsql / mssql_... - пераметры подключения к БД
loading_cash_data / ms_output / plan_task_output - модули для работы с клиентами станционных диспетчеров
"""

import yaml
import os.path

file_path = os.path.dirname(__file__)

# **********************************************************

host_file = os.path.join(file_path, 'host_cfg.yaml')
with open(host_file, 'r') as hf:
    host_conf = yaml.load(hf, Loader=yaml.FullLoader)

host_cfg = host_conf.get('host')
PORT = host_cfg.get('port')
BUF_SIZE = host_cfg.get('buf_size')
clients_listen = host_cfg.get('listen')
stations = host_conf.get('stations_list')
clients = host_conf.get('cliets_list')

# **********************************************************

pg_file = os.path.join(file_path, 'pg_cfg.yaml')
with open(pg_file, 'r') as pf:
    pg_conf = yaml.load(pf, Loader=yaml.FullLoader)

pg_srv = pg_conf.get('srv')
pg_max_clients = pg_conf.get('max_client')
pg_dbname = pg_conf.get('dbname')
pg_user = pg_conf.get('user')
pg_password = pg_conf.get('password')
pg_host = pg_conf.get('host')
pg_port = pg_conf.get('port')

# **********************************************************

ms_file = os.path.join(file_path, 'ms_cfg.yaml')
with open(ms_file, 'r') as ms:
    ms_conf = yaml.load(ms, Loader=yaml.FullLoader)

ms_srv = ms_conf.get('server')
ms_dbname = ms_conf.get('database')
ms_user = ms_conf.get('user')
ms_password = ms_conf.get('password')
ms_charset = ms_conf.get('charset')

# **********************************************************

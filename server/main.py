import socket, pickle, threading, logging
import config.cfg as cfg

from dispatcher.upload_daily_report import report_gen
from stations.loading_cash_data import LoaderData

logging.basicConfig(filename='server_log.log', format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG, datefmt='%d-%m-%Y %H:%M:%S')

HOST = socket.gethostbyname(socket.gethostname())
PORT = cfg.PORT
TYPE = socket.AF_INET
PROTOCOL = socket.SOCK_STREAM
BUF_SIZE = cfg.BUF_SIZE
SOC_LVL = socket.SOL_SOCKET
SOC_UM = socket.SO_REUSEADDR
SOC_BOOL = True
AMOUNT_CLIENTS = cfg.clients_listen
logging.debug(f'HOST: {HOST} \n\t TYPE: {TYPE} \n\t PROTOCOL: {PROTOCOL} \n\t BUF_SIZE: {cfg.BUF_SIZE} \n\t AMOUNT_CLIENTS: {AMOUNT_CLIENTS}')

print_lock = threading.Lock()
stats = cfg.stations
clients = cfg.clients
logging.debug(f'Stations: {cfg.stations} \n\t Clients: {cfg.clients}')

def threaded(clientsock, addr):
    while True:
        try:
            pal = clientsock.recv(BUF_SIZE)
            if not pal:
                print_lock.release()
                logging.warning('No data received, the stream is released.')
                break
            else: 
                data = pickle.loads(pal)
                if data[1] == clients[0]:
                    logging.info(f'The {data[1]} client has connected.')
                    logging.debug(f'{data} received from {addr}')
                    ans = report_gen(data[0])
                    ans = pickle.dumps(ans)
                    clientsock.sendall(ans)
                    logging.debug(f'Sent dataset to {data[1]}')
                    break
                elif data[1] == clients[1]:
                    pass
                elif data[1] in stats:
                    logging.info(f'The station client is connected: {data[1]}')
                    logging.debug(f'Received from {addr} / {pal}')
                    # logging.debug(f'data0: {data[0]} \n\t data0: {data[1]} \n\t data0: {data[2]} \n\t data0: {data[3]} \n\t data0: {data[4]}')
                    ans = LoaderData(data[0], data[1], data[2], data[3], data[4])
                    logging.debug(f'LoaderData result: {ans}')
                    # ans = pickle.dumps(ans)
                    # clientsock.sendall(ans)
                    # logging.debug(f'Sent dataset to {data[1]}')
                    break
                else:
                    logging.warning(f'THE CLIENT HAS NOT BEEN IDENTIFIED: {addr}')
                    break
        except Exception as e:
            logging.warning(f'Exception: {e}', exc_info=True)
        finally:
            pal = None
            data = None
            ans = None
            print_lock.release()
            logging.debug(f'Variable values are reset: \n\t pal = {pal} \n\t data = {data} \n\t ans = {ans}')
            logging.info('Exiting the stream')

    clientsock.close()
    logging.info('*' * 80)
    logging.debug(f'Listening to the address: {HOST} / {PORT}')

def main():

    srv = socket.socket(TYPE, PROTOCOL)
    srv.setsockopt(SOC_LVL, SOC_UM, SOC_BOOL)
    srv.bind((HOST, PORT))

    srv.listen(AMOUNT_CLIENTS)
    logging.info('The server is running.')

    while True:

        logging.debug(f'Listening to the address: {HOST} / {PORT}')
        
        clientsock, addr = srv.accept()
        logging.info('-' * 80)
        logging.debug(f'The client is connected: {clientsock}')

        thread = threading.Thread(target=threaded, args=(clientsock, addr))
        print_lock.acquire()
        thread.start()
        logging.info('New thread has been launched.')

if __name__ == '__main__':
    main()
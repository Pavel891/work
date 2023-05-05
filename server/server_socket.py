import socket
import pickle
from upload_daily_report import report_gen

HOST = ''
PORT = 9090

TYPE = socket.AF_INET
PROTOCOL = socket.SOCK_STREAM

srv = socket.socket(TYPE, PROTOCOL)
srv.bind((HOST, PORT))

while True:
    srv.listen(1)
    print('Слушаю порт', PORT)
    sock, addr = srv.accept()
    print('Подключен клиент', addr)
    while True:
        pal = sock.recv(1024)
        if not pal:
            break
        else:
            date = pal.decode('utf-8')
            day_rep = report_gen(date, 'place_report')
        print('Получено от %s:%s:' % addr, pal)
        day_df = pickle.dumps(day_rep)
        sock.sendall(day_df)
        print('Отправлено %s:%s:' % addr, day_df)
    sock.close()
    print('Соединение закрыто')
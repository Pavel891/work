# import socket
# import pickle


# HOST = 'localhost'
# PORT = 9090

# sock = socket.socket()
# sock.connect((HOST, PORT))

# msg = input('Введите дату отчета в формате гггг-мм-дд: ')
# sock.send(msg.encode('utf-8'))

# data = sock.recv(10240)
# data = pickle.loads(data, encoding='utf-8')
# print(data)
# data.to_csv('report_df.csv', index=False)
# sock.close()
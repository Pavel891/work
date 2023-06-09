import sys, os, socket, pickle, yaml, logging
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QApplication
from upload_client_gui import Ui_MainWindow

logging.basicConfig(filename='uploader_log.log', format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG, datefmt='%d-%m-%Y %H:%M:%S')

HOST = socket.gethostbyname(socket.gethostname())
TYPE = socket.AF_INET
PROTOCOL = socket.SOCK_STREAM
logging.debug(f'HOST: {HOST} \n\t TYPE: {TYPE} \n\t PROTOCOL: {PROTOCOL}')

class Gui(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.download_folder = None
        self.client_id = 'uploader'
        self.ui.pushButton.clicked.connect(self.get_folder)
        self.ui.pushButton_2.clicked.connect(self.send_msg)
        self.conf = self.cfg()
    
    def cfg(self):
        with open('cfg_up.yaml', 'r') as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            logging.debug(f'Config: {conf}')
        return conf

    # выбор даты выгружаемого отчета (работает)
    def calendar_date(self):
        date = self.ui.calendarWidget.selectedDate()
        string_date = str(date.toPyDate())
        logging.debug(f'Calendar date: {string_date}')
        return string_date

    # открывается проводник для выбора папки в которую будет сохраняться файл (работает)
    def get_folder(self):
        try:
            self.download_folder = QtWidgets.QFileDialog.getExistingDirectory(self, 'Выберете папку для сохранения')
            self.download_folder = self.download_folder
        except Exception as e:
            logging.warning(f'Exception in get folder: {e}', exc_info=True)

    # отправка даты на сервер через сокет (работает)
    def send_msg(self):
        self.sock = socket.socket(TYPE, PROTOCOL)
        self.sock.connect((HOST, self.conf.get('port')))

        msg = (self.calendar_date(), self.client_id)
        msg_send = pickle.dumps(msg)
        self.sock.sendall(msg_send)

        data = self.sock.recv(self.conf.get('buf_size'))
        try:
            data = pickle.loads(data, encoding='utf-8')
            logging.info(f'Dataset received!')
            try:
                target_fld = self.download_folder
                logging.debug(f'Target folder: {target_fld}')
                try:
                    data.to_csv(os.path.join(str(target_fld), f'report_df_{msg[0]}.csv'), index=False)
                    logging.info(f'Dataset on {self.calendar_date()} is saved!')
                except AttributeError as ae:
                    logging.warning(f'Exception in upload: {ae}', exc_info=True)
            except Exception as e:
                logging.warning(f'Exception in target folder: {e}', exc_info=True)
        except EOFError:
            logging.warning('Данные на эту дату еще не сформированы.', exc_info=True)
        finally:
            self.sock.close()
            logging.info('Socket is closed!')
            logging.info('*' * 50)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = Gui()
    win.show()
    sys.exit(app.exec())


import sys, socket, pickle, itertools, yaml, logging
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QApplication
from server_client_gui import Ui_MainWindow

logging.basicConfig(filename='station_log.log', format='%(asctime)s - %(levelname)s - %(message)s',
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

        self.client_id = None
        self.passed = None
        self.cash_sel = None
        self.nocash_sel = None

        self.ui.pushButton.clicked.connect(self.send_msg)
        self.conf = self.cfg()
    
    def cfg(self):
        with open('cfg_st.yaml', 'r') as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            logging.debug(f'Config: {conf}')
        return conf

    def on_rb_toggled(self):
        if self.ui.radioButton_1.isChecked():
            self.client_id = 'prospect_report'
        elif self.ui.radioButton_2.isChecked():
            self.client_id = 'uralmash_report'
        elif self.ui.radioButton_3.isChecked():
            self.client_id = 'mash_report'
        elif self.ui.radioButton_4.isChecked():
            self.client_id = 'uralskay_report'
        elif self.ui.radioButton_5.isChecked():
            self.client_id = 'dinamo_report'
        elif self.ui.radioButton_6.isChecked():
            self.client_id = 'place_report'
        elif self.ui.radioButton_7.isChecked():
            self.client_id = 'geolog_report'
        elif self.ui.radioButton_8.isChecked():
            self.client_id = 'chkalov_report'
        elif self.ui.radioButton_9.isChecked():
            self.client_id = 'botan_report'
        logging.debug(f'Client ID: {self.client_id}')
        return self.client_id

    def calendar_date(self):
        date = self.ui.calendarWidget.selectedDate()
        string_date = str(date.toPyDate())
        logging.debug(f'Calendar date: {string_date}')
        return string_date

    def manual_input_data(self):
        self.passed = self.ui.spinBox.value()
        self.cash_sel = self.ui.spinBox_2.value()
        self.nocash_sel = self.ui.spinBox_3.value()
        logging.debug(f'Manual control: {self.passed} \n\t Tokens for cash: {self.cash_sel} \n\t Tokens for non-cash: {self.nocash_sel}')
        return self.passed, self.cash_sel, self.nocash_sel

    def send_msg(self):
        self.sock = socket.socket(TYPE, PROTOCOL)
        self.sock.connect((HOST, self.conf.get('port')))

        msg = list(itertools.chain(self.manual_input_data()))
        msg = [str(v) for v in msg]
        msg.insert(0, self.on_rb_toggled()) # type: ignore
        msg.insert(0, self.calendar_date())
        logging.debug(f'Message to sent: {msg}')
        msg = pickle.dumps(msg)
        self.sock.sendall(msg)
        logging.info('The message has been sent!')

        # answer = self.sock.recv(self.conf.get('buf_size'))
        # answer = pickle.loads(answer, encoding='utf-8')
        # logging.debug(f'Received message: {answer}')
        self.sock.close()
        logging.info('Socket is closed!')
        logging.info('*' * 50)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = Gui()
    win.show()
    sys.exit(app.exec())
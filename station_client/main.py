"""Client application for use at stations.

The application is used to transfer information to the database that 
cannot be processed automatically and requires manual input.

Connects to the server using the TCP protocol of the socket library.

Important application operations are recorded in the log file - station_log.log.

The socket configuration parameters that can be changed are in the cfg_st.yaml file.
"""
import sys, socket, pickle, itertools, yaml, logging
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QApplication, QMessageBox
from station_client_gui import Ui_MainWindow

logging.basicConfig(filename='station_log.log', format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG, datefmt='%d-%m-%Y %H:%M:%S')

HOST = socket.gethostbyname(socket.gethostname())
TYPE = socket.AF_INET
PROTOCOL = socket.SOCK_STREAM
logging.debug(f'HOST: {HOST} \n\t TYPE: {TYPE} \n\t PROTOCOL: {PROTOCOL}')

class Gui(QtWidgets.QMainWindow):
    """
    The class displays the user interface.
    ...
    Attributes
    -----------

    self.client_id : str
        Takes a value depending on the activated radio button in 
        the def on_rb_toggled() method

    self.passed : int
        Takes a quantitative indicator from the self.ui.spinBox.value() 
        method manual_input_data()

    self.cash_sel : int
        Takes a quantitative indicator from the self.ui.spinBox_2.value() 
        method manual_input_data()

    self.nochash_sel : int
        Takes a quantitative indicator from the self.ui.spinBox_3.value() 
        method manual_input_data()

    self.d_msg : str
        Predefined messages for the information window

    self.conf : tuple
        Configuration parameters from the cfg_st.yaml file

    Methods
    -----------

    cfg(self)
        Processes the configuration file for subsequent use of 
        the parameters contained in it

    on_rb_toggled(self)
        Provides interaction with radio buttons

    calendar_date(self)
        Provides interaction with the calendar

    manual_input_data(self)
        Provides interaction with input fields

    send_msg(self)
        Checks the data previously entered by the user, forms a list of them 
        and sends a request to the server.

    mb(self, d_msg)
        Calls the information window
    """
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.client_id = None
        self.passed = None
        self.cash_sel = None
        self.nocash_sel = None
        self.d_msg = None

        self.ui.pushButton.clicked.connect(self.send_msg)
        self.conf = self.cfg()
    
    def cfg(self):
        """
        Returns
        --------
        tuple
            a configuration parameters
        """
        with open('cfg_st.yaml', 'r') as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)
            logging.debug(f'Config: {conf}')
        return conf

    def on_rb_toggled(self):
        """
        Returns
        --------
        str
            id worked station
        """
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
        """
        Returns
        --------
        str
            selected date
        """
        date = self.ui.calendarWidget.selectedDate()
        string_date = str(date.toPyDate())
        logging.debug(f'Calendar date: {string_date}')
        return string_date

    def manual_input_data(self):
        """
        Returns
        --------
        int
            quantitative parameters with manual input
        """
        self.passed = self.ui.spinBox.value()
        self.cash_sel = self.ui.spinBox_2.value()
        self.nocash_sel = self.ui.spinBox_3.value()
        logging.debug(f'Manual control: {self.passed} \n\t \
                        Tokens for cash: {self.cash_sel} \n\t \
                        Tokens for non-cash: {self.nocash_sel}')
        return self.passed, self.cash_sel, self.nocash_sel

    def send_msg(self):
        while True:
            try:
                self.sock = socket.socket(TYPE, PROTOCOL)
                self.sock.connect((HOST, self.conf.get('port')))
            except Exception as e:
                logging.warning(f'Exception in upload: {e}', exc_info=True)
                self.d_msg = 'Нет соединения с сервером!'
                self.mb(self.d_msg)
                break
            else:
                msg = list(itertools.chain(self.manual_input_data()))
                msg = [str(v) for v in msg]
                msg.insert(0, self.on_rb_toggled()) # type: ignore
                msg.insert(0, self.calendar_date())
                logging.debug(f'Message to sent: {msg}')
                if msg[1] == None:
                    self.d_msg = 'Станция не выбрана!'
                    self.mb(self.d_msg)
                    continue
                else:
                    msg = pickle.dumps(msg)
                    self.sock.sendall(msg)
                    logging.info('The message has been sent!')

                answer = self.sock.recv(self.conf.get('buf_size'))
                answer = pickle.loads(answer, encoding='utf-8')
                self.mb(answer)
                logging.debug(f'Received message: {answer}')
                break
            finally:
                self.sock.close()
                logging.info('Socket is closed!')
                break
        logging.info('*' * 50)

    def mb(self, d_msg):
        """Generates an information window.

            Parameter
            -----------
            d_msg : str, required
                    Text to display in the information window.
        """
        dlg = QMessageBox(self)
        dlg.setWindowTitle('Внимание!')
        dlg.setText(d_msg)
        dlg.setIcon(QMessageBox.Icon.Information)
        dlg.exec()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = Gui()
    win.show()
    sys.exit(app.exec())
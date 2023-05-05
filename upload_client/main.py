import sys
import os
import socket
import pickle
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QApplication
from upload_client_gui import Ui_MainWindow

class Gui(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('localhost', 9090))

        self.download_folder = None
        self.ui.pushButton.clicked.connect(self.get_folder)
        self.ui.pushButton_2.clicked.connect(self.send_msg)

    # выбор даты выгружаемого отчета (работает)
    def calendar_date(self):
        date = self.ui.calendarWidget.selectedDate()
        string_date = str(date.toPyDate())
        # print(string_date)
        return string_date

    # открывается проводник для выбора папки в которую будет сохраняться файл (работает)
    def get_folder(self):
        try:
            self.download_folder = QtWidgets.QFileDialog.getExistingDirectory(self, 'Выберете папку для сохранения')
            self.download_folder = self.download_folder
            # print(self.download_folder)
        except:
            pass

    # отправка даты на сервер через сокет (работает)
    def send_msg(self):
        msg = self.calendar_date()
        self.sock.send(msg.encode('utf-8'))

        data = self.sock.recv(10240)
        data = pickle.loads(data, encoding='utf-8')
        # print(data)
        try:
            target_fld = self.download_folder
            # print(target_fld)
            data.to_csv(os.path.join(str(target_fld), f'report_df_{msg}.csv'), index=False)
            # print('file upload')
            self.sock.close()
        except Exception as e:
            print(e)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = Gui()
    win.show()
    sys.exit(app.exec())

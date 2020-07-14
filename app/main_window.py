import random

import toml
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QFileDialog, QDialog, QApplication

from util.HDFS import get_file_from_hdfs, copy_file_to_hdfs
from util.SSH import execute, execute_and_write
from config import config


class MainWindow(QDialog):
    output_location_in_hdfs = None

    def __init__(self, dialog):
        super().__init__(dialog)
        dialog.resize(1007, 630)
        size_policy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        size_policy.setHorizontalStretch(0)
        size_policy.setVerticalStretch(0)
        size_policy.setHeightForWidth(dialog.sizePolicy().hasHeightForWidth())
        dialog.setSizePolicy(size_policy)
        dialog.setMinimumSize(QtCore.QSize(1007, 630))
        dialog.setMaximumSize(QtCore.QSize(1007, 630))
        self.centralwidget = QtWidgets.QWidget(dialog)
        self.widget = QtWidgets.QWidget(self.centralwidget)
        self.widget.setGeometry(QtCore.QRect(21, 29, 962, 558))
        self.verticalLayout_8 = QtWidgets.QVBoxLayout(self.widget)
        self.verticalLayout_8.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_6 = QtWidgets.QVBoxLayout()
        self.horizontalLayout_8 = QtWidgets.QHBoxLayout()
        self.DatasetQLabel = QtWidgets.QLabel(self.widget)
        self.DatasetQLabel.setMinimumSize(QtCore.QSize(90, 0))
        self.DatasetQLabel.setMaximumSize(QtCore.QSize(90, 95545))
        font16_not_bold = QtGui.QFont()
        font16_not_bold.setPointSize(16)
        font16_not_bold.setBold(False)
        font16_not_bold.setWeight(50)
        self.DatasetQLabel.setFont(font16_not_bold)
        self.DatasetQLabel.setAlignment(QtCore.Qt.AlignCenter)
        self.horizontalLayout_8.addWidget(self.DatasetQLabel)
        self.DatasetPathQLineEdit = QtWidgets.QLineEdit(self.widget)
        self.DatasetPathQLineEdit.setMinimumSize(QtCore.QSize(300, 30))
        self.DatasetPathQLineEdit.setFont(font16_not_bold)
        self.horizontalLayout_8.addWidget(self.DatasetPathQLineEdit)
        self.DatasetBrowseQPushButton = QtWidgets.QPushButton(self.widget)
        self.DatasetBrowseQPushButton.setMinimumSize(QtCore.QSize(120, 30))
        self.DatasetBrowseQPushButton.setMaximumSize(QtCore.QSize(120, 30))
        font16 = QtGui.QFont()
        font16.setPointSize(16)
        self.DatasetBrowseQPushButton.setFont(font16)
        self.horizontalLayout_8.addWidget(self.DatasetBrowseQPushButton)
        self.verticalLayout_6.addLayout(self.horizontalLayout_8)
        self.horizontalLayout_9 = QtWidgets.QHBoxLayout()
        self.JarQLabel = QtWidgets.QLabel(self.widget)
        self.JarQLabel.setMinimumSize(QtCore.QSize(90, 0))
        self.JarQLabel.setMaximumSize(QtCore.QSize(90, 95545))
        self.JarQLabel.setFont(font16_not_bold)
        self.JarQLabel.setAlignment(QtCore.Qt.AlignCenter)
        self.horizontalLayout_9.addWidget(self.JarQLabel)
        self.JarPathQLineEdit = QtWidgets.QLineEdit(self.widget)
        self.JarPathQLineEdit.setMinimumSize(QtCore.QSize(300, 30))
        self.JarPathQLineEdit.setFont(font16_not_bold)
        self.horizontalLayout_9.addWidget(self.JarPathQLineEdit)
        self.JarBrowseQPushButton = QtWidgets.QPushButton(self.widget)
        self.JarBrowseQPushButton.setMinimumSize(QtCore.QSize(120, 30))
        self.JarBrowseQPushButton.setMaximumSize(QtCore.QSize(120, 30))
        self.JarBrowseQPushButton.setFont(font16)
        self.horizontalLayout_9.addWidget(self.JarBrowseQPushButton)
        self.verticalLayout_6.addLayout(self.horizontalLayout_9)
        self.verticalLayout_8.addLayout(self.verticalLayout_6)
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.FunctionsQGroupBox = QtWidgets.QGroupBox(self.widget)
        font14 = QtGui.QFont()
        font14.setPointSize(14)
        self.FunctionsQGroupBox.setFont(font14)
        self.FunctionsQGroupBox.setTitle("")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout(self.FunctionsQGroupBox)
        self.verticalLayout = QtWidgets.QVBoxLayout()
        self.horizontalLayout_3 = QtWidgets.QHBoxLayout()
        self.label_6 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_6.setFont(font14)
        self.horizontalLayout_3.addWidget(self.label_6)
        self.label = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label.setEnabled(True)
        self.label.setMinimumSize(QtCore.QSize(431, 0))
        self.label.setMaximumSize(QtCore.QSize(431, 16777215))
        font12 = QtGui.QFont()
        font12.setPointSize(12)
        self.label.setFont(font12)
        self.horizontalLayout_3.addWidget(self.label)
        self.verticalLayout.addLayout(self.horizontalLayout_3)
        self.horizontalLayout_4 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_4.setObjectName("horizontalLayout_4")
        self.label_7 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_7.setFont(font14)
        self.horizontalLayout_4.addWidget(self.label_7)
        self.label_2 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_2.setMinimumSize(QtCore.QSize(431, 0))
        self.label_2.setMaximumSize(QtCore.QSize(431, 16777215))
        self.label_2.setFont(font12)
        self.horizontalLayout_4.addWidget(self.label_2)
        self.verticalLayout.addLayout(self.horizontalLayout_4)
        self.horizontalLayout_5 = QtWidgets.QHBoxLayout()
        self.label_8 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_8.setFont(font14)
        self.horizontalLayout_5.addWidget(self.label_8)
        self.label_3 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_3.setMinimumSize(QtCore.QSize(401, 0))
        self.label_3.setMaximumSize(QtCore.QSize(401, 16777215))
        self.label_3.setFont(font12)
        self.horizontalLayout_5.addWidget(self.label_3)
        self.verticalLayout.addLayout(self.horizontalLayout_5)
        self.horizontalLayout_6 = QtWidgets.QHBoxLayout()
        self.label_9 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_9.setFont(font14)
        self.horizontalLayout_6.addWidget(self.label_9)
        self.label_4 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_4.setMinimumSize(QtCore.QSize(431, 0))
        self.label_4.setMaximumSize(QtCore.QSize(445, 16777215))
        self.label_4.setFont(font12)
        self.horizontalLayout_6.addWidget(self.label_4)
        self.verticalLayout.addLayout(self.horizontalLayout_6)
        self.horizontalLayout_7 = QtWidgets.QHBoxLayout()
        self.label_10 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_10.setFont(font14)
        self.horizontalLayout_7.addWidget(self.label_10)
        self.label_5 = QtWidgets.QLabel(self.FunctionsQGroupBox)
        self.label_5.setMinimumSize(QtCore.QSize(381, 0))
        self.label_5.setMaximumSize(QtCore.QSize(381, 16777215))
        self.label_5.setFont(font12)
        self.horizontalLayout_7.addWidget(self.label_5)
        self.verticalLayout.addLayout(self.horizontalLayout_7)
        self.verticalLayout_2.addLayout(self.verticalLayout)
        self.horizontalLayout.addWidget(self.FunctionsQGroupBox)
        self.verticalLayout_5 = QtWidgets.QVBoxLayout()
        self.groupBox = QtWidgets.QGroupBox(self.widget)
        self.groupBox.setFont(font14)
        self.verticalLayout_4 = QtWidgets.QVBoxLayout(self.groupBox)
        self.verticalLayout_3 = QtWidgets.QVBoxLayout()
        self.LiveNodeCountQLabel = QtWidgets.QLabel(self.groupBox)
        self.LiveNodeCountQLabel.setFont(font14)
        self.verticalLayout_3.addWidget(self.LiveNodeCountQLabel)
        self.MasterLabelQLabel = QtWidgets.QLabel(self.groupBox)
        self.MasterLabelQLabel.setFont(font14)
        self.verticalLayout_3.addWidget(self.MasterLabelQLabel)
        self.verticalLayout_4.addLayout(self.verticalLayout_3)
        self.verticalLayout_5.addWidget(self.groupBox)
        self.groupBox_3 = QtWidgets.QGroupBox(self.widget)
        self.groupBox_3.setMinimumSize(QtCore.QSize(221, 0))
        self.groupBox_3.setMaximumSize(QtCore.QSize(5454, 16777215))
        self.groupBox_3.setFont(font14)
        self.horizontalLayout_10 = QtWidgets.QHBoxLayout(self.groupBox_3)
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.OutputFolderPathQLineEdit = QtWidgets.QLineEdit(self.groupBox_3)
        self.OutputFolderPathQLineEdit.setMinimumSize(QtCore.QSize(300, 30))
        self.OutputFolderPathQLineEdit.setFont(font16_not_bold)
        self.horizontalLayout_2.addWidget(self.OutputFolderPathQLineEdit)
        self.OutputFolderBrowseQPushButton = QtWidgets.QPushButton(self.groupBox_3)
        self.OutputFolderBrowseQPushButton.setMinimumSize(QtCore.QSize(120, 30))
        self.OutputFolderBrowseQPushButton.setMaximumSize(QtCore.QSize(120, 30))
        self.OutputFolderBrowseQPushButton.setFont(font16)
        self.horizontalLayout_2.addWidget(self.OutputFolderBrowseQPushButton)
        self.horizontalLayout_10.addLayout(self.horizontalLayout_2)
        self.verticalLayout_5.addWidget(self.groupBox_3)
        self.horizontalLayout.addLayout(self.verticalLayout_5)
        self.verticalLayout_8.addLayout(self.horizontalLayout)
        self.verticalLayout_7 = QtWidgets.QVBoxLayout()
        self.horizontalLayout_11 = QtWidgets.QHBoxLayout()
        self.StartServicesQPushButton = QtWidgets.QPushButton(self.widget)
        self.StartServicesQPushButton.setMinimumSize(QtCore.QSize(201, 30))
        self.StartServicesQPushButton.setMaximumSize(QtCore.QSize(201, 30))
        font14_not_bold = QtGui.QFont()
        font14_not_bold.setPointSize(14)
        font14_not_bold.setBold(False)
        font14_not_bold.setWeight(50)
        self.StartServicesQPushButton.setFont(font14_not_bold)
        self.horizontalLayout_11.addWidget(self.StartServicesQPushButton)
        self.StopServicesQPushButton = QtWidgets.QPushButton(self.widget)
        self.StopServicesQPushButton.setMinimumSize(QtCore.QSize(201, 30))
        self.StopServicesQPushButton.setMaximumSize(QtCore.QSize(201, 30))
        self.StopServicesQPushButton.setFont(font14_not_bold)
        self.StopServicesQPushButton.setObjectName("StopServicesQPushButton")
        self.horizontalLayout_11.addWidget(self.StopServicesQPushButton)
        self.horizontalLayout_11.addItem(QtWidgets.QSpacerItem(288, 20, QtWidgets.QSizePolicy.Expanding,
                                                               QtWidgets.QSizePolicy.Minimum))
        self.StartQPushButton = QtWidgets.QPushButton(self.widget)
        self.StartQPushButton.setMinimumSize(QtCore.QSize(201, 30))
        self.StartQPushButton.setMaximumSize(QtCore.QSize(201, 30))
        self.StartQPushButton.setFont(font14_not_bold)
        self.horizontalLayout_11.addWidget(self.StartQPushButton)
        self.verticalLayout_7.addLayout(self.horizontalLayout_11)
        self.LogQTextEdt = QtWidgets.QTextEdit(self.widget)
        self.LogQTextEdt.setMinimumSize(QtCore.QSize(750, 250))
        self.LogQTextEdt.setMaximumSize(QtCore.QSize(4564, 250))
        font9 = QtGui.QFont()
        font9.setPointSize(9)
        self.LogQTextEdt.setFont(font9)
        self.verticalLayout_7.addWidget(self.LogQTextEdt)
        self.verticalLayout_8.addLayout(self.verticalLayout_7)
        dialog.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(dialog)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 1007, 21))
        dialog.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(dialog)
        dialog.setStatusBar(self.statusbar)

        dialog.setWindowTitle("MainWindow")
        self.DatasetQLabel.setText("Dataset")
        self.DatasetBrowseQPushButton.setText("Browse")
        self.JarQLabel.setText("Jar")
        self.JarBrowseQPushButton.setText("Browse")
        self.label_6.setText("Sum")
        self.label.setText("(review count of each product)")
        self.label_7.setText("Avg")
        self.label_2.setText("(average rating score of each product)")
        self.label_8.setText("Min-Max")
        self.label_3.setText("(the most and least helpful comments)")
        self.label_9.setText("Std")
        self.label_4.setText("(standard deviation of each product scores)")
        self.label_10.setText("Correlation")
        self.label_5.setText("(pearson correlation of helpfulness_ration and scores)")
        self.groupBox.setTitle("Cluster Status")
        self.LiveNodeCountQLabel.setText("Live nodes")
        self.MasterLabelQLabel.setText("Master IP address=")
        self.groupBox_3.setTitle("Output Folder")
        self.OutputFolderBrowseQPushButton.setText("Browse")
        self.StartServicesQPushButton.setText("Start Services")
        self.StopServicesQPushButton.setText("Stop Services")
        self.StartQPushButton.setText("Start")

        self.is_alive = False

        self.DatasetBrowseQPushButton.clicked.connect(lambda: self.browse_file("dataset"))
        self.JarBrowseQPushButton.clicked.connect(lambda: self.browse_file("jar"))
        self.OutputFolderBrowseQPushButton.clicked.connect(lambda: self.browse_file("output"))
        self.StartQPushButton.clicked.connect(self.run)
        self.StartServicesQPushButton.clicked.connect(self.start_services)
        self.StopServicesQPushButton.clicked.connect(self.stop_services)

        QtCore.QMetaObject.connectSlotsByName(dialog)

    def init_widgets(self):
        # Live node count
        command = config.hadoop_home_path + "/bin/hdfs dfsadmin -report"
        result = execute(command)
        matched_lines = [line for line in result.split('\n') if "Live datanodes" in line]
        live_node_count = matched_lines[0].split(':')[0]

        # Master IP address
        command = "cat /etc/hosts"
        result = execute(command)
        matched_lines = [line for line in result.split('\n') if "master" in line]
        master_ip_address = matched_lines[0].split(' ')[0]

        self.LiveNodeCountQLabel.setText(live_node_count)
        self.MasterLabelQLabel.setText("Master IP address=" + master_ip_address)

    def browse_file(self, id):
        if id == "dataset":
            filename = QFileDialog.getOpenFileName(self, 'Open File', '/home/osemrt/Desktop')[0]
            self.DatasetPathQLineEdit.setText(filename)
        elif id == "jar":
            filename = QFileDialog.getOpenFileName(self, 'Open File', '/home/osemrt/Desktop', '*.jar')[0]
            self.JarPathQLineEdit.setText(filename)
        elif id == "output":
            dialog = QFileDialog()
            foo_dir = dialog.getExistingDirectory(self, 'Select  directory')
            self.OutputFolderPathQLineEdit.setText(foo_dir)

    def run(self):
        if not self.is_alive:
            self.LogQTextEdt.setText(self.LogQTextEdt.toPlainText() + "\nPlease, start services first!\n")
        else:
            self.change_button_text("Stop")
            start_command = self.get_start_command()
            execute_and_write(self.LogQTextEdt, start_command)
            self.get_file_from_hdfs()
            self.change_button_text("Start")

        QApplication.processEvents()

    def get_start_command(self):
        jar_path = self.JarPathQLineEdit.text()
        dataset_path = self.DatasetPathQLineEdit.text()
        filename_in_hdfs = str(random.randint(0, 1000000))

        dataset_filename = dataset_path.split('/')[-1]
        copy_file_to_hdfs(dataset_path, "/app/dataset/")
        self.output_location_in_hdfs = "/app/outputs/" + filename_in_hdfs
        return config.hadoop_home_path + "/bin/hadoop" + " jar " + jar_path + " " + \
               "/app/dataset/" + dataset_filename + " " + self.output_location_in_hdfs

    def get_file_from_hdfs(self):
        save_to = self.OutputFolderPathQLineEdit.text()
        get_file_from_hdfs(self.output_location_in_hdfs, save_to)

    def start_services(self):
        self.StartServicesQPushButton.setText("Stop")
        execute_and_write(self.LogQTextEdt, config.hadoop_home_path + '/sbin/start-all.sh')
        # self.ssh.execute_and_write(self.LogQTextEdt, variables.HADOOP_HOME_PATH + '/sbin/start-dfs.sh')
        # self.ssh.execute_and_write(self.LogQTextEdt, variables.HADOOP_HOME_PATH + '/sbin/start-yarn.sh')
        self.is_alive = True
        self.StartServicesQPushButton.setText("Start Services")
        self.init_widgets()

    def stop_services(self):
        self.StopServicesQPushButton.setText("Stop")
        execute_and_write(self.LogQTextEdt, config.hadoop_home_path + '/sbin/stop-all.sh')
        self.StopServicesQPushButton.setText("Stop Services")
        self.LiveNodeCountQLabel.setText("Live nodes")
        self.MasterLabelQLabel.setText("Master IP address=")

    def change_button_text(self, text):
        self.StartQPushButton.setText(text)


if __name__ == "__main__":
    import sys

    app = QtWidgets.QApplication(sys.argv)
    main_window = QtWidgets.QMainWindow()
    ui = MainWindow(main_window)
    main_window.show()
    sys.exit(app.exec_())

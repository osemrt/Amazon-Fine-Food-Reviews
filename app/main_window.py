import random

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QFileDialog, QDialog, QApplication

from config import config
from util.HDFS import get_file_from_hdfs, copy_file_to_hdfs
from util.SSH import execute, execute_and_write


class MainWindow(QDialog):
    output_location_in_hdfs = None

    def __init__(self, main_window):
        self.main_window = main_window
        super().__init__(self.main_window)
        self.main_window.resize(1007, 630)
        size_policy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        size_policy.setHorizontalStretch(0)
        size_policy.setVerticalStretch(0)
        size_policy.setHeightForWidth(self.main_window.sizePolicy().hasHeightForWidth())
        self.main_window.setSizePolicy(size_policy)
        self.main_window.setMinimumSize(QtCore.QSize(1007, 630))
        self.main_window.setMaximumSize(QtCore.QSize(1007, 630))
        central_widget = QtWidgets.QWidget(self.main_window)
        widget = QtWidgets.QWidget(central_widget)
        widget.setGeometry(QtCore.QRect(21, 29, 962, 558))
        vertical_layout8 = QtWidgets.QVBoxLayout(widget)
        vertical_layout8.setContentsMargins(0, 0, 0, 0)
        vertical_layout6 = QtWidgets.QVBoxLayout()
        horizontal_layout8 = QtWidgets.QHBoxLayout()
        dataset_label = QtWidgets.QLabel(widget)
        dataset_label.setMinimumSize(QtCore.QSize(90, 0))
        dataset_label.setMaximumSize(QtCore.QSize(90, 95545))
        font16_not_bold = QtGui.QFont()
        font16_not_bold.setPointSize(16)
        font16_not_bold.setBold(False)
        font16_not_bold.setWeight(50)
        dataset_label.setFont(font16_not_bold)
        dataset_label.setAlignment(QtCore.Qt.AlignCenter)
        horizontal_layout8.addWidget(dataset_label)
        self.dataset_path_line_edit = QtWidgets.QLineEdit(widget)
        self.dataset_path_line_edit.setMinimumSize(QtCore.QSize(300, 30))
        self.dataset_path_line_edit.setFont(font16_not_bold)
        horizontal_layout8.addWidget(self.dataset_path_line_edit)
        dataset_browse_button = QtWidgets.QPushButton(widget)
        dataset_browse_button.setMinimumSize(QtCore.QSize(120, 30))
        dataset_browse_button.setMaximumSize(QtCore.QSize(120, 30))
        font16 = QtGui.QFont()
        font16.setPointSize(16)
        dataset_browse_button.setFont(font16)
        horizontal_layout8.addWidget(dataset_browse_button)
        vertical_layout6.addLayout(horizontal_layout8)
        horizontal_layout9 = QtWidgets.QHBoxLayout()
        jar_label = QtWidgets.QLabel(widget)
        jar_label.setMinimumSize(QtCore.QSize(90, 0))
        jar_label.setMaximumSize(QtCore.QSize(90, 95545))
        jar_label.setFont(font16_not_bold)
        jar_label.setAlignment(QtCore.Qt.AlignCenter)
        horizontal_layout9.addWidget(jar_label)
        self.jar_path_line_edit = QtWidgets.QLineEdit(widget)
        self.jar_path_line_edit.setMinimumSize(QtCore.QSize(300, 30))
        self.jar_path_line_edit.setFont(font16_not_bold)
        horizontal_layout9.addWidget(self.jar_path_line_edit)
        jar_browse_push_button = QtWidgets.QPushButton(widget)
        jar_browse_push_button.setMinimumSize(QtCore.QSize(120, 30))
        jar_browse_push_button.setMaximumSize(QtCore.QSize(120, 30))
        jar_browse_push_button.setFont(font16)
        horizontal_layout9.addWidget(jar_browse_push_button)
        vertical_layout6.addLayout(horizontal_layout9)
        vertical_layout8.addLayout(vertical_layout6)
        horizontal_layout = QtWidgets.QHBoxLayout()
        functions_group_box = QtWidgets.QGroupBox(widget)
        font14 = QtGui.QFont()
        font14.setPointSize(14)
        functions_group_box.setFont(font14)
        functions_group_box.setTitle("")
        vertical_layout2 = QtWidgets.QVBoxLayout(functions_group_box)
        vertical_layout = QtWidgets.QVBoxLayout()
        horizontal_layout3 = QtWidgets.QHBoxLayout()
        label6 = QtWidgets.QLabel(functions_group_box)
        label6.setFont(font14)
        horizontal_layout3.addWidget(label6)
        label = QtWidgets.QLabel(functions_group_box)
        label.setEnabled(True)
        label.setMinimumSize(QtCore.QSize(431, 0))
        label.setMaximumSize(QtCore.QSize(431, 16777215))
        font12 = QtGui.QFont()
        font12.setPointSize(12)
        label.setFont(font12)
        horizontal_layout3.addWidget(label)
        vertical_layout.addLayout(horizontal_layout3)
        horizontal_layout4 = QtWidgets.QHBoxLayout()
        horizontal_layout4.setObjectName("horizontalLayout_4")
        label7 = QtWidgets.QLabel(functions_group_box)
        label7.setFont(font14)
        horizontal_layout4.addWidget(label7)
        label2 = QtWidgets.QLabel(functions_group_box)
        label2.setMinimumSize(QtCore.QSize(431, 0))
        label2.setMaximumSize(QtCore.QSize(431, 16777215))
        label2.setFont(font12)
        horizontal_layout4.addWidget(label2)
        vertical_layout.addLayout(horizontal_layout4)
        horizontal_layout5 = QtWidgets.QHBoxLayout()
        label8 = QtWidgets.QLabel(functions_group_box)
        label8.setFont(font14)
        horizontal_layout5.addWidget(label8)
        label3 = QtWidgets.QLabel(functions_group_box)
        label3.setMinimumSize(QtCore.QSize(401, 0))
        label3.setMaximumSize(QtCore.QSize(401, 16777215))
        label3.setFont(font12)
        horizontal_layout5.addWidget(label3)
        vertical_layout.addLayout(horizontal_layout5)
        horizontal_layout6 = QtWidgets.QHBoxLayout()
        label9 = QtWidgets.QLabel(functions_group_box)
        label9.setFont(font14)
        horizontal_layout6.addWidget(label9)
        label4 = QtWidgets.QLabel(functions_group_box)
        label4.setMinimumSize(QtCore.QSize(431, 0))
        label4.setMaximumSize(QtCore.QSize(445, 16777215))
        label4.setFont(font12)
        horizontal_layout6.addWidget(label4)
        vertical_layout.addLayout(horizontal_layout6)
        horizontal_layout7 = QtWidgets.QHBoxLayout()
        label10 = QtWidgets.QLabel(functions_group_box)
        label10.setFont(font14)
        horizontal_layout7.addWidget(label10)
        label5 = QtWidgets.QLabel(functions_group_box)
        label5.setMinimumSize(QtCore.QSize(381, 0))
        label5.setMaximumSize(QtCore.QSize(381, 16777215))
        label5.setFont(font12)
        horizontal_layout7.addWidget(label5)
        vertical_layout.addLayout(horizontal_layout7)
        vertical_layout2.addLayout(vertical_layout)
        horizontal_layout.addWidget(functions_group_box)
        vertical_layout5 = QtWidgets.QVBoxLayout()
        group_box = QtWidgets.QGroupBox(widget)
        group_box.setFont(font14)
        vertical_layout4 = QtWidgets.QVBoxLayout(group_box)
        vertical_layout3 = QtWidgets.QVBoxLayout()
        self.live_node_count_label = QtWidgets.QLabel(group_box)
        self.live_node_count_label.setFont(font14)
        vertical_layout3.addWidget(self.live_node_count_label)
        self.master_label = QtWidgets.QLabel(group_box)
        self.master_label.setFont(font14)
        vertical_layout3.addWidget(self.master_label)
        vertical_layout4.addLayout(vertical_layout3)
        vertical_layout5.addWidget(group_box)
        group_box3 = QtWidgets.QGroupBox(widget)
        group_box3.setMinimumSize(QtCore.QSize(221, 0))
        group_box3.setMaximumSize(QtCore.QSize(5454, 16777215))
        group_box3.setFont(font14)
        horizontal_layout10 = QtWidgets.QHBoxLayout(group_box3)
        horizontal_layout2 = QtWidgets.QHBoxLayout()
        self.output_folder_path_line_edit = QtWidgets.QLineEdit(group_box3)
        self.output_folder_path_line_edit.setMinimumSize(QtCore.QSize(300, 30))
        self.output_folder_path_line_edit.setFont(font16_not_bold)
        horizontal_layout2.addWidget(self.output_folder_path_line_edit)
        output_folder_browse_button = QtWidgets.QPushButton(group_box3)
        output_folder_browse_button.setMinimumSize(QtCore.QSize(120, 30))
        output_folder_browse_button.setMaximumSize(QtCore.QSize(120, 30))
        output_folder_browse_button.setFont(font16)
        horizontal_layout2.addWidget(output_folder_browse_button)
        horizontal_layout10.addLayout(horizontal_layout2)
        vertical_layout5.addWidget(group_box3)
        horizontal_layout.addLayout(vertical_layout5)
        vertical_layout8.addLayout(horizontal_layout)
        vertical_layout7 = QtWidgets.QVBoxLayout()
        horizontal_layout11 = QtWidgets.QHBoxLayout()
        self.start_services_button = QtWidgets.QPushButton(widget)
        self.start_services_button.setMinimumSize(QtCore.QSize(201, 30))
        self.start_services_button.setMaximumSize(QtCore.QSize(201, 30))
        font14_not_bold = QtGui.QFont()
        font14_not_bold.setPointSize(14)
        font14_not_bold.setBold(False)
        font14_not_bold.setWeight(50)
        self.start_services_button.setFont(font14_not_bold)
        horizontal_layout11.addWidget(self.start_services_button)
        self.stop_services_button = QtWidgets.QPushButton(widget)
        self.stop_services_button.setMinimumSize(QtCore.QSize(201, 30))
        self.stop_services_button.setMaximumSize(QtCore.QSize(201, 30))
        self.stop_services_button.setFont(font14_not_bold)
        self.stop_services_button.setObjectName("StopServicesQPushButton")
        horizontal_layout11.addWidget(self.stop_services_button)
        horizontal_layout11.addItem(QtWidgets.QSpacerItem(288, 20, QtWidgets.QSizePolicy.Expanding,
                                                          QtWidgets.QSizePolicy.Minimum))
        self.start_button = QtWidgets.QPushButton(widget)
        self.start_button.setMinimumSize(QtCore.QSize(201, 30))
        self.start_button.setMaximumSize(QtCore.QSize(201, 30))
        self.start_button.setFont(font14_not_bold)
        horizontal_layout11.addWidget(self.start_button)
        vertical_layout7.addLayout(horizontal_layout11)
        self.log_text_area = QtWidgets.QTextEdit(widget)
        self.log_text_area.setMinimumSize(QtCore.QSize(750, 250))
        self.log_text_area.setMaximumSize(QtCore.QSize(4564, 250))
        font9 = QtGui.QFont()
        font9.setPointSize(9)
        self.log_text_area.setFont(font9)
        vertical_layout7.addWidget(self.log_text_area)
        vertical_layout8.addLayout(vertical_layout7)
        self.main_window.setCentralWidget(central_widget)
        menubar = QtWidgets.QMenuBar(self.main_window)
        menubar.setGeometry(QtCore.QRect(0, 0, 1007, 21))
        self.main_window.setMenuBar(menubar)
        status_bar = QtWidgets.QStatusBar(self.main_window)
        self.main_window.setStatusBar(status_bar)

        self.main_window.setWindowTitle("MainWindow")
        dataset_label.setText("Dataset")
        dataset_browse_button.setText("Browse")
        jar_label.setText("Jar")
        jar_browse_push_button.setText("Browse")
        label6.setText("Sum")
        label.setText("(review count of each product)")
        label7.setText("Avg")
        label2.setText("(average rating score of each product)")
        label8.setText("Min-Max")
        label3.setText("(the most and least helpful comments)")
        label9.setText("Std")
        label4.setText("(standard deviation of each product scores)")
        label10.setText("Correlation")
        label5.setText("(pearson correlation of helpfulness_ration and scores)")
        group_box.setTitle("Cluster Status")
        self.live_node_count_label.setText("Live nodes")
        self.master_label.setText("Master IP address=")
        group_box3.setTitle("Output Folder")
        output_folder_browse_button.setText("Browse")
        self.start_services_button.setText("Start Services")
        self.stop_services_button.setText("Stop Services")
        self.start_button.setText("Start")

        self.is_alive = False

        dataset_browse_button.clicked.connect(lambda: self.browse_file("dataset"))
        jar_browse_push_button.clicked.connect(lambda: self.browse_file("jar"))
        output_folder_browse_button.clicked.connect(lambda: self.browse_file("output"))
        self.start_button.clicked.connect(self.run)
        self.start_services_button.clicked.connect(lambda: self.start_services(self.main_window))
        self.stop_services_button.clicked.connect(self.stop_services)

        QtCore.QMetaObject.connectSlotsByName(self.main_window)

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

        self.live_node_count_label.setText(live_node_count)
        self.master_label.setText("Master IP address=" + master_ip_address)

    def browse_file(self, id):
        if id == "dataset":
            filename = QFileDialog.getOpenFileName(self, 'Open File', '/home/osemrt/Desktop')[0]
            self.dataset_path_line_edit.setText(filename)
        elif id == "jar":
            filename = QFileDialog.getOpenFileName(self, 'Open File', '/home/osemrt/Desktop', '*.jar')[0]
            self.jar_path_line_edit.setText(filename)
        elif id == "output":
            dialog = QFileDialog()
            foo_dir = dialog.getExistingDirectory(self, 'Select  directory')
            self.output_folder_path_line_edit.setText(foo_dir)

    def run(self):
        if not self.is_alive:
            self.log_text_area.setText(self.log_text_area.toPlainText() + "\nPlease, start services first!\n")
        else:
            self.change_button_text("Stop")
            start_command = self.get_start_command()
            execute_and_write(self.log_text_area, start_command)
            self.get_file_from_hdfs()
            self.change_button_text("Start")

        QApplication.processEvents()

    def get_start_command(self):
        jar_path = self.jar_path_line_edit.text()
        dataset_path = self.dataset_path_line_edit.text()
        filename_in_hdfs = str(random.randint(0, 1000000))

        dataset_filename = dataset_path.split('/')[-1]
        copy_file_to_hdfs(dataset_path, "/app/dataset/")
        self.output_location_in_hdfs = "/app/outputs/" + filename_in_hdfs
        return config.hadoop_home_path + "/bin/hadoop" + " jar " + jar_path + " " + \
               "/app/dataset/" + dataset_filename + " " + self.output_location_in_hdfs

    def get_file_from_hdfs(self):
        save_to = self.output_folder_path_line_edit.text()
        get_file_from_hdfs(self.output_location_in_hdfs, save_to)

    def start_services(self, dialog):
        self.start_services_button.setText("Stop")
        if not execute_and_write(self.log_text_area, config.hadoop_home_path + '/sbin/start-all.sh'):
            QtWidgets.QMessageBox.warning(dialog, "Error happenned", "Unable to connect to SSH server")
            self.start_services_button.setText("Start Services")
            return
        # self.ssh.execute_and_write(self.LogQTextEdt, variables.HADOOP_HOME_PATH + '/sbin/start-dfs.sh')
        # self.ssh.execute_and_write(self.LogQTextEdt, variables.HADOOP_HOME_PATH + '/sbin/start-yarn.sh')
        self.is_alive = True
        self.start_services_button.setText("Start Services")
        self.init_widgets()

    def stop_services(self):
        self.stop_services_button.setText("Stop")
        execute_and_write(self.log_text_area, config.hadoop_home_path + '/sbin/stop-all.sh')
        self.stop_services_button.setText("Stop Services")
        self.live_node_count_label.setText("Live nodes")
        self.master_label.setText("Master IP address=")

    def change_button_text(self, text):
        self.start_button.setText(text)


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = MainWindow(QtWidgets.QMainWindow())
    ui.main_window.show()
    sys.exit(app.exec_())

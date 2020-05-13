import paramiko
import time

from PyQt5.QtWidgets import QApplication

from config.variables import HOSTNAME
from config.variables import USERNAME
from config.variables import PASSWORD
from config.variables import PORT


class SSH:
    def execute_and_write(self, LogQTextEdt, command):
        print("COMMAND: " + command)
        ssh_client = self.get_client()
        stdin, stdout, stderr = ssh_client.exec_command(command, get_pty=True)

        for line in iter(stdout.readline, ""):
            LogQTextEdt.setText(LogQTextEdt.toPlainText() + line)
            QApplication.processEvents()
            print(line, end="")

        output = stdout.readlines()
        ssh_client.close()
        return ''.join(output)

    def execute(self, command):
        print("COMMAND: " + command)
        ssh_client = self.get_client()
        stdin, stdout, stderr = ssh_client.exec_command(command, get_pty=True)

        output = stdout.readlines()
        ssh_client.close()
        return ''.join(output)

    def get_client(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=HOSTNAME, username=USERNAME, password=PASSWORD, port=PORT)
        return ssh_client

    def get_file(self, file_path, save_to):
        ssh_client = self.get_client()
        sftp_client = ssh_client.open_sftp()
        sftp_client.get(file_path, save_to)
        sftp_client.close()
        ssh_client.close()

    def send_file(self, file_path, save_to):
        ssh_client = self.get_client()
        sftp_client = ssh_client.open_sftp()
        sftp_client.put(file_path, save_to)
        sftp_client.close()
        ssh_client.close()






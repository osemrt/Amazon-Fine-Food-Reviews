import sys

import paramiko
from PyQt5.QtWidgets import QApplication

from config import config

sys.path.append("..")


def execute_and_write(log_text_edit, command):
    print("COMMAND: " + command)
    ssh_client = get_client()
    stdin, stdout, stderr = ssh_client.exec_command(command, get_pty=True)

    for line in iter(stdout.readline, ""):
        log_text_edit.setText(log_text_edit.toPlainText() + line)
        QApplication.processEvents()
        print(line, end="")

    output = stdout.readlines()
    ssh_client.close()
    return ''.join(output)


def execute(command):
    print("COMMAND: " + command)
    ssh_client = get_client()
    stdin, stdout, stderr = ssh_client.exec_command(command, get_pty=True)

    output = stdout.readlines()
    ssh_client.close()
    return ''.join(output)


def get_client():
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=config.host_name, username=config.username, password=config.password,
                       port=config.port)
    return ssh_client


def get_file(file_path, save_to):
    ssh_client = get_client()
    sftp_client = ssh_client.open_sftp()
    sftp_client.get(file_path, save_to)
    sftp_client.close()
    ssh_client.close()


def send_file(file_path, save_to):
    ssh_client = get_client()
    sftp_client = ssh_client.open_sftp()
    sftp_client.put(file_path, save_to)
    sftp_client.close()
    ssh_client.close()

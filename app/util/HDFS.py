from util import SSH
from config.variables import HADOOP_HOME_PATH


class HDFS:
    def __init__(self):
        self.ssh = SSH.SSH()

    def run(self, jar_path, dataset_path, output_path):
        command = HADOOP_HOME_PATH + '/bin/hadoop jar ' + jar_path + ' ' + dataset_path + ' ' + output_path

    def copy_file_to_HDFS(self, file_path, save_to):
        command = HADOOP_HOME_PATH + '/bin/hdfs dfs -copyFromLocal ' + file_path + ' ' + save_to
        ssh_client = self.ssh.get_client()
        print("copy_file_to_HDFS:" + command)
        stdin, stdout, stderr = ssh_client.exec_command(command)
        ssh_client.close()
        return stdout

    def get_file_from_HDFS(self, file_path, save_to):
        command = HADOOP_HOME_PATH + '/bin/hdfs dfs -get ' + file_path + ' ' + save_to
        ssh_client = self.ssh.get_client()
        print("get_file_from_HDFS:" + command)
        stdin, stdout, stderr = ssh_client.exec_command(command)
        ssh_client.close()
        return stdout

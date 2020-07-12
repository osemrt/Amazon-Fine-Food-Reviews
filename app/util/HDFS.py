from config.variables import HADOOP_HOME_PATH
from util.SSH import get_client


def copy_file_to_hdfs(file_path, save_to):
    command = HADOOP_HOME_PATH + '/bin/hdfs dfs -copyFromLocal ' + file_path + ' ' + save_to
    ssh_client = get_client()
    print("copy_file_to_HDFS:" + command)
    stdin, stdout, stderr = ssh_client.exec_command(command)
    ssh_client.close()
    return stdout


def get_file_from_hdfs(file_path, save_to):
    command = HADOOP_HOME_PATH + '/bin/hdfs dfs -get ' + file_path + ' ' + save_to
    ssh_client = get_client()
    print("get_file_from_HDFS:" + command)
    stdin, stdout, stderr = ssh_client.exec_command(command)
    ssh_client.close()
    return stdout

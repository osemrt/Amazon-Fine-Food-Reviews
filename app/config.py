import toml


class Config:
    def __init__(self):
        variables = toml.load("config.toml")
        self.username = variables["SSH-configurations"]["username"]
        self.password = variables["SSH-configurations"]["password"]
        self.host_name = variables["SSH-configurations"]["server"]
        self.port = variables["SSH-configurations"]["port"]
        self.hadoop_home_path = variables["HADOOP_HOME_PATH"]


config = Config()

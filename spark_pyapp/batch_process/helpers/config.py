import configparser
# from batch_process.constants.module_constants import ModuleConstants


# singleton by restricting direct constructor call restriction, forcing to use get_instance static method
class ConfigManager:
    __instance = None

    @staticmethod
    def get_instance():
        if ConfigManager.__instance is None:
            ConfigManager()
        return ConfigManager.__instance

    def __init__(self):
        if ConfigManager.__instance is not None:
            raise Exception("ConfigManager class is a Singleton, use get_instance !")
        else:
            ConfigManager.__instance = self
            self.__config = configparser.ConfigParser()
            self.__config.read('application.ini')

    @property
    def config(self):
        return self.__config


def config():
    return ConfigManager.get_instance().config


# test singleton

# c1 = ConfigManager.get_instance().config
# c2 = ConfigManager.get_instance().config
#
# if c1 is c2:
#     print(f'{c1} and {c2} are equal !!')
#
# print(ConfigManager.get_instance().config)


# print(config()['HIVE'][ModuleConstants.HIVE_APP_TABLE])
# print(config()['FILE'][ModuleConstants.HADOOP_FILE_COPY_PATH])
# print(config()['FILE'][ModuleConstants.FILE_DOWNLOAD_LOCATION])
# print(config()['QUERIES'][ModuleConstants.QUERIES].split('^'))
# print(config()['QUERIES'][ModuleConstants.RESULT_TABLES].split(','))

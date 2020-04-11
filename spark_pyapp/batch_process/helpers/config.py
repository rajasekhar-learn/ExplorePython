import configparser
# from batch_process.constants.module_constants import ModuleConstants

CONFIG = None


def config():
    global CONFIG
    if CONFIG is None:
        CONFIG = configparser.ConfigParser()
        CONFIG.read('application.ini')
    return CONFIG


# print(config()['HIVE'][ModuleConstants.HIVE_APP_TABLE])
# print(config()['FILE'][ModuleConstants.HADOOP_FILE_COPY_PATH])
# print(config()['FILE'][ModuleConstants.FILE_DOWNLOAD_LOCATION])
# print(config()['QUERIES'][ModuleConstants.QUERIES].split('^'))
# print(config()['QUERIES'][ModuleConstants.RESULT_TABLES].split(','))

from pyspark.sql import SparkSession
from batch_process.helpers.config import config
from batch_process.constants.module_constants import ModuleConstants


# singleton using meta class
class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SparkUtil(metaclass=Singleton):
    __spark_session = None

    def __init__(self):
        if self.__spark_session is None:
            self.__spark_session = SparkSession.builder.master('local[*]').appName('Analyser') \
                .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") \
                .config("spark.sql.warehouse.dir", config()['HIVE'][ModuleConstants.HIVE_WAREHOUSE_LOCATION]) \
                .enableHiveSupport().getOrCreate()

    @property
    def get_spark_session(self):
        return self.__spark_session;

    @staticmethod
    def spark_session():
        return SparkUtil().get_spark_session

    @staticmethod
    def load_data(data_format, path):
        # read the file from HDFS location and crate data frame
        return SparkUtil.spark_session().read \
            .format(data_format) \
            .option("header", "true") \
            .option("inferSchema", "true").load(path)

    @staticmethod
    def execute_show_query(query):
        # get records count and show
        SparkUtil.spark_session().sql(query).show()

    @staticmethod
    def execute_quey_store_results(queries, tables):
        for index in range(len(queries)):
            print("running query :: {0} , storing results in :: {1}".format(queries[index], tables[index]))
            query_df = SparkUtil.spark_session().sql(queries[index])
            query_df.show()
            query_df.write \
                .mode('overwrite') \
                .format("jdbc") \
                .option("driver", config()['DATABASE'][ModuleConstants.APP_RESULTS_DB_DRIVER]) \
                .option("url", config()['DATABASE'][ModuleConstants.APP_RESULTS_DB_URL]) \
                .option("dbtable", tables[index]) \
                .option("user", config()['DATABASE'][ModuleConstants.APP_RESULTS_DB_USER]) \
                .option("password", config()['DATABASE'][ModuleConstants.APP_RESULTS_DB_PWD]) \
                .save()

from pyspark.sql import SparkSession

SPARK_SESSION_REF = None


class SparkUtil:

    @staticmethod
    def spark_session():
        global SPARK_SESSION_REF
        if SPARK_SESSION_REF is None:
            SPARK_SESSION_REF = SparkSession.builder.master('local[*]').appName('Analyser') \
                .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") \
                .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/hive-warehouse") \
                .enableHiveSupport().getOrCreate()
        return SPARK_SESSION_REF

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
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("url", "jdbc:mysql://localhost:3306/metricsdb") \
                .option("dbtable", tables[index]) \
                .option("user", "root") \
                .option("password", "hive") \
                .save()

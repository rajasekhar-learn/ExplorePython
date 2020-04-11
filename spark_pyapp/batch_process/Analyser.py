#!/usr/bin/env python
# coding: utf-8

# In[45]:

import time
from sys import argv

from batch_process.helpers.config import config
from batch_process.helpers.ioutil import download_data, put_file_in_hdfs
from batch_process.helpers.sparkutil import SparkUtil
from batch_process.constants.module_constants import ModuleConstants

# download file to local path and then push to hdfs.
local_path = config()['FILE'][ModuleConstants.FILE_DOWNLOAD_LOCATION]
download_data(argv[2], local_path)
hdfs_file_path = config()['FILE'][ModuleConstants.HADOOP_FILE_COPY_PATH] + 'data_' + \
                 str(int(round(time.time() * 1000))) + '.csv'
print(hdfs_file_path)
put_file_in_hdfs(local_path, hdfs_file_path)

batch_data_frame = SparkUtil.load_data("com.databricks.spark.csv", hdfs_file_path)
# In[55]:
# trimming column High_Confidence_Limit and invalid column name Age(months) to Age
batch_data_frame = batch_data_frame \
    .withColumnRenamed("High_Confidence_Limit ", "High_Confidence_Limit") \
    .withColumnRenamed("Age(months)", "Age")
batch_data_frame.describe()

# In[56]:

# Writing data frame to hive as table
batch_data_frame.write.mode("overwrite").saveAsTable(config()['HIVE'][ModuleConstants.HIVE_APP_TABLE])

# In[58]:3

# get records count and show
query = 'select count(*) from ' + config()['HIVE'][ModuleConstants.HIVE_APP_TABLE]
SparkUtil.execute_show_query(query)

# In[44]:

# running queries and storing to tables

queries = config()['QUERIES'][ModuleConstants.QUERIES].split('^')
tabales = config()['QUERIES'][ModuleConstants.RESULT_TABLES].split(',')
SparkUtil.execute_quey_store_results(queries, tabales)

# stopping spark session

SparkUtil.spark_session().stop()

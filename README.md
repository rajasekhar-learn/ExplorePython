### ExplorePython

**This repo contains sample batch process Aalyser implemented in python using pyspark.**

to run this app, please follow below steps:\
1.softwares used:\
python : 3.7.6\
spark(pyspark) 2.4.5\
anaconda python 3.7 version compatible (for testing and playing using jupytor notebook)

2. download the code spark-pyapp

3.before running the application , please setup environment:

pip install findspark (for jupytor, need to call init)\
make sure hive-site.xml,core-site.xml,hdfs-site.xml and mysql-connector.jar are in classpath. to do that \
you can place them in SPARK_HOME\conf and SPARK_HOME\jars or pass them as part of spark-submit script.

cd C:\Users\veera\ExplorePython\spark-pyapp\
spark-submit batch_process\analyser.py 1 https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD

local run:

![Local Run ](/pyappLocalRun.JPG)
Format: ![Alt Text](url)

#!/usr/bin/env python
# coding: utf-8

# In[45]:

from sys import argv

from batch_process.helpers.sparkutil import SparkUtil
from batch_process.processimpls.surveyprocess import run_batch_process

try:
    if int(argv[1]) > 0:
        run_batch_process(argv[2])
    else:
        print('invalid processId !!')
except ValueError:
    print('invalid value passed to child process! ')
except RuntimeError as err:
    print('error occurred !!, {0 }'.format(err))

    # stopping spark session
    SparkUtil.spark_session().stop()

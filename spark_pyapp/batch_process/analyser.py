#!/usr/bin/env python
# coding: utf-8

# In[45]:

from sys import argv

from batch_process.processimpls.surveyprocess import run_batch_process

try:
    if len(argv) == 3:
        if int(argv[1]) is 1:
            run_batch_process(argv[2])
    else:
        print('expecting processId and url to start process !!')
except ValueError:
    print('invalid processId !, processId should be number.')
except Exception as err:
    print(err)



#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pandas as pd
import numpy as np
import os.path
# #these packages are for sending email with attachment csv file
# from sendgrid import SendGridAPIClient
# from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition, ContentId)
# from apikey import *
# import base64

#these packages are for scheduling task on cloud machine
import schedule
import time
import pytz 
import datetime

import pymongo
import json

from os import environ as env

from dotenv import load_dotenv
load_dotenv()
# DB_URL_LOCAL = 'mongodb://localhost:27017'
DB_URL = env['DB_URL']
print(DB_URL)

import websites.ivivu as ivivu
import websites.viettravel as viettravel
import websites.vietnambooking as vietnambooking



# In [ ]:


def crawl(): 
    
    

    all_df = []
    check = True

    try:
        
        all_df.append(ivivu.job_ivivu())
    except Exception as e:
        check = False
        print('Ivivu has exception : '+ str(e))

    # try:
    #     all_df.append(viettravel.job_viettravel())
    # except Exception as e:
    #     check = False
    #     print('viettravel has exception : '+ str(e))


    # try:
    #     all_df.append(vietnambooking.job_vietnambooking())
    # except Exception as e:
    #     check = False
    #     print('vietnambooking has exception :' + str(e))
        
    if check:
        
        client = pymongo.MongoClient(DB_URL)
        db = client.Tour
        mycol = db.TourUpdate

        df_all = pd.concat(all_df, ignore_index = True)
        records = json.loads(df_all.T.to_json()).values()

        #delete duplicates record
        unique_record_list = { each['Tour_id'] : each for each in records }.values()

        #insert records to database
        insert = mycol.insert_many(unique_record_list)

        #update database
        mycol = db.Tour


        new_id_record_list = [e['Tour_id'] for e in unique_record_list]
        old_record_list = mycol.find({})

        remove_record_list = []

        list_id_old = []
        for item in old_record_list:
                list_id_old.append(item['Tour_id'])
                if item['Tour_id'] not in new_id_record_list:
                    remove_record_list.append(item['Tour_id'])
        new_record_list = []
        for id in new_id_record_list:
            if id not in list_id_old:
                new_record_list.append(id)

        total_record_crawl = len(new_id_record_list)
        total_old_record = len(list_id_old)
        total_remove_record = len(remove_record_list)
        total_new_record_crawl = len(new_record_list)

        system_time = time.localtime(time.time())
        day = system_time.tm_mday
        month = system_time.tm_mon
        year = system_time.tm_year
        print('time:'+str(day)+'-'+str(month)+'-'+str(year))
        print('total old record :'+str(total_old_record))
        print('total records crawled :'+str(total_record_crawl))
        print('total records removed:'+str(total_remove_record))
        print('total new records crawled:'+str(total_new_record_crawl))
        mycol.drop()
        mycol = db.TourUpdate.rename("Tour")

    else:
        print('co loi')

    
# In[ ]:
    crawl()
# #schedule your job
# # schedule.every(5).minutes.do(crawl)
# schedule.every().day.at("23:59").do(crawl)

# while True:
#     schedule.run_pending()
#     time.sleep(1) 




# %%

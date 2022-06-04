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
    fail_website = []
    error_website = []
    success_website = []
    try:
        
        all_df.append(ivivu.job_ivivu())
        success_website.append(ivivu.getWebName())
    except Exception as e:
        check = False
        error_website.append(ivivu.getWebName())
        print('Ivivu has exception : '+ str(e))
        fail_website.append(ivivu.getWebName() + ' has exception : '+ str(e))

    try:
        all_df.append(viettravel.job_viettravel())
        success_website.append(viettravel.getWebName())
    except Exception as e:
        check = False
        error_website.append(viettravel.getWebName())
        print('viettravel has exception : '+ str(e))
        fail_website.append(viettravel.getWebName() +' has exception : '+ str(e))


    try:
        all_df.append(vietnambooking.job_vietnambooking())
        success_website.append(vietnambooking.getWebName())
    except Exception as e:
        check = False
        error_website.append(vietnambooking.getWebName())
        print('vietnambooking has exception :' + str(e))
        fail_website.append(vietnambooking.getWebName()+' has exception : '+ str(e))
    # system_time = time.localtime(time.time())
    # day = system_time.tm_mday
    # month = system_time.tm_mon
    # year = system_time.tm_year
    
    system_time = time.localtime()

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


        # tracking database
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

        
        
        
        mycol.drop()
        mycol = db.TourUpdate.rename("Tour")

        # insert log

        
        time_crawl_format = time.strftime("%d/%m/%Y, %H:%M:%S", system_time)
        time_crawl = datetime.datetime.strptime(time_crawl_format,'%d/%m/%Y, %H:%M:%S')
        print(time_crawl_format)
        print('total old record :'+str(total_old_record))
        print('total records crawled :'+str(total_record_crawl))
        print('total records removed:'+str(total_remove_record))
        print('total new records crawled:'+str(total_new_record_crawl))

        
        crawl_log = {
            "time":time_crawl,
            "time_format":time_crawl_format,
            "total_old_record":total_old_record,
            "total_record_crawled":total_record_crawl,
            "total_record_removed":total_remove_record,
            "total_new_record_crawled":total_new_record_crawl,
        }
        
        logcol = db.CrawlHistories

        log = logcol.insert_one(crawl_log)

    else:
        client = pymongo.MongoClient(DB_URL)
        db = client.Tour
        mycol = db.TourUpdate
        oldcol = db.Tour
        logcol = db.CrawlHistories

        # get old record form website error when crawl
        if all_df:

             # get records crawled
            df_all = pd.concat(all_df, ignore_index = True)
            records = json.loads(df_all.T.to_json()).values()

            unique_record_list = { each['Tour_id'] : each for each in records }.values()

            mycol.insert_many(unique_record_list)
            
            no_remove_record_list = []        
            for website in error_website:
                old_record_list = oldcol.find({'Web_name':website})
                for record in old_record_list:
                    tour_id = record['Tour_id']
                    web_name = record['Web_name']
                    web_logo = record['Web_logo']
                    tour_name = record['Tour_name']
                    tour_link = record['Tour_link']
                    tour_img = record['Tour_img']
                    tour_duration = record['Tour_duration']
                    tour_start_day = record['Tour_start_day']
                    tour_price = record['Tour_price']

                    no_remove_record_list.append(tour_id)

                    tour = {"Tour_id":tour_id,
                            "Web_name":web_name, 
                            "Web_logo":web_logo, 
                            "Tour_name": tour_name, 
                            "Tour_link": tour_link, 
                            "Tour_img": tour_img, 
                            "Tour_duration":tour_duration,
                            "Tour_start_day":tour_start_day,
                            "Tour_price": tour_price 
                            }
                    mycol.insert_one(tour)
            print(no_remove_record_list)
           


            # update database
            # mycol = db.Tour

            # tracking database
            new_id_record_list = [e['Tour_id'] for e in unique_record_list] 

            old_record_list1 = oldcol.find({'Web_name': {"$in": success_website }})
          
            remove_record_list = []

            list_id_old = []
            for item in old_record_list1:
                         list_id_old.append(item["Tour_id"])
                         if item["Tour_id"] not in new_id_record_list:
                            remove_record_list.append(item["Tour_id"])
            new_record_list = []
            for id in new_id_record_list:
                if id not in list_id_old:
                    new_record_list.append(id)

            total_record_crawl = len(new_id_record_list)
            total_old_record = len(list_id_old)+len(no_remove_record_list)
            total_remove_record = len(remove_record_list)
            total_new_record_crawl = len(new_record_list)
        
        #    Update database
            oldcol.drop()
            mycol = db.TourUpdate.rename("Tour")


            # insert log

            time_crawl_format = time.strftime("%d/%m/%Y, %H:%M:%S", system_time)
            time_crawl = datetime.datetime.strptime(time_crawl_format,'%d/%m/%Y, %H:%M:%S')
            print(time_crawl)
            print('total old record :'+str(total_old_record))
            print('total records crawled :'+str(total_record_crawl))
            print('total records removed:'+str(total_remove_record))
            print('total new records crawled:'+str(total_new_record_crawl))


            crawl_log = {
                "time":time_crawl,
                "time_format":time_crawl_format,
                "total_old_record":total_old_record,
                "total_record_crawled":total_record_crawl,
                "total_record_removed":total_remove_record,
                "total_new_record_crawled":total_new_record_crawl,
                "fail":fail_website,
            }
            

            log = logcol.insert_one(crawl_log)
           
            
        else:
            time_crawl_format = time.strftime("%d/%m/%Y, %H:%M:%S", system_time)
            time_crawl = datetime.datetime.strptime(time_crawl_format,'%d/%m/%Y, %H:%M:%S')
            crawl_log = {"time":time_crawl,"time_format":time_crawl_format,"fail":fail_website}
            log = logcol.insert_one(crawl_log)
        

    
# In[ ]:
    crawl()
# #schedule your job
# # schedule.every(5).minutes.do(crawl)
# schedule.every().day.at("23:59").do(crawl)

# while True:
#     schedule.run_pending()
#     time.sleep(1) 




# %%

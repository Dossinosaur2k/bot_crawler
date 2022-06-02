
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os.path

from unidecode import unidecode

web_name = 'Ivivu'

def job_ivivu():
#     mycol.drop()
#     mycol = db.tourUpdate
#     myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#     dblist = myclient.list_database_names()
#     mydb = myclient["Tour"]
#     mycol = mydb["tour3"]
#     mycol.drop()
#     mycol = mydb["tour3"]
    #this is the url of the very first page listing used Apple product we got earlier.
    url='https://www.ivivu.com'

    

    site = 'https://www.ivivu.com/du-lich/tour-phu-quoc?'
    web_request = Request(site, headers={'User-Agent':'Mozilla/5.0'})

    web_page = urlopen(web_request).read()
    soup = BeautifulSoup(web_page, 'html.parser')
    

    # lấy link
    group_link1 = soup.find('div', attrs={"class":"panel-group"}).find_all('a')
#     print(group_link1)
    group_link2 = soup.find('div', attrs={"id":"collapseOne", "class":"panel-collapse"}).find_all('a')
#     print(group_link2)
    
    group_links_all = group_link1 + group_link2
    page_links=[site]
    for i in group_links_all:
        page_links.append(url+i['href'])
    print(page_links)
    #Lấy thông tin của toàn bộ page trong link
   
    all_df = []
    dfs=[]
    for page in page_links:
        
        web_request = Request(page, headers={'User-Agent':'Mozilla/5.0'})

        web_page = urlopen(web_request).read()
        soup = BeautifulSoup(web_page, 'html.parser')
        
        tour_items = soup.find_all('div', attrs={"class":"tourItem"})
    #     print(tour_items)
        tour_imgs = []
        tour_links = []
        tour_names = []
        tour_names_no_vietnamese = []
        tour_prices = []
        web_logos = []
        tour_durations = []
        tour_start_days = []
        tour_ids = []
        web_names = []
        
        for item in tour_items:
            tour_price = item.find('span' , attrs={"class":"tourItemPrice"})
            if (tour_price is not None):

                web_names.append(web_name)

                web_logos.append(url+soup.find('a', attrs={"class":"navbar-brand"}).find('img')['src'])
                #convert to int 
#                 tour_price = int(tour_price.get_text().replace('VND','').replace('.','').strip())

                tour_imgs.append(item.find('img')['data-src'])
    
                tour_links.append(url+item.find('a' , attrs={"class":"linkDetail"})['href'])
        
                tour_names.append(item.find('span' , attrs={"class":"tourItemName"}).get_text().strip())

                tour_names_no_vietnamese.append(unidecode(item.find('span' , attrs={"class":"tourItemName"}).get_text().strip()))
            
                tour_prices.append(int(tour_price.get_text().replace("VND", "").replace('.','').strip()))
                
                tour_infors = item.find('div', attrs={"class":"v-margin-top-10"}).find_all('span', attrs={"class":"v-margin-right-15"})

                tour_ids.append(tour_infors[0].get_text().replace('Mã: ','').strip())
                                   
                tour_durations.append(tour_infors[1].get_text().strip())
                                   
                tour_start_days.append(item.find('span',attrs={"class":"tourItemDateTime"}).get_text().strip().replace("Khởi hành:", ""))
#                 tour = { "TourName": tour_name, "TourLink": tour_link, "TourImg": tour_img, "TourPrice": tour_price }
#                 insert = mycol.insert_one(tour)
              
        df = pd.DataFrame({"Tour_id": tour_ids, 
                           "Web_name":web_names,
                           "Web_logo":web_logos, 
                           "Tour_name": tour_names,
                           "Tour_name_no_vietnamese":tour_names_no_vietnamese,
                           "Tour_link": tour_links, 
                           "Tour_img": tour_imgs,
                           "Tour_duration":tour_durations,
                           "Tour_start_day":tour_start_days,
                           "Tour_price": tour_prices 
                          })
                          
        dfs.append(df)
        # print(df)
    df = pd.concat(dfs, ignore_index=True)
    # df.drop_duplicates(subset=["Tour_id","Tour_name"],keep=False, inplace=True,ignore_index=True)
    return df
def getWebName():
    return web_name

    
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os.path

def job_viettravel():
    #this is the url of the very first page listing used Apple product we got earlier.
    url='https://travel.com.vn'
    site = 'https://travel.com.vn/'
    web_name ='Viettravel'
    web_request = Request(site, headers={'User-Agent':'Mozilla/5.0'})

    web_page = urlopen(web_request).read()
    soup = BeautifulSoup(web_page, 'html.parser')
    
    page_links = []
    div_page_links = soup.find_all('ul', attrs={"class":"list-unstyled"})
    for div_link in div_page_links:
        link = div_link.find('a' , attrs={"class":"fw-bold"})
        if(link is not None) and (link['href'].find('du-lich-nuoc-ngoai',) < 0):
            page_links.append(url+link['href'])
#             print(link)


    
    index=0
    dfs = []
    for site in page_links:   

        
        web_request = Request(site, headers={'User-Agent':'Mozilla/5.0'})
        web_page = urlopen(web_request).read()
        soup = BeautifulSoup(web_page, 'html.parser')
        pagination_links=[]
        pagination = soup.find("div", attrs={"class":"pager_simple_orange"})

        pagination_links.append(url+pagination.find('td', attrs={"class":"active"}).find('a')['href'])
        a=1 
        while a<100:
            web_request = Request(pagination_links[-1], headers={'User-Agent':'Mozilla/5.0'})
            web_page = urlopen(web_request).read()
            soup = BeautifulSoup(web_page, 'html.parser')
            paginations = soup.find("div", attrs={"class":"pager_simple_orange"}).find_all('a')
            for i in paginations:
                if(i.get_text().strip() == str(a+1)):
                        pagination_links.append(url + i['href'])
            if(pagination_links[-1] == url+paginations[-1]['href']):
                a=100
            a+=1
        print(pagination_links)
        print("\n")
        #assign
        web_names = []
        tour_ids = [] 
        tour_imgs = []
        tour_links = []
        tour_names = []
        tour_prices = []
        web_logos = []
        tour_durations = []
        tour_start_days = []
        
        for page in pagination_links:
            
            web_request = Request(page, headers={'User-Agent':'Mozilla/5.0'})
            web_page = urlopen(web_request).read()
            soup = BeautifulSoup(web_page, 'html.parser')
            
            tour_items = soup.find_all('div', attrs={"class":"tour-item"})
            
            for item in tour_items:
                tour_id = item.find('div', attrs={"class":"tour-item__code"}).find('div',attrs={"class":"font-weight-bold"}).get_text().strip()
                tour_img = item.find('img')['src']
                tour_link = item.find('a')['href']
                tour_name = item.find('h3', attrs={"class":"tour-item__title"}).find('a').get_text().strip()

                tour_price = item.find('span', attrs={"class":"tour-item__price--current__number"}).get_text().replace("₫", "").strip()
                tour_price = int(tour_price.replace(',',''))

                tour_infors = item.find('p', attrs={"class":"tour-item__date"}).get_text().strip().split('-')
                
                tour_start_day = tour_infors[0]
                
                tour_duration = tour_infors[1].replace('N', ' ngày ').replace('Đ', ' đêm')
                
                tour_ids.append(tour_id)

                web_names.append(web_name)
                
                tour_imgs.append(tour_img)
    
                tour_links.append(url+tour_link)
        
                tour_names.append(tour_name)
            
                tour_prices.append(tour_price)
                
                web_logos.append(url+soup.find('a', attrs={"class":"navbar-brand"}).find('img')['src'])
                
                tour_durations.append(tour_duration)
                
                tour_start_days.append(tour_start_day)
                
#                 tour = { "TourName": tour_name, "TourLink": tour_link, "TourImg": tour_img, "TourPrice": tour_price }
#                 index += 1
#                 print(tour)
        df = pd.DataFrame({"Tour_id":tour_ids,
                            "Web_name":web_names, 
                           "Web_logo":web_logos, 
                           "Tour_name": tour_names, 
                           "Tour_link": tour_links, 
                           "Tour_img": tour_imgs, 
                           "Tour_duration":tour_durations,
                           "Tour_start_day":tour_start_days,
                           "Tour_price": tour_prices 
                          })
        dfs.append(df)
        # print(df)
    df= pd.concat(dfs, ignore_index = True)
    return df
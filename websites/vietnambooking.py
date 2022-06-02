from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os.path

from unidecode import unidecode

web_name= 'vietnambooking'

def job_vietnambooking():
    url='https://www.vietnambooking.com'
    site = 'https://www.vietnambooking.com/du-lich'
    
    web_request = Request(site, headers={'User-Agent':'Mozilla/5.0'})
    web_page = urlopen(web_request,timeout=45).read()
    soup = BeautifulSoup(web_page, 'html.parser')

    page_links = []
    div_page_links = soup.find('div', attrs={"class":"category-tour-box-type-country-inner"}).find_all('div', attrs={"class":"box-link-title"})
    for div_link in div_page_links:
        link = div_link.find('a')
        page_links.append(link['href'])
    
    page_links.pop(0)
    
    print(page_links)
    
    
    all_df = []
    for site in page_links:
        

        web_request = Request(site, headers={'User-Agent':'Mozilla/5.0'})
        web_page = urlopen(web_request,timeout=45).read()
        soup = BeautifulSoup(web_page, 'html.parser')
        
         #crawling for the links in pagniation
        pagination = soup.find("a", attrs={"class":"page-numbers"}, href=True)
        pagination_links = [site]
        if (pagination is None):
            print('')
        else:
            pagination_links.append( (url + pagination['href']) ) 
#         print(pagination_links[-1])
            a=2
            while a < 100:
                site = pagination_links[-1]
                web_request = Request(site, headers={'User-Agent':'Mozilla/5.0'})
                web_page = urlopen(web_request).read()
                soup = BeautifulSoup(web_page, 'html.parser')
                pagination = soup.find_all("a", attrs={"class":"page-numbers"}, href=True)
#                 print(pagination)
#                 print(a)
                for i in pagination:
                    if(i.get_text().strip() == str(a+1)):
                        pagination_links.append(url + i['href'])
                a+=1
        print(pagination_links)
#         print("\n")
        list_df = []
        for page in pagination_links:

            web_request = Request(page, headers={'User-Agent':'Mozilla/5.0'})
            web_page = urlopen(web_request).read()
            soup = BeautifulSoup(web_page, 'html.parser')
        
            div_tours = soup.find('div', attrs={"class":"category-box-list-default-inner"})
        
        
            Imgs = []
            tour_imgs = div_tours.find_all('div', attrs={"class":"box-img"})
            for tour_img in tour_imgs:
                Imgs.append(tour_img.find('img')['src'])

            Tour_names = []
            Tour_names_no_vietnamese = []
            Tour_links = []
            tour_names = div_tours.find_all('div', attrs={"class":"box-title-content"})
            for tour_name in tour_names:
                div_name = tour_name.find('a')
                Tour_names.append(div_name.get_text().strip())
                Tour_names_no_vietnamese.append(unidecode(div_name.get_text().strip()))
                Tour_links.append(div_name['href'])


            Tour_prices = []
            tour_prices = div_tours.find_all('div', attrs={"class":"box-price-promotion-tour"})
            for tour_price in tour_prices:
                temp_tour_price = tour_price.find('span').get_text().replace("đ", "").replace(',','').strip()
                if temp_tour_price == 'Liên hệ':
                    Tour_prices.append(0)
                else:
                     Tour_prices.append(int(temp_tour_price))
    
            web_logos = []
            web_names = []
            for i in range(len(Tour_names)):
                web_logos.append(soup.find('div',attrs={"class":"header-box-logo"}).find('img')['src'])
                web_names.append(web_name)
            
            tour_ids = []
            tour_durations = []
            tb_tour_infors = div_tours.find_all('table', attrs={"class":"tlb-time-and-traffic-tour"})
            for tb_tour_infor in tb_tour_infors:
                tour_infors = tb_tour_infor.find_all('td')
                tour_ids.append(tour_infors[0].get_text().replace('Mã:','').strip())
                tour_durations.append(tour_infors[1].get_text().strip())
            
            df = pd.DataFrame({'Tour_id':tour_ids,
                                'Web_name':web_names,
                                'Web_logo':web_logos,
                               'Tour_name':Tour_names, 
                                'Tour_name_no_vietnamese':Tour_names_no_vietnamese,
                               'Tour_link':Tour_links, 
                               'Tour_img':Imgs,
                               'Tour_duration':tour_durations, 
                               'Tour_price':Tour_prices})
            list_df.append(df)
            
        df = pd.concat(list_df, ignore_index=True)
        all_df.append(df)
        # print(df)
    df = pd.concat(all_df, ignore_index=True)
    return(df)

def getWebName():
    return web_name



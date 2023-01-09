#%%
from itertools import product
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
import re
import math
import requests
from itertools import chain
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging
import re
import timeit
import sys
import random
import asyncio


user_agent_list = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]


def get_driver(DRIVER_PATH):
    user_agent = random.choice(user_agent_list)
    options = webdriver.ChromeOptions()
    options.add_argument(f'user-agent={user_agent}')                                                                                                                        
    # options.add_argument('--headless')
    options.add_argument("--disable-infobars")
    options.add_argument('--no-sandbox')
    # options.add_argument("--disable-extensions")
    # options.add_argument("--incognito")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])         
    options.add_argument('--disable-dev-shm-usage')
    try: 
        driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=options)
    except: 
        driver = webdriver.Chrome(DRIVER_PATH)   
    return driver

def open_website(driver, url, county): 
    driver.get(url+county)

def open_link(driver, url, county, town): 
    town = town.replace(" ", "_").lower()
    driver.get(url+county+'/'+town+'.html')
    
def click_town(driver,town):
    driver.find_element_by_link_text(town).click()

def go_back(driver): 
    driver.execute_script("window.history.go(-1)")
        
def soup_page(driver):
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    return soup

def find_names(driver,soup): 
    generic_soup = soup.find('ul')
    generic_list =[]
    for li in generic_soup.find_all('li'):
        for a in li.find_all('a'):
            generic_list.append(a.get_text())
    return generic_list

    
#%% 
DRIVER_PATH = 'chromedriver.exe'
URL = 'https://geographic.org/streetview/usa/md/'
COUNTIES = ['anne_arundel', 'montgomery', 'prince_george_s']
#%% 

county = COUNTIES[1]
#%% 
driver = get_driver(DRIVER_PATH)
open_website(driver, URL, county)

home_page = soup_page(driver)
town_list = find_names(driver, home_page)

towns = []
cities = []
for town in town_list:
    open_link(driver, URL, county, town)
    city_page = soup_page(driver)
    city_list = find_names(driver, city_page)
    cities.extend(city_list)
    town_list = [town] * len(city_list)
    towns.extend(town_list)

#%%
zipped = zip(towns, cities)
df = pd.DataFrame(list(zipped))


#%% 
df['county'] = county
df.columns =['town', 'city', 'county']
df.to_csv(f"{county}_locations.csv")
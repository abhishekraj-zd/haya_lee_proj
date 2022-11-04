#%%
from itertools import product
from os import remove
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
import re
import math
import requests
from itertools import chain
import concurrent.futures
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging
import re
import sys
import random

def get_driver(DRIVER_PATH, user_agent_list):
    user_agent = random.choice(user_agent_list)
    options = webdriver.ChromeOptions()
    options.add_argument(f'user-agent={user_agent}')                                                                                                                        
    options.add_argument('--headless')
    options.add_argument("--disable-infobars")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument("--incognito")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])         
    options.add_argument('--disable-dev-shm-usage')
    try: 
        driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=options)
    except: 
        driver = webdriver.Chrome(DRIVER_PATH)   
    return driver

def open_website(driver, url): 
    driver.get(url)
    
def select_option(driver, option_id, option_list, option_index):
    """id - element id
    list - items you want to match"""
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, option_id)))
    select = Select(driver.find_element(By.ID, option_id))
    option_ = [o.text for o in select.options]
    [item.replace("'", "") for item in option_]
    [item.replace("'", "") for item in option_list]
    index = option_.index(option_list[option_index])
    select.select_by_index(index)
    
def remove_suffix(st_list, suffix_list):
    cleaned_st_list = st_list.apply(lambda x: [word for word in x if word not in suffix_list])
    cleaned_st_list = cleaned_st_list.apply(lambda x: ' '.join(x))
    cleaned_st_list = [word.strip() for word in cleaned_st_list if word != None]
    cleaned_st_list = [word for word in cleaned_st_list if len(word) > 3]
    return list(set(cleaned_st_list))

def load_suffixes(file_name): 
    suffixes = pd.read_csv(f"{file_name}")
    suffix_list = suffixes['suffix'].apply(lambda x: x.lower())
    return [word for word in suffix_list]

def load_locations(file_name):
    locations = pd.read_csv(f"{file_name}")
    cleaned_st = locations['city'].apply(lambda x: x.lower())
    cleaned_st = cleaned_st.apply(lambda x: x.split())
    return cleaned_st

def row_text(driver, index):
    text = driver.find_element(By.ID, f"cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchResult_gv_SearchResult_lnkDetails_{index}").text
    return text

def click_row(driver,index): 
    try: 
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, f"cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchResult_gv_SearchResult_lnkDetails_{index}"))).click()
    except (TimeoutException): 
        time.sleep(10)
        driver.find_element(By.ID, f'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchResult_gv_SearchResult_lnkDetails_{index}').click()
         

def find_row_details(driver,first_row): 
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, '//table/tbody/tr')))
    rows = len(driver.find_elements(By.XPATH, "//table/tbody/tr"))
    try: 
        page = driver.find_element(By.CLASS_NAME, "Pager").text[0]
    except: 
        page = 0
    if page == 0: 
        last_row = rows - 5
    else: 
        last_row = rows - 9
    return first_row, last_row, rows

def go_back(driver, id_1):    
    try: 
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, id_1))).click()
    except: 
        time.sleep(5)
        driver.find_element(By.ID, id_1).click()
            
def current_page(driver): 
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'Pager')))
    t = driver.find_element(By.CLASS_NAME, "Pager").text
    test_num = re.findall('(\d+)', t)
    num = re.findall('(\d+)', t)[0]
    return num 

def last_page(driver): 
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'Pager')))
    t = driver.find_element(By.CLASS_NAME, "Pager").text
    num = re.findall('(\d+)', t)[-1]    
    return num 

def roundup(x):
    return int(math.ceil(x / 10.0)) * 10

def page_skips(driver, sleeptime): 
    if len(driver.find_elements(By.CLASS_NAME, "Pager")) > 0:
        try: 
            if len(driver.find_elements(By.LINK_TEXT, ">>")) > 0:
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.LINK_TEXT, ">>"))).click()
                time.sleep(sleeptime)
                total_pages = last_page(driver)
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.LINK_TEXT, "<"))).click()
                time.sleep(sleeptime)
            else: 
                total_pages = last_page(driver)
        except: 
            time.sleep(1)
            total_pages = last_page(driver)
    else: 
        total_pages = 0 
    return total_pages 

def next_skip(driver):
    try: 
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.LINK_TEXT, "...")))
        driver.find_elements(By.LINK_TEXT, "...")[-1].click()
    except (NoSuchElementException, TimeoutException): 
        print("Can't skip to next page.")
    
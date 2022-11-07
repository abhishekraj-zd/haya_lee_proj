#%% 
from argparse import PARSER
from itertools import product
from numpy.core.fromnumeric import sort
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from itertools import chain
from collections import Counter 
import argparse
import pandas as pd
import numpy as np
from functions import sdat_scraper as sdat
import sqlite3

from log_utils import create_log_object

connection_obj = sqlite3.Connection("maryland.db")
cursor_obj = connection_obj.cursor()
log = create_log_object("prince_george_s")

user_agent_list = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]

DRIVER_PATH = 'chromedriver.exe'
URL = 'http://propertytaxesmobile.princegeorgescountymd.gov/'
COUNTIES = ['Anne Arundel County', 'Montgomery County', "Prince George's County"]
U_COUNTIES = [i.upper() for i in COUNTIES]
ACCOUNT_ID = 'acctno'
ACCOUNT_GO_ID = 'Button2'
NEW_SEARCH_ID = 'HyperLink2'
NEW_SEARCH_ID_2 = 'HyperLink1'
DATA_TABLE_ID = 'Table6'
TAX_AMOUNT_TABLE_ID = 'tblpayment'
DESCRIPTION_TABLE_ID = 'Table3'
BLOCK_ID = 'lblblock'
OWNER_ID = 'lblowner'
TAX_SALE_ID = 'lblinfo'
ATTORNEY_NAME_ID = 'lblatty'
ATTORNEY_PHONE_NUMBER_ID = 'lblattyph'
PURCHASER_NAME_ID = 'lblpurnm'
EQUITY_CASE_NUMBER_ID = 'lblcasenm'
BID_AMOUNT_ID = 'lblbidamt'
BASE_ID = 'lblbase1'
IP_ID = 'lblinp1'
AMOUNT_ID = 'lbltot1'
#%% 

def get_data(driver):
    try: 
        tax_month = driver.find_element(By.ID,'lblpyln5col1').text
    except: 
        tax_month = None
    try:     
        tax_amount = driver.find_element(By.ID, 'lblpyln5col9').text
    except: 
        tax_amount = None
    try: 
        account_number = driver.find_element(By.ID, 'lblacct').text
    except: 
        account_number = None
    try: 
        owner = driver.find_element(By.ID, OWNER_ID).text
    except: 
        owner = None
    # try:
    #     block = driver.find_element(By.ID, BLOCK_ID).text
    # except:
    #     block = None
    try:
        attorney_name = driver.find_element(By.ID, ATTORNEY_NAME_ID).text
    except:
        attorney_name = None
    try:
        attorney_phone = driver.find_element(By.ID, ATTORNEY_PHONE_NUMBER_ID).text
    except:
        attorney_phone = None
    try:
        purchaser_name = driver.find_element(By.ID, PURCHASER_NAME_ID).text
    except:
        purchaser_name = None
    try:
        equity_case_no = driver.find_element(By.ID, EQUITY_CASE_NUMBER_ID).text
    except:
        equity_case_no = None
    try:
        bid_amount = driver.find_element(By.ID, BID_AMOUNT_ID).text
    except:
        bid_amount = None
    try:
        base = driver.find_element(By.ID, BASE_ID).text
    except:
        base = None
    try:
        ip = driver.find_element(By.ID, IP_ID).text
    except:
        ip = None
    try:
        amount = driver.find_element(By.ID, AMOUNT_ID).text
    except:
        amount = None
        
    tax_sale = None
    sql_tax = '''INSERT INTO PG_TAX ( "tax_month", "tax_amount", "parcel_ID", "owner", "tax_sale", "attorney_name", "attorney_phone", "purchaser_name", "equity_case_no", "bid_amount", "base", "ip", "amount") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cursor_obj.execute(sql_tax, (tax_month, tax_amount, account_number, owner, tax_sale, attorney_name, attorney_phone, purchaser_name, equity_case_no, bid_amount, base, ip, amount))
    connection_obj.commit()
    return tax_month, tax_amount, account_number, owner, tax_sale, attorney_name, attorney_phone, purchaser_name, equity_case_no, bid_amount, base, ip, amount #block

def get_sale_data(driver):
    try: 
        tax_month = driver.find_element(By.ID, 'lblpyln5col1').text
    except:
        tax_month = None
    try:
        tax_amount = driver.find_element(By.ID, 'lblpyln5col9').text
    except: 
        tax_amount = None
    try: 
        account_number = driver.find_element(By.ID, 'lblacct').text
    except: 
        account_number = None
    try: 
        owner = driver.find_element(By.ID, OWNER_ID).text
    except: 
        owner = None
    try: 
        tax_sale = driver.find_element(By.XPATH, '//*[@id="Table1"]/tbody/tr[2]/td').text
    except: 
        tax_sale = 'THIS ACCOUNT IS IN TAX SALE'
    # try:
    #     block = driver.find_element(By.ID, BLOCK_ID).text
    # except:
    #     block = None
    try:
        attorney_name = driver.find_element(By.ID, ATTORNEY_NAME_ID).text
    except:
        attorney_name = None
    try:
        attorney_phone = driver.find_element(By.ID, ATTORNEY_PHONE_NUMBER_ID).text
    except:
        attorney_phone = None
    try:
        purchaser_name = driver.find_element(By.ID, PURCHASER_NAME_ID).text
    except:
        purchaser_name = None
    try:
        equity_case_no = driver.find_element(By.ID, EQUITY_CASE_NUMBER_ID).text
    except:
        equity_case_no = None
    try:
        bid_amount = driver.find_element(By.ID, BID_AMOUNT_ID).text
    except:
        bid_amount = None
    try:
        base = driver.find_element(By.ID, BASE_ID).text
    except:
        base = None
    try:
        ip = driver.find_element(By.ID, IP_ID).text
    except:
        ip = None
    try:
        amount = driver.find_element(By.ID, AMOUNT_ID).text
    except:
        amount = None
    sql_tax = '''INSERT INTO PG_TAX ( "tax_month", "tax_amount", "parcel_ID", "owner", "tax_sale", "attorney_name", "attorney_phone", "purchaser_name", "equity_case_no", "bid_amount", "base", "ip", "amount") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cursor_obj.execute(sql_tax, (tax_month, tax_amount, account_number, owner, tax_sale, attorney_name, attorney_phone, purchaser_name, equity_case_no, bid_amount, base, ip, amount))
    connection_obj.commit()
    return tax_month, tax_amount, account_number, owner, tax_sale, attorney_name, attorney_phone, purchaser_name, equity_case_no, bid_amount, base, ip, amount #block

def get_to_last_page(driver):
    while True:
        try:
            WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.LINK_TEXT, "Next"))).click()
        except:
            return

def new_search(driver):
    try: 
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, 'New Search'))).click()
    except: 
         WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, 'New Search'))).click()

#%% 
def main(accounts, path):
    driver = sdat.get_driver(DRIVER_PATH, user_agent_list)
    sdat.open_website(driver, URL)
    searched = []
    data = pd.DataFrame(columns=['tax_month', 'tax_amount', 'parcel_ID', 'owner', 'tax_sale', 'attorney_name', 'attorney_phone', 'purchaser_name', 'equity_case_no', 'bid_amount', 'base', 'ip', 'amount']) #block
    for account in accounts:
        print(account)
        exceptions = 0
        try: 
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, ACCOUNT_ID))).send_keys(account)
            driver.find_element(By.ID, ACCOUNT_GO_ID).click()
            if [e.text for e in driver.find_elements(By.ID, DATA_TABLE_ID)] != ['']:
                if [e.text for e in driver.find_elements(By.LINK_TEXT, 'Next')] == ['Next']:
                    get_to_last_page(driver)
                    if driver.find_element(By.XPATH, '//*[@id="dgSummary"]/tbody/tr[last()-1]/td[3]').text == 'TAX SALE DETAILS':
                        driver.find_element(By.XPATH, '//*[@id="dgSummary"]/tbody/tr[last()-1]/td[1]').click()
                        data.loc[len(data)] = get_sale_data(driver)
                    else: 
                        driver.find_element(By.XPATH, '//*[@id="dgSummary"]/tbody/tr[last()-1]/td[1]').click()
                        data.loc[len(data)] = get_data(driver)
                else: 
                    if driver.find_element(By.XPATH, '//*[@id="dgSummary"]/tbody/tr[last()]/td[3]').text == 'TAX SALE DETAILS':
                        driver.find_element(By.XPATH, '//*[@id="dgSummary"]/tbody/tr[last()]/td[1]').click()
                        data.loc[len(data)] = get_sale_data(driver)
                    else: 
                        driver.find_element(By.XPATH, '//*[@id="dgSummary"]/tbody/tr[last()]/td[1]').click()
                        data.loc[len(data)] = get_data(driver)
            else: 
                print("No data found. Moving on..")
                cursor_obj.execute('''INSER INTO PG_RETRY ("parcel_id","index_parcel","status") VALUES (?,?,?);''',
                                   (account, accounts.index(account), "NO_DATA"))
                connection_obj.commit()

                searched.append(account)
                pass
            print(f'{len(data)}/{len(accounts)} accounts done.')
            new_search(driver)
            searched.append(account)
            if len(data) % 500 == 0:
                driver.delete_all_cookies()
            else:
                pass
        except Exception as e:
            print(e)
            print("Something went wrong. Skipping account..")
            # data.to_csv(f'{path}backup_pg_tax_{len(searched)}_{len(accounts)}.csv') # add worker
            exceptions += 1 
            if exceptions < 20:
                sdat.open_website(driver, URL)
                continue
            else:
                print('Too many exceptions, stopping loop..')
                break
    data.to_csv(f"{path}Prince George's County_TAX.csv") # add worker
    print(f'Loop completed with {len(searched)} out of {len(accounts)} accounts for .') # add worker


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument("worker", help="Specify the number of worker from 1 - 10")
    # parser.add_argument("last_acc", help="Specify the last account the script completed.")
    #
    # args = parser.parse_args()
    # arg_worker = int(args.worker)
    # arg_last_acc = int(args.last_acc)

    county_file_name =  "pg"
    path = "complete/"

    df = pd.read_csv(f"complete/Prince George's County_SDAT.csv")
    # df_tax = pd.read_csv(f'complete/merged_complete_tax_pg_data.csv')
    
    df['parcel_ID'] = df['district'].str.rpartition("-")[2]
    df['parcel_ID'] = df['parcel_ID'].str.strip()
    
    # s_accounts = list(df['account_no'].dropna().unique().astype(int))
    # t_accounts = df_tax['account_number'].dropna().astype(int).unique()
    # accounts = [str(a).zfill(7) for a in s_accounts if not a in t_accounts]

    accounts = list(df['parcel_ID'].dropna().unique())


    # main(np.array_split(accounts, 5)[arg_worker][arg_last_acc:], arg_worker, path)
    main(accounts, path)
#%%




    

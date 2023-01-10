#%% 
from argparse import PARSER
from numpy.core.fromnumeric import sort
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
import argparse
import pandas as pd
import numpy as np
import re
import time
from functions import sdat_scraper as sdat
import sqlite3
from log_utils import create_log_object
import mysql.connector


# connection_obj = mysql.connector.connect(host='database-1.ckd6qdeu3wza.ap-northeast-1.rds.amazonaws.com',
#                                          database='haya_lee',
#                                          user='admin',
#                                          password='Qwerty12345678')
#
# cursor_obj = connection_obj.cursor()


# connection_obj = mysql.connector.connect(host='localhost',
#                                          database='haya_lee',
#                                          user='root',
#                                          password='12345678')

pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="celery_test",
            pool_size=7,
            pool_reset_session=True,
            host="localhost",
            port=3306,
            user="root",
            password="1234",
            db='property_tax'
            )
connection_obj = pool.get_connection()

cursor_obj = connection_obj.cursor()


# connection_obj = sqlite3.Connection("maryland.db")
# cursor_obj = connection_obj.cursor()
# log = create_log_object("anne_arundel_tax")

user_agent_list = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]

DRIVER_PATH = 'chromedriver.exe'
URL = 'https://aacounty.munisselfservice.com/citizens/RealEstate/Default.aspx?mode=new'
COUNTIES = ['Anne Arundel County', 'Montgomery County', "Prince George's County"]
U_COUNTIES = [i.upper() for i in COUNTIES]
ACCOUNT_ID = 'ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_Control_ParcelIdSearchFieldLayout_ctl01_ParcelIDTextBox' #id 
SEARCH_ID = 'ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_Control_FormLayoutItem7_ctl01_Button1' #id 
NEW_SEARCH_ID = 'submenulast' #class
DATA_TABLE_ID = 'ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_BillsGridView'
TAX_AMOUNT_TABLE_ID = 'tblpayment'
DESCRIPTION_TABLE_ID = 'Table3'
BLOCK_ID = 'lblblock'
OWNER_ID = 'ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_ViewBill1_OwnerLabel'
TAX_SALE_ID = 'lblinfo'
PARCEL_ID = 'ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_ViewBill1_CategoryLabel'


def get_data(driver,log):
    try:
        pageSource = driver.page_source
        df = pd.read_html(pageSource)
        x = (df[-1]['Pay By'])
        li = [i for i in x if not i.isalnum()]
        tax_month = li[-1]
        log.info(tax_month)
        # tax_month_row = driver.find_elements(By.CSS_SELECTOR, '#ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_ViewBill1_BillDetailsUpdatePanel > .datatable > tbody > tr')
        # for row in tax_month_row[::-1]:
        #     data = row.find_elements(By.TAG_NAME, "td")
        #     if len(data) == 7:
        #         tax_month = row.find_elements(By.TAG_NAME, "td")[1].text
        #         break
        #     else:
        #         tax_month = None
        # # tax_month = tax_month_row[-1].find_elements(By.TAG_NAME, 'td')[1].text
        # log.info(tax_month)
    except Exception as e:
        log.info(e)
        tax_month = None
    try:
        row = driver.find_elements(By.CSS_SELECTOR, "#ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_ViewBill1_BillDetailsUpdatePanel > table > tbody > tr")[-1]
        tax_amount = row.find_elements(By.TAG_NAME, "td")[-1].text
        # tax_amount = driver.find_element(By.XPATH,'//*[@id="ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_ViewBill1_BillDetailsUpdatePanel"]/table/tbody/tr[last()]/td[2]').text
    except: 
        tax_amount = None
    try: 
        account_number = driver.find_element(By.ID, PARCEL_ID).text
    except: 
        account_number = None
    try: 
        owner = driver.find_element(By.ID, OWNER_ID).text
    except: 
        owner = None
    sql_tax = '''INSERT INTO aa_tax ( tax_month, tax_amount, parcel_ID, owner) VALUES (%s,%s,%s,%s) '''
    log.info(f"DATA IN aa_tax TABLE : {(tax_month, tax_amount, account_number, owner)}")
    cursor_obj.execute(sql_tax,(tax_month, tax_amount, account_number, owner))
    connection_obj.commit()
    return tax_month, tax_amount, account_number, owner


def get_to_last_page(driver):
    while True:
        try:
            WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.LINK_TEXT, "Next"))).click()
        except:
            return

def new_search(driver):
    try: 
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, 'New Search'))).click()
    except: 
         WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, 'New Search'))).click()
         
def find_account_number(x):
    district = re.findall('(\d+)', x)[0].lstrip('0')
    subdiv = re.findall('(\d+)', x)[1]
    account = re.findall('(\d+)', x)[2]
    return f'{str(district)}{str(subdiv)}{str(account)}'

def func_check_site_down(driver):
    check = True
    while check:
        if driver.find_element(By.ID, "ctl00_ctl00_ContentMessageParagraph").text :
            time.sleep(900)  # SLEEP FOR 15 MINUTES
        else:
            check = False

#%% 
def main(accounts, path,log): # , worker
    driver = sdat.get_driver(DRIVER_PATH, user_agent_list)
    sdat.open_website(driver, URL)
    searched = []
    exceptions = 0
    data = pd.DataFrame(columns=['tax_month', 'tax_amount', 'parcel_ID', 'owner'])
    for account in accounts:
        log.info(account)
        account_ = find_account_number(account)
        try:
            # func_check_site_down(driver)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, ACCOUNT_ID))).send_keys(account_)
            driver.find_element(By.ID, SEARCH_ID).click()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, DATA_TABLE_ID)))
            # # if [e.text for e in driver.find_elements(By.ID, DATA_TABLE_ID)] != ['']:
            # WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_BillsGridView_ctl17_ViewBillLinkButton')))
            # driver.find_element(By.XPATH, '//*[@id="ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_BillsGridView"]/tbody/tr[last()]/td[7]').click()
            rows_list = driver.find_elements(By.CSS_SELECTOR, "#ctl00_ctl00_PrimaryPlaceHolder_ContentPlaceHolderMain_BillsGridView > tbody > tr")
            rows_list[-1].find_element(By.CSS_SELECTOR, ".nowrap > a").click()
            data.loc[len(data)] = get_data(driver,log)
            # else:
            #     log.info(account)
            #     log.info("No data found. Moving on..")
            #     pass

            log.info(f'{len(data)}/{len(accounts)} accounts done.')
            # cursor_obj.execute('''INSERT INTO aa_retry (parcel_id,index_parcel,status) VALUES (%s,%s,%s);''',(account_,accounts.index(account),"DONE"))
            # connection_obj.commit()
            searched.append(account_)
            new_search(driver)
            if len(data) % 500 == 0:
                driver.delete_all_cookies()
            else:
                pass
            cursor_obj.execute(f'''UPDATE aa_status SET status = "DONE" WHERE account = "{account}"''')
            connection_obj.commit()
        except NoSuchElementException:
            print("it came here")
            log.info("data not available")
            # cursor_obj.execute('''INSERT INTO aa_retry (parcel_id,index_parcel,status) VALUES (%s,%s,%s);''',(account_,accounts.index(account),"NO_DATA"))
            # connection_obj.commit()
            log.info(f"DATA IN aa_tax TABLE : {(account_,accounts.index(account),'NO_DATA')}")
            cursor_obj.execute(f'''UPDATE aa_status SET status = "DONE_but_no_data" WHERE account = "{account}"''')
            connection_obj.commit()
        except Exception as e :
            print("it came there")
            log.info(e)
            log.info("Something went wrong. Skipping account..")
            # cursor_obj.execute('''INSERT INTO aa_retry (parcel_id,index_parcel,status) VALUES (%s,%s,%s);''',(account,accounts.index(account),"ERROR"))
            # connection_obj.commit()
            log.info(f"DATA IN aa_tax TABLE : {(account_, accounts.index(account), 'ERROR')}")
            cursor_obj.execute(f'''UPDATE aa_status SET status = "DONE_but_no_data" WHERE account = "{account}"''')
            connection_obj.commit()

            # searched.append(account)
            # exceptions += 1
            # if exceptions > 2000:
            #     data.to_csv(f'{path}backup_pg_tax_{len(searched)}_{len(accounts)}.csv') # add worker
            #     break
            # else:
            sdat.open_website(driver, URL)
            continue

    data.to_csv(f'{path}Anne Arundel County_TAX.csv') # add worker
    log.info(f'Loop completed with {len(searched)} out of {len(accounts)} accounts for.') # add worker
#%% 
wait_to_update_db_aa = 0
def aa_tax_main_fn(start_index,end_index):
# if __name__ == '__main__':
    global wait_to_update_db_aa
    print("=============== SCRIPT STARTED ============== ")
    # parser = argparse.ArgumentParser()
    # parser.add_argument("start_index", help="Specify the start_index")
    # parser.add_argument("end_index", help="Specify the end_index")

    # args = parser.parse_args()
    # start_index = int(args.start_index)
    # end_index = int(args.end_index)
    log = create_log_object("anne_arundel_tax", start_index, end_index)
    log.info("============= started new load ==============")
    path = "complete/"
    # df = pd.read_csv(f'complete/Anne Arundel County_SDAT_test.csv')
    # log.info(df)
    # df['account_no'] = df['district'].apply(lambda x: find_account_number(x))
    # accounts = list(df['account_no'].dropna().unique())
    print("=========== scraping started =======================")
    # cursor_obj.execute('''SELECT * FROM aa_retry ORDER BY created_at DESC LIMIT 1;''')
    # db_data = cursor_obj.fetchone()
    # # log.info(db_data)
    # index_parcel_db = db_data[1] if db_data else 0
    # WHILE(FLAG!=TRUE):
    # SLEELCT * FROM SYN_AA_FLAG:
    #UPDATE FLAG =FALSE
    check = True
    while check:
        cursor_obj.execute('''SELECT flag FROM flag_table WHERE county_index = 0;''')
        data = cursor_obj.fetchall()

        if data[0][0] == 1:
            cursor_obj.execute('''UPDATE flag_table SET flag = FALSE where county_index = 0;''')
            cursor_obj.execute(f'''SELECT distinct(aa_table.district) FROM aa_table
                                    left JOIN aa_status ON aa_table.district = aa_status.account WHERE aa_status.account IS NULL;''')
            account_db = cursor_obj.fetchall()
            accounts = [i[0] for i in account_db[start_index:end_index]]
        # log.info(accounts)
        # print("loading data into status table")
        # for i in accounts:
        #     cursor_obj.execute('''INSERT INTO aa_status (account,start_index,end_index,status)
        #     VALUES (%s,%s,%s,%s)''',(i,start_index,end_index,"RUNNING"))
        #     connection_obj.commit()

            print(f"loading data into status table for :{len(accounts)}")
            query_data = []
            for i in accounts:
                query_data.append((i, start_index, end_index, "RUNNING"))
            log.info(query_data)
            query = '''INSERT INTO aa_status (account,start_index,end_index,status)
                   VALUES (%s,%s,%s,%s)'''
            cursor_obj.executemany(query, query_data)
            connection_obj.commit()
        #UPDATE FLAG=TRUE

            print("data inserted in status table")
            cursor_obj.execute('''UPDATE flag_table SET flag = TRUE where county_index = 0;''')
        # main(np.array_split(accounts, 5)[arg_worker][arg_last_acc:], arg_worker, path)
            main(accounts, path,log)
            check = False
        else:
            log.info("===== WAITING TO UPDATE DATABASE ============")
            print("============ WAITING TO UPDATE DATABASE ========== ")
            if wait_to_update_db_aa >= 5:
                cursor_obj.execute('''UPDATE flag_table SET flag = FALSE where county_index = 1;''')
                connection_obj.commit()
                wait_to_update_db_aa = 0
            time.sleep(3)


    print("============ script ended ================")
# aa_tax_main_fn(0,20)

#%%
import time
from argparse import PARSER
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
from selenium.webdriver.support.ui import Select
import argparse
import pandas as pd
import numpy as np
from functions import sdat_scraper as sdat
import sqlite3
import mysql.connector
from log_utils import create_log_object

#
# connection_obj = mysql.connector.connect(host='database-1.ckd6qdeu3wza.ap-northeast-1.rds.amazonaws.com',
#                                          database='haya_lee',
#                                          user='admin',
#                                          password='Qwerty12345678')

connection_obj = mysql.connector.connect(host='localhost',
                                         database='haya_lee',
                                         user='root',
                                         password='1234')

cursor_obj = connection_obj.cursor()


# connection_obj = sqlite3.Connection("maryland.db")
# cursor_obj = connection_obj.cursor()


user_agent_list = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]

DRIVER_PATH = 'chromedriver.exe'
URL = 'https://apps.montgomerycountymd.gov/realpropertytax/'
COUNTIES = ['Anne Arundel County', 'Montgomery County', "Prince George's County"]
U_COUNTIES = [i.upper() for i in COUNTIES]
ACCOUNT_ID = 'ctl00_MainContent_ParcelCode'
ACCOUNT_GO_ID = 'ctl00_MainContent_btnAcc'
NEW_SEARCH_ID = 'Search Bills'
DATA_TABLE_ID = 'ctl00_MainContent_tblDataDrid'
DESCRIPTION_ID = 'Table3'
VIEW_ID = 'ctl00_MainContent_grdParcel_ctl02_lnkedit2'
BILL_ID = 'ctl00_MainContent_grdParcel_ctl02_lnkedit'
OWNER_ID = 'ctl00_MainContent_lblName'
ACCOUNT_NO_ID = 'ctl00_MainContent_lblAccountNumber'
TAX_AMOUNT_ID = 'ctl00_MainContent_lblTotalAmtDue'
TAX_PERIOD_ID = 'ctl00_MainContent_lblTaxPeriod'
ADDRESS_ID = 'ctl00_MainContent_lblCompleteAddress'
OCCUPANCY_ID = 'ctl00_MainContent_lblOccupancy'
MORTGAGE_ID = 'ctl00_MainContent_lblMortgage'
PROP_ADDRESS_ID = 'ctl00_MainContent_lblPropAddress'
LOT_ID = 'ctl00_MainContent_lblLot'
BLOCK_ID = 'ctl00_MainContent_lblBlock'
DISTRICT_ID = 'ctl00_MainContent_lblDistict'
SUB_ID = 'ctl00_MainContent_lblSub'
CLASS_ID = 'ctl00_MainContent_lblClass'
CANCEL_POP_XPATH = '//*[@id="acsMainInvite"]/div/a[1]'
#%% 
def get_data(driver, tax_year):
    try: 
        owner = driver.find_element(By.ID, OWNER_ID).text
    except: 
        owner = None
    try: 
        tax_amount = driver.find_element(By.ID, TAX_AMOUNT_ID).text
    except: 
        tax_amount = None
    try: 
        tax_period = driver.find_element(By.ID, TAX_PERIOD_ID).text
    except: 
        tax_period = None
    try: 
        lot = driver.find_element(By.ID, LOT_ID).text
    except: 
        lot = None
    # try:
    #     block = driver.find_element(By.ID, BLOCK_ID).text
    # except:
    #     block = None
    # try:
    #     district = driver.find_element(By.ID, DISTRICT_ID).text
    # except:
    #     district = None
    # try:
    #     sub = driver.find_element(By.ID, SUB_ID).text
    # except:
    #     sub = None
    try: 
        class_id = driver.find_element(By.ID, CLASS_ID).text
    except: 
        class_id = None    
    # try:
    #     occupancy = driver.find_element(By.ID, OCCUPANCY_ID).text
    # except:
    #     occupancy = None
    try: 
        mortgage = driver.find_element(By.ID, MORTGAGE_ID).text
    except: 
        mortgage = None 
    # try:
    #     prop_address = driver.find_element(By.ID, PROP_ADDRESS_ID).text
    # except:
    #     prop_address = None
    # try:
    #     detail_address = driver.find_element(By.ID, ADDRESS_ID).text
    # except:
    #     detail_address = None
    try: 
        account_number =  driver.find_element(By.ID, ACCOUNT_NO_ID).text
    except:
        account_number = None
    tax_lien_status = "NO"
    sql_tax = '''INSERT INTO mnt_tax ( parcel_ID, owner, tax_amount, tax_period, lot, class_id, mortgage, tax_lien_status,tax_lien_amount, tax_year) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, %s) '''
    cursor_obj.execute(sql_tax,
                       (account_number, owner, tax_amount, tax_period, lot, class_id, mortgage, tax_lien_status,0, tax_year))
    connection_obj.commit()
    return account_number, owner, tax_amount, tax_period, lot, class_id, mortgage # detail_address, prop_address, occupancy, block, district, sub


def get_data_lien(driver,tax_amount, tax_year):
    try:
        owner = driver.find_element(By.ID, "lblarbh_name").text
    except:
        owner = None
    try:
        tax_lien_amount = driver.find_element(By.ID, "lblsales").text
    except:
        tax_lien_amount = None
    try:
        tax_period = driver.find_element(By.ID, TAX_PERIOD_ID).text
    except:
        tax_period = None
    try:
        lot = driver.find_element(By.ID, "lblLot").text
    except:
        lot = None
    # try:
    #     block = driver.find_element(By.ID, BLOCK_ID).text
    # except:
    #     block = None
    # try:
    #     district = driver.find_element(By.ID, DISTRICT_ID).text
    # except:
    #     district = None
    # try:
    #     sub = driver.find_element(By.ID, SUB_ID).text
    # except:
    #     sub = None
    try:
        class_id = driver.find_element(By.ID, "lblJurisdiction").text
    except:
        class_id = None
        # try:
    #     occupancy = driver.find_element(By.ID, OCCUPANCY_ID).text
    # except:
    #     occupancy = None
    try:
        mortgage = driver.find_element(By.ID, "lblLenderDesc").text
    except:
        mortgage = None
        # try:
    #     prop_address = driver.find_element(By.ID, PROP_ADDRESS_ID).text
    # except:
    #     prop_address = None
    # try:
    #     detail_address = driver.find_element(By.ID, ADDRESS_ID).text
    # except:
    #     detail_address = None
    try:
        account_number = driver.find_element(By.ID, "lblParcelCode").text
    except:
        account_number = None
    tax_lien_status = "YES"
    sql_tax = '''INSERT INTO mnt_tax ( parcel_ID, owner, tax_amount, tax_period, lot, class_id, mortgage, tax_lien_status,tax_lien_amount, tax_year) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, %s) '''
    cursor_obj.execute(sql_tax,(account_number, owner, tax_amount, tax_period, lot, class_id, mortgage, tax_lien_status,tax_lien_amount, tax_year))
    connection_obj.commit()
    return account_number, owner, tax_amount, tax_period, lot, class_id, mortgage  # detail_address, prop_address, occupancy, block, district, sub

def get_to_last_page(driver):
    while True:
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "Next"))).click()
        except:
            return log.info("At the last page")

def new_search(driver):
    try: 
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.LINK_TEXT, 'Search Bills'))).click()
    except: 
        try: 
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, CANCEL_POP_XPATH))).click()
        except: 
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.LINK_TEXT, 'Search Bills'))).click()

def view_details(driver):
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, BILL_ID))).click()
    except: 
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, VIEW_ID))).click()



def main(data, accounts, path, start_index, end_index):
    searched = []
    driver = sdat.get_driver(DRIVER_PATH, user_agent_list)
    sdat.open_website(driver, URL)

    for account in accounts:
        account_ = account.strip()
        log.info(account)
        # try:
        #     table_rows = driver.find_element(By.CSS_SELECTOR, ".tblReptorcls > tbody > tr")
        #     table_rows[1].find_elements(By.TAG_NAME, "td")[-1].click()
        #     data.loc[len(data)] = get_data(driver)
        #     searched.append(account)
        #     log.info(f'{len(data)} account done')
        # except Exception as e:
        #     log.info(e)
        #     print(e)
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, ACCOUNT_ID))).send_keys(account_)
            driver.find_element(By.ID, ACCOUNT_GO_ID).click()
            try:
                try:
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".tblReptorcls > tbody > tr")))
                    table_rows = driver.find_elements(By.CSS_SELECTOR, ".tblReptorcls > tbody > tr")
                    tax_amount = driver.find_element(By.ID, "ctl00_MainContent_grdParcel_ctl02_Label1").text
                    tax_year = driver.find_element(By.CSS_SELECTOR, "#aspnetForm > section > div > div.row > div.container > div:nth-child(4) > table > tbody > tr:nth-child(2) > td:nth-child(1)")[0].text
                    table_rows[1].find_elements(By.TAG_NAME, "td")[-1].click()
                    time.sleep(3)
                    data.loc[len(data)] = get_data_lien(driver,tax_amount, tax_year)
                except:
                # view_details(driver)
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#ctl00_MainContent_grdParcel > tbody > tr")))
                    table_rows = driver.find_elements(By.CSS_SELECTOR, "#ctl00_MainContent_grdParcel > tbody > tr")
                    tax_year = driver.find_elements(By.CSS_SELECTOR, "#ctl00_MainContent_grdParcel > tbody > tr:nth-child(2) > td:nth-child(1)")[0].text
                    table_rows[1].find_elements(By.TAG_NAME, "td")[-1].click()
                    time.sleep(3)
                    data.loc[len(data)] = get_data(driver, tax_year)
                searched.append(account)
                log.info(f'{len(data)} account done')
                cursor_obj.execute(f'''UPDATE mnt_status SET status = "DONE" WHERE account = "{account}"''')
                connection_obj.commit()
            except Exception as e:
                log.info(e)
                log.info("No data. Moving on..")
                # cursor_obj.execute('''INSERT INTO MNT_RETRY (parcel_id,index_parcel,status,start_index,end_index) VALUES (%s,%s,%s,%s,%s);''',
                #                    (account, accounts.index(account), "NO_DATA", start_index, end_index))
                # connection_obj.commit()
                # log.info(f"DATA IN MNT_RETRY TABLE : {(account, accounts.index(account), 'NO_DATA')}")
                cursor_obj.execute(f'''UPDATE mnt_status SET status = "NO_DATA" WHERE account = "{account}"''')
                connection_obj.commit()
                new_search(driver)
                searched.append(account)
                continue
            try:
                new_search(driver)
            except:
                driver.back()
                time.sleep(3)
                new_search(driver)
            if len(data) % 500 == 0:
                driver.delete_all_cookies()
            else:
                pass
        except:
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="acsMainInvite"]/div/a[1]'))).click()
                log.info("Clicked pop up..")
                sdat.open_website(driver, URL)
                continue
            except:
                data.to_csv(f'{path}backup_pg_tax_{len(searched)}_{len(accounts)}.csv') # add worker
                log.info("Something went wrong. Skipping account..")
                print((account, accounts.index(account), "ERROR", start_index, end_index))
                # cursor_obj.execute('''INSERT INTO MNT_RETRY (parcel_id,index_parcel,status,start_index,end_index)
                #  VALUES (%s,%s,%s,%s,%s);''',(account, accounts.index(account), "ERROR", start_index, end_index))
                # connection_obj.commit()
                # log.info(f"DATA IN  MNT_RETRY TABLE : {(account, accounts.index(account), 'ERROR')}")
                cursor_obj.execute(f'''UPDATE mnt_status SET status = "ERROR" WHERE account = "{account}"''')
                connection_obj.commit()
                sdat.open_website(driver, URL)
                continue
        
    data.to_csv(f'{path}Montgomery County_TAX_test.csv') # add worker
    log.info(f'Loop completed with {len(searched)} out of {len(accounts)} accounts for.') # add worker

#%% 
if __name__ == '__main__':
    print("============== script started ==================")

    parser = argparse.ArgumentParser()
    parser.add_argument("start_index", help="Specify the start_index")
    parser.add_argument("end_index", help="Specify the end_index")

    args = parser.parse_args()
    start_index = int(args.start_index)
    end_index = int(args.end_index)
    log = create_log_object("montgomery_tax", start_index, end_index)
    log.info("============= started new load ==============")

    county_file_name =  "mnt"
    path = "complete/"

    check = True
    while check:
        print(f"about to check database")
        cursor_obj.execute('''SELECT flag FROM flag_table WHERE county_index = 1;''')
        data = cursor_obj.fetchall()
        print(f"flag : {data[0][0]}")
        if data[0][0] == 1:
            cursor_obj.execute('''UPDATE flag_table SET flag = FALSE where county_index = 1;''')
    # cursor_obj.execute(f'''SELECT DISTINCT account FROM mnt_table;''')
            cursor_obj.execute(f'''SELECT distinct(mnt_table.parcel_ID) FROM mnt_table  
                                    left JOIN mnt_status ON trim(mnt_table.parcel_ID) = trim(mnt_status.account) WHERE mnt_status.account IS NULL;''')
            accounts_db = cursor_obj.fetchall()
            accounts = [i[0] for i in accounts_db[start_index:end_index] ]
            query_data = []
            for i in accounts:
                query_data.append((i,start_index,end_index,"RUNNING"))
                # cursor_obj.execute('''INSERT INTO pg_status (account,start_index,end_index,status)
                # VALUES (%s,%s,%s,%s)''',(i,start_index,end_index,"RUNNING"))
                # connection_obj.commit()
            log.info(query_data)
            print(len(query_data))

            query = '''INSERT INTO mnt_status (account,start_index,end_index,status)
                VALUES (%s,%s,%s,%s)'''
            cursor_obj.executemany(query, query_data)
            connection_obj.commit()
            print("data inserted in status table")
            cursor_obj.execute('''UPDATE flag_table SET flag = TRUE where county_index = 1;''')
            data = pd.DataFrame(columns=['parcel_ID',
                                         'owner',
                                         'tax_amount',
                                         'tax_period',
                                         'lot',
                                         # 'block',
                                         # 'district',
                                         # 'sub',
                                         'class_id',
                                         # 'occupancy',
                                         'mortgage',
                                         # 'prop_address',
                                         # 'detail_address',
                                         ],
                                )
            # df = pd.read_csv(f"complete/Montgomery County_SDAT_test.csv")
            # # df_tax = pd.read_csv(f'complete/merged_complete_tax_{county_file_name}_data.csv')
            #
            # df['parcel_ID'] = df['district'].str.rpartition("-")[2]
            # df['parcel_ID'] = df['parcel_ID'].str.strip()
            # # s_accounts = list(df['account_no'].dropna().unique().astype(int))
            # # t_accounts = df_tax['account_number'].dropna().astype(int).unique()
            # # accounts = [str(a).zfill(8) for a in s_accounts if not a in t_accounts]
            # accounts = list(df['parcel_ID'].dropna().unique())

            # main(np.array_split(accounts, 5)[arg_worker][arg_last_acc:], arg_worker, path)
            main(data, accounts, path, start_index, end_index)
            check = False
        else:
            log.info("===== WAITING TO UPDATE DATABASE ============")
            print("============ WAITING TO UPDATE DATABASE ========== ")
            time.sleep(10)
    print("=========== ended script =====================")
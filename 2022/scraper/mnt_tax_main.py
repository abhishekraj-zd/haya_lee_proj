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
def get_data(driver):
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
                          
    return account_number, owner, tax_amount, tax_period, lot, class_id, mortgage # detail_address, prop_address, occupancy, block, district, sub


def get_data_lien(driver):
    try:
        owner = driver.find_element(By.ID, "lblarbh_name").text
    except:
        owner = None
    try:
        tax_amount = driver.find_element(By.ID, "lblsales").text
    except:
        tax_amount = None
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

    return account_number, owner, tax_amount, tax_period, lot, class_id, mortgage  # detail_address, prop_address, occupancy, block, district, sub

def get_to_last_page(driver):
    while True:
        try:
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "Next"))).click()
        except:
            return print("At the last page")

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



def main(accounts, path):
    searched = []
    driver = sdat.get_driver(DRIVER_PATH, user_agent_list)
    sdat.open_website(driver, URL)
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
    for account in accounts:
        print(account)
        try:
            table_rows = driver.find_element(By.CLASS_NAME, ".tblReptorcls > tbody > tr")
            table_rows[1].find_elements(By.TAG_NAME, "td")[-1].click()
            data.loc[len(data)] = get_data(driver)
            searched.append(account)
            print(f'{len(data)} account done')
        except:
            try:
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, ACCOUNT_ID))).send_keys(account)
                driver.find_element(By.ID, ACCOUNT_GO_ID).click()
                try:
                    try:
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".tblReptorcls > tbody > tr")))
                        table_rows = driver.find_elements(By.CSS_SELECTOR, ".tblReptorcls > tbody > tr")
                        table_rows[1].find_elements(By.TAG_NAME, "td")[1].click()
                        data.loc[len(data)] = get_data_lien(driver)
                    except:
                    # view_details(driver)
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#ctl00_MainContent_grdParcel > tbody > tr")))
                        table_rows = driver.find_elements(By.CSS_SELECTOR, "#ctl00_MainContent_grdParcel > tbody > tr")
                        table_rows[1].find_elements(By.TAG_NAME, "td")[-1].click()
                        data.loc[len(data)] = get_data(driver)
                    searched.append(account)
                    print(f'{len(data)} account done')
                except Exception as e:
                    print(e)
                    print("No data. Moving on..")
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
                    print("Clicked pop up..")
                    sdat.open_website(driver, URL)
                    continue
                except:
                    data.to_csv(f'{path}backup_pg_tax_{len(searched)}_{len(accounts)}.csv') # add worker
                    print("Something went wrong. Skipping account..")
                    sdat.open_website(driver, URL)
                    continue
        
    data.to_csv(f'{path}Montgomery County_TAX_test.csv') # add worker
    print(f'Loop completed with {len(searched)} out of {len(accounts)} accounts for.') # add worker

#%% 
if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument("worker", help="Specify the number of worker from 1 - 5")
    # parser.add_argument("last_acc", help="Specify the last account the script completed.")
    #
    #
    # args = parser.parse_args()
    # arg_worker = int(args.worker)
    # arg_last_acc = int(args.last_acc)

    county_file_name =  "mnt"
    path = "complete/"


    df = pd.read_csv(f"complete/Montgomery County_SDAT_test.csv")
    # df_tax = pd.read_csv(f'complete/merged_complete_tax_{county_file_name}_data.csv')
    
    df['parcel_ID'] = df['district'].str.rpartition("-")[2]
    df['parcel_ID'] = df['parcel_ID'].str.strip()
    # s_accounts = list(df['account_no'].dropna().unique().astype(int))
    # t_accounts = df_tax['account_number'].dropna().astype(int).unique()
    # accounts = [str(a).zfill(8) for a in s_accounts if not a in t_accounts]
    accounts = list(df['parcel_ID'].dropna().unique())

    # main(np.array_split(accounts, 5)[arg_worker][arg_last_acc:], arg_worker, path)
    main(accounts[250:280], path)

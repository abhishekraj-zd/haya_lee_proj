from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
import mysql.connector.pooling
import pandas as pd
import time
import timeit
from functions import sdat_scraper as sdat
from selenium.webdriver.common.keys import Keys
import sqlite3
import argparse
from final import find_account_number
from log_utils import create_log_object

data_list_aa = []
data_list_mnt = []
data_list_pg = []
data_list_retry = []
'''MY SQL CONNECTION'''

# connection_obj = mysql.connector.connect(host='database-1.ckd6qdeu3wza.ap-northeast-1.rds.amazonaws.com',
#                                          database='haya_lee',
#                                          user='admin',
#                                          password='Qwerty12345678')

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



# '''sql lite connection'''
# connection_obj = sqlite3.Connection("maryland.db")
# cursor_obj = connection_obj.cursor()
# index_of_street_db = 0

use_restrict_list = ["COMMERCIAL", "INDUSTRIAL", "AGRICULTURE", "APARTMENT", "CONDOMINIUM", "COMMERCIAL CONDOMINIUM"]

user_agent_list = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]

exception_streets = ["allard", "anne chambers"]

DRIVER_PATH = 'chromedriver.exe'
URL = 'https://sdat.dat.maryland.gov/RealProperty/Pages/default.aspx'
COUNTIES = ['Anne Arundel County', 'Montgomery County', "Prince George's County"]
U_COUNTIES = [i.upper() for i in COUNTIES]
COUNTY_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchType_ddlCounty'
METHOD_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchType_ddlSearchType'
SEARCH_METHOD = ['STREET ADDRESS' , 'PROPERTY ACCOUNT IDENTIFIER', 'MAP/PARCEL', 'PROPERTY SALES']
CONTINUE_CLASS = 'btnNext'
STREET_NAME_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucEnterData_txtStreetName'
NEW_SEARCH_ID = 'btnNewSearch'
PREVIOUS_CLASS = 'btnPrevious verifyCancel'
RESULT_PREVIOUS_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_btnStepPreviousButton_top'
HOME_PREVIOUS_ID = "cphMainContentArea_ucSearchType_wzrdRealPropertySearch_StepNavigationTemplateContainerID_btnStepPreviousButton"
SEARCH_RESULT_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchResult_gv_SearchResult'
DETAILS_PREVIOUS_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_btnPrevious_top2'
ERROR_ID = 'cphMainContentArea_ucSearchType_lblErr'

# -------SDAT DATA ID------------ #
DISTRICT_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblDetailsStreetHeader_0'
ACCOUNT_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblDetailsStreetHeader_0'
OWNER_NAME_ID_1 =  'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblOwnerName_0'
OWNER_NAME_ID_2 = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblOwnerName2_0'
OWNER_NAME_ID_3 = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblOwnerName3_0'
MAILING_ADDRESS_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblMailingAddress_0'
USE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblUse_0'
PRINCIPAL_RESIDENCE_ID =  'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblPrinResidence_0'
DEED_REFERENCE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblDedRef_0'
MAP_ID =  'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label5_0'
PARCEL_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label7_0'
BLOCK_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label11_0'
SUBDIVISION_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label9_0'
PLAT_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label1_0'
STRUCTURE_BUILT_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label18_0'
LIVING_AREA_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label19_0'
LAND_AREA_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label20_0'
BASEMENT_AREA_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label27_0'
BASEMENT_ID = "cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label23_0"
LAND_VALUE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblBaseLandNow_0'
ASSESSED_VALUE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblBaseTotalNow_0'
IMPROVEMENT_VALUE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblBaseImproveNow_0'
SELLER_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label38_0'
DATE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label39_0'
PRICE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label40_0'
PREMISES_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblPremisesAddress_0'
TRANSFER_INFO_TYPE_ID = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label41_0'
HOMESTEAD_APPLICATION_STATUS = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblHomeStatus_0'
HOMEOWNER_TAX_CREDIT = 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblHTC_Status_0'
LEGAL_DESCRIPTION = "cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_lblLegalDescription_0"
STORIES = "cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label22_0"
BATH = "cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucDetailsSearch_dlstDetaisSearch_Label34_0"
#%%

from time import perf_counter

# arg_county_index = 1
# start_index = 1501
# end_index = 2000
#
# if COUNTIES[arg_county_index] == 'Anne Arundel County':
#     county_file_name = "anne_arundel"
#     path = "complete/"
# elif COUNTIES[arg_county_index] == 'Montgomery County':
#     county_file_name = "montgomery"
#     path = "complete/"
# else:
#     county_file_name = "prince_george_s"
#     path = "complete/"
#
# log = create_log_object(county_file_name, start_index, end_index)

class catchtime:
    def __enter__(self):
        self.start_time = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        end_time = perf_counter()
        self.total_time = end_time - self.start_time
        self.readout = f'Time: {self.total_time:.3f} seconds and started at : {self.start_time} and finished at : {end_time}'
        # log.info(self.readout)

def get_data(driver, county,log):
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, DISTRICT_ID)))
    global data_list_aa
    global data_list_mnt
    global data_list_pg
    try:
        district = driver.find_element(By.ID, DISTRICT_ID).text
    except:
        district = None
    try:
        account_no = driver.find_element(By.ID, ACCOUNT_ID).text.split("Account Number -")[1]
    except:
        account_no = None
    try:
        log.info(f"OWNER NAME : {driver.find_element(By.ID, OWNER_NAME_ID_1).text}")
        if "&" in driver.find_element(By.ID, OWNER_NAME_ID_1).text:
            owner_name_1 = driver.find_element(By.ID, OWNER_NAME_ID_1).text.split("&")[0]
            owner_name_2 = driver.find_element(By.ID, OWNER_NAME_ID_1).text.split("&")[1]
        else:
            owner_name_1 = driver.find_element(By.ID, OWNER_NAME_ID_1).text.strip()
            owner_name_2 = driver.find_element(By.ID, OWNER_NAME_ID_2).text.strip()
        try:
            # owner_name_1 = driver.find_element(By.ID, OWNER_NAME_ID_1).text
            name_list = owner_name_1.split(" ")
            owner_name_1_first_name = name_list[0]
            name_list.pop(0)
            owner_name_1_last_name = " ".join(name_list)

        except:
            owner_name_1_first_name = None
            owner_name_1_last_name = None
        try:
            # owner_name_2 = driver.find_element(By.ID, OWNER_NAME_ID_2).text
            name_list = owner_name_2.split(" ")
            owner_name_2_first_name = name_list[0]
            name_list.pop(0)
            owner_name_2_last_name = " ".join(name_list)

        except:
            owner_name_2_first_name = None
            owner_name_2_last_name = None
    except:
        owner_name_1_first_name = None
        owner_name_1_last_name = None
        owner_name_2_first_name = None
        owner_name_2_last_name = None
    # try:
    #     owner_name_3 = driver.find_element(By.ID, OWNER_NAME_ID_3).text
    # except:
    #     owner_name_3 = None
    try:
        mailing = driver.find_element(By.ID, MAILING_ADDRESS_ID ).text
    except:
        mailing = None
    try:
        use = driver.find_element(By.ID, USE_ID).text
    except:
        use = None
    try:
        principal_residence = driver.find_element(By.ID, PRINCIPAL_RESIDENCE_ID).text
    except:
        principal_residence = None
    try:
        deed_reference = driver.find_element(By.ID, DEED_REFERENCE_ID).text
    except:
        deed_reference = None
    try:
        map_id = driver.find_element(By.ID, MAP_ID).text
    except:
        map_id = None
    try:
        parcel = driver.find_element(By.ID, PARCEL_ID).text
    except:
        parcel = None
    try:
        block_id = driver.find_element(By.ID, BLOCK_ID).text
    except:
        block_id = None
    try:
        subdivision = driver.find_element(By.ID, SUBDIVISION_ID).text
    except:
        subdivision = None
    try:
        plat_id = driver.find_element(By.ID, PLAT_ID).text
    except:
        plat_id = None
    try:
        structure_built = driver.find_element(By.ID, STRUCTURE_BUILT_ID).text
    except:
        structure_built = None
    try:
        living_area = driver.find_element(By.ID, LIVING_AREA_ID).text
    except:
        living_area = None
    try:
        land_area = driver.find_element(By.ID, LAND_AREA_ID).text
    except:
        land_area = None
    try:
        basement = driver.find_element(By.ID, BASEMENT_ID).text
        if basement == "":
            basement = "NA"
    except:
        basement = None
    try:
        finished_basement_area = driver.find_element(By.ID, BASEMENT_AREA_ID).text
        if finished_basement_area == "":
            finished_basement_area = "NA"
    except:
        finished_basement_area = "NA"
    try:
        land_value = driver.find_element(By.ID, LAND_VALUE_ID).text
    except:
        land_value = None
    try:
        improvement_value = driver.find_element(By.ID, IMPROVEMENT_VALUE_ID).text
    except:
        improvement_value = None
    try:
        seller = driver.find_element(By.ID, SELLER_ID).text
    except:
        seller = None
    try:
        date = driver.find_element(By.ID, DATE_ID).text
    except:
        date = None
    try:
        price = driver.find_element(By.ID, PRICE_ID).text
    except:
        price = None
    try:
        premises = driver.find_element(By.ID, PREMISES_ID).text
    except:
        premises = None
    try:
        assessed_value = driver.find_element(By.ID, ASSESSED_VALUE_ID).text
    except:
        assessed_value = None
    try:
        transfer_type = driver.find_element(By.ID, TRANSFER_INFO_TYPE_ID).text
    except:
        transfer_type = None
    try:
        homestead_application_status_list = driver.find_element(By.ID, HOMESTEAD_APPLICATION_STATUS).text
        if "Approved" in homestead_application_status_list:
            homestead_application_status = "Yes"
        else:
            homestead_application_status = "No"
    except:
        homestead_application_status = False
    try:
        homeowner_tax_credit = driver.find_element(By.ID, HOMEOWNER_TAX_CREDIT).text
    except:
        homeowner_tax_credit = None
    try:
        legal_description = driver.find_element(By.ID, LEGAL_DESCRIPTION).text
    except:
        legal_description = None
    try:
        stories = driver.find_element(By.ID, STORIES).text
    except:
        stories = None
    try:
        bath = driver.find_element(By.ID, BATH).text
    except:
        bath = None
    if county == 0:
        table = "aa_table"
        parcel_id = find_account_number(district)
    elif county == 1:
        table = "mnt_table"
    else:
        table = "pg_table"
    # parcel_id ='0'
    # maryland_sql = f'''INSERT INTO {table}
    #                     (district,account,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath,parcel_id)
    #                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
    # maryland_data = (district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name, owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel, block_id, subdivision, plat_id, structure_built, living_area, land_area, basement, finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price, transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath,parcel_id)
    # if use.upper() not in use_restrict_list:
    #     try:
    #         cursor_obj.execute(maryland_sql, maryland_data)
    #         # connection_obj.commit()
    #     except Exception as e:
    #         print(cursor_obj.statement)
    #         raise e
    #
    #     log.info("===================== DATA ADDED TO DATABASE =============================")
    if county == 0:
        table = "aa_table"
        parcel_id = find_account_number(district)
        data = (district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name,
                owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel,
                block_id, subdivision, plat_id, structure_built, living_area, land_area, basement,
                finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price,
                transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath,
                parcel_id)
        if use.upper() not in use_restrict_list:
            data_list_aa.append(data)
        aa_sql = f'''INSERT INTO {table} 
                                (district,account,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath,parcel_id)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        # maryland_data = (district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name, owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel, block_id, subdivision, plat_id, structure_built, living_area, land_area, basement, finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price, transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath,parcel_id)
        aa_data = data_list_aa
        # length_aa_data = len(aa_data)
        
        # log.info("aa_data length======>>>>",length_aa_data)
        if len(aa_data) >= 5:
            try:
                cursor_obj.executemany(aa_sql, aa_data)
                connection_obj.commit()
                data_list_aa = []
                log.info("===================== DATA ADDED TO DATABASE =============================")
            except Exception as e:
                # log.info("aa database error  ===>>>",e)
                print("error ",e)
                print(cursor_obj.statement)
                raise e
    elif county == 1:
        table = "mnt_table"
        data = (district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name,
                owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel,
                block_id, subdivision, plat_id, structure_built, living_area, land_area, basement,
                finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price,
                transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath)
        if use.upper() not in use_restrict_list:
            data_list_mnt.append(data)
        mnt_sql = f'''INSERT INTO {table} 
                                (district,parcel_ID,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        # maryland_data = (district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name, owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel, block_id, subdivision, plat_id, structure_built, living_area, land_area, basement, finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price, transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath,parcel_id)
        mnt_data = data_list_mnt
        # log.info("mnt_data  length ===>>>>",len(mnt_data))
        if len(mnt_data) >= 5:
            try:
                cursor_obj.executemany(mnt_sql, mnt_data)
                connection_obj.commit()
                data_list_mnt = []
                log.info("===================== DATA ADDED TO DATABASE =============================")
            except Exception as e:
                # log.info("mnt database error ====>>>",e)
                print(cursor_obj.statement)
                raise e
    else:
        table = "pg_table"
        data = (district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name,
                owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel,
                block_id, subdivision, plat_id, structure_built, living_area, land_area, basement,
                finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price,
                transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath)
        if use.upper() not in use_restrict_list:
            data_list_pg.append(data)
        pg_sql = f'''INSERT INTO {table} 
                                (district,parcel_ID,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        # maryland_data = (district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name, owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel, block_id, subdivision, plat_id, structure_built, living_area, land_area, basement, finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price, transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath,parcel_id)
        pg_data = data_list_pg
        # log.info("pg data length =====>>>>",len(pg_data))
        if len(pg_data) == 5:
            try:
                cursor_obj.executemany(pg_sql, pg_data)
                connection_obj.commit()
                data_list_pg = []
                log.info("===================== DATA ADDED TO DATABASE =============================")
            except Exception as e:
                # log.info("pg database error ====>>>>>",e)
                print(cursor_obj.statement)
                raise e
    return district, str(account_no), owner_name_1_last_name, owner_name_1_first_name, owner_name_2_last_name, owner_name_2_first_name, mailing, premises, use, principal_residence, deed_reference, map_id, parcel, block_id, subdivision, plat_id, structure_built, living_area, land_area, basement, finished_basement_area, land_value, improvement_value, assessed_value, seller, date, price, transfer_type, homestead_application_status, homeowner_tax_credit, legal_description, stories, bath

def run_loop(data, driver, DETAILS_PREVIOUS_ID, county, path, index_of_street, street, page, start_row, start, end,log):
    first_row, last_row, rows = sdat.find_row_details(driver, start_row)
    global data_list_retry
    for i in range(first_row, last_row+1):
    # for i in range(first_row, first_row+5):
        try:
            # log.info(f"searching for {sdat.row_text(driver, i)}")
            sdat.click_row(driver, i)
            # time.sleep(5)
            # use_check = driver.find_element(By.ID, USE_ID).text
            use_check = WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.ID, USE_ID))).text
            if use_check.upper() not in use_restrict_list:
                # log.info(data)
                log.info(len(data))
                data.loc[len(data)] = get_data(driver,county,log)
                log.info("========== DATA SCRAPE =============")
                status = "FETCHED"
            else:
                status = "NO_NEED_TO_SCRAPE"
                pass


        # except (NoSuchElementException, TimeoutException, ElementNotInteractableException):
        except Exception as e:
            print(e)
            log.info(e)
            log.info(f"No data for {i}. Skipping to next row..")
            status = "ERROR_NO_DATA"
            # continue
        log.info(
            f" the query string is : {(county, COUNTIES[county], index_of_street, street, page, i, status, start, end)}")
        data_list_retry.append((county, COUNTIES[county], index_of_street, street, page, i, status, start, end))
        if len(data_list_retry) == 5:
            cursor_obj.executemany(
                '''INSERT INTO retry_table (county_index, county_name, street_index, street, page_no, row_, status, start_index, end_index) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);''',data_list_retry)
            data_list_retry = []
            connection_obj.commit()
            log.info("============================= RETRY_INFO ADDED TO DATABASE ============================")
        sdat.go_back(driver, DETAILS_PREVIOUS_ID)
    return data, last_row

def get_data_in_all_pages(data, driver, total_pages, DETAILS_PREVIOUS_ID, search_term, sleeptime, county, path, index_of_street, current_page, start_row, start, end,log):
    total_pages = int(total_pages)
    # current_page = sdat.current_page(driver)
    # current_page = int(current_page)
    # if current_page > 1 and current_page < 10:
    #     current_page = 11
    # elif current_page > 10:
    #     current_page = ((current_page // 10) * 10 ) + 1
    #     driver.find_element(By.LINK_TEXT, str(current_page)).click()
    #     time.sleep(5)
    log.info(f"CURRENT PAGE : {current_page}")
    try:
        driver.find_element(By.LINK_TEXT, str(current_page)).click()
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID,
                                                                    'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchResult_gv_SearchResult_txtOwnerName_1')))
    except:
        pass
    while total_pages >= current_page:
        data, last_row = run_loop(data, driver, DETAILS_PREVIOUS_ID, county, path, index_of_street, search_term,current_page, start_row, start, end,log)

        log.info(f"'{search_term}' page {current_page} completed with {last_row} rows..")
        current_page += 1
        if current_page > total_pages:
            return data
        else:
            try:
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.LINK_TEXT, f"{current_page}"))).click()
                start_row = 0
                log.info(f"Clicked on page {current_page}.")
                time.sleep(10)
            except:
                if total_pages - current_page > 1:
                    log.info(f"Cannot access page {current_page}. Skipping to {current_page+1} page.")
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, f"{current_page+1}"))).click()
                    time.sleep(10)
                    continue
                else:
                    log.info("No more pages to skip to. Breaking out of loop to next search term.")
                    return data
    return data

def loop_search_terms(driver, search_terms, data, rand_sec, STREET_NAME_ID, CONTINUE_CLASS, RESULT_PREVIOUS_ID, DETAILS_PREVIOUS_ID, county, path, index_of_street_db, page, start_row, start, end,log,arg_county_index):
    searched_terms = []
    global data_list_retry

    try:
        for street in search_terms:
            with catchtime() as t:
                try:
                    print(f"street name : {street} search start")
                    log.info(f"street name : {street} search start")
                    driver.find_element(By.ID, STREET_NAME_ID).send_keys(street)
                    driver.find_element(By.CLASS_NAME, CONTINUE_CLASS).click()
                    try:
                        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, 'cphMainContentArea_ucSearchType_wzrdRealPropertySearch_ucSearchResult_gv_SearchResult_txtOwnerName_1')))
                    except (NoSuchElementException, TimeoutException, ElementNotInteractableException):
                        try:
                            log.info(f"No results found for {street}. Moving on..")
                            searched_terms.append(street)
                            sdat.go_back(driver, RESULT_PREVIOUS_ID)
                            time.sleep(5)
                            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, STREET_NAME_ID))).clear()
                            time.sleep(5)
                            status = "NO_STREET_DATA"

                            log.info(
                                 f" the query string is : {(county, COUNTIES[county], index_of_street, street, page, i, status, start, end)}")
                            data_list_retry.append((county, COUNTIES[county], index_of_street, street, page, i, status, start, end))
                            if len(data_list_retry) == 5:
                                    cursor_obj.executemany(
                                '''INSERT INTO retry_table (county_index, county_name, street_index, street, page_no, row_, status, start_index, end_index) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);''',data_list_retry)
                                    data_list_retry = []
                                    connection_obj.commit()
                                    log.info("========= Loading data into RETRY_INFO table ============================")
                            index_of_street_db += 1
                            log.info("========== Loading data to database ============================")
                            continue
                        except:
                            log.info(f" ONLY ONE RESULT FOUND FOR {street}. Proceeding to scrape data")
                            use_check = driver.find_element(By.ID, USE_ID).text
                            if use_check.upper() not in use_restrict_list:
                                # log.info(data)
                                log.info(len(data))
                                data.loc[len(data)] = get_data(driver, county,log)
                                log.info("=============== DATA SCRAPED ==============")
                                status = "FETCHED"
                            else:
                                status = "NO_NEED_TO_SCRAPE"
                                pass
                            sdat.go_back(driver, DETAILS_PREVIOUS_ID)
                            time.sleep(5)
                            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, STREET_NAME_ID))).clear()
                            time.sleep(5)

                            log.info(
                                    f" the query string is : {(county, COUNTIES[county], index_of_street, street, page, i, status, start, end)}")
                            data_list_retry.append((county, COUNTIES[county], index_of_street, street, page, i, status, start, end))
                            if len(data_list_retry) == 5:
                                cursor_obj.executemany(
                                    '''INSERT INTO retry_table (county_index, county_name, street_index, street, page_no, row_, status, start_index, end_index) 
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);''',data_list_retry)
                                data_list_retry = []
                                connection_obj.commit()
                                log.info("========= Loading data into RETRY_INFO table ============================")
                            index_of_street_db += 1
                            
                            continue
                    total_pages = sdat.page_skips(driver, 10)
                    if int(total_pages) > 0:
                        page_skips = sdat.roundup(int(total_pages) - 1)/10 - 1
                        if page_skips > 0:
                            log.info(f"'{street}' has more than 10 pages. Proceeding to scrape all {total_pages} pages with {page_skips} page skips.")
                            time.sleep(rand_sec)
                            skipped_count = 0
                            while skipped_count <= page_skips:
                                pages_in_page = sdat.last_page(driver)
                                pages_in_page = int(pages_in_page)
                                data = get_data_in_all_pages(data, driver, pages_in_page, DETAILS_PREVIOUS_ID, street, rand_sec, county, path, index_of_street_db, page, start_row, start, end,log)
                                index_of_street_db += 1
                                time.sleep(1)
                                if skipped_count == page_skips:
                                    log.info(f"Skips completed for {street}.")
                                    skipped_count += 1
                                    continue
                                else:
                                    sdat.next_skip(driver)
                                    driver.find_element(By.TAG_NAME,"body").send_keys(Keys.HOME)
                                    time.sleep(rand_sec)
                                    log.info(f"Skipped to the next set of pages for {street}.")
                                    skipped_count += 1
                                    time.sleep(10)
                        else:
                            log.info(f"{street} has {total_pages} pages. Proceeding to scrape all.")
                            data = get_data_in_all_pages(data, driver, total_pages, DETAILS_PREVIOUS_ID, street, rand_sec, county, path, index_of_street_db, page, start_row, start, end,log)
                            index_of_street_db += 1
                    else:
                        time.sleep(3)
                        log.info(f"{street} has only one page. Proceeding to scrape all data.")
                        data, last_row = run_loop(data, driver, DETAILS_PREVIOUS_ID, county, path, index_of_street_db, street, page, start_row, start, end,log)
                        index_of_street_db += 1
                        log.info(f"{street} completed with {last_row} rows.")
                        time.sleep(1)
                    searched_terms.append(street)
                    driver.delete_all_cookies()
                    sdat.go_back(driver, RESULT_PREVIOUS_ID)
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, STREET_NAME_ID))).clear()
                    start_row = 0
                    page = 1
                except KeyboardInterrupt as e:
                    raise e
                except Exception as e:
                    log.info(e)
                    log.info("**********  Retring main search due to some error **********")
                    cursor_obj.execute(f'''SELECT * FROM retry_table WHERE county_name = "{COUNTIES[arg_county_index]}" AND start_index = "{start}" AND end_index = "{end}" ORDER BY created_at DESC LIMIT 1;''')
                    db_data = cursor_obj.fetchone()
                    log.info(f"retry db_data {db_data}")
                    county_db = db_data[0] if db_data else 0
                    index_of_street_db = db_data[2] if db_data else 0
                    current_page_db = db_data[4] if db_data else 0
                    last_row_db = db_data[5] if db_data else 0
                    start_index_db = db_data[8] if db_data else 0
                    end_index_db = db_data[9] if db_data else 500
                    main_search(driver, search_terms[int(index_of_street_db):], int(county_db), path, int(index_of_street_db),
                                int(current_page_db), int(last_row_db),int(start_index_db), int(end_index_db),log,arg_county_index)

    except Exception as e:
        log.info(e)
        log.info("Something unexpected happened.")
    finally:
        return data, searched_terms


#%%
def main_search(driver, search_terms, county, path, index_of_street, page, start_row, start, end,log,arg_county_index):
    start_time_1 = timeit.default_timer()
    # searched_terms = []
    sdat.open_website(driver, URL)
    time.sleep(3)
    sdat.select_option(driver, COUNTY_ID, U_COUNTIES, county)
    sdat.select_option(driver, METHOD_ID, SEARCH_METHOD, 0)
    # time.sleep(3)
    driver.find_element(By.CLASS_NAME, CONTINUE_CLASS).click()
    time.sleep(3)
    rand_sec = 5
    # 'mailing_address' removed this column from dataframe
    data = pd.DataFrame(columns=['district',
                                 'account',
                                 'owner 1 first name',
                                 'owner 1 last name',
                                 'owner 2 first name',
                                 'owner 2 last name',
                                 'mailing',
                                 'premises',
                                 'use',
                                 'principal_residence',
                                 'deed_reference',
                                 'map_id',
                                 'parcel',
                                 'block',
                                 'subdivision',
                                 'plat_id',
                                 'structure_built',
                                 'living_area',
                                 'land_area',
                                 'basement',
                                 'finished basement area',
                                 'land_value',
                                 'improvement_value',
                                 'assessed_value',
                                 'seller',
                                 'date',
                                 'price',
                                 'transfer_type',
                                 "homestead_application_status",
                                 'homeowner_tax_credit',
                                 'legal_description',
                                 'stories',
                                 'bath']
                        )
    data, searched_terms = loop_search_terms(driver, search_terms, data, rand_sec, STREET_NAME_ID, CONTINUE_CLASS, RESULT_PREVIOUS_ID, DETAILS_PREVIOUS_ID, county, path, index_of_street, page, start_row, start, end,log,arg_county_index)

    stop = timeit.default_timer()
    execution_time = stop - start_time_1
    #deleted worker from file name i.e data.to_csv(f"{path}{COUNTIES[county]}_{len(searched_terms)}_{len(search_terms)}_complete_{worker}.csv")
    data.to_csv(f"{path}{COUNTIES[county]}_SDAT_test.csv")
    #removing worker from {} from f string
    log.info(f"{len(searched_terms)} out of {len(search_terms)} terms completed in {str(execution_time)} seconds.")
    driver.close()

def func_check_site_down(driver):
    check = True
    while check:
        if "may be unavailable before 7:00 AM for maintenance" in driver.find_element(By.CSS_SELECTOR, "p > strong").text :
            time.sleep(900)  # SLEEP FOR 15 MINUTES
        else:
            check = False

#%%
def multi_main_fn(arg_county_index,start_index,end_index):
# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument("county_index", help="0 - Anne Arundel, 1 - Montgomery, 2 - Prince George's")
#     parser.add_argument("start_index", help="start index to run")
#     parser.add_argument("end_index", help="end index to run")
#     args = parser.parse_args()
#     print('args: ', args)
#     arg_county_index = int(args.county_index)
#     start_index = int(args.start_index)
#     end_index = int(args.end_index)

    if COUNTIES[arg_county_index] == 'Anne Arundel County':
        county_file_name = "anne_arundel"
        path = "complete/"
    elif COUNTIES[arg_county_index] == 'Montgomery County':
        county_file_name = "montgomery"
        path = "complete/"
    else:
        county_file_name = "prince_george_s"
        path = "complete/"

    log = create_log_object(county_file_name, start_index, end_index)

    print(" ***************** Starting script ********")
    log.info(" ****************************** LOGGING NEW LOAD **********************************")

    suffix_list = sdat.load_suffixes("data/street_name_suffixes.csv")
    locations = sdat.load_locations(f"data/{county_file_name}_locations.csv")
    log.info(f"LOCATIONS RAW : {len(locations)}")
    search_terms = sdat.remove_suffix(locations, suffix_list)
    search_terms = sorted(search_terms)
    log.info(f"NUMBER OF ADDRESS : {len(search_terms)}")
    # assert True
    log.info(f"Started scraping {COUNTIES[arg_county_index]} for worker arg_worker from search term arg_last_page onwards.")
    cursor_obj.execute(f'''SELECT * FROM retry_table WHERE county_name = "{COUNTIES[arg_county_index]}" AND start_index = "{start_index}" AND end_index = "{end_index}" ORDER BY created_at DESC LIMIT 1;''')
    db_data = cursor_obj.fetchone()
    log.info(db_data)
    county_db = db_data[0] if db_data else arg_county_index
    index_of_street_db = db_data[2] if db_data else 0
    current_page_db = db_data[4] if db_data else 1
    last_row_db = db_data[5] if db_data else 0
    start_index_db = db_data[8] if db_data else start_index
    end_index_db = db_data[9] if db_data else end_index
    driver = sdat.get_driver(DRIVER_PATH, user_agent_list)
    main_search(driver, search_terms[int(start_index_db)+int(index_of_street_db):int(end_index_db)], int(county_db), path, int(index_of_street_db), int(current_page_db), int(last_row_db), int(start_index_db), int(end_index_db),log,arg_county_index)
    # main_search(driver, search_terms[500:1000], arg_county_index, path, 0, 1, 0, 500, 1000)
    print(" ***************** Finished script ********")
#     data_merge_func(arg_county_index, "sdat")


# multi_main_fn(2,0,5)


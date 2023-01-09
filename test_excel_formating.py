# %%
from argparse import PARSER
import argparse
import pandas as pd
import numpy as np
# import usaddress
import re
from tqdm import tqdm
import mysql.connector

connection_obj = mysql.connector.connect(host='localhost',
                                         database='haya_lee',
                                         user='root',
                                         password='1234')

cursor_obj = connection_obj.cursor()

def load_merge(table_name, county_index):
    if county_index == 0:
        # tax_query = f'''SELECT DISTINCT tax_month, tax_amount, parcel_ID, owner FROM {table_name}_TAX;'''
        tax_query = f'''select distinct tax_month, tax_amount, aa_tax.parcel_ID, owner from aa_tax left join aa_table on trim(aa_tax.parcel_ID) = trim(aa_table.parcel_ID) where aa_table.parcel_ID is not NULL;'''
        cursor_obj.execute(tax_query)
        tax = pd.DataFrame(cursor_obj.fetchall(), columns=['tax_month', 'tax_amount', 'parcel_ID', 'owner'])

        # sdat_query = f'''SELECT DISTINCT district,account,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath,parcel_ID FROM {table_name}_TABLE WHERE `use` not in ("COMMERCIAL", "INDUSTRIAL", "AGRICULTURE", "APARTMENT", "CONDOMINIUM", "COMMERCIAL CONDOMINIUM");'''
        sdat_query = f'''select DISTINCT district,account,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath,aa_table.parcel_ID from aa_table left join aa_tax on trim(aa_table.parcel_ID) = trim(aa_tax.parcel_ID) where aa_tax.parcel_ID is not NULL and aa_table.use not in ("COMMERCIAL", "INDUSTRIAL", "AGRICULTURE", "APARTMENT", "CONDOMINIUM", "COMMERCIAL CONDOMINIUM");'''
        cursor_obj.execute(sdat_query)
        sdat = pd.DataFrame(cursor_obj.fetchall(),
                            columns=['district', 'account', 'owner 1 first name', 'owner 1 last name',
                                     'owner 2 first name', 'owner 2 last name', 'mailing', 'premises', 'use',
                                     'principal_residence', 'deed_reference', 'map_id', 'parcel', 'block',
                                     'subdivision', 'plat_id', 'structure_built', 'living_area', 'land_area',
                                     'basement', 'finished basement area', 'land_value', 'improvement_value',
                                     'assessed_value', 'seller', 'date', 'price', 'transfer_type',
                                     'homestead_application_status', 'homeowner_tax_credit', 'legal_description',
                                     'stories', 'bath', 'parcel_ID'])
    elif county_index == 1:
        # tax_query = f'''SELECT DISTINCT parcel_ID, owner, tax_amount, tax_period, lot, class_id, mortgage, status FROM {table_name}_TAX;'''
        tax_query = f'''select distinct mnt_tax.parcel_ID, owner, tax_amount, tax_period, lot, class_id, mortgage, tax_lien_status, tax_lien_amount, tax_year from mnt_tax left join mnt_table on trim(mnt_tax.parcel_ID) = trim(mnt_table.parcel_ID) where mnt_table.parcel_ID is not NULL;'''
        cursor_obj.execute(tax_query)
        tax = pd.DataFrame(cursor_obj.fetchall(), columns=['parcel_ID', 'owner', 'tax_amount', 'tax_period', 'lot', 'class_id', 'mortgage', 'tax_lien_status', 'tax_lien_amount', 'tax_year'])

        # sdat_query = f'''SELECT DISTINCT district,parcel_ID,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath FROM {table_name}_TABLE WHERE `use` not in ("COMMERCIAL", "INDUSTRIAL", "AGRICULTURE", "APARTMENT", "CONDOMINIUM", "COMMERCIAL CONDOMINIUM");'''
        sdat_query = f'''select distinct district, mnt_table.parcel_ID,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath from mnt_table left join mnt_tax on trim(mnt_table.parcel_ID) = trim(mnt_tax.parcel_ID) where mnt_tax.parcel_ID is not NULL;'''
        cursor_obj.execute(sdat_query)
        sdat = pd.DataFrame(cursor_obj.fetchall(), columns=['district', 'parcel_ID', 'owner 1 first name', 'owner 1 last name', 'owner 2 first name', 'owner 2 last name', 'mailing', 'premises', 'use', 'principal_residence', 'deed_reference', 'map_id', 'parcel', 'block', 'subdivision', 'plat_id', 'structure_built', 'living_area', 'land_area', 'basement', 'finished basement area', 'land_value', 'improvement_value', 'assessed_value', 'seller', 'date', 'price', 'transfer_type', 'homestead_application_status', 'homeowner_tax_credit', 'legal_description', 'stories', 'bath'])

    else:
        # tax_query = f'''SELECT DISTINCT tax_month, tax_amount, parcel_ID, owner, tax_sale, attorney_name, attorney_phone, purchaser_name, equity_case_no, bid_amount, base, ip, amount, status FROM {table_name}_TAX;'''
        tax_query = f'''select DISTINCT tax_month, tax_amount, pg_tax.parcel_ID, owner, tax_sale, attorney_name, attorney_phone, purchaser_name, equity_case_no, bid_amount, base, ip, amount, status from pg_tax left join pg_table on trim(pg_tax.parcel_ID) = trim(pg_table.parcel_ID) where pg_table.parcel_ID is not NULL;'''
        cursor_obj.execute(tax_query)
        tax = pd.DataFrame(cursor_obj.fetchall(), columns=['tax_month', 'tax_amount', 'parcel_ID', 'owner', 'tax_sale', 'attorney_name', 'attorney_phone', 'purchaser_name', 'equity_case_no', 'bid_amount', 'base', 'ip', 'amount', 'status'])
        # sdat_query = f'''SELECT DISTINCT district,parcel_ID,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath FROM {table_name}_TABLE WHERE `use` not in ("COMMERCIAL", "INDUSTRIAL", "AGRICULTURE", "APARTMENT", "CONDOMINIUM", "COMMERCIAL CONDOMINIUM");'''
        sdat_query = f'''select distinct district, pg_table.parcel_ID,owner_1_first_name,owner_1_last_name,owner_2_first_name,owner_2_last_name,mailing,premises,`use`,principal_residence,deed_reference,map_id,parcel,block,subdivision,plat_id,structure_built,living_area,land_area,basement,finished_basement_area,land_value,improvement_value,assessed_value,seller,date,price,transfer_type,homestead_application_status,homeowner_tax_credit,legal_description,stories,bath from pg_table left join pg_tax on trim(pg_table.parcel_ID) = trim(pg_tax.parcel_ID) where pg_tax.parcel_ID is not NULL;'''
        cursor_obj.execute(sdat_query)
        sdat = pd.DataFrame(cursor_obj.fetchall(), columns=['district', 'parcel_ID', 'owner 1 first name', 'owner 1 last name', 'owner 2 first name', 'owner 2 last name', 'mailing', 'premises', 'use', 'principal_residence', 'deed_reference', 'map_id', 'parcel', 'block', 'subdivision', 'plat_id', 'structure_built', 'living_area', 'land_area', 'basement', 'finished basement area', 'land_value', 'improvement_value', 'assessed_value', 'seller', 'date', 'price', 'transfer_type', 'homestead_application_status', 'homeowner_tax_credit', 'legal_description', 'stories', 'bath'])

    return tax, sdat


def convert_to_int(x):
    try:
        x = int(x)
    except:
        x = ''
    return x


def account(df, column_name, account_index):
    accounts = df[f'{column_name}'].apply(lambda x: re.findall('\d+', x))
    i = account_index
    df[f'parcel_ID'] = ['' if len(n) <= i else n[i] for n in accounts]
    # df[f'parcel_ID'] = df[f'parcel_ID'].apply(lambda x: str(x).lstrip("0"))


def district(df, column_name, district_index):
    accounts = df[f'{column_name}'].apply(lambda x: re.findall('\d+', x))
    i = district_index
    df[f'district_number'] = ['' if len(n) <= i else n[i] for n in accounts]


def split_address_column(df, column_name):
    df[f'{column_name}'] = df[f'{column_name}'].astype(str)
    df[f'{column_name}'] = df[f'{column_name}'].apply(lambda x: x.splitlines())
    df[f'{column_name}_street'] = ['' if len(n) <= 0 else n[0].strip() for n in df[f'{column_name}']]
    df[f'{column_name}_area'] = ['' if len(n) <= 1 else n[1].strip() for n in df[f'{column_name}']]
    df[f'{column_name}'] = df[f'{column_name}'].apply(lambda x: ", ".join(x))


def split_owner_names(data, column_name):
    data[f'{column_name}'] = data[f'{column_name}'].astype(str)
    # owner_names = data[f'{column_name}'].apply(lambda x: x.split('&'))
    data[f'{column_name}_1'] = ['' if len(n) <= 0 else n[0].strip() for n in data[f'{column_name}']]
    # data[f'{column_name}_1'] = ['' if len(n) <= 0 else n[0].strip() for n in owner_names]
    # data[f'{column_name}_2'] = ['' if len(n) <= 1 else n[1].strip() for n in owner_names]
    # data[f'{column_name}_3'] = ['' if len(n) <= 2 else n[2].strip() for n in owner_names]


def find_account_number(x):
    district = re.findall('(\d+)', x)[0].lstrip('0')
    subdiv = re.findall('(\d+)', x)[1]
    account = re.findall('(\d+)', x)[2]
    # print(account)
    # print(f"----------- {str(account)}")
    return f'{str(district)}{str(subdiv)}{str(account)}'


def split_address(df, column_name):
    city = []
    state = []
    zipcode = []
    address_number = []
    street_name = []
    street_name_pt = []
    check_mailing = []

    for i in range(len(df[column_name])):
        check_mailing.append(df[column_name][i].split("\n")[0])
        try:
            tagged_address, address_type = usaddress.tag(df[column_name][i])
        except:
            tagged_address = None
        try:
            address_number.append(tagged_address['AddressNumber'])
        except:
            address_number.append(None)
        try:
            street_name.append(tagged_address['StreetName'])
        except:
            street_name.append(None)
        try:
            street_name_pt.append(tagged_address['StreetNamePostType'])
        except:
            street_name_pt.append(None)
        try:
            city.append(tagged_address['PlaceName'])
        except:
            city.append(None)
        try:
            state.append(tagged_address['StateName'])
        except:
            state.append(None)
        try:
            zipcode.append(tagged_address['ZipCode'])
        except:
            zipcode.append(None)
    df[f'{column_name}_address_number'] = address_number
    df[f'{column_name}_street_name'] = street_name
    df[f'{column_name}_street_name_pt'] = street_name_pt
    df[f'{column_name}_address_city'] = city
    df[f'{column_name}_address_state'] = state
    df[f'{column_name}_address_zipcode'] = zipcode
    df[f'{column_name}_street_address'] = check_mailing
    # df[f'{column_name}_street_address'] = df[f'{column_name}_address_number'] + " " + df[f'{column_name}_street_name'] + " " + df[f'{column_name}_street_name_pt']
    # df[f'{column_name}_street_address'] = df[f'{column_name}_street_address'].apply(lambda x: x.strip() if x not None else None)


def get_premise_zipcode(tlist):
    re_list = []
    for i in tqdm(tlist):
        try:
            res = re.findall(r'\d+', i)
        except:
            res = 'None'
        if len(res) == 3:
            if len(res[1]) > 4:
                re_list.append(res[1])
            else:
                re_list.append(None)
        elif len(res) == 2:
            if len(res[0]) > 4:
                re_list.append(res[0])
            else:
                re_list.append(None)
        else:
            re_list.append(None)
    return re_list


def get_premise_address_num(tlist):
    re_list = []
    for i in tqdm(tlist):
        try:
            res = re.findall(r'\d+', i)
        except:
            res = 'None'
        try:
            re_list.append(res[0])
        except:
            re_list.append(None)
    return re_list


def split_premise_address(df, col):
    first = []
    second = []
    third = []
    for i in tqdm(df[col]):
        try:
            split_premises = i.splitlines()
            try:
                first.append(split_premises[0])
            except:
                first.append(None)
            try:
                second.append(split_premises[1])
            except:
                second.append(None)
            try:
                third.append(split_premises[2])
            except:
                third.append(None)
        except:
            first.append(None)
            second.append(None)
            third.append(None)

    def split_further(tlist):
        second_one = []
        for i in tqdm(tlist):
            try:
                found = re.findall(r'([^\d]+)', i)
            except:
                found = 'None'
            try:
                second_one.append(found[0].strip())
            except:
                second_one.append(None)
        return second_one

    second_one = split_further(second)
    first_one = split_further(first)
    return first, second, first_one, second_one


def clean_premises_address(df, col):
    first, second, first_one, second_one = split_premise_address(df, col)
    df[f'{col}_street_name'], df[f'{col}_city'] = first_one, second_one
    df[f'{col}_zipcode'] = get_premise_zipcode(second)
    df[f'premises_state'] = "MD"
    # df[f'{col}_address_number'] = get_premise_address_num(first)
    list_of_split_words = second[0].split(" ")
    # premises_city = ""
    # for word in list_of_split_words:
    #     if not  word.isnumeric():
    #         premises_city += word
    #     else:
    #         break
    #
    df[f'{col}_street_address'] = first
    # df[f'{col}_city'] = premises_city


# def premise_address_personal(df,col):

# %%
def test_excel_formating_fn():
# if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument("county", help="0 - Anne Arundel, 1 - Montgomery, 2 - Prince George's")
    # args = parser.parse_args()
    # arg_county = int(args.county)
    #
    county_dict = {0: 'AA', 1: 'MNT', 2: "PG"}
    for i in range(3):
        arg_county = i
        # if arg_county == 0:
        #     county = "Anne Arundel"
        # elif arg_county == 1:
        #     county = "Montgomery"
        # elif arg_county == 2:
        #     county = "Prince George"

        if arg_county == 0:
            county = "Anne Arundel"
        elif arg_county == 1:
            county = "Montgomery"
        elif arg_county == 2:
            county = "Prince George"

        tax, sdat = load_merge(county_dict[arg_county],arg_county)
        split_address(sdat, 'mailing')
        clean_premises_address(sdat, 'premises')
        # split_owner_names(sdat, 'owner_name')
        if arg_county == 0:
            sdat['parcel_ID'] = sdat['district'].apply(lambda x: find_account_number(x))
            # sdat['account'] = sdat['account'].astype(str)
        else:
            account(sdat, 'district', 1)

        district(sdat, 'district', 0)
        #
        # tax['parcel_ID'] = tax['parcel_ID'].apply(lambda x: convert_to_int(x))
        # sdat['parcel_ID'] = sdat['parcel_ID'].apply(lambda x: convert_to_int(x))
        tax['parcel_ID'] = tax['parcel_ID'].astype(str)
        sdat['parcel_ID'] = sdat['parcel_ID'].astype(str)

        merged_df = pd.merge(sdat, tax, how='left', on='parcel_ID', suffixes=('_sdat', '_tax'))
        # merged_df = sdat.merge(tax, on='parcel_ID', how='left' )
        merged_df['county'] = county
        '''removed column list
                1.mailing address
                2.mailing_address_street_address
                3.premises
                4.premises_address_number
                5.premises_street_name
                6.premises_place_name
                7.date
            appended column list
                1.premises_street_address
                2.premises_city
                3.premises_statex
                4.legal_description
                5.stories
                6.full_half_bath
                7.last_transfer_date
                8.Trasfer Type
                9.
        '''
        total_columns = ['county',
                         'district_number',
                         'account',
                         'parcel_ID',
                         'owner 1 first name',
                         'owner 1 last name',
                         'owner 2 first name',
                         'owner 2 last name',
                         'mailing_street_address',
                         'mailing_address_city',
                         'mailing_address_state',
                         'mailing_address_zipcode',
                         'premises_street_address',
                         # 'premises_street_name',
                         'premises_city',
                         # 'premises',
                         'premises_state',
                         'premises_zipcode',
                         'use',
                         'principal_residence',
                         'deed_reference',
                         'map_id',
                         'parcel',
                         'block',
                         'lot',
                         # 'sub',
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
                         'homestead_application_status',
                         'homeowner_tax_credit',
                         'legal_description',
                         'stories',
                         'bath',
                         'tax_month',
                         'tax_period',
                         'tax_amount',
                         'class_id',
                         'occupancy',
                         'mortgage',
                         'prop_address',
                         'detail_address',
                         'tax_sale',
                         'attorney_name',
                         'attorney_phone',
                         'purchaser_name',
                         'equity_case_no',
                         'bid_amount',
                         'base',
                         'ip',
                         'amount',
                         'tax_lien_status',
                         'tax_lien_amount',
                         'tax_year',
                         ]

        if arg_county == 0:
            final_columns = [e for e in total_columns if e not in (
            'block', 'tax_period', 'lot', 'sub', 'occupancy', 'mortgage', 'prop_address', 'detail_address', 'tax_sale',
            'class_id', 'attorney_name', 'attorney_phone', 'purchaser_name', 'equity_case_no', 'bid_amount', 'base',
            'ip', 'amount','tax_lien_status', 'tax_lien_amount','tax_year')]  # class_id
        elif arg_county == 1:
            final_columns = [e for e in total_columns if e not in (
            'tax_month', 'tax_sale', 'prop_address', 'detail_address', 'occupancy', 'account', 'attorney_name',
            'attorney_phone', 'purchaser_name', 'equity_case_no', 'bid_amount', 'base', 'ip', 'amount')]
        elif arg_county == 2:
            final_columns = [e for e in total_columns if e not in (
            'tax_period', 'lot', 'sub', 'class_id', 'occupancy', 'mortgage', 'prop_address', 'account',
            'detail_address','tax_lien_status','tax_lien_amount','tax_year')]
        # print(final_columns)
        print("county index >>>>>",arg_county,"columns",final_columns)
        final_df = merged_df[final_columns]
        print(final_df)
        # final_df = merged_df[total_columns]
        final_df = final_df.replace(np.NaN, "NA")
        final_df.rename(columns={'date': 'last_transfer_date', 'price': 'last_sale_price'}, inplace=True)
        # final_df.to_csv(f"{county} - Complete Tax Data.csv", index=False)



        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(f"{county} - Complete Tax Data_column_formats.xlsx", engine='xlsxwriter')

        # Convert the dataframe to an XlsxWriter Excel object.
        final_df.to_excel(writer, sheet_name='Sheet1')

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Add some cell formats.
        format1 = workbook.add_format({'num_format': '#,##0.00'})
        # format2 = workbook.add_format({'num_format': '0%'})

        # Note: It isn't possible to format any cells that already have a format such
        # as the index or headers or any cells that contain dates or datetimes.

        # Set the column width and format.
        worksheet.set_column(3, 3, 18, format1)

        # Set the format but not the column width.
        # worksheet.set_column(2, 2, None, format2)

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()
# test_excel_formating_fn()
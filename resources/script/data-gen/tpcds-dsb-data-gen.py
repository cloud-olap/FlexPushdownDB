# Put this under the code of tpc-ds/dsb
# For example, `tpcds-kit/tools` for tpc-ds, `dsb/code/tools` for dsb
#   `tpcds-kit`: https://github.com/gregrahn/tpcds-kit.git
#   `dsb`: https://github.com/microsoft/dsb.git
# Make sure dsdgen has been built, i.e., run `make` inside the code dir

import os
import platform
import math
import multiprocessing
from multiprocessing import Pool

# configurable parameters
sf = 1  # require sf >= 1
num_partitions_dict = {
    'call_center': 1,
    'catalog_page': 1,
    'catalog_returns': 1,
    'catalog_sales': 1,
    'customer': 1,
    'customer_address': 1,
    'customer_demographics': 1,
    'date_dim': 1,
    'dbgen_version': 1,
    'household_demographics': 1,
    'income_band': 1,
    'inventory': 1,
    'item': 1,
    'promotion': 1,
    'reason': 1,
    'ship_mode': 1,
    'store': 1,
    'store_returns': 1,
    'store_sales': 1,
    'time_dim': 1,
    'warehouse': 1,
    'web_page': 1,
    'web_returns': 1,
    'web_sales': 1,
    'web_site': 1
}
benchmark = 'dsb'   # specify whether data is from `dsb` or standard `tpcds`

def get_num_lines_partition(num_lines, num_partitions):
    return int(math.ceil(float(num_lines) / float(num_partitions)))

def get_num_digits_suffix(num_partitions):
    return int(math.ceil(math.log(num_partitions, 10)))

def run_command(cmd):
    os.system(cmd)

def format_data_for_table(table, column_names, num_partitions):
    global split_func, data_dir

    table_file = table + ".dat"
    partition_file_prefix = table_file + "."
    partition_dir = table + "_sharded"

    # split table file if it has multiple partitions
    if num_partitions > 1:
        # get num of lines
        num_lines = int(os.popen('wc -l < {}'.format(table_file)).read())

        # split table into partitions
        num_lines_partition = get_num_lines_partition(num_lines, num_partitions)
        num_digits_suffix = get_num_digits_suffix(num_partitions)
        os.system('{} -a {} -d -l {} {} {}'.format(split_func,
                                                   num_digits_suffix,
                                                   num_lines_partition,
                                                   table_file,
                                                   partition_file_prefix))

        # remove leading 0s in the suffix of partitions
        partition_files = []
        for i in range(num_partitions):
            old_name = partition_file_prefix + str(i).zfill(num_digits_suffix)
            new_name = partition_file_prefix + str(i)
            os.system('mv {} {} 2>/dev/null'.format(old_name, new_name))
            partition_files.append(new_name)

        # remove '|' at the end of each row
        os.system('chmod -R +rw .')
        pool = Pool(processes=multiprocessing.cpu_count()*2)
        for partition_file in partition_files:
            if platform.system() == "Darwin":
                cmd_remove_end = 'sed -i \'\' \'s/.$//\' {}'.format(partition_file)
            else:
                cmd_remove_end = 'sed -i \'s/.$//\' {}'.format(partition_file)
            pool.apply_async(run_command, args=(cmd_remove_end,))
        pool.close()
        pool.join()

        # add column names
        os.system('chmod -R +rw .')
        pool = Pool(processes=multiprocessing.cpu_count()*2)
        for partition_file in partition_files:
            if platform.system() == "Darwin":
                cmd_add_column_names = 'sed -i \'\' \'1s/^/{}\\\'$\'\\n/\' {}'.format(column_names, partition_file)
            else:
                cmd_add_column_names = 'sed -i \'1i {}\' {}'.format(column_names, partition_file)
            pool.apply_async(run_command, args=(cmd_add_column_names,))
        pool.close()
        pool.join()

        # move partitions into the directory
        os.system('mkdir {}'.format(partition_dir))
        os.system('mv {}* {}/'.format(partition_file_prefix, partition_dir))

        # move data into data_dir
        os.system('mv {} {}'.format(partition_dir, data_dir))

    else:
        # remove '|' at the end of each row
        if platform.system() == "Darwin":
            os.system('sed -i \'\' \'s/.$//\' {}'.format(table_file))
        else:
            os.system('sed -i \'s/.$//\' {}'.format(table_file))

        # add column names
        if platform.system() == "Darwin":
            os.system('sed -i \'\' \'1s/^/{}\\\'$\'\\n/\' {}'.format(column_names, table_file))
        else:
            os.system('sed -i \'1i {}\' {}'.format(column_names, table_file))

        # move data into data_dir
        os.system('mv {} {}'.format(table_file, data_dir))

column_names_dict = {
    'call_center': 'CC_CALL_CENTER_SK|CC_CALL_CENTER_ID|CC_REC_START_DATE|CC_REC_END_DATE|CC_CLOSED_DATE_SK|CC_OPEN_DATE_SK|CC_NAME|CC_CLASS|CC_EMPLOYEES|CC_SQ_FT|CC_HOURS|CC_MANAGER|CC_MKT_ID|CC_MKT_CLASS|CC_MKT_DESC|CC_MARKET_MANAGER|CC_DIVISION|CC_DIVISION_NAME|CC_COMPANY|CC_COMPANY_NAME|CC_STREET_NUMBER|CC_STREET_NAME|CC_STREET_TYPE|CC_SUITE_NUMBER|CC_CITY|CC_COUNTY|CC_STATE|CC_ZIP|CC_COUNTRY|CC_GMT_OFFSET|CC_TAX_PERCENTAGE',
    'catalog_page': 'CP_CATALOG_PAGE_SK|CP_CATALOG_PAGE_ID|CP_START_DATE_SK|CP_END_DATE_SK|CP_DEPARTMENT|CP_CATALOG_NUMBER|CP_CATALOG_PAGE_NUMBER|CP_DESCRIPTION|CP_TYPE',
    'catalog_returns': 'CR_RETURNED_DATE_SK|CR_RETURNED_TIME_SK|CR_ITEM_SK|CR_REFUNDED_CUSTOMER_SK|CR_REFUNDED_CDEMO_SK|CR_REFUNDED_HDEMO_SK|CR_REFUNDED_ADDR_SK|CR_RETURNING_CUSTOMER_SK|CR_RETURNING_CDEMO_SK|CR_RETURNING_HDEMO_SK|CR_RETURNING_ADDR_SK|CR_CALL_CENTER_SK|CR_CATALOG_PAGE_SK|CR_SHIP_MODE_SK|CR_WAREHOUSE_SK|CR_REASON_SK|CR_ORDER_NUMBER|CR_RETURN_QUANTITY|CR_RETURN_AMOUNT|CR_RETURN_TAX|CR_RETURN_AMT_INC_TAX|CR_FEE|CR_RETURN_SHIP_COST|CR_REFUNDED_CASH|CR_REVERSED_CHARGE|CR_STORE_CREDIT|CR_NET_LOSS',
    'catalog_sales': 'CS_SOLD_DATE_SK|CS_SOLD_TIME_SK|CS_SHIP_DATE_SK|CS_BILL_CUSTOMER_SK|CS_BILL_CDEMO_SK|CS_BILL_HDEMO_SK|CS_BILL_ADDR_SK|CS_SHIP_CUSTOMER_SK|CS_SHIP_CDEMO_SK|CS_SHIP_HDEMO_SK|CS_SHIP_ADDR_SK|CS_CALL_CENTER_SK|CS_CATALOG_PAGE_SK|CS_SHIP_MODE_SK|CS_WAREHOUSE_SK|CS_ITEM_SK|CS_PROMO_SK|CS_ORDER_NUMBER|CS_QUANTITY|CS_WHOLESALE_COST|CS_LIST_PRICE|CS_SALES_PRICE|CS_EXT_DISCOUNT_AMT|CS_EXT_SALES_PRICE|CS_EXT_WHOLESALE_COST|CS_EXT_LIST_PRICE|CS_EXT_TAX|CS_COUPON_AMT|CS_EXT_SHIP_COST|CS_NET_PAID|CS_NET_PAID_INC_TAX|CS_NET_PAID_INC_SHIP|CS_NET_PAID_INC_SHIP_TAX|CS_NET_PROFIT',
    'customer': 'C_CUSTOMER_SK|C_CUSTOMER_ID|C_CURRENT_CDEMO_SK|C_CURRENT_HDEMO_SK|C_CURRENT_ADDR_SK|C_FIRST_SHIPTO_DATE_SK|C_FIRST_SALES_DATE_SK|C_SALUTATION|C_FIRST_NAME|C_LAST_NAME|C_PREFERRED_CUST_FLAG|C_BIRTH_DAY|C_BIRTH_MONTH|C_BIRTH_YEAR|C_BIRTH_COUNTRY|C_LOGIN|C_EMAIL_ADDRESS|C_LAST_REVIEW_DATE_SK',
    'customer_address': 'CA_ADDRESS_SK|CA_ADDRESS_ID|CA_STREET_NUMBER|CA_STREET_NAME|CA_STREET_TYPE|CA_SUITE_NUMBER|CA_CITY|CA_COUNTY|CA_STATE|CA_ZIP|CA_COUNTRY|CA_GMT_OFFSET|CA_LOCATION_TYPE',
    'customer_demographics': 'CD_DEMO_SK|CD_GENDER|CD_MARITAL_STATUS|CD_EDUCATION_STATUS|CD_PURCHASE_ESTIMATE|CD_CREDIT_RATING|CD_DEP_COUNT|CD_DEP_EMPLOYED_COUNT|CD_DEP_COLLEGE_COUNT',
    'date_dim': 'D_DATE_SK|D_DATE_ID|D_DATE|D_MONTH_SEQ|D_WEEK_SEQ|D_QUARTER_SEQ|D_YEAR|D_DOW|D_MOY|D_DOM|D_QOY|D_FY_YEAR|D_FY_QUARTER_SEQ|D_FY_WEEK_SEQ|D_DAY_NAME|D_QUARTER_NAME|D_HOLIDAY|D_WEEKEND|D_FOLLOWING_HOLIDAY|D_FIRST_DOM|D_LAST_DOM|D_SAME_DAY_LY|D_SAME_DAY_LQ|D_CURRENT_DAY|D_CURRENT_WEEK|D_CURRENT_MONTH|D_CURRENT_QUARTER|D_CURRENT_YEAR',
    'dbgen_version': 'DV_VERSION|DV_CREATE_DATE|DV_CREATE_TIME|DV_CMDLINE_ARGS',
    'household_demographics': 'HD_DEMO_SK|HD_INCOME_BAND_SK|HD_BUY_POTENTIAL|HD_DEP_COUNT|HD_VEHICLE_COUNT',
    'income_band': 'IB_INCOME_BAND_SK|IB_LOWER_BOUND|IB_UPPER_BOUND',
    'inventory': 'INV_DATE_SK|INV_ITEM_SK|INV_WAREHOUSE_SK|INV_QUANTITY_ON_HAND',
    'item': 'I_ITEM_SK|I_ITEM_ID|I_REC_START_DATE|I_REC_END_DATE|I_ITEM_DESC|I_CURRENT_PRICE|I_WHOLESALE_COST|I_BRAND_ID|I_BRAND|I_CLASS_ID|I_CLASS|I_CATEGORY_ID|I_CATEGORY|I_MANUFACT_ID|I_MANUFACT|I_SIZE|I_FORMULATION|I_COLOR|I_UNITS|I_CONTAINER|I_MANAGER_ID|I_PRODUCT_NAME',
    'promotion': 'P_PROMO_SK|P_PROMO_ID|P_START_DATE_SK|P_END_DATE_SK|P_ITEM_SK|P_COST|P_RESPONSE_TARGET|P_PROMO_NAME|P_CHANNEL_DMAIL|P_CHANNEL_EMAIL|P_CHANNEL_CATALOG|P_CHANNEL_TV|P_CHANNEL_RADIO|P_CHANNEL_PRESS|P_CHANNEL_EVENT|P_CHANNEL_DEMO|P_CHANNEL_DETAILS|P_PURPOSE|P_DISCOUNT_ACTIVE',
    'reason': 'R_REASON_SK|R_REASON_ID|R_REASON_DESC',
    'ship_mode': 'SM_SHIP_MODE_SK|SM_SHIP_MODE_ID|SM_TYPE|SM_CODE|SM_CARRIER|SM_CONTRACT',
    'store': 'S_STORE_SK|S_STORE_ID|S_REC_START_DATE|S_REC_END_DATE|S_CLOSED_DATE_SK|S_STORE_NAME|S_NUMBER_EMPLOYEES|S_FLOOR_SPACE|S_HOURS|S_MANAGER|S_MARKET_ID|S_GEOGRAPHY_CLASS|S_MARKET_DESC|S_MARKET_MANAGER|S_DIVISION_ID|S_DIVISION_NAME|S_COMPANY_ID|S_COMPANY_NAME|S_STREET_NUMBER|S_STREET_NAME|S_STREET_TYPE|S_SUITE_NUMBER|S_CITY|S_COUNTY|S_STATE|S_ZIP|S_COUNTRY|S_GMT_OFFSET|S_TAX_PRECENTAGE',
    'store_returns': 'SR_RETURNED_DATE_SK|SR_RETURN_TIME_SK|SR_ITEM_SK|SR_CUSTOMER_SK|SR_CDEMO_SK|SR_HDEMO_SK|SR_ADDR_SK|SR_STORE_SK|SR_REASON_SK|SR_TICKET_NUMBER|SR_RETURN_QUANTITY|SR_RETURN_AMT|SR_RETURN_TAX|SR_RETURN_AMT_INC_TAX|SR_FEE|SR_RETURN_SHIP_COST|SR_REFUNDED_CASH|SR_REVERSED_CHARGE|SR_STORE_CREDIT|SR_NET_LOSS',
    'store_sales': 'SS_SOLD_DATE_SK|SS_SOLD_TIME_SK|SS_ITEM_SK|SS_CUSTOMER_SK|SS_CDEMO_SK|SS_HDEMO_SK|SS_ADDR_SK|SS_STORE_SK|SS_PROMO_SK|SS_TICKET_NUMBER|SS_QUANTITY|SS_WHOLESALE_COST|SS_LIST_PRICE|SS_SALES_PRICE|SS_EXT_DISCOUNT_AMT|SS_EXT_SALES_PRICE|SS_EXT_WHOLESALE_COST|SS_EXT_LIST_PRICE|SS_EXT_TAX|SS_COUPON_AMT|SS_NET_PAID|SS_NET_PAID_INC_TAX|SS_NET_PROFIT',
    'time_dim': 'T_TIME_SK|T_TIME_ID|T_TIME|T_HOUR|T_MINUTE|T_SECOND|T_AM_PM|T_SHIFT|T_SUB_SHIFT|T_MEAL_TIME',
    'warehouse': 'W_WAREHOUSE_SK|W_WAREHOUSE_ID|W_WAREHOUSE_NAME|W_WAREHOUSE_SQ_FT|W_STREET_NUMBER|W_STREET_NAME|W_STREET_TYPE|W_SUITE_NUMBER|W_CITY|W_COUNTY|W_STATE|W_ZIP|W_COUNTRY|W_GMT_OFFSET',
    'web_page': 'WP_WEB_PAGE_SK|WP_WEB_PAGE_ID|WP_REC_START_DATE|WP_REC_END_DATE|WP_CREATION_DATE_SK|WP_ACCESS_DATE_SK|WP_AUTOGEN_FLAG|WP_CUSTOMER_SK|WP_URL|WP_TYPE|WP_CHAR_COUNT|WP_LINK_COUNT|WP_IMAGE_COUNT|WP_MAX_AD_COUNT',
    'web_returns': 'WR_RETURNED_DATE_SK|WR_RETURNED_TIME_SK|WR_ITEM_SK|WR_REFUNDED_CUSTOMER_SK|WR_REFUNDED_CDEMO_SK|WR_REFUNDED_HDEMO_SK|WR_REFUNDED_ADDR_SK|WR_RETURNING_CUSTOMER_SK|WR_RETURNING_CDEMO_SK|WR_RETURNING_HDEMO_SK|WR_RETURNING_ADDR_SK|WR_WEB_PAGE_SK|WR_REASON_SK|WR_ORDER_NUMBER|WR_RETURN_QUANTITY|WR_RETURN_AMT|WR_RETURN_TAX|WR_RETURN_AMT_INC_TAX|WR_FEE|WR_RETURN_SHIP_COST|WR_REFUNDED_CASH|WR_REVERSED_CHARGE|WR_ACCOUNT_CREDIT|WR_NET_LOSS',
    'web_sales': 'WS_SOLD_DATE_SK|WS_SOLD_TIME_SK|WS_SHIP_DATE_SK|WS_ITEM_SK|WS_BILL_CUSTOMER_SK|WS_BILL_CDEMO_SK|WS_BILL_HDEMO_SK|WS_BILL_ADDR_SK|WS_SHIP_CUSTOMER_SK|WS_SHIP_CDEMO_SK|WS_SHIP_HDEMO_SK|WS_SHIP_ADDR_SK|WS_WEB_PAGE_SK|WS_WEB_SITE_SK|WS_SHIP_MODE_SK|WS_WAREHOUSE_SK|WS_PROMO_SK|WS_ORDER_NUMBER|WS_QUANTITY|WS_WHOLESALE_COST|WS_LIST_PRICE|WS_SALES_PRICE|WS_EXT_DISCOUNT_AMT|WS_EXT_SALES_PRICE|WS_EXT_WHOLESALE_COST|WS_EXT_LIST_PRICE|WS_EXT_TAX|WS_COUPON_AMT|WS_EXT_SHIP_COST|WS_NET_PAID|WS_NET_PAID_INC_TAX|WS_NET_PAID_INC_SHIP|WS_NET_PAID_INC_SHIP_TAX|WS_NET_PROFIT',
    'web_site': 'WEB_SITE_SK|WEB_SITE_ID|WEB_REC_START_DATE|WEB_REC_END_DATE|WEB_NAME|WEB_OPEN_DATE_SK|WEB_CLOSE_DATE_SK|WEB_CLASS|WEB_MANAGER|WEB_MKT_ID|WEB_MKT_CLASS|WEB_MKT_DESC|WEB_MARKET_MANAGER|WEB_COMPANY_ID|WEB_COMPANY_NAME|WEB_STREET_NUMBER|WEB_STREET_NAME|WEB_STREET_TYPE|WEB_SUITE_NUMBER|WEB_CITY|WEB_COUNTY|WEB_STATE|WEB_ZIP|WEB_COUNTRY|WEB_GMT_OFFSET|WEB_TAX_PERCENTAGE'
}
tables = sorted(column_names_dict.keys())

if __name__ == "__main__":
    # pick appropriate split tool
    if platform.system() == "Darwin":
        split_func = "gsplit"
    else:
        split_func = "split"

    # create the directory to put data
    data_dir = benchmark + '-sf' + str(sf)
    os.system('rm -rf {}'.format(data_dir))
    os.system('mkdir {}'.format(data_dir))
    os.system('rm -rf *.dat')

    # generate data
    run_command('./dsdgen -scale {}'.format(sf))
    print("Tables generated.")

    # format for each table
    for table in tables:
        print("Formatting " + table + "... ", end='', flush=True)
        format_data_for_table(table, column_names_dict[table], num_partitions_dict[table])
        print('done')
    print("Tables formatted.")

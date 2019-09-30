# setup
#######################################################################################################################
import datetime
import json
import os
import re
import sys
import time

import numpy as np
import pandas as pd
import requests
import selenium.webdriver.support.ui as ui
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

global options
global driver
global wait

path = os.getcwd()

# with open(f'{path}\\cred.json', 'r') as r:
#     cred = json.loads(r.read())
# email = cred['email']
# pw = cred['pw']
# line_notify = cred['line_notify']

# chrome_cookie_argument = 'user-data-dir=C:\\Users\\kaiyun.chua\\AppData\\Local\\Google\\Chrome\\User Data\\Default'
chrome_cookie_argument = f'user-data-dir={path}\\cookie'

capabilities = {
    'chromeOptions': {
        'useAutomationExtension': False,
        'args': ['--start-maximized',
                 '--disable-infobars',
                 '--disable-notifications',
                 '--disable-popup-blocking',
                 chrome_cookie_argument
                 ]
    }
}

driver = webdriver.Chrome(
    f'{path}\\chromedriver.exe', desired_capabilities=capabilities)
wait = ui.WebDriverWait(driver, 60)

limit_time_restart = 60
limit_scrape_per_page = 10000

digit = re.compile('\d+')

#######################################################################################################################


def check_page_loading(type, num_load):
    print('++++++++\t line58: loading page..')
    print("type: ", type, " num_load: ", num_load)
    init_time_1 = time.time()
    while True:
        time.sleep(0.1)
        # print(f'++++++++\t line 63: in check_page_loading() -- waiting {(time.time() - init_time_1):.1f} seconds')
        if time.time() - init_time_1 >= limit_time_restart:
            print('++++++++\t line65: in check_page_loading(), restarting..')
            driver.quit()
            os.execv(sys.executable, ['python'] + sys.argv)
        elif type == 'statement':
            page = driver.find_element_by_xpath(
                "//*[@id='statement-table']").get_attribute("innerHTML")    #KY: statement-table contains statement records formatted in table with thead and tbody tags
            page_text = BeautifulSoup(page, 'html.parser')      #KY: BeautifulSoup extracts data from HTML and XML files. html.parser indicates that you want to extract HTML file (including code tags)
            page_row = len(page_text.findAll('tr')) - 2
            if page_row >= num_load:
                # print(f'page_row {page_row}')
                return True
        elif type == 'checkout':
            page = driver.find_element_by_xpath(
                "//*[@id='normal-checkout-table']").get_attribute("innerHTML")  #KY: normal-checkout-table contains checkout records formatted in table with thead and tbody tags
            page_text = BeautifulSoup(page, 'html.parser')      
            page_row = len(page_text.findAll('tr')) - 2
            if page_row >= num_load:
                # print(f'page_row {page_row}')
                return True


def check_overide_js(total_statement, total_checkout, type, num_load):
    print("++++++++\t line87: in check_overide_js")
    override_js(total_statement, total_checkout)
    override_js(total_statement, total_checkout)
    check_page_loading(type, num_load)
    print("++++++++\t line91: complete check_override_js")
    return True


def open_backend():
    print('++++++++\t line96: open backend')
    init_time_5 = time.time()       #KY: Current time in unixtime
    while True:
        time.sleep(0.1)             #time.sleep(sec) input parameter is in seconds.
        print(f'++++++++\t line100: in open_backend(), -- waiting {(time.time() - init_time_5):.1f} seconds')
        if time.time() - init_time_5 >= limit_time_restart:     #KY: If > 60 seconds
            print('++++++++\t line102: in open_backend(), restarting..')
            driver.quit()
            os.execv(sys.executable, ['python'] + sys.argv)
        else:
            try:
                driver.get('https://admin.shopee.co.id/payment/match_payment_request/')   #KY: get the requested page
                time.sleep(3)
                
                if driver.current_url == 'https://admin.shopee.co.id/payment/match_payment_request/':
                    return
                else:
                    time.sleep(180) #KY: pause automation for 3min to manually log in and saved as cookie
            except:
                pass
                # open_backend()


def override_js(total_statement, total_checkout):
    print("++++++++\t line121: in override_js")
    with open(f'{path}\\js\\manual_matching.js', 'r') as r:
        js_text = r.read()
    js_text = js_text.replace('{total_statement}', str(total_statement))
    js_text = js_text.replace('{total_checkout}', str(total_checkout))
    driver.execute_script(js_text)      #KY: execute_script runs javascript to find an element and converts the returned DOM object to a WebElement object
    print("++++++++\t line127: after executing script")


def scrape_statement(page_text):
    global df_statement
    global columns_statement
    for _, tag in enumerate(page_text.findAll('table', attrs={'id': 'statement-table'})[0].find('tbody')):
        for k, td in enumerate(tag.findAll('td')):
            if k == 1:
                statement_id = td.text          #KY: statement_id, statement_status, etc are keys found in table headers from left to right
            elif k == 2:
                statement_status = td.text
            elif k == 3:
                statement_amount = float(td.text[2:])
            elif k == 4:
                statement_recieved_time = (td.text).split('+')[0]
            elif k == 5:
                statement_uploaded_time = (td.text).split('+')[0]
            elif k == 6:
                statement_updated_time = (td.text).split('+')[0]
            elif k == 7:
                buyer_name = td.text
            elif k == 8:
                buyer_bank = td.text
            elif k == 9:
                buyer_account = td.text
            elif k == 10:
                shopee_bank = td.text
                if 'bca' in shopee_bank.lower():
                    shopee_bank = 'bca'
                elif 'bni' in shopee_bank.lower():
                    shopee_bank = 'bni'
                elif 'bri' in shopee_bank.lower():
                    shopee_bank = 'bri'
                elif 'cimb' in shopee_bank.lower():
                    shopee_bank = 'cimb'
                elif 'mandiri' in shopee_bank.lower():
                    shopee_bank = 'mandiri'
            elif k == 11:
                transfer_method = td.text
            elif k == 12:
                description = td.text
            elif k == 13:
                checkout_candidates = td.text
            elif k == 14:
                action = td.text

                columns = [ 'statement_id', 'statement_status', 'statement_amount', 'statement_recieved_time', 
                            'statement_uploaded_time', 'statement_updated_time', 'buyer_name', 'buyer_bank', 'buyer_account', 
                            'shopee_bank', 'transfer_method', 'description', 'checkout_candidates', 'action']
 
                data = [[statement_id, statement_status, statement_amount, statement_recieved_time, 
                            statement_uploaded_time, statement_updated_time, buyer_name, buyer_bank, buyer_account, 
                            shopee_bank, transfer_method, description, checkout_candidates, action]]
        
        df_statement = df_statement.append(pd.DataFrame(
            columns=columns, data=data, index=[0])[columns_statement], ignore_index=True)


def scrape_checkout(page_text):
    global df_checkout
    global columns_checkout

    for _, tag in enumerate(page_text.findAll('table', attrs={'id': 'normal-checkout-table'})[0].find('tbody')):
        for k, td in enumerate(tag.findAll('td')):
            if k == 1:
                checkout_id = td.text           #KY: checkout_id, payment_status, buyer_name, etc are keys found in table headers from left to right
            elif k == 2:
                buyer_name = td.text
            elif k == 3:
                payment_missing_amount = int(td.text[3:])
            elif k == 4:
                price_after_unique_price = int(td.text[3:])
            elif k == 5:
                shipping_name = td.text
            elif k == 6:
                payment_created_time = (td.text).split('+')[0]
            elif k == 7:
                customer_name = (td.text).split(',')[-1]
            elif k == 8:
                date_of_transfer = (td.text).replace(',', '+').split('+')[-2]
            elif k == 9:
                tranfer_from_bank = td.text
            elif k == 10:
                transfer_from_bank_account = td.text
            elif k == 11:
                transfer_to_shopee_bank = td.text    
                if 'bca' in transfer_to_shopee_bank.lower():
                    transfer_to_shopee_bank = 'bca'
                elif 'bni' in transfer_to_shopee_bank.lower():
                    transfer_to_shopee_bank = 'bni'
                elif 'bri' in transfer_to_shopee_bank.lower():
                    transfer_to_shopee_bank = 'bri'
                elif 'cimb' in transfer_to_shopee_bank.lower():
                    transfer_to_shopee_bank = 'cimb'
                elif 'mandiri' in transfer_to_shopee_bank.lower():
                    transfer_to_shopee_bank = 'mandiri'
            elif k == 12:
                amount_transfered = 0.00
                amt_float = [float(i) for i in td.text.split('Rp ')[1:]]
                amount_transfered = sum(amt_float)
            elif k == 13:
                proof_status = td.text
            elif k == 14:
                proof_id = td.text
            elif k == 15:
                memo = td.text

                columns = [ 'checkout_id', 'buyer_name', 'payment_missing_amount', 'price_after_unique_price', 
                            'shipping_name', 'payment_created_time', 'customer_name', 'date_of_transfer',
                            'tranfer_from_bank', 'transfer_from_bank_account', 'transfer_to_shopee_bank',
                            'amount_transfered', 'proof_status', 'proof_id', 'memo']
    
                data = [[checkout_id, buyer_name, payment_missing_amount, price_after_unique_price, 
                            shipping_name, payment_created_time, customer_name, date_of_transfer,
                            tranfer_from_bank, transfer_from_bank_account, transfer_to_shopee_bank,
                            amount_transfered, proof_status, proof_id, memo]]
    
        df_checkout = df_checkout.append(pd.DataFrame(
            columns=columns, data=data, index=[0])[columns_checkout], ignore_index=True)
        

def scrape_process():
    print("++++++++\t line240: in scrape_process()")
    #KY: Process(?) - Scrape all checkouts where proof_status = proof_update_confirm
    try:
        # driver.find_element_by_xpath(
        #     '//*[@x="proof_status"]').send_keys('PROOF_UPDATE_CONFIRM')
        startDate = driver.find_element_by_xpath('//*[@x="proof_ctime__gte"]')
        startDate.send_keys('01062019')
        startDate.send_keys(Keys.ENTER)

        time.sleep(15)
        check_overide_js(5, limit_scrape_per_page, 'checkout', 15) 
        #KY: limit_scrape_per_page = 10000;  ^to load 15 each time?
        #KY: Is the output(?) of check_overide_js used for subsequent steps, eg. scrape_checkout?
        print('++++++++\t line257: in scrape_process(), wait 30 seconds')
        time.sleep(30)
        page = driver.find_element_by_xpath("//*").get_attribute("innerHTML")
        page_text = BeautifulSoup(page, 'html.parser')
        print("++++++++\t line261: in scrape_process(), before scrape_checkout()")
        scrape_checkout(page_text)
        print("++++++++\t line263: in scrape_process(), AFTER scrape_checkout()")
    except:
        time.sleep(300)
        print('++++++++\t line266: in scrape_process(), restarting..')
        driver.quit()
        os.execv(sys.executable, ['python'] + sys.argv)

    #KY: Process(?) - Scrape all statements
    check_overide_js(limit_scrape_per_page, 5, 'statement', 15)     

    print('++++++++\t line273: in scrape_process(), wait 30 seconds')
    time.sleep(30)
    page = driver.find_element_by_xpath("//*").get_attribute("innerHTML")
    page_text = BeautifulSoup(page, 'html.parser')
    try:
        print("++++++++\t line278: in scrape_process(), before scrape_statement()")
        scrape_statement(page_text)
        print("++++++++\t line280: in scrape_process(), AFTER scrape_statement()")
    except:
        print('++++++++\t line282: in scrape_process(), restarting..')
        driver.quit()
        os.execv(sys.executable, ['python'] + sys.argv)
    print("++++++++\t line285: complete scrape_process")


def func_LineNotify(Message, LineToken):
    url = "https://notify-api.line.me/api/notify"       #KY: Notification on Line App
    data = ({'message': Message})
    LINE_HEADERS = {"Authorization": "Bearer " + LineToken}
    session = requests.Session()
    response = session.post(url, headers=LINE_HEADERS, data=data)
    return response

    

def name_matching(match_dict):
    for key, value in match_dict.items():
        user_name = value.get('buyer_name_x')
        customer_name = value.get('customer_name')
        statement_buyer_name = value.get('buyer_name_y')
        description = value.get('description')

        found_name = 25

        if customer_name and len(customer_name) >= 5 and ('shopee' not in customer_name):

            if (statement_buyer_name) and statement_buyer_name != 'nan' and (customer_name in statement_buyer_name):
                found_name = 1

            elif (description) and description != 'nan' and (customer_name in description):
                found_name = 3

            else:
                if statement_buyer_name and statement_buyer_name != 'nan':
                    ratio = fuzz.ratio(customer_name, statement_buyer_name)
                    partial_ratio = fuzz.partial_ratio(
                        customer_name, statement_buyer_name)
                    token_sort_ratio = fuzz.token_sort_ratio(
                        customer_name, statement_buyer_name)
                    token_set_ratio = fuzz.token_set_ratio(
                        customer_name, statement_buyer_name)

                    if ratio >= 65 and token_set_ratio >= 65:
                        found_name = 5

                    match_dict[key]['ratio'] = ratio
                    match_dict[key]['partial_ratio'] = partial_ratio
                    match_dict[key]['token_sort_ratio'] = token_sort_ratio
                    match_dict[key]['token_set_ratio'] = token_set_ratio

        else:
            if (statement_buyer_name) and statement_buyer_name != 'nan' and (user_name in statement_buyer_name):
                found_name = 2

            elif (description) and description != 'nan' and (user_name in description):
                found_name = 4

        match_dict[key]['found_name'] = found_name

    return match_dict


while True:
    print("start - line300")

    columns_statement = [ 'statement_id', 'statement_status', 'statement_amount', 'statement_recieved_time', 
                            'statement_uploaded_time', 'statement_updated_time', 'buyer_name', 'buyer_bank', 'buyer_account', 
                            'shopee_bank', 'transfer_method', 'description', 'checkout_candidates', 'action']
    df_statement = pd.DataFrame(columns=columns_statement)

    columns_checkout = ['checkout_id', 'buyer_name', 'payment_missing_amount', 'price_after_unique_price', 
                                'shipping_name', 'payment_created_time', 'customer_name', 'date_of_transfer',
                                'tranfer_from_bank', 'transfer_from_bank_account', 'transfer_to_shopee_bank',
                                'amount_transfered', 'proof_status', 'proof_id', 'memo']
    df_checkout = pd.DataFrame(columns=columns_checkout)


    columns_merge = [   'statement_id','statement_amount', 'statement_recieved_time','shopee_bank','buyer_name_x', 
                        'description', 'checkout_id','buyer_name_y','price_after_unique_price', 'date_of_transfer', 
                        'transfer_to_shopee_bank', 'customer_name', 'shipping_name', 'payment_created_time', 'time_after_checkout', 
                        'time_after_proof', 'c_freq', 's_freq', 'found_name', 'ratio', 'partial_ratio', 
                        'token_sort_ratio', 'token_set_ratio']

    today = f'{datetime.date.today()}-{datetime.datetime.today().hour:02}-{datetime.datetime.today().minute:02}'
    open_backend()
    time.sleep(5)
    scrape_process()

    df_statement.to_csv('scrape_statement_raw.csv', header=True)
    df_checkout.to_csv('scrape_checkout_raw.csv', header=True)

    print(f'total_statement --> {df_statement.shape[0]}')
    print(f'total_checkout --> {df_checkout.shape[0]}')

    #KY: Extracting columns to match with checkout table. Observed that not all records have buyer's name filled,
    #KY: but buyer's name is more likely to be indicated in description field
    df_statement_m = df_statement[['statement_id','statement_amount', 'statement_recieved_time','shopee_bank','buyer_name', 'description']]
    df_statement_m['statement_amount'] = df_statement_m['statement_amount'].astype(np.int64)     #KY: convert amount from float to string such that its value can be compared when using pd.merge
    df_statement_m['statement_recieved_time'] = df_statement_m['statement_recieved_time'].apply(
        lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S', errors='coerce'))           #KY: convert to datetime to calculate time difference later
    df_statement_m['buyer_name'] = df_statement_m['buyer_name'].str.lower().str.strip()     #KY: strip to remove whitespace
    df_statement_m['description'] = df_statement_m['description'].str.lower().str.strip()



    df_checkout_m = df_checkout[['checkout_id','buyer_name','price_after_unique_price', 'payment_created_time', 'date_of_transfer', 
                        'transfer_to_shopee_bank', 'customer_name', 'shipping_name']]
    df_checkout_m['price_after_unique_price'] = df_checkout_m['price_after_unique_price'].astype(np.int64)
    df_checkout_m['payment_created_time'] = df_checkout_m['payment_created_time'].apply(
        lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S', errors='coerce'))
    df_checkout_m['date_of_transfer'] = df_checkout_m['date_of_transfer'].apply(
        lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S', errors='coerce'))
    df_checkout_m['customer_name'] = df_checkout_m['customer_name'].str.lower().str.strip()
    df_checkout_m['shipping_name'] = df_checkout_m['shipping_name'].str.lower().str.strip()
    df_checkout_m['transfer_to_shopee_bank'] = df_checkout_m['transfer_to_shopee_bank'].str.lower().str.strip()

    # df_statement_m.sort_values('statement_amount', inplace=True, ascending=True) 
    # df_checkout_m.sort_values('price_after_unique_price', inplace=True, ascending=True) 

    print("check stmt and checkout size after formatting")
    print(f'total_statement --> {df_statement_m.shape[0]}')
    print(f'total_checkout --> {df_checkout_m.shape[0]}')


    #KY: Process - Match the statement to checkout based on statement amount = pmt amount AND time difference < 2mins
    #KY: Matching Criterias:
    #KY: (1) Statement amount == payment amount from checkout table, bank specified in statement == bank specified in checkout
    #KY: (2), else check if buyer_name from statement table can be found in customer_name of checkout table.
    match = df_statement_m.merge(
        df_checkout_m, left_on=['statement_amount', 'shopee_bank'], right_on=['price_after_unique_price', 'transfer_to_shopee_bank'], how='inner')
    match['time_after_checkout'] = match['statement_recieved_time'] - match['payment_created_time']
    match['time_after_proof'] = match['statement_recieved_time'] - match['date_of_transfer']
    #KY: Additional calculation is done to calculate stmt_received_time - checkout_time, labelled as time_after_checkout
    #KY: time_dif ^ is now 'time_after_proof' for better clarity
    print("++++++++\t line355: first match")
    # print("++++++++\t line356: name match, saving to csv")
    # match.to_csv("check match before name.csv", header=True)

    #KY: Timedelta is used to measure time diff in units. Using this to remove time_dif with negative day
    match = match[(match['time_after_checkout'] >= pd.Timedelta(0, 'D'))]    #KY: Logic has been modified here. time_after_checkout must be >= 0

    #KY: checkout_freq and stmt_freq are used to check the freq of each checkoutid and stmtid after the first matching.
    #KY: Records with checkout_freq and stmt_freq being 1 means that the match is unique. These records are stored in unique_matches df.
    checkout_freq = match.groupby(['checkout_id']).size().reset_index(name='c_freq')
    stmt_freq = match.groupby(['statement_id']).size().reset_index(name='s_freq')
    match = match.merge(checkout_freq, on=['checkout_id'], how='left')
    match = match.merge(stmt_freq, on=['statement_id'], how='left')

    #KY: To sort match by time_dif in ascending order and the sorted table is reassigned to match. Without in_place, need to assign match.sort_value() to a variable
    match.sort_values('time_after_checkout', inplace=True, ascending=True)  #KY: Logic modified here to sort by time_after_checkout
    match = match.reset_index()
    match = match.drop(columns='index')


    unique_matches = match[match['c_freq'] == 1]
    unique_matches = unique_matches[unique_matches['s_freq'] == 1]
    unique_matches.reindex(columns=columns_merge, fill_value=25)
    unique_matches['unique'] = 1
    unique_matches['found_name'] = 0        #KY: Name matching not required for unique_matches, so label as 0 first.
    print("++++++++\t line381: saving to csv")
    unique_matches.to_csv("unique_matches.csv", header=True)

    non_unique_matches = match.drop(unique_matches.index)       #KY: Identify non-unique matches for name matching later
    # non_unique_matches.to_csv("non_unique_matches.csv", header=True)

    match_dict = non_unique_matches.to_dict(orient='index')     #KY: Converting df to python dictionary to extract values from each row later during name matching
    match_dict = name_matching(match_dict)  #KY: name_matching() method


    print(f'total_statement --> {df_statement_m.shape[0]}')
    print(f'total_checkout --> {df_checkout_m.shape[0]}')
    print(f'total matching --> {match.shape[0]}')
    print(f'total unique --> {unique_matches.shape[0]}')
    print(f'total non-unique --> {non_unique_matches.shape[0]}')

    name_match = pd.DataFrame.from_dict(match_dict, orient='index', columns=columns_merge)
    print("++++++++\t line462: name match, saving to csv")
    name_match.to_csv("check name match.csv", header=True)


    name_match = name_match[name_match['found_name'] < 15]

    print(f'total name match --> {name_match.shape[0]}')

    name_match.sort_values(['found_name', 'time_after_proof'], inplace=True, ascending=True)

    name_match = name_match.drop(['c_freq', 's_freq'], axis=1) #drop and redo freq count
    checkout_freq = name_match.groupby(['checkout_id']).size().reset_index(name='c_freq')
    stmt_freq = name_match.groupby(['statement_id']).size().reset_index(name='s_freq')
    name_match = name_match.merge(checkout_freq, on=['checkout_id'], how='left')
    name_match = name_match.merge(stmt_freq, on=['statement_id'], how='left')
    name_match = name_match[name_match['c_freq'] == 1]
    name_match = name_match[name_match['s_freq'] == 1]

    print(f'total UNIQUE name match --> {name_match.shape[0]}')

    combine = unique_matches.append(name_match, ignore_index=True)
    combine = combine.reindex(columns=name_match.columns)
    combine.to_csv("check combine.csv", header=True)
    print(f'total combine --> {combine.shape[0]}')




    break



    # match['statement_amount'] = match['statement_amount'].astype(np.float64)
    # match['payment_total_amount'] = match['payment_total_amount'].astype(np.float64)
    # match['amount_transfered'] = match['amount_transfered'].astype(np.float64)
    # match['payment_missing_amount'] = match['payment_missing_amount'].astype(np.float64)

    # match = match[match['amount_match'] == 1]     #KY: Unsure what this line does
    # match.drop(columns=['result'], inplace=True)

    # except_price = pd.read_excel(
    #     'config.xlsx', sheet_name='except_price', encoding='utf-8-sig')
    # list_except_price = except_price['price'].tolist()  #KY: tolist() converts Dataframe to Python List.

    # #KY: isin returns boolean results for each element in match['statement_amount'], whether each element is contained list_except_price
    # #KY: ~ will inverse the boolean values. True will be for cells with initial value not in list_except_price
    # #KY: Unsure why result of isin and ~ is reassigned to match..
    # match = match[~match['statement_amount'].isin(list_except_price)]
    # promo_match = match[match['statement_amount'].isin(list_except_price)]
    # match.to_csv(f'./record/match-{today}.csv',
    #              index=False, encoding='utf-8-sig')

    # #KY: What's the difference between match and promo_match?

    # print(f'total_statement --> {df_statement_m.shape[0]}')     #KY: df.shape[0] returns num of rows
    # print(f'total_checkout --> {df_checkout_m.shape[0]}')
    # print(f'total matching --> {match.shape[0]}')
    # print(f'total exceptional promo matching --> {promo_match.shape[0]}')
    # config = pd.read_excel(
    #     'config.xlsx', sheet_name='main', encoding='utf-8-sig')

    # texts_matching = list()
    # for row in config.index:
    #     bot_number = config.loc[row, 'tier']
    #     min = config.loc[row, 'min']    #KY: Locate value at row, and col 'min' ?
    #     max = config.loc[row, 'max']
    #     if bot_number == 23:
    #         temp = match[(match['statement_amount'] >= min)]
    #         print(f'{bot_number:02} --> {temp.shape[0]}')
    #         texts_matching.append(temp.shape[0])
    #         if temp.shape[0] > 0:
    #             temp.to_csv(
    #                 f'../BOT-{bot_number:02}/input/BOT-{bot_number:02}-{today}.csv', index=False, encoding='utf-8-sig')
    #     else:
    #         temp = match[(match['statement_amount'] >= min) &
    #                      (match['statement_amount'] <= max)]    #KY: Narrow down to rows where statement_amount is within min and max
    #         print(f'{bot_number:02} --> {temp.shape[0]}')
    #         texts_matching.append(temp.shape[0])                #KY: Append number of rows in temp to text_matching
    #         if temp.shape[0] > 0:
    #             temp.to_csv(
    #                 f'../BOT-{bot_number:02}/input/BOT-{bot_number:02}-{today}.csv', index=False, encoding='utf-8-sig')

    # #KY: Unsure from here onwards
    # texts_backlog = list()
    # for i in range(23):
    #     files = [file for file in os.listdir(f'../BOT-{i+1:02}/input') if file.endswith('.csv')]
    #     texts_backlog.append(len(files))

    # # line notify
    # Message = f"\r\n {datetime.datetime.now()}"
    # Message += f"\r\n bot --> matching_txn --> backlog_files"
    # for index, text in enumerate(texts_matching):
    #     if texts_backlog[index] > 3:
    #         Message += f"\r\n {index+1:02} --> {text} --> {texts_backlog[index]} ***"
    #     else:
    #         Message += f"\r\n {index+1:02} --> {text} --> {texts_backlog[index]}"

    # # try:
    # #     Response = func_LineNotify(Message, line_notify)
    # # except Exception as e:
    # #     print(e)

    # print(f'{datetime.datetime.now()}')
    # print('wait for the next cycle 60 seconds')
    # print(
    #     '--------------------------------------------------------------------------')
    # time.sleep(60)

### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SETUP >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

###############
### Modules ###
###############

from datetime import datetime
import json
import os
import re
import sys
import time
from datetime import datetime
import math

import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import *

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from bs4 import BeautifulSoup

global options
global driver
global wait

######################
### Selenium Setup ###
######################

path = os.getcwd()

capabilities = {
    'chromeOptions': {
        'useAutomationExtension': False,
        'args': ['--start-maximized',
                 '--disable-infobars',
                 '--disable-notifications',
                 '--disable-popup-blocking'
                 ]
    }
}

chrome_options = Options()
chrome_options.add_argument("--user-data-dir=cookie")

driver = webdriver.Chrome(f'{path}\\chromedriver.exe', desired_capabilities=capabilities, options=chrome_options)
if not os.path.exists('cookie'):
    chrome_options.add_argument("user-data-dir=cookie") 
wait = WebDriverWait(driver, 15, ignored_exceptions=[ElementNotVisibleException, ElementNotSelectableException])
wait_be = WebDriverWait(driver, 60, ignored_exceptions=[ElementNotVisibleException, ElementNotSelectableException])

##################
### Parameters ### EDIT AS REQUIRED!!
##################

loading_page__restart_time = 10     # 1) How long to wait for loading of pages
loading_page__restart_attempt = 6   # 2) Maximum attempts to wait for loading of pages
name_match__min_len = 5             # 3) Minimum length to do exact name match (>= len)
exact_name_match__min_ratio = .4    # 4) Minimum ratio to do exact name match
max_ratio_cutoff = 88               # 5) Minimum max ratio threshold
simple_ratio_cutoff = 64            # 6) Minimum simple ratio threshold
refresh = False                     # 7) Decide if should refresh page or not
stmt_scraped = 1

# List of regex
re_list = [
    # 1) Transfer channels
    r'(edc|wbnk)(setor(an)?|(.*)(sa|str)\b)?',
    r'flip(tech lentera ins[a-z]* (pertiwi)?)?',
    r'branch(.*)(transfer|sdr(i|.)|bpk|echannel|setor|ibu)',
    r'kartu(.*)\|',
    r'(trsf e-banking (cr|db)|switching cr)( tanggal)?',
    r'transf(er)?|trs?f(la|hmb?| (fr|to))?',
    r'\b(((to( |.)pt( |.))?|(pt( |.))?|(to( |.))?)(air?.?pay|divisi) inter[a-z]*)\b',
    r'\b(atmb?((.*)(transfer|t?fr|(s|l)trbca|xmd|plus|credi?t|\-[a-z]*))?)(?:\b|$)',
    r'mcm inhousetrf cs-cs',
    r'(payfazz?|kudo)( tekh?nologi (nusantara|n|indo))?',
    r'go mobile',
    r'inw(.*)mcs',
    # 2) Trim banks names
    r'\b(bank|maybank|bca|bri(agro)?|bni|mandiri?|cimb( niaga|(.*)trf fr)?|bpd|jateng|negara|muamalat|jatim|syariah|commonwealth|sinarmas|woori saudara|ntt|mega|bjb|dbs|papua|bankaltimtara|mnc|btn|bsm)\b',
    # 3) Location names
    r'\b(bal ?ikpapan|tunai|bali|sulut|riau|dki|surya indo ?jaya|pacitan)\b',
    # 4) Product names
    r'pembayaran((.*)shopp?ee?)?',
    # 5) Payment method
    r'\b(bizchannel|bersama from la|wdocash|(sa|ob|ca|cr)|v.?i.?a|(ibnk|i(nter)?ba?nk)|toko|m-bk trf ca\/sa|sms|str|trx|pg_txn|pt|wsid|wbnk|tunai|tabungan|ovo|(prma|bsm) *cr|no book|cash deposit|sweep balance)\b',
    # 6) Other words
    r'\b(muhamm?ad|dr|mp|ibu|shopee?|unuk|utk|pembelian|dari|indonesia|antar|setor(an)?)\b',
    # 7) Numeric, alpha-numeric
    r'(\"|\:|\||\-|\(|\)|\.|\/|\*|\#)',
    r'([a-z]|[0-9]|\/)*[0-9]([a-z]|[0-9]|\/)*'
]

remove = re.compile('|'.join(re_list),flags=re.I)
replace_empty = r'^(\s+)\b|\b(\s*)$|^(\s*)$'

# Original Javascript
with open(f'{path}\\js\\match_payment_request_pool.js', 'r') as r:
    js_text = r.read()

########################
### Parameters - END ### 
########################

### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> FUNCTIONS >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

def open_backend():
    global loading_page__restart_time
    original_loading_page__restart_time = loading_page__restart_time

    url = 'https://admin.shopee.co.id/payment/match_payment_request_pools/'
    # url = 'https://admin.uat.shopee.co.id/payment/match_payment_request_pools/'

    print('\n++++++++\t (0) Opening backend...')
    init_time_1 = time.time()       #KY: Current time in unixtime
    while True:
        time.sleep(0.1)
        if time.time() - init_time_1 >= loading_page__restart_time:
            print('++++++++\t     Restarting open_backend()...\n')
            open_backend()
        else:
            try:
                driver.get(url)
                time.sleep(2)
                # wait.until(EC.visibility_of_element_located((By.XPATH, '//*[contains(text(), "Sign in")]'))) # and @id="headingText"
                
                if driver.current_url == url:
                    print("++++++++\t     Success\n")
                    loading_page__restart_time = original_loading_page__restart_time
                    return
                else:
                    print("++++++++\t     Failure: Please authenticate in the next 1 minute(s)\n")
                    loading_page__restart_time = 120
                    wait_be.until(EC.title_contains('Match Payment Request'))
            except:
                pass


def refresh_statement():
    refresh_button = driver.find_element_by_xpath("//*[@class='ui teal button js-reload-bankstmt agent']")
    refresh_button.click()


def scrape_statement_df():
    # Setup DataFrame
    columns_statement = ['statement_id', 'statement_amount', 'statement_recieved_time', 'statement_uploaded_time', 'shopee_bank', 'description', 'checkout_candidates']

    df_statement = pd.DataFrame(columns=columns_statement)

    # Begin scraping
    bs_obj = BeautifulSoup(driver.page_source, 'html.parser')
    cells = bs_obj.find(id = 'statement-table').find('tbody').find('tr').find_all('td')

    statement_id = int(cells[1].text)
    statement_amount = int(cells[3].text[3:])
    statement_recieved_time = datetime.strptime((cells[4].text).split(' +')[0], '%d-%m-%Y %H:%M:%S')
    statement_uploaded_time = datetime.strptime((cells[5].text).split(' +')[0], '%d-%m-%Y %H:%M:%S')
    shopee_bank = cells[10].text
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
    description = cells[12].text
    checkout_candidates = cells[13].text

    data = [[statement_id, statement_amount, statement_recieved_time, statement_uploaded_time, shopee_bank, description, checkout_candidates]]

    df_statement = df_statement.append(pd.DataFrame(columns=columns_statement, data=data, index=[0]), ignore_index=True)

    return df_statement


def scrape_checkout_df():
    # Setup DataFrame
    columns_checkout = ['checkout_id', 'shipping_name', 'customer_name', 'date_of_transfer', 'transfer_to_shopee_bank']

    df_checkout = pd.DataFrame(columns=columns_checkout)

    bs_obj = BeautifulSoup(driver.page_source, 'html.parser')
    rows = bs_obj.find(id = 'normal-checkout-table').find('tbody').find_all('tr')

    for row in rows:
        cells = row.find_all('td')

        checkout_id = int(cells[1].text)
        shipping_name_list = [x for x in cells[5].find('div').contents if getattr(x, 'name', None) != 'br']
        # shipping_name = str(cells[5].text)
        customer_name = str(cells[7].text)
        try:
            date_of_transfer = datetime.strptime((cells[8].text).split(' +')[0], '%d-%m-%Y %H:%M:%S')
        except ValueError: # When proof not available
            date_of_transfer = None
        transfer_to_shopee_bank = cells[11].text
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

        data = [[checkout_id, x, customer_name, date_of_transfer, transfer_to_shopee_bank] for x in shipping_name_list]
        # data = [[checkout_id, shipping_name, customer_name, date_of_transfer, transfer_to_shopee_bank]]

        df_checkout = df_checkout.append(pd.DataFrame(columns=columns_checkout, data=data), ignore_index=True)

    return df_checkout


def find_relevant_checkouts(statement_amount, statement_bank, input_fields = ['missing_amount','price_after_unique_code','transfer_amount']):

    print('\n>>>>>>>>>>\t (2) Loading full checkout table...\n')

    # Step 1: Setup
    ####################################
    columns_checkout = ['checkout_id', 'shipping_name', 'customer_name', 'date_of_transfer', 'transfer_to_shopee_bank']
    df_checkout = pd.DataFrame(columns=columns_checkout)

    # Step 2: Collect checkouts
    ####################################
    for input_field in input_fields:

        # Enter Price and Bank
        input_price = driver.find_element_by_xpath(f'//*[@x="{input_field}"]')
        input_price.send_keys(statement_amount)
        input_price.send_keys(Keys.ENTER)

        # Wait until loading screen disappears
        try:
            try_until_success('load_checkout')
        except TimeoutException: # if timeout after 6 attempts
            print('\n[!] TableLoadingError: Refreshing and continuing to next statement...\n')
            continue

        # Collect checkouts if exist
        pages = driver.find_element_by_css_selector('.choose-checkout.ui.segment').find_elements_by_css_selector('.item.number-button')

        # Load pages (if any)
        if len(pages) == 0:
            print('\tScrape failure (no available checkouts for price column): ',input_field)
            pass
        else:
            # if len(pages) > 1:
            #     load_all_pages(len(pages)*10, 15)
            #     global refresh
            #     refresh = True

            # Attempt scraping     
            df = scrape_checkout_df()
            df_checkout = df_checkout.append(df, ignore_index=True)

            print('\tScrape success: ',input_field)

        # Wait until loading screen disappears (if any)
        try:
            try_until_success('clear_chkout_loading')
        except TimeoutException: # if timeout after 6 attempts
            print('\n[!] TableLoadingError: Refreshing and continuing to next statement...\n')
            continue
        try:
            try_until_success('clear_input',element=input_price,element_name=input_field)
        except:
            continue

    # Step 3: Reoganise checkout table -- Clean up irrelevant checkouts (duplicates, unrelated banks)
    ####################################
    if df_checkout.empty:
        pass
    else:    
        df_checkout.drop_duplicates(subset='checkout_id', inplace=True)
        df_checkout = df_checkout[(df_checkout['transfer_to_shopee_bank']=='')|(df_checkout['transfer_to_shopee_bank']==stmt_bank)]

        # Step 4: Wide to Long table
        ####################################
        id_col = ['checkout_id', 'date_of_transfer', 'transfer_to_shopee_bank']
        # id_col = ['checkout_id', 'buyer_name', 'payment_created_time', 'date_of_transfer', 'tranfer_from_bank', 'transfer_from_bank_account', 'transfer_to_shopee_bank', 
        # 'payment_missing_amount', 'price_after_unique_price', 'amount_transfered', 'proof_status', 'proof_id', 'memo']

        df_checkout = pd.melt(df_checkout,id_vars=id_col,var_name='name_type',value_name='name')

        # Step 5: Clean Customer Names
        ####################################
        df_checkout['name_clean'] = df_checkout['name'].str.replace(remove,'',regex=True)
        df_checkout['name_clean'].replace(replace_empty,'',inplace=True,regex=True)

        # Step 6: Final cleaning
        ####################################
        df_checkout = df_checkout[df_checkout['name_clean'].str.len() >= name_match__min_len]
        df_checkout.drop_duplicates(subset=['checkout_id','name_clean'], inplace=True)
        
        df_checkout.reset_index(drop=True, inplace=True)
    
    return df_checkout


def load_all_pages(total_checkout, load_freq):

    # Execute java script modification
    js_text_new = js_text.replace('{total_checkout}', str(total_checkout))
    driver.execute_script(js_text_new)      #KY: execute_script runs javascript to find an element and converts the returned DOM object to a WebElement object
    print("\t\t  -- Executing JS Override")

    # Load all pages
    init_time_1 = time.time()
    while True:
        time.sleep(0.1)
        if time.time() - init_time_1 >= loading_page__restart_time:
            return True
        else:
            page = driver.find_element_by_xpath(
                "//*[@id='normal-checkout-table']").get_attribute("innerHTML")  #KY: normal-checkout-table contains checkout records formatted in table with thead and tbody tags
            page_text = BeautifulSoup(page, 'html.parser')
            page_row = len(page_text.findAll('tr')) - 2
            if page_row >= load_freq:
                return True


def process_name_matching(df, desc):

    # Setup 
    #############
    chkoutid = None
    pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None, None, None, None
    candidate_chkout, candidate_names, p0_scores, p1_scores, p2_scores, p3_scores = None, None, None, None, None, None


    if len(df.index) == 0:
        ###################
        ### Situation 1 ### -- No candidate checkouts
        ###################
        chkoutid = 'Amount / Bank wrong'
        pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None, None, None, None
        candidate_chkout, candidate_names, p0_scores, p1_scores, p2_scores, p3_scores = None, None, None, None, None, None
    else:
        df_exact = df[df['name_clean'].map(lambda x: x in desc if x != '' and (len(x)/len(desc)) > exact_name_match__min_ratio else False)]

        if len(df_exact.index) == 0:
            ###################
            ### Situation 2 ### -- No exact match
            ###################

            # Get 4 ratios for candidate checkouts
            score1 = list(process.extractWithoutOrder(desc, df['name_clean'].tolist(), scorer=fuzz.ratio))
            df['simple_ratio'] = pd.Series([s[1] for s in score1])
            score2 = list(process.extractWithoutOrder(desc, df['name_clean'].tolist(), scorer=fuzz.partial_ratio))
            df['partial_ratio'] = pd.Series([s[1] for s in score2])
            score3 = list(process.extractWithoutOrder(desc, df['name_clean'].tolist(), scorer=fuzz.token_sort_ratio))
            df['sort_ratio'] = pd.Series([s[1] for s in score3])
            score4 = list(process.extractWithoutOrder(desc, df['name_clean'].tolist(), scorer=fuzz.token_set_ratio))
            df['set_ratio'] = pd.Series([s[1] for s in score4])

            # Get max score
            df['max_ratio'] = df[['simple_ratio','partial_ratio','sort_ratio','set_ratio']].max(axis=1)

            # Get max of max
            df_approx = df[(df['max_ratio']>max_ratio_cutoff) & (df['simple_ratio']>simple_ratio_cutoff)]
            best_ratio = df_approx['max_ratio'].max()
            df_best = df_approx[df_approx['max_ratio'] == best_ratio]
            
            if len(df_best.index) == 0:
                #####################
                ### Situation 2.1 ### -- No exact match + No suitable candidates
                #####################
                chkoutid = None
                pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None, None, None, None

            else:
                df_best.drop_duplicates(subset='checkout_id', inplace=True)

                if len(df_best.index) == 1:
                    #####################
                    ### Situation 2.2 ### -- No exact match + 1 BEST found
                    #####################
                    chkoutid = df_best['checkout_id'].item()
                    pmatch_name = df_best['name_clean'].item()
                    pmatch_time = df_best['date_of_transfer'].item() # proof upload time
                    pmatch_max_score = df_best['max_ratio'].item()
                    pmatch_simple_score = df_best['simple_ratio'].item()
                    pmatch_partial_score = df_best['partial_ratio'].item()
                    pmatch_sort_score = df_best['sort_ratio'].item()
                    pmatch_set_score = df_best['set_ratio'].item()
                else:
                    #####################
                    ### Situation 2.2 ### -- No exact match + MANY BEST found
                    #####################
                    chkoutid = df_best['checkout_id'].tolist()
                    pmatch_name = df_best['name_clean'].tolist()
                    pmatch_time = df_best['date_of_transfer'].tolist() # proof upload time
                    pmatch_max_score = df_best['max_ratio'].tolist()
                    pmatch_simple_score = df_best['simple_ratio'].tolist()
                    pmatch_partial_score = df_best['partial_ratio'].tolist()
                    pmatch_sort_score = df_best['sort_ratio'].tolist()
                    pmatch_set_score = df_best['set_ratio'].tolist()

            candidate_chkout = df['checkout_id'].tolist() # check subset of amount & bank
            candidate_names = df['name_clean'].tolist()
            p0_scores = df['simple_ratio'].tolist()
            p1_scores = df['partial_ratio'].tolist()
            p2_scores = df['sort_ratio'].tolist()
            p3_scores = df['set_ratio'].tolist()

        elif len(df_exact.index) == 1:
            ###################
            ### Situation 3 ### -- 1 exact match
            ###################
            chkoutid = df_exact['checkout_id'].item() # use subset of amount, bank & name
            pmatch_name = df_exact['name_clean'].item()
            pmatch_time = df_exact['date_of_transfer'].item()
            pmatch_max_score = '[EXACT MATCH: ARE YOU SURE]'
            pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None
            candidate_chkout, candidate_names, p0_scores, p1_scores, p2_scores, p3_scores = None, None, None, None, None, None
        else:
            ###################
            ### Situation 4 ### -- MANY exact match
            ###################
            chkoutid = 'Many names found'
            pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None, None, None, None

            candidate_chkout = df_exact['checkout_id'].tolist() # check subset of amount & bank
            candidate_names = df_exact['name_clean'].tolist()
            p0_scores, p1_scores, p2_scores, p3_scores = None, None, None, None

    return (chkoutid, pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score, candidate_chkout, candidate_names, p0_scores, p1_scores, p2_scores, p3_scores)


def do_statement_matching(stmt_id, chkout_id):

    # Click statement button
    try:
        stmt_btn = driver.find_element_by_xpath(f'//*[@name="match-stmt" and @match-statement="{str(stmt_id)}"]')
    except NoSuchElementException:
        print('\nWarning: Statement has been changed')
        raise
    stmt_btn.click()

    # Find checkout in table & click radio button
    chkoutid = driver.find_element_by_xpath('//*[@x="checkoutid"]')
    chkoutid.send_keys(str(chkout_id))
    chkoutid.send_keys(Keys.ENTER)

    # Wait for checkout to finish loading
    try:
        try_until_success('load_checkout')
    except TimeoutException: # if timeout after 6 attempts
        raise

    # Try to click checkout radio button, raise exception other wise
    try:
        chkout_btn = driver.find_element_by_xpath(f'//*[@name="match-checkout" and @match-checkout="{str(chkout_id)}"]')
        chkout_btn.click()
    except NoSuchElementException:
        chkoutid.clear()
        print('\nWarning: Checkout has been changed')
        raise

    # Match
    match_btn = driver.find_element_by_xpath('//*[@class="ui teal button js-match w-control agent"]')
    match_btn.click()

    try:
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.ui.small.modal.transition.visible.active')))
    except TimeoutException:
        print('\t\t\t  -- Loading Page Timeout')
        pass

    approve_btn = driver.find_element_by_xpath('//*[@class="ui positive right labeled icon button js-matching-approve"]')
    approve_btn.click()

    try:
        wait.until(EC.alert_is_present())
    except TimeoutException:
        print('\t\t\t  -- Alert Timeout')
        pass

    try:
        alert = driver.switch_to.alert
        alert.accept()
    except NoAlertPresentException:
        print('Retrying alert acceptance...')
        try:
            wait.until(EC.alert_is_present())
        except TimeoutException:
            print('\t\t\t  -- Alert Timeout #2')
            pass
        alert = driver.switch_to.alert
        alert.accept()

    try:
        # wait for approval module to disappear
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, '.ui.small.modal.transition.visible.active')))
        # wait for checkout table to show
        wait.until(EC.visibility_of_element_located((By.ID, 'normal-checkout-table')))
        # Wait until loading screen disappears (if any)
        try:
            try_until_success('clear_chkout_loading')
        except TimeoutException: # if timeout after 6 attempts
            raise
    except TimeoutException:
        print('\t\t\t  -- Alert Timeout')
        pass

    # Clear checkout
    while True:
        try:
            chkoutid.clear()
            break
        except:
            ### BackendErrorCatchment: #2 server error after matching ###
            #############################################################
            try:
                button_server_error = driver.find_element_by_xpath('//div[@class="ui modal transition visible active scrolling"]').find_element_by_xpath('.//div[@class="ui ok button"]')
                button_server_error.click()
            except InvalidSelectorException:
                pass
            except NoSuchElementException:
                pass

    print(f'\n*** Statement ID #{str(stmt_id)} matched successfully to Checkout ID #{str(chkout_id)} ***\n')


def manage_error(error):

    if error == 'server_error':
        # Catch Type 1
        try:
            button_server_error = driver.find_element_by_xpath('//div[@class="ui modal transition visible active scrolling"]').find_element_by_xpath('.//div[@class="ui ok button"]')
            button_server_error.click()
            print('<< ErrorAvoided: Server Error #1 >>')
        except NoSuchElementException:
            # Catch Type 2
            try:
                button_server_error = driver.find_element_by_xpath('//div[@class="ui modal transition visible active"]').find_element_by_xpath('.//div[@class="ui ok button"]')
                button_server_error.click()
                print('<< ErrorAvoided: Server Error #2 >>')
            except NoSuchElementException:
                pass
        finally:
            return


def try_until_success(function, element=None, element_name=None):

    attempt = 0

    while True:
        #############
        ### Setup ###
        #############

        attempt += 1
        # What to do if attempt fails after 6 attempts
        if attempt > loading_page__restart_attempt:
            if function == 'page_refresh':
                print('[!][!] CriticalError: Refreshing page ineffective. Please restart BOT')
                driver.quit()
            elif function == 'load_checkout':
                try_until_success('page_refresh')
                raise TimeoutException
            elif function == 'clear_chkout_loading':
                try_until_success('page_refresh')
                raise TimeoutException
            elif function == 'clear_input':
                pass

        #########################
        ### Actual Attempt #1 ### Page Refresh
        #########################
        if function == 'page_refresh':
            try:
                driver.refresh()
                wait.until(EC.visibility_of_element_located((By.ID, 'article')))
                wait.until(EC.visibility_of_element_located((By.ID, 'normal-checkout-table')))
                break
            except TimeoutException:
                print('\t\t\t  -- Page Refresh Timeout')
                pass

        #########################
        ### Actual Attempt #2 ### Wait for checkout load
        #########################
        elif function == 'load_checkout':
            try:
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.choose-checkout.ui.segment.loading')))
                try:
                    wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, '.choose-checkout.ui.segment.loading')))
                    break
                except TimeoutException:
                    print('\t\t\t  -- Loading Table Timeout ... Retrying ...')
            except TimeoutException: # loading screen not visible after 10s. Assume load finished
                break

        #########################
        ### Actual Attempt #3 ### CLear sudden checkout loading
        #########################
        elif function == 'clear_chkout_loading':
            try:
                wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, '.choose-checkout.ui.segment.loading')))
                break
            except TimeoutException:
                print('\t\t\t  -- Loading Table Timeout ... Retrying ...')

        #########################
        ### Actual Attempt #4 ### Clear input price
        #########################
        elif function == 'clear_input':
            try:
                element.clear()
                break
            except InvalidElementStateException:
                try:
                    element = driver.find_element_by_xpath(f'//*[@x="{element_name}"]')
                    element.clear()
                    break
                except:
                    print('??Retrying unknown error??')
                    import pdb; pdb.set_trace()
                    pass
    return


### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> START SCRIPT >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

#########################################
### [A]: Opens BE & get new statement ###
#########################################
open_backend()

input_name = str(input('Enter BOT name (e.g. mustika1): '))

input_time = int(input('Enter BOT run time (in minutes): '))*60
interval_once = 30*60
interval_total = math.ceil(input_time/interval_once)

# Create new folder
now = datetime.now()
folder_name = 'results_live/'+input_name+'__'+now.strftime('%Y-%m-%d__%H-%M-%S')
os.mkdir(folder_name)

###########################
### [B] Setup Intervals ###
###########################

for run_count in range(interval_total):

    # Setup variables for next interval
    #######################
    elapsed_time = 0
    init_time = time.time()

    if run_count+1 == interval_total:
        if input_time % interval_once == 0:
            run_time = interval_once
        else:
            run_time = input_time % interval_once
    else:
        run_time = interval_once

    # Step 0.0: Setup next interval
    #######################
    columns_statement = ['statement_id', 'checkout_candidates', 'statement_amount', 'shopee_bank', 'statement_recieved_time', 'statement_uploaded_time', 'description', 'desc_clean', 
    '[match] chkoutid','[match] name','[match] proof upload time','[match] max score','[match] simple score','[match] partial score','[match] sort score','[match] set score',
    'candidate chkoutid','candidate names','candidate scores simple','candidate scores partial','candidate scores sort','candidate scores set','match_reason']

    df_statement_full = pd.DataFrame(columns=columns_statement)

    #####################
    ### [C] Begin BOT ###
    #####################
    while elapsed_time < run_time:

        # Step 1: Refresh and get statement
        #######################
        print(f'\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n>>>>>>>>>>\t (1) Getting New Statement... (#{stmt_scraped}) \t<<<<<<<<<<\n')
        stmt_scraped += 1

        ### BackendErrorCatchment: #1.1 server error after new statement ###
        ##################################################################
        manage_error('server_error')

        while True:
            try:
                time.sleep(1)
                refresh_statement()
                # Wait for statement to finish loading
                try:
                    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.choose-statement.ui.once.blurring.dimmable.segment.loading')))
                    wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, '.choose-statement.ui.once.blurring.dimmable.segment.loading')))
                except TimeoutException:
                    print('\t\t\t  -- Loading Page Timeout')
                    pass

                df_statement = scrape_statement_df()
                break
            except AttributeError:
                print('[!] ReleaseError: No statement currently available')
                print('\t-- Waiting for new statements...')
                time.sleep(60)
            except:
                ### BackendErrorCatchment: #1.2 server error after new statement ###
                ####################################################################
                print('\n++++++++\t Handling Server Error...\n')
                manage_error('server_error')

        stmt_amt = df_statement['statement_amount'][0]
        stmt_bank = df_statement['shopee_bank'][0]
        stmt_desc = df_statement['description'][0]
        print('Statement Amount: ',stmt_amt)
        print('Statement Description: ',stmt_desc)

        # Step 3a: Clean statement description
        #######################
        df_statement['desc_clean'] = df_statement['description'].str.replace(remove,'',regex=True)
        df_statement['desc_clean'].replace(replace_empty,'',inplace=True,regex=True)
        # Step 3b: Next statement if nothing left after cleaning
        #######################
        if len(df_statement['desc_clean'].item()) < name_match__min_len:
            df_statement['match_reason'] = 'desc name too short for matching'
            df_statement_full = df_statement_full.append(df_statement, ignore_index=True)
            print('\n++++++++\t Unable to match this statement\n')
            continue

        # Step 4: Retrieve relevant checkouts (based on statement amount and bank)
        #######################
        df_checkout = find_relevant_checkouts(str(stmt_amt), stmt_bank)

        # Step 5: Approximate matching
        #######################
        results = process_name_matching(df_checkout, df_statement['desc_clean'].item())

        df_statement[[
        '[match] chkoutid','[match] name','[match] proof upload time','[match] max score','[match] simple score','[match] partial score','[match] sort score','[match] set score',
        'candidate chkoutid','candidate names','candidate scores simple','candidate scores partial','candidate scores sort','candidate scores set'
        ]] = pd.DataFrame([list(results)])

        if isinstance(df_statement['[match] chkoutid'].item(), int):
            print('\n\tCongrats! Match Found, attempting matching now...')
            stmt_id = df_statement['statement_id'].item()
            chkout_id = df_statement['[match] chkoutid'].item()

            ### BackendErrorCatchment: #3 BE refreshes statement unexpectedly when matching ###
            ###################################################################################
            try:
                driver.find_element_by_xpath('//div[@class="ui inverted dimmer active"]')
                warning_msg = 'Bot unable to match due to BE error, please match manually'
                df_statement['match_reason'] = 'MatchError: ' + warning_msg
                print('[!] MatchError: ',warning_msg,'\n')
                continue
            except NoSuchElementException:
                try:
                    do_statement_matching(stmt_id, chkout_id)
                except NoSuchElementException:
                    warning_msg = 'Checkout has been cleared by other agents/bot'
                    df_statement['match_reason'] = 'MatchError: ' + warning_msg
                    print('[!] MatchError: ',warning_msg,'\n')
                except TimeoutException:
                    print('\n[!] TableLoadingError: Refreshing and continuing to next statement...\n')
        else:
            print('\n\tNo match found :( continuing to next statement...')

        # Refresh if JS overide
        #######################
        if refresh:
            refresh = False
            print('\n++++++++\t Refreshing page...\n')
            try_until_success('page_refresh')
            
            # Wait until checkout loading appears and disappears
            try:
                try_until_success('load_checkout')
            except TimeoutException: # if timeout after 6 attempts
                print('\n[!] TableLoadingError: Refreshing and continuing to next statement...\n')
                continue
                
        df_statement_full = df_statement_full.append(df_statement, ignore_index=True)

        elapsed_time = time.time() - init_time
        elapsed_time_print = elapsed_time+(interval_once*run_count)
        print('Run Time:\t',input_time,'s')
        print('Elapsed Time:\t',int(round(elapsed_time_print,0)),'s')

    ####################################
    ### To do after every while loop ###
    ####################################

    # Save each run attempt
    df_statement_full = df_statement_full[['statement_id', 'checkout_candidates', 'statement_amount', 'shopee_bank', 'statement_recieved_time', 'statement_uploaded_time', 'description', 'desc_clean', 
    '[match] chkoutid','[match] name','[match] proof upload time','[match] max score','[match] simple score','[match] partial score','[match] sort score','[match] set score',
    'candidate chkoutid','candidate names','candidate scores simple','candidate scores partial','candidate scores sort','candidate scores set','match_reason']]
    df_statement_full.drop_duplicates(subset='statement_id', inplace=True)

    df_statement_full.to_csv(f'{folder_name}/match_log_{run_count}.csv')

    # Refresh at end of every interval 
    try_until_success('page_refresh')

###########################
### End BOT save script ###
###########################

elapsed_time = time.time() - init_time
elapsed_time_print = elapsed_time+(interval_once*run_count)
print('\nMatch Completed')
print('\nTime Elapsed: ',int(round(elapsed_time_print,0)),'s')

driver.quit()
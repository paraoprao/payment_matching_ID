# -*- coding: future_fstrings -*-

### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> SETUP >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
### >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

###############
### Modules ###
###############

from __future__ import division
from __future__ import with_statement
from __future__ import absolute_import
from datetime import datetime
import json
import os
import re
import sys
import time
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
from io import open

global options
global driver
global wait

######################
### Selenium Setup ###
######################

path = os.getcwdu()

capabilities = {
    u'chromeOptions': {
        u'useAutomationExtension': False,
        u'args': [u'--start-maximized',
                 u'--disable-infobars',
                 u'--disable-notifications',
                 u'--disable-popup-blocking'
                 ]
    }
}

chrome_options = Options()
chrome_options.add_argument(u"--user-data-dir=cookie")

driver = webdriver.Chrome(f'{path}\\chromedriver.exe', desired_capabilities=capabilities, options=chrome_options)
if not os.path.exists(u'cookie'):
    chrome_options.add_argument(u"user-data-dir=cookie") 
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
    ur'(edc|wbnk)(setor(an)?|(.*)(sa|str)\b)?',
    ur'flip(tech lentera ins[a-z]* (pertiwi)?)?',
    ur'branch(.*)(transfer|sdr(i|.)|bpk|echannel|setor|ibu)',
    ur'kartu(.*)\|',
    ur'(trsf e-banking (cr|db)|switching cr)( tanggal)?',
    ur'transf(er)?|trs?f(la|hmb?| (fr|to))?',
    ur'\b(((to( |.)pt( |.))?|(pt( |.))?|(to( |.))?)(air?.?pay|divisi) inter[a-z]*)\b',
    ur'\b(atmb?((.*)(transfer|t?fr|(s|l)trbca|xmd|plus|credi?t|\-[a-z]*))?)(?:\b|$)',
    ur'mcm inhousetrf cs-cs',
    ur'(payfazz?|kudo)( tekh?nologi (nusantara|n|indo))?',
    ur'go mobile',
    ur'inw(.*)mcs',
    # 2) Trim banks names
    ur'\b(bank|maybank|bca|bri(agro)?|bni|mandiri?|cimb( niaga|(.*)trf fr)?|bpd|jateng|negara|muamalat|jatim|syariah|commonwealth|sinarmas|woori saudara|ntt|mega|bjb|dbs|papua|bankaltimtara|mnc|btn|bsm)\b',
    # 3) Location names
    ur'\b(bal ?ikpapan|tunai|bali|sulut|riau|dki|surya indo ?jaya|pacitan)\b',
    # 4) Product names
    ur'pembayaran((.*)shopp?ee?)?',
    # 5) Payment method
    ur'\b(bizchannel|bersama from la|wdocash|(sa|ob|ca|cr)|v.?i.?a|(ibnk|i(nter)?ba?nk)|toko|m-bk trf ca\/sa|sms|str|trx|pg_txn|pt|wsid|wbnk|tunai|tabungan|ovo|(prma|bsm) *cr|no book|cash deposit|sweep balance)\b',
    # 6) Other words
    ur'\b(muhamm?ad|dr|mp|ibu|shopee?|unuk|utk|pembelian|dari|indonesia|antar|setor(an)?)\b',
    # 7) Numeric, alpha-numeric
    ur'(\"|\:|\||\-|\(|\)|\.|\/|\*|\#)',
    ur'([a-z]|[0-9]|\/)*[0-9]([a-z]|[0-9]|\/)*'
]

remove = re.compile(u'|'.join(re_list),flags=re.I)
replace_empty = ur'^(\s+)\b|\b(\s*)$|^(\s*)$'

# Original Javascript
with open(f'{path}\\js\\match_payment_request_pool.js', u'r') as r:
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

    url = u'https://admin.shopee.co.id/payment/match_payment_request_pools/'
    # url = 'https://admin.uat.shopee.co.id/payment/match_payment_request_pools/'

    print u'\n++++++++\t (0) Opening backend...'
    init_time_1 = time.time()       #KY: Current time in unixtime
    while True:
        time.sleep(0.1)
        if time.time() - init_time_1 >= loading_page__restart_time:
            print u'++++++++\t     Restarting open_backend()...\n'
            open_backend()
        else:
            try:
                driver.get(url)
                time.sleep(2)
                # wait.until(EC.visibility_of_element_located((By.XPATH, '//*[contains(text(), "Sign in")]'))) # and @id="headingText"
                
                if driver.current_url == url:
                    print u"++++++++\t     Success\n"
                    loading_page__restart_time = original_loading_page__restart_time
                    return
                else:
                    print u"++++++++\t     Failure: Please authenticate in the next 1 minute(s)\n"
                    loading_page__restart_time = 120
                    wait_be.until(EC.title_contains(u'Match Payment Request'))
            except:
                pass


def refresh_statement():
    refresh_button = driver.find_element_by_xpath(u"//*[@class='ui teal button js-reload-bankstmt agent']")
    refresh_button.click()


def scrape_statement_df():
    # Setup DataFrame
    columns_statement = [u'statement_id', u'statement_amount', u'statement_recieved_time', u'statement_uploaded_time', u'shopee_bank', u'description', u'checkout_candidates']

    df_statement = pd.DataFrame(columns=columns_statement)

    # Begin scraping
    bs_obj = BeautifulSoup(driver.page_source, u'html.parser')
    cells = bs_obj.find(id = u'statement-table').find(u'tbody').find(u'tr').find_all(u'td')

    statement_id = int(cells[1].text)
    statement_amount = int(cells[3].text[3:])
    statement_recieved_time = datetime.strptime((cells[4].text).split(u' +')[0], u'%d-%m-%Y %H:%M:%S')
    statement_uploaded_time = datetime.strptime((cells[5].text).split(u' +')[0], u'%d-%m-%Y %H:%M:%S')
    shopee_bank = cells[10].text
    if u'bca' in shopee_bank.lower():
        shopee_bank = u'bca'
    elif u'bni' in shopee_bank.lower():
        shopee_bank = u'bni'
    elif u'bri' in shopee_bank.lower():
        shopee_bank = u'bri'
    elif u'cimb' in shopee_bank.lower():
        shopee_bank = u'cimb'
    elif u'mandiri' in shopee_bank.lower():
        shopee_bank = u'mandiri'
    description = cells[12].text
    checkout_candidates = cells[13].text

    data = [[statement_id, statement_amount, statement_recieved_time, statement_uploaded_time, shopee_bank, description, checkout_candidates]]

    df_statement = df_statement.append(pd.DataFrame(columns=columns_statement, data=data, index=[0]), ignore_index=True)

    return df_statement


def scrape_checkout_df():
    # Setup DataFrame
    columns_checkout = [u'checkout_id', u'shipping_name', u'customer_name', u'date_of_transfer', u'transfer_to_shopee_bank']

    df_checkout = pd.DataFrame(columns=columns_checkout)

    bs_obj = BeautifulSoup(driver.page_source, u'html.parser')
    rows = bs_obj.find(id = u'normal-checkout-table').find(u'tbody').find_all(u'tr')

    for row in rows:
        cells = row.find_all(u'td')

        checkout_id = int(cells[1].text)
        shipping_name_list = [x for x in cells[5].find(u'div').contents if getattr(x, u'name', None) != u'br']
        # shipping_name = str(cells[5].text)
        customer_name = unicode(cells[7].text)
        try:
            date_of_transfer = datetime.strptime((cells[8].text).split(u' +')[0], u'%d-%m-%Y %H:%M:%S')
        except ValueError: # When proof not available
            date_of_transfer = None
        transfer_to_shopee_bank = cells[11].text
        if u'bca' in transfer_to_shopee_bank.lower():
            transfer_to_shopee_bank = u'bca'
        elif u'bni' in transfer_to_shopee_bank.lower():
            transfer_to_shopee_bank = u'bni'
        elif u'bri' in transfer_to_shopee_bank.lower():
            transfer_to_shopee_bank = u'bri'
        elif u'cimb' in transfer_to_shopee_bank.lower():
            transfer_to_shopee_bank = u'cimb'
        elif u'mandiri' in transfer_to_shopee_bank.lower():
            transfer_to_shopee_bank = u'mandiri'

        data = [[checkout_id, x, customer_name, date_of_transfer, transfer_to_shopee_bank] for x in shipping_name_list]
        # data = [[checkout_id, shipping_name, customer_name, date_of_transfer, transfer_to_shopee_bank]]

        df_checkout = df_checkout.append(pd.DataFrame(columns=columns_checkout, data=data), ignore_index=True)

    return df_checkout


def find_relevant_checkouts(statement_amount, statement_bank, input_fields = [u'missing_amount',u'price_after_unique_code',u'transfer_amount']):

    print u'\n>>>>>>>>>>\t (2) Loading full checkout table...\n'

    # Step 1: Setup
    ####################################
    columns_checkout = [u'checkout_id', u'shipping_name', u'customer_name', u'date_of_transfer', u'transfer_to_shopee_bank']
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
            print u'\n[!] TableLoadingError: Refreshing and continuing to next statement...\n'
            continue

        # Collect checkouts if exist
        pages = driver.find_element_by_css_selector(u'.choose-checkout.ui.segment').find_elements_by_css_selector(u'.item.number-button')

        # Load pages (if any)
        if len(pages) == 0:
            print u'\tScrape failure (no available checkouts for price column): ',input_field
            pass
        else:
            # if len(pages) > 1:
            #     load_all_pages(len(pages)*10, 15)
            #     global refresh
            #     refresh = True

            # Attempt scraping     
            df = scrape_checkout_df()
            df_checkout = df_checkout.append(df, ignore_index=True)

            print u'\tScrape success: ',input_field

        # Wait until loading screen disappears
        try:
            try_until_success('clear_chkout_loading')
        except TimeoutException: # if timeout after 6 attempts
            print u'\n[!] TableLoadingError: Refreshing and continuing to next statement...\n'
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
        df_checkout.drop_duplicates(subset=u'checkout_id', inplace=True)
        df_checkout = df_checkout[(df_checkout[u'transfer_to_shopee_bank']==u'')|(df_checkout[u'transfer_to_shopee_bank']==stmt_bank)]

        # Step 4: Wide to Long table
        ####################################
        id_col = [u'checkout_id', u'date_of_transfer', u'transfer_to_shopee_bank']
        # id_col = ['checkout_id', 'buyer_name', 'payment_created_time', 'date_of_transfer', 'tranfer_from_bank', 'transfer_from_bank_account', 'transfer_to_shopee_bank', 
        # 'payment_missing_amount', 'price_after_unique_price', 'amount_transfered', 'proof_status', 'proof_id', 'memo']

        df_checkout = pd.melt(df_checkout,id_vars=id_col,var_name=u'name_type',value_name=u'name')

        # Step 5: Clean Customer Names
        ####################################
        df_checkout[u'name_clean'] = df_checkout[u'name'].replace(remove,u'',regex=True)
        df_checkout[u'name_clean'].replace(replace_empty,u'',inplace=True,regex=True)

        # Step 6: Final cleaning
        ####################################
        df_checkout = df_checkout[df_checkout[u'name_clean'].str.len() >= name_match__min_len]
        df_checkout.drop_duplicates(subset=[u'checkout_id',u'name_clean'], inplace=True)
        
        df_checkout.reset_index(drop=True, inplace=True)
    
    return df_checkout


def load_all_pages(total_checkout, load_freq):

    # Execute java script modification
    js_text_new = js_text.replace(u'{total_checkout}', unicode(total_checkout))
    driver.execute_script(js_text_new)      #KY: execute_script runs javascript to find an element and converts the returned DOM object to a WebElement object
    print u"\t\t  -- Executing JS Override"

    # Load all pages
    init_time_1 = time.time()
    while True:
        time.sleep(0.1)
        if time.time() - init_time_1 >= loading_page__restart_time:
            return True
        else:
            page = driver.find_element_by_xpath(
                u"//*[@id='normal-checkout-table']").get_attribute(u"innerHTML")  #KY: normal-checkout-table contains checkout records formatted in table with thead and tbody tags
            page_text = BeautifulSoup(page, u'html.parser')
            page_row = len(page_text.findAll(u'tr')) - 2
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
        chkoutid = u'Amount / Bank wrong'
        pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None, None, None, None
        candidate_chkout, candidate_names, p0_scores, p1_scores, p2_scores, p3_scores = None, None, None, None, None, None
    else:
        df_exact = df[df[u'name_clean'].map(lambda x: x in desc if x != u'' and (len(x)/len(desc)) > exact_name_match__min_ratio else False)]

        if len(df_exact.index) == 0:
            ###################
            ### Situation 2 ### -- No exact match
            ###################

            # Get 4 ratios for candidate checkouts
            score1 = list(process.extractWithoutOrder(desc, df[u'name_clean'].tolist(), scorer=fuzz.ratio))
            df[u'simple_ratio'] = pd.Series([s[1] for s in score1])
            score2 = list(process.extractWithoutOrder(desc, df[u'name_clean'].tolist(), scorer=fuzz.partial_ratio))
            df[u'partial_ratio'] = pd.Series([s[1] for s in score2])
            score3 = list(process.extractWithoutOrder(desc, df[u'name_clean'].tolist(), scorer=fuzz.token_sort_ratio))
            df[u'sort_ratio'] = pd.Series([s[1] for s in score3])
            score4 = list(process.extractWithoutOrder(desc, df[u'name_clean'].tolist(), scorer=fuzz.token_set_ratio))
            df[u'set_ratio'] = pd.Series([s[1] for s in score4])

            # Get max score
            df[u'max_ratio'] = df[[u'simple_ratio',u'partial_ratio',u'sort_ratio',u'set_ratio']].max(axis=1)

            # Get max of max
            df_approx = df[(df[u'max_ratio']>max_ratio_cutoff) & (df[u'simple_ratio']>simple_ratio_cutoff)]
            best_ratio = df_approx[u'max_ratio'].max()
            df_best = df_approx[df_approx[u'max_ratio'] == best_ratio]
            
            if len(df_best.index) == 0:
                #####################
                ### Situation 2.1 ### -- No exact match + No suitable candidates
                #####################
                chkoutid = None
                pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None, None, None, None

            else:
                df_best.drop_duplicates(subset=u'checkout_id', inplace=True)

                if len(df_best.index) == 1:
                    #####################
                    ### Situation 2.2 ### -- No exact match + 1 BEST found
                    #####################
                    chkoutid = df_best[u'checkout_id'].item()
                    pmatch_name = df_best[u'name_clean'].item()
                    pmatch_time = df_best[u'date_of_transfer'].item() # proof upload time
                    pmatch_max_score = df_best[u'max_ratio'].item()
                    pmatch_simple_score = df_best[u'simple_ratio'].item()
                    pmatch_partial_score = df_best[u'partial_ratio'].item()
                    pmatch_sort_score = df_best[u'sort_ratio'].item()
                    pmatch_set_score = df_best[u'set_ratio'].item()
                else:
                    #####################
                    ### Situation 2.2 ### -- No exact match + MANY BEST found
                    #####################
                    chkoutid = df_best[u'checkout_id'].tolist()
                    pmatch_name = df_best[u'name_clean'].tolist()
                    pmatch_time = df_best[u'date_of_transfer'].tolist() # proof upload time
                    pmatch_max_score = df_best[u'max_ratio'].tolist()
                    pmatch_simple_score = df_best[u'simple_ratio'].tolist()
                    pmatch_partial_score = df_best[u'partial_ratio'].tolist()
                    pmatch_sort_score = df_best[u'sort_ratio'].tolist()
                    pmatch_set_score = df_best[u'set_ratio'].tolist()

            candidate_chkout = df[u'checkout_id'].tolist() # check subset of amount & bank
            candidate_names = df[u'name_clean'].tolist()
            p0_scores = df[u'simple_ratio'].tolist()
            p1_scores = df[u'partial_ratio'].tolist()
            p2_scores = df[u'sort_ratio'].tolist()
            p3_scores = df[u'set_ratio'].tolist()

        elif len(df_exact.index) == 1:
            ###################
            ### Situation 3 ### -- 1 exact match
            ###################
            chkoutid = df_exact[u'checkout_id'].item() # use subset of amount, bank & name
            pmatch_name = df_exact[u'name_clean'].item()
            pmatch_time = df_exact[u'date_of_transfer'].item()
            pmatch_max_score = u'[EXACT MATCH: ARE YOU SURE]'
            pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None
            candidate_chkout, candidate_names, p0_scores, p1_scores, p2_scores, p3_scores = None, None, None, None, None, None
        else:
            ###################
            ### Situation 4 ### -- MANY exact match
            ###################
            chkoutid = u'Many names found'
            pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score = None, None, None, None, None, None, None

            candidate_chkout = df_exact[u'checkout_id'].tolist() # check subset of amount & bank
            candidate_names = df_exact[u'name_clean'].tolist()
            p0_scores, p1_scores, p2_scores, p3_scores = None, None, None, None

    return (chkoutid, pmatch_name, pmatch_time, pmatch_max_score, pmatch_simple_score, pmatch_partial_score, pmatch_sort_score, pmatch_set_score, candidate_chkout, candidate_names, p0_scores, p1_scores, p2_scores, p3_scores)


def do_statement_matching(stmt_id, chkout_id):

    # Try to click on statement button, if statement changes, raise exception
    try:
        stmt_btn = driver.find_element_by_xpath(f'//*[@name="match-stmt" and @match-statement="{str(stmt_id)}"]')
    except NoSuchElementException:
        print u'\nWarning: Statement has been changed'
        raise
    stmt_btn.click()

    # Find checkout in table & click radio button
    chkoutid = driver.find_element_by_xpath(u'//*[@x="checkoutid"]')
    chkoutid.send_keys(unicode(chkout_id))
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
        print u'\nWarning: Checkout has been changed'
        raise

    # Match
    match_btn = driver.find_element_by_xpath(u'//*[@class="ui teal button js-match w-control agent"]')
    match_btn.click()

    try:
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, u'.ui.small.modal.transition.visible.active')))
    except TimeoutException:
        print u'\t\t\t  -- Loading Page Timeout'
        pass

    approve_btn = driver.find_element_by_xpath(u'//*[@class="ui positive right labeled icon button js-matching-approve"]')
    approve_btn.click()

    try:
        wait.until(EC.alert_is_present())
    except TimeoutException:
        print u'\t\t\t  -- Alert Timeout'
        pass

    try:
        alert = driver.switch_to.alert
        alert.accept()
    except NoAlertPresentException:
        print u'Retrying alert acceptance...'
        try:
            wait.until(EC.alert_is_present())
        except TimeoutException:
            print u'\t\t\t  -- Alert Timeout #2'
            pass
        alert = driver.switch_to.alert
        alert.accept()

    try:
        # wait for approval module to disappear
        wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, u'.ui.small.modal.transition.visible.active')))
        # wait for checkout table to show
        wait.until(EC.visibility_of_element_located((By.ID, u'normal-checkout-table')))
        # Wait until loading screen disappears
        try:
            try_until_success('clear_chkout_loading')
        except TimeoutException: # if timeout after 6 attempts
            raise
    except TimeoutException:
        print u'\t\t\t  -- Alert Timeout'
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

    print f'\n*** Statement ID #{str(stmt_id)} matched successfully to Checkout ID #{str(chkout_id)} ***\n'


def manage_error(error):

    if error == 'server_error':
        # Catch Type 1
        try:
            button_server_error = driver.find_element_by_xpath('//div[@class="ui modal transition visible active scrolling"]').find_element_by_xpath('.//div[@class="ui ok button"]')
            button_server_error.click()
            print u'<< ErrorAvoided: Server Error #1 >>'
        except NoSuchElementException:
            # Catch Type 2
            try:
                button_server_error = driver.find_element_by_xpath('//div[@class="ui modal transition visible active"]').find_element_by_xpath('.//div[@class="ui ok button"]')
                button_server_error.click()
                print u'<< ErrorAvoided: Server Error #2 >>'
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
            if function == u'page_refresh':
                print u'[!][!] CriticalError: Refreshing page ineffective. Please restart BOT'
                driver.quit()
            elif function == u'load_checkout':
                try_until_success(u'page_refresh')
                raise TimeoutException
            elif function == u'clear_chkout_loading':
                try_until_success(u'page_refresh')
                raise TimeoutException
            elif function == u'clear_input':
                pass

        #########################
        ### Actual Attempt #1 ### Page Refresh
        #########################
        if function == u'page_refresh':
            try:
                driver.refresh()
                wait.until(EC.visibility_of_element_located((By.ID, u'article')))
                wait.until(EC.visibility_of_element_located((By.ID, u'normal-checkout-table')))
                break
            except TimeoutException:
                print u'\t\t\t  -- Page Refresh Timeout'
                pass

        #########################
        ### Actual Attempt #2 ### Wait for checkout load
        #########################
        elif function == u'load_checkout':
            try:
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, u'.choose-checkout.ui.segment.loading')))
                try:
                    wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, u'.choose-checkout.ui.segment.loading')))
                    break
                except TimeoutException:
                    print u'\t\t\t  -- Loading Page Timeout ... Retrying ...'
            except TimeoutException: # loading screen not visible after 10s. Assume load finished
                break

        #########################
        ### Actual Attempt #3 ### CLear sudden checkout loading
        #########################
        elif function == u'clear_chkout_loading':
            try:
                wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, u'.choose-checkout.ui.segment.loading')))
                break
            except TimeoutException:
                print u'\t\t\t  -- Loading Page Timeout ... Retrying ...'

        #########################
        ### Actual Attempt #4 ### Clear input price
        #########################
        elif function == u'clear_input':
            try:
                element.clear()
                break
            except InvalidElementStateException:
                try:
                    element = driver.find_element_by_xpath(f'//*[@x="{element_name}"]')
                    element.clear()
                    break
                except:
                    print u'??Retrying unknown error??'
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

input_name = raw_input(u'Enter BOT name (e.g. mustika1): ')

input_time = int(input(u'Enter BOT run time (in minutes): '))*60
interval_once = 30*60
interval_total = int(math.ceil(float(input_time)/float(interval_once)))

# Create new folder
now = datetime.now()
folder_name = 'results_live/'+input_name+'__'+now.strftime('%Y-%m-%d__%H-%M-%S')
os.mkdir(folder_name)

###########################
### [B] Setup Intervals ###
###########################

for run_count in xrange(interval_total):

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
    columns_statement = [u'statement_id', u'checkout_candidates', u'statement_amount', u'shopee_bank', u'statement_recieved_time', u'statement_uploaded_time', u'description', u'desc_clean', 
    u'[match] chkoutid',u'[match] name',u'[match] proof upload time',u'[match] max score',u'[match] simple score',u'[match] partial score',u'[match] sort score',u'[match] set score',
    u'candidate chkoutid',u'candidate names',u'candidate scores simple',u'candidate scores partial',u'candidate scores sort',u'candidate scores set',u'match_reason']

    df_statement_full = pd.DataFrame(columns=columns_statement)

    #####################
    ### [C] Begin BOT ###
    #####################
    while elapsed_time < run_time:

        # Step 1: Refresh and get statement
        #######################
        print f'\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n>>>>>>>>>>\t (1) Getting New Statement... (#{stmt_scraped}) \t<<<<<<<<<<\n'
        stmt_scraped += 1

        ### BackendErrorCatchment: #1.1 server error after new statement ###
        ####################################################################
        manage_error('server_error')

        while True:
            try:
                time.sleep(1)
                refresh_statement()
                try:
                    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.choose-statement.ui.once.blurring.dimmable.segment.loading')))
                    wait.until(EC.invisibility_of_element_located((By.CSS_SELECTOR, '.choose-statement.ui.once.blurring.dimmable.segment.loading')))
                except TimeoutException:
                    print u'\t\t\t  -- Loading Page Timeout'
                    pass

                df_statement = scrape_statement_df()
                break
            except AttributeError:
                print u'[!] ReleaseError: No statement currently available'
                print u'\t-- Waiting for new statements...'
                time.sleep(60)
            except:
                ### BackendErrorCatchment: #1.2 server error after new statement ###
                ####################################################################
                print u'\n++++++++\t Handling Server Error...\n'
                manage_error('server_error')

        stmt_amt = df_statement[u'statement_amount'][0]
        stmt_bank = df_statement[u'shopee_bank'][0]
        stmt_desc = df_statement[u'description'][0]
        print u'Statement Amount: ',stmt_amt
        print u'Statement Description: ',stmt_desc

        # Step 3a: Clean statement description
        #######################
        df_statement[u'desc_clean'] = df_statement[u'description'].replace(remove,u'',regex=True)
        df_statement[u'desc_clean'].replace(replace_empty,u'',inplace=True,regex=True)
        # Step 3b: Next statement if nothing left after cleaning
        #######################
        if len(df_statement[u'desc_clean'].item()) < name_match__min_len:
            df_statement[u'match_reason'] = u'desc name too short for matching'
            df_statement_full = df_statement_full.append(df_statement, ignore_index=True)
            print u'\n++++++++\t Unable to match this statement\n'
            continue

        # Step 4: Retrieve relevant checkouts (based on statement amount and bank)
        #######################
        df_checkout = find_relevant_checkouts(unicode(stmt_amt), stmt_bank)

        if refresh:
            refresh = False
            print u'\n++++++++\t Refreshing page...\n'
            try_until_success('page_refresh')

            # Wait until loading screen disappears
            try:
                try_until_success('load_checkout')
            except TimeoutException: # if timeout after 6 attempts
                print u'\n[!] TableLoadingError: Refreshing and continuing to next statement...\n'
                continue

        # Step 5: Approximate matching
        #######################
        results = process_name_matching(df_checkout, df_statement[u'desc_clean'].item())

        df_statement[[
        u'[match] chkoutid',u'[match] name',u'[match] proof upload time',u'[match] max score',u'[match] simple score',u'[match] partial score',u'[match] sort score',u'[match] set score',
        u'candidate chkoutid',u'candidate names',u'candidate scores simple',u'candidate scores partial',u'candidate scores sort',u'candidate scores set'
        ]] = pd.DataFrame([list(results)])

        if isinstance(df_statement[u'[match] chkoutid'].item(), int):
            print u'\n\tCongrats! Match Found, attempting matching now...'
            stmt_id = df_statement[u'statement_id'].item()
            chkout_id = df_statement[u'[match] chkoutid'].item()

            ### BackendErrorCatchment: #3 BE refreshes statement unexpectedly when matching ###
            ###################################################################################
            try:
                driver.find_element_by_xpath('//div[@class="ui inverted dimmer active"]')
                warning_msg = u'Bot unable to match due to BE error, please match manually'
                df_statement[u'match_reason'] = u'MatchError: ' + warning_msg
                print u'[!] MatchError: ',warning_msg,u'\n'
                continue
            except NoSuchElementException:
                try:
                    do_statement_matching(stmt_id, chkout_id)
                except NoSuchElementException:
                    warning_msg = u'Checkout has been cleared by other agents/bot'
                    df_statement[u'match_reason'] = u'MatchError: ' + warning_msg
                    print u'[!] MatchError: ',warning_msg,u'\n'
                except TimeoutException:
                    print u'\n[!] TableLoadingError: Refreshing and continuing to next statement...\n'
        else:
            print u'\n\tNo match found :( continuing to next statement...'

        df_statement_full = df_statement_full.append(df_statement, ignore_index=True)

        elapsed_time = time.time() - init_time
        elapsed_time_print = elapsed_time+(interval_once*run_count)
        print u'Run Time:\t',input_time,u's'
        print u'Elapsed Time:\t',int(round(elapsed_time_print,0)),u's'

    ####################################
    ### To do after every while loop ###
    ####################################

    # Save each run attempt
    df_statement_full = df_statement_full[[u'statement_id', u'checkout_candidates', u'statement_amount', u'shopee_bank', u'statement_recieved_time', u'statement_uploaded_time', u'description', u'desc_clean', 
    u'[match] chkoutid',u'[match] name',u'[match] proof upload time',u'[match] max score',u'[match] simple score',u'[match] partial score',u'[match] sort score',u'[match] set score',
    u'candidate chkoutid',u'candidate names',u'candidate scores simple',u'candidate scores partial',u'candidate scores sort',u'candidate scores set',u'match_reason']]
    df_statement_full.drop_duplicates(subset=u'statement_id', inplace=True)

    df_statement_full.to_csv(f'{folder_name}/match_log_{run_count}.csv')

    # Refresh at end of every interval 
    try_until_success('page_refresh')

###########################
### End BOT save script ###
###########################

elapsed_time = time.time() - init_time
elapsed_time_print = elapsed_time+(interval_once*run_count)
print u'\nMatch Completed'
print u'\nTime Elapsed: ',int(round(elapsed_time_print,0)),u's'

driver.quit()
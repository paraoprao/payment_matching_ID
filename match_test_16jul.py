# setup
#######################################################################################################################
import time
import pandas as pd
import numpy as np
from functools import partial

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

#######################################################################################################################

def categorise_banks(df, bank_column):
    conditions = [
        bank_column.str.contains('bri', case=False, na=False),
        bank_column.str.contains('bca', case=False, na=False),
        bank_column.str.contains('bni', case=False, na=False),
        bank_column.str.contains('mandiri', case=False, na=False),
        bank_column.str.contains('cimb', case=False, na=False)
    ]
    choices = ['BRI', 'BCA', 'BNI', 'Mandiri', 'CIMB']
    df['[A] script_bank_cat'] = np.select(conditions, choices, default='unknown / other bank')
    print('done')
    return df

# Version 1: Amount + Bank
def find_potential_checkouts(df_chkout, stmt_amt, stmt_bank):
    chkoutid = None

    # Step 1: Filter potential checkouts by proof amount & bank
    potential_chkouts = df_chkout[(df_chkout['proof_amount']==stmt_amt) & (df_chkout['[A] script_bank_cat']==stmt_bank)]
    # check num of potential checkouts
    if len(potential_chkouts) == 0:
        chkoutid = 'None found'
    elif len(potential_chkouts) == 1:
        chkoutid = potential_chkouts['checkoutid'].item()
    else: # >2 checkouts
        chkoutid = 'Many found'
    return chkoutid

# Version 2: Amount + Bank + Name match (exact & approximate)
def find_potential_checkouts_v2(df_chkout, stmt_amt, stmt_bank, stmt_desc):
    # Definitions:
    ################
    # _ab : subset of amt & bank
    # _abn: subset of amt, bank & exact name
    chkoutid, pmax_name, pmax_score, chkout_candidates, p0_names, p0_scores, p1_names, p1_scores, p2_names, p2_scores = None, None, None, None, None, None, None, None, None, None
    
    # Step 1: 
    # Filter potential checkouts by proof amount & bank
    ################
    potential_chkouts_ab = df_chkout[(df_chkout['proof_amount']==stmt_amt) & (df_chkout['[A] script_bank_cat']==stmt_bank)]
    # 
    # Step 2: Further filter potential checkouts if proof cust name is in description
    if len(potential_chkouts_ab.index) == 0:
        # Situation 1: No Amt Bank match
        chkoutid = 'Amount / Bank wrong'
        pmax_name, pmax_score, chkout_candidates, p0_names, p0_scores, p1_names, p1_scores, p2_names, p2_scores = None, None, None, None, None, None, None, None, None
    else:
        # Situation 2: Amt & Bank match, proceed to confirm using name (proof name ~ stmt desc)
        potential_chkouts_ab['[B] proof_cust_name_clean'] = potential_chkouts_ab['[B] proof_cust_name_clean'].fillna('').str.lower().str.replace('\"','') # (1) fills na with un-matchable name (2) cleans it
        potential_chkouts_abn = potential_chkouts_ab[potential_chkouts_ab['[B] proof_cust_name_clean'].map(lambda x: x in stmt_desc)] 
        # 
        if len(potential_chkouts_abn.index) == 1:
            # Situation 2a: Single match using Amt, Bank & exact Name
            chkoutid = potential_chkouts_abn['checkoutid'].item() # use subset of amount, bank & name
            pmax_name, pmax_score, chkout_candidates, p0_names, p0_scores, p1_names, p1_scores, p2_names, p2_scores = None, None, None, None, None, None, None, None, None
        elif len(potential_chkouts_abn.index) == 0:
########################
### WORK IN PROGRESS ###
########################
            # Situation 2b: (amt & bank --> some candidates, no exact match with name --> 2 options: possibility of approx match / no match at all)
            chkout_candidates = potential_chkouts_ab['checkoutid'].tolist() # check subset of amount & bank
            pmax = process.extractOne(stmt_desc, potential_chkouts_ab['[B] proof_cust_name_clean'].tolist(), scorer=fuzz.token_set_ratio, score_cutoff=50)
            if pmax is None:
                pmax_name, pmax_score = None, None
            else:
                pmax_name = pmax[0]
                pmax_score = pmax[1]
                try:
                    chkoutid = potential_chkouts_ab[potential_chkouts_ab['[B] proof_cust_name_clean'] == str(pmax_name)]['checkoutid'].item()
                except ValueError:
                    chkoutid = None

            p0 = list(process.extractWithoutOrder(stmt_desc, potential_chkouts_ab['[B] proof_cust_name_clean'].tolist()))
            p0_names = [x[0] for x in p0]
            p0_scores = [x[1] for x in p0]
            p1 = list(process.extractWithoutOrder(stmt_desc, potential_chkouts_ab['[B] proof_cust_name_clean'].tolist(), scorer=fuzz.token_sort_ratio)) # Note: this is using token_sort_ratio
            p1_names = [x[0] for x in p1]
            p1_scores = [x[1] for x in p1]
            p2 = list(process.extractWithoutOrder(stmt_desc, potential_chkouts_ab['[B] proof_cust_name_clean'].tolist(), scorer=fuzz.token_set_ratio)) # Note: this is using token_set_ratio
            p2_names = [x[0] for x in p2]
            p2_scores = [x[1] for x in p2]
########################
### WORK IN PROGRESS ###
########################
        else:
            chkoutid = 'Many names found'
            pmax_name, pmax_score, chkout_candidates, p0_names, p0_scores, p1_names, p1_scores, p2_names, p2_scores = None, None, None, None, None, None, None, None, None
    #
    return (chkoutid, pmax_name, pmax_score, chkout_candidates, p0_names, p0_scores, p1_names, p1_scores, p2_names, p2_scores)

### Layer 0:
### -- Clean historical statements
##################################################################

print("Reading historical files ...")

folder_name = '15jun_test'
# path_stmt = f'{folder_name}\\stmt_with_ans.csv'
path_chkout = f'{folder_name}\\chkout_withinrange.csv'

# df_stmt = pd.read_csv(path_stmt, encoding='ISO-8859-1', error_bad_lines=False),
df_chkout = pd.read_csv(path_chkout, encoding='ISO-8859-1', dtype={'proof_status': object})

# # Step 1: Only status 1
# df_stmt = df_stmt[df_stmt['chkout_status'] == 1]
# # Step 2: Non auto match
# df_stmt = df_stmt[~(df_stmt['chkout_match_type']=='auto_match')]
# # Step 3: If more than 1 count, take latest statement
# idx = df_stmt.groupby(['stmt_id'], sort=False)['proof_upload_time'].transform(max) == df_stmt['proof_upload_time']
# df_stmt = df_stmt[idx]
# print(df_stmt.shape)
# df_stmt.to_csv(f'{folder_name}\\stmt_with_ans_clean.csv')

### Layer 1:
### -- Match statement amount & bank to proof amount & bank -- sufficient because of unique price
##################################################################

# df_stmt = pd.read_csv(f'{folder_name}\\stmt_with_ans_clean.csv')

# print('stmt shape: ', df_stmt.shape)
# print('chkout shape: ', df_chkout.shape)

# print('stmt min time: ', min(df_stmt['stmt_received_time']))
# print('stmt max time: ', max(df_stmt['stmt_received_time']))

# print('chkout min time: ', min(df_chkout['checkout_time']))
# print('chkout max time: ', max(df_chkout['checkout_time']))

# # 1a: Organise stmt & chkout banks
# df_stmt['stmt_bank'].replace(to_replace='niaga', value='', regex=True, inplace=True)
# df_chkout['proof_trf_to_bank'].replace(to_replace='niaga', value='', regex=True, inplace=True)

# df_stmt = categorise_banks(df_stmt, df_stmt['stmt_bank'])
# df_chkout = categorise_banks(df_chkout, df_chkout['proof_trf_to_bank'])

# # 1b: Find potential checkout ids
# # df_stmt = df_stmt.reset_index(drop=True)
# # df_chkout = df_chkout.reset_index(drop=True)
# init_time = time.time()
# _ =  map(partial(find_potential_checkouts, df_chkout), df_stmt['stmt_amount'], df_stmt['[A] script_bank_cat'])
# df_stmt['*script_matched_chkoutid'] = pd.Series(list(_))
# print('Time elapsed: ', time.time() - init_time)
# df_stmt.to_csv('results.csv')

### Layer 1.1 - NEW:
### -- Match statement amount & bank & name/desc to proof amount & bank & name
##################################################################

df_stmt = pd.read_csv(f'{folder_name}\\stmt_FINAL.csv', dtype={'proof_reason': object})

print('stmt shape: ', df_stmt.shape)
print('chkout shape: ', df_chkout.shape)

print('stmt min time: ', min(df_stmt['stmt_received_time']))
print('stmt max time: ', max(df_stmt['stmt_received_time']))

print('chkout min time: ', min(df_chkout['checkout_time']))
print('chkout max time: ', max(df_chkout['checkout_time']))

# 1a: Organise stmt & chkout banks
df_stmt['stmt_bank'].replace(to_replace='niaga', value='', regex=True, inplace=True)
df_chkout['proof_trf_to_bank'].replace(to_replace='niaga', value='', regex=True, inplace=True)

df_stmt = categorise_banks(df_stmt, df_stmt['stmt_bank'])
df_chkout = categorise_banks(df_chkout, df_chkout['proof_trf_to_bank'])

# 1b: Clean desc and names
remove = r'(branch(.*)(transfer|sdr(i|.)|bpk|echannel))|(kartu(.*)[0-9]{4,4})|((trsf e-banking cr|switching cr)( tanggal)?)|(\b(((to( |.)pt( |.))?|(pt( |.))?)(airpay|aipay) inter[a-z]*)\b)|(atm(.*)trf fr )|(mcm inhousetrf cs-cs)|(cimb(.*)trf fr)|(payfazz?( tekh?nologi nusantara)?)|(muhamm?ad|\bdr\b|bank|cimb niaga|ibnk|shopee|transfer|toko|edc|atm|m-bk trf ca\/sa|dari|sms|original|clear|wsid:|\")|([0-9][0-9]\/[0-9][0-9])|(\b([0-9]*)\b)'
df_stmt['[B] stmt_desc_clean'] = df_stmt['stmt_desc'].str.replace(remove,'',case=False,regex=True)
df_chkout['[B] proof_cust_name_clean'] = df_chkout['proof_cust_name'].str.replace(remove,'',case=False,regex=True)

# 1c: Find potential checkout ids
init_time = time.time()
results = map(partial(find_potential_checkouts_v2, df_chkout), df_stmt['stmt_amount'], df_stmt['[A] script_bank_cat'], df_stmt['[B] stmt_desc_clean'])
r = list(results)

r_chkout = [x[0] for x in r]
r_maxname = [x[1] for x in r]
r_maxscore = [x[2] for x in r]
r_chkout_candidate = [x[3] for x in r]
r_namelist_partial = [x[4] for x in r]
r_scorelist_partial = [x[5] for x in r]
r_namelist_sort = [x[6] for x in r]
r_scorelist_sort = [x[7] for x in r]
r_namelist_set = [x[8] for x in r]
r_scorelist_set = [x[9] for x in r]

df_stmt['[match] chkoutid'] = pd.Series(r_chkout)
df_stmt['[match] name'] = pd.Series(r_maxname)
df_stmt['[match] score'] = pd.Series(r_maxscore)
df_stmt['candidate chkoutid'] = pd.Series(r_chkout_candidate)
df_stmt['candidate names partial'] = pd.Series(r_namelist_partial)
df_stmt['candidate scores partial'] = pd.Series(r_scorelist_partial)
df_stmt['candidate names sort'] = pd.Series(r_namelist_sort)
df_stmt['candidate scores sort'] = pd.Series(r_scorelist_sort)
df_stmt['candidate names set'] = pd.Series(r_namelist_set)
df_stmt['candidate scores set'] = pd.Series(r_scorelist_set)

# df_stmt['token_sort_names'] = pd.Series(list(p1_names))
# df_stmt['token_sort_scores'] = pd.Series(list(p1_scores))
# df_stmt['token_set_names'] = pd.Series(list(p2_names))
# df_stmt['token_set_scores'] = pd.Series(list(p2_scores))

# df_stmt['extra1'] = pd.Series(list(extra1))
# df_stmt['extra2'] = pd.Series(list(extra2))

print('Time elapsed: ', time.time() - init_time)
df_stmt.to_csv('results_v3.csv')



### Explanation of situations
##################################################################

# situation 1 (usually bca, bni, cimb)
# step1: match amt
# step2: match bank (to_acct to stmt_bank)
# step3: match name (proof name to stmt_name or stmt_desc)

# sitaution 2 (usually bri, mandiri)
# step1: match amt
# step2: match bank 
# step3: match from_acct (from_acct to stmt_desc)

# sitaution 3 (usually no proof (when proof_amt = ""))
# step1: match amt

### Test code
##################################################################
# break
# df_stmt.iloc[0:2,:]
#results = map(partial(find_potential_checkouts_v2, df_chkout), df_stmt['stmt_amount'][[4941,7304]], df_stmt['[A] script_bank_cat'][[4941,7304]], df_stmt['stmt_desc'][[4941,7304]])
#r = list(results)
#

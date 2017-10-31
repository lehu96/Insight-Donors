'''
Main file of the data processing module. Task is broken down into separate individual functions to ease 
division of tasks.

Python version used: 3.6.3

Packages needed: numpy, pandas


@author Laney Huang
'''

import numpy as np
import pandas as pd
import sys


HEADER_NAMES = np.array(['CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM',
                        'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE',
                        'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT',
                        'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT,SUB_ID'])

BY_ZIP = pd.DataFrame(columns=['CMTE_ID', 'ZIP_CODE', 'RUN_MED', 'NUM_ZIP_TRANS', 'TOTAL_ZIP_AMT'])
BY_ZIP['RUN_MED'] = BY_ZIP['RUN_MED'].astype(int)
BY_ZIP['NUM_ZIP_TRANS'] = BY_ZIP['NUM_ZIP_TRANS'].astype(int)
BY_ZIP['TOTAL_ZIP_AMT'] = BY_ZIP['TOTAL_ZIP_AMT'].astype(int)
SEEN_ZIPS = {} # zip: [count, sum]

BY_DATE = pd.DataFrame(columns=['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'NUM_TRANS', 'TOTAL_AMT'])
BY_DATE['MEDIAN'] = BY_DATE['MEDIAN'].astype(int)
BY_DATE['NUM_TRANS'] = BY_DATE['NUM_TRANS'].astype(int)
BY_DATE['TOTAL_AMT'] = BY_DATE['TOTAL_AMT'].astype(int)



def preprocessing(pre_df):
    '''
    Takes an input dataframe in a 'raw' form.
    Drops rows not as individual contributions, with empty recipient IDs, and has a valid transaction amt. 
    Truncates the ZIP_CODE column to the first 5 digits.
    Transforms the date column to datetime types. 
    '''
    temp_df = pd.DataFrame(pre_df.loc[(pre_df['OTHER_ID'] == '') & (pre_df['CMTE_ID'] != '') & 
        (np.invert(pre_df['TRANSACTION_AMT'].isnull())), :])
    temp_df.loc[:, 'ZIP_CODE'] = temp_df.loc[:, 'ZIP_CODE'].astype(str).str[:5]
    temp_df.loc[:, 'TRANSACTION_DT'] = pd.to_datetime(temp_df['TRANSACTION_DT'], format="%m%d%Y", errors='coerce')
    return temp_df


def process_by_zip(input_df):
    '''
    Takes a preprocessed dataframe as input.
    Drops rows with invaid zip codes.
    Iterates through the rows of the dataframe, appending the correct values to a list of dicts, list_dicts.
    Also updates global dictionary SEEN_ZIPS with each iteration, keeping track of visited zips, as well as their current count and total contribution amounts.
    Concatenates this chunked dataframe to global dataframe BY_ZIP
    '''
    global BY_ZIP
    valid_zip_df = pd.DataFrame(input_df.loc[input_df['ZIP_CODE'].str.len() == 5, :])
    list_dicts = []
    for row in valid_zip_df.itertuples():
        if row.ZIP_CODE not in SEEN_ZIPS:
            SEEN_ZIPS[row.ZIP_CODE] = [1, row.TRANSACTION_AMT]
        else:
            SEEN_ZIPS[row.ZIP_CODE] = [SEEN_ZIPS[row.ZIP_CODE][0]+1, SEEN_ZIPS[row.ZIP_CODE][1]+row.TRANSACTION_AMT]
        list_dicts.append({'CMTE_ID': row.CMTE_ID, 
                             'ZIP_CODE': row.ZIP_CODE,
                             'RUN_MED': row.TRANSACTION_AMT,
                             'NUM_ZIP_TRANS': SEEN_ZIPS[row.ZIP_CODE][0],
                             'TOTAL_ZIP_AMT': SEEN_ZIPS[row.ZIP_CODE][1]})
    BY_ZIP = pd.concat([BY_ZIP, pd.DataFrame(list_dicts)])

    
def process_by_date(input_df):
    '''
    Takes a preprocessed dataframe.
    Drops rows with invalid dates.
    Iterates through rows of the dataframe, appending the correct values for each column to a list of dicts.
    Concatenates the list of 'rows' to global dataframe BY_DATE
    '''
    global BY_DATE
    valid_date_df = pd.DataFrame(input_df.loc[np.invert(input_df['TRANSACTION_AMT'].isnull()), :])
    list_dicts = []
    for row in valid_date_df.itertuples():
        list_dicts.append({'CMTE_ID': row.CMTE_ID,
                           'TRANSACTION_DT': row.TRANSACTION_DT,
                           'MEDIAN': row.TRANSACTION_AMT,
                           'NUM_TRANS': 1,
                           'TOTAL_AMT': row.TRANSACTION_AMT
                           })
    BY_DATE = pd.concat([BY_DATE, pd.DataFrame(list_dicts)])


def running_med():
    '''
    Directly modifies global dataframe BY_ZIP

    Iterates through the dictionary of SEEN_ZIPS, subsetting by each zip and calculating that subset's running median, while keeping track of the numerical index of the value. Appends this pair to a list of dictionaries, later to be converted to another pandas dataframe.
    
    Merges the original BY_ZIP dataframe by this new dataframe consisting of running medians, keeping only the columns desired. 
    '''
    global BY_ZIP
    global SEEN_ZIPS
    BY_ZIP.reset_index(inplace=True, drop=True)
    BY_ZIP['Index'] = BY_ZIP.index
    BY_ZIP.set_index('ZIP_CODE', inplace=True)
    list_zips = []
    for zips in SEEN_ZIPS:
        run_med = pd.Series(BY_ZIP.loc[zips, 'RUN_MED']).expanding().median().round().astype(int)
        indices = pd.Series(BY_ZIP.loc[zips, 'Index'])
        for index, med in zip(indices,run_med):
            list_zips.append({'RUN_MED': med, 
                              'Indices': index})
    temp_df = pd.DataFrame(list_zips)
    temp_df.set_index('Indices', inplace=True)
    BY_ZIP.reset_index(inplace=True)
    BY_ZIP = BY_ZIP.loc[:, ['ZIP_CODE', 'CMTE_ID', 'NUM_ZIP_TRANS', 'TOTAL_ZIP_AMT']].merge(
        temp_df, how='outer', left_index=True, right_index=True, sort=True)


def date_recip_order():
    '''
    Directly modifies global dataframe BY_DATE

    Utilizes pandas' groupby method to group by both the recipient and the date, aggregating the separate columns by the desired, individual functions. 
    '''
    global BY_DATE
    BY_DATE = BY_DATE.groupby(['CMTE_ID', 'TRANSACTION_DT']).agg(
        {'MEDIAN': np.median, 'NUM_TRANS': 'count', 'TOTAL_AMT': 'sum'})
    BY_DATE['MEDIAN'] = BY_DATE['MEDIAN'].round().astype(int)
    BY_DATE.reset_index(inplace=True)

def post_process_zip():
    '''
    Orders the columns in BY_ZIP to the order needed for writing.
    '''
    global BY_ZIP
    BY_ZIP = BY_ZIP[['CMTE_ID', 'ZIP_CODE', 'RUN_MED', 'NUM_ZIP_TRANS', 'TOTAL_ZIP_AMT']]

def post_process_date():
    '''
    Orders the columns in BY_DATE to the order needed for writing. 
    Converts the format of the dates back to string, output format.
    Sorts the dataframe by recipient ID and the transaction date.
    '''
    global BY_DATE
    BY_DATE = BY_DATE[['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'NUM_TRANS', 'TOTAL_AMT']]
    BY_DATE.loc[:,'TRANSACTION_DT'] = BY_DATE.loc[:,'TRANSACTION_DT'].dt.strftime('%m%d%Y')
    BY_DATE.sort_values(['CMTE_ID', 'TRANSACTION_DT'], inplace=True)





if __name__ == '__main__':

    reader = pd.read_csv(sys.argv[1], sep='|', header=None, names=HEADER_NAMES,
                         usecols=['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID'],
                         index_col=False, na_filter = False, chunksize=100000, dtype={'ZIP_CODE': object, 'TRANSACTION_DT': object})

    for chunk in reader:
        preprocessed_df = preprocessing(chunk)
        process_by_zip(preprocessed_df)
        process_by_date(preprocessed_df)

    running_med()
    date_recip_order()

    post_process_zip()
    post_process_date()

    BY_ZIP.to_csv(sys.argv[2], sep='|', header=False, index=False)

    BY_DATE.to_csv(sys.argv[3], sep='|', header=False, index=False)





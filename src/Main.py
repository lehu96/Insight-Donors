'''
Main file of the data processing module.
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
SEEN_ZIPS = set() # Set of seen zips to later iterate through

BY_DATE = pd.DataFrame(columns=['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'NUM_TRANS', 'TOTAL_AMT'])
BY_DATE['MEDIAN'] = BY_DATE['MEDIAN'].astype(int)
BY_DATE['NUM_TRANS'] = BY_DATE['NUM_TRANS'].astype(int)
BY_DATE['TOTAL_AMT'] = BY_DATE['TOTAL_AMT'].astype(int)


def preprocessing(pre_df):
    temp_df = pd.DataFrame(pre_df.loc[(pre_df['OTHER_ID'] == '') & (pre_df['CMTE_ID'] != '') & (
    np.invert(pre_df['TRANSACTION_AMT'].isnull())), :])
    temp_df.loc[:, 'ZIP_CODE'] = temp_df.loc[:, 'ZIP_CODE'].astype(str).str[:5]
    temp_df.loc[:, 'TRANSACTION_DT'] = pd.to_datetime(temp_df['TRANSACTION_DT'], format="%m%d%Y", errors='coerce')
    return temp_df


def process_by_zip(input_df):
    global BY_ZIP
    valid_zip_df = pd.DataFrame(input_df.loc[input_df['ZIP_CODE'].str.len() == 5, :])
    for row in valid_zip_df.itertuples():
        if row.ZIP_CODE not in SEEN_ZIPS:
            SEEN_ZIPS.add(row.ZIP_CODE)
        BY_ZIP = BY_ZIP.append(pd.DataFrame({'CMTE_ID': [row.CMTE_ID],
                                             'ZIP_CODE': [row.ZIP_CODE],
                                             'RUN_MED': [row.TRANSACTION_AMT],
                                             'NUM_ZIP_TRANS': [1],
                                             'TOTAL_ZIP_AMT': [row.TRANSACTION_AMT]}))


def process_by_date(input_df):
    global BY_DATE
    valid_date_df = pd.DataFrame(input_df.loc[np.invert(input_df['TRANSACTION_AMT'].isnull()), :])
    for row in valid_date_df.itertuples():
        BY_DATE = BY_DATE.append(pd.DataFrame({'CMTE_ID': [row.CMTE_ID],
                                               'TRANSACTION_DT': row.TRANSACTION_DT,
                                               'MEDIAN': row.TRANSACTION_AMT,
                                               'NUM_TRANS': 1,
                                               'TOTAL_AMT': row.TRANSACTION_AMT
                                               }))

def running_med():
    global BY_ZIP
    global SEEN_ZIPS
    BY_ZIP.reset_index(inplace=True, drop=True)
    BY_ZIP['Index'] = BY_ZIP.index
    BY_ZIP.set_index('ZIP_CODE', inplace=True)
    for zips in SEEN_ZIPS:
        cum_num = pd.Series(BY_ZIP.loc[zips, 'NUM_ZIP_TRANS']).expanding().sum().astype(int)
        cum_amt = pd.Series(BY_ZIP.loc[zips, 'TOTAL_ZIP_AMT']).expanding().sum().astype(int)
        run_med = pd.Series(BY_ZIP.loc[zips, 'RUN_MED']).expanding().median().round().astype(int)
        ind = pd.Series(BY_ZIP.loc[zips, 'Index'])
        for index, num, amt, med in zip(ind,cum_num,cum_amt,run_med):
            BY_ZIP.loc[(BY_ZIP['Index'] == index), 'NUM_ZIP_TRANS'] = num
            BY_ZIP.loc[(BY_ZIP['Index'] == index), 'TOTAL_ZIP_AMT'] = amt
            BY_ZIP.loc[(BY_ZIP['Index'] == index), 'RUN_MED'] = med
    BY_ZIP.reset_index(inplace=True)

def date_recip_order():
    global BY_DATE
    BY_DATE = BY_DATE.groupby(['CMTE_ID', 'TRANSACTION_DT']).agg(
        {'MEDIAN': np.median, 'NUM_TRANS': 'count', 'TOTAL_AMT': 'sum'})
    BY_DATE['MEDIAN'] = BY_DATE['MEDIAN'].round().astype(int)

    BY_DATE.reset_index(inplace=True)

def post_process_zip():
    global BY_ZIP
    BY_ZIP = BY_ZIP[['CMTE_ID', 'ZIP_CODE', 'RUN_MED', 'NUM_ZIP_TRANS', 'TOTAL_ZIP_AMT']]

def post_process_date():
    global BY_DATE
    BY_DATE = BY_DATE[['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'NUM_TRANS', 'TOTAL_AMT']]
    BY_DATE.sort_values(['CMTE_ID', 'TRANSACTION_DT'], inplace=True)




if __name__ == '__main__':
    reader = pd.read_csv(sys.argv[1], sep='|', header=None, names=HEADER_NAMES,
                         usecols=['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID'],
                         index_col=False, na_filter = False, chunksize=50)

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





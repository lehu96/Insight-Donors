'''
Set of a few unit tests run to check functionality of the helper functions defined in Main. Input/output expected
are taken from the test files given by Insight in their test suite. Additional Interactve_Test.ipynb was used
personally to debug a few issues, as well as observe functionality of Main.py in a more informative interface.
'''

import main_processing
import unittest
import numpy as np
import pandas as pd


class TestHelperMethods(unittest.TestCase):


    def process(self):
        test_df = pd.read_csv('../input/itcont_short.txt', sep='|', header=None, names=Main.HEADER_NAMES,
                         usecols=['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID'],
                         index_col=False, na_filter = False, dtype={'ZIP_CODE': object, 'TRANSACTION_DT': object})
        test_df = Main.preprocessing(test_df)
        Main.process_by_zip(test_df)
        Main.process_by_date(test_df)
        return test_df

    def process_step2(self):
        test_df = self.process()
        Main.running_med()
        Main.date_recip_order()
        return test_df

    def post_process(self):
        self.process_step2()
        Main.post_process_zip()
        Main.post_process_date()

    def test_preprocess(self):
        test_df = self.process()
        self.assertEqual(test_df.shape[0], 6)


    def test_process_by_zip(self):
        self.process()
        self.assertTrue(all(Main.BY_ZIP.loc[:,'ZIP_CODE'].str.len() == 5))
        self.assertEqual(len(Main.SEEN_ZIPS), 4)
        self.assertCountEqual(set(Main.SEEN_ZIPS.keys()), set(Main.BY_ZIP.loc[:,'ZIP_CODE'].values))


    def test_process_by_date(self):
        self.process()
        self.assertTrue(all(Main.BY_DATE.loc[:, 'TRANSACTION_DT']))


    def test_running_med(self):
        test_df = self.process_step2()
        self.assertNotEqual(sum(Main.BY_ZIP['RUN_MED']), 
            sum(test_df['TRANSACTION_AMT']))



    def test_date_recip_order(self):
        self.process_step2()
        self.assertEqual(Main.BY_DATE.shape[0], 2)
        self.assertEqual(len(Main.BY_DATE['CMTE_ID']), len(set(Main.BY_DATE['CMTE_ID'])))
        self.assertEqual(len(Main.BY_DATE['TRANSACTION_DT']), len(set(Main.BY_DATE['TRANSACTION_DT'])))

        
    def test_post_process_zip(self):
        self.post_process()
        self.assertListEqual(list(Main.BY_ZIP), 
            ['CMTE_ID', 'ZIP_CODE', 'RUN_MED', 'NUM_ZIP_TRANS', 'TOTAL_ZIP_AMT'])


    def test_post_process_date(self):
        self.post_process()
        self.assertListEqual(list(Main.BY_DATE), 
            ['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'NUM_TRANS', 'TOTAL_AMT'])





if __name__ == '__main__':

    

    unittest.main()
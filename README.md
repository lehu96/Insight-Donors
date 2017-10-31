# Description

This program was written all to one file, main_processing.py. 


The main function of main_processing.py calls a series of helper functions to process the incoming data and manipulate it into the final form we want.

Since the input file may be large, the program splits the input into chunks to be read and processed one at a time. It first preprocesses, getting only the columns we want from the raw data in the desired format. Then, separate functions are called for each preprocessed input to handle individual processing for medianvals by zip or date. (Extracts only the valid rows for each).

This results in two global dataframes, one for zips and one for dates. The next pair of called functions calculates the running median for the medianvals by zip and the aggregated values for the medianvals by date, making the appropriate changes to their respective global dataframes. 

Finally, each dataframe is post_processed into the output format desired. This entails making sure the column order is accurate, and in the case of the medianvals by dates, ensures that the ordering is by recipient and then date. Then both files are written to the ouput directory.






Python version: 3.6.3

Python libraries required to run this program(pip installed or conda installed):
- numpy (version 1.13.3)
- pandas (version 0.21.0)
- dependencies co-installed: 
  - python-dateutil (2.6.1)
  - pytz (2017.3)
  - six (1.11.0)


@Author: Lei(Laney) Huang

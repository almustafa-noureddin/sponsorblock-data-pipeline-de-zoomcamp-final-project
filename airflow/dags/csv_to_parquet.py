import os
import glob
import logging

import pyarrow.csv as pv
import pyarrow.parquet as pq

def csv_to_parquet_callable():
    logging.info('converting csv files to parquet ...')
    for root, dirs, files in os.walk(os.getcwd()):
        # find all file in the path "filepath" then filters them by the expression '*.csv'
        files = glob.glob(os.path.join(root,'*.csv'))
        for file in files :
            logging.info(f'converting {file[len(root)+1:]} to parquet')
            try:
                table = pv.read_csv(file)
            except Exception as e:
                logging.info(f'Error: could not read the file {file[len(root)+1:]}')
                raise e
            try:
                pq.write_table(table, file.replace('.csv', '.parquet'))
            except Exception as e:
                logging.info(f'Error: could not convert the file {file[len(root)+1:]} to parquet')
                raise e
            logging.info(f'converting {file[len(root)+1:]} to parquet completed')

def main():
    ''' Function to convert csv files to parquet .
    '''
    FILEPATH=os.getcwd()
    csv_to_parquet_callable(FILEPATH)

if __name__ == "__main__":
    main()
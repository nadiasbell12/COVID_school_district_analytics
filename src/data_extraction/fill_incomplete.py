# local imports
from scrape_publicholidays import get_dates

# package imports
from selenium import webdriver
import pandas as pd

# python standard library
import os
import time

# This program does the same thing as scrape_publicholidays.py
# however, it is only meant to fill in missing data (where scrape_publicholidays.py
# failed to extract the data from the web)

def main():
    driver = webdriver.Chrome()
    driver.implicitly_wait(10)

    # read in files from /migrated and fill out any missing rows
    for state_file in os.listdir(f'output/migrated'):
        if state_file not in os.listdir(f'output/'):
            print(state_file)
            try:
                df = pd.read_excel(f'output/migrated/{state_file}', index_col=0)
                idx_missing = df.startDate.isnull()
                print(idx_missing.sum())
                if idx_missing.sum() > 0:
                    df_missing = df.loc[idx_missing, :]
                    # district_url = df.loc[idx_missing, 'url'].values[0]
                    for url in df_missing.url:
                        start_date, end_date = get_dates(driver, url)
                        df.loc[df.url == url, 'startDate'] = start_date
                        df.loc[df.url == url, 'endDate'] = end_date
                        time.sleep(240)
            except:
                pass

            # output in /output folder
            df.to_excel(f'output/{state_file}')

    driver.close()


if __name__ == '__main__':
    # execute only if run as a script
    main()

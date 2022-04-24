import time
import os
import logging

from bs4 import BeautifulSoup
import requests
from urllib.request import urlretrieve, quote
from urllib.parse import urljoin

def webscraper_callable():
    SITE='https://sb-archive.mchang.xyz/mirror/'
    
    try:
        html_text=requests.get(SITE).text
    except Exception as e:
        logging.info('no internet connection')
        raise e
    soup=BeautifulSoup(html_text,'lxml')
    #scraps all links from the site
    links=soup.find_all('a')
    #grabs the last link which is the newest data
    link=links[-1]
    logging.info(link)
    filehref = link.get('href')
    logging.info(filehref)
    _url = urljoin(SITE, quote(filehref))
    logging.info(_url)
    parent_dir=os.getcwd()
    directory="data"
    path = os.path.join(parent_dir, directory)
    
    try:
        urlretrieve(_url, os.path.join(path, filehref))
        logging.info("file downloaded")
    except Exception as e:
        logging.info('failed to download')
        raise e

def main():
    ''' Function to scrape sponsorblock data from a daily updated mirror database.
    '''
    webscraper_callable()

if __name__ == "__main__":
    main()
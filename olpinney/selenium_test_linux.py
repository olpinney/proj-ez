'''  crawler.py

This modules contains functions that will calculate route information about traveling to
different cities in the US. 

It uses faredetective.com to gather its data. 

'''

# %load_ext autoreload
# %autoreload 2

import sys
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
import time
import pandas as pd

_AIRFARE_URL = "https://www.faredetective.com/farehistory/"
_CITIZEN_URL = "https://citizen.com/explore"

#need code and gecko and env to be in same directory
#create env by : source install.sh 
#enter by: source env/bin/activate
# run by: selenium_test_linux.py


_DATA_DICT_DEFAULT = {"to_city": [], "from_city": [], "fare": []}

def citizen_selenium():
    firefox_options = Options()
    firefox_options.add_argument("--headless") #dont want headless yet
    firefox_options.add_argument("--window-size=1920x1080")

    print("Crawling page...")
    driver = webdriver.Firefox(options=firefox_options)
    driver.get(_CITIZEN_URL)

    print("arrived")
    #incidents_links = driver.find_elements(By.CLASS_NAME,"incident-row regular")
    incidents_links = driver.find_elements(By.CLASS_NAME,"incident-container")


    print(len(incidents_links))

#class="incident-row regular"
#all set up in python. keep testing

    return 


def crawl(cities=[]):
    '''
        Crawls the faredective website and places the average fare prices inside the
        context provided.

        Args:
            cities([string]) - a list of cities to update in the fare context while crawling.
                                [] list represents to update all the popular destintations
    '''
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.add_argument("--window-size=1920x1080")

    print("Crawling route data...")
    driver = webdriver.Firefox(options=firefox_options)
    driver.get(_AIRFARE_URL)
    popular_dest_links = driver.find_elements(By.XPATH,"//td/a")
    data_dict = _DATA_DICT_DEFAULT
    found_cities = 0

    for i in range(len(popular_dest_links)):
        if found_cities == len(cities) and cities:
            break
        link =  driver.find_elements(By.XPATH,"//td/a")[i]
        print(link.text)
        m = re.search(r"Airfares\s*to\s*(.+)", link.text)
        if m:
            to_city = m.group(1).strip()
            if not cities or to_city in cities:
                link.click()
                time.sleep(1)
                found_cities += 1
                row = 1
                while True:
                    tab_link =  driver.find_elements(By.XPATH,
                        f"//table[@class='table table-history']/tbody/tr[{row}]/td[1]/a")

                    fare_elm =  driver.find_elements(By.XPATH,
                        f"//table[@class='table table-history']/tbody/tr[{row}]/td[@class='text-right']")
                    row += 1
                    if tab_link:
                        m = re.search(
                            r"Airfares\sfrom\s([\w\d\s]+)\s\(", tab_link[0].text)
                        if m:
                            from_city = m.group(1).strip()
                            if fare_elm:
                                m = re.search(r'\$(\d+)', fare_elm[0].text)
                                if m:
                                    fare = round(float(m.group(1).strip()), 2)
                                    # Update data_dict with all the data
                                    data_dict["from_city"].append(from_city)
                                    data_dict["to_city"].append(to_city)
                                    data_dict["fare"].append(fare)
                    else:
                        break
                driver.back()
                time.sleep(1)
    return pd.DataFrame(data_dict, columns=['from_city', 'to_city', 'fare'])


if __name__ == "__main__":
    citizen_selenium()
    # if len(sys.argv) >= 2:
    #     cities = sys.argv[1:]
    #     df = crawl(cities)
    # else:
    #     df = crawl()
    # print(df)

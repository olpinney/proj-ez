'''  crawler.py

This modules contains functions that will calculate route information about traveling to
different cities in the US. 

It uses faredetective.com to gather its data. 

'''
import sys
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import time
import pandas as pd

_AIRFARE_URL = "https://www.faredetective.com/farehistory/"
_DATA_DICT_DEFAULT = {"to_city": [], "from_city": [], "fare": []}

def crawl(cities=[]):
    '''
        Crawls the faredective website and places the average fare prices inside the
        context provided.

        Args:
            cities([string]) - a list of cities to update in the fare context while crawling.
                                [] list represents to update all the popular destintations
    '''
    ff_options = Options()
    ff_options.add_argument("--headless")
    ff_options.add_argument("--window-size=1920x1080")

    print("Crawling route data...")
    
    driver = webdriver.Firefox(options=ff_options)

    #geckodriver
    #https://stackoverflow.com/questions/40208051/selenium-using-python-geckodriver-executable-needs-to-be-in-path


    #driver = webdriver.Chrome(options=chrome_options)
    driver.get(_AIRFARE_URL)
    popular_dest_links = driver.find_elements_by_xpath("//td/a")
    data_dict = _DATA_DICT_DEFAULT
    found_cities = 0

    for i in range(len(popular_dest_links)):
        if found_cities == len(cities) and cities:
            break
        link = driver.find_elements_by_xpath("//td/a")[i]
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
                    tab_link = driver.find_elements_by_xpath(
                        f"//table[@class='table table-history']/tbody/tr[{row}]/td[1]/a")

                    fare_elm = driver.find_elements_by_xpath(
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
    if len(sys.argv) >= 2:
        cities = sys.argv[1:]
        df = crawl(cities)
    else:
        df = crawl()
    print(df)
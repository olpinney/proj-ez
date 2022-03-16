'''
This file connects to Chicago Crime Data Portal via API
and writes data to a sql database.
'''

import datetime
import requests
import pandas as pd
import util


SQL_PATH="proj_ez.sqlite3"

def download_relevant_data(file_name, start_date, end_date):
    '''
    Downloads relevant tnp data from City of Chicago data portal

    Inputs:
        table_name(str): table to update

        Outputs: None (saved csvs)
    '''
    #Get intro into loop, and clean up, use get request, clean, upload
    os = 0
    #Set response to a list of length 1000, to get into while loop
    response = [0] * 50
    start_date = str(start_date)
    end_date = str(end_date)
    start_date = start_date.replace(" ","T")
    end_date = end_date.replace(" ","T")
    params = {'$limit': 50, '$offset': os}
    response = requests.get('https://data.cityofchicago.org/resource/' + \
                                  'crimes.json?$where=Date%20' + \
                                  'between%20%27{start}%27%20and'.format(start = start_date)+ \
                                  '%20%27{end}%27'.format(end = end_date), params).json()
    df = pd.DataFrame.from_dict(response)
    os += 50
    #output to sql
    util.insert_sql(df,'Chi_Data_Portal',sql_path=SQL_PATH)
    while len(response) >= 50:
        print(start_date)
        print(end_date)
        params = {'$limit': 50, '$offset': os}
        response = requests.get('https://data.cityofchicago.org/resource/' + \
                                'crimes.json?$where=Date%20' + \
                                'between%20%27{start}%27%20and'.format(start = start_date)+ \
                                '%20%27{end}%27'.format(end = end_date), params).json()
        df = pd.DataFrame.from_dict(response)
        os += 50
        util.insert_sql(df,'Chi_Data_Portal',sql_path=SQL_PATH)

def wrapper(start_date, end_date):
    '''
    Creates intervals between start and end data and pulls data between that interval
    Inputs:
        start_date(date)
        end_date(date)
    Output: None
    '''

    intervals = pd.date_range(start_date,end_date)
    for i in range(len(intervals)-1):
        print("pulling from {i} to {j}".format(i = intervals[i], j = intervals[i+1]))
        download_relevant_data('crime_data'+'{}'.format(i), intervals[i], intervals[i+1])

def call_api():
    '''
    calls the police_api module
    output: None
    '''
    #set to run from date off last entry until today
    last_updated = util.last_updated('Chi_Data_Portal', 'date')
    wrapper(last_updated,datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

if __name__ == "__main__":
    call_api()

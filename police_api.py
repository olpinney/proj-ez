import string
import requests
import pandas as pd
from datetime import datetime
import time
import csv


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
    df.to_csv('crime_data_folder/'+ file_name, encoding='utf-8', index=False)
    os += 50
    while len(response) >= 50:
        print(start_date)
        print(end_date)
        params = {'$limit': 50, '$offset': os}
        response = requests.get('https://data.cityofchicago.org/resource/' + \
                                'crimes.json?$where=Date%20' + \
                                'between%20%27{start}%27%20and'.format(start = start_date)+ \
                                '%20%27{end}%27'.format(end = end_date), params).json()
        df = pd.DataFrame.from_dict(response)
        print(df)
        #df = df_cleaner(df)
            # writing the data rows
        df.to_csv('crime_data_folder/'+ file_name, encoding='utf-8', index=False, mode = 'a')
        os += 50
        
def wrapper(start_date, end_date):
  intervals = pd.date_range(start_date,end_date)
  for i in range(len(intervals)-1):
    print("pulling from {i} to {j}".format(i = intervals[i], j = intervals[i+1]))
    download_relevant_data('crime_data'+'{}'.format(i), intervals[i], intervals[i+1])

wrapper("2022-01-01T00:00:00","2022-02-28T00:00:00")
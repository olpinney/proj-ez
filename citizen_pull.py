'''
API requests from Citizen (https://citizen.com/explore)

Author:Olivia Pinney 
proj-ez
'''
#from cmath import isnan
import sqlite3
import os
import pandas as pd
import requests
import util

PULL_SCALING=2

#personal notes
'''
%load_ext autoreload
%autoreload 2

import citizen_pull as c

dict_keys(['created_at', 'updated_at', 'title', 'address', 'city_code', 'location', 'neighborhood', 'categories', 
'ranking.level', 'ranking.has_video', 'ranking.views', 'ranking.notifications', 
'severity', '_geoloc', 'updates', 'objectID', '_highlightResult'])

'''

# url_ext=["theft","shooting","assualt","knife"]


def citizen_refresh(search=None,table_name="citizen",limit=20):
    '''
    Update csv and sql per search term 

    Inputs:
        search (str): optional filter term, default off
        table_name (str): name of table in csv and sql
   
    ''' 
    #customize per search term 
    if search is not None:
        table_name+=f"_{search}"

    #get data
    new_df=pull_recent(table_name,search,limit)

    #create csv label 
    max_new_date=max(new_df['created_at'])
    min_new_date=min(new_df['created_at'])

    #save data
    util.df_to_csv(new_df,f"olpinney/data/{table_name}_{min_new_date}-{max_new_date}.csv")
    util.insert_sql(new_df,table_name)

def pull_recent(table_name,search,limit):
    c=util.get_cursor()

    #pull in old data
    try:
        query= f"select created_at from {table_name}" #specifically for the citizen table 
        old_dates=c.execute(query).fetchall()
        old_dates_cleaned=[int(date[0]) for date in old_dates]
        max_old_date=max(old_dates_cleaned)
    except sqlite3.OperationalError:
        max_old_date=0

    #pull in new data
    new_df=get_incidents_chicago(limit,search)

    while max_old_date < min(new_df['created_at']):
        limit=limit*PULL_SCALING

        new_df=get_incidents_chicago(limit,search)

    return new_df

 
def get_incidents_chicago(limit=1,search=None):
    '''
    pulls incidents from citizen.com/explore specifically for chicago region

    Inputs:
        limit (int): how many incidents to pull, default of 1
        search (str): optional filter term, default off 

    Return:
        citizen_dict (dict): dictionary of incident dictionaries
    '''
    lat_long=(41.763221610079455,-87.75672119312583,41.93975658843911,-87.56055880687256)
    return get_incidents(lat_long,limit,search)

def get_incidents(lat_long,limit,search=None):
    '''
    pulls incidents from citizen.com/explore 

    Inputs:
        lat_long (tuple): min latitude, min longitude, max latitude, max longitude integers
        limit (int): how many incidents to pull
        search (str): optional filter term, default off

    Return:
        citizen_dict (dict): dictionary of incident dictionaries
    '''

    api_url="https://citizen.com/api/incident/search?insideBoundingBox[0]={}&insideBoundingBox[1]={}&insideBoundingBox[2]={}&insideBoundingBox[3]={}&limit={}"
    params=[entry for entry in lat_long]
    params.append(limit)
    if search:
        api_url+="&q={}&"
        params.append(search)
        
    url=api_url.format(*params)
    request=requests.get(url) #add max-age cache-control for refreshes
    citizen_dict=request.json()
    
    results_df=pd.DataFrame(citizen_dict['hits'])
    return results_df


if __name__ == "__main__":
    #url_ext=["theft","weapon","assualt","knife"]
    citizen_refresh()
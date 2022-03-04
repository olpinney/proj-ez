'''
API requests from Citizen (https://citizen.com/explore)

Author:Olivia Pinney 
proj-ez
'''
from cmath import isnan
import requests
import bs4
import csv
import json
import sqlite3
import os
import pandas as pd

import util


#personal notes
'''
%load_ext autoreload
%autoreload 2
import citizen_pull as c
test=c.get_request_chicago(limit=5)

dict_keys(['created_at', 'updated_at', 'title', 'address', 'city_code', 'location', 'neighborhood', 'categories', 
'ranking.level', 'ranking.has_video', 'ranking.views', 'ranking.notifications', 
'severity', '_geoloc', 'updates', 'objectID', '_highlightResult'])

#put into sql - only exclude rankings
#see if there are dupe object_ids
#pull all categories
#figure out time frame of guns
#pull all red severity

#create connection
#try and accept to make sure table exists

    cur = conn.cursor()
    insert_query = ''INSERT INTO public.profile_data VALUES %s''
    print("got to tuples")
    tpls = [tuple(x) for x in df.to_numpy()]
    print(tpls[1])

    psycopg2.extras.execute_values(cur, insert_query, tpls, template=temp)

you know how many columns there will be, so can just insert them
create the tuple, and just insert it 

make a first query file to see if the query exists
if it doesnt, then create query (create table with these columns)
have this create query in the code
else append 

'''

# url_ext=["theft","shooting","assualt","knife"]
# want to identify the businesses, churches, schools, hospitals, nursing homes that they are interacting with
# car jacking, sexualizations


def citizen_refresh(search=None,table_name="citizen"):
    '''
    Update csv and sql per search term 

    Inputs:
        search (str): optional filter term, default off
        table_name (str): name of table in csv and sql
   
    ''' 
    #customize per search term 
    if search is not None:
        table_name+=f"_{search}"

    c=util.get_cursor()

    #pull in old data
    try:
        query= f"select updated_at from {table_name}" #specifically for the citizen table 
        old_dates=c.execute(query).fetchall()
        old_dates_cleaned=[int(date[0]) for date in old_dates]
        max_old_date=max(old_dates_cleaned)
    except sqlite3.OperationalError:
        max_old_date=0

    #pull in new data
    min_new_date=0
    limit=20
    limit_scaling=2
    new_df=get_incidents_chicago(limit,search)

    while max_old_date<min_new_date:
        limit=limit*limit_scaling

        new_df=get_incidents_chicago(limit,search)
        min_new_date=min(new_df['updated_at'])

    max_new_date=max(new_df['updated_at'])

    #save data
    util.df_to_csv(new_df,f"data/{table_name}_{min_new_date}-{max_new_date}.csv")
    util.insert_sql(new_df,table_name)

 
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
    citizen_refresh()

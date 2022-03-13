'''
API requests from Citizen (https://citizen.com/explore)

Author:Olivia Pinney 
proj-ez
'''
import sqlite3
import os
import pandas as pd
import requests
import util
import datetime

PULL_SCALING=2
TABLE_NAME='citizen'

'''
%load_ext autoreload
%autoreload 2

import citizen_pull as c

# to refresh run this:
c.citizen_refresh() #limit = int is ozptional 

#to delete sql object and create a new one:
c.reset_citizen(limit) #pick a big limit 

#tbd
citizen_backfill()
probably will be helpful to use: clean_citizen(df)


#get citizen to work with the search terms
#save into one SQL
#figure out what is wrong with old sql 

'''

def citizen_refresh(limit=20,search=None,table_name=TABLE_NAME):
    '''
    Pulls data since latest pull and updates csv and sql 

    Inputs:
        search (str): optional filter term, default off
        table_name (str): name of table in csv and sql
        limit (int): how many incidents to pull for first itteration
   
    ''' 
    #customize per search term 
    if search is not None:
        table_name+=f"_{search}"

    #get data
    new_df=pull_recent(limit,search,table_name)
    save_citizen_csv(new_df,table_name)
 
    new_df_cleaned=clean_citizen(new_df) 
    citizen_dedupe_SQL(new_df_cleaned,table_name)


def reset_citizen(limit,search=None,table_name=TABLE_NAME):
    '''
    Deletes SQL table for citizen and sets it using latest data

    Inputs:
        search (str): optional filter term, default off
        table_name (str): name of table in csv and sql
        limit (int): how many incidents to pull for first itteration
    
    '''
     #customize per search term 
    if search is not None:
        table_name+=f"_{search}"

    util.drop_table(table_name)
    new_df=get_incidents_chicago(limit,search)
    save_citizen_csv(new_df,table_name)
    
    new_df_cleaned=clean_citizen(new_df)
    util.insert_sql(new_df_cleaned,table_name)

def citizen_backfill(table_name,file_path):
    '''
    Back fills SQL table using csvs
    '''

    # for all csvs in project ez/ olpinney / data
    #import 
    with open(file_path,'r') as file:
        df=pd.read_csv(file,mangle_dupe_cols=False)

    return df

def citizen_dedupe_SQL(new_df,table_name):
    ''' 
    remove duplicated data between new_df and SQL and replace SQL

    Inputs:
        new_df (df): new data
        table_name (str): name of table in sql

    '''

    connection=util.get_connection()
    
    #pull in old data
    query= f"select * from {table_name}" #specifically for the citizen table 
    old_df=pd.read_sql(query, con = connection)

    #add to new data

    print("about old df")
    print(type(old_df))
    print(old_df.columns)

    print("about new df")
    print(type(new_df))
    print(new_df.columns)

    df=pd.concat(old_df,new_df)

    df.drop_duplicates(keep='first')

    #save to sql
    util.drop_table(table_name)
    util.insert_sql(new_df,table_name)

def save_citizen_csv(new_df,table_name):
    '''
    saves citizen to csv and sql

    Inputs:
        new_df (df): data to save
        table_name (str): name of table in csv and sql
    '''

    #create csv label 
    max_new_date=max(new_df['created_at'])
    min_new_date=min(new_df['created_at'])

    #save data 
    # updated util to create two files, new one from date and appends to existing citizen csv
    util.df_to_csv(new_df,f"olpinney/data/{table_name}_{min_new_date}-{max_new_date}.csv")

def clean_citizen(df):
    '''
    removes extra columns from citizen data, and fixes lat and long 
    '''
    df['lat']=df["_geoloc"].apply(lambda x: x[0]['lat']) 
    df['long']=df["_geoloc"].apply(lambda x: x[0]['lng']) 
    to_drop=['_geoloc','city_code','ranking.level','ranking.has_video','ranking.views','ranking.notifications','_highlightResult']
    df.drop(columns=to_drop)
    return df

def pull_recent(limit,search,table_name):
    '''
    pull data from citizen until SQL filled back to last saved incident
    
    Inputs:
        search (str): optional filter term, default off
        table_name (str): name of table in sql
        limit (int): how many incidents to pull for first itteration

    Returns
        new_df (df): dataframe containing all incidences since last pull 
    '''
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

    i=0
    while max_old_date < min(new_df['created_at']):


        print(f"max old date is {convert_dt(max_old_date)} while min date is {convert_dt(min(new_df['created_at']))}")
        print(f"limit is {limit}")
        i+=1
        if i==2:
            break 

        limit=limit*PULL_SCALING
        old_df=new_df
        new_df=get_incidents_chicago(limit,search)
        if new_df is None:
            new_df=old_df
            break

    return new_df


def get_incidents_chicago(limit,search=None):
    '''
    pulls incidents from citizen.com/explore specifically for chicago region

    Inputs:
        limit (int): how many incidents to pull
        search (str): optional filter term, default off 

    Return:
        citizen_dict (dict): dictionary of incident dictionaries
    '''
    lat_long=(41.763221610079455,-87.75672119312583,41.93975658843911,-87.56055880687256)
    return get_incidents(lat_long,limit,search)

def convert_dt(timestamp):
    """
    converts unix format for date and time to datetime object
    """
    timestamp = int(timestamp) / 1000
    dt_obj = datetime.datetime.fromtimestamp(timestamp)
    return dt_obj


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
    
    try:
        results_df=pd.DataFrame(citizen_dict['hits'])
        return results_df

    except KeyError:
        #API request was too big
        print(f"limit of {limit} was too big")
        return None



if __name__ == "__main__":
    #url_ext=["theft","weapon","assualt","knife"]
    citizen_refresh()
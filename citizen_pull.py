'''
API requests from Citizen (https://citizen.com/explore)

Author:Olivia Pinney 
proj-ez
'''
import sqlite3
import os
from unicodedata import category
import pandas as pd
import requests
import util
import datetime
import re
import json


PULL_SCALING=2
TABLE_NAME='citizen'

'''
%load_ext autoreload
%autoreload 2

import citizen_pull as c

# to refresh run this:
c.citizen_refresh() 
OR
c.citizen_searches_refresh()
#Notes: limit = int is optional. ~500 incidences happen per day, but max of 1000 can be pulled at once

#to delete sql object and create a new one:
c.reset_citizen(limit) #pick a 1000 limit 
c.citizen_backfill('citizen') #to backfill with old data 
OR
c.citizen_searches_reset_and_backfill()

#To see the results
x=c.citizen_get_sql(search="{desired search term}",table_name="citizen")

#to pull from citizen 
y=c.get_incidents_chicago(limit) #select limit= how many rows you want
y=c.clean_citizen(y)


'''

SEARCH_CLEANED={None:None,'Assault':'Assault / Fight','Break_In':'Break In','Gun':'Gun Related','Harassment':'Harassment','Police':'Police Related','Pursuit':'Pursuit / Search','Theft':'Robbery / Theft','Weapon':'Weapon'}
#this needs to be swapped 

def add_cleaned_search(search,table_name):
    if search is not None:
        search=search.replace("/", "AND") #to avoid errors in writing to csv
        search=search.replace(" ", "_") #to avoid errors in writing to sql
        table_name+=f"_{search}"
    return table_name

def citizen_refresh(limit=100,search=None,table_name=TABLE_NAME):
    '''
    Pulls data since latest pull and updates csv and sql 

    Inputs:
        search (str): optional filter term, default off
        table_name (str): name of table in csv and sql
        limit (int): how many incidents to pull for first itteration
   
    ''' 
    #get data
    new_df=pull_recent(limit,search,table_name)
    save_citizen_csv(new_df,search,table_name)
 
    new_df_cleaned=clean_citizen(new_df)
    citizen_dedupe_SQL(new_df_cleaned,search,table_name)


def citizen_searches_refresh(searches=SEARCH_CLEANED.keys()):
    for search in searches:
        citizen_refresh(search=search)

def citizen_searches_reset_and_backfill(searches=SEARCH_CLEANED.keys()):
    for search in searches:
        reset_citizen(search=search)
        citizen_backfill(search=search)
        print(f"finished {search}")

def reset_citizen(limit=1000,search=None,table_name=TABLE_NAME):
    '''
    Deletes SQL table for citizen and sets it using latest data

    Inputs:
        search (str): optional filter term, default off
        table_name (str): name of table in csv and sql
        limit (int): how many incidents to pull for first itteration
    
    '''
    table_name_w_search = add_cleaned_search(search,table_name)

    util.drop_table(table_name_w_search)
    new_df=get_incidents_chicago(limit,search)
    save_citizen_csv(new_df,search,table_name)
    
    new_df_cleaned=clean_citizen(new_df)
    util.insert_sql(new_df_cleaned,table_name_w_search)

def citizen_backfill(search=None,table_name=TABLE_NAME,file_path="olpinney/data"):
    '''
    Back fills SQL table using csvs
    '''
    table_name_w_search = add_cleaned_search(search,table_name)

    sql_df=citizen_get_sql(search,table_name)

    file_names=os.listdir(file_path)

    for file in file_names:
        if re.match(f"^{table_name_w_search}_\d.*-.*\d\.csv$",file):
            with open(f"{file_path}/{file}",'r') as file:
                df=pd.read_csv(file,mangle_dupe_cols=True)
                
            #make _geoloc back into list containing single dictionary 
            df["_geoloc"]=df["_geoloc"].apply(lambda x: [json.loads(x[1:-1].replace("'", "\""))])
            #make categories into string from string represetnation of a single entry list
            df["categories"]=df["_categories"].apply(lambda x: x[2:-2])
            df_clean=clean_citizen(df)
            sql_df=pd.concat([sql_df,df_clean])
                
    sql_df.drop_duplicates(keep='first')
    print(sql_df)

    #save to sql
    util.drop_table(table_name_w_search)
    util.insert_sql(sql_df,table_name_w_search)            


def citizen_dedupe_SQL(new_df,search=None,table_name=TABLE_NAME):
    ''' 
    remove duplicated data between new_df and SQL and replace SQL

    Inputs:
        new_df (df): new data
        table_name (str): name of table in sql

    '''
    table_name_w_search = add_cleaned_search(search,table_name)

    #combine the data 
    old_df=citizen_get_sql(search,table_name)

    df=pd.concat([old_df,new_df])
    df.drop_duplicates(keep='first')

    #save to sql
    util.drop_table(table_name_w_search)
    util.insert_sql(new_df,table_name_w_search)

def citizen_get_sql(search=None,table_name=TABLE_NAME):
    ''' 
    get citizen data from SQL

    Inputs:
        table_name (str): name of table in sql

    Output:
        df (df): deduped df
    '''
    table_name_w_search = add_cleaned_search(search,table_name)
    connection=util.get_connection()
    
    #pull in old data
    query= f"select * from {table_name_w_search}" #specifically for the citizen table 
    df=pd.read_sql(query, con = connection)

    df=df.drop(columns='index')

    return df 


def save_citizen_csv(new_df,search=None,table_name=TABLE_NAME):
    '''
    saves citizen to csv and sql

    Inputs:
        new_df (df): data to save
        table_name (str): name of table in csv and sql
    '''
    table_name_w_search = add_cleaned_search(search,table_name)

    #create csv label 
    max_new_date=convert_dt(max(new_df['created_at'])).date()
    min_new_date=convert_dt(min(new_df['created_at'])).date()

    #save data 
    # updated util to create two files, new one from date and appends to existing citizen csv
    util.df_to_csv(new_df,f"olpinney/data/{table_name_w_search}_{min_new_date}-{max_new_date}.csv")

def clean_citizen(df):
    '''
    removes extra columns from citizen data, and fixes lat and long 
    '''
    df['lat']=df["_geoloc"].apply(lambda x: x[0]['lat']) 
    df['long']=df["_geoloc"].apply(lambda x: x[0]['lng']) 
    df['categories']=df['categories'].apply(lambda x: ", ".join(x))

    to_drop=['_geoloc','updates','city_code','ranking.level','ranking.has_video','ranking.views','ranking.notifications','_highlightResult']
    df=df.drop(columns=to_drop)

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

    table_name_w_search = add_cleaned_search(search,table_name)

    #pull in old data
    try:
        query= f"select created_at from {table_name_w_search}" #specifically for the citizen table 
        old_dates=c.execute(query).fetchall()
        old_dates_cleaned=[int(date[0]) for date in old_dates]
        max_old_date=max(old_dates_cleaned)
    except sqlite3.OperationalError:
        max_old_date=0

    #pull in new data
    new_df=get_incidents_chicago(limit,search)

    while max_old_date < min(new_df['created_at']) and limit*PULL_SCALING<=1000: #API restricts pulls at 1000

        print(f"max old date is {convert_dt(max_old_date)} while min date is {convert_dt(min(new_df['created_at']))}")
        print(f"limit is {limit}")

        limit=limit*PULL_SCALING
        old_df=new_df
        new_df=get_incidents_chicago(limit,search)
        if new_df is None: #e.g. the pull failed, then use last successful pull 
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
        try:
            params.append(SEARCH_CLEANED[search])
        except KeyError:
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
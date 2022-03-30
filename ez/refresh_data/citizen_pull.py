'''API requests from Citizen (https://citizen.com/explore)

Script pulls up to 1000 records from the Citizen website interface
Currently set to pull data in the Chicago region, but can be set for other geographies

Primary functions:
    __main__() calls citizen_refresh() 
    get_incidents() pulls incidents from citizen for specified coordinate range
    citizen_refresh() pulls 1000 records or all records since the last pull (whichever is smaller)
        and saves records into sql and csv backups. Set to Chicago
    reset_citizen() deletes sql table and resets it with 1000 records. Set to Chicago
    citizen_backfill() backfulls sql table from csvs. Set to Chicago

Suggested file structure:
    #Parent folder
        syntax #folder storing code
        data #folder storing data (if named otherwise, change DATA_PATH variable)

Caution: uses global variables, which should be set to individual purposes

Author:Olivia Pinney
Updated: March 2022

'''
import sqlite3
import os
import re
import pandas as pd
import requests
import util
import datetime
import json


PULL_SCALING=2 #factor to increase pull by when pull is too small
STARTING_PULL=100
TABLE_NAME='citizen' #sql table name
DATA_PATH="../data" #where csv files are saved for backfill
SQL_PATH="proj_ez.sqlite3" #name of sql file for project ez


def citizen_refresh(limit:int,table_name=TABLE_NAME) -> None:
    '''
    Pulls data since latest pull and updates csv and sql

    Inputs:
        table_name (str): name of table in csv and sql
        limit (int): how many incidents to pull for first itteration

    '''
    #get data
    new_df=pull_recent(limit,table_name)
    save_citizen_csv(new_df,table_name)

    new_df_cleaned=clean_citizen(new_df)
    citizen_dedupe_sql(new_df_cleaned,table_name)

def reset_citizen(limit=1000,table_name=TABLE_NAME) -> None:
    '''
    Deletes SQL table for citizen and sets it using latest data

    Inputs:
        table_name (str): name of table in csv and sql
        limit (int): how many incidents to pull for first itteration

    '''
    drop_table(table_name)
    new_df=get_incidents_chicago(limit)
    save_citizen_csv(new_df,table_name)

    new_df_cleaned=clean_citizen(new_df)
    insert_sql(new_df_cleaned,table_name)

def citizen_backfill(table_name=TABLE_NAME,file_path=DATA_PATH) -> None:
    '''
    Back fills SQL table using csvs

    Inputs:
        table_name (str): name of table in csv and sql
        file_path (str): location of data to backfill

    '''
    sql_df=citizen_get_sql(table_name)

    file_names=os.listdir(file_path)

    for file in file_names:
        if re.match(f"^{table_name}_\d.*.csv$",file):
            with open(f"{file_path}/{file}",'r') as file:
                df=pd.read_csv(file,mangle_dupe_cols=True)
                #should be false once pandas package supports it

            #make _geoloc back into list containing single dictionary
            df["_geoloc"]=df["_geoloc"].apply(lambda x: [json.loads(x[1:-1].replace("'", "\""))])
            #make categories into string from string represetnation of a single entry list
            df["categories"]=df["categories"].apply(lambda x: x[1:-1].replace("'",""))
            df_clean=clean_citizen(df)
            sql_df=pd.concat([sql_df,df_clean])

    overwrite_sql_w_dedup(sql_df,table_name)


def citizen_dedupe_sql(new_df:pd.DataFrame,table_name=TABLE_NAME) -> None:
    '''
    remove duplicated data between new_df and SQL and replace SQL

    Inputs:
        new_df (df): new data
        table_name (str): name of table in sql

    '''
    #combine the data
    old_df=citizen_get_sql(table_name)

    df=pd.concat([old_df,new_df])
    overwrite_sql_w_dedup(df,table_name)

def overwrite_sql_w_dedup(df,table_name=TABLE_NAME) -> None:
    '''
    Overwrite sql file with deduplicated data

    Inputs:
        df (df): updated data
        table_name (str): name of table in sql
    '''
    dedupded_df=df.drop_duplicates(subset='objectID',keep='last')

    #save to sql
    drop_table(table_name)
    insert_sql(dedupded_df,table_name)

def citizen_get_sql(table_name=TABLE_NAME) -> pd.DataFrame:
    '''
    get citizen data from SQL

    Inputs:
        table_name (str): name of table in sql

    Output:
        df (df): deduped df
    '''
    connection=sqlite3.connect(SQL_PATH)

    #pull in old data
    query= f"select * from {table_name}" #specifically for the citizen table
    df=pd.read_sql(query, con = connection)

    df=df.drop(columns='index')

    return df


def save_citizen_csv(new_df:pd.DataFrame,table_name=TABLE_NAME) -> None:
    '''
    saves citizen to csv and sql

    Inputs:
        new_df (df): data to save
        table_name (str): name of table in csv and sql
    '''

    #create csv label
    max_new_date=convert_dt(max(new_df['created_at']))
    max_month=max_new_date.month
    max_year=max_new_date.year

    df_to_csv(new_df,f"{DATA_PATH}/{table_name}_{max_month}-{max_year}.csv")


def clean_citizen(df:pd.DataFrame) -> pd.DataFrame:
    '''
    removes extra columns from citizen data, and fixes lat and long
    '''
    df['lat']=df["_geoloc"].apply(lambda x: x[0]['lat'])
    df['long']=df["_geoloc"].apply(lambda x: x[0]['lng'])
    # df['categories']=df['categories'].apply(lambda x: ", ".join(x))
    df['categories']=df['categories'].apply(lambda x: ", ".join(x) if isinstance(x,list) else x)

    to_drop=['_geoloc','updates','city_code','ranking.level','ranking.has_video',
            'ranking.views','ranking.notifications','_highlightResult']
    df=df.drop(columns=to_drop)

    return df

def pull_recent(limit:int,table_name:str) -> pd.DataFrame:
    '''
    pull data from citizen until SQL filled back to last saved incident
    
    Inputs:
        table_name (str): name of table in sql
        limit (int): how many incidents to pull for first itteration

    Returns
        new_df (df): dataframe containing all incidences since last pull
    '''
    c=get_cursor()

    #pull in old data
    try:
        query= f"select created_at from {table_name}" #specifically for the citizen table
        old_dates=c.execute(query).fetchall()
        old_dates_cleaned=[int(float(date[0])) for date in old_dates]
        max_old_date=max(old_dates_cleaned)
    except sqlite3.OperationalError:
        max_old_date=0

    #pull in new data
    new_df=get_incidents_chicago(limit)

    while max_old_date < min(new_df['created_at']) and limit*PULL_SCALING<=1000: #API restricts pulls at 1000

        print(f"max old date is {convert_dt(max_old_date)} while min date is {convert_dt(min(new_df['created_at']))}")
        print(f"limit is {limit}")

        limit=limit*PULL_SCALING
        old_df=new_df
        new_df=get_incidents_chicago(limit)
        if new_df is None: #e.g. the pull failed, then use last successful pull
            new_df=old_df
            break

    return new_df


def get_incidents_chicago(limit:int) -> pd.DataFrame or None:
    '''
    pulls incidents from citizen.com/explore specifically for chicago region

    Inputs:
        limit (int): how many incidents to pull

    Return:
        citizen_dict (dict): dictionary of incident dictionaries
    '''
    lat_long=(41.763221610079455,-87.75672119312583,41.93975658843911,-87.56055880687256)
    return get_incidents(lat_long,limit)


def get_incidents(lat_long:tuple,limit:int) -> pd.DataFrame or None:
    '''
    pulls incidents from citizen.com/exploreq

    Inputs:
        lat_long (tuple): min latitude, min longitude, max latitude, max longitude integers
        limit (int): how many incidents to pull

    Return:
        citizen_dict (dict): dictionary of incident dictionaries
    '''

    api_url="https://citizen.com/api/incident/search?insideBoundingBox[0]={}&insideBoundingBox[1]={}&insideBoundingBox[2]={}&insideBoundingBox[3]={}&limit={}"
    params=[entry for entry in lat_long]
    params.append(limit)

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

def convert_dt(timestamp:float) -> datetime.datetime:
    """
    converts unix format for date and time to datetime object
    """
    timestamp = int(timestamp) / 1000
    dt_obj = datetime.datetime.fromtimestamp(timestamp)
    return dt_obj

def df_to_csv(df:pd.DataFrame, file_path:str) -> None:
    '''
    save pandas to csv, and append if not previously created

    Inputs:
        results_df (df): data frame with new data
        path (str): location to save data
    '''
    header_bool=(not os.path.exists(file_path))

    with open(file_path,'a') as file:
        df.to_csv(file,index=True, header=header_bool, line_terminator='\n')

def insert_sql(df:pd.DataFrame,table_name:str,sql_path=SQL_PATH) -> None:
    '''
    insert latest data into sql table

    Inputs:
        incidents (df): dataframe of new data
        name (str): name of table to insert into
        path (str): file path to sql file

    '''

    connection = sqlite3.connect(sql_path)
    df=df.astype('string') #needed this because list objects were giving me issues
    df.to_sql(table_name, connection, if_exists='append')

def drop_table(table_name:str) -> None:
    '''
    drop table from SQL database
    '''
    c=get_cursor()

    #delete old table
    try:
        query= f"drop table {table_name}"
        c.execute(query).fetchall()
    except sqlite3.OperationalError:
        print("table already dropped")

def get_cursor(sql_path=SQL_PATH) -> sqlite3.Cursor:
    '''
    get cursor for sql document

    Inputs:
        path (str): file path to sql file

    Returns:
        (cursor): cursor to sql

    '''

    return sqlite3.connect(sql_path).cursor()

def last_updated(table_name:str, date_col:str) -> datetime.datetime or int:
    '''
    pull data from citizen until SQL filled back to last saved incident

    Inputs
        table_name (str): name of table in sql
        date_col (str): name of date column in sql table

    Returns
        date/time of most recent entry in dataframe in "%Y-%m-%dT%H:%M:%S" format
    '''
    c = get_cursor()

    #pull in old data
    try:
        query= f"select {date_col} from {table_name}" #specifically for the citizen table
        old_dates=c.execute(query).fetchall()
        if 'citizen' in table_name:
            old_dates_cleaned=[int(date[0]) for date in old_dates]
            max_old_date=max(old_dates_cleaned)
        else:
            max_old_date=max(old_dates)[0][:19]

    except sqlite3.OperationalError:
        max_old_date=0

    return max_old_date


if __name__ == "__main__":
    citizen_refresh(STARTING_PULL)
    
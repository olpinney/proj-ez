'''
Util functions for saving data

Author:Olivia Pinney 
proj-ez
'''

import pandas as pd
import os
import sqlite3

SQL_PATH="proj_ez.sqlite3"

def df_to_csv(df,file_path):
    '''
    save pandas to csv, and append if not previously created

    Inputs:
        results_df (df): data frame with new data
        path (str): location to save data
    '''

    header_bool=(not os.path.exists(file_path))

    with open(file_path,'a') as file:
        df.to_csv(file,index=True,header=header_bool,line_terminator='\n')




def insert_sql(df,table_name,sql_path=SQL_PATH):
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

def drop_table(table_name):
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

def get_cursor(sql_path=SQL_PATH):
    '''
    get cursor for sql document

    Inputs:
        path (str): file path to sql file

    Returns:
        (cursor): cursor to sql  

    '''

    connection = sqlite3.connect(sql_path)

    return connection.cursor()

def get_connection(sql_path=SQL_PATH):
    '''
    get connection for sql document

    Inputs:
        path (str): file path to sql file

    Returns:
        (connection): connection to sql  
    '''
    return sqlite3.connect(sql_path)

def pull_recent(table_name, date_col):
    '''
    pull data from citizen until SQL filled back to last saved incident
    
    Inputs
        table_name (str): name of table in sql
        date_col (str): name of date column in sql table

    Returns
        date/time of most recent entry in dataframe in "%Y-%m-%dT%H:%M:%S" format
    '''
    c=util.get_cursor()

    #pull in old data
    try:
        query= f"select {date_col} from {table_name}" #specifically for the citizen table 
        old_dates=c.execute(query).fetchall()
        if table_name == 'citizen':
            old_dates_cleaned=[int(date[0]) for date in old_dates]
            max_old_date=max(old_dates_cleaned)
        else:
            max_old_date=max(old_dates)[0][:19]
        
    except sqlite3.OperationalError:
        max_old_date=0
    
    return max_old_date

    
# def create_table_sql(cursor,cols,name):
#     '''
#     Create new table in SQL document if it doesnt already exist

#     Inputs:
#         cursor (cursor): cursor object for SQL table 
#         cols (list): columns for new table
#         name (str): name of new table
#     '''
#     try: 
#         col_count=len(cols)
#         #note that list type is not accepted 
#         create_query="CREATE TABLE "+name+" ("+", ".join(cols)+")"
#         cursor.execute(create_query)
#     except sqlite3.OperationalError:
#         print("table already exists")
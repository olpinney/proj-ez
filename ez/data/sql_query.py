import pandas as pd
import sqlite3

#taken from Lamont PA3
def get_header(cursor):
    '''
    Given a cursor object, returns the appropriate header (column names)

    Inputs:
        Cursor (object): connected to SQLITE database

    Returns column headers for creating pandas dataframe
    '''
    header = []
    for i in cursor.description:
        s = i[0]
        if "." in s:
            s = s[s.find(".")+1:]
        header.append(s)
    return header

def create_df():    
    #created connection
    connection = sqlite3.connect("proj_ez.sqlite3")
    citi = connection.cursor()
    chi = connection.cursor()
    mock = connection.cursor()

    citizen_query = '''
        SELECT 
            title as description,
            created_at as date, 
            lat as latitude,
            long as longitude,
            categories as primary_type
        FROM citizen
        '''
    chi_query = '''
        SELECT 
            date,
            latitude, 
            longitude, 
            primary_type,	
            description
        FROM Chi_Data_Portal
        '''
    mock_query = '''
        SELECT
            cast(house_num as int) as house_num,
            street_name,
            street_type,
            street_intersection,
            date,
            time,
            primary_type
        FROM mock
        '''
    citizen_query = (citi.execute(citizen_query).fetchall())
    chi_query = (chi.execute(chi_query).fetchall())
    mock_query = (mock.execute(mock_query).fetchall())

    citizen_df = pd.DataFrame(citizen_query, columns=get_header(citi))
    chi_df = pd.DataFrame(chi_query, columns=get_header(chi))
    mock_df = pd.DataFrame(mock_query, columns=get_header(mock))

    return (citizen_df, chi_df, mock_df)
import pandas as pd
import sqlite3
from geopy.geocoders import Nominatim as nom
from geopy import distance

CONNECTION = sqlite3.connect("refresh_data/proj_ez.sqlite3")

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


def get_lat_long(mock_df):
    """
    Converts inputted location data from mock data into latitude
    and longitude tuples. Updates dataframe in place. 

    Inputs:
        mock_df (dataframe): inputted data capturing ECCSC first
                            response activity
    
    Returns None (updates dataframe in place)
    """
    # user agent is how requests to geopy are tracked (DON'T CALL ON FULL pandas)
    lat_longs = []
    for _, row in mock_df.iterrows():
        if row['house_num'] and row['street_type']:
            address = " ".join([row['house_num'], row['street_name'], row['street_type'], "Chicago"])
        elif row['house_num']:
            address = " ".join([row['house_num'], row['street_name'], "Chicago"])
        elif row['street_intersection']:
            address = " ".join([row['street_name'], row['street_type'], "and", row['street_intersection'], "Chicago"])
        else:
            print("insufficent location data")
            continue
        locator = nom(user_agent= 'laurenq@uchicago.edu')
        location = locator.geocode(address)
        if location:
            lat = location.latitude
            long = location.longitude
        else:
            lat = None
            long = None
            print("couldn't find coordinates", address)
        lat_long = lat, long
        lat_longs.append(lat_long)
    mock_df['lat_long'] = lat_longs


def create_survey_df():
    '''
    Query sql database and create pandas dataframe

    Inputs:
        None

    Returns (tuple of pandas dataframes) citizen and crime dataframes
    '''    
    connection = CONNECTION
    mock = connection.cursor()

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

    mock_query = (mock.execute(mock_query).fetchall())

    mock_df = pd.DataFrame(mock_query, columns=get_header(mock))

    mock_df['house_num'] = mock_df.house_num.fillna(0)
    mock_df['house_num'] = mock_df.house_num.astype(int)
    mock_df['house_num'] = mock_df.house_num.astype(str)

    get_lat_long(mock_df)
    return mock_df

def create_report_df(): 
    '''
    Query sql database and create pandas dataframe

    Inputs:
        None

    Returns (tuple of pandas dataframes) citizen and crime dataframes
    '''   
    connection = CONNECTION
    citi = connection.cursor()
    chi = connection.cursor()

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
    citizen_query = (citi.execute(citizen_query).fetchall())
    chi_query = (chi.execute(chi_query).fetchall())

    citizen_df = pd.DataFrame(citizen_query, columns=get_header(citi))
    chi_df = pd.DataFrame(chi_query, columns=get_header(chi))

    return (citizen_df, chi_df)


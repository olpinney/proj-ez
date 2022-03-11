
import pandas as pd
from geopy.geocoders import Nominatim as nom
from geopy import distance
import datetime
import sqlite3
import ast

# import geopy as geo
# import geopandas as geopd

chi_data = 'Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings.csv'
incidents = 'ECCSC First Responder_ Incident Report (Responses) - Form Responses 1.csv'
citizen = 'olpinney/data/citizen_0-1646429990000.csv'

#taken from Lamont PA3
def get_header(cursor):
    '''
    Given a cursor object, returns the appropriate header (column names)
    '''
    header = []
    for i in cursor.description:
        s = i[0]
        if "." in s:
            s = s[s.find(".")+1:]
        header.append(s)
    return header

def go():

    #created connection
    connection = sqlite3.connect("proj_ez.sqlite3")
    citi = connection.cursor()
    chi = connection.cursor()

    #REMOVE LIMIT 10
    citizen_query = '''
        SELECT 
            title, 
            created_at as date, 
            lat_long as lat_long, 
            categories
        FROM citizen
        LIMIT 10
        '''

    chi_query = '''
        SELECT 
            date,
            latitude, 
            longitude, 
            primary_type,	
            description
        FROM Chi_Data_Portal
        LIMIT 10
        '''

    citizen_query = (citi.execute(citizen_query).fetchall())
    chi_query = (chi.execute(chi_query).fetchall())

    citizen_df = pd.DataFrame(citizen_query, columns=get_header(citi))
    chi_df = pd.DataFrame(chi_query, columns=get_header(chi))

    clean_lat_long(citizen_df, 'citizen')
    clean_lat_long(chi_df, 'chi')

    standard_date_time(citizen_df, 'citizen')
    standard_date_time(chi_df, 'chi')
    
    return citizen_df, chi_df

#get citizen lat/long column to two columns to match with chigao database
#converts from list of str to list of tuples nd splits 
#turns out location is lat/long in one so can use that if prefet
def clean_lat_long(df, source):
    """
    if df citizen take lat/long string column to tuple
    if df chi lat/long columns zipped to tulpe

    Inputs:
        df (pandas df): dataframe to update lat/long for
        source (str): citizen or chi
    Returns: None (updates in place)
    """
    if source == 'citizen':
        lst = df['lat_long'].tolist()
        tup_lst = []
        for s in lst:
            tup_lst.append(ast.literal_eval(s))
        df['lat_long'] = tup_lst
    elif source == 'chi':
        df['lat_long'] = list(zip(df.latitude, df.longitude))


def read_csv_file(filename):
    """
    using to run tests in advance of receipt of Chicago API data

    Inputs:
        filename (string): name of csv file
    """
    cols_to_keep = ['CASE_NUMBER', 'BLOCK', 'DATE', 'INCIDENT_PRIMARY', 'ZIP_CODE', 
                    'WARD','COMMUNITY_AREA', 'STREET_OUTREACH_ORGANIZATION']
    df = pd.read_csv(filename, index_col = 'CASE_NUMBER', usecols = \
                                cols_to_keep)
    return df

# geopandas will need BLOCK + ZIPCODE to calculate lat / lon
# Chicago API and Citizen data already provided lat / lon

def get_lat_long():
    """
    converts block and zipcode into a lat / lon coordinate

    Inputs:
        
    """
    # user agent is how requests to geopy are tracked (DON'T CALL ON FULL pandas)
    locator = nom(user_agent= 'ecjackson1821@gmail.com')
    location = locator.geocode("3400 W CHICAGO AVE 60651")
    return location

def reported_difference_in_dist(loc_1, loc_2, lower_bound = 0, upper_bound = 2):
    """
    takes in two lat/long tuples and finds the geodesic distance between the 
    two points. Uses lower and upper bound limits to determine likelihood of
    accurate match
    """
    dist = distance.great_circle(loc_1, loc_2).miles
    if dist >= lower_bound and dist <= upper_bound:
        return True
    return False

def standard_date_time(df, source):
    """
    Recieves 13-digit unix time/date format from citizen. 
    This function updates the dataframe in-place. It updates
    the "created_at" field and replaces it with a date time object

    Inputs:
        df
        data_attributes (tuple)

    Returns (pandas dataframe) updates date col in place and 
        replaces with a datetime object
    """
    dt_objs = []
    cal_objs = []
    time_objs = []
    for index, row in df.iterrows():
        timestamp = row['date']
        print(timestamp)
        if source == "citizen":
            timestamp = int(timestamp) / 1000
            dt_obj = datetime.datetime.fromtimestamp(timestamp)
        elif source == "chi":
            dt_obj = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        dt_objs.append(dt_obj)
        cal_date = dt_obj.date()
        time = dt_obj.time()
        dt_objs.append(dt_obj)
        cal_objs.append(cal_date)
        time_objs.append(time)
    df['date'] = dt_objs
    df['calendar day'] = cal_objs
    df['time'] = time_objs
    return df

def link_records(chi, citizen, time_lower_bound, time_upper_bound):
    """
    """
    with open(output_filename, "w") as file:
        spamwriter = csv.writer(file, delimiter = ",")
        chi.eq(citizen)
        for month in range(12):
            for chi_row in chi.itertuples():
                for citizen_row in citizen.itertuples():
                    citi_date_obj, chi_date_obj = citizen_row['date'], chi_row['date']
                
                



# standard_date_time(citizen_df, date_col = 'created_at')





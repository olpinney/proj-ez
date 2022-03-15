
import pandas as pd
from geopy.geocoders import Nominatim as nom
from geopy import distance
import datetime
import sqlite3
import ast
import numpy as np
import csv

# import geopy as geo
# import geopandas as geopd

chi_data = 'Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings.csv'
incidents = 'ECCSC First Responder_ Incident Report (Responses) - Form Responses 1.csv'
citizen = 'olpinney/data/citizen_0-1646429990000.csv'

DIST_LOWER_BOUND = 0
DIST_UPPER_BOUND = .25
TIME_LOWER_BOUND = 1
TIME_UPPER_BOUND = 1

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
    """
    Runs entire py file
    """
    #created connection
    connection = sqlite3.connect("proj_ez.sqlite3")
    citi = connection.cursor()
    chi = connection.cursor()
    mock = connection.cursor()

    citizen_query = '''
        SELECT 
            title as description,
            created_at as date, 
            lat,
            long,
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

    #update mock_df house num to int to str
    mock_df['house_num'] = mock_df.house_num.fillna(0)
    mock_df['house_num'] = mock_df.house_num.astype(int)
    mock_df['house_num'] = mock_df.house_num.astype(str)

    clean_lat_long(citizen_df, 'citizen')
    clean_lat_long(chi_df, 'chi')

    standard_date_time(citizen_df, 'citizen')
    standard_date_time(chi_df, 'chi')

    print("length before drops", "chi data length:", len(chi_df), "citizen data length", len(citizen_df))
    chi_df = chi_df.drop_duplicates(keep = 'first')
    citizen_df = citizen_df.drop_duplicates(keep = 'first')
    print("length after drops", "chi data length:", len(chi_df), "citizen data length", len(citizen_df))

    link_records(citizen_df, chi_df, DIST_LOWER_BOUND, DIST_UPPER_BOUND, TIME_LOWER_BOUND, TIME_UPPER_BOUND )
    print_date_timeframes(citizen_df, chi_df)
    return citizen_df, chi_df, mock_df

def print_date_timeframes(citizen_df, chi_df):
    """
    Call to print out the duration of each dataframe
    """
    earliest_match_date = max(chi_df['date'].min(), citizen_df['date'].min())
    latest_match_date = min(chi_df['date'].max(), citizen_df['date'].max())
    print("Chicago Data Portal:", "Start", chi_df['date'].min(),  "End", chi_df['date'].max())
    print("Citizen:", "Start", citizen_df['date'].min(), "End", citizen_df['date'].max())
    print("Overlap", earliest_match_date, "to", latest_match_date)
    print("Num days of overlap", latest_match_date - earliest_match_date)


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
        #lst = df['lat_long'].tolist()
        #tup_lst = []
        #for s in lst:
        #    tup_lst.append(ast.literal_eval(s))
        #df['lat_long'] = tup_lst
        df['lat_long'] = list(zip(df.lat, df.long))
    elif source == 'chi':
        df.astype({'latitude': 'float64', 'longitude': 'float64'})
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

# currently not using this... chicago and citizen have lat / long
# keep for mock data
def get_lat_long(mock_df):
    """
    converts block and zipcode into a lat / lon coordinate

    Inputs:
        
    """
    # user agent is how requests to geopy are tracked (DON'T CALL ON FULL pandas)
    # mock_df['house_num'] = mock_df['house_num'].astype(float).round().astype(int)
    # mock_df.astype({'house_num': 'float64'})
    # mock_df['house_num'].fillna(0).astype(int(float))
    # mock_df.astype({'house_num': str})
    lat_longs = []
    for _, row in mock_df.iterrows():
        # print("address", [row['house_num'], row['street_name'], row['street_type'], "Chicago"])
        if row['house_num'] and row['street_type']:
            address = " ".join([row['house_num'], row['street_name'], row['street_type'], "Chicago"])
        elif row['house_num']:
            address = " ".join([row['house_num'], row['street_name'], "Chicago"])
        elif row['street_intersection']:
            address = " ".join([row['street_name'], row['street_type'], "and", row['street_intersection'], "Chicago"])
        else:
            print("insufficent location data")
            continue
        locator = nom(user_agent= 'ecjackson1821@gmail.com')
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


def reported_difference_in_dist(loc_1, loc_2, lower_bound, upper_bound):
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
    time_objs = []
    for index, row in df.iterrows():
        timestamp = row['date']
        if source == "citizen":
            timestamp = float(timestamp)
            timestamp = int(timestamp) / 1000
            dt_obj = datetime.datetime.fromtimestamp(timestamp)
        elif source == "chi":
            dt_obj = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        day_obj = dt_obj.date()
        dt_objs.append(day_obj)
        time = dt_obj.time()
        time_objs.append(time)
    df['date'] = dt_objs
    df['time'] = time_objs
    return df

def link_records(citizen, chi, dist_lower_bound, dist_upper_bound, time_lower_bound, time_upper_bound):
    """
    """
    if len(chi) < len(citizen):
        smaller_df = chi
        suffix = '_chi'
        suffix2 ='_citizen'
        larger_df = citizen
    else:
        smaller_df = citizen
        suffix = '_citizen'
        suffix2 = '_chi'
        larger_df = chi

    header = list(smaller_df.add_suffix(suffix).columns) + list(larger_df.add_suffix(suffix2).columns)

    with open('match_file.csv', "w") as file:
        spamwriter = csv.writer(file, delimiter = ",")
        spamwriter.writerow(header)
        for _,small_row in smaller_df.iterrows():
            filtered_df = larger_df.loc[larger_df['date'] == small_row['date']]
            #filtered_df = np.where(larger_df['date'] == small_row['date'])
            for _,large_row in filtered_df.iterrows():
                #fix time to handle edge cases
                if small_row['time'].hour >= large_row['time'].hour - time_lower_bound and \
                    small_row['time'].hour <= large_row['time'].hour + time_upper_bound:
                    match = reported_difference_in_dist(small_row['lat_long'], large_row['lat_long'], dist_lower_bound, dist_upper_bound) #can pass different upper,lower 
                    # print("found time bound with this row")
                    # print("******************************************************")
                    # print("small", small_row) 
                    # print("large", large_row)
                    if match:
                        print("*!%#($#))!%(%#*!)%!#))%#!*(*^!)!_")
                        print("there is a match")
                        output = pd.concat([small_row, large_row], axis=0)
                        spamwriter.writerow(output)




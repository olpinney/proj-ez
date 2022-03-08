
import pandas as pd
from geopy.geocoders import Nominatim as nom
from geopy import distance
import datetime

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

#created connection
connection = sqlite3.connect("proj_ez.sqlite3")
c = connection.cursor()

citizen_query = '''
       SELECT title, created_at as Date, lat_long as lat_long, 
       title, catagories
       FROM citizen
       LIMIT 10
    '''

query = (c.execute(citizen_query).fetchall())
#converts to dataframe
df = pd.DataFrame(query, columns=get_header(c))
#converts from list of str to list of tuples nd splits 
#lat/long column to two columns to match with chigao database
lst = df['lat_long'].tolist()
tup_lst = []
for s in lst:
    tup_lst.append(ast.literal_eval(s))
df[['LATITUDE', 'LONGITUDE']] = tup_lst


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

def standard_date_time(df, date_col = 'created_at'):
    """
    Recieves 13-digit unix time/date format from citizen
    """
    date_objs = [] 
    for index, row in df.iterrows():
        timestamp = row[date_col]
        print(timestamp)
        timestamp = timestamp / 1000
        date_obj = datetime.datetime.fromtimestamp(timestamp)
        date_objs.append(date_obj)
    df[date_col] = date_objs
    return df








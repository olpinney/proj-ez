
import pandas as pd
from geopy.geocoders import Nominatim as nom
from geopy import distance

# import geopy as geo
# import geopandas as geopd

filename = 'Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings.csv'

def read_csv_file(filename):
    """
    using to run tests in advance of receipt of Chicago API data

    Inputs:
        filename (string): name of csv file
    """
    cols_to_keep = ['CASE_NUMBER', 'BLOCK', 'DATE', 'INCIDENT_PRIMARY', 'ZIP_CODE', 
                    'WARD','COMMUNITY_AREA', 'STREET_OUTREACH_ORGANIZATION']
    chicago_api = pd.read_csv(filename, index_col = 'CASE_NUMBER', usecols = \
                                cols_to_keep)
    return chicago_api

# geopandas will need BLOCK + ZIPCODE to calculate lat / long

def get_lat_long():
    """
    converts address in a lat / long coordinate

    Inputs:
        
    """
    # user agent is how requests to geopy are tracked (DON'T CALL ON FULL PD.)
    locator = nom(user_agent= 'ecjackson1821@gmail.com')
    location = locator.geocode("3400 W CHICAGO AVE 60651")
    pass

def reported_difference_in_dist(loc_1, loc_2, lower_bound = 0, upper_bound):
    """
    takes in two lat/long tuples and finds the geodesic distance between the 
    two points. Uses lower and upper bound limits to determine likelihood of
    accurate match
    """
    dist = distance.great_circle.(loc_1, loc_2).ft



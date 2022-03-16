'''
This file pulls data from ECCSC survey data and inserts into sql table
'''

import csv
import pandas as pd
import util

# use if future google sheet has more than one tab
# sheet_id = '1TtuU7wu05m-Jefzwyj4NAYzr4XSYA3NHV44LvMgh0ko'
# sheet_name = “mock_data”
# url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

def read_sheet():
    """
    connects to google spreadsheet, pulls data and inserts into sql table
    """
    sheet_url = r'https://docs.google.com/spreadsheets/d/1TtuU7wu05m-Jefzwyj4NAYzr4XSYA3NHV44LvMgh0ko/edit#gid=0'
    url_1 = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')

    mock = pd.read_csv(url_1)
    mock.rename(columns = {'House Number' : 'house_num', 'Street Name': 'street_name',
        'Street Type': 'street_type',
        'Intersecting Street Name': 'street_intersection',
        'Date of Incident (only needed if this form is filled out after the date of incident has passed) ': 'date',
        'Time of incident (best estimate)': 'time', 'Incident Type': 'primary_type',
        'Was the incident attempted or completed?': 'attempted',
        'Did ECCSC arrive before Chicago Police?': 'before_police',
        'Were there other community street outreach organizations on the scene?': 'orgs_at_scene',
        'If yes, list the organization(s)': 'orgs',
        'How did ECCSC hear about the incident? ': 'alerted_by',
        'Optional: Additional notes / commentary': 'notes'}, inplace = True)
    util.insert_sql(mock, 'mock')
    